#!/usr/bin/env python3
"""
Jitter-injecting variant of file_playback_connector.py: loops a local WAV
file back to the caller over the same Awaaz wire protocol, but deliberately
perturbs outbound send timing instead of the clean, fixed 20ms pacing the
plain tool uses.

Use this to reproduce/tune caller-audible jitter without a live LiveKit
agent: if a given jitter profile reproduces what testers hear on real calls,
that points at delivery timing (e.g. connector.py's _pump_agent_audio, which
has no pacing loop of its own -- see README/plan notes) rather than encoding
or LiveKit-specific mechanics.

Two jitter mechanisms, combined:
  - continuous per-chunk jitter: every frame's send time is perturbed by a
    random +/- offset (uniform between --jitter-min-ms and --jitter-max-ms).
  - occasional bursts: with probability --burst-prob per frame, simulate a
    stall of --burst-min-ms..--burst-max-ms, then flush the chunks that
    would have queued up during that stall back-to-back with no pacing --
    mimicking what _pump_agent_audio does when LiveKit's AudioStream
    delivers audio in uneven batches after a delay.

Accepts any mono 16-bit WAV; if it isn't already 8kHz, it's resampled via
ffmpeg first (mimicking the 48kHz agent-track -> 8kHz downsample step
connector.py's rtc.AudioStream performs before chunks reach Awaaz).

Usage:
    python3 scripts/jitter_playback_connector.py path/to/audio.wav \
        [--jitter-min-ms 20] [--jitter-max-ms 50] \
        [--burst-prob 0.02] [--burst-min-ms 100] [--burst-max-ms 400]
"""

import argparse
import array
import asyncio
import base64
import json
import logging
import os
import random
import signal
import struct
import time
import wave

import websockets

LISTEN_HOST = os.environ.get("CONNECTOR_HOST", "0.0.0.0")
LISTEN_PORT = int(os.environ.get("CONNECTOR_PORT", "8080"))

SAMPLE_RATE = 8000
NUM_CHANNELS = 1
CHUNK_BYTES = 320  # 20 ms of L16 @ 8 kHz
FRAME_MS = 20

TURN_GAP_MS = 200
FRAME_LOG_EVERY = 50

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
)
log = logging.getLogger("jitter-playback-connector")


def _wav_header(data_len: int, rate: int = SAMPLE_RATE,
                channels: int = NUM_CHANNELS, bits: int = 16) -> bytes:
    byte_rate = rate * channels * bits // 8
    block_align = channels * bits // 8
    return (
        b"RIFF" + struct.pack("<I", 36 + data_len) + b"WAVE"
        + b"fmt " + struct.pack("<IHHIIHH", 16, 1, channels, rate,
                                byte_rate, block_align, bits)
        + b"data" + struct.pack("<I", data_len)
    )


def _resample_linear(samples: array.array, src_rate: int, dst_rate: int) -> array.array:
    """Simple linear-interpolation resampler (stdlib only, no ffmpeg/numpy).
    No anti-alias filtering, but that's fine here: the 48kHz test file was
    itself upsampled from an 8kHz source, so there's no energy above 4kHz to
    alias -- good enough for a jitter test tool, not a general-purpose DSP."""
    ratio = dst_rate / src_rate
    n_in = len(samples)
    n_out = int(n_in * ratio)
    out = array.array("h", bytes(2 * n_out))
    for i in range(n_out):
        src_pos = i / ratio
        idx = int(src_pos)
        frac = src_pos - idx
        a = samples[idx]
        b = samples[idx + 1] if idx + 1 < n_in else a
        out[i] = int(a + (b - a) * frac)
    return out


def load_playback_audio(path: str) -> bytes:
    """Load a mono 16-bit WAV, resampling to 8kHz if needed -- mirrors the
    48kHz agent-track -> 8kHz downsample connector.py performs."""
    with wave.open(path, "rb") as wf:
        rate = wf.getframerate()
        channels = wf.getnchannels()
        width = wf.getsampwidth()
        raw = wf.readframes(wf.getnframes())
    if channels != 1 or width != 2:
        raise SystemExit(f"{path} must be mono 16-bit PCM (got {channels}ch, {width * 8}-bit)")

    if rate == SAMPLE_RATE:
        return raw

    log.info("resampling %s from %d Hz to %d Hz (mimics agent-track downsample)",
              path, rate, SAMPLE_RATE)
    samples = array.array("h")
    samples.frombytes(raw)
    return _resample_linear(samples, rate, SAMPLE_RATE).tobytes()


class Session:
    """One call: reads Awaaz `start`/`media`/`stop`, loops PLAYBACK_PCM back
    with injected send-timing jitter instead of clean real-time pacing."""

    def __init__(self, ws: websockets.WebSocketServerProtocol, playback_pcm: bytes, jitter_cfg: dict):
        self.ws = ws
        self.playback_pcm = playback_pcm
        self.jitter_cfg = jitter_cfg
        self.stream_sid: str | None = None
        self.call_sid: str | None = None
        self._send_lock = asyncio.Lock()
        self._tasks: list[asyncio.Task] = []
        self._closed = asyncio.Event()
        self._seq = 0
        self._chunk = 0
        self._ts_ms = 0
        self._last_send_ms = 0.0
        self._outbound_peak_since_log = 0
        self._outbound_max_gap_since_log = 0.0

    async def run(self):
        try:
            async for raw in self.ws:
                await self._on_message(raw)
        except websockets.ConnectionClosed:
            log.info("[%s] Awaaz WebSocket closed", self.stream_sid)
        finally:
            await self.close()

    async def close(self):
        if self._closed.is_set():
            return
        self._closed.set()
        for t in self._tasks:
            t.cancel()
        log.info("[%s] session closed", self.stream_sid)

    async def _on_message(self, raw: str):
        msg = json.loads(raw)
        event = msg.get("event")
        if event == "start":
            await self._handle_start(msg)
        elif event == "stop":
            log.info("[%s] received stop", self.stream_sid)
            await self.close()
        else:
            log.debug("ignoring event: %s", event)

    async def _handle_start(self, msg: dict):
        start = msg.get("start", {})
        self.stream_sid = msg.get("stream_sid") or start.get("stream_sid")
        self.call_sid = start.get("call_sid")
        log.info("[%s] start (call_sid=%s) -- looping playback with jitter profile %s",
                 self.stream_sid, self.call_sid, self.jitter_cfg)
        self._tasks.append(asyncio.create_task(self._playback_loop()))

    async def _playback_loop(self):
        """Loop the file, but perturb send timing: continuous random jitter
        on every frame, plus occasional stall+burst episodes."""
        cfg = self.jitter_cfg
        pos = 0
        n = len(self.playback_pcm)
        next_send = time.monotonic()
        burst_remaining = 0  # chunks left to flush back-to-back after a stall

        while not self._closed.is_set():
            chunk = self.playback_pcm[pos:pos + CHUNK_BYTES]
            if len(chunk) < CHUNK_BYTES:
                remainder = CHUNK_BYTES - len(chunk)
                chunk = chunk + self.playback_pcm[:remainder]
                pos = remainder
            else:
                pos += CHUNK_BYTES
                if pos >= n:
                    pos = 0

            await self._send_media(chunk)

            if burst_remaining > 0:
                # flush queued-up chunks with no pacing delay at all
                burst_remaining -= 1
                next_send = time.monotonic()
                continue

            if random.random() < cfg["burst_prob"]:
                stall_ms = random.uniform(cfg["burst_min_ms"], cfg["burst_max_ms"])
                burst_remaining = max(1, int(stall_ms // FRAME_MS))
                log.info("[%s] simulated stall: %.0f ms -> will burst %d queued chunks",
                         self.stream_sid, stall_ms, burst_remaining)
                await asyncio.sleep(stall_ms / 1000)
                next_send = time.monotonic()
                continue

            jitter_ms = random.uniform(cfg["jitter_min_ms"], cfg["jitter_max_ms"])
            sign = random.choice((-1, 1))
            next_send += (FRAME_MS + sign * jitter_ms) / 1000
            sleep_for = next_send - time.monotonic()
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)
            else:
                next_send = time.monotonic()

    async def _send_media(self, pcm: bytes):
        self._seq += 1
        self._chunk += 1
        self._ts_ms += 20
        now = time.monotonic() * 1000

        samples = struct.unpack(f"<{len(pcm) // 2}h", pcm)
        peak = max(map(abs, samples)) if samples else 0
        self._outbound_peak_since_log = max(self._outbound_peak_since_log, peak)

        if self._chunk == 1:
            log.info("[%s] sending FIRST playback frame to Awaaz", self.stream_sid)
        else:
            gap = now - self._last_send_ms
            self._outbound_max_gap_since_log = max(self._outbound_max_gap_since_log, gap)
            if gap > TURN_GAP_MS:
                log.info("[%s] playback resumed after %.0f ms gap", self.stream_sid, gap)
            if self._chunk % FRAME_LOG_EVERY == 0:
                log.info("[%s] sent %d playback frames (peak=%d, max_gap=%.0fms)",
                          self.stream_sid, self._chunk,
                          self._outbound_peak_since_log, self._outbound_max_gap_since_log)
                self._outbound_peak_since_log = 0
                self._outbound_max_gap_since_log = 0.0
        self._last_send_ms = now

        wav = _wav_header(len(pcm)) + pcm
        await self._send({
            "event": "media",
            "sequence_number": self._seq,
            "stream_sid": self.stream_sid,
            "media": {
                "chunk": self._chunk,
                "timestamp": str(self._ts_ms),
                "payload": base64.b64encode(wav).decode(),
            },
        })

    async def _send(self, obj: dict):
        async with self._send_lock:
            try:
                await self.ws.send(json.dumps(obj))
            except websockets.ConnectionClosed:
                pass


async def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("wav_path")
    parser.add_argument("--jitter-min-ms", type=float, default=5)
    parser.add_argument("--jitter-max-ms", type=float, default=30)
    parser.add_argument("--burst-prob", type=float, default=0.02,
                         help="probability per frame of a simulated stall+burst episode")
    parser.add_argument("--burst-min-ms", type=float, default=100)
    parser.add_argument("--burst-max-ms", type=float, default=400)
    args = parser.parse_args()

    jitter_cfg = {
        "jitter_min_ms": args.jitter_min_ms,
        "jitter_max_ms": args.jitter_max_ms,
        "burst_prob": args.burst_prob,
        "burst_min_ms": args.burst_min_ms,
        "burst_max_ms": args.burst_max_ms,
    }

    playback_pcm = load_playback_audio(args.wav_path)
    log.info("loaded %d bytes (%.1fs) of playback audio from %s",
             len(playback_pcm), len(playback_pcm) / 2 / SAMPLE_RATE, args.wav_path)
    log.info("jitter profile: %s", jitter_cfg)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    async def handler(ws: websockets.WebSocketServerProtocol):
        log.info("new Awaaz connection from %s", ws.remote_address)
        await Session(ws, playback_pcm, jitter_cfg).run()

    async with websockets.serve(handler, LISTEN_HOST, LISTEN_PORT, max_size=None):
        log.info("jitter-playback connector listening on ws://%s:%d", LISTEN_HOST, LISTEN_PORT)
        await stop.wait()
    log.info("shutting down")


if __name__ == "__main__":
    asyncio.run(main())
