#!/usr/bin/env python3
"""
Drop-in replacement for connector.py that bypasses LiveKit entirely: on
`start`, it loops a local WAV file straight back to the caller over the same
Awaaz WebSocket wire protocol connector.py uses (JSON `media` events,
base64 WAV-wrapped L16 PCM @ 8 kHz), with the same jitter/gap logging.

Use this to isolate where turn-start jitter comes from: if it still shows up
here, the cause is in the connector<->mod_livekit<->network leg, not LiveKit
or the agent. If it's clean here but present on a real call, LiveKit/agent is
the more likely source.

Usage:
    python3 scripts/file_playback_connector.py path/to/8k_mono_16bit.wav

Point Awaaz (or your test module) at this instead of connector.py -- same
CONNECTOR_HOST/CONNECTOR_PORT/LOG_LEVEL env vars, no LIVEKIT_* vars needed.
"""

import asyncio
import base64
import json
import logging
import os
import signal
import struct
import sys
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
log = logging.getLogger("file-playback-connector")


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


def load_playback_audio(path: str) -> bytes:
    with wave.open(path, "rb") as wf:
        if wf.getframerate() != SAMPLE_RATE or wf.getnchannels() != 1 or wf.getsampwidth() != 2:
            raise SystemExit(
                f"{path} must be mono 16-bit PCM @ {SAMPLE_RATE} Hz "
                f"(got {wf.getframerate()} Hz, {wf.getnchannels()}ch, "
                f"{wf.getsampwidth() * 8}-bit). Convert with:\n"
                f"  ffmpeg -i {path} -ar {SAMPLE_RATE} -ac 1 -sample_fmt s16 -f wav fixed.wav"
            )
        return wf.readframes(wf.getnframes())


class Session:
    """One call: reads Awaaz `start`/`media`/`stop`, loops PLAYBACK_PCM back."""

    def __init__(self, ws: websockets.WebSocketServerProtocol, playback_pcm: bytes):
        self.ws = ws
        self.playback_pcm = playback_pcm
        self.stream_sid: str | None = None
        self.call_sid: str | None = None
        self._send_lock = asyncio.Lock()
        self._tasks: list[asyncio.Task] = []
        self._closed = asyncio.Event()
        self._seq = 0
        self._chunk = 0
        self._ts_ms = 0
        self._last_send_ms = 0.0
        self._last_inbound_ms = 0.0
        self._inbound_frame_count = 0
        self._inbound_peak_since_log = 0
        self._inbound_max_gap_since_log = 0.0
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
        elif event == "media":
            self._handle_inbound_media(msg)
        elif event == "dtmf":
            log.info("[%s] dtmf: %s", self.stream_sid, msg.get("dtmf", {}).get("digit"))
        elif event == "mark":
            log.debug("[%s] mark ack: %s", self.stream_sid, msg.get("mark"))
        elif event == "stop":
            log.info("[%s] received stop", self.stream_sid)
            await self.close()
        else:
            log.debug("ignoring unknown event: %s", event)

    async def _handle_start(self, msg: dict):
        start = msg.get("start", {})
        self.stream_sid = msg.get("stream_sid") or start.get("stream_sid")
        self.call_sid = start.get("call_sid")
        log.info("[%s] start (call_sid=%s) -- bypassing LiveKit, looping playback file",
                 self.stream_sid, self.call_sid)
        self._tasks.append(asyncio.create_task(self._playback_loop()))

    def _handle_inbound_media(self, msg: dict):
        payload = msg.get("media", {}).get("payload")
        if not payload:
            return
        pcm = base64.b64decode(payload)
        now = time.monotonic() * 1000
        if self._last_inbound_ms:
            gap = now - self._last_inbound_ms
            self._inbound_max_gap_since_log = max(self._inbound_max_gap_since_log, gap)
        self._last_inbound_ms = now

        self._inbound_frame_count += 1
        samples = struct.unpack(f"<{len(pcm) // 2}h", pcm)
        peak = max(map(abs, samples)) if samples else 0
        self._inbound_peak_since_log = max(self._inbound_peak_since_log, peak)
        if self._inbound_frame_count % FRAME_LOG_EVERY == 0:
            log.info("[%s] received %d inbound frames from Awaaz (peak=%d, max_gap=%.0fms)",
                      self.stream_sid, self._inbound_frame_count,
                      self._inbound_peak_since_log, self._inbound_max_gap_since_log)
            self._inbound_peak_since_log = 0
            self._inbound_max_gap_since_log = 0.0

    async def _playback_loop(self):
        """Loop the file continuously at real-time (20ms/frame) cadence,
        same pacing an agent's audio track would produce."""
        pos = 0
        n = len(self.playback_pcm)
        next_send = time.monotonic()
        while not self._closed.is_set():
            chunk = self.playback_pcm[pos:pos + CHUNK_BYTES]
            if len(chunk) < CHUNK_BYTES:
                # wrap around to the start of the file
                remainder = CHUNK_BYTES - len(chunk)
                chunk = chunk + self.playback_pcm[:remainder]
                pos = remainder
            else:
                pos += CHUNK_BYTES
                if pos >= n:
                    pos = 0
            await self._send_media(chunk)
            next_send += FRAME_MS / 1000
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
            log.info("[%s] sending FIRST playback frame to Awaaz (%d B PCM + 44 B WAV)",
                     self.stream_sid, len(pcm))
        else:
            gap = now - self._last_send_ms
            self._outbound_max_gap_since_log = max(self._outbound_max_gap_since_log, gap)
            if gap > TURN_GAP_MS:
                log.info("[%s] playback resumed after %.0f ms gap -- jitter in send loop itself",
                         self.stream_sid, gap)
            if self._chunk % FRAME_LOG_EVERY == 0:
                log.info("[%s] sent %d playback frames to Awaaz (peak=%d, max_gap=%.0fms)",
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
    if len(sys.argv) != 2:
        raise SystemExit(f"usage: {sys.argv[0]} path/to/8k_mono_16bit.wav")
    playback_pcm = load_playback_audio(sys.argv[1])
    log.info("loaded %d bytes (%.1fs) of playback audio from %s",
             len(playback_pcm), len(playback_pcm) / 2 / SAMPLE_RATE, sys.argv[1])

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    async def handler(ws: websockets.WebSocketServerProtocol):
        log.info("new Awaaz connection from %s", ws.remote_address)
        await Session(ws, playback_pcm).run()

    async with websockets.serve(handler, LISTEN_HOST, LISTEN_PORT, max_size=None):
        log.info("file-playback connector (bypassing LiveKit) listening on ws://%s:%d",
                 LISTEN_HOST, LISTEN_PORT)
        await stop.wait()
    log.info("shutting down")


if __name__ == "__main__":
    asyncio.run(main())
