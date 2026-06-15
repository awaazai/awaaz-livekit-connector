#!/usr/bin/env python3
"""
Awaaz Media Streams  <->  LiveKit connector (reference implementation).

This is the bridge a client needs to run to connect their LiveKit voice agent
to the Awaaz telephony platform. Awaaz streams live call audio to this service
over a WebSocket using the Awaaz Media Streams protocol
(https://docs.awaaz.de/voice-hosting/websocket). This service:

  1. Accepts the inbound Awaaz WebSocket connection (Awaaz dials out to us).
  2. On `start`, joins the caller into a LiveKit room and dispatches the agent.
  3. Caller audio  -> LiveKit:  base64 L16 8k  ->  published mic track.
  4. Agent  audio  -> caller:   subscribed agent track  ->  base64 L16 8k.
  5. Relays DTMF to the room, and forwards `clear` (barge-in) back to Awaaz.

The client's LiveKit *agent* code does not change. This is a standalone
service that sits in front of it.

Protocol note vs Twilio Media Streams (if you are adapting Twilio code):
  - Audio is linear 16-bit PCM @ 8 kHz, NOT mu-law. Do not mu-law decode.
  - JSON keys are snake_case (stream_sid, custom_parameters), not camelCase.
"""

import asyncio
import base64
import json
import logging
import os
import signal
import struct

import websockets
from livekit import api, rtc

# ---------------------------------------------------------------------------
# Configuration (via environment)
# ---------------------------------------------------------------------------
LIVEKIT_URL = os.environ.get("LIVEKIT_URL", "wss://your-project.livekit.cloud")
LIVEKIT_API_KEY = os.environ["LIVEKIT_API_KEY"]
LIVEKIT_API_SECRET = os.environ["LIVEKIT_API_SECRET"]

# Name of your LiveKit agent for explicit dispatch. Leave empty if your agent
# uses automatic dispatch (joins every room).
AGENT_NAME = os.environ.get("LIVEKIT_AGENT_NAME", "")

LISTEN_HOST = os.environ.get("CONNECTOR_HOST", "0.0.0.0")
LISTEN_PORT = int(os.environ.get("CONNECTOR_PORT", "8080"))

# Telephony audio is mono 16-bit PCM @ 8 kHz. 20 ms == 160 samples == 320 bytes.
SAMPLE_RATE = 8000
NUM_CHANNELS = 1
CHUNK_BYTES = 320  # bytes per outbound media frame (20 ms of L16 @ 8 kHz)

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
)
log = logging.getLogger("awaaz-livekit")


def _http_url(ws_url: str) -> str:
    """LiveKitAPI wants an http(s) URL; the SDK room wants ws(s)."""
    return ws_url.replace("wss://", "https://").replace("ws://", "http://")


def _wav_header(data_len: int, rate: int = SAMPLE_RATE,
                channels: int = NUM_CHANNELS, bits: int = 16) -> bytes:
    """44-byte canonical WAV/PCM header.

    mod_audio_fork plays JSON `media` payloads by skipping the first
    44 bytes and treating the rest as L16 PCM (lws_glue.cpp: `data + 44`), so
    every chunk we send must be a WAV file = this header + `data_len` bytes PCM.
    """
    byte_rate = rate * channels * bits // 8
    block_align = channels * bits // 8
    return (
        b"RIFF" + struct.pack("<I", 36 + data_len) + b"WAVE"
        + b"fmt " + struct.pack("<IHHIIHH", 16, 1, channels, rate,
                                byte_rate, block_align, bits)
        + b"data" + struct.pack("<I", data_len)
    )


class Session:
    """One live phone call: one Awaaz WebSocket <-> one LiveKit room."""

    def __init__(self, ws: websockets.WebSocketServerProtocol):
        self.ws = ws
        self.stream_sid: str | None = None
        self.call_sid: str | None = None
        self.room: rtc.Room | None = None
        self.source: rtc.AudioSource | None = None
        self._send_lock = asyncio.Lock()
        self._inbound: asyncio.Queue[bytes] = asyncio.Queue(maxsize=200)
        self._tasks: list[asyncio.Task] = []
        self._closed = asyncio.Event()
        # outbound bookkeeping for messages we send back to Awaaz
        self._seq = 0
        self._chunk = 0
        self._ts_ms = 0
        self._outbuf = bytearray()

    # -- lifecycle ---------------------------------------------------------

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
        if self.room is not None:
            try:
                await self.room.disconnect()
            except Exception:  # noqa: BLE001
                pass
        log.info("[%s] session closed", self.stream_sid)

    # -- Awaaz -> us -------------------------------------------------------

    async def _on_message(self, raw: str):
        msg = json.loads(raw)
        event = msg.get("event")
        if event == "start":
            await self._handle_start(msg)
        elif event == "media":
            self._handle_inbound_media(msg)
        elif event == "dtmf":
            await self._handle_dtmf(msg)
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
        params = start.get("custom_parameters", {}) or {}

        # Room/agent can be driven from custom_parameters, else derived.
        room_name = params.get("room_name") or f"awaaz-{self.call_sid or self.stream_sid}"
        agent_name = params.get("agent_name") or AGENT_NAME
        metadata = json.dumps(params)

        log.info("[%s] start: room=%s agent=%s params=%s",
                 self.stream_sid, room_name, agent_name or "(auto)", params)

        # 1) connect to the room as the caller
        token = (
            api.AccessToken(LIVEKIT_API_KEY, LIVEKIT_API_SECRET)
            .with_identity(f"caller-{self.call_sid or self.stream_sid}")
            .with_name("Phone caller")
            .with_metadata(metadata)
            .with_grants(api.VideoGrants(
                room_join=True, room=room_name,
                can_publish=True, can_subscribe=True,
            ))
            .to_jwt()
        )
        self.room = rtc.Room()
        self.room.on("track_subscribed", self._on_track_subscribed)
        self.room.on("data_received", self._on_data_received)
        await self.room.connect(LIVEKIT_URL, token)

        # 2) publish the caller's audio (8 kHz mono; LiveKit resamples for us)
        self.source = rtc.AudioSource(SAMPLE_RATE, NUM_CHANNELS)
        track = rtc.LocalAudioTrack.create_audio_track("caller-audio", self.source)
        await self.room.local_participant.publish_track(
            track, rtc.TrackPublishOptions(source=rtc.TrackSource.SOURCE_MICROPHONE)
        )

        # 3) explicit agent dispatch (skip if your agent auto-dispatches)
        if agent_name:
            await self._dispatch_agent(room_name, agent_name, metadata)

        # 4) start the inbound writer (caller PCM -> LiveKit)
        self._tasks.append(asyncio.create_task(self._inbound_writer()))

    async def _dispatch_agent(self, room_name: str, agent_name: str, metadata: str):
        lkapi = api.LiveKitAPI(_http_url(LIVEKIT_URL), LIVEKIT_API_KEY, LIVEKIT_API_SECRET)
        try:
            await lkapi.agent_dispatch.create_dispatch(
                api.CreateAgentDispatchRequest(
                    agent_name=agent_name, room=room_name, metadata=metadata
                )
            )
            log.info("[%s] dispatched agent %s", self.stream_sid, agent_name)
        finally:
            await lkapi.aclose()

    def _handle_inbound_media(self, msg: dict):
        payload = msg.get("media", {}).get("payload")
        if not payload:
            return
        pcm = base64.b64decode(payload)
        try:
            self._inbound.put_nowait(pcm)
        except asyncio.QueueFull:
            # drop oldest to stay real-time rather than build latency
            try:
                self._inbound.get_nowait()
                self._inbound.put_nowait(pcm)
            except asyncio.QueueEmpty:
                pass

    async def _handle_dtmf(self, msg: dict):
        if self.room is None:
            return
        digit = msg.get("dtmf", {}).get("digit")
        payload = json.dumps({"type": "dtmf", "digit": digit}).encode()
        await self.room.local_participant.publish_data(payload, topic="dtmf")
        log.info("[%s] DTMF '%s' -> room", self.stream_sid, digit)

    # -- caller PCM -> LiveKit --------------------------------------------

    async def _inbound_writer(self):
        assert self.source is not None
        while not self._closed.is_set():
            pcm = await self._inbound.get()
            frame = rtc.AudioFrame(
                data=pcm,
                sample_rate=SAMPLE_RATE,
                num_channels=NUM_CHANNELS,
                samples_per_channel=len(pcm) // 2,
            )
            # capture_frame paces internally to keep the stream real-time
            await self.source.capture_frame(frame)

    # -- LiveKit -> caller -------------------------------------------------

    def _on_track_subscribed(self, track: rtc.Track, *_):
        if track.kind == rtc.TrackKind.KIND_AUDIO:
            log.info("[%s] subscribed to agent audio track", self.stream_sid)
            self._tasks.append(asyncio.create_task(self._pump_agent_audio(track)))

    async def _pump_agent_audio(self, track: rtc.Track):
        # Ask LiveKit to resample the agent's audio (usually 48 kHz) down to 8 kHz.
        stream = rtc.AudioStream(track, sample_rate=SAMPLE_RATE, num_channels=NUM_CHANNELS)
        try:
            async for event in stream:
                self._outbuf.extend(event.frame.data.tobytes())
                while len(self._outbuf) >= CHUNK_BYTES:
                    chunk = bytes(self._outbuf[:CHUNK_BYTES])
                    del self._outbuf[:CHUNK_BYTES]
                    await self._send_media(chunk)
        finally:
            await stream.aclose()

    # -- us -> Awaaz -------------------------------------------------------

    async def _send_media(self, pcm: bytes):
        self._seq += 1
        self._chunk += 1
        self._ts_ms += 20
        if self._chunk == 1:
            log.info("[%s] sending FIRST media frame to Awaaz (%d B PCM + 44 B WAV)",
                     self.stream_sid, len(pcm))
        elif self._chunk % 50 == 0:
            log.info("[%s] sent %d media frames to Awaaz", self.stream_sid, self._chunk)
        # mod_audio_fork skips a 44-byte WAV header on playback, so wrap the PCM.
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

    async def _send_clear(self):
        await self._send({"event": "clear", "stream_sid": self.stream_sid})
        log.info("[%s] sent clear (barge-in) -> Awaaz", self.stream_sid)

    async def _send(self, obj: dict):
        async with self._send_lock:
            try:
                await self.ws.send(json.dumps(obj))
            except websockets.ConnectionClosed:
                pass

    # -- agent control (barge-in) -----------------------------------------

    def _on_data_received(self, data: rtc.DataPacket):
        """The agent signals interruption by publishing a data packet on the
        'audio_control' topic, e.g. {"event": "clear"}. We forward it to Awaaz
        so any buffered TTS already sent to the caller is flushed."""
        if data.topic != "audio_control":
            return
        try:
            payload = json.loads(bytes(data.data).decode())
        except (ValueError, UnicodeDecodeError):
            return
        if payload.get("event") == "clear":
            self._outbuf.clear()
            asyncio.create_task(self._send_clear())


async def _handler(ws: websockets.WebSocketServerProtocol):
    log.info("new Awaaz connection from %s", ws.remote_address)
    await Session(ws).run()


async def main():
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    async with websockets.serve(_handler, LISTEN_HOST, LISTEN_PORT, max_size=None):
        log.info("Awaaz<->LiveKit connector listening on ws://%s:%d",
                 LISTEN_HOST, LISTEN_PORT)
        await stop.wait()
    log.info("shutting down")


if __name__ == "__main__":
    asyncio.run(main())
