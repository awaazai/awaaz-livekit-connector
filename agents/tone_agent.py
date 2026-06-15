#!/usr/bin/env python3
"""
Continuous-TONE LiveKit agent -- the most deterministic playback test.

Unlike echo_agent.py (which only emits audio when the caller speaks), this
publishes a steady sine tone the instant it joins the room. So if the
connector -> mod_audio_fork -> caller playback path works AT ALL, you hear a constant
beep on the call without speaking. Silence => the playback hop is broken
(audio isn't leaving the connector, or mod_audio_fork isn't injecting it).

No API keys needed. Auto-dispatched into each room the connector creates.

Run:
    set -a && source ../.env && set +a
    python tone_agent.py dev

Env:
    TONE_HZ     tone frequency in Hz (default 440)
"""

import asyncio
import logging
import math
import os
import struct

from livekit import agents, rtc

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
log = logging.getLogger("tone-agent")

RATE = 8000
CHANNELS = 1
SAMPLES_PER_FRAME = 160          # 20 ms @ 8 kHz
TONE_HZ = float(os.environ.get("TONE_HZ", "440"))


async def entrypoint(ctx: agents.JobContext):
    await ctx.connect(auto_subscribe=agents.AutoSubscribe.AUDIO_ONLY)
    log.info("tone agent joined room %s -- emitting %g Hz tone", ctx.room.name, TONE_HZ)

    source = rtc.AudioSource(RATE, CHANNELS)
    track = rtc.LocalAudioTrack.create_audio_track("agent-tone", source)
    await ctx.room.local_participant.publish_track(
        track, rtc.TrackPublishOptions(source=rtc.TrackSource.SOURCE_MICROPHONE))

    phase = 0.0
    inc = 2 * math.pi * TONE_HZ / RATE
    sent = 0
    while True:
        buf = bytearray()
        for _ in range(SAMPLES_PER_FRAME):
            buf += struct.pack("<h", int(0.3 * 32767 * math.sin(phase)))
            phase += inc
            if phase > 2 * math.pi:
                phase -= 2 * math.pi
        frame = rtc.AudioFrame(data=bytes(buf), sample_rate=RATE,
                               num_channels=CHANNELS,
                               samples_per_channel=SAMPLES_PER_FRAME)
        await source.capture_frame(frame)   # paces itself in real time
        sent += 1
        if sent == 1 or sent % 100 == 0:
            log.info("tone: published %d frames (%.1fs)", sent, sent * 0.02)


if __name__ == "__main__":
    agents.cli.run_app(agents.WorkerOptions(entrypoint_fnc=entrypoint))
