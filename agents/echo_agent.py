#!/usr/bin/env python3
"""
Zero-dependency LiveKit ECHO agent -- the fastest way to prove the connector's
audio path works end to end, with NO STT/LLM/TTS API keys.

It runs as a LiveKit *worker*: it registers with your LiveKit project and is
dispatched into each room the connector creates per call. It subscribes to the
caller's audio and republishes it back, so the caller hears themselves. If you
call your Awaaz number and hear your own voice echoed back, the connector and
both audio legs (caller->LiveKit and LiveKit->caller) are working.

It also fires a barge-in `clear` on the `audio_control` topic ~700 ms after it
starts hearing the caller, to verify the connector forwards `clear` to Awaaz.
(Toggle with ECHO_BARGE_IN=0.)

Dispatch model:
  - No agent_name set below  => auto-dispatch: this worker joins EVERY room.
    Leave LIVEKIT_AGENT_NAME EMPTY in the connector's .env. (simplest)
  - To use explicit dispatch instead, set agent_name in WorkerOptions AND set
    LIVEKIT_AGENT_NAME to the same value in the connector's .env.

Run:
    set -a && source ../.env && set +a
    python echo_agent.py dev
"""

import asyncio
import logging
import os

from livekit import agents, rtc

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
log = logging.getLogger("echo-agent")

BARGE_IN = os.environ.get("ECHO_BARGE_IN", "1") == "1"


async def entrypoint(ctx: agents.JobContext):
    await ctx.connect(auto_subscribe=agents.AutoSubscribe.AUDIO_ONLY)
    log.info("echo agent joined room %s", ctx.room.name)
    barged = {"done": False}

    async def echo(track: rtc.Track):
        stream = rtc.AudioStream(track)
        source: rtc.AudioSource | None = None
        started = None
        async for ev in stream:
            f = ev.frame
            if source is None:
                source = rtc.AudioSource(f.sample_rate, f.num_channels)
                out = rtc.LocalAudioTrack.create_audio_track("agent-echo", source)
                await ctx.room.local_participant.publish_track(
                    out, rtc.TrackPublishOptions(
                        source=rtc.TrackSource.SOURCE_MICROPHONE))
                started = asyncio.get_event_loop().time()
                log.info("echoing back @ %d Hz", f.sample_rate)
            await source.capture_frame(f)

            if (BARGE_IN and not barged["done"] and started
                    and asyncio.get_event_loop().time() - started > 0.7):
                barged["done"] = True
                await ctx.room.local_participant.publish_data(
                    b'{"event": "clear"}', topic="audio_control")
                log.info("sent test barge-in clear on audio_control")

    @ctx.room.on("track_subscribed")
    def _on_sub(track, pub, participant):
        if track.kind == rtc.TrackKind.KIND_AUDIO:
            log.info("subscribed caller audio from %s", participant.identity)
            asyncio.create_task(echo(track))

    # echo any track already present when we joined
    for p in ctx.room.remote_participants.values():
        for pub in p.track_publications.values():
            if pub.track and pub.track.kind == rtc.TrackKind.KIND_AUDIO:
                asyncio.create_task(echo(pub.track))


if __name__ == "__main__":
    # No agent_name => auto-dispatch into every room (simplest for testing).
    agents.cli.run_app(agents.WorkerOptions(entrypoint_fnc=entrypoint))
