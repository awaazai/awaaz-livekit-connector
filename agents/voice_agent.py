#!/usr/bin/env python3
"""
Realistic LiveKit VOICE agent (STT -> LLM -> TTS) for testing the connector
with an actual conversation. Use this once echo_agent.py proves the audio path.

Runs as a LiveKit worker, auto-dispatched into each room the connector creates.
Requires API keys for the plugins below (swap providers as you like):
    DEEPGRAM_API_KEY     (STT)
    OPENAI_API_KEY       (LLM)
    ELEVEN_API_KEY       (TTS - ElevenLabs)

It also wires barge-in: when the user starts speaking over the agent, it
publishes {"event":"clear"} on the `audio_control` topic, which the connector
forwards to Awaaz to flush already-sent TTS. THIS is the real-world barge-in
hook your client copies into their own agent.

Install:
    pip install -r requirements.txt
Run:
    set -a && source ../.env && set +a
    python voice_agent.py dev
"""

import logging
import os

from livekit import agents
from livekit.agents import Agent, AgentSession
from livekit.plugins import deepgram, openai, elevenlabs, silero

logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
log = logging.getLogger("voice-agent")


class Assistant(Agent):
    def __init__(self):
        super().__init__(
            instructions=(
                "You are a friendly voice assistant testing a phone connector. "
                "Keep replies short and conversational. If asked, confirm you "
                "are running on LiveKit."))


async def entrypoint(ctx: agents.JobContext):
    await ctx.connect()
    log.info("voice agent joined room %s", ctx.room.name)

    session = AgentSession(
        stt=deepgram.STT(),
        llm=openai.LLM(model="gpt-4o-mini"),
        tts=elevenlabs.TTS(),
        vad=silero.VAD.load(),
    )

    # Barge-in: tell the connector to flush queued TTS when the user interrupts.
    @session.on("user_started_speaking")
    def _on_interrupt():
        import asyncio
        asyncio.create_task(ctx.room.local_participant.publish_data(
            b'{"event": "clear"}', topic="audio_control"))
        log.info("user_started_speaking -> sent barge-in clear")

    await session.start(room=ctx.room, agent=Assistant())
    await session.generate_reply(
        instructions="Greet the caller and ask how you can help.")


if __name__ == "__main__":
    agents.cli.run_app(agents.WorkerOptions(entrypoint_fnc=entrypoint))
