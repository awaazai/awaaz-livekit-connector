# Sample LiveKit agents

Three agents to test the connector and to use as a starting point. All run as
LiveKit **workers** (auto-dispatched into each per-call room the connector
creates), so they work with live calls, not just a fixed test room.

| Agent | Needs API keys? | Use it to... |
|---|---|---|
| `tone_agent.py` | No | Deterministic playback test: caller hears a steady tone the instant the agent joins. Proves connector -> mod_audio_fork -> caller. |
| `echo_agent.py` | No | Echo the caller's audio back. Proves both legs with real (non-tone) audio. |
| `voice_agent.py` | Yes | A real conversation (Deepgram STT + OpenAI LLM + ElevenLabs TTS) and the barge-in hook to copy into your own agent. |

## Setup

```bash
cd agents
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
set -a && source ../.env && set +a      # same LiveKit creds the connector uses
```

For `voice_agent.py`, also add provider keys to the repo-root `.env`:
```bash
DEEPGRAM_API_KEY=...
OPENAI_API_KEY=...
ELEVEN_API_KEY=...
```

## Run (local worker, dev mode)

```bash
python tone_agent.py dev      # then call your Awaaz number -> hear a tone
python echo_agent.py dev      # speak -> hear yourself (ECHO_BARGE_IN=0 to mute the test clear)
python voice_agent.py dev     # full conversation
```

Keep `LIVEKIT_AGENT_NAME` empty in `.env` — these agents use auto-dispatch
(join every room), so the connector doesn't need to dispatch by name.

## Barge-in hook

`voice_agent.py` publishes `{"event":"clear"}` on the `audio_control` topic on
`user_started_speaking`; the connector forwards that to Awaaz to flush queued
TTS. Copy this into your own agent to support interruption.

## Deploying the agent

- **Local / self-hosted (recommended for latency):** run the worker with
  `python voice_agent.py start` on your own host (co-located with the connector).
- **LiveKit Cloud:** use the included `Dockerfile` + `livekit.toml`. Run
  `lk agent create` to scaffold/link, set provider keys as secrets, then
  `lk agent deploy`. See the top-level README for the full comparison.
