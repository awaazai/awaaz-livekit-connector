# Awaaz ⟷ LiveKit connector

A standalone service that bridges the **Awaaz Media Streams** WebSocket protocol
to a **LiveKit** room, so a LiveKit voice agent can take phone calls hosted on
the Awaaz platform.

It plays the same role as LiveKit's hosted Twilio connector (`ConnectTwilioCall`),
but for Awaaz's protocol — which you run yourself. **Your agent code does not
change**; this service sits in front of it.

```
  Awaaz telephony            this connector              your LiveKit
  (mod_audio_fork)                                          agent
        |   Awaaz Media Streams      |        WebRTC (room)     |
        |   WebSocket, L16 8k        |                          |
        | -------------------------> | -----------------------> |   caller audio
        | <------------------------- | <----------------------- |   agent audio
```

## Repo layout

```
connector.py          the bridge (Awaaz WebSocket <-> LiveKit room)
requirements.txt      connector deps
.env.example          config template (copy to .env)
Dockerfile            connector container image
agents/               sample LiveKit agents + their deploy files
  tone_agent.py         deterministic playback test (no API keys)
  echo_agent.py         echo caller audio back (no API keys)
  voice_agent.py        full STT/LLM/TTS conversation + barge-in
  Dockerfile            agent image for LiveKit Cloud
  livekit.toml          LiveKit Cloud agent config template
  README.md             how to run/deploy the agents
```

## Three pieces (know what runs where)

| Piece | What it is | Who hosts it |
|---|---|---|
| **Connector** | This service. Bridges Awaaz WebSocket ⟷ a LiveKit room. | **Always you** — near your FreeSWITCH. |
| **LiveKit SFU** | The media server that rooms live in. | LiveKit Cloud **or** self-hosted by you. |
| **Agent** | Your LiveKit voice agent (STT/LLM/TTS). | LiveKit Cloud **or** locally by you. |

The connector is required in **both** deployment paths below — it is your bridge
and is never hosted by LiveKit.

## How it differs from a Twilio connector

The Awaaz protocol is the **same shape** as Twilio Media Streams
(`start` / `media` / `dtmf` / `mark` / `clear` / `stop`), with these differences.
If you adapt existing Twilio code (e.g. Pipecat's `TwilioFrameSerializer`),
change exactly these:

| | Twilio Media Streams | Awaaz Media Streams |
|---|---|---|
| Caller audio (inbound) | base64 **μ-law** 8 kHz | base64 **linear 16-bit PCM** 8 kHz |
| Playback audio (outbound) | base64 μ-law | base64 **WAV** (44-byte header + L16 8 kHz PCM) |
| JSON keys | camelCase (`streamSid`) | snake_case (`stream_sid`) |
| First message | `connected` then `start` | `start` (then `connected`) |

> The #1 mistake porting from Twilio is leaving the μ-law decode in place.
> Awaaz audio is linear PCM. Also note the **outbound** `media` payload must be a
> WAV file: `mod_audio_fork` skips a 44-byte header before playing the PCM.

Full protocol spec: https://docs.awaaz.de/voice-hosting/websocket

---

# Deployment

Two supported paths. Both run the connector; they differ in where the LiveKit
SFU and your agent live.

## Path A — LiveKit Cloud (easiest to start)

LiveKit hosts the SFU; you run the connector, and run the agent either locally
(recommended, see below) or on LiveKit Cloud.

1. **Create a LiveKit Cloud project** at https://cloud.livekit.io.
   Settings → Keys: copy `LIVEKIT_URL` (`wss://<proj>.livekit.cloud`),
   `LIVEKIT_API_KEY`, `LIVEKIT_API_SECRET`.
2. **Deploy the connector** on your own infra near FreeSWITCH
   (see *Connector deployment* below). Set the three values above in its env.
3. **Run the agent**:
   - **Locally (recommended):** `python agents/voice_agent.py start` on your own
     host. It registers with your LiveKit Cloud project and is auto-dispatched.
   - **On LiveKit Cloud:** from `agents/`, `lk agent create` (uses the included
     `Dockerfile` + `livekit.toml`), set provider keys as secrets, `lk agent deploy`.
4. **Point Awaaz** at the connector's public `wss://` URL.

## Path B — Local / self-hosted (lowest latency)

You run everything — SFU, connector, agent — ideally co-located with (or near)
your FreeSWITCH, so audio never leaves your network.

1. **Run the LiveKit SFU** (Docker):
   ```bash
   docker run -d --name livekit -p 7880:7880 -p 7881:7881 -p 7882:7882/udp \
     -e LIVEKIT_KEYS="<key>: <secret>" livekit/livekit-server --bind 0.0.0.0
   ```
   For production set real keys and expose the UDP media port (`7882`). Point the
   connector/agent at `LIVEKIT_URL=ws://<host>:7880` (or `wss://` behind TLS).
2. **Deploy the connector** on the same network (see below).
3. **Run the agent** locally as a worker against the self-hosted SFU.
4. **Point Awaaz** at the connector's `wss://` URL.

## Recommendation: run the agent locally

The audio loop is **caller → connector → LiveKit SFU → agent → back**. Each leg
is a network hop, so latency is lowest when the components are close together.

- **Run the agent locally**, co-located with the connector (and your telephony),
  rather than far from it. This keeps the connector↔agent path short.
- For the **lowest** latency, prefer **Path B**: self-host the SFU next to the
  connector so the whole audio loop stays on your own network instead of
  round-tripping to a cloud region on every frame.
- If you do use **Path A** (Cloud SFU), pick the LiveKit region nearest your
  connector/telephony to minimize the WAN legs.

## Connector deployment (both paths)

The connector is always self-hosted and needs a **stable public `wss://`
endpoint** reachable from your FreeSWITCH box.

```bash
docker build -t awaaz-connector .
docker run -d --name connector -p 8080:8080 --env-file .env awaaz-connector
```

1. Config via env (`.env`): `LIVEKIT_URL`, `LIVEKIT_API_KEY`,
   `LIVEKIT_API_SECRET`, `LIVEKIT_AGENT_NAME` (optional, for explicit dispatch),
   `CONNECTOR_HOST`/`CONNECTOR_PORT`.
2. Put a **TLS-terminating reverse proxy** in front (nginx, Caddy, ALB, etc.) so
   Awaaz connects to `wss://connector.yourdomain.com`. The connector dials **out**
   to the SFU, so it needs outbound 443 plus inbound 443 for the WebSocket.
3. **Scale horizontally** behind a WebSocket-aware load balancer — each call is
   an independent `Session`.

---

# Quick start (local testing)

Run everything locally and expose the connector with ngrok for a first
end-to-end test. Sample agents and details in [`agents/README.md`](agents/README.md).

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # fill in your LiveKit project values
set -a && source .env && set +a
python connector.py
# in another terminal, expose it:  ngrok http 8080
# give Awaaz the wss:// form of the ngrok URL
```

Then start an agent (separate terminal): `cd agents && python tone_agent.py dev`
and place a call — you should hear a tone.

## What the connector does

1. Accepts the inbound Awaaz WebSocket (Awaaz dials out to you).
2. On `start`: mints a LiveKit token, joins a room, publishes the caller's audio
   track, and (optionally) dispatches your agent.
   - Room/agent names can come from `custom_parameters` (`room_name`,
     `agent_name`); otherwise the room is derived from `call_sid` and the agent
     name from `LIVEKIT_AGENT_NAME`.
3. **Caller → agent**: decodes each `media` payload (L16 8 kHz) into a LiveKit
   `AudioSource`. LiveKit resamples to 48 kHz internally.
4. **Agent → caller**: subscribes to the agent's track, resamples to 8 kHz,
   rechunks to 20 ms frames, wraps each in a WAV header, sends `media` back.
5. **DTMF**: relayed into the room on topic `dtmf`.
6. **Barge-in**: forwarded to Awaaz when the agent publishes `{"event":"clear"}`
   on the `audio_control` topic (see `agents/README.md`).

## Notes

- **Status**: reference implementation. Validate against your LiveKit version and
  load-test before production.
- **Sample rates**: all audio is pinned to 8 kHz mono telephony; LiveKit does the
  8k↔48k resampling on both legs.
- **Outbound payload**: each `media` payload is a WAV file (44-byte header +
  320-byte L16 PCM = 364 bytes) because `mod_audio_fork` skips the header.

## License

MIT — see [LICENSE](LICENSE).
