# Agent image for LiveKit Cloud (or self-hosted worker) deployment.
# Builds voice_agent.py. Provider keys are supplied at runtime as secrets,
# never baked into the image.
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY voice_agent.py .

# Pre-download model files (e.g. Silero VAD) so cold starts are fast.
RUN python voice_agent.py download-files || true

# Production worker mode (no autoreload).
CMD ["python", "voice_agent.py", "start"]
