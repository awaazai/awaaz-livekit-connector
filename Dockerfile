# Connector image: bridges Awaaz Media Streams <-> a LiveKit room.
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY connector.py .

# WebSocket port Awaaz connects to (put a TLS proxy in front for wss://).
EXPOSE 8080
ENV CONNECTOR_HOST=0.0.0.0 CONNECTOR_PORT=8080

CMD ["python", "connector.py"]
