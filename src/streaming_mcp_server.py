"""Streaming MCP Server - consumes from Amazon MSK (IAM auth) and pushes to WebSocket clients."""

import asyncio
import json
import os
import time
from collections import deque
from confluent_kafka import Consumer, KafkaError
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import websockets

BOOTSTRAP_SERVERS = os.environ.get("MSK_BOOTSTRAP", "localhost:9098")
TOPIC = os.environ.get("MSK_TOPIC", "app-logs")
REGION = os.environ.get("AWS_REGION", "us-east-1")
WS_PORT = int(os.environ.get("WS_PORT", "8765"))


def oauth_cb(config):
    """Generate IAM auth token for MSK SASL/OAUTHBEARER."""
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(REGION)
    return auth_token, expiry_ms / 1000


def create_consumer():
    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "mcp-streaming-server",
        "auto.offset.reset": "latest",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "OAUTHBEARER",
        "oauth_cb": oauth_cb,
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])
    return consumer


# --- State ---
event_buffer = deque(maxlen=100)
subscribers = {}  # ws -> asyncio.Queue
anomaly_state = {"error_count": 0, "total_count": 0, "last_spike": None}


def detect_anomaly(event):
    """Track error rates in rolling buffer."""
    event_buffer.append(event)
    anomaly_state["total_count"] = len(event_buffer)
    anomaly_state["error_count"] = sum(
        1 for e in event_buffer if e.get("level") == "ERROR"
    )
    rate = anomaly_state["error_count"] / max(anomaly_state["total_count"], 1)
    if rate > 0.3:
        anomaly_state["last_spike"] = time.time()
    return rate > 0.3


async def kafka_consumer_loop():
    """Poll MSK and push events to all subscribers."""
    consumer = create_consumer()
    loop = asyncio.get_event_loop()

    while True:
        msg = await loop.run_in_executor(None, lambda: consumer.poll(1.0))
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"Kafka error: {msg.error()}")
            continue

        event = json.loads(msg.value().decode("utf-8"))
        detect_anomaly(event)

        # Push to all subscribers
        dead = []
        for ws, queue in subscribers.items():
            if queue.qsize() < 500:
                await queue.put({"type": "event", "data": event})
            else:
                dead.append(ws)
        for ws in dead:
            subscribers.pop(ws, None)


async def handle_client(websocket):
    """Handle WebSocket client: subscribe/unsubscribe/get_anomalies/get_context."""
    queue = asyncio.Queue(maxsize=500)
    subscribers[websocket] = queue
    print(f"Client connected: {websocket.remote_address}")

    async def send_events():
        try:
            while True:
                msg = await queue.get()
                await websocket.send(json.dumps(msg))
        except websockets.ConnectionClosed:
            pass

    sender = asyncio.create_task(send_events())

    try:
        async for raw in websocket:
            request = json.loads(raw)
            action = request.get("action")

            if action == "get_anomalies":
                rate = anomaly_state["error_count"] / max(anomaly_state["total_count"], 1)
                await websocket.send(json.dumps({
                    "type": "response",
                    "action": "get_anomalies",
                    "data": {
                        "error_rate": round(rate, 3),
                        "error_count": anomaly_state["error_count"],
                        "window_size": anomaly_state["total_count"],
                        "is_anomaly": rate > 0.3,
                        "last_spike": anomaly_state["last_spike"],
                    },
                }))
            elif action == "get_context":
                await websocket.send(json.dumps({
                    "type": "response",
                    "action": "get_context",
                    "data": {
                        "subscribers": len(subscribers),
                        "buffer_size": len(event_buffer),
                        "topic": TOPIC,
                        "bootstrap": BOOTSTRAP_SERVERS,
                    },
                }))
            elif action == "unsubscribe":
                break
    except websockets.ConnectionClosed:
        pass
    finally:
        sender.cancel()
        subscribers.pop(websocket, None)
        print(f"Client disconnected: {websocket.remote_address}")


async def main():
    asyncio.create_task(kafka_consumer_loop())
    async with websockets.serve(handle_client, "0.0.0.0", WS_PORT):
        print(f"Streaming MCP Server running on ws://0.0.0.0:{WS_PORT}")
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    asyncio.run(main())
