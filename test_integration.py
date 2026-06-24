"""Integration test - uses in-process mock Kafka + real Bedrock invocation."""

import asyncio
import json
import time
from collections import deque
import threading
import websockets
import boto3

# --- Mock Kafka (in-process queue) ---
_topic_queue = deque(maxlen=500)


class MockConsumer:
    def __init__(self, *a, **kw):
        self._subscribed = False

    def subscribe(self, topics):
        self._subscribed = True

    def poll(self, timeout):
        if _topic_queue:
            return MockMessage(_topic_queue.popleft())
        time.sleep(timeout)
        return None


class MockProducer:
    def __init__(self, *a, **kw):
        pass

    def produce(self, topic, value):
        _topic_queue.append(value)

    def flush(self):
        pass


class MockMessage:
    def __init__(self, data):
        self._data = data

    def value(self):
        return self._data if isinstance(self._data, bytes) else self._data.encode()

    def error(self):
        return None


# --- Monkey-patch the server module ---
import src.streaming_mcp_server as server

server.create_consumer = lambda: MockConsumer()

# --- Test ---
WS_PORT = 8766  # use different port for test
server.WS_PORT = WS_PORT

BEDROCK_MODEL_ID = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"


def produce_events():
    """Simulate log events including an error spike."""
    time.sleep(2)  # wait for server to start
    producer = MockProducer()
    print("[Producer] Sending 15 normal events...")
    for i in range(15):
        event = {"timestamp": time.time(), "service": "api-gateway", "level": "INFO", "message": "Request OK"}
        producer.produce("app-logs", json.dumps(event))
        _topic_queue.append(json.dumps(event).encode())
        time.sleep(0.1)

    print("[Producer] Sending 10 ERROR events (spike)...")
    for i in range(10):
        event = {"timestamp": time.time(), "service": "auth-service", "level": "ERROR", "message": "ConnectionRefused: auth-service failed"}
        _topic_queue.append(json.dumps(event).encode())
        time.sleep(0.1)

    print("[Producer] Sending 5 more normal events...")
    for i in range(5):
        event = {"timestamp": time.time(), "service": "order-service", "level": "INFO", "message": "Order processed"}
        _topic_queue.append(json.dumps(event).encode())
        time.sleep(0.1)

    time.sleep(3)  # let agent process


async def agent_test():
    """Connect as agent, receive events, detect anomaly, call Bedrock."""
    await asyncio.sleep(3)  # wait for server + events
    error_window = deque(maxlen=20)
    results = {"events_received": 0, "anomaly_detected": False, "bedrock_response": None}

    async with websockets.connect(f"ws://localhost:{WS_PORT}") as ws:
        print("[Agent] Connected to MCP server")
        try:
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=10)
                msg = json.loads(raw)
                if msg.get("type") != "event":
                    continue
                event = msg["data"]
                results["events_received"] += 1
                is_error = event.get("level") == "ERROR"
                error_window.append(1 if is_error else 0)

                if len(error_window) >= 10:
                    rate = sum(error_window) / len(error_window)
                    if rate > 0.3 and not results["anomaly_detected"]:
                        results["anomaly_detected"] = True
                        print(f"[Agent] ANOMALY DETECTED! Error rate: {rate:.0%}")

                        # Query server
                        await ws.send(json.dumps({"action": "get_anomalies"}))
                        resp = json.loads(await ws.recv())
                        print(f"[Agent] Server anomaly state: {resp.get('data')}")

                        # Call Bedrock
                        print("[Agent] Invoking Bedrock for analysis...")
                        bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")
                        response = bedrock.invoke_model(
                            modelId=BEDROCK_MODEL_ID,
                            contentType="application/json",
                            accept="application/json",
                            body=json.dumps({
                                "anthropic_version": "bedrock-2023-05-31",
                                "max_tokens": 256,
                                "messages": [{"role": "user", "content": f"Briefly analyze this error spike in 2-3 sentences: {json.dumps(event)}"}],
                            }),
                        )
                        result = json.loads(response["body"].read())
                        results["bedrock_response"] = result["content"][0]["text"]
                        print(f"[Agent] Bedrock response: {results['bedrock_response'][:200]}")
                        break
        except asyncio.TimeoutError:
            pass

    return results


async def run_test():
    # Start server
    server_task = asyncio.create_task(server.main())

    # Start producer in thread
    producer_thread = threading.Thread(target=produce_events, daemon=True)
    producer_thread.start()

    # Run agent
    results = await agent_test()

    # Results
    print("\n" + "=" * 60)
    print("INTEGRATION TEST RESULTS")
    print("=" * 60)
    print(f"  Events received:  {results['events_received']}")
    print(f"  Anomaly detected: {results['anomaly_detected']}")
    print(f"  Bedrock invoked:  {results['bedrock_response'] is not None}")
    if results["bedrock_response"]:
        print(f"  Bedrock analysis: {results['bedrock_response'][:300]}")
    print("=" * 60)

    all_pass = results["events_received"] > 0 and results["anomaly_detected"] and results["bedrock_response"]
    print(f"\n{'PASS' if all_pass else 'FAIL'}")

    server_task.cancel()
    return all_pass


if __name__ == "__main__":
    success = asyncio.run(run_test())
    exit(0 if success else 1)
