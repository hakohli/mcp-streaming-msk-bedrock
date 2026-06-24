"""Live test - runs against real MSK Serverless + Bedrock from within VPC."""

import asyncio
import json
import os
import time
import threading
from collections import deque

os.environ.setdefault("MSK_BOOTSTRAP", "boot-hutmev55.c2.kafka-serverless.us-east-1.amazonaws.com:9098")
os.environ.setdefault("MSK_TOPIC", "app-logs")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

import sys
sys.path.insert(0, os.path.dirname(__file__))

from src.streaming_mcp_server import create_consumer, detect_anomaly, WS_PORT
import src.streaming_mcp_server as server
from src.log_simulator import create_producer, generate_event
import websockets
import boto3

MODEL_ID = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"


def produce_events():
    """Produce events to real MSK."""
    time.sleep(3)
    print("[Producer] Creating MSK producer...")
    producer = create_producer()
    topic = os.environ.get("MSK_TOPIC", "app-logs")

    print("[Producer] Sending 15 normal events to MSK...")
    for i in range(15):
        event = generate_event(force_error=False)
        producer.produce(topic, json.dumps(event).encode("utf-8"))
    producer.flush()
    print("[Producer] Normal events sent.")
    time.sleep(2)

    print("[Producer] Sending 10 ERROR events (spike) to MSK...")
    for i in range(10):
        event = generate_event(force_error=True)
        producer.produce(topic, json.dumps(event).encode("utf-8"))
    producer.flush()
    print("[Producer] Error spike sent.")
    time.sleep(2)

    print("[Producer] Sending 5 more normal events...")
    for i in range(5):
        event = generate_event(force_error=False)
        producer.produce(topic, json.dumps(event).encode("utf-8"))
    producer.flush()
    print("[Producer] Done producing.")


async def agent_test():
    """Connect to local WS server, detect anomaly, call Bedrock."""
    await asyncio.sleep(5)
    error_window = deque(maxlen=20)
    results = {"events_received": 0, "anomaly_detected": False, "bedrock_response": None}

    print(f"[Agent] Connecting to ws://localhost:{WS_PORT}...")
    async with websockets.connect(f"ws://localhost:{WS_PORT}") as ws:
        print("[Agent] Connected!")
        try:
            while True:
                raw = await asyncio.wait_for(ws.recv(), timeout=30)
                msg = json.loads(raw)
                if msg.get("type") != "event":
                    continue
                event = msg["data"]
                results["events_received"] += 1
                is_error = event.get("level") == "ERROR"
                error_window.append(1 if is_error else 0)
                symbol = "!" if is_error else "."
                print(symbol, end="", flush=True)

                if len(error_window) >= 10:
                    rate = sum(error_window) / len(error_window)
                    if rate > 0.3 and not results["anomaly_detected"]:
                        results["anomaly_detected"] = True
                        print(f"\n[Agent] ANOMALY DETECTED! Error rate: {rate:.0%}")

                        await ws.send(json.dumps({"action": "get_anomalies"}))
                        resp = json.loads(await ws.recv())
                        print(f"[Agent] Server state: {json.dumps(resp.get('data'), indent=2)}")

                        print("[Agent] Calling Bedrock...")
                        bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")
                        response = bedrock.invoke_model(
                            modelId=MODEL_ID,
                            contentType="application/json",
                            accept="application/json",
                            body=json.dumps({
                                "anthropic_version": "bedrock-2023-05-31",
                                "max_tokens": 256,
                                "messages": [{"role": "user", "content": f"Briefly analyze this error in 2-3 sentences: {json.dumps(event)}"}],
                            }),
                        )
                        result = json.loads(response["body"].read())
                        results["bedrock_response"] = result["content"][0]["text"]
                        print(f"[Agent] Bedrock: {results['bedrock_response']}")
                        break
        except asyncio.TimeoutError:
            print("\n[Agent] Timeout waiting for events")

    return results


async def main():
    print("=" * 60)
    print("LIVE TEST: MSK Serverless + Bedrock Agent")
    print(f"MSK: {os.environ.get('MSK_BOOTSTRAP')}")
    print("=" * 60)

    server_task = asyncio.create_task(server.main())
    producer_thread = threading.Thread(target=produce_events, daemon=True)
    producer_thread.start()

    results = await agent_test()

    print("\n" + "=" * 60)
    print("LIVE TEST RESULTS")
    print("=" * 60)
    print(f"  MSK Bootstrap:    {os.environ.get('MSK_BOOTSTRAP')}")
    print(f"  Events received:  {results['events_received']}")
    print(f"  Anomaly detected: {results['anomaly_detected']}")
    print(f"  Bedrock invoked:  {results['bedrock_response'] is not None}")
    if results["bedrock_response"]:
        print(f"  Bedrock analysis: {results['bedrock_response'][:300]}")
    print("=" * 60)

    passed = results["events_received"] > 0 and results["anomaly_detected"] and results["bedrock_response"]
    print(f"\n{'PASS' if passed else 'FAIL'}")
    server_task.cancel()
    return passed


if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)
