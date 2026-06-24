"""Bedrock Agent Client - subscribes to streaming MCP server, uses Bedrock for anomaly analysis."""

import asyncio
import json
import os
from collections import deque
import boto3
import websockets

WS_URL = os.environ.get("MCP_SERVER_URL", "ws://localhost:8765")
MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "us.anthropic.claude-sonnet-4-5-20250929-v1:0")
REGION = os.environ.get("AWS_REGION", "us-east-1")
ERROR_THRESHOLD = 0.3

bedrock = boto3.client("bedrock-runtime", region_name=REGION)
error_window = deque(maxlen=20)


def analyze_with_bedrock(errors: list[dict]) -> str:
    """Send error batch to Bedrock for root cause analysis and remediation suggestions."""
    prompt = f"""You are an SRE agent monitoring live application logs.
The following errors were detected in a spike (>{ERROR_THRESHOLD*100}% error rate):

{json.dumps(errors, indent=2)}

Provide:
1. Root cause analysis (brief)
2. Suggested remediation steps
3. Severity assessment (P1/P2/P3)"""

    response = bedrock.invoke_model(
        modelId=MODEL_ID,
        contentType="application/json",
        accept="application/json",
        body=json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 512,
            "messages": [{"role": "user", "content": prompt}],
        }),
    )
    result = json.loads(response["body"].read())
    return result["content"][0]["text"]


async def run_agent():
    print(f"Bedrock Agent connecting to {WS_URL}")
    async with websockets.connect(WS_URL) as ws:
        print("Connected. Monitoring stream...")
        event_count = 0

        async for raw in ws:
            msg = json.loads(raw)
            if msg.get("type") != "event":
                continue

            event = msg["data"]
            event_count += 1
            is_error = event.get("level") == "ERROR"
            error_window.append(1 if is_error else 0)

            # Print event
            level = event.get("level", "INFO")
            symbol = "\u2757" if is_error else "\u2705"
            print(f"  {symbol} [{level}] {event.get('service')}: {event.get('message')}")

            # Check anomaly threshold
            if len(error_window) == 20:
                rate = sum(error_window) / 20
                if rate > ERROR_THRESHOLD:
                    print(f"\n\u26a0\ufe0f  ANOMALY DETECTED - Error rate: {rate:.0%}")

                    # Query server for full anomaly state
                    await ws.send(json.dumps({"action": "get_anomalies"}))
                    resp = json.loads(await ws.recv())
                    print(f"   Server anomaly state: {resp.get('data')}")

                    # Collect recent errors and send to Bedrock
                    recent_errors = [event]  # at minimum the triggering event
                    print("   Invoking Bedrock for analysis...")
                    try:
                        analysis = analyze_with_bedrock(recent_errors)
                        print(f"\n\U0001f916 Bedrock Analysis:\n{analysis}\n")
                    except Exception as e:
                        print(f"   Bedrock error: {e}")

                    error_window.clear()

            if event_count % 50 == 0:
                print(f"\n--- {event_count} events processed ---\n")


if __name__ == "__main__":
    asyncio.run(run_agent())
