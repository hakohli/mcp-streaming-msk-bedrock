"""Lambda handler for Bedrock Agent Action Group - reads from MSK and returns anomaly/context data."""

import json
import os
from collections import deque
from confluent_kafka import Consumer
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

BOOTSTRAP_SERVERS = os.environ.get("MSK_BOOTSTRAP")
TOPIC = os.environ.get("MSK_TOPIC", "app-logs")
REGION = os.environ.get("AWS_REGION", "us-east-1")


def oauth_cb(config):
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(REGION)
    return auth_token, expiry_ms / 1000


def get_recent_events(max_events=100):
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "bedrock-agent-lambda",
        "auto.offset.reset": "latest",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "OAUTHBEARER",
        "oauth_cb": oauth_cb,
    })
    consumer.subscribe([TOPIC])
    events = deque(maxlen=max_events)
    # Poll briefly to get recent messages
    for _ in range(max_events):
        msg = consumer.poll(0.5)
        if msg is None or msg.error():
            break
        events.append(json.loads(msg.value().decode("utf-8")))
    consumer.close()
    return list(events)


def lambda_handler(event, context):
    """Bedrock Agent action group handler. Supports get_anomalies and get_context actions."""
    action = event.get("actionGroup", "")
    api_path = event.get("apiPath", "")
    
    if api_path == "/get_anomalies":
        events = get_recent_events()
        error_count = sum(1 for e in events if e.get("level") == "ERROR")
        total = len(events)
        rate = error_count / max(total, 1)
        body = {
            "error_rate": round(rate, 3),
            "error_count": error_count,
            "window_size": total,
            "is_anomaly": rate > 0.3,
            "recent_errors": [e for e in events if e.get("level") == "ERROR"][-5:],
        }
    elif api_path == "/get_context":
        body = {
            "topic": TOPIC,
            "bootstrap": BOOTSTRAP_SERVERS,
            "status": "running",
        }
    else:
        body = {"error": f"Unknown action: {api_path}"}

    return {
        "messageVersion": "1.0",
        "response": {
            "actionGroup": action,
            "apiPath": api_path,
            "httpMethod": "GET",
            "httpStatusCode": 200,
            "responseBody": {"application/json": {"body": json.dumps(body)}},
        },
    }
