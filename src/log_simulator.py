"""Log Simulator - produces realistic app logs to Amazon MSK topic."""

import json
import os
import random
import time
from confluent_kafka import Producer
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

BOOTSTRAP_SERVERS = os.environ.get("MSK_BOOTSTRAP", "localhost:9098")
TOPIC = os.environ.get("MSK_TOPIC", "app-logs")
REGION = os.environ.get("AWS_REGION", "us-east-1")

SERVICES = ["auth-service", "api-gateway", "payment-service", "user-service", "order-service"]
LEVELS = ["INFO", "WARN", "ERROR"]
ERROR_TYPES = ["ConnectionRefused", "OutOfMemory", "Timeout", "Deadlock", "SSLHandshake", "DiskFull"]
NORMAL_MESSAGES = [
    "Request processed successfully",
    "Cache hit for user session",
    "Database query completed in 45ms",
    "Health check passed",
    "Connection pool refreshed",
]


def oauth_cb(config):
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(REGION)
    return auth_token, expiry_ms / 1000


def create_producer():
    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "OAUTHBEARER",
        "oauth_cb": oauth_cb,
    }
    return Producer(conf)


def generate_event(force_error=False):
    level = "ERROR" if force_error else random.choices(LEVELS, weights=[80, 15, 5])[0]
    service = random.choice(SERVICES)
    if level == "ERROR":
        msg = f"{random.choice(ERROR_TYPES)}: {service} failed"
    else:
        msg = random.choice(NORMAL_MESSAGES)
    return {
        "timestamp": time.time(),
        "service": service,
        "level": level,
        "message": msg,
    }


def main():
    producer = create_producer()
    event_count = 0
    print(f"Log Simulator producing to {BOOTSTRAP_SERVERS} topic={TOPIC}")

    while True:
        # Every ~30 seconds, inject an error spike (10 errors in a row)
        if event_count > 0 and event_count % 30 == 0:
            print(">>> Injecting error spike!")
            for _ in range(10):
                event = generate_event(force_error=True)
                producer.produce(TOPIC, json.dumps(event).encode("utf-8"))
            producer.flush()
        else:
            event = generate_event()
            producer.produce(TOPIC, json.dumps(event).encode("utf-8"))
            producer.flush()

        event_count += 1
        time.sleep(1)


if __name__ == "__main__":
    main()
