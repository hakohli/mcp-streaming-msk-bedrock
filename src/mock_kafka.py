"""Local dev override - patches server and simulator to use local Kafka (no IAM auth)."""

import os

# Set these before importing the main modules
os.environ.setdefault("MSK_BOOTSTRAP", "localhost:9092")

# Monkey-patch to disable SASL for local dev
_LOCAL_PRODUCER_CONF = {
    "bootstrap.servers": "localhost:9092",
}
_LOCAL_CONSUMER_CONF = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "mcp-streaming-server",
    "auto.offset.reset": "latest",
}


def patch_for_local():
    """Call this to override MSK IAM auth for local Docker Kafka."""
    import src.streaming_mcp_server as server
    import src.log_simulator as sim
    from confluent_kafka import Consumer, Producer

    def local_consumer():
        c = Consumer(_LOCAL_CONSUMER_CONF)
        c.subscribe([os.environ.get("MSK_TOPIC", "app-logs")])
        return c

    def local_producer():
        return Producer(_LOCAL_PRODUCER_CONF)

    server.create_consumer = local_consumer
    sim.create_producer = local_producer


if __name__ == "__main__":
    patch_for_local()
    print("Patched for local Kafka. Import and run server/simulator normally.")
