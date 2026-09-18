import json
import random
import time
import logging
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
    StringSerializer,
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


KAFKA_BOOTSTRAP = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8082"
TOPIC = "orders"
SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "order.avsc"

PRODUCTS = ["Laptop", "Phone", "Tablet", "Monitor", "Keyboard", "Mouse", "Headset"]
PRICE_RANGE = (10.0, 1500.0)
PRODUCE_INTERVAL_SEC = 1  # seconds between messages

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

def load_schema(path: Path) -> str:
    """Read the .avsc file and return its JSON string."""
    with open(path, "r") as f:
        return f.read()


def order_to_dict(order, ctx):
    """Called by AvroSerializer to convert an order dict to a serializable dict."""
    return order


def delivery_callback(err, msg):
    """Called once per produced message to report success/failure."""
    if err is not None:
        logger.error("DELIVERY FAILED for %s: %s", msg.key(), err)
    else:
        logger.info(
            "Delivered -> topic=%s partition=%s offset=%s key=%s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
            msg.key(),
        )

def main():
    # Schema Registry client
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    # Avro serializer — auto-registers schema on first produce
    avro_serializer = AvroSerializer(
        schema_registry_client=sr_client,
        schema_str=load_schema(SCHEMA_PATH),
        to_dict=order_to_dict,
    )

    key_serializer = StringSerializer("utf_8")

    # Kafka producer
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "all",             # wait for all replicas (durability)
        "retries": 5,              # built-in retries for transient broker errors
        "retry.backoff.ms": 300,
    })

    logger.info("Producer started — sending to topic '%s'", TOPIC)

    order_counter = 1000
    try:
        while True:
            order_counter += 1
            order = {
                "orderId": str(order_counter),
                "product": random.choice(PRODUCTS),
                "price": round(random.uniform(*PRICE_RANGE), 2),
            }

            producer.produce(
                topic=TOPIC,
                key=key_serializer(order["orderId"]),
                value=avro_serializer(
                    order,
                    SerializationContext(TOPIC, MessageField.VALUE),
                ),
                on_delivery=delivery_callback,
            )

            # Trigger delivery callbacks
            producer.poll(0)

            logger.info("Produced: %s", order)
            time.sleep(PRODUCE_INTERVAL_SEC)

    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
    finally:
        # Block until all messages are delivered
        producer.flush(timeout=10)
        logger.info("Producer shut down cleanly.")


if __name__ == "__main__":
    main()