import json
import logging
from pathlib import Path

from confluent_kafka import Consumer, Producer, KafkaError
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

KAFKA_BOOTSTRAP = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8082"
TOPIC = "orders"
DLQ_TOPIC = "orders-dlq"
GROUP_ID = "order-consumers"
SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "order.avsc"

MAX_RETRIES = 3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

def load_schema(path: Path) -> str:
    with open(path, "r") as f:
        return f.read()


def dict_from_order(obj, ctx):
    """Called by AvroDeserializer — returns the dict as-is."""
    return obj


class RunningAverage:
    """Maintains a running average without storing all values."""

    def __init__(self):
        self.count = 0
        self.total = 0.0

    def update(self, value: float) -> float:
        self.count += 1
        self.total += value
        return self.average

    @property
    def average(self) -> float:
        if self.count == 0:
            return 0.0
        return round(self.total / self.count, 2)


def send_to_dlq(dlq_producer: Producer, original_msg_value: bytes, reason: str):
    """Publish a permanently failed message to the DLQ topic."""
    headers = [("dlq-reason", reason.encode("utf-8"))]
    dlq_producer.produce(
        topic=DLQ_TOPIC,
        value=original_msg_value,
        headers=headers,
    )
    dlq_producer.flush(timeout=5)
    logger.warning("Sent message to DLQ (%s): %s", DLQ_TOPIC, reason)


def process_order(order: dict, running_avg: RunningAverage) -> None:
    """
    Business logic: validate and aggregate.
    Raises ValueError for permanent failures, RuntimeError for transient ones.
    """
    # Permanent failure: bad data
    if order.get("price") is None or order["price"] < 0:
        raise ValueError(f"Invalid price in order {order.get('orderId')}")

    #  Transient failure simulation (for demo purposes) 
    # Uncomment the lines below to simulate random transient errors:
    # import random
    # if random.random() < 0.8:
    #     raise RuntimeError("Simulated transient failure")

    #  Happy path: aggregate 
    avg = running_avg.update(order["price"])
    logger.info(
        "Processed order=%s  product=%-10s price=%8.2f  |  running_avg=%.2f  (n=%d)",
        order["orderId"],
        order["product"],
        order["price"],
        avg,
        running_avg.count,
    )

def main():
    # Schema Registry + Avro deserializer
    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(
        schema_registry_client=sr_client,
        schema_str=load_schema(SCHEMA_PATH),
        from_dict=dict_from_order,
    )

    # Consumer
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # manual commit after successful processing
    })

    # DLQ producer (plain bytes — schema doesn't matter for the DLQ)
    dlq_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    consumer.subscribe([TOPIC])
    running_avg = RunningAverage()

    logger.info(
        "Consumer started — group=%s, topic=%s, DLQ=%s",
        GROUP_ID, TOPIC, DLQ_TOPIC,
    )

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            # Kafka-level errors (e.g. partition EOF)
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("Reached end of partition %s", msg.partition())
                else:
                    logger.error("Consumer error: %s", msg.error())
                continue

            # Deserialize
            try:
                order = avro_deserializer(
                    msg.value(),
                    SerializationContext(TOPIC, MessageField.VALUE),
                )
            except Exception as e:
                logger.error("Deserialization failed — sending to DLQ: %s", e)
                send_to_dlq(dlq_producer, msg.value(), f"Deserialization error: {e}")
                consumer.commit(message=msg)
                continue

            # Process with retry logic
            retries = 0
            success = False

            while retries <= MAX_RETRIES:
                try:
                    process_order(order, running_avg)
                    success = True
                    break

                except ValueError as e:
                    # Permanent failure — no point retrying
                    logger.error("Permanent failure: %s", e)
                    send_to_dlq(dlq_producer, msg.value(), str(e))
                    break

                except RuntimeError as e:
                    # Transient failure — retry
                    retries += 1
                    if retries <= MAX_RETRIES:
                        logger.warning(
                            "Transient error (attempt %d/%d): %s",
                            retries, MAX_RETRIES, e,
                        )
                    else:
                        logger.error(
                            "Max retries (%d) exceeded — sending to DLQ", MAX_RETRIES,
                        )
                        send_to_dlq(dlq_producer, msg.value(), f"Retries exhausted: {e}")

            # Commit offset only after processing (success or DLQ'd)
            consumer.commit(message=msg)

    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        consumer.close()
        logger.info("Consumer shut down cleanly.")


if __name__ == "__main__":
    main()