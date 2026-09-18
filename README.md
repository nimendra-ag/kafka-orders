# Kafka Order Processing System

A Kafka-based system that produces and consumes order messages using Avro serialization, with real-time price aggregation, retry logic, and a Dead Letter Queue (DLQ).

## Architecture

```
Producer ──(Avro)──▶ Kafka Topic: "orders" ──▶ Consumer
                                                  │
                                                  ├── success → running average
                                                  ├── transient fail → retry (max 3)
                                                  └── permanent fail → "orders-dlq"
```

## Prerequisites

- Docker Desktop
- Python 3.10+
- Git

## Setup

### 1. Clone and install dependencies

```bash
git clone https://github.com/nimendra-ag/kafka-orders
cd kafka-orders
python -m venv venv
source venv/bin/activate        # Linux/Mac
venv\Scripts\activate           # Windows
pip install -r requirements.txt
```

### 2. Start Kafka infrastructure

```bash
docker-compose up -d
```

```bash
curl http://localhost:8082/subjects
# Expected: []
```

### 3. Run the system

Open two separate terminals:

```bash
# Terminal 1 — Producer
python producer/producer.py

# Terminal 2 — Consumer
python consumer/consumer.py
```


## Features

**Avro Serialization** — Messages are serialized/deserialized via Confluent Schema Registry (auto-registered on first produce).

**Real-time Aggregation** — Consumer computes a running average of prices incrementally (count + total, no storage of all values).

**Retry Logic** — Transient failures (RuntimeError) are retried up to 3 times before routing to the DLQ.

**Dead Letter Queue** — Permanently failed messages (bad data or exhausted retries) are published to `orders-dlq` with a reason header.

## Testing the DLQ

Uncomment the transient failure simulation in `consumer/consumer.py` (inside `process_order`), restart the consumer, and verify:

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic orders-dlq \
  --from-beginning \
  --timeout-ms 10000
```

## Tech Stack

- **Kafka** — Confluent Platform 7.5.0 (Docker)
- **Schema Registry** — Confluent Schema Registry
- **Serialization** — Apache Avro
- **Language** — Python 3 (`confluent-kafka[avro]`)
