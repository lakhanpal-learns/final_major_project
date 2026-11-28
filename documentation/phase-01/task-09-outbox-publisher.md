# Task 09 — PostgreSQL Outbox Publisher to Kafka

## 1. Objective

Implement the first application component that publishes committed transaction events from PostgreSQL's `outbox_events` table to the Kafka topic `bank.transactions.v1`.

The publisher is a separate background process. It is not part of the FastAPI request handler and does not change the live banking engine's database transaction.

## 2. Why this component is needed

Task 07 made the ledger transaction and outbox insert atomic. Task 08 provisioned Kafka and the versioned transaction topic. These pieces are not connected until a publisher reads pending outbox rows and sends them to Kafka.

The intended path is:

```text
Live banking engine
  -> PostgreSQL transaction + outbox row (one DB transaction)
  -> Outbox publisher
  -> Kafka bank.transactions.v1
  -> Future independent fraud, AML and data-lake consumer groups
```

Kafka is not the ledger. PostgreSQL remains the source of truth.

## 3. Publication contract

For each pending outbox event:

1. Select rows where `published_at IS NULL`, ordered by creation time and event ID.
2. Lock selected rows using PostgreSQL row-level locking with `SKIP LOCKED`, so concurrent publisher processes do not select the same currently locked rows.
3. Publish the JSON payload to `bank.transactions.v1`.
4. Use the transaction aggregate ID as the Kafka message key, so events for the same transaction use the same partition.
5. Wait for Kafka's broker acknowledgement.
6. Set `published_at` only after the acknowledgement succeeds.
7. On a publication error, increment `attempts`, store a bounded `last_error`, and leave `published_at` null for retry.

The initial implementation may hold the database transaction and row locks while waiting for broker acknowledgements. This is intentionally simple for the local project and short batches; it increases lock duration and is not a high-throughput production design.

## 4. Delivery semantics and failure window

This design provides **at-least-once publication**, not exactly-once delivery across PostgreSQL and Kafka.

If Kafka acknowledges a message and the publisher process crashes before PostgreSQL commits `published_at`, the same event can be published again after restart. Consumers must therefore be idempotent, using `event_id` as the deduplication key.

A successful Kafka acknowledgement followed by a successful database commit is the only condition under which an event is marked published.

## 5. Initial scope

Included:
- Configurable Kafka bootstrap address and topic.
- Bounded batch polling.
- Broker acknowledgement before marking an event published.
- Retryable failure recording.
- A stoppable polling loop.
- Unit tests with a fake producer.
- Local integration verification against Docker Kafka and PostgreSQL.

Not included yet:
- Fraud, AML or data-lake consumers.
- Dead-letter topic or permanent-failure policy.
- Distributed leases or a separate claim table.
- Exactly-once claims.
- Production authentication, TLS, monitoring or high availability.
- Automated deployment as a Compose worker service.

## 6. Configuration

Initial local defaults:
- Kafka bootstrap server: `localhost:29092` for a publisher running on the Windows host; `kafka:9092` when run inside the Compose network.
- Topic: `bank.transactions.v1`.
- Batch size: 100 events.
- Poll interval: 1 second.
- Acknowledgement timeout: 10 seconds.

The implementation must allow configuration overrides and must not silently assume the host address when running inside a container.

## 7. Verification requirements

The task is not complete until tests and local integration checks demonstrate:

1. A pending outbox event is sent to the configured topic.
2. The Kafka message key is the transaction aggregate ID.
3. The event is marked published only after the producer confirms broker acknowledgement.
4. A send failure leaves the event pending, increments attempts and records an error.
5. A later successful retry marks the event published.
6. The message payload retains the existing event contract and contains no fraud/AML training labels.
7. A real event created in PostgreSQL can be consumed from the local Kafka topic.

SQLite-based unit tests do not prove PostgreSQL row-lock behavior. Local Docker integration is required for that boundary.



## 8. Initial implementation added (2026-11-28)

The implementation is committed and has passed the user's local automated and PostgreSQL/Kafka integration checks described in Section 9.

### Files added or changed

| File | Purpose |
|---|---|
| `backend/app/services/outbox_publisher.py` | Creates a Kafka producer, selects a bounded batch of pending outbox rows, publishes each event, waits for its acknowledgement, records success/failure, and provides a stoppable polling loop. |
| `backend/app/publish_outbox.py` | Standalone process entry point; creates the database session factory and Kafka producer and handles Ctrl+C/SIGTERM shutdown. |
| `backend/tests/test_outbox_publisher.py` | Fake-producer tests for acknowledgement, failure recording, retry, batch limits and invalid settings. |
| `backend/requirements.txt` | Adds `kafka-python` as the Kafka client dependency. |
| `backend/.env.example` | Documents host bootstrap address, topic, batch size, polling interval and acknowledgement timeout. |

### Important implementation details

- The publisher uses `SELECT ... FOR UPDATE SKIP LOCKED` and holds the database transaction open while waiting for each broker acknowledgement. This is a simple initial concurrency strategy, but it holds PostgreSQL row locks during network I/O and is not intended as a high-throughput production design.
- Kafka message key is the transaction aggregate ID; the JSON value is the existing outbox payload.
- Producer acknowledgement mode is `acks=all`. The event is marked published only after the send future returns successfully.
- A send failure increments `attempts`, stores an error message and leaves `published_at` null. The next poll can retry it.
- A crash after Kafka acknowledgement but before the PostgreSQL commit can still produce a duplicate. Delivery remains at-least-once; downstream consumers must deduplicate by event ID.
- The unit tests use SQLite. SQLite does not prove PostgreSQL's row-lock and `SKIP LOCKED` behavior.

### Run locally (PowerShell)

From the backend directory, after pulling the commits:

```powershell
python -m pip install -r requirements-dev.txt
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
$env:KAFKA_TRANSACTION_TOPIC = "bank.transactions.v1"
python -m app.publish_outbox
```

Stop the publisher with Ctrl+C. Run the API and the publisher in separate terminals. Do not start multiple publisher processes until the PostgreSQL integration check is complete.

### Verification status

The implementation and the local end-to-end path have been verified as described below. See the evidence boundaries in Section 9; these checks do not establish production reliability or throughput.


## 9. Local verification results (2026-11-28)

The user ran the following from the project root after pulling the implementation:

```powershell
python -m pytest
```

Reported result: **40 passed**.

The publisher was then started from the `backend` directory with:

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
$env:KAFKA_TRANSACTION_TOPIC = "bank.transactions.v1"
python -m app.publish_outbox
```

The publisher log included:

```text
Outbox batch: {'selected': 1, 'published': 1, 'failed': 0}
```

A direct PostgreSQL query of the latest outbox row returned:

```text
event_id:     22290321-491b-4a52-9849-5765905276d8
aggregate_id: e5139fba38fc4f0dbfbc2e0a3c958a3d
published_at: 2026-11-28 06:59:51.172009+00:00
attempts:     0
last_error:   None
```

The user then read the Kafka topic using the console consumer:

```powershell
docker exec sentinel-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bank.transactions.v1 --from-beginning --max-messages 1
```

Kafka returned one message with the same event ID and transaction ID:

```json
{
  "event_id": "22290321-491b-4a52-9849-5765905276d8",
  "event_type": "bank.transaction.created.v1",
  "event_version": 1,
  "transaction": {
    "transaction_id": "e5139fba38fc4f0dbfbc2e0a3c958a3d",
    "sender_account_id": "ACC00000001",
    "receiver_account_id": "ACC00000002",
    "amount": "1.00",
    "currency": "INR",
    "transaction_type": "TRANSFER",
    "channel": "UPI",
    "status": "COMPLETED"
  }
}
```

### What this proves

- The reported automated suite passed: 40 tests.
- The publisher connected to the local Kafka broker and received a successful acknowledgement for one pending event.
- The corresponding PostgreSQL outbox row was marked published, with zero attempts and no recorded error.
- A Kafka console consumer independently read the event from `bank.transactions.v1`.
- The matching event and transaction IDs establish that the observed Kafka message corresponds to the inspected PostgreSQL outbox row.

This is a successful **single-event local end-to-end integration check** of PostgreSQL outbox → publisher → Kafka → consumer read.

### Evidence boundaries

This test does not prove:
- crash recovery in the acknowledgement-before-database-commit failure window;
- duplicate delivery and downstream idempotency;
- PostgreSQL `SKIP LOCKED` behavior under concurrent publisher processes;
- sustained throughput, latency, or load capacity;
- production security, high availability, or operational reliability.

The Kafka console consumer check proves that the event was readable from the topic; it is not a test of the future fraud, AML, or data-lake consumer implementations.
