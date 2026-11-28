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

## 8. Current implementation status

This document records the approved implementation plan. Code, tests and runtime results will be added as the task progresses. Do not treat this design document as proof that the publisher is implemented or running.
