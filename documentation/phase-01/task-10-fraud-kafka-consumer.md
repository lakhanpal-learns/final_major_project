# Task 10 — Independent Fraud Kafka Event Consumer

## 1. Objective

Implement the first independent consumer of `bank.transactions.v1`. It validates the versioned transaction event and durably records it in a fraud-specific inbox in PostgreSQL.

This task is event ingestion only. It does not train a model, perform fraud inference, generate predictions, or create fraud alerts. Those belong to later fraud-pipeline tasks.

## 2. Consumer architecture

The fraud consumer is a separate Python process and uses its own Kafka consumer group:

- Topic: `bank.transactions.v1`
- Group: `fraud-detector-v1`
- Bootstrap address for a host-run process: `localhost:29092`
- Auto offset commit: disabled
- Initial offset policy for a new group: `earliest`
- Maximum records returned per poll: 1

The future AML and dashboard consumers will use different group IDs. They will independently receive the same topic events; they will not compete with the fraud consumer for records.

## 3. Processing sequence

For each Kafka record:

1. Read the raw message value.
2. Parse JSON and validate the strict `bank.transaction.created.v1` version-1 schema.
3. Reject unknown fields, including fraud/AML labels and scenario metadata.
4. For a valid event, insert it into `fraud_event_inbox`, keyed by `event_id`.
5. If the same event ID is replayed with the same transaction identity, treat it as a duplicate without inserting another row.
6. If the event is malformed, persist its source topic/partition/offset, raw value (bounded to 1 MB), and validation error in `fraud_event_rejections`.
7. Commit only the explicit Kafka offset for that record, and only after the database transaction has completed.

Database failures propagate and stop the process without committing the current offset. After restart, Kafka can redeliver that event. If the database insert committed but the Kafka offset commit failed, the event is safely recognized as a duplicate on replay.

Malformed messages are durably quarantined in PostgreSQL before their offsets are committed. This prevents one invalid record from blocking its partition indefinitely. A dedicated Kafka dead-letter topic and operator replay workflow are not part of this task.

## 4. Database tables

### `fraud_event_inbox`

| Column | Purpose |
|---|---|
| `event_id` | Primary key and deduplication identity |
| `transaction_id` | Canonical transaction reference |
| `event_type` | Versioned event type |
| `event_version` | Event schema version |
| `payload` | Validated original JSON event |
| `received_at` | Time this consumer persisted the event |

### `fraud_event_rejections`

| Column | Purpose |
|---|---|
| `rejection_id` | Quarantine record ID |
| `source_topic`, `source_partition`, `source_offset` | Unique Kafka source position |
| `raw_value` | Bounded raw message content |
| `error` | Parsing or schema validation failure |
| `received_at` | Quarantine persistence time |

The inbox is a processing record, not a second banking ledger. It does not copy account balances or mutate canonical transaction rows.

## 5. Files added or changed

| File | Purpose |
|---|---|
| `backend/app/models/events.py` | Strict Pydantic v2 transaction event contract |
| `backend/app/db/tables.py` | Fraud inbox and durable rejection table mappings |
| `backend/app/services/fraud_event_consumer.py` | Kafka consumer setup, validation, idempotent persistence, quarantine, and manual offset commit |
| `backend/app/consume_fraud.py` | Standalone process entry point and graceful shutdown |
| `backend/tests/test_fraud_event_consumer.py` | Tests for valid events, duplicates, malformed/label-contaminated events, quarantine, and offset ordering |
| `backend/.env.example` | Fraud consumer group and poll timeout settings |
| `backend/README.md` | Local run instructions |

## 6. Create the new database tables

The existing PostgreSQL volume must be preserved. Do not run `docker compose down -v`.

From the `backend` directory, after PostgreSQL is healthy:

```powershell
python -c "from app.db.database import create_db_engine, create_schema; engine = create_db_engine(); create_schema(engine); engine.dispose(); print('Schema updated successfully')"
```

The existing `create_schema()` helper uses SQLAlchemy `create_all()`, which creates missing tables without dropping existing tables or rows. It is not a general-purpose migration system.

## 7. Run locally (PowerShell)

Ensure Docker PostgreSQL and Kafka are healthy. Start the outbox publisher in one terminal if you want newly generated live transactions to be published.

In another terminal:

```powershell
cd backend
python -m pip install -r requirements-dev.txt
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
$env:KAFKA_TRANSACTION_TOPIC = "bank.transactions.v1"
$env:KAFKA_FRAUD_CONSUMER_GROUP_ID = "fraud-detector-v1"
python -m app.consume_fraud
```

Stop with Ctrl+C. The new consumer group starts at the earliest retained event. Therefore, it can ingest previously published events still retained in the topic as well as new events.

## 8. Test plan

Run from the project root:

```powershell
python -m pytest
```

Focused tests cover:
- Valid v1 event persisted in the fraud inbox.
- Same event ID and identical payload replayed without a duplicate row.
- Reused event ID with a different payload quarantined as an identity conflict.
- Malformed JSON and invalid UTF-8 durably quarantined.
- Fraud and AML labels rejected by the strict event contract.
- Repeated quarantine of the same Kafka source position does not create duplicate rejection rows.
- Kafka offset commit happens after the inbox database transaction is visible.
- Database failure leaves the Kafka offset uncommitted.

SQLite tests validate persistence and consumer control flow. They do not prove PostgreSQL concurrency behavior. The following local checks were also completed on 2026-11-28:

- Full test suite: user-reported result of **50 passed**.
- PostgreSQL schema creation: `Schema updated successfully`.
- Fraud consumer connected to the local Kafka broker, joined `fraud-detector-v1`, received all three topic partition assignments, and logged `Fraud event ingestion: stored`.
- PostgreSQL query confirmed the inbox row for event `22290321-491b-4a52-9849-5765905276d8`, referencing transaction `e5139fba38fc4f0dbfbc2e0a3c958a3d`, with event type `bank.transaction.created.v1`.

This verifies one successful local Kafka-to-PostgreSQL ingestion path. It does not prove concurrency behavior, crash recovery, duplicate delivery under live broker failures, throughput, or production reliability.

## 9. Scope boundaries and known limitations

Included:
- Independent fraud consumer group.
- Strict event contract validation.
- Durable PostgreSQL inbox.
- Event-ID deduplication.
- Durable malformed-event quarantine.
- Manual per-record offset commits after database persistence.
- Standalone process and graceful shutdown.

Not included:
- Fraud feature engineering or model inference.
- Fraud prediction or alert tables.
- Kafka dead-letter topic, replay tooling, or quarantine UI.
- Consumer lag metrics, retry backoff, health endpoint, or production monitoring.
- Production TLS/authentication or multi-broker high availability.
- Compose-managed consumer service.

The design is at-least-once at the Kafka delivery boundary with idempotent inbox persistence. It does not claim exactly-once processing.
