# Task 07 — Transactional Outbox Persistence

## 1. Objective

Persist a durable event record whenever the continuous banking engine commits a transfer. The transaction ledger, account balance changes, and outbox event must be part of one database transaction.

This task does **not** publish to Kafka. It establishes the reliable database-side handoff that a later publisher can read.

## 2. Why the outbox is needed

The banking ledger is the system of record. Later, fraud detection, AML monitoring, and data-lake ingestion need to receive each committed transaction as an event.

Publishing directly to Kafka after committing PostgreSQL creates a failure gap: the database commit may succeed while the Kafka publish fails. The transaction then exists in the ledger but downstream consumers never receive its event.

The outbox closes that gap by storing the event in PostgreSQL in the same transaction as the ledger write:

1. Update sender and receiver balances.
2. Insert the transaction ledger row.
3. Insert the corresponding outbox event.
4. Commit all three together.

If the transaction rolls back, all three database changes roll back. A later publisher will read pending outbox rows, publish them to Kafka, and mark them as published. That publisher is outside Task 07.

## 3. Files changed

| File | Purpose |
|---|---|
| `backend/app/db/tables.py` | Adds the SQLAlchemy mapping for `outbox_events`. |
| `backend/app/db/repository.py` | Writes the transaction and its event in the same session transaction. |
| `backend/tests/test_outbox.py` | Verifies event creation, payload safety, and rollback behavior. |

## 4. Outbox table design

| Column | Purpose |
|---|---|
| `event_id` | Unique event identifier (UUID string). |
| `aggregate_type` | Entity category; currently `transaction`. |
| `aggregate_id` | The related transaction ID. |
| `event_type` | Versioned event name: `bank.transaction.created.v1`. |
| `payload` | JSON event body for downstream consumers. |
| `created_at` | When the outbox record was created. |
| `published_at` | Null until a future publisher confirms publication. |
| `attempts` | Future publisher retry counter; starts at zero. |
| `last_error` | Future publisher diagnostic field. |

The outbox is not a second transaction ledger. The `transactions` table remains the authoritative financial record; the outbox tracks delivery intent and publication state.

## 5. Event contract and data safety

The event payload contains an event ID, event type/version, occurrence time, and transaction fields required by downstream processing.

It deliberately excludes `is_fraud`, `is_aml_suspicious`, `fraud_scenario`, and `aml_typology`. These are labels/scenario metadata and must not leak into model input events. The current continuous engine generates ordinary transfers; future labelled scenario events will need a separately reviewed event contract.

The amount is serialized as a decimal string to avoid JSON floating-point precision loss.

## 6. Transaction and delivery guarantees

- Database atomicity: account balance changes, transaction row, and outbox row commit or roll back together.
- Kafka delivery: not implemented in this task.
- Future publisher behavior: expected at-least-once delivery. A publisher can send an event and crash before recording `published_at`; it may send the same event again after restart.
- Therefore downstream consumers must be idempotent, using `event_id` or another durable deduplication key.
- This design does not claim end-to-end exactly-once processing.

## 7. Verification

Run from the repository root:

```powershell
python -m pytest
```

The new tests verify:
- one matching outbox event is created for a committed live transfer;
- event fields match the persisted transaction;
- model labels and scenario metadata are absent from the event payload;
- rolling back the session restores both account balances and removes both the transaction and outbox rows.

SQLite is used for automated unit tests. These tests verify SQLAlchemy transaction behavior but do not prove PostgreSQL concurrency/row-lock behavior. The earlier Task 06 local PostgreSQL checks remain separate evidence.

## 8. Database rollout

The new table is additive. The existing `create_schema()` function uses SQLAlchemy `create_all()`, which creates the missing `outbox_events` table without deleting the existing PostgreSQL volume or transaction rows.

Do not run `docker compose down -v`; that command would delete the named database volume.

## 9. Scope and limitations

- No Kafka broker, topic, producer, polling publisher, retry scheduler, or dead-letter queue is implemented yet.
- `published_at`, `attempts`, and `last_error` are stored for the next publisher task but are not updated in Task 07.
- The outbox has no cleanup/retention policy yet.
- No multi-publisher locking/claim mechanism is needed until the publisher is introduced; that task must prevent competing publishers from claiming the same rows concurrently.
- A successful unit test run is not evidence of production throughput or end-to-end Kafka delivery.

## 10. Completion criteria

Task 07 is complete when the user runs the full test suite locally and confirms the reported test result. Kafka integration should begin only after this database-side behavior is verified.


## 11. Implementation source excerpts

### 11.1 Outbox ORM model — `backend/app/db/tables.py`

```python
class OutboxEventRow(Base):
    """Durable event awaiting publication to downstream messaging infrastructure."""

    __tablename__ = "outbox_events"

    event_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    aggregate_type: Mapped[str] = mapped_column(String(50), nullable=False)
    aggregate_id: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
```

Purpose: one row represents one event that must eventually be delivered. A null `published_at` means it is still pending. The aggregate fields let future publisher/consumer code identify the business record represented by the event.

### 11.2 Atomic write — `backend/app/db/repository.py`

The transfer repository operation now adds both records before the caller commits:

```python
event_id = str(uuid.uuid4())
event_type = "bank.transaction.created.v1"
event_payload = {
    "event_id": event_id,
    "event_type": event_type,
    "event_version": 1,
    "occurred_at": timestamp.isoformat(),
    "transaction": {
        "transaction_id": transaction_id,
        "sender_account_id": sender_account_id,
        "receiver_account_id": receiver_account_id,
        "amount": str(transfer_amount),
        "currency": sender.currency,
        "transaction_type": "TRANSFER",
        "channel": channel,
        "status": "COMPLETED",
        "timestamp": timestamp.isoformat(),
    },
}

session.add(TransactionRow(
    # transaction ledger fields and balance snapshots
    transaction_id=transaction_id,
    sender_account_id=sender_account_id,
    receiver_account_id=receiver_account_id,
    amount=transfer_amount,
    currency=sender.currency,
    transaction_type="TRANSFER",
    channel=channel,
    status="COMPLETED",
    timestamp=timestamp,
    sender_balance_before=sender_before,
    sender_balance_after=sender.balance,
    receiver_balance_before=receiver_before,
    receiver_balance_after=receiver.balance,
    is_fraud=False,
    is_aml_suspicious=False,
    fraud_scenario=None,
    aml_typology=None,
))
session.add(OutboxEventRow(
    event_id=event_id,
    aggregate_type="transaction",
    aggregate_id=transaction_id,
    event_type=event_type,
    payload=event_payload,
    created_at=datetime.now(timezone.utc),
    published_at=None,
    attempts=0,
    last_error=None,
))
session.flush()
```

The live engine then calls `session.commit()`. The repository does not commit independently; this keeps transaction ownership with the caller and lets both inserts and the balance updates share one commit/rollback boundary.

### 11.3 Test coverage — `backend/tests/test_outbox.py`

The test file contains three focused tests:
1. `test_live_transfer_creates_matching_outbox_event` — checks a committed transfer has exactly one matching event and validates core payload fields.
2. `test_outbox_payload_does_not_contain_training_labels_or_scenario_metadata` — guards against sending labels/scenario fields into downstream model input.
3. `test_rollback_reverts_balances_transaction_and_outbox_together` — explicitly rolls back the SQLAlchemy session and verifies both rows disappear and both balances return to their original values.

The existing Task 06 tests remain in place for worker configuration and lifecycle behavior.
