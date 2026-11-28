import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db.database import create_schema
from app.db.tables import FraudEventInboxRow, FraudEventRejectionRow
from app.services.fraud_event_consumer import (
    process_fraud_message,
    run_fraud_consumer,
)


@pytest.fixture
def session_factory():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    create_schema(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    yield factory
    engine.dispose()


def valid_payload(event_id=None):
    timestamp = datetime(2026, 11, 28, 6, 18, 39, tzinfo=timezone.utc).isoformat()
    return {
        "event_id": event_id or str(uuid4()),
        "event_type": "bank.transaction.created.v1",
        "event_version": 1,
        "occurred_at": timestamp,
        "transaction": {
            "transaction_id": "TXN-FRAUD-001",
            "sender_account_id": "ACC00000001",
            "receiver_account_id": "ACC00000002",
            "amount": "125.50",
            "currency": "INR",
            "transaction_type": "TRANSFER",
            "channel": "UPI",
            "status": "COMPLETED",
            "timestamp": timestamp,
        },
    }


def count_rows(session_factory, model):
    with session_factory() as session:
        return session.scalar(select(func.count()).select_from(model))


def test_valid_event_is_durably_saved_to_fraud_inbox(session_factory):
    payload = valid_payload()

    result = process_fraud_message(
        session_factory,
        topic="bank.transactions.v1",
        partition=0,
        offset=12,
        value=json.dumps(payload).encode(),
    )

    assert result == "stored"
    assert count_rows(session_factory, FraudEventInboxRow) == 1
    with session_factory() as session:
        row = session.get(FraudEventInboxRow, payload["event_id"])
        assert row.transaction_id == "TXN-FRAUD-001"
        assert row.event_type == "bank.transaction.created.v1"
        assert row.event_version == 1
        assert row.payload == payload


def test_replayed_event_id_does_not_create_duplicate_inbox_row(session_factory):
    payload = valid_payload()
    raw = json.dumps(payload).encode()

    first = process_fraud_message(
        session_factory, topic="bank.transactions.v1",
        partition=1, offset=20, value=raw,
    )
    replay = process_fraud_message(
        session_factory, topic="bank.transactions.v1",
        partition=1, offset=20, value=raw,
    )

    assert first == "stored"
    assert replay == "duplicate"
    assert count_rows(session_factory, FraudEventInboxRow) == 1


@pytest.mark.parametrize(
    "raw",
    [
        b"{not-json",
        b"not valid utf-8: \xff",
        json.dumps({**valid_payload(), "is_fraud": True}).encode(),
        json.dumps({
            **valid_payload(),
            "transaction": {
                **valid_payload()["transaction"],
                "is_aml_suspicious": True,
            },
        }).encode(),
    ],
)
def test_invalid_or_label_contaminated_event_is_quarantined(session_factory, raw):
    result = process_fraud_message(
        session_factory,
        topic="bank.transactions.v1",
        partition=2,
        offset=44,
        value=raw,
    )

    assert result == "rejected"
    assert count_rows(session_factory, FraudEventInboxRow) == 0
    assert count_rows(session_factory, FraudEventRejectionRow) == 1


def test_replayed_invalid_source_position_does_not_duplicate_rejection(session_factory):
    raw = b"{bad-json"

    first = process_fraud_message(
        session_factory, topic="bank.transactions.v1",
        partition=0, offset=5, value=raw,
    )
    replay = process_fraud_message(
        session_factory, topic="bank.transactions.v1",
        partition=0, offset=5, value=raw,
    )

    assert first == "rejected"
    assert replay == "duplicate_rejection"
    assert count_rows(session_factory, FraudEventRejectionRow) == 1


class FakeMessage:
    topic = "bank.transactions.v1"
    partition = 2
    offset = 9

    def __init__(self, value):
        self.value = value


class FakeConsumer:
    def __init__(self, message, stop_event, session_factory):
        self.message = message
        self.stop_event = stop_event
        self.session_factory = session_factory
        self.committed = None
        self.closed = False

    def poll(self, timeout_ms, max_records):
        assert max_records == 1
        self.stop_event.set()
        from kafka.structs import TopicPartition
        return {TopicPartition(self.message.topic, self.message.partition): [self.message]}

    def commit(self, offsets):
        assert count_rows(self.session_factory, FraudEventInboxRow) == 1
        self.committed = offsets

    def close(self):
        self.closed = True


def test_consumer_commits_explicit_offset_only_after_inbox_commit(session_factory):
    import threading
    from kafka.structs import TopicPartition

    payload = valid_payload()
    stop_event = threading.Event()
    consumer = FakeConsumer(
        FakeMessage(json.dumps(payload).encode()),
        stop_event,
        session_factory,
    )

    run_fraud_consumer(consumer, session_factory, stop_event=stop_event)

    tp = TopicPartition("bank.transactions.v1", 2)
    assert consumer.committed[tp].offset == 10



def test_reused_event_id_with_different_payload_is_quarantined(session_factory):
    payload = valid_payload()
    process_fraud_message(
        session_factory, topic="bank.transactions.v1",
        partition=0, offset=30, value=json.dumps(payload).encode(),
    )
    conflicting = {**payload, "transaction": {**payload["transaction"], "amount": "999.00"}}

    result = process_fraud_message(
        session_factory, topic="bank.transactions.v1",
        partition=0, offset=31, value=json.dumps(conflicting).encode(),
    )

    assert result == "rejected"
    assert count_rows(session_factory, FraudEventInboxRow) == 1
    assert count_rows(session_factory, FraudEventRejectionRow) == 1


def test_database_failure_does_not_commit_kafka_offset():
    import threading

    class FailingSessionFactory:
        def begin(self):
            raise RuntimeError("database unavailable")

    payload = valid_payload()
    stop_event = threading.Event()
    consumer = FakeConsumer(
        FakeMessage(json.dumps(payload).encode()),
        stop_event,
        session_factory=None,
    )

    with pytest.raises(RuntimeError, match="database unavailable"):
        run_fraud_consumer(
            consumer,
            FailingSessionFactory(),
            stop_event=stop_event,
        )

    assert consumer.committed is None
