import json
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db.database import create_schema
from app.db.tables import FraudEventInboxRow, FraudEventRejectionRow
from app.replay_fraud_rejections import replay_fraud_rejections


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


def valid_event():
    timestamp = datetime(2025, 1, 1, 7, 55, 15, tzinfo=timezone.utc).isoformat()
    return {
        "event_id": str(uuid4()),
        "event_type": "bank.transaction.created.v1",
        "event_version": 1,
        "occurred_at": timestamp,
        "transaction": {
            "transaction_id": "TXN-REPLAY-001",
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


def add_rejection(session_factory, offset, raw_value):
    with session_factory.begin() as session:
        session.add(FraudEventRejectionRow(
            rejection_id=str(uuid4()),
            source_topic="bank.transactions.v1",
            source_partition=0,
            source_offset=offset,
            raw_value=raw_value,
            error="historical validation failure",
            received_at=datetime.now(timezone.utc),
        ))


def test_replay_restores_valid_event_and_keeps_rejection_audit(session_factory):
    event = valid_event()
    add_rejection(session_factory, 10, json.dumps(event))
    add_rejection(session_factory, 11, "{malformed-json")

    result = replay_fraud_rejections(session_factory)

    assert result == {
        "total_rejections": 2,
        "stored": 1,
        "predicted_existing_inbox": 0,
        "duplicates": 0,
        "rejected_again": 1,
    }
    assert count_rows(session_factory, FraudEventInboxRow) == 1
    assert count_rows(session_factory, FraudEventRejectionRow) == 2


def test_replay_is_idempotent(session_factory):
    event = valid_event()
    add_rejection(session_factory, 20, json.dumps(event))

    first = replay_fraud_rejections(session_factory)
    second = replay_fraud_rejections(session_factory)

    assert first["stored"] == 1
    assert second["duplicates"] == 1
    assert count_rows(session_factory, FraudEventInboxRow) == 1
    assert count_rows(session_factory, FraudEventRejectionRow) == 1


def test_replay_uses_inference_for_existing_inbox_without_prediction(session_factory):
    event = valid_event()
    add_rejection(session_factory, 30, json.dumps(event))

    class RecordingInference:
        def __init__(self):
            self.calls = 0

        def predict_and_persist(self, session, parsed_event):
            self.calls += 1
            assert str(parsed_event.event_id) == event["event_id"]

    inference = RecordingInference()
    result = replay_fraud_rejections(
        session_factory,
        inference_service=inference,
    )

    assert result["stored"] == 1
    assert inference.calls == 1
