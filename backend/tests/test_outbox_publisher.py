from datetime import datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db.database import create_schema
from app.db.repository import save_registry
from app.db.tables import OutboxEventRow
from app.services.outbox_publisher import publish_pending_events
from app.services.registry import build_registry


@pytest.fixture
def session_factory():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    create_schema(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    with factory() as session:
        save_registry(session, build_registry(customer_count=10, seed=17))
        session.commit()
    yield factory
    engine.dispose()


class FakeDelivery:
    def __init__(self, error=None):
        self.error = error

    def get(self, timeout):
        if self.error:
            raise self.error
        return object()


class FakeProducer:
    def __init__(self, error=None):
        self.error = error
        self.messages = []

    def send(self, topic, *, key, value):
        self.messages.append({"topic": topic, "key": key, "value": value})
        return FakeDelivery(self.error)


def insert_event(session_factory, *, aggregate_id="TXN-PUBLISH-001"):
    event_id = str(uuid4())
    payload = {
        "event_id": event_id,
        "event_type": "bank.transaction.created.v1",
        "event_version": 1,
        "transaction": {"transaction_id": aggregate_id, "amount": "12.50"},
    }
    with session_factory.begin() as session:
        session.add(
            OutboxEventRow(
                event_id=event_id,
                aggregate_type="transaction",
                aggregate_id=aggregate_id,
                event_type="bank.transaction.created.v1",
                payload=payload,
                created_at=datetime.now(timezone.utc),
                published_at=None,
                attempts=0,
                last_error=None,
            )
        )
    return event_id, payload


def test_publisher_marks_event_only_after_broker_acknowledgement(session_factory):
    event_id, payload = insert_event(session_factory)
    producer = FakeProducer()

    stats = publish_pending_events(session_factory, producer)

    assert stats == {"selected": 1, "published": 1, "failed": 0}
    assert producer.messages == [{
        "topic": "bank.transactions.v1",
        "key": "TXN-PUBLISH-001",
        "value": payload,
    }]
    with session_factory() as session:
        event = session.get(OutboxEventRow, event_id)
        assert event.published_at is not None
        assert event.attempts == 0
        assert event.last_error is None


def test_failed_publish_remains_pending_and_records_retry_state(session_factory):
    event_id, _ = insert_event(session_factory)
    producer = FakeProducer(error=TimeoutError("broker acknowledgement timed out"))

    stats = publish_pending_events(session_factory, producer)

    assert stats == {"selected": 1, "published": 0, "failed": 1}
    with session_factory() as session:
        event = session.get(OutboxEventRow, event_id)
        assert event.published_at is None
        assert event.attempts == 1
        assert "timed out" in event.last_error


def test_pending_event_can_be_retried_after_failure(session_factory):
    event_id, _ = insert_event(session_factory)
    failed = publish_pending_events(
        session_factory,
        FakeProducer(error=ConnectionError("Kafka unavailable")),
    )
    succeeded = publish_pending_events(session_factory, FakeProducer())

    assert failed["failed"] == 1
    assert succeeded["published"] == 1
    with session_factory() as session:
        event = session.get(OutboxEventRow, event_id)
        assert event.published_at is not None
        assert event.attempts == 1
        assert event.last_error is None


def test_batch_size_limits_selected_events(session_factory):
    insert_event(session_factory, aggregate_id="TXN-PUBLISH-001")
    insert_event(session_factory, aggregate_id="TXN-PUBLISH-002")
    producer = FakeProducer()

    stats = publish_pending_events(session_factory, producer, batch_size=1)

    assert stats == {"selected": 1, "published": 1, "failed": 0}
    assert len(producer.messages) == 1


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"batch_size": 0}, "batch_size"),
        ({"acknowledgement_timeout_seconds": 0}, "timeout"),
    ],
)
def test_invalid_publisher_settings_are_rejected(session_factory, kwargs, message):
    with pytest.raises(ValueError, match=message):
        publish_pending_events(session_factory, FakeProducer(), **kwargs)
