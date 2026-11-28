"""Idempotent durable ingestion for the independent fraud Kafka consumer."""

from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from typing import Any, Callable
from uuid import uuid4

from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata, TopicPartition
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from app.db.tables import FraudEventInboxRow, FraudEventRejectionRow, FraudPredictionRow
from app.models.events import TransactionCreatedEvent
from app.services.fraud_inference_service import FraudInferenceService

DEFAULT_TOPIC = "bank.transactions.v1"
DEFAULT_GROUP_ID = "fraud-detector-v1"
MAX_STORED_RAW_VALUE_CHARS = 1_000_000


def create_fraud_kafka_consumer(
    bootstrap_servers: str | None = None,
    *,
    topic: str = DEFAULT_TOPIC,
    group_id: str = DEFAULT_GROUP_ID,
) -> KafkaConsumer:
    """Create a manually-committed consumer; values stay raw until validation."""
    servers = bootstrap_servers or os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"
    )
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=servers.split(","),
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=1,
    )
    return consumer


def _raw_text(value: bytes | str | Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, str):
        return value
    raise ValueError("Kafka message value must be UTF-8 JSON bytes or text")


def _store_rejection(
    session_factory: sessionmaker,
    *,
    topic: str,
    partition: int,
    offset: int,
    raw_value: str,
    error: str,
) -> str:
    """Persist a malformed event before its Kafka offset can be acknowledged."""
    with session_factory.begin() as session:
        existing = session.scalar(
            select(FraudEventRejectionRow).where(
                FraudEventRejectionRow.source_topic == topic,
                FraudEventRejectionRow.source_partition == partition,
                FraudEventRejectionRow.source_offset == offset,
            )
        )
        if existing is not None:
            return "duplicate_rejection"
        session.add(
            FraudEventRejectionRow(
                rejection_id=str(uuid4()),
                source_topic=topic,
                source_partition=partition,
                source_offset=offset,
                raw_value=raw_value[:MAX_STORED_RAW_VALUE_CHARS],
                error=error[:4000],
                received_at=datetime.now(timezone.utc),
            )
        )
    return "rejected"


def process_fraud_message(
    session_factory: sessionmaker,
    *,
    topic: str,
    partition: int,
    offset: int,
    value: bytes | str,
    inference_service: FraudInferenceService | None = None,
) -> str:
    """Validate, persist and optionally score one event; return stored/duplicate/rejected.

    Database exceptions intentionally propagate. The caller must not commit the
    Kafka offset unless this function returns after its database transaction.
    """
    try:
        raw_value = _raw_text(value)
        payload = json.loads(raw_value)
        event = TransactionCreatedEvent.model_validate(payload)
    except (UnicodeDecodeError, json.JSONDecodeError, ValidationError, ValueError) as exc:
        try:
            raw_for_storage = value.decode("utf-8", errors="replace") if isinstance(value, bytes) else str(value)
        except Exception:
            raw_for_storage = repr(value)
        return _store_rejection(
            session_factory,
            topic=topic,
            partition=partition,
            offset=offset,
            raw_value=raw_for_storage,
            error=f"{exc.__class__.__name__}: {exc}",
        )

    event_id = str(event.event_id)
    identity_conflict = False
    inferred_existing_inbox = False
    with session_factory.begin() as session:
        existing = session.get(FraudEventInboxRow, event_id)
        if existing is not None:
            if (
                existing.transaction_id == event.transaction.transaction_id
                and existing.event_type == event.event_type
                and existing.event_version == event.event_version
                and existing.payload == payload
            ):
                # Support safe recovery of inbox rows created before inference
                # was deployed, or after an older consumer committed the inbox.
                prediction = session.get(
                    FraudPredictionRow, event.transaction.transaction_id
                )
                if inference_service is not None and prediction is None:
                    inference_service.predict_and_persist(session, event)
                    inferred_existing_inbox = True
                else:
                    return "duplicate"
            else:
                identity_conflict = True
        else:
            session.add(
                FraudEventInboxRow(
                    event_id=event_id,
                    transaction_id=event.transaction.transaction_id,
                    event_type=event.event_type,
                    event_version=event.event_version,
                    payload=payload,
                    received_at=datetime.now(timezone.utc),
                )
            )
            session.flush()
            if inference_service is not None:
                inference_service.predict_and_persist(session, event)

    if identity_conflict:
        return _store_rejection(
            session_factory,
            topic=topic,
            partition=partition,
            offset=offset,
            raw_value=raw_value,
            error="event_id collision: existing inbox record has different event identity",
        )
    if inferred_existing_inbox:
        return "predicted_existing_inbox"
    return "stored"


def run_fraud_consumer(
    consumer: Any,
    session_factory: sessionmaker,
    *,
    stop_event: threading.Event | None = None,
    poll_timeout_ms: int = 1000,
    on_result: Callable[[str], None] | None = None,
    inference_service: FraudInferenceService | None = None,
) -> None:
    """Consume one record per poll and commit only its durable source position.

    Infrastructure/database errors are allowed to stop the process. Because
    auto-commit is disabled and the explicit offset is not committed, Kafka
    redelivers the event after restart.
    """
    if poll_timeout_ms < 1:
        raise ValueError("poll_timeout_ms must be positive")
    stop = stop_event or threading.Event()

    while not stop.is_set():
        records = consumer.poll(timeout_ms=poll_timeout_ms, max_records=1)
        for _topic_partition, messages in records.items():
            for message in messages:
                result = process_fraud_message(
                    session_factory,
                    topic=message.topic,
                    partition=message.partition,
                    offset=message.offset,
                    value=message.value,
                    inference_service=inference_service,
                )
                topic_partition = TopicPartition(message.topic, message.partition)
                consumer.commit({
                    topic_partition: OffsetAndMetadata(message.offset + 1, "", -1)
                })
                if on_result is not None:
                    on_result(result)


def fraud_consumer_settings() -> dict[str, Any]:
    """Read independent fraud consumer settings from environment."""
    settings = {
        "topic": os.getenv("KAFKA_TRANSACTION_TOPIC", DEFAULT_TOPIC),
        "group_id": os.getenv("KAFKA_FRAUD_CONSUMER_GROUP_ID", DEFAULT_GROUP_ID),
        "poll_timeout_ms": int(os.getenv("FRAUD_CONSUMER_POLL_TIMEOUT_MS", "1000")),
    }
    if not settings["topic"]:
        raise ValueError("KAFKA_TRANSACTION_TOPIC must not be empty")
    if not settings["group_id"]:
        raise ValueError("KAFKA_FRAUD_CONSUMER_GROUP_ID must not be empty")
    if settings["poll_timeout_ms"] < 1:
        raise ValueError("FRAUD_CONSUMER_POLL_TIMEOUT_MS must be positive")
    return settings
