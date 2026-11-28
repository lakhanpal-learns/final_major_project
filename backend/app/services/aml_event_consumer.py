"""Independent, idempotent AML consumer for the shared transaction topic."""
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

from app.db.tables import AMLEventInboxRow, AMLFindingRow, TransactionRow
from app.models.events import TransactionCreatedEvent
from app.services.aml_rules import evaluate_aml_event, persist_aml_findings

DEFAULT_TOPIC = "bank.transactions.v1"
DEFAULT_GROUP_ID = "aml-monitor-v1"


def create_aml_kafka_consumer(bootstrap_servers: str | None = None, *, topic: str = DEFAULT_TOPIC, group_id: str = DEFAULT_GROUP_ID) -> KafkaConsumer:
    servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    return KafkaConsumer(topic, bootstrap_servers=servers.split(","), group_id=group_id,
                         enable_auto_commit=False, auto_offset_reset="earliest", max_poll_records=1)


def process_aml_message(session_factory: sessionmaker, *, value: bytes | str) -> str:
    """Validate and persist inbox plus AML findings atomically."""
    raw = value.decode("utf-8") if isinstance(value, bytes) else value
    event = TransactionCreatedEvent.model_validate(json.loads(raw))
    event_id = str(event.event_id)
    with session_factory.begin() as session:
        existing = session.get(AMLEventInboxRow, event_id)
        if existing is not None:
            if existing.transaction_id != event.transaction.transaction_id or existing.payload != json.loads(raw):
                raise ValueError("AML event_id collision with different payload")
            # Recovery path for an inbox row committed without findings.
            has_findings = session.scalar(select(AMLFindingRow.finding_id).where(
                AMLFindingRow.transaction_id == event.transaction.transaction_id).limit(1))
            if has_findings is not None:
                return "duplicate"
        else:
            session.add(AMLEventInboxRow(
                event_id=event_id, transaction_id=event.transaction.transaction_id,
                payload=json.loads(raw), received_at=datetime.now(timezone.utc),
            ))
            session.flush()
        findings = evaluate_aml_event(session, event)
        count = persist_aml_findings(session, event, findings)
    return f"processed:{count}"


def aml_consumer_settings() -> dict[str, Any]:
    topic = os.getenv("KAFKA_TRANSACTION_TOPIC", DEFAULT_TOPIC)
    group_id = os.getenv("KAFKA_AML_CONSUMER_GROUP_ID", DEFAULT_GROUP_ID)
    poll_timeout_ms = int(os.getenv("AML_CONSUMER_POLL_TIMEOUT_MS", "1000"))
    if not topic or not group_id or poll_timeout_ms < 1:
        raise ValueError("Invalid AML consumer settings")
    return {"topic": topic, "group_id": group_id, "poll_timeout_ms": poll_timeout_ms}


def run_aml_consumer(consumer: Any, session_factory: sessionmaker, *, stop_event: threading.Event | None = None,
                     poll_timeout_ms: int = 1000, on_result: Callable[[str], None] | None = None) -> None:
    stop = stop_event or threading.Event()
    while not stop.is_set():
        records = consumer.poll(timeout_ms=poll_timeout_ms, max_records=1)
        for _partition, messages in records.items():
            for message in messages:
                result = process_aml_message(session_factory, value=message.value)
                tp = TopicPartition(message.topic, message.partition)
                consumer.commit({tp: OffsetAndMetadata(message.offset + 1, "", -1)})
                if on_result:
                    on_result(result)
