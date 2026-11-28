"""Publish PostgreSQL outbox events to Kafka with at-least-once delivery."""

from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable

from kafka import KafkaProducer
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from app.db.tables import OutboxEventRow

DEFAULT_TOPIC = "bank.transactions.v1"


def create_kafka_producer(bootstrap_servers: str | None = None) -> KafkaProducer:
    """Create a synchronous-ack Kafka producer for the local publisher process."""
    servers = bootstrap_servers or os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"
    )
    return KafkaProducer(
        bootstrap_servers=servers.split(","),
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, separators=(",", ":")).encode(
            "utf-8"
        ),
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
    )


def publish_pending_events(
    session_factory: sessionmaker,
    producer: Any,
    *,
    topic: str = DEFAULT_TOPIC,
    batch_size: int = 100,
    acknowledgement_timeout_seconds: float = 10.0,
) -> dict[str, int]:
    """Publish one bounded batch and persist acknowledgement or retry state.

    PostgreSQL row locks are held until the batch transaction commits. SKIP LOCKED
    lets another publisher process work on different rows rather than waiting for
    this batch. Kafka acknowledgement happens before published_at is written.
    """
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")
    if acknowledgement_timeout_seconds <= 0:
        raise ValueError("acknowledgement timeout must be positive")

    stats = {"selected": 0, "published": 0, "failed": 0}
    with session_factory.begin() as session:
        events = session.scalars(
            select(OutboxEventRow)
            .where(OutboxEventRow.published_at.is_(None))
            .order_by(OutboxEventRow.created_at, OutboxEventRow.event_id)
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        ).all()
        stats["selected"] = len(events)

        for event in events:
            try:
                delivery = producer.send(
                    topic,
                    key=event.aggregate_id,
                    value=event.payload,
                )
                delivery.get(timeout=acknowledgement_timeout_seconds)
                event.published_at = datetime.now(timezone.utc)
                event.last_error = None
                stats["published"] += 1
            except Exception as exc:  # broker/network errors must remain retryable
                event.attempts += 1
                event.last_error = str(exc)[:2000] or exc.__class__.__name__
                stats["failed"] += 1

    return stats


def run_publisher_loop(
    session_factory: sessionmaker,
    producer: Any,
    *,
    topic: str = DEFAULT_TOPIC,
    batch_size: int = 100,
    poll_interval_seconds: float = 1.0,
    acknowledgement_timeout_seconds: float = 10.0,
    stop_event: threading.Event | None = None,
    on_batch: Callable[[dict[str, int]], None] | None = None,
) -> None:
    """Continuously publish pending events until the supplied stop event is set."""
    if poll_interval_seconds <= 0:
        raise ValueError("poll interval must be positive")
    stop = stop_event or threading.Event()

    while not stop.is_set():
        stats = publish_pending_events(
            session_factory,
            producer,
            topic=topic,
            batch_size=batch_size,
            acknowledgement_timeout_seconds=acknowledgement_timeout_seconds,
        )
        if on_batch is not None:
            on_batch(stats)
        if stats["selected"] == 0:
            stop.wait(poll_interval_seconds)
        elif stats["failed"] > 0:
            stop.wait(poll_interval_seconds)


def publisher_settings() -> dict[str, Any]:
    """Read publisher settings from environment variables."""
    return {
        "topic": os.getenv("KAFKA_TRANSACTION_TOPIC", DEFAULT_TOPIC),
        "batch_size": int(os.getenv("OUTBOX_BATCH_SIZE", "100")),
        "poll_interval_seconds": float(os.getenv("OUTBOX_POLL_INTERVAL_SECONDS", "1")),
        "acknowledgement_timeout_seconds": float(
            os.getenv("KAFKA_ACK_TIMEOUT_SECONDS", "10")
        ),
    }
