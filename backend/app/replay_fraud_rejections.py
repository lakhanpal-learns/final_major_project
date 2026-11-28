"""Replay previously quarantined fraud events from the durable rejection table.

The rejection rows are intentionally retained as an audit trail. Replaying is
idempotent because the normal fraud inbox and prediction persistence paths are
used for every stored raw event.
"""

from __future__ import annotations

import logging
from collections import Counter
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from app.db.database import create_db_engine, create_schema, create_session_factory
from app.db.tables import FraudEventRejectionRow
from app.services.fraud_event_consumer import process_fraud_message
from app.services.fraud_inference_service import FraudInferenceService

logger = logging.getLogger("sentinel.fraud_replay")


def replay_fraud_rejections(
    session_factory: sessionmaker,
    *,
    inference_service: FraudInferenceService | None = None,
) -> dict[str, Any]:
    """Replay all quarantined payloads without deleting their rejection records."""
    with session_factory() as session:
        rows = session.scalars(
            select(FraudEventRejectionRow).order_by(
                FraudEventRejectionRow.source_partition,
                FraudEventRejectionRow.source_offset,
            )
        ).all()
        records = [
            (
                row.source_topic,
                row.source_partition,
                row.source_offset,
                row.raw_value,
            )
            for row in rows
        ]

    outcomes: Counter[str] = Counter()
    for topic, partition, offset, raw_value in records:
        result = process_fraud_message(
            session_factory,
            topic=topic,
            partition=partition,
            offset=offset,
            value=raw_value,
            inference_service=inference_service,
        )
        outcomes[result] += 1

    return {
        "total_rejections": len(records),
        "stored": outcomes["stored"],
        "predicted_existing_inbox": outcomes["predicted_existing_inbox"],
        "duplicates": outcomes["duplicate"],
        "rejected_again": outcomes["rejected"] + outcomes["duplicate_rejection"],
    }


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    engine = create_db_engine()
    try:
        create_schema(engine)
        session_factory = create_session_factory(engine)
        inference_service = FraudInferenceService.from_environment()
        result = replay_fraud_rejections(
            session_factory,
            inference_service=inference_service,
        )
        logger.info("Fraud rejection replay completed: %s", result)
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
