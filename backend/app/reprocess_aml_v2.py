"""Replay persisted AML inbox payloads through the active AML rule version.

This does not reset Kafka offsets or delete historical V1 findings.
"""
from __future__ import annotations

import json
from datetime import datetime
from sqlalchemy import select

from app.db.database import create_db_engine, create_session_factory
from app.db.tables import AMLEventInboxRow
from app.services.aml_rules import RULE_VERSION
from app.models.events import TransactionCreatedEvent
from app.services.aml_event_consumer import process_aml_message


def main() -> None:
    engine = create_db_engine()
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as session:
            rows = session.scalars(select(AMLEventInboxRow)).all()
            payloads = [dict(row.payload) for row in rows]

        def event_key(payload: dict):
            event = TransactionCreatedEvent.model_validate(payload)
            return event.transaction.timestamp, event.transaction.transaction_id

        payloads.sort(key=event_key)
        processed = 0
        findings_inserted = 0
        for payload in payloads:
            result = process_aml_message(
                session_factory,
                value=json.dumps(payload, separators=(",", ":")),
            )
            if result.startswith("processed:"):
                processed += 1
                findings_inserted += int(result.split(":", 1)[1])

        with session_factory() as session:
            from sqlalchemy import func
            from app.db.tables import AMLFindingRow
            active_findings = session.scalar(
                select(func.count()).select_from(AMLFindingRow).where(
                    AMLFindingRow.rule_version == RULE_VERSION
                )
            ) or 0

        print(f"AML replay completed for {RULE_VERSION}.")
        print(f"  inbox payloads scanned: {len(payloads)}")
        print(f"  events re-evaluated: {processed}")
        print(f"  findings inserted in this run: {findings_inserted}")
        print(f"  active-version findings in database: {active_findings}")
        print("  historical V1 findings were retained.")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
