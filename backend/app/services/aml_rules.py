"""Versioned, explainable AML typology rules for Sentinel.

V2 uses stronger multi-event evidence. These are synthetic academic heuristics,
not regulatory thresholds or proof of money laundering.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.tables import TransactionRow
from app.models.events import TransactionCreatedEvent

RULE_VERSION = "aml_rules_v2"
FAN_WINDOW = timedelta(minutes=60)
LOOKBACK = timedelta(hours=24)
LAYERING_WINDOW = timedelta(hours=2)
MIN_COUNTERPARTIES = 4
FAN_MIN_AGGREGATE = Decimal("150000.00")
STRUCTURING_MAX_SINGLE = Decimal("100000.00")
STRUCTURING_MIN_TOTAL = Decimal("100000.00")
STRUCTURING_MIN_COUNT = 4
PASS_THROUGH_MIN_RATIO = Decimal("0.80")
PASS_THROUGH_MAX_RATIO = Decimal("1.05")
CYCLE_MIN_RATIO = Decimal("0.80")
CYCLE_MAX_RATIO = Decimal("1.20")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _finding(typology: str, subject: str, related: set[str], evidence: dict[str, Any], severity: str = "MEDIUM") -> dict[str, Any]:
    return {
        "finding_id": str(uuid4()),
        "typology": typology,
        "subject_account_id": subject,
        "related_account_ids": sorted(related),
        "evidence": evidence,
        "severity": severity,
        "rule_version": RULE_VERSION,
    }


def _ordered_path(
    rows: list[TransactionRow], start: str, target: str, *, max_hops: int = 4
) -> list[TransactionRow] | None:
    """Find a strictly time-ordered path; one edge cannot be reused."""
    ordered = sorted(rows, key=lambda row: (_utc(row.timestamp), row.transaction_id))

    def visit(node: str, path: list[TransactionRow], seen: set[str]):
        if len(path) >= max_hops:
            return None
        last_time = _utc(path[-1].timestamp) if path else None
        for row in ordered:
            if row.transaction_id in seen or row.sender_account_id != node:
                continue
            row_time = _utc(row.timestamp)
            if last_time is not None and row_time <= last_time:
                continue
            if path:
                previous_amount = Decimal(path[-1].amount)
                current_amount = Decimal(row.amount)
                if previous_amount <= 0:
                    continue
                ratio = current_amount / previous_amount
                if not CYCLE_MIN_RATIO <= ratio <= CYCLE_MAX_RATIO:
                    continue
            next_path = path + [row]
            if row.receiver_account_id == target and len(next_path) >= 2:
                return next_path
            result = visit(row.receiver_account_id, next_path, seen | {row.transaction_id})
            if result:
                return result
        return None

    return visit(start, [], set())


def evaluate_aml_event(session: Session, event: TransactionCreatedEvent) -> list[dict[str, Any]]:
    """Evaluate one canonical transfer using strictly earlier completed ledger rows."""
    tx = event.transaction
    canonical = session.get(TransactionRow, tx.transaction_id)
    if canonical is None:
        raise ValueError(f"canonical transaction not found: {tx.transaction_id}")
    if (
        canonical.sender_account_id != tx.sender_account_id
        or canonical.receiver_account_id != tx.receiver_account_id
        or Decimal(canonical.amount) != tx.amount
        or _utc(canonical.timestamp) != _utc(tx.timestamp)
        or canonical.status != tx.status
    ):
        raise ValueError("Kafka transaction does not match the canonical ledger")

    event_time = _utc(tx.timestamp)
    prior = session.scalars(
        select(TransactionRow).where(
            TransactionRow.status == "COMPLETED",
            TransactionRow.timestamp >= event_time - LOOKBACK,
            TransactionRow.timestamp < event_time,
        ).order_by(TransactionRow.timestamp, TransactionRow.transaction_id)
    ).all()

    findings: list[dict[str, Any]] = []
    sender, receiver, amount = tx.sender_account_id, tx.receiver_account_id, tx.amount

    recent = [row for row in prior if _utc(row.timestamp) >= event_time - FAN_WINDOW]
    inbound = [row for row in recent if row.receiver_account_id == receiver]
    inbound_senders = {row.sender_account_id for row in inbound} | {sender}
    inbound_total = sum((Decimal(row.amount) for row in inbound), amount)
    if len(inbound_senders) >= MIN_COUNTERPARTIES and inbound_total >= FAN_MIN_AGGREGATE:
        base = _finding(
            "FAN_IN", receiver, inbound_senders - {receiver},
            {"window_minutes": 60, "distinct_senders": len(inbound_senders),
             "aggregate_amount": str(inbound_total),
             "pattern_transaction_ids": [row.transaction_id for row in inbound] + [tx.transaction_id],
             "triggering_transaction_id": tx.transaction_id},
        )
        for member in inbound + [canonical]:
            findings.append({**base, "finding_id": str(uuid4()), "transaction_id": member.transaction_id})

    outbound = [row for row in recent if row.sender_account_id == sender]
    outbound_receivers = {row.receiver_account_id for row in outbound} | {receiver}
    outbound_total = sum((Decimal(row.amount) for row in outbound), amount)
    if len(outbound_receivers) >= MIN_COUNTERPARTIES and outbound_total >= FAN_MIN_AGGREGATE:
        base = _finding(
            "FAN_OUT", sender, outbound_receivers - {sender},
            {"window_minutes": 60, "distinct_receivers": len(outbound_receivers),
             "aggregate_amount": str(outbound_total),
             "pattern_transaction_ids": [row.transaction_id for row in outbound] + [tx.transaction_id],
             "triggering_transaction_id": tx.transaction_id},
        )
        for member in outbound + [canonical]:
            findings.append({**base, "finding_id": str(uuid4()), "transaction_id": member.transaction_id})

    small_outgoing = [
        row for row in prior
        if row.sender_account_id == sender and Decimal(row.amount) <= STRUCTURING_MAX_SINGLE
    ]
    if amount <= STRUCTURING_MAX_SINGLE:
        rows = small_outgoing + [canonical]
        total = sum((Decimal(row.amount) for row in rows), Decimal("0"))
        recipients = {row.receiver_account_id for row in rows}
        if (
            len(rows) >= STRUCTURING_MIN_COUNT
            and len(recipients) >= 3
            and total >= STRUCTURING_MIN_TOTAL
        ):
            base = _finding(
                "STRUCTURING", sender, recipients,
                {"window_hours": 24, "transaction_count": len(rows),
                 "distinct_recipients": len(recipients), "aggregate_amount": str(total),
                 "pattern_transaction_ids": [row.transaction_id for row in rows],
                 "maximum_single_amount": str(STRUCTURING_MAX_SINGLE),
                 "minimum_aggregate_amount": str(STRUCTURING_MIN_TOTAL)},
            )
            for member in rows:
                findings.append({**base, "finding_id": str(uuid4()), "transaction_id": member.transaction_id})

    # Layering requires a near-value pass-through, not merely any incoming payment.
    recent_incoming = [
        row for row in prior
        if row.receiver_account_id == sender
        and _utc(row.timestamp) >= event_time - LAYERING_WINDOW
    ]
    pass_through = []
    for row in recent_incoming:
        incoming_amount = Decimal(row.amount)
        if incoming_amount <= 0:
            continue
        ratio = amount / incoming_amount
        if PASS_THROUGH_MIN_RATIO <= ratio <= PASS_THROUGH_MAX_RATIO:
            pass_through.append((row, ratio))
    if pass_through:
        source_accounts = {row.sender_account_id for row, _ in pass_through}
        base = _finding(
            "LAYERING", sender, source_accounts | {receiver},
            {"window_hours": 2,
             "matched_incoming_transaction_ids": [row.transaction_id for row, _ in pass_through],
             "forwarding_transaction_id": tx.transaction_id,
             "amount_ratio_range": [str(PASS_THROUGH_MIN_RATIO), str(PASS_THROUGH_MAX_RATIO)]},
            severity="HIGH",
        )
        for member, _ratio in pass_through:
            findings.append({**base, "finding_id": str(uuid4()), "transaction_id": member.transaction_id})
        findings.append({**base, "finding_id": str(uuid4()), "transaction_id": canonical.transaction_id})

    # Current edge closes a cycle only when the prior path is chronological
    # and amounts remain within a bounded transfer ratio.
    path = _ordered_path(prior, receiver, sender)
    if path:
        closing_ratio = amount / Decimal(path[-1].amount)
        if not CYCLE_MIN_RATIO <= closing_ratio <= CYCLE_MAX_RATIO:
            path = None
    if path:
        path_accounts = {path[0].sender_account_id}
        for row in path:
            path_accounts.add(row.receiver_account_id)
        base = _finding(
            "CIRCULAR_TRANSFER", sender, path_accounts | {receiver},
            {"window_hours": 24,
             "prior_path_transaction_ids": [row.transaction_id for row in path],
             "prior_path": [[row.sender_account_id, row.receiver_account_id] for row in path],
             "closing_transaction_id": tx.transaction_id},
            severity="HIGH",
        )
        for member in path + [canonical]:
            findings.append({**base, "finding_id": str(uuid4()), "transaction_id": member.transaction_id})

    return findings


def persist_aml_findings(session: Session, event: TransactionCreatedEvent, findings: list[dict[str, Any]]) -> int:
    """Persist idempotent findings for the active rule version."""
    from app.db.tables import AMLFindingRow

    inserted = 0
    for item in findings:
        exists = session.scalar(
            select(AMLFindingRow).where(
                AMLFindingRow.transaction_id == item.get("transaction_id", event.transaction.transaction_id),
                AMLFindingRow.typology == item["typology"],
                AMLFindingRow.rule_version == item["rule_version"],
            )
        )
        if exists is not None:
            continue
        session.add(AMLFindingRow(
            finding_id=item["finding_id"],
            event_id=str(event.event_id),
            transaction_id=item.get("transaction_id", event.transaction.transaction_id),
            typology=item["typology"],
            subject_account_id=item["subject_account_id"],
            related_account_ids=item["related_account_ids"],
            evidence=item["evidence"],
            severity=item["severity"],
            rule_version=item["rule_version"],
        ))
        inserted += 1
    session.flush()
    return inserted
