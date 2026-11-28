"""Explainable, point-in-time AML typology rules for the shared bank ledger.

These configurable heuristics are an academic demonstration, not regulatory
thresholds or a substitute for a bank's risk-based AML programme.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.tables import AccountRow, TransactionRow
from app.models.events import TransactionCreatedEvent

RULE_VERSION = "aml_rules_v1"
FAN_WINDOW = timedelta(minutes=60)
LOOKBACK = timedelta(hours=24)
MIN_COUNTERPARTIES = 3
STRUCTURING_MAX_SINGLE = Decimal("100000.00")
STRUCTURING_MIN_TOTAL = Decimal("150000.00")
STRUCTURING_MIN_COUNT = 3


def _utc(value: datetime) -> datetime:\n    """Normalize database/event timestamps before exact event matching."""\n    if value.tzinfo is None:\n        return value.replace(tzinfo=timezone.utc)\n    return value.astimezone(timezone.utc)\n\n\ndef _finding(typology: str, subject: str, related: set[str], evidence: dict[str, Any], severity: str = "MEDIUM") -> dict[str, Any]:
    return {
        "finding_id": str(uuid4()),
        "typology": typology,
        "subject_account_id": subject,
        "related_account_ids": sorted(related),
        "evidence": evidence,
        "severity": severity,
        "rule_version": RULE_VERSION,
    }


def _has_path(graph: dict[str, set[str]], start: str, target: str, max_hops: int = 4) -> list[str] | None:
    """Return one bounded directed path, if one exists."""
    stack = [(start, [start])]
    while stack:
        node, path = stack.pop()
        if len(path) - 1 >= max_hops:
            continue
        for neighbour in graph.get(node, set()):
            if neighbour == target:
                return path + [neighbour]
            if neighbour not in path:
                stack.append((neighbour, path + [neighbour]))
    return None


def evaluate_aml_event(session: Session, event: TransactionCreatedEvent) -> list[dict[str, Any]]:
    """Evaluate one canonical transfer using only strictly earlier ledger rows."""
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

    start = tx.timestamp - LOOKBACK
    prior = session.scalars(
        select(TransactionRow).where(
            TransactionRow.status == "COMPLETED",
            TransactionRow.timestamp >= start,
            TransactionRow.timestamp < tx.timestamp,
        ).order_by(TransactionRow.timestamp, TransactionRow.transaction_id)
    ).all()

    findings: list[dict[str, Any]] = []
    sender, receiver = tx.sender_account_id, tx.receiver_account_id
    amount = tx.amount

    # Fan-in: multiple distinct accounts transfer to the same beneficiary
    # within one hour. The current event contributes one sender.
    fan_in = [row for row in prior if row.receiver_account_id == receiver and row.timestamp >= tx.timestamp - FAN_WINDOW]
    inbound_senders = {row.sender_account_id for row in fan_in} | {sender}
    if len(inbound_senders) >= MIN_COUNTERPARTIES:
        findings.append(_finding(
            "FAN_IN", receiver, inbound_senders - {receiver},
            {"window_minutes": 60, "distinct_senders": len(inbound_senders),
             "prior_transaction_ids": [row.transaction_id for row in fan_in],
             "current_transaction_id": tx.transaction_id},
        ))

    # Fan-out: one originator sends to multiple distinct beneficiaries.
    fan_out = [row for row in prior if row.sender_account_id == sender and row.timestamp >= tx.timestamp - FAN_WINDOW]
    outbound_receivers = {row.receiver_account_id for row in fan_out} | {receiver}
    if len(outbound_receivers) >= MIN_COUNTERPARTIES:
        findings.append(_finding(
            "FAN_OUT", sender, outbound_receivers - {sender},
            {"window_minutes": 60, "distinct_receivers": len(outbound_receivers),
             "prior_transaction_ids": [row.transaction_id for row in fan_out],
             "current_transaction_id": tx.transaction_id},
        ))

    # Structuring heuristic: several individually smaller transfers whose
    # rolling aggregate crosses a configurable demonstration threshold.
    small_outgoing = [
        row for row in prior
        if row.sender_account_id == sender and row.amount <= STRUCTURING_MAX_SINGLE
    ]
    if amount <= STRUCTURING_MAX_SINGLE:
        structuring_rows = small_outgoing + [canonical]
        total = sum((Decimal(row.amount) for row in structuring_rows), Decimal("0"))
        if len(structuring_rows) >= STRUCTURING_MIN_COUNT and total >= STRUCTURING_MIN_TOTAL:
            findings.append(_finding(
                "STRUCTURING", sender, {row.receiver_account_id for row in structuring_rows},
                {"window_hours": 24, "transaction_count": len(structuring_rows),
                 "aggregate_amount": str(total),
                 "maximum_single_amount": str(STRUCTURING_MAX_SINGLE),
                 "minimum_aggregate_amount": str(STRUCTURING_MIN_TOTAL)},
            ))

    # Pass-through layering heuristic: the sending account received funds
    # during the lookback and now forwards funds to another account.
    recent_incoming = [row for row in prior if row.receiver_account_id == sender]
    if recent_incoming:
        source_accounts = {row.sender_account_id for row in recent_incoming}
        findings.append(_finding(
            "LAYERING", sender, source_accounts | {receiver},
            {"window_hours": 24, "prior_incoming_transaction_ids": [row.transaction_id for row in recent_incoming],
             "forwarding_transaction_id": tx.transaction_id},
        ))

    # Circular transfer: current sender -> receiver closes a directed cycle
    # when a bounded prior path exists from receiver back to sender.
    graph: dict[str, set[str]] = defaultdict(set)
    for row in prior:
        graph[row.sender_account_id].add(row.receiver_account_id)
    path = _has_path(graph, receiver, sender)
    if path:
        findings.append(_finding(
            "CIRCULAR_TRANSFER", sender, set(path) | {receiver},
            {"window_hours": 24, "prior_path": path,
             "closing_transaction_id": tx.transaction_id},
            severity="HIGH",
        ))

    return findings


def persist_aml_findings(session: Session, event: TransactionCreatedEvent, findings: list[dict[str, Any]]) -> int:
    """Insert deterministic event findings; uniqueness is enforced by the DB."""
    from app.db.tables import AMLFindingRow

    inserted = 0
    for item in findings:
        exists = session.scalar(
            select(AMLFindingRow).where(
                AMLFindingRow.transaction_id == event.transaction.transaction_id,
                AMLFindingRow.typology == item["typology"],
                AMLFindingRow.rule_version == item["rule_version"],
            )
        )
        if exists is not None:
            continue
        session.add(AMLFindingRow(
            finding_id=item["finding_id"],
            event_id=str(event.event_id),
            transaction_id=event.transaction.transaction_id,
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
