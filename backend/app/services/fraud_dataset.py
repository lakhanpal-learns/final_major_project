"""Extract the labelled seed cohort and prepare an offline fraud dataset."""

import csv
import json
from pathlib import Path

from sqlalchemy import exists, select
from sqlalchemy.orm import Session

from app.db.tables import AccountRow, OutboxEventRow, TransactionRow
from app.services.fraud_features import FEATURE_COLUMNS, TARGET_COLUMN, build_fraud_feature_rows
from app.services.fraud_dataset_validation import (
    SPLIT_COLUMN,
    assign_chronological_splits,
    validate_fraud_dataset,
)


def load_seed_cohort_records(session: Session) -> list[dict]:
    """Load labelled seed transactions, excluding live transactions with outbox rows.

    Project convention: seed_database() writes transactions without outbox rows;
    persist_live_transfer() writes each live transaction and its outbox event in
    the same database transaction. This query therefore isolates the original
    labelled seed cohort from the continuously generated all-normal live stream.
    """
    has_outbox = exists(
        select(OutboxEventRow.event_id).where(
            OutboxEventRow.aggregate_id == TransactionRow.transaction_id
        )
    )
    statement = (
        select(
            TransactionRow,
            AccountRow.account_type.label("sender_account_type"),
        )
        .join(AccountRow, AccountRow.account_id == TransactionRow.sender_account_id)
        .where(~has_outbox)
        .order_by(TransactionRow.timestamp, TransactionRow.transaction_id)
    )
    rows = session.execute(statement).all()

    receiver_types = {
        account_id: account_type
        for account_id, account_type in session.execute(
            select(AccountRow.account_id, AccountRow.account_type)
        ).all()
    }

    records = []
    for transaction, sender_type in rows:
        records.append({
            "transaction_id": transaction.transaction_id,
            "sender_account_id": transaction.sender_account_id,
            "receiver_account_id": transaction.receiver_account_id,
            "amount": transaction.amount,
            "currency": transaction.currency,
            "transaction_type": transaction.transaction_type,
            "channel": transaction.channel,
            "timestamp": transaction.timestamp,
            "sender_account_type": sender_type,
            "receiver_account_type": receiver_types[transaction.receiver_account_id],
            TARGET_COLUMN: transaction.is_fraud,
        })
    return records


def prepare_fraud_dataset(session: Session) -> tuple[list[dict], dict]:
    """Extract, engineer, chronologically split and validate the fraud dataset."""
    source_records = load_seed_cohort_records(session)
    feature_rows = build_fraud_feature_rows(source_records)
    split_rows = assign_chronological_splits(feature_rows)
    summary = validate_fraud_dataset(split_rows)
    summary["source_cohort"] = "seed transactions without matching outbox aggregate"
    summary["excluded_live_transactions"] = session.query(TransactionRow).filter(
        exists(
            select(OutboxEventRow.event_id).where(
                OutboxEventRow.aggregate_id == TransactionRow.transaction_id
            )
        )
    ).count()
    summary["excluded_columns"] = [
        "transaction_id",
        "sender_account_id",
        "receiver_account_id",
        "is_aml_suspicious",
        "fraud_scenario",
        "aml_typology",
        "sender_balance_before",
        "sender_balance_after",
        "receiver_balance_before",
        "receiver_balance_after",
    ]
    return split_rows, summary


def write_fraud_dataset(rows: list[dict], summary: dict, output_dir: str | Path) -> tuple[Path, Path]:
    """Write a model-ready CSV and an auditable JSON summary."""
    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    csv_path = destination / "fraud_dataset.csv"
    metadata_path = destination / "fraud_dataset_metadata.json"

    columns = list(FEATURE_COLUMNS) + [TARGET_COLUMN, SPLIT_COLUMN]
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)

    with metadata_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)
        handle.write("\n")
    return csv_path, metadata_path
