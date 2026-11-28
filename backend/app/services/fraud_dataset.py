"""Load labelled cohorts and prepare leakage-controlled fraud datasets."""

import csv
import json
from datetime import datetime
from pathlib import Path
from decimal import Decimal

from sqlalchemy import exists, select
from sqlalchemy.orm import Session

from app.db.tables import AccountRow, OutboxEventRow, TransactionRow
from app.services.fraud_features import (
    FEATURE_COLUMNS,
    TARGET_COLUMN,
    build_fraud_feature_rows,
    ordered_fraud_records,
)
from app.services.fraud_dataset_validation import (
    SPLIT_COLUMN,
    assign_chronological_splits,
    validate_fraud_dataset,
)


def _parse_bool(value: str, field: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"true", "1"}:
        return True
    if normalized in {"false", "0"}:
        return False
    raise ValueError(f"Invalid boolean value for {field}: {value!r}")


def load_synthetic_csv_records(input_dir: str | Path) -> tuple[list[dict], dict]:
    """Load the offline generator export and resolve canonical account types."""
    source = Path(input_dir)
    metadata_path = source / "metadata.json"
    accounts_path = source / "accounts.csv"
    transactions_path = source / "transactions.csv"
    for path in (metadata_path, accounts_path, transactions_path):
        if not path.is_file():
            raise FileNotFoundError(f"Required synthetic dataset file not found: {path}")

    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    if metadata.get("dataset_type") != "synthetic":
        raise ValueError("Input metadata dataset_type must be 'synthetic'.")

    with accounts_path.open(newline="", encoding="utf-8") as handle:
        account_rows = list(csv.DictReader(handle))
    account_types: dict[str, str] = {}
    for row in account_rows:
        account_id = row.get("account_id", "").strip()
        account_type = row.get("account_type", "").strip()
        if not account_id or not account_type:
            raise ValueError("accounts.csv requires non-empty account_id and account_type.")
        if account_id in account_types:
            raise ValueError(f"Duplicate account_id in accounts.csv: {account_id}")
        account_types[account_id] = account_type

    required = {
        "transaction_id", "sender_account_id", "receiver_account_id",
        "amount", "currency", "transaction_type", "channel", "timestamp", "is_fraud",
    }
    records = []
    seen_ids: set[str] = set()
    with transactions_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"transactions.csv missing required columns: {sorted(missing)}")
        for line_number, row in enumerate(reader, start=2):
            transaction_id = row["transaction_id"].strip()
            sender_id = row["sender_account_id"].strip()
            receiver_id = row["receiver_account_id"].strip()
            if not transaction_id or transaction_id in seen_ids:
                raise ValueError(f"Missing or duplicate transaction_id at CSV line {line_number}.")
            seen_ids.add(transaction_id)
            if sender_id not in account_types or receiver_id not in account_types:
                raise ValueError(f"Unknown account reference at CSV line {line_number}.")
            if sender_id == receiver_id:
                raise ValueError(f"Self-transfer at CSV line {line_number}.")
            amount = Decimal(row["amount"])
            if not amount.is_finite() or amount <= 0:
                raise ValueError(f"Invalid transaction amount at CSV line {line_number}.")
            timestamp = datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00"))
            records.append({
                "transaction_id": transaction_id,
                "sender_account_id": sender_id,
                "receiver_account_id": receiver_id,
                "amount": amount,
                "currency": row["currency"],
                "transaction_type": row["transaction_type"],
                "channel": row["channel"],
                "timestamp": timestamp,
                "sender_account_type": account_types[sender_id],
                "receiver_account_type": account_types[receiver_id],
                TARGET_COLUMN: _parse_bool(row["is_fraud"], "is_fraud"),
            })
    if len(records) != metadata.get("counts", {}).get("transaction_count"):
        raise ValueError(
            "transactions.csv row count does not match metadata.json transaction_count."
        )
    return records, metadata


def prepare_fraud_dataset_from_csv(input_dir: str | Path) -> tuple[list[dict], dict]:
    """Prepare the expanded offline synthetic cohort without requiring PostgreSQL."""
    source_records, source_metadata = load_synthetic_csv_records(input_dir)
    ordered_records = ordered_fraud_records(source_records)
    feature_rows = build_fraud_feature_rows(ordered_records)
    split_rows = assign_chronological_splits(
        feature_rows, [record["timestamp"] for record in ordered_records]
    )
    summary = validate_fraud_dataset(split_rows)
    summary["source_cohort"] = "offline synthetic CSV export"
    summary["source_dataset_type"] = source_metadata["dataset_type"]
    summary["source_parameters"] = source_metadata.get("parameters", {})
    summary["source_counts"] = source_metadata.get("counts", {})
    summary["excluded_columns"] = [
        "transaction_id", "sender_account_id", "receiver_account_id",
        "is_aml_suspicious", "fraud_scenario", "aml_typology",
        "sender_balance_before", "sender_balance_after",
        "receiver_balance_before", "receiver_balance_after",
    ]
    return split_rows, summary


def load_seed_cohort_records(session: Session) -> list[dict]:
    """Load labelled seed transactions, excluding live transactions with outbox rows."""
    has_outbox = exists(
        select(OutboxEventRow.event_id).where(
            OutboxEventRow.aggregate_id == TransactionRow.transaction_id
        )
    )
    statement = (
        select(TransactionRow, AccountRow.account_type.label("sender_account_type"))
        .join(AccountRow, AccountRow.account_id == TransactionRow.sender_account_id)
        .where(~has_outbox)
        .order_by(TransactionRow.timestamp, TransactionRow.transaction_id)
    )
    rows = session.execute(statement).all()
    receiver_types = dict(session.execute(
        select(AccountRow.account_id, AccountRow.account_type)
    ).all())
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
    """Prepare the PostgreSQL seed cohort (legacy/local operational source)."""
    source_records = load_seed_cohort_records(session)
    ordered_records = ordered_fraud_records(source_records)
    feature_rows = build_fraud_feature_rows(ordered_records)
    split_rows = assign_chronological_splits(
        feature_rows, [record["timestamp"] for record in ordered_records]
    )
    summary = validate_fraud_dataset(split_rows)
    summary["source_cohort"] = "seed transactions without matching outbox aggregate"
    summary["excluded_live_transactions"] = session.query(TransactionRow).filter(
        exists(select(OutboxEventRow.event_id).where(
            OutboxEventRow.aggregate_id == TransactionRow.transaction_id
        ))
    ).count()
    summary["excluded_columns"] = [
        "transaction_id", "sender_account_id", "receiver_account_id",
        "is_aml_suspicious", "fraud_scenario", "aml_typology",
        "sender_balance_before", "sender_balance_after",
        "receiver_balance_before", "receiver_balance_after",
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
