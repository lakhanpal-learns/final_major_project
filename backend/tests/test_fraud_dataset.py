from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.db.database import create_schema
from app.db.repository import save_registry, save_transactions
from app.db.tables import OutboxEventRow
from app.services.fraud_dataset import prepare_fraud_dataset, write_fraud_dataset
from app.services.registry import build_registry
from app.services.transaction_generator import generate_transactions
from app.services.fraud_dataset_validation import assign_chronological_splits, validate_fraud_dataset
from app.services.fraud_features import build_fraud_feature_rows


@pytest.fixture
def session():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    create_schema(engine)
    with Session(engine) as db:
        yield db
    engine.dispose()


def test_dataset_extracts_seed_cohort_and_excludes_live_outbox_transactions(session):
    registry = build_registry(customer_count=20, seed=12)
    transactions = generate_transactions(
        registry, normal_count=30, fraud_count=6, aml_count=6, seed=13
    )
    save_registry(session, registry)
    save_transactions(session, transactions)

    live_id = "LIVE_TXN_0000000000000001"
    sender = registry.accounts[0]
    receiver = registry.accounts[1]
    live_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    from app.db.tables import TransactionRow
    session.add(TransactionRow(
        transaction_id=live_id,
        sender_account_id=sender.account_id,
        receiver_account_id=receiver.account_id,
        amount=Decimal("10.00"),
        currency="INR",
        transaction_type="TRANSFER",
        channel="UPI",
        status="COMPLETED",
        timestamp=live_time,
        is_fraud=False,
        is_aml_suspicious=False,
    ))
    session.add(OutboxEventRow(
        event_id="00000000-0000-0000-0000-000000000001",
        aggregate_type="transaction",
        aggregate_id=live_id,
        event_type="bank.transaction.created.v1",
        payload={},
        created_at=live_time,
        attempts=0,
    ))
    session.commit()

    rows, summary = prepare_fraud_dataset(session)
    assert summary["total_rows"] == len(transactions)
    assert summary["excluded_live_transactions"] == 1
    assert summary["fraud"] == sum(item.is_fraud for item in transactions)
    assert all(row["dataset_split"] in {"train", "validation", "test"} for row in rows)
    assert "sender_balance_after" in summary["excluded_columns"]


def test_chronological_split_preserves_order_and_expected_partition_sizes():
    source = [{"value": index} for index in range(20)]
    split_rows = assign_chronological_splits(source)
    assert [row["dataset_split"] for row in split_rows] == (
        ["train"] * 14 + ["validation"] * 3 + ["test"] * 3
    )


def test_dataset_validation_rejects_unexpected_leakage_column():
    now = datetime(2025, 1, 1, tzinfo=timezone.utc)
    record = {
        "transaction_id": "x",
        "sender_account_id": "a",
        "receiver_account_id": "b",
        "amount": Decimal("10"),
        "currency": "INR",
        "transaction_type": "TRANSFER",
        "channel": "UPI",
        "timestamp": now,
        "sender_account_type": "SAVINGS",
        "receiver_account_type": "CURRENT",
        "is_fraud": False,
    }
    row = build_fraud_feature_rows([record])[0]
    split = {**row, "dataset_split": "train", "transaction_id": "x"}
    with pytest.raises(ValueError, match="invalid schema"):
        validate_fraud_dataset([split])


def test_dataset_export_writes_csv_and_metadata(tmp_path):
    rows = [{
        "amount": 10.0, "log_amount": 2.39, "currency": "INR",
        "transaction_type": "TRANSFER", "channel": "UPI",
        "transaction_hour": 10, "day_of_week": 2,
        "sender_account_type": "SAVINGS", "receiver_account_type": "CURRENT",
        "sender_txn_count_1h": 0, "sender_txn_count_24h": 0,
        "sender_amount_sum_24h": 0.0, "sender_unique_recipients_24h": 0,
        "receiver_txn_count_24h": 0, "sender_avg_amount_30d": 0.0,
        "is_fraud": 0, "dataset_split": "train",
    }]
    from app.services.fraud_dataset_validation import validate_fraud_dataset
    summary = validate_fraud_dataset(rows)
    csv_path, metadata_path = write_fraud_dataset(rows, summary, tmp_path)
    assert csv_path.exists()
    assert metadata_path.exists()
    assert "dataset_split" in csv_path.read_text(encoding="utf-8").splitlines()[0]
