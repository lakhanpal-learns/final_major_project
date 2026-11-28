from datetime import datetime, timedelta, timezone
from decimal import Decimal

from app.services.fraud_features import FEATURE_COLUMNS, build_fraud_feature_rows


def _record(transaction_id, timestamp, sender="A", receiver="B", amount="100", fraud=False):
    return {
        "transaction_id": transaction_id,
        "sender_account_id": sender,
        "receiver_account_id": receiver,
        "amount": Decimal(amount),
        "currency": "INR",
        "transaction_type": "TRANSFER",
        "channel": "UPI",
        "timestamp": timestamp,
        "sender_account_type": "SAVINGS",
        "receiver_account_type": "CURRENT",
        "is_fraud": fraud,
    }


def test_features_use_only_strictly_earlier_sender_activity():
    start = datetime(2025, 1, 1, 10, tzinfo=timezone.utc)
    rows = build_fraud_feature_rows([
        _record("first", start, receiver="B", amount="100"),
        _record("same-time", start, receiver="C", amount="200"),
        _record("later", start + timedelta(minutes=30), receiver="D", amount="300"),
    ])

    later = rows[2]
    assert later["sender_txn_count_1h"] == 2
    assert later["sender_txn_count_24h"] == 2
    assert later["sender_amount_sum_24h"] == 300.0
    assert later["sender_unique_recipients_24h"] == 2
    assert later["sender_avg_amount_30d"] == 150.0
    assert rows[0]["sender_txn_count_24h"] == 0
    assert rows[1]["sender_txn_count_24h"] == 0


def test_receiver_velocity_counts_prior_incoming_transfers():
    start = datetime(2025, 1, 1, 10, tzinfo=timezone.utc)
    rows = build_fraud_feature_rows([
        _record("one", start, sender="A", receiver="Z"),
        _record("two", start + timedelta(hours=1), sender="B", receiver="Z"),
        _record("three", start + timedelta(hours=2), sender="C", receiver="Z"),
    ])
    assert rows[2]["receiver_txn_count_24h"] == 2


def test_short_window_velocity_and_recipient_concentration_features():
    start = datetime(2025, 1, 1, 10, tzinfo=timezone.utc)
    rows = build_fraud_feature_rows([
        _record("one", start, receiver="B"),
        _record("two", start + timedelta(minutes=2), receiver="B"),
        _record("three", start + timedelta(minutes=4), receiver="C"),
        _record("target", start + timedelta(minutes=6), receiver="D"),
    ])
    target = rows[-1]
    assert target["sender_txn_count_5m"] == 1
    assert target["sender_txn_count_15m"] == 3
    assert target["sender_unique_recipients_1h"] == 2
    assert target["sender_unique_recipients_24h"] == 2
    assert target["sender_max_recipient_txn_count_24h"] == 2
    assert target["sender_recipient_repeat_ratio_24h"] == 2 / 3


def test_same_timestamp_events_do_not_leak_into_short_window_features():
    start = datetime(2025, 1, 1, 10, tzinfo=timezone.utc)
    rows = build_fraud_feature_rows([
        _record("one", start, receiver="B"),
        _record("two", start, receiver="C"),
        _record("later", start + timedelta(minutes=1), receiver="D"),
    ])
    assert rows[0]["sender_txn_count_5m"] == 0
    assert rows[1]["sender_txn_count_5m"] == 0
    assert rows[2]["sender_txn_count_5m"] == 2


def test_features_have_no_identifiers_or_non_fraud_labels():
    now = datetime(2025, 1, 1, tzinfo=timezone.utc)
    row = build_fraud_feature_rows([_record("secret-id", now)])[0]
    assert set(row) == set(FEATURE_COLUMNS) | {"is_fraud"}
    assert not any("account_id" in name or "scenario" in name for name in row)
