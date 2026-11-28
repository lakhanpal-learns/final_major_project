import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db.database import create_schema
from app.db.tables import (
    AccountRow,
    CustomerRow,
    FraudEventInboxRow,
    FraudPredictionRow,
    TransactionRow,
)
from app.models.events import TransactionCreatedEvent
from app.services.fraud_event_consumer import process_fraud_message
from app.services.fraud_features import FEATURE_COLUMNS
from app.services.fraud_inference_service import (
    FraudInferenceService,
    build_online_fraud_features,
)


@pytest.fixture
def session_factory():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    create_schema(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    yield factory
    engine.dispose()


def event_payload(event_id=None):
    timestamp = datetime(2026, 11, 28, 6, 18, 39, tzinfo=timezone.utc)
    return {
        "event_id": event_id or str(uuid4()),
        "event_type": "bank.transaction.created.v1",
        "event_version": 1,
        "occurred_at": timestamp.isoformat(),
        "transaction": {
            "transaction_id": "TXN-CURRENT-001",
            "sender_account_id": "ACC00000001",
            "receiver_account_id": "ACC00000002",
            "amount": "125.50",
            "currency": "INR",
            "transaction_type": "TRANSFER",
            "channel": "UPI",
            "status": "COMPLETED",
            "timestamp": timestamp.isoformat(),
        },
    }


def seed_ledger(session_factory):
    payload = event_payload()
    target = datetime.fromisoformat(payload["transaction"]["timestamp"])
    with session_factory.begin() as session:
        session.add(CustomerRow(
            customer_id="CUS00000001",
            full_name="Synthetic Customer",
            customer_type="INDIVIDUAL",
            city="Mohali",
            created_at=target - timedelta(days=60),
        ))
        session.flush()
        for account_id, account_type in (
            ("ACC00000001", "SAVINGS"),
            ("ACC00000002", "CURRENT"),
            ("ACC00000003", "SAVINGS"),
        ):
            session.add(AccountRow(
                account_id=account_id,
                account_number=f"0000000000{account_id[-1]}",
                customer_id="CUS00000001",
                account_type=account_type,
                currency="INR",
                balance=Decimal("10000.00"),
                status="ACTIVE",
                opened_at=target - timedelta(days=60),
            ))
        session.flush()
        session.add(TransactionRow(
            transaction_id="TXN-PRIOR-001",
            sender_account_id="ACC00000001",
            receiver_account_id="ACC00000003",
            amount=Decimal("50.00"),
            currency="INR",
            transaction_type="TRANSFER",
            channel="UPI",
            status="COMPLETED",
            timestamp=target - timedelta(minutes=8),
            is_fraud=False,
            is_aml_suspicious=False,
        ))
        session.add(TransactionRow(
            transaction_id="TXN-PRIOR-IN-001",
            sender_account_id="ACC00000003",
            receiver_account_id="ACC00000002",
            amount=Decimal("25.00"),
            currency="INR",
            transaction_type="TRANSFER",
            channel="NEFT",
            status="COMPLETED",
            timestamp=target - timedelta(minutes=4),
            is_fraud=False,
            is_aml_suspicious=False,
        ))
        session.add(TransactionRow(
            transaction_id="TXN-CURRENT-001",
            sender_account_id="ACC00000001",
            receiver_account_id="ACC00000002",
            amount=Decimal("125.50"),
            currency="INR",
            transaction_type="TRANSFER",
            channel="UPI",
            status="COMPLETED",
            timestamp=target,
            is_fraud=True,
            is_aml_suspicious=True,
            fraud_scenario="TEST_ONLY_LABEL",
            aml_typology="TEST_ONLY_TYPOLOGY",
        ))
    return payload


class FixedProbabilityModel:
    classes_ = [0, 1]

    def __init__(self, probability):
        self.probability = probability
        self.last_columns = None

    def predict_proba(self, frame):
        self.last_columns = tuple(frame.columns)
        assert self.last_columns == FEATURE_COLUMNS
        return [[1 - self.probability, self.probability]]


def row_count(session_factory, model):
    with session_factory() as session:
        return session.scalar(select(func.count()).select_from(model))


def test_online_features_match_feature_contract_and_exclude_labels(session_factory):
    payload = seed_ledger(session_factory)
    event = TransactionCreatedEvent.model_validate(payload)

    with session_factory() as session:
        features = build_online_fraud_features(session, event)

    assert tuple(features) == FEATURE_COLUMNS
    assert features["sender_txn_count_5m"] == 0
    assert features["sender_txn_count_15m"] == 1
    assert features["sender_amount_sum_24h"] == 50.0
    assert features["sender_unique_recipients_24h"] == 1
    assert features["receiver_txn_count_24h"] == 1
    assert "is_fraud" not in features
    assert "is_aml_suspicious" not in features
    assert "fraud_scenario" not in features
    assert "aml_typology" not in features


def test_consumer_persists_prediction_with_inbox_atomically(session_factory):
    payload = seed_ledger(session_factory)
    model = FixedProbabilityModel(0.8)
    inference = FraudInferenceService(model, 0.4, model_version="test-model")

    result = process_fraud_message(
        session_factory,
        topic="bank.transactions.v1",
        partition=0,
        offset=101,
        value=json.dumps(payload).encode(),
        inference_service=inference,
    )

    assert result == "stored"
    assert row_count(session_factory, FraudEventInboxRow) == 1
    assert row_count(session_factory, FraudPredictionRow) == 1
    with session_factory() as session:
        prediction = session.get(FraudPredictionRow, "TXN-CURRENT-001")
        assert prediction.event_id == payload["event_id"]
        assert prediction.model_version == "test-model"
        assert float(prediction.fraud_probability) == pytest.approx(0.8)
        assert float(prediction.decision_threshold) == pytest.approx(0.4)
        assert prediction.predicted_is_fraud is True
        assert tuple(prediction.feature_snapshot) == FEATURE_COLUMNS
        assert "is_fraud" not in prediction.feature_snapshot

    replay = process_fraud_message(
        session_factory,
        topic="bank.transactions.v1",
        partition=0,
        offset=101,
        value=json.dumps(payload).encode(),
        inference_service=inference,
    )
    assert replay == "duplicate"
    assert row_count(session_factory, FraudPredictionRow) == 1


def test_inference_failure_rolls_back_inbox_and_prediction(session_factory):
    payload = seed_ledger(session_factory)

    class FailingModel:
        classes_ = [0, 1]

        def predict_proba(self, frame):
            raise RuntimeError("model scoring failed")

    inference = FraudInferenceService(FailingModel(), 0.5)
    with pytest.raises(RuntimeError, match="model scoring failed"):
        process_fraud_message(
            session_factory,
            topic="bank.transactions.v1",
            partition=0,
            offset=102,
            value=json.dumps(payload).encode(),
            inference_service=inference,
        )

    assert row_count(session_factory, FraudEventInboxRow) == 0
    assert row_count(session_factory, FraudPredictionRow) == 0


def test_missing_canonical_transaction_fails_closed(session_factory):
    payload = event_payload()
    event = TransactionCreatedEvent.model_validate(payload)
    with session_factory() as session:
        with pytest.raises(ValueError, match="missing from the canonical ledger"):
            build_online_fraud_features(session, event)
