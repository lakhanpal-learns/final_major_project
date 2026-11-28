"""Point-in-time fraud inference against the canonical PostgreSQL ledger."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
from typing import Any

import joblib
import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.tables import AccountRow, FraudPredictionRow, TransactionRow
from app.models.events import TransactionCreatedEvent
from app.services.fraud_features import FEATURE_COLUMNS


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def build_online_fraud_features(
    session: Session,
    event: TransactionCreatedEvent,
) -> dict[str, Any]:
    """Build the offline feature contract from strictly earlier ledger events.

    The transaction must already exist in the canonical ledger because Kafka
    events are published from the transactional outbox after ledger commit.
    Scenario labels, fraud labels, AML labels and balance snapshots are never
    read or returned as model features.
    """
    tx = event.transaction
    current = session.get(TransactionRow, tx.transaction_id)
    if current is None:
        raise ValueError(
            f"Transaction {tx.transaction_id} is missing from the canonical ledger."
        )
    if (
        current.sender_account_id != tx.sender_account_id
        or current.receiver_account_id != tx.receiver_account_id
        or Decimal(current.amount) != tx.amount
        or current.currency != tx.currency
        or current.transaction_type != tx.transaction_type
        or current.channel != tx.channel
        or current.status != tx.status
        or _utc(current.timestamp) != _utc(tx.timestamp)
    ):
        raise ValueError(
            f"Kafka event does not match canonical ledger transaction {tx.transaction_id}."
        )

    sender = session.get(AccountRow, tx.sender_account_id)
    receiver = session.get(AccountRow, tx.receiver_account_id)
    if sender is None or receiver is None:
        raise ValueError("Transaction references an account missing from the canonical registry.")

    target_time = _utc(tx.timestamp)
    history_30d = session.scalars(
        select(TransactionRow)
        .where(
            TransactionRow.sender_account_id == tx.sender_account_id,
            TransactionRow.timestamp < tx.timestamp,
            TransactionRow.timestamp > target_time - timedelta(days=30),
            TransactionRow.status == "COMPLETED",
        )
        .order_by(TransactionRow.timestamp, TransactionRow.transaction_id)
    ).all()
    incoming_24h = session.scalars(
        select(TransactionRow)
        .where(
            TransactionRow.receiver_account_id == tx.receiver_account_id,
            TransactionRow.timestamp < tx.timestamp,
            TransactionRow.timestamp > target_time - timedelta(hours=24),
            TransactionRow.status == "COMPLETED",
        )
        .order_by(TransactionRow.timestamp, TransactionRow.transaction_id)
    ).all()

    prior_5m = []
    prior_15m = []
    prior_1h = []
    prior_24h = []
    prior_30d = []
    for row in history_30d:
        row_time = _utc(row.timestamp)
        if target_time - timedelta(minutes=5) < row_time < target_time:
            prior_5m.append(row)
        if target_time - timedelta(minutes=15) < row_time < target_time:
            prior_15m.append(row)
        if target_time - timedelta(hours=1) < row_time < target_time:
            prior_1h.append(row)
        if target_time - timedelta(hours=24) < row_time < target_time:
            prior_24h.append(row)
        if target_time - timedelta(days=30) < row_time < target_time:
            prior_30d.append(row)

    recipient_counts = Counter(row.receiver_account_id for row in prior_24h)
    max_recipient_count = max(recipient_counts.values(), default=0)
    amount = float(tx.amount)

    features = {
        "amount": amount,
        "log_amount": __import__("math").log1p(amount),
        "currency": tx.currency,
        "transaction_type": tx.transaction_type,
        "channel": tx.channel,
        "transaction_hour": target_time.hour,
        "day_of_week": target_time.weekday(),
        "sender_account_type": sender.account_type,
        "receiver_account_type": receiver.account_type,
        "sender_txn_count_5m": len(prior_5m),
        "sender_txn_count_15m": len(prior_15m),
        "sender_txn_count_1h": len(prior_1h),
        "sender_txn_count_24h": len(prior_24h),
        "sender_amount_sum_24h": sum(float(row.amount) for row in prior_24h),
        "sender_unique_recipients_1h": len({row.receiver_account_id for row in prior_1h}),
        "sender_unique_recipients_24h": len(recipient_counts),
        "sender_max_recipient_txn_count_24h": max_recipient_count,
        "sender_recipient_repeat_ratio_24h": (
            max_recipient_count / len(prior_24h) if prior_24h else 0.0
        ),
        "receiver_txn_count_24h": len(incoming_24h),
        "sender_avg_amount_30d": (
            sum(float(row.amount) for row in prior_30d) / len(prior_30d)
            if prior_30d else 0.0
        ),
    }
    if tuple(features) != FEATURE_COLUMNS:
        raise RuntimeError("Online fraud feature order does not match FEATURE_COLUMNS.")
    return features


class FraudInferenceService:
    """Load one frozen model and score events without retraining."""

    def __init__(
        self,
        model: Any,
        threshold: float,
        *,
        model_version: str = "fraud_random_forest_v2",
    ) -> None:
        if not 0 <= float(threshold) <= 1:
            raise ValueError("Fraud decision threshold must be between 0 and 1.")
        self.model = model
        self.threshold = float(threshold)
        self.model_version = model_version

    @classmethod
    def from_files(
        cls,
        model_path: str | Path,
        metrics_path: str | Path,
    ) -> "FraudInferenceService":
        model_file = Path(model_path)
        metrics_file = Path(metrics_path)
        if not model_file.is_file():
            raise FileNotFoundError(f"Fraud model artifact not found: {model_file}")
        if not metrics_file.is_file():
            raise FileNotFoundError(f"Fraud model metrics not found: {metrics_file}")
        metrics = json.loads(metrics_file.read_text(encoding="utf-8"))
        if tuple(metrics.get("feature_columns", ())) != FEATURE_COLUMNS:
            raise ValueError("Model metrics feature_columns do not match the online feature contract.")
        threshold = metrics.get("selected_threshold")
        if threshold is None:
            raise ValueError("Model metrics do not contain selected_threshold.")
        model = joblib.load(model_file)
        if not hasattr(model, "predict_proba"):
            raise TypeError("Fraud model artifact must expose predict_proba().")
        classes = getattr(model, "classes_", None)
        if classes is not None and list(classes) != [0, 1]:
            raise ValueError("Fraud model classes must be ordered as [0, 1].")
        return cls(model, float(threshold), model_version=model_file.stem)

    @classmethod
    def from_environment(cls) -> "FraudInferenceService":
        backend_dir = Path(__file__).resolve().parents[2]
        model_path = Path(os.getenv(
            "FRAUD_MODEL_PATH",
            str(backend_dir / "models" / "fraud" / "fraud_random_forest_v2.joblib"),
        ))
        metrics_path = Path(os.getenv(
            "FRAUD_MODEL_METRICS_PATH",
            str(backend_dir / "models" / "fraud" / "fraud_model_metrics_v2.json"),
        ))
        return cls.from_files(model_path, metrics_path)

    def predict_and_persist(
        self,
        session: Session,
        event: TransactionCreatedEvent,
    ) -> dict[str, Any]:
        """Score one transaction and add its prediction to the caller's DB transaction."""
        tx = event.transaction
        existing = session.get(FraudPredictionRow, tx.transaction_id)
        if existing is not None:
            if existing.event_id != str(event.event_id):
                raise ValueError(
                    f"Transaction {tx.transaction_id} already has a prediction for another event."
                )
            return {
                "transaction_id": tx.transaction_id,
                "fraud_probability": float(existing.fraud_probability),
                "predicted_is_fraud": bool(existing.predicted_is_fraud),
                "status": "existing",
            }

        feature_values = build_online_fraud_features(session, event)
        frame = pd.DataFrame([feature_values], columns=list(FEATURE_COLUMNS))
        probability = float(self.model.predict_proba(frame)[0][1])
        if not 0.0 <= probability <= 1.0:
            raise ValueError("Fraud model returned a probability outside [0, 1].")
        predicted = probability >= self.threshold

        session.add(FraudPredictionRow(
            transaction_id=tx.transaction_id,
            event_id=str(event.event_id),
            model_version=self.model_version,
            fraud_probability=Decimal(str(probability)),
            decision_threshold=Decimal(str(self.threshold)),
            predicted_is_fraud=predicted,
            feature_snapshot=feature_values,
            created_at=datetime.now(timezone.utc),
        ))
        session.flush()
        return {
            "transaction_id": tx.transaction_id,
            "fraud_probability": probability,
            "decision_threshold": self.threshold,
            "predicted_is_fraud": predicted,
            "model_version": self.model_version,
            "status": "predicted",
        }
