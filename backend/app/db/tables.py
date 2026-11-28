"""Relational table mappings for the canonical banking ledger and event outbox."""

from datetime import datetime
from decimal import Decimal

from sqlalchemy import Boolean, DateTime, ForeignKey, Integer, JSON, Numeric, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.db.database import Base


class CustomerRow(Base):
    __tablename__ = "customers"

    customer_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    full_name: Mapped[str] = mapped_column(String(160), nullable=False)
    customer_type: Mapped[str] = mapped_column(String(32), nullable=False)
    city: Mapped[str] = mapped_column(String(100), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class AccountRow(Base):
    __tablename__ = "accounts"

    account_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    account_number: Mapped[str] = mapped_column(String(32), nullable=False, unique=True)
    customer_id: Mapped[str] = mapped_column(
        String(32), ForeignKey("customers.customer_id"), nullable=False, index=True
    )
    account_type: Mapped[str] = mapped_column(String(32), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    balance: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class TransactionRow(Base):
    __tablename__ = "transactions"

    transaction_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    sender_account_id: Mapped[str] = mapped_column(
        String(32), ForeignKey("accounts.account_id"), nullable=False, index=True
    )
    receiver_account_id: Mapped[str] = mapped_column(
        String(32), ForeignKey("accounts.account_id"), nullable=False, index=True
    )
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    transaction_type: Mapped[str] = mapped_column(String(32), nullable=False)
    channel: Mapped[str] = mapped_column(String(20), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    sender_balance_before: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    sender_balance_after: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    receiver_balance_before: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    receiver_balance_after: Mapped[Decimal | None] = mapped_column(Numeric(18, 2), nullable=True)
    is_fraud: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_aml_suspicious: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    fraud_scenario: Mapped[str | None] = mapped_column(String(80), nullable=True)
    aml_typology: Mapped[str | None] = mapped_column(String(80), nullable=True)


class OutboxEventRow(Base):
    """Durable event awaiting publication to downstream messaging infrastructure."""

    __tablename__ = "outbox_events"

    event_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    aggregate_type: Mapped[str] = mapped_column(String(50), nullable=False)
    aggregate_id: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)



class FraudEventInboxRow(Base):
    """Durable inbox of validated transaction events received by fraud consumer."""

    __tablename__ = "fraud_event_inbox"

    event_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    transaction_id: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    event_version: Mapped[int] = mapped_column(Integer, nullable=False)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class FraudPredictionRow(Base):
    """Immutable fraud inference result for a canonical banking transaction."""

    __tablename__ = "fraud_predictions"

    transaction_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    event_id: Mapped[str] = mapped_column(String(36), nullable=False, unique=True)
    model_version: Mapped[str] = mapped_column(String(100), nullable=False)
    fraud_probability: Mapped[Decimal] = mapped_column(Numeric(10, 8), nullable=False)
    decision_threshold: Mapped[Decimal] = mapped_column(Numeric(10, 8), nullable=False)
    predicted_is_fraud: Mapped[bool] = mapped_column(Boolean, nullable=False, index=True)
    feature_snapshot: Mapped[dict] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class FraudEventRejectionRow(Base):
    """Durable quarantine for malformed events, keyed by Kafka source position."""

    __tablename__ = "fraud_event_rejections"
    __table_args__ = (
        UniqueConstraint(
            "source_topic", "source_partition", "source_offset",
            name="uq_fraud_rejection_source_position",
        ),
    )

    rejection_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    source_topic: Mapped[str] = mapped_column(String(249), nullable=False)
    source_partition: Mapped[int] = mapped_column(Integer, nullable=False)
    source_offset: Mapped[int] = mapped_column(Integer, nullable=False)
    raw_value: Mapped[str] = mapped_column(Text, nullable=False)
    error: Mapped[str] = mapped_column(Text, nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
