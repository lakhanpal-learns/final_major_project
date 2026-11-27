"""Relational table mappings for the canonical banking ledger."""

from datetime import datetime
from decimal import Decimal

from sqlalchemy import Boolean, DateTime, ForeignKey, Numeric, String
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
