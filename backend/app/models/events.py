"""Versioned Kafka event contracts consumed by Sentinel pipelines."""

from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class TransactionCreatedPayload(BaseModel):
    """Public transaction fields allowed in the v1 event contract."""

    model_config = ConfigDict(extra="forbid")

    transaction_id: str = Field(min_length=1, max_length=32)
    sender_account_id: str = Field(min_length=1, max_length=32)
    receiver_account_id: str = Field(min_length=1, max_length=32)
    amount: Decimal = Field(gt=0)
    currency: str = Field(min_length=3, max_length=3)
    transaction_type: Literal["TRANSFER"]
    channel: Literal["UPI", "IMPS", "NEFT", "RTGS", "INTERNAL"]
    status: Literal["COMPLETED"]
    timestamp: datetime


class TransactionCreatedEvent(BaseModel):
    """Strict schema for bank.transaction.created.v1."""

    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    event_type: Literal["bank.transaction.created.v1"]
    event_version: Literal[1]
    occurred_at: datetime
    transaction: TransactionCreatedPayload
