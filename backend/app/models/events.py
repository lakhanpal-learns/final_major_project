"""Versioned Kafka event contracts consumed by Sentinel pipelines."""

import re
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


def _normalize_timezone_offset(value: Any) -> Any:
    """Normalize PostgreSQL's short UTC offset (+00) to ISO-8601 (+00:00)."""
    if isinstance(value, str):
        return re.sub(r"([+-][0-9]{2})$", r"\1:00", value)
    return value


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

    @field_validator("timestamp", mode="before")
    @classmethod
    def normalize_timestamp_offset(cls, value: Any) -> Any:
        return _normalize_timezone_offset(value)

    @model_validator(mode="after")
    def validate_distinct_accounts(self):
        if self.sender_account_id == self.receiver_account_id:
            raise ValueError("sender and receiver accounts must be different")
        return self


class TransactionCreatedEvent(BaseModel):
    """Strict schema for bank.transaction.created.v1."""

    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    event_type: Literal["bank.transaction.created.v1"]
    event_version: Literal[1]
    occurred_at: datetime
    transaction: TransactionCreatedPayload

    @field_validator("occurred_at", mode="before")
    @classmethod
    def normalize_occurred_at_offset(cls, value: Any) -> Any:
        return _normalize_timezone_offset(value)
