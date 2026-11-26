"""Canonical synthetic banking entities shared by Sentinel pipelines."""

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


class Customer(BaseModel):
    """Synthetic customer with a stable Sentinel-wide identifier."""

    model_config = ConfigDict(frozen=True)

    customer_id: str
    full_name: str
    customer_type: str
    city: str
    created_at: datetime


class Account(BaseModel):
    """Synthetic bank account owned by a canonical customer."""

    model_config = ConfigDict(frozen=True)

    account_id: str
    account_number: str
    customer_id: str
    account_type: str
    currency: str = "INR"
    balance: Decimal = Field(ge=0)
    status: str = "ACTIVE"
    opened_at: datetime
