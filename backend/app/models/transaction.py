"""Transaction domain model for the shared synthetic bank ledger."""

from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class Transaction(BaseModel):
    """A synthetic transfer recorded in Sentinel's canonical ledger."""

    model_config = ConfigDict(frozen=True)

    transaction_id: str
    sender_account_id: str
    receiver_account_id: str
    amount: Decimal = Field(gt=0)
    currency: str = "INR"
    transaction_type: Literal["TRANSFER"] = "TRANSFER"
    channel: Literal["UPI", "IMPS", "NEFT", "RTGS", "INTERNAL"]
    status: Literal["COMPLETED"] = "COMPLETED"
    timestamp: datetime
    is_fraud: bool = False
    is_aml_suspicious: bool = False
    fraud_scenario: str | None = None
    aml_typology: str | None = None
