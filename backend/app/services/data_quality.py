"""Validation and summary checks for generated Sentinel transaction ledgers."""

from collections import Counter
from typing import Iterable

from app.models.transaction import Transaction
from app.services.registry import BankingRegistry


# Candidate fields for future model features. Derived hour/day fields and
# account types must be materialized by feature engineering, not read as labels.
MODEL_FEATURE_ALLOWLIST = (
    "amount",
    "channel",
    "transaction_type",
    "hour_of_day",
    "day_of_week",
    "sender_account_type",
    "receiver_account_type",
    "sender_balance_before",
    "receiver_balance_before",
)

# Never pass identifiers, ground-truth labels, or scenario metadata to a model.
MODEL_EXCLUDED_FIELDS = (
    "transaction_id",
    "sender_account_id",
    "receiver_account_id",
    "is_fraud",
    "is_aml_suspicious",
    "fraud_scenario",
    "aml_typology",
)


def audit_ledger(
    registry: BankingRegistry,
    transactions: Iterable[Transaction],
) -> dict[str, object]:
    """Validate references, labels, balance arithmetic and report label counts.

    This checks generator consistency; it does not establish real-world
    representativeness, fairness, or the truth of synthetic labels.
    """
    account_ids = {account.account_id for account in registry.accounts}
    seen_ids: set[str] = set()
    label_counts: Counter[str] = Counter()
    total = 0

    for transaction in transactions:
        total += 1
        if transaction.transaction_id in seen_ids:
            raise ValueError(f"duplicate transaction ID: {transaction.transaction_id}")
        seen_ids.add(transaction.transaction_id)

        if transaction.sender_account_id not in account_ids:
            raise ValueError(f"unknown sender account: {transaction.sender_account_id}")
        if transaction.receiver_account_id not in account_ids:
            raise ValueError(f"unknown receiver account: {transaction.receiver_account_id}")
        if transaction.sender_account_id == transaction.receiver_account_id:
            raise ValueError("self-transfers are not allowed")
        if transaction.timestamp.tzinfo is None:
            raise ValueError("transaction timestamps must be timezone-aware")

        if transaction.is_fraud != (transaction.fraud_scenario is not None):
            raise ValueError("fraud label and fraud scenario metadata are inconsistent")
        if transaction.is_aml_suspicious != (transaction.aml_typology is not None):
            raise ValueError("AML label and AML typology metadata are inconsistent")

        sender_before = transaction.sender_balance_before
        sender_after = transaction.sender_balance_after
        receiver_before = transaction.receiver_balance_before
        receiver_after = transaction.receiver_balance_after
        if any(value is None for value in (
            sender_before, sender_after, receiver_before, receiver_after
        )):
            raise ValueError("generated transactions must include balance snapshots")
        if sender_before is None or sender_after is None:
            raise ValueError("sender balance snapshots are required")
        if receiver_before is None or receiver_after is None:
            raise ValueError("receiver balance snapshots are required")
        if sender_before < transaction.amount:
            raise ValueError("transaction overdraws the sender account")
        if sender_before - transaction.amount != sender_after:
            raise ValueError("sender balance arithmetic is inconsistent")
        if receiver_before + transaction.amount != receiver_after:
            raise ValueError("receiver balance arithmetic is inconsistent")

        label_key = (
            f"fraud_{int(transaction.is_fraud)}_aml_{int(transaction.is_aml_suspicious)}"
        )
        label_counts[label_key] += 1

    return {
        "transaction_count": total,
        "normal_only": label_counts["fraud_0_aml_0"],
        "fraud_only": label_counts["fraud_1_aml_0"],
        "aml_only": label_counts["fraud_0_aml_1"],
        "fraud_and_aml": label_counts["fraud_1_aml_1"],
        "label_combinations": dict(sorted(label_counts.items())),
    }
