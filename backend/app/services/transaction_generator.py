"""Deterministic synthetic transaction generation over the canonical registry."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import random

from app.models.transaction import Transaction
from app.services.registry import BankingRegistry


_CHANNELS = ("UPI", "IMPS", "NEFT", "RTGS", "INTERNAL")


def generate_transactions(
    registry: BankingRegistry,
    normal_count: int = 500,
    fraud_count: int = 25,
    aml_count: int = 25,
    seed: int = 99,
) -> list[Transaction]:
    """Generate normal transfers and explicitly labelled synthetic scenarios.

    Labels are generator ground truth for evaluation, not predictions from a
    trained model. AML scenario events are arranged as simple graph patterns.
    """
    for name, count in (
        ("normal_count", normal_count),
        ("fraud_count", fraud_count),
        ("aml_count", aml_count),
    ):
        if count < 0:
            raise ValueError(f"{name} cannot be negative")
    if len(registry.accounts) < 2:
        raise ValueError("registry must contain at least two accounts")
    if aml_count >= 3 and len(registry.accounts) < 4:
        raise ValueError("at least four accounts are required for AML graph scenarios")

    rng = random.Random(seed)
    accounts = list(registry.accounts)
    base_time = datetime(2025, 1, 1, tzinfo=timezone.utc)
    transactions: list[Transaction] = []
    sequence = 1

    def add(
        sender: str,
        receiver: str,
        amount: Decimal,
        timestamp: datetime,
        *,
        fraud: bool = False,
        aml: bool = False,
        fraud_scenario: str | None = None,
        aml_typology: str | None = None,
    ) -> None:
        nonlocal sequence
        if sender == receiver:
            raise ValueError("self-transfers are not allowed")
        transactions.append(
            Transaction(
                transaction_id=f"TXN{sequence:010d}",
                sender_account_id=sender,
                receiver_account_id=receiver,
                amount=amount.quantize(Decimal("0.01")),
                channel=rng.choice(_CHANNELS),
                timestamp=timestamp,
                is_fraud=fraud,
                is_aml_suspicious=aml,
                fraud_scenario=fraud_scenario,
                aml_typology=aml_typology,
            )
        )
        sequence += 1

    for index in range(normal_count):
        sender, receiver = rng.sample(accounts, 2)
        add(
            sender.account_id,
            receiver.account_id,
            Decimal(rng.randint(100, 150_000)),
            base_time + timedelta(minutes=index * 3 + rng.randint(0, 2)),
        )

    fraud_pairs = [rng.sample(accounts, 2) for _ in range(fraud_count)]
    for index, (sender, receiver) in enumerate(fraud_pairs):
        add(
            sender.account_id,
            receiver.account_id,
            Decimal(rng.randint(250_000, 900_000)),
            base_time + timedelta(days=2, minutes=index),
            fraud=True,
            fraud_scenario="HIGH_VALUE_UNUSUAL_TRANSFER",
        )

    aml_remaining = aml_count
    aml_group = 0
    while aml_remaining > 0:
        aml_group += 1
        group_size = min(aml_remaining, 4)
        chosen = rng.sample(accounts, min(len(accounts), max(3, group_size + 1)))
        event_time = base_time + timedelta(days=3, minutes=aml_group * 10)

        if group_size == 1:
            sender, receiver = chosen[:2]
            add(sender.account_id, receiver.account_id, Decimal("98000"),
                event_time, aml=True, aml_typology="STRUCTURING")
        elif group_size == 2:
            source, destination = chosen[:2]
            for offset in range(2):
                add(source.account_id, destination.account_id, Decimal("99000"),
                    event_time + timedelta(minutes=offset), aml=True,
                    aml_typology="STRUCTURING")
        elif group_size == 3:
            intermediary = chosen[0]
            for source in chosen[1:4]:
                add(source.account_id, intermediary.account_id, Decimal("45000"),
                    event_time, aml=True, aml_typology="FAN_IN")
        else:
            source_a, source_b, intermediary, sink = chosen[:4]
            add(source_a.account_id, intermediary.account_id, Decimal("70000"),
                event_time, aml=True, aml_typology="FAN_IN")
            add(source_b.account_id, intermediary.account_id, Decimal("65000"),
                event_time + timedelta(minutes=1), aml=True, aml_typology="FAN_IN")
            add(intermediary.account_id, sink.account_id, Decimal("130000"),
                event_time + timedelta(minutes=2), aml=True, aml_typology="LAYERING")
            add(sink.account_id, source_a.account_id, Decimal("125000"),
                event_time + timedelta(minutes=3), aml=True, aml_typology="CIRCULAR_TRANSFER")
        aml_remaining -= group_size

    return transactions
