"""Deterministic, shared synthetic transaction generation for Sentinel.

The generator creates one mixed ledger: normal activity, fraud scenarios and
AML network typologies all use the same canonical accounts and time window.
Synthetic labels are ground truth for controlled experiments, not real findings.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN
import math
import random

from app.models.transaction import Transaction
from app.services.registry import BankingRegistry


_CHANNELS = ("UPI", "IMPS", "NEFT", "RTGS", "INTERNAL")
_FRAUD_SCENARIOS = (
    "HIGH_VALUE_BEHAVIORAL_DEVIATION",
    "RAPID_REPEATED_TRANSFERS",
    "UNUSUAL_TIME_CHANNEL_ACTIVITY",
    "MULTI_RECIPIENT_BURST",
    "LOW_VALUE_HIGH_VELOCITY",
    "REPEATED_BENEFICIARY_BURST",
)
_AML_TYPOLOGIES = (
    "STRUCTURING",
    "FAN_IN",
    "FAN_OUT",
    "LAYERING",
    "CIRCULAR_TRANSFER",
)
_CENT = Decimal("0.01")
_MAX_TRANSFER_SHARE = Decimal("0.25")


@dataclass(frozen=True)
class _BehaviorProfile:
    typical_amount: Decimal
    active_hour: int
    channels: tuple[str, ...]


@dataclass
class _Event:
    sender: str
    receiver: str
    requested_amount: Decimal
    timestamp: datetime
    channel: str
    tie_break: float
    is_fraud: bool = False
    is_aml: bool = False
    fraud_scenario: str | None = None
    aml_typology: str | None = None


def _profile_accounts(registry: BankingRegistry, rng: random.Random) -> dict[str, _BehaviorProfile]:
    """Create reproducible per-account behavior profiles for normal activity."""
    profiles: dict[str, _BehaviorProfile] = {}
    for account in registry.accounts:
        if account.account_type == "CURRENT":
            channels = ("NEFT", "RTGS", "IMPS", "INTERNAL")
            typical = Decimal(rng.randrange(8_000, 80_001))
        else:
            channels = ("UPI", "IMPS", "INTERNAL", "NEFT")
            typical = Decimal(rng.randrange(500, 30_001))
        profiles[account.account_id] = _BehaviorProfile(
            typical_amount=typical,
            active_hour=rng.choice((8, 9, 10, 12, 17, 18, 19, 20)),
            channels=channels,
        )
    return profiles


def _timestamp(
    base_time: datetime,
    rng: random.Random,
    profile: _BehaviorProfile | None = None,
) -> datetime:
    day = rng.randrange(30)
    if profile is None:
        hour = rng.randrange(24)
    else:
        hour = (profile.active_hour + rng.randint(-4, 4)) % 24
    return base_time + timedelta(
        days=day,
        hours=hour,
        minutes=rng.randrange(60),
        seconds=rng.randrange(60),
    )


def _event(
    sender: str,
    receiver: str,
    amount: int | Decimal,
    timestamp: datetime,
    rng: random.Random,
    *,
    channels: tuple[str, ...] = _CHANNELS,
    fraud: bool = False,
    aml: bool = False,
    fraud_scenario: str | None = None,
    aml_typology: str | None = None,
) -> _Event:
    if sender == receiver:
        raise ValueError("self-transfers are not allowed")
    return _Event(
        sender=sender,
        receiver=receiver,
        requested_amount=Decimal(amount).quantize(_CENT),
        timestamp=timestamp,
        channel=rng.choice(channels),
        tie_break=rng.random(),
        is_fraud=fraud,
        is_aml=aml,
        fraud_scenario=fraud_scenario,
        aml_typology=aml_typology,
    )


def _aml_group_sizes(
    count: int,
    rng: random.Random,
    min_size: int = 3,
    max_size: int = 6,
) -> list[int]:
    """Partition AML events into balanced network groups of three to six edges."""
    if count == 0:
        return []
    group_count = math.ceil(count / max_size)
    base_size, remainder = divmod(count, group_count)
    if base_size < min_size:
        raise ValueError("AML groups must contain at least three events")
    # Balanced deterministic groups make each typology's intended motif
    # reproducible; the RNG parameter remains for call-site compatibility.
    return [base_size + 1] * remainder + [base_size] * (group_count - remainder)


def generate_transactions(
    registry: BankingRegistry,
    normal_count: int = 500,
    fraud_count: int = 25,
    aml_count: int = 25,
    seed: int = 99,
    overlap_count: int | None = None,
) -> list[Transaction]:
    """Generate one mixed, reproducible ledger over the canonical registry.

    normal_count counts transactions with neither label. fraud_count and
    aml_count count positive labels, including any overlapping events. By
    default one event is cross-labelled when both positive classes exist.
    AML patterns are generated in groups of at least three transactions.
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
    if aml_count in (1, 2):
        raise ValueError("aml_count must be 0 or at least 3 to form a network pattern")
    if aml_count and len(registry.accounts) < 6:
        raise ValueError("at least six accounts are required for AML network scenarios")

    if overlap_count is None:
        overlap_count = 1 if fraud_count and aml_count else 0
    if overlap_count < 0 or overlap_count > min(fraud_count, aml_count):
        raise ValueError("overlap_count must be between zero and both positive label counts")

    rng = random.Random(seed)
    accounts = list(registry.accounts)
    account_ids = [account.account_id for account in accounts]
    profiles = _profile_accounts(registry, rng)
    base_time = datetime(2025, 1, 1, tzinfo=timezone.utc)
    events: list[_Event] = []

    # Normal activity: profile-influenced amounts, channels and active hours.
    # High-value and legitimate fan-in/fan-out lookalikes are hard negatives.
    normal_hub: str | None = None
    normal_group_time: datetime | None = None
    normal_group_is_fan_in = True
    for index in range(normal_count):
        group_offset = index % 20
        if group_offset < 3:
            if group_offset == 0:
                normal_hub = rng.choice(account_ids)
                normal_group_time = base_time + timedelta(
                    days=rng.randrange(30), hours=rng.randrange(24), minutes=rng.randrange(60)
                )
                normal_group_is_fan_in = (index // 20) % 2 == 0
            assert normal_hub is not None and normal_group_time is not None
            other = rng.choice([item for item in account_ids if item != normal_hub])
            if normal_group_is_fan_in:
                sender_id, receiver_id = other, normal_hub
            else:
                sender_id, receiver_id = normal_hub, other
            profile = profiles[sender_id]
            timestamp = normal_group_time + timedelta(minutes=group_offset)
        else:
            sender, receiver = rng.sample(accounts, 2)
            sender_id, receiver_id = sender.account_id, receiver.account_id
            profile = profiles[sender_id]
            timestamp = _timestamp(base_time, rng, profile)

        if rng.random() < 0.08:
            amount = Decimal(rng.randrange(50_000, 350_001))
        else:
            sampled = math.exp(rng.gauss(math.log(float(profile.typical_amount)), 0.8))
            amount = Decimal(str(min(200_000, max(100, round(sampled)))))
        events.append(
            _event(
                sender_id,
                receiver_id,
                amount,
                timestamp,
                rng,
                channels=profile.channels,
            )
        )

    # AML patterns are multi-edge groups, not isolated suspicious transactions.
    aml_events: list[_Event] = []
    group_number = 0
    for group_size in _aml_group_sizes(aml_count, rng):
        typology = _AML_TYPOLOGIES[group_number % len(_AML_TYPOLOGIES)]
        group_number += 1
        group_time = base_time + timedelta(
            days=rng.randrange(30), hours=rng.randrange(24), minutes=rng.randrange(60)
        )
        if typology == "CIRCULAR_TRANSFER":
            chosen = rng.sample(account_ids, group_size)
            pairs = [
                (chosen[index], chosen[(index + 1) % group_size])
                for index in range(group_size)
            ]
        else:
            chosen = rng.sample(account_ids, group_size + 1)
            if typology == "STRUCTURING":
                pairs = [(chosen[0], destination) for destination in chosen[1:]]
            elif typology == "FAN_IN":
                pairs = [(source, chosen[-1]) for source in chosen[:-1]]
            elif typology == "FAN_OUT":
                pairs = [(chosen[0], destination) for destination in chosen[1:]]
            else:
                pairs = [(chosen[index], chosen[index + 1]) for index in range(group_size)]

        pass_through_base = rng.randrange(20_000, 24_001)
        for offset, (sender, receiver) in enumerate(pairs):
            if typology == "STRUCTURING":
                # Four or more modest transfers create an aggregate pattern.
                amount = rng.randrange(25_000, 45_001)
            elif typology in ("FAN_IN", "FAN_OUT"):
                # AML hub motifs have five meaningful-value edges; normal
                # hard-negative hub motifs deliberately contain only three.
                amount = rng.randrange(45_000, 100_001)
            else:
                # Layering and cycle edges preserve most of the transferred
                # value, giving the rules a measurable pass-through signal.
                amount = int(Decimal(pass_through_base) * (Decimal("0.97") ** offset))
            aml_events.append(
                _event(
                    sender,
                    receiver,
                    amount,
                    group_time + timedelta(minutes=offset),
                    rng,
                    aml=True,
                    aml_typology=typology,
                )
            )

    # Cross-label AML network events to create the 1/1 label combination.
    for item in aml_events[:overlap_count]:
        item.is_fraud = True
        item.fraud_scenario = "CROSS_DOMAIN_ACCOUNT_MISUSE"
    events.extend(aml_events)

    # Fraud-only events are grouped to represent velocity and beneficiary bursts.
    remaining = fraud_count - overlap_count
    fraud_group_number = 0
    while remaining:
        group_size = min(rng.randint(2, 6), remaining)
        scenario = _FRAUD_SCENARIOS[fraud_group_number % len(_FRAUD_SCENARIOS)]
        fraud_group_number += 1
        sender = rng.choice(account_ids)
        available_recipients = [item for item in account_ids if item != sender]
        if scenario == "REPEATED_BENEFICIARY_BURST":
            beneficiary = rng.choice(available_recipients)
            recipients = [beneficiary] * group_size
        elif group_size <= len(available_recipients):
            recipients = rng.sample(available_recipients, group_size)
        else:
            recipients = [rng.choice(available_recipients) for _ in range(group_size)]
        profile = profiles[sender]
        start_time = _timestamp(base_time, rng, profile)
        for offset, receiver in enumerate(recipients):
            if scenario == "HIGH_VALUE_BEHAVIORAL_DEVIATION":
                amount = min(400_000, max(20_000, int(profile.typical_amount) * rng.randint(3, 8)))
            elif scenario == "RAPID_REPEATED_TRANSFERS":
                amount = rng.randrange(2_000, 80_001)
            elif scenario == "UNUSUAL_TIME_CHANNEL_ACTIVITY":
                amount = rng.randrange(5_000, 250_001)
            elif scenario == "LOW_VALUE_HIGH_VELOCITY":
                amount = rng.randrange(100, 2_001)
            elif scenario == "REPEATED_BENEFICIARY_BURST":
                amount = rng.randrange(2_000, 90_001)
            else:
                amount = rng.randrange(3_000, 120_001)
            event_time = start_time + timedelta(minutes=offset * rng.randrange(1, 4))
            event_channels = _CHANNELS
            if scenario == "UNUSUAL_TIME_CHANNEL_ACTIVITY":
                unusual_hour = (profile.active_hour + rng.choice((7, 8, 9, 10))) % 24
                event_time = start_time.replace(hour=unusual_hour)
                event_channels = tuple(channel for channel in _CHANNELS if channel not in profile.channels)
            events.append(
                _event(
                    sender,
                    receiver,
                    amount,
                    event_time,
                    rng,
                    channels=event_channels or _CHANNELS,
                    fraud=True,
                    fraud_scenario=scenario,
                )
            )
        remaining -= group_size

    # Mix all classes in one time-ordered ledger. Random tie-breaking prevents
    # append order from exposing generation class to downstream consumers.
    events.sort(key=lambda item: (item.timestamp, item.tie_break))
    balances = {account.account_id: account.balance for account in accounts}
    transactions: list[Transaction] = []
    used_ids: set[str] = set()

    for item in events:
        sender_before = balances[item.sender]
        receiver_before = balances[item.receiver]
        # Cap each transfer to a fraction of available funds to prevent overdraft.
        amount = min(item.requested_amount, sender_before * _MAX_TRANSFER_SHARE)
        amount = amount.quantize(_CENT, rounding=ROUND_DOWN)
        if amount < _CENT:
            amount = sender_before.quantize(_CENT, rounding=ROUND_DOWN)
        if amount <= 0:
            raise ValueError("account balance is too small to complete a transfer")

        sender_after = (sender_before - amount).quantize(_CENT)
        receiver_after = (receiver_before + amount).quantize(_CENT)
        balances[item.sender] = sender_after
        balances[item.receiver] = receiver_after

        transaction_id = f"TXN{rng.getrandbits(64):016X}"
        while transaction_id in used_ids:
            transaction_id = f"TXN{rng.getrandbits(64):016X}"
        used_ids.add(transaction_id)

        transactions.append(
            Transaction(
                transaction_id=transaction_id,
                sender_account_id=item.sender,
                receiver_account_id=item.receiver,
                amount=amount,
                channel=item.channel,
                timestamp=item.timestamp,
                sender_balance_before=sender_before,
                sender_balance_after=sender_after,
                receiver_balance_before=receiver_before,
                receiver_balance_after=receiver_after,
                is_fraud=item.is_fraud,
                is_aml_suspicious=item.is_aml,
                fraud_scenario=item.fraud_scenario,
                aml_typology=item.aml_typology,
            )
        )

    return transactions
