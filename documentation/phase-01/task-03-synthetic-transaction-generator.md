# Phase 01 — Task 03: Shared Synthetic Transaction Generator Redesign

**Status:** Code and tests committed; local execution is pending  
**Design choice:** Option A — custom PaySim-inspired and AMLSim-inspired synthetic generation. No original PaySim or IBM AMLSim records are imported.

## 1. Objective

Create one deterministic synthetic bank environment that generates normal activity, fraud scenarios and AML network typologies using the same canonical customers and accounts. Both downstream pipelines consume the same transaction ledger.

## 2. Files and responsibilities

| File | Purpose |
|---|---|
| `backend/app/services/registry.py` | Builds canonical customer/account records and deterministic synthetic opening balances. |
| `backend/app/models/transaction.py` | Defines transaction fields, independent labels and balance snapshots. |
| `backend/app/services/transaction_generator.py` | Creates and mixes normal, fraud and AML events; simulates balances; assigns non-sequential IDs. |
| `backend/app/services/data_quality.py` | Validates generated ledger consistency and declares feature candidates/exclusions. |
| `backend/tests/test_transaction_generator.py` | Tests counts, account references, reproducibility, label overlap, timestamps, balances, AML patterns and legitimate hard negatives. |
| `backend/tests/test_data_quality.py` | Tests all four label combinations, metadata consistency and feature leakage safeguards. |

## 3. Generation design

### Normal activity

- Each account receives a deterministic synthetic behavior profile based on account type.
- Profiles influence typical transaction amount, preferred channel set and active hours.
- Amounts use a skewed distribution rather than a uniform random range.
- Approximately 8% of normal events are high-value hard negatives in expectation; this is a generator parameter, not a measured bank prevalence.
- Every twentieth normal event group begins a three-transfer legitimate fan-in/fan-out-like pattern. These rows remain 0/0 labels and help test whether a model overreacts to network shape alone.

### Fraud scenarios

| Scenario | Generated pattern |
|---|---|
| `HIGH_VALUE_BEHAVIORAL_DEVIATION` | Larger requested transfer amounts, overlapping the normal amount range. |
| `RAPID_REPEATED_TRANSFERS` | Multiple transfers from one sender to different recipients in a short interval. |
| `UNUSUAL_TIME_CHANNEL_ACTIVITY` | Transfers at a time and through a channel outside the account profile's preferred behavior. |
| `MULTI_RECIPIENT_BURST` | A short burst of transfers from one sender to several recipients. |

These are transaction-level synthetic proxies. The current schema does not simulate login events, device fingerprints, IP addresses or historical beneficiary onboarding. It therefore cannot establish a true account takeover or a genuinely new beneficiary.

### AML network typologies

| Typology | Generated graph structure |
|---|---|
| Structuring | One sender makes multiple split transfers to distinct recipients. |
| Fan-in | Multiple source accounts transfer to one hub. |
| Fan-out | One hub distributes funds to multiple destinations. |
| Layering | Funds move along a multi-edge chain of intermediary accounts. |
| Circular transfer | Transfers form a directed cycle. |

AML scenarios are groups of at least three transactions. A single transfer is not labelled as structuring. Amounts are illustrative simulation values, not legal or regulatory thresholds.

### Independent labels and overlap

Fraud and AML ground-truth labels are separate booleans. The generator supports all four combinations:

| `is_fraud` | `is_aml_suspicious` | Meaning |
|---:|---:|---|
| 0 | 0 | Normal-only transaction |
| 1 | 0 | Fraud-only transaction |
| 0 | 1 | AML-only transaction |
| 1 | 1 | Cross-labelled transaction |

The default overlap is one transaction when both positive class counts are non-zero. `overlap_count` can be set explicitly. `normal_count` counts only 0/0 transactions; `fraud_count` and `aml_count` count positive labels including overlap. Total rows equal `normal_count + fraud_count + aml_count - overlap_count`. `aml_count` must be zero or at least three so an AML label represents a multi-transaction pattern.

### Shared time, IDs and balances

- All event types receive timestamps across the same 30-day synthetic window and are merged into one chronological ledger.
- Seeded random tie-breaking mixes events that share a timestamp.
- Transaction IDs use seeded random hexadecimal values instead of a sequential counter.
- Both pipelines use the same canonical account IDs.
- Account registry balances are opening balances. Each transaction stores sender/receiver balance-before and balance-after snapshots.
- Each transfer is capped at 25% of the sender's available balance. The account table still stores opening balance; a persisted end-of-period account balance projection is not implemented.

## 4. Leakage controls

`MODEL_FEATURE_ALLOWLIST` defines candidate future inputs: amount, channel, transaction type, hour/day derived from timestamp, sender/receiver account type and event-time balance snapshots.

`MODEL_EXCLUDED_FIELDS` excludes transaction IDs, account IDs, fraud/AML labels and scenario metadata. Future training code must use the allowlist and must never pass labels or scenario descriptions as input features.

Use time-based holdouts plus account/scenario holdouts. A random row split alone can put related events from the same generated pattern into both training and test sets.

## 5. Data-quality audit

`audit_ledger()` checks unique transaction IDs, canonical account references, self-transfers, timezone-aware timestamps, label/metadata consistency, required balance snapshots, overdrafts and balance arithmetic. It reports all four label-combination counts.

These checks validate internal consistency only. They do not prove real-world representativeness, fairness or production model accuracy.

## 6. Complete source and test code

The code below is copied from the current committed source files. Each listing is the implementation referenced by this task.

### backend/app/services/registry.py

**Purpose:** Creates stable canonical customer/account IDs and deterministic synthetic opening balances.

```python
"""Generate one deterministic customer/account registry for all Sentinel pipelines."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import random

from app.models.banking import Account, Customer


_NAMES = (
    "Aarav Sharma", "Vivaan Patel", "Aditya Singh", "Vihaan Kumar",
    "Arjun Gupta", "Reyansh Verma", "Aanya Shah", "Diya Mehta",
    "Anaya Reddy", "Ira Nair", "Kabir Malhotra", "Ishaan Joshi",
    "Mira Kapoor", "Saanvi Rao", "Rohan Das", "Priya Iyer",
)
_CITIES = ("Ludhiana", "Delhi", "Mumbai", "Pune", "Jaipur", "Bengaluru", "Chennai")
_ACCOUNT_TYPES = ("SAVINGS", "CURRENT")


@dataclass(frozen=True)
class BankingRegistry:
    """In-memory canonical registry; persistence is added in a later task."""

    customers: tuple[Customer, ...]
    accounts: tuple[Account, ...]

    def get_customer(self, customer_id: str) -> Customer | None:
        return next((item for item in self.customers if item.customer_id == customer_id), None)

    def get_account(self, account_id: str) -> Account | None:
        return next((item for item in self.accounts if item.account_id == account_id), None)


def build_registry(customer_count: int = 100, seed: int = 42) -> BankingRegistry:
    """Build repeatable synthetic customers and accounts.

    Every customer receives one account; every tenth customer receives a second
    account. IDs are generated by Sentinel and shared by fraud and AML flows.
    Opening balances are deliberately broad synthetic starting values so the
    transaction simulator can evolve balances without overdrawing accounts.
    """
    if customer_count < 1:
        raise ValueError("customer_count must be at least 1")

    rng = random.Random(seed)
    base_time = datetime(2024, 1, 1, tzinfo=timezone.utc)
    customers: list[Customer] = []
    accounts: list[Account] = []

    for index in range(1, customer_count + 1):
        customer_id = f"CUST{index:06d}"
        customers.append(
            Customer(
                customer_id=customer_id,
                full_name=_NAMES[(index - 1) % len(_NAMES)],
                customer_type="INDIVIDUAL",
                city=_CITIES[(index - 1) % len(_CITIES)],
                created_at=base_time + timedelta(days=rng.randint(0, 900)),
            )
        )

        account_count = 2 if index % 10 == 0 else 1
        for account_index in range(1, account_count + 1):
            account_sequence = len(accounts) + 1
            accounts.append(
                Account(
                    account_id=f"ACC{account_sequence:08d}",
                    account_number=f"{rng.randrange(10**11, 10**12):012d}",
                    customer_id=customer_id,
                    account_type=_ACCOUNT_TYPES[(account_sequence - 1) % len(_ACCOUNT_TYPES)],
                    balance=Decimal(rng.randrange(10_000_000, 1_000_000_001)) / Decimal("100"),
                    opened_at=base_time + timedelta(days=rng.randint(0, 900)),
                )
            )

    return BankingRegistry(customers=tuple(customers), accounts=tuple(accounts))

```

### backend/app/models/transaction.py

**Purpose:** Defines the immutable transaction record, independent fraud/AML labels, scenario metadata and event-time balance snapshots.

```python
"""Transaction domain model for the shared synthetic bank ledger."""

from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class Transaction(BaseModel):
    """A synthetic transfer with independent fraud/AML ground-truth labels.

    Balance fields are event-time snapshots. They are generated by the ledger
    simulator and are not model predictions.
    """

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
    sender_balance_before: Decimal | None = Field(default=None, ge=0)
    sender_balance_after: Decimal | None = Field(default=None, ge=0)
    receiver_balance_before: Decimal | None = Field(default=None, ge=0)
    receiver_balance_after: Decimal | None = Field(default=None, ge=0)
    is_fraud: bool = False
    is_aml_suspicious: bool = False
    fraud_scenario: str | None = None
    aml_typology: str | None = None

```

### backend/app/services/transaction_generator.py

**Purpose:** Generates normal activity, legitimate network hard negatives, fraud scenarios and AML network patterns into one reproducible ledger.

```python
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


def _aml_group_sizes(count: int, max_size: int = 5) -> list[int]:
    """Partition AML events into groups of at least three transactions."""
    sizes: list[int] = []
    remaining = count
    while remaining:
        size = min(max_size, remaining)
        remainder = remaining - size
        if remainder in (1, 2):
            size -= 3 - remainder
        sizes.append(size)
        remaining -= size
    return sizes


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
    for group_size in _aml_group_sizes(aml_count):
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

        for offset, (sender, receiver) in enumerate(pairs):
            if typology == "STRUCTURING":
                amount = rng.randrange(15_000, 95_001)
            else:
                amount = rng.randrange(8_000, 180_001)
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
        group_size = min(4, remaining)
        scenario = _FRAUD_SCENARIOS[fraud_group_number % len(_FRAUD_SCENARIOS)]
        fraud_group_number += 1
        sender = rng.choice(account_ids)
        available_recipients = [item for item in account_ids if item != sender]
        if group_size <= len(available_recipients):
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

```

### backend/app/services/data_quality.py

**Purpose:** Audits ledger references, label metadata and balance arithmetic; defines candidate model features and fields excluded to prevent leakage.

```python
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

```

### backend/tests/test_transaction_generator.py

**Purpose:** Tests generator counts, canonical account references, reproducibility, independent labels, balance snapshots, AML typologies, normal hard negatives and small-registry behavior.

```python
import pytest

from app.services.registry import build_registry
from app.services.transaction_generator import generate_transactions


def test_generator_creates_requested_label_counts_with_overlap() -> None:
    registry = build_registry(customer_count=30, seed=3)
    transactions = generate_transactions(
        registry,
        normal_count=40,
        fraud_count=5,
        aml_count=6,
        seed=11,
        overlap_count=1,
    )

    assert len(transactions) == 50
    assert sum(item.is_fraud for item in transactions) == 5
    assert sum(item.is_aml_suspicious for item in transactions) == 6
    assert sum(not item.is_fraud and not item.is_aml_suspicious for item in transactions) == 40
    assert sum(item.is_fraud and item.is_aml_suspicious for item in transactions) == 1


def test_every_transaction_uses_registry_accounts_and_no_self_transfer() -> None:
    registry = build_registry(customer_count=25)
    known_accounts = {item.account_id for item in registry.accounts}
    transactions = generate_transactions(registry, 20, 4, 8)

    assert all(item.sender_account_id in known_accounts for item in transactions)
    assert all(item.receiver_account_id in known_accounts for item in transactions)
    assert all(item.sender_account_id != item.receiver_account_id for item in transactions)


def test_transaction_ids_are_unique_and_generation_is_reproducible() -> None:
    registry = build_registry(customer_count=20, seed=5)
    first = generate_transactions(registry, 25, 5, 8, seed=77)
    second = generate_transactions(registry, 25, 5, 8, seed=77)

    assert len({item.transaction_id for item in first}) == len(first)
    assert [item.model_dump() for item in first] == [item.model_dump() for item in second]
    assert not first[0].transaction_id.endswith("0000000001")


def test_fraud_and_aml_have_independent_scenario_metadata() -> None:
    registry = build_registry(customer_count=30)
    transactions = generate_transactions(registry, 10, 8, 10, overlap_count=2)

    fraud_events = [item for item in transactions if item.is_fraud]
    aml_events = [item for item in transactions if item.is_aml_suspicious]
    both_events = [item for item in transactions if item.is_fraud and item.is_aml_suspicious]

    assert len(fraud_events) == 8
    assert len(aml_events) == 10
    assert len(both_events) == 2
    assert all(item.fraud_scenario is not None for item in fraud_events)
    assert all(item.aml_typology is not None for item in aml_events)
    assert all(item.aml_typology is None for item in fraud_events if not item.is_aml_suspicious)


def test_generator_mixes_timestamps_and_creates_balance_snapshots() -> None:
    registry = build_registry(customer_count=30, seed=31)
    transactions = generate_transactions(registry, 100, 20, 15, seed=32)

    assert transactions == sorted(transactions, key=lambda item: item.timestamp)
    assert (transactions[-1].timestamp - transactions[0].timestamp).days >= 10
    for item in transactions:
        assert item.sender_balance_before is not None
        assert item.sender_balance_after is not None
        assert item.receiver_balance_before is not None
        assert item.receiver_balance_after is not None
        assert item.sender_balance_before >= item.amount
        assert item.sender_balance_before - item.amount == item.sender_balance_after
        assert item.receiver_balance_before + item.amount == item.receiver_balance_after


def test_aml_generator_covers_multiple_network_typologies() -> None:
    registry = build_registry(customer_count=40, seed=8)
    transactions = generate_transactions(registry, 100, 10, 25, seed=9)
    typologies = {item.aml_typology for item in transactions if item.is_aml_suspicious}

    assert {
        "STRUCTURING", "FAN_IN", "FAN_OUT", "LAYERING", "CIRCULAR_TRANSFER"
    }.issubset(typologies)


def test_generator_rejects_invalid_counts_and_small_registry() -> None:
    registry = build_registry(customer_count=10)
    try:
        generate_transactions(registry, normal_count=-1)
    except ValueError as error:
        assert "cannot be negative" in str(error)
    else:
        raise AssertionError("Expected ValueError for negative count")

    with pytest.raises(ValueError, match="at least 3"):
        generate_transactions(registry, aml_count=2)

    small_registry = build_registry(customer_count=1)
    with __import__("pytest").raises(ValueError, match="at least two accounts"):
        generate_transactions(small_registry)

def test_normal_activity_includes_legitimate_fan_in_hard_negative() -> None:
    registry = build_registry(customer_count=12, seed=17)
    transactions = generate_transactions(
        registry, normal_count=3, fraud_count=0, aml_count=0, seed=18
    )

    assert len(transactions) == 3
    assert all(not item.is_fraud and not item.is_aml_suspicious for item in transactions)
    assert len({item.receiver_account_id for item in transactions}) == 1
    assert len({item.timestamp.date() for item in transactions}) == 1

def test_fraud_generation_supports_two_account_registry() -> None:
    registry = build_registry(customer_count=2, seed=19)
    transactions = generate_transactions(
        registry, normal_count=2, fraud_count=5, aml_count=0, seed=20
    )

    assert len(transactions) == 7
    assert sum(item.is_fraud for item in transactions) == 5
    assert all(item.sender_account_id != item.receiver_account_id for item in transactions)

```

### backend/tests/test_data_quality.py

**Purpose:** Tests all four label combinations, metadata consistency and explicit feature/exclusion lists.

```python
import pytest

from app.services.data_quality import (
    MODEL_EXCLUDED_FIELDS,
    MODEL_FEATURE_ALLOWLIST,
    audit_ledger,
)
from app.services.registry import build_registry
from app.services.transaction_generator import generate_transactions


def test_audit_reports_all_four_independent_label_combinations() -> None:
    registry = build_registry(customer_count=30, seed=101)
    transactions = generate_transactions(
        registry,
        normal_count=20,
        fraud_count=8,
        aml_count=8,
        seed=102,
        overlap_count=2,
    )

    report = audit_ledger(registry, transactions)

    assert report["transaction_count"] == 34
    assert report["normal_only"] == 20
    assert report["fraud_only"] == 6
    assert report["aml_only"] == 6
    assert report["fraud_and_aml"] == 2


def test_audit_rejects_inconsistent_label_metadata() -> None:
    registry = build_registry(customer_count=10)
    transactions = generate_transactions(registry, 5, 3, 3, seed=103)
    normal_event = next(item for item in transactions if not item.is_fraud)
    corrupted = normal_event.model_copy(update={"fraud_scenario": "FALSE_METADATA"})
    altered = [
        corrupted if item.transaction_id == normal_event.transaction_id else item
        for item in transactions
    ]

    with pytest.raises(ValueError, match="fraud label"):
        audit_ledger(registry, altered)


def test_feature_allowlist_and_exclusions_are_explicit() -> None:
    assert "amount" in MODEL_FEATURE_ALLOWLIST
    assert "channel" in MODEL_FEATURE_ALLOWLIST
    assert "is_fraud" in MODEL_EXCLUDED_FIELDS
    assert "is_aml_suspicious" in MODEL_EXCLUDED_FIELDS
    assert "transaction_id" in MODEL_EXCLUDED_FIELDS
    assert "fraud_scenario" in MODEL_EXCLUDED_FIELDS
    assert "aml_typology" in MODEL_EXCLUDED_FIELDS

```
## 7. Verification status

- Generator and data-quality tests are committed.
- They have **not** been executed in the user's local environment during this implementation session.
- Run `python -m pytest` from `backend/` and share the complete output before marking this redesign verified.
- Existing PostgreSQL databases need the additive schema migration documented in Task 04.

## 8. Limitations

- All customer behavior and labels are synthetic, not sourced from real bank activity.
- The 8% expected high-value hard-negative rate and default one-row overlap are design parameters, not empirical estimates.
- The current generator models transfers only; it does not yet create explicit salary, utility, merchant, cash, card or loan event types.
- Login/device signals and historical beneficiary relationships are not generated.
- Legitimate fan-in/fan-out lookalikes are included, but a broader library of legitimate graph motifs and behavior segments remains future work.
- Synthetic results cannot prove absence of all bias or real-bank detection accuracy.
- Continuous generation, a Synthetic Banking API, Kafka publishing and model training are separate future tasks.
