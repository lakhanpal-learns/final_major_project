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
