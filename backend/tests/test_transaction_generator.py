from app.services.registry import build_registry
from app.services.transaction_generator import generate_transactions


def test_generator_creates_requested_counts_and_labels() -> None:
    registry = build_registry(customer_count=30, seed=3)
    transactions = generate_transactions(
        registry, normal_count=40, fraud_count=5, aml_count=6, seed=11
    )

    assert len(transactions) == 51
    assert sum(item.is_fraud for item in transactions) == 5
    assert sum(item.is_aml_suspicious for item in transactions) == 6
    assert sum(not item.is_fraud and not item.is_aml_suspicious for item in transactions) == 40


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


def test_fraud_and_aml_labels_are_separate() -> None:
    registry = build_registry(customer_count=30)
    transactions = generate_transactions(registry, 10, 4, 4)

    fraud_events = [item for item in transactions if item.is_fraud]
    aml_events = [item for item in transactions if item.is_aml_suspicious]

    assert all(item.fraud_scenario == "HIGH_VALUE_UNUSUAL_TRANSFER" for item in fraud_events)
    assert all(item.aml_typology is not None for item in aml_events)


def test_generator_rejects_negative_counts_and_too_small_registry() -> None:
    registry = build_registry(customer_count=10)
    try:
        generate_transactions(registry, normal_count=-1)
    except ValueError as error:
        assert "cannot be negative" in str(error)
    else:
        raise AssertionError("Expected ValueError for negative count")

    small_registry = build_registry(customer_count=1)
    try:
        generate_transactions(small_registry)
    except ValueError as error:
        assert "at least two accounts" in str(error)
    else:
        raise AssertionError("Expected ValueError for one-account registry")
