from app.services.registry import build_registry


def test_registry_generates_expected_customer_and_account_counts() -> None:
    registry = build_registry(customer_count=20, seed=7)

    assert len(registry.customers) == 20
    assert len(registry.accounts) == 22


def test_registry_ids_are_unique_and_accounts_reference_customers() -> None:
    registry = build_registry(customer_count=30)

    customer_ids = {customer.customer_id for customer in registry.customers}
    account_ids = [account.account_id for account in registry.accounts]
    account_numbers = [account.account_number for account in registry.accounts]

    assert len(customer_ids) == len(registry.customers)
    assert len(account_ids) == len(set(account_ids))
    assert len(account_numbers) == len(set(account_numbers))
    assert all(account.customer_id in customer_ids for account in registry.accounts)


def test_registry_is_reproducible_for_same_seed() -> None:
    first = build_registry(customer_count=15, seed=123)
    second = build_registry(customer_count=15, seed=123)

    assert [item.model_dump() for item in first.customers] == [
        item.model_dump() for item in second.customers
    ]
    assert [item.model_dump() for item in first.accounts] == [
        item.model_dump() for item in second.accounts
    ]


def test_registry_lookup_uses_canonical_ids() -> None:
    registry = build_registry(customer_count=10)

    assert registry.get_customer("CUST000001") is not None
    assert registry.get_account("ACC00000001") is not None
    assert registry.get_customer("UNKNOWN") is None
    assert registry.get_account("UNKNOWN") is None


def test_registry_rejects_zero_customers() -> None:
    try:
        build_registry(customer_count=0)
    except ValueError as error:
        assert "at least 1" in str(error)
    else:
        raise AssertionError("Expected ValueError for zero customers")
