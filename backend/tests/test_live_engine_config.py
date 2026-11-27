import pytest

from app.config import get_transactions_per_second


def test_default_live_rate_is_ten(monkeypatch):
    monkeypatch.delenv("TRANSACTIONS_PER_SECOND", raising=False)
    assert get_transactions_per_second() == 10.0


def test_live_rate_is_configurable(monkeypatch):
    monkeypatch.setenv("TRANSACTIONS_PER_SECOND", "2.5")
    assert get_transactions_per_second() == 2.5


@pytest.mark.parametrize("value", ["0", "-1", "fast"])
def test_invalid_live_rate_is_rejected(monkeypatch, value):
    monkeypatch.setenv("TRANSACTIONS_PER_SECOND", value)
    with pytest.raises(ValueError):
        get_transactions_per_second()
