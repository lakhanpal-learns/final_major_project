"""Runtime settings for the Sentinel local development backend."""
import os


def get_transactions_per_second() -> float:
    """Read the configurable live simulator rate (default: 10 events/second)."""
    raw_value = os.getenv("TRANSACTIONS_PER_SECOND", "10")
    try:
        value = float(raw_value)
    except ValueError as exc:
        raise ValueError("TRANSACTIONS_PER_SECOND must be a positive number") from exc
    if value <= 0:
        raise ValueError("TRANSACTIONS_PER_SECOND must be a positive number")
    return value
