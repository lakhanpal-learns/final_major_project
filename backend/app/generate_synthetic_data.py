"""Generate an isolated, reproducible synthetic training cohort.

This command writes CSV/JSON files only. It never modifies PostgreSQL or Kafka.
"""
from __future__ import annotations

import csv
from collections import Counter
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any

from app.services.data_quality import audit_ledger
from app.services.registry import build_registry
from app.services.transaction_generator import generate_transactions


DEFAULTS = {
    "customer_count": 1_000,
    "normal_count": 20_000,
    "fraud_count": 1_000,
    "aml_count": 1_000,
    "registry_seed": 42,
    "transaction_seed": 99,
}


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    with temporary_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    temporary_path.replace(path)


def generate_to_directory(
    output_dir: Path,
    *,
    customer_count: int = DEFAULTS["customer_count"],
    normal_count: int = DEFAULTS["normal_count"],
    fraud_count: int = DEFAULTS["fraud_count"],
    aml_count: int = DEFAULTS["aml_count"],
    registry_seed: int = DEFAULTS["registry_seed"],
    transaction_seed: int = DEFAULTS["transaction_seed"],
) -> dict[str, Any]:
    """Generate, audit and export one offline dataset without touching the DB."""
    registry = build_registry(customer_count=customer_count, seed=registry_seed)
    transactions = generate_transactions(
        registry,
        normal_count=normal_count,
        fraud_count=fraud_count,
        aml_count=aml_count,
        seed=transaction_seed,
    )
    quality = audit_ledger(registry, transactions)

    output_dir.mkdir(parents=True, exist_ok=True)
    customers = [
        customer.model_dump(mode="json")
        for customer in registry.customers
    ]
    accounts = [
        account.model_dump(mode="json")
        for account in registry.accounts
    ]
    transaction_rows = [
        {
            **transaction.model_dump(mode="json"),
            "timestamp": transaction.timestamp.isoformat(),
            "amount": str(transaction.amount),
            "sender_balance_before": str(transaction.sender_balance_before),
            "sender_balance_after": str(transaction.sender_balance_after),
            "receiver_balance_before": str(transaction.receiver_balance_before),
            "receiver_balance_after": str(transaction.receiver_balance_after),
        }
        for transaction in transactions
    ]

    _write_csv(
        output_dir / "customers.csv",
        customers,
        list(customers[0].keys()) if customers else [],
    )
    _write_csv(
        output_dir / "accounts.csv",
        accounts,
        list(accounts[0].keys()) if accounts else [],
    )
    _write_csv(
        output_dir / "transactions.csv",
        transaction_rows,
        list(transaction_rows[0].keys()) if transaction_rows else [],
    )

    fraud_scenarios = dict(sorted(Counter(
        item.fraud_scenario for item in transactions if item.fraud_scenario
    ).items()))
    aml_typologies = dict(sorted(Counter(
        item.aml_typology for item in transactions if item.aml_typology
    ).items()))
    metadata = {
        "dataset_type": "synthetic",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "parameters": {
            "customer_count": customer_count,
            "normal_count": normal_count,
            "fraud_count": fraud_count,
            "aml_count": aml_count,
            "registry_seed": registry_seed,
            "transaction_seed": transaction_seed,
        },
        "counts": {
            "customers": len(customers),
            "accounts": len(accounts),
            **quality,
        },
        "fraud_scenario_counts": fraud_scenarios,
        "aml_typology_counts": aml_typologies,
        "time_range": {
            "first_transaction": transactions[0].timestamp.isoformat() if transactions else None,
            "last_transaction": transactions[-1].timestamp.isoformat() if transactions else None,
        },
        "files": ["customers.csv", "accounts.csv", "transactions.csv"],
        "limitations": [
            "All records and labels are synthetic and generated from predefined rules.",
            "Configured class counts are experiment parameters, not real-world prevalence.",
            "Good model generalization is not established by dataset size or this audit.",
            "The generator models transfers, not a complete bank product/event lifecycle.",
        ],
    }
    metadata_path = output_dir / "metadata.json"
    temporary_metadata_path = metadata_path.with_suffix(".json.tmp")
    temporary_metadata_path.write_text(
        json.dumps(metadata, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    temporary_metadata_path.replace(metadata_path)
    return metadata


def _environment_integer(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        return int(raw_value)
    except ValueError as error:
        raise ValueError(f"{name} must be an integer, received {raw_value!r}") from error


def main() -> None:
    """CLI entry point; counts can be overridden with environment variables."""
    backend_dir = Path(__file__).resolve().parents[1]
    output_dir = Path(os.getenv(
        "SENTINEL_SYNTHETIC_OUTPUT_DIR",
        str(backend_dir / "data" / "raw" / "synthetic"),
    ))
    metadata = generate_to_directory(
        output_dir,
        customer_count=_environment_integer("SENTINEL_CUSTOMER_COUNT", DEFAULTS["customer_count"]),
        normal_count=_environment_integer("SENTINEL_NORMAL_COUNT", DEFAULTS["normal_count"]),
        fraud_count=_environment_integer("SENTINEL_FRAUD_COUNT", DEFAULTS["fraud_count"]),
        aml_count=_environment_integer("SENTINEL_AML_COUNT", DEFAULTS["aml_count"]),
        registry_seed=_environment_integer("SENTINEL_REGISTRY_SEED", DEFAULTS["registry_seed"]),
        transaction_seed=_environment_integer("SENTINEL_TRANSACTION_SEED", DEFAULTS["transaction_seed"]),
    )
    print("Synthetic offline dataset generated and audited:")
    print(f"  output: {output_dir.resolve()}")
    for name, value in metadata["counts"].items():
        print(f"  {name}: {value}")
    print(f"  fraud scenarios: {metadata['fraud_scenario_counts']}")
    print(f"  AML typologies: {metadata['aml_typology_counts']}")


if __name__ == "__main__":
    main()
