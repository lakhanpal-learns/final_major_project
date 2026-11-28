from pathlib import Path

from app.generate_synthetic_data import generate_to_directory


def test_offline_generator_exports_shared_registry_and_requested_counts(tmp_path: Path) -> None:
    metadata = generate_to_directory(
        tmp_path,
        customer_count=30,
        normal_count=40,
        fraud_count=5,
        aml_count=6,
        registry_seed=3,
        transaction_seed=11,
    )

    assert metadata["counts"]["customers"] == 30
    assert metadata["counts"]["accounts"] == 33
    assert metadata["counts"]["transaction_count"] == 50
    assert metadata["counts"]["normal_only"] == 40
    assert metadata["counts"]["fraud_and_aml"] == 1
    assert (tmp_path / "customers.csv").exists()
    assert (tmp_path / "accounts.csv").exists()
    assert (tmp_path / "transactions.csv").exists()
    assert (tmp_path / "metadata.json").exists()


def test_offline_generator_is_reproducible_except_generation_timestamp(tmp_path: Path) -> None:
    first_dir = tmp_path / "first"
    second_dir = tmp_path / "second"
    parameters = {
        "customer_count": 25,
        "normal_count": 80,
        "fraud_count": 12,
        "aml_count": 15,
        "registry_seed": 14,
        "transaction_seed": 15,
    }

    generate_to_directory(first_dir, **parameters)
    generate_to_directory(second_dir, **parameters)

    for filename in ("customers.csv", "accounts.csv", "transactions.csv"):
        assert (first_dir / filename).read_bytes() == (second_dir / filename).read_bytes()
