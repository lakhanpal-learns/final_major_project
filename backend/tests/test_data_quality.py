import pytest

from app.services.data_quality import MODEL_EXCLUDED_FIELDS, audit_ledger
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
    corrupted = transactions[0].model_copy(update={"fraud_scenario": "FALSE_METADATA"})

    with pytest.raises(ValueError, match="fraud label"):
        audit_ledger(registry, [corrupted, *transactions[1:]])


def test_model_excluded_fields_contain_labels_and_identifiers() -> None:
    assert "is_fraud" in MODEL_EXCLUDED_FIELDS
    assert "is_aml_suspicious" in MODEL_EXCLUDED_FIELDS
    assert "transaction_id" in MODEL_EXCLUDED_FIELDS
    assert "fraud_scenario" in MODEL_EXCLUDED_FIELDS
    assert "aml_typology" in MODEL_EXCLUDED_FIELDS
