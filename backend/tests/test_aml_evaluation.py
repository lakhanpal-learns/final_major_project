from app.services.aml_evaluation import build_aml_evaluation_report


def tx(txid, suspicious=False, typology=None):
    return {
        "transaction_id": txid,
        "is_aml_suspicious": suspicious,
        "aml_typology": typology,
    }


def finding(txid, typology):
    return {"transaction_id": txid, "typology": typology}


def test_reports_per_typology_and_overall_confusion_counts():
    transactions = [
        tx("T1", True, "FAN_IN"),
        tx("T2", True, "FAN_IN"),
        tx("T3", True, "STRUCTURING"),
        tx("T4"),
        tx("T5"),
    ]
    findings = [
        finding("T1", "FAN_IN"),
        finding("T3", "FAN_OUT"),
        finding("T4", "LAYERING"),
    ]
    report, details = build_aml_evaluation_report(
        transactions, findings, total_ledger_transactions=10, inbox_event_count=5
    )

    assert report["evaluated_transactions"] == 5
    assert report["total_ledger_transactions"] == 10
    assert report["ledger_transaction_coverage"] == 0.5
    assert report["overall_transaction_metrics"] == {
        "tp": 2, "fp": 1, "fn": 1, "tn": 1,
        "precision": 2 / 3, "recall": 2 / 3,
        "f1": 2 / 3, "specificity": 0.5,
    }
    assert report["per_typology"]["FAN_IN"]["tp"] == 1
    assert report["per_typology"]["FAN_IN"]["fn"] == 1
    assert report["per_typology"]["FAN_OUT"]["fp"] == 1
    assert report["normal_transactions_flagged"] == 1
    assert len(details) == 5


def test_no_findings_means_all_ground_truth_positives_are_missed():
    report, _ = build_aml_evaluation_report(
        [tx("T1", True, "LAYERING"), tx("T2")], []
    )
    assert report["overall_transaction_metrics"]["fn"] == 1
    assert report["overall_transaction_metrics"]["tn"] == 1
    assert report["per_typology"]["LAYERING"]["fn"] == 1


def test_duplicate_evaluated_transaction_is_rejected():
    import pytest

    with pytest.raises(ValueError, match="duplicate evaluated transaction"):
        build_aml_evaluation_report([tx("T1"), tx("T1")], [])


def test_finding_for_transaction_outside_evaluation_cohort_is_ignored():
    report, _ = build_aml_evaluation_report(
        [tx("T1")], [finding("OUTSIDE", "FAN_IN")]
    )
    assert report["predicted_typology_finding_counts"] == {}
    assert report["overall_transaction_metrics"]["fp"] == 0
