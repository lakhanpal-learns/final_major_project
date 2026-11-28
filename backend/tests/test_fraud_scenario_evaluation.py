import numpy as np
import pandas as pd
import pytest

from app.services.fraud_scenario_evaluation import build_scenario_report


def test_scenario_report_calculates_per_scenario_recall_and_hard_negative_rate():
    audit = pd.DataFrame({
        "is_fraud": [1, 1, 1, 0, 0],
        "fraud_scenario": ["RAPID", "RAPID", "BURST", "", ""],
        "amount": [100, 200, 300, 90000, 1000],
    })
    report = build_scenario_report(audit, np.array([1, 0, 1, 1, 0]), np.array([.9, .2, .8, .7, .1]))
    assert report["fraud_scenarios"]["RAPID"]["fraud_transactions"] == 2
    assert report["fraud_scenarios"]["RAPID"]["detected"] == 1
    assert report["fraud_scenarios"]["RAPID"]["recall"] == 0.5
    assert report["fraud_scenarios"]["BURST"]["recall"] == 1.0
    assert report["normal_high_value_transactions"]["transactions"] == 1
    assert report["normal_high_value_transactions"]["false_positives"] == 1


def test_scenario_report_rejects_misaligned_arrays():
    audit = pd.DataFrame({"is_fraud": [1], "fraud_scenario": ["X"], "amount": [10]})
    with pytest.raises(ValueError, match="equal lengths"):
        build_scenario_report(audit, np.array([]), np.array([]))
