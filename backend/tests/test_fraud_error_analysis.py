import numpy as np
import pandas as pd
import pytest

from app.services.fraud_error_analysis import (
    aggregate_feature_importance,
    build_error_analysis,
)


def test_aggregate_feature_importance_combines_one_hot_columns():
    result = aggregate_feature_importance(
        ["categorical__channel_UPI", "categorical__channel_WEB", "numeric__amount"],
        np.array([0.2, 0.3, 0.5]),
        ("channel", "amount"),
    )
    assert result == [
        {"feature": "amount", "importance": 0.5},
        {"feature": "channel", "importance": pytest.approx(0.5)},
    ]


def test_build_error_analysis_reports_false_positives_and_negatives():
    audit = pd.DataFrame({
        "transaction_id": ["t1", "t2", "t3", "t4"],
        "is_fraud": [1, 1, 0, 0],
        "fraud_scenario": ["RAPID", "BURST", "", ""],
    })
    features = pd.DataFrame({"amount": [100.0, 200.0, 300.0, 400.0]})
    summary, errors = build_error_analysis(
        audit,
        features,
        np.array([0, 1, 1, 0]),
        np.array([0.2, 0.9, 0.8, 0.1]),
        threshold=0.5,
        feature_importance=[{"feature": "amount", "importance": 1.0}],
    )
    assert summary["false_negatives"] == 1
    assert summary["false_positives"] == 1
    assert summary["fraud_scenarios"]["RAPID"]["missed"] == 1
    assert set(errors["error_type"]) == {"FALSE_NEGATIVE", "FALSE_POSITIVE"}
    assert set(errors["transaction_id"]) == {"t1", "t3"}
    amounts_by_id = dict(zip(errors["transaction_id"], errors["amount"]))
    assert amounts_by_id == {"t1": 100.0, "t3": 300.0}
    feature_amounts_by_id = dict(zip(errors["transaction_id"], errors["feature_amount"]))
    assert feature_amounts_by_id == {"t1": 100.0, "t3": 300.0}


def test_build_error_analysis_rejects_misaligned_inputs():
    audit = pd.DataFrame({"is_fraud": [1], "fraud_scenario": ["RAPID"]})
    with pytest.raises(ValueError, match="equal lengths"):
        build_error_analysis(
            audit,
            pd.DataFrame(),
            np.array([]),
            np.array([]),
            threshold=0.5,
            feature_importance=[],
        )
