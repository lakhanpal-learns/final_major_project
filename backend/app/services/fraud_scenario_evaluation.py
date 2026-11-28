"""Scenario-wise evaluation for the synthetic fraud test partition.

Scenario metadata is used only for post-hoc analysis and is never passed to the model.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def build_scenario_report(
    audit: pd.DataFrame,
    predictions: np.ndarray,
    probabilities: np.ndarray,
    *,
    high_value_cutoff: float = 50_000.0,
) -> dict:
    """Summarize recall by fraud scenario and false positives on normal high values.

    Inputs must already represent the chronological test split and be row-aligned.
    """
    required = {"is_fraud", "fraud_scenario", "amount"}
    if not required.issubset(audit.columns):
        raise ValueError(f"Audit data missing columns: {sorted(required - set(audit.columns))}")
    y_true = audit["is_fraud"].astype(int).to_numpy()
    y_pred = np.asarray(predictions, dtype=int)
    scores = np.asarray(probabilities, dtype=float)
    if not (len(audit) == len(y_true) == len(y_pred) == len(scores)):
        raise ValueError("Audit rows, predictions and probabilities must have equal lengths.")
    if not set(np.unique(y_true)).issubset({0, 1}) or not set(np.unique(y_pred)).issubset({0, 1}):
        raise ValueError("Labels and predictions must be binary.")
    if not np.isfinite(scores).all():
        raise ValueError("Predicted probabilities must be finite.")

    scenarios = {}
    fraud_rows = audit.loc[audit["is_fraud"].astype(int) == 1]
    for scenario, group in fraud_rows.groupby("fraud_scenario", dropna=False):
        name = str(scenario) if pd.notna(scenario) and str(scenario).strip() else "UNSPECIFIED"
        indices = group.index.to_numpy()
        # The caller supplies a test frame with its original positional index.
        positions = audit.index.get_indexer(indices)
        actual = y_true[positions]
        predicted = y_pred[positions]
        count = int(len(positions))
        detected = int(((actual == 1) & (predicted == 1)).sum())
        scenarios[name] = {
            "fraud_transactions": count,
            "detected": detected,
            "missed": count - detected,
            "recall": float(detected / count) if count else 0.0,
            "mean_predicted_fraud_probability": float(scores[positions].mean()) if count else 0.0,
        }

    normal_high = (y_true == 0) & (pd.to_numeric(audit["amount"]).to_numpy() >= high_value_cutoff)
    normal_high_count = int(normal_high.sum())
    normal_high_fp = int(((y_pred == 1) & normal_high).sum())
    return {
        "test_rows": int(len(audit)),
        "fraud_scenarios": scenarios,
        "normal_high_value_transactions": {
            "definition": f"normal-labelled test transactions with amount >= {high_value_cutoff:.2f}",
            "transactions": normal_high_count,
            "false_positives": normal_high_fp,
            "false_positive_rate": (
                float(normal_high_fp / normal_high_count) if normal_high_count else None
            ),
        },
        "limitations": [
            "Scenario labels are used only for post-hoc evaluation, not model features.",
            "Scenario recall measures detection within each generated scenario, not real-world typology performance.",
            "High-value normal transactions are identified by the stated amount cutoff; this is a proxy, not a generator-provided hard-negative label.",
        ],
    }
