"""Post-hoc error analysis for a trained fraud model.

This module reports test-set false positives/negatives and global feature
importance. Scenario and transaction identifiers are audit-only fields and
are never passed to the model.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def aggregate_feature_importance(
    transformed_names: list[str] | np.ndarray,
    importances: np.ndarray,
    feature_columns: tuple[str, ...],
) -> list[dict]:
    """Aggregate one-hot encoded importances back to original input columns."""
    names = [str(name).split("__", 1)[-1] for name in transformed_names]
    values = np.asarray(importances, dtype=float)
    if len(names) != len(values):
        raise ValueError("Transformed feature names and importances must have equal lengths.")
    if not np.isfinite(values).all() or (values < 0).any():
        raise ValueError("Feature importances must be finite and non-negative.")

    rows = []
    for feature in feature_columns:
        total = sum(
            float(value)
            for name, value in zip(names, values)
            if name == feature or name.startswith(feature + "_")
        )
        rows.append({"feature": feature, "importance": total})
    return sorted(rows, key=lambda row: (-row["importance"], row["feature"]))


def build_error_analysis(
    audit: pd.DataFrame,
    features: pd.DataFrame,
    predictions: np.ndarray,
    probabilities: np.ndarray,
    *,
    threshold: float,
    feature_importance: list[dict],
) -> tuple[dict, pd.DataFrame]:
    """Build a summary and a row-level CSV frame for test-set errors."""
    if "is_fraud" not in audit.columns:
        raise ValueError("Audit data must contain is_fraud.")
    if "fraud_scenario" not in audit.columns:
        raise ValueError("Audit data must contain fraud_scenario.")
    y_true = audit["is_fraud"].astype(int).to_numpy()
    y_pred = np.asarray(predictions, dtype=int)
    scores = np.asarray(probabilities, dtype=float)
    if not (len(audit) == len(features) == len(y_true) == len(y_pred) == len(scores)):
        raise ValueError("Audit, features, predictions and probabilities must have equal lengths.")
    if not set(np.unique(y_true)).issubset({0, 1}):
        raise ValueError("Actual fraud labels must be binary.")
    if not set(np.unique(y_pred)).issubset({0, 1}):
        raise ValueError("Predictions must be binary.")
    if not np.isfinite(scores).all() or ((scores < 0) | (scores > 1)).any():
        raise ValueError("Predicted probabilities must be finite and between 0 and 1.")
    if not 0 <= float(threshold) <= 1:
        raise ValueError("Threshold must be between 0 and 1.")

    false_negative = (y_true == 1) & (y_pred == 0)
    false_positive = (y_true == 0) & (y_pred == 1)
    errors = audit.reset_index(drop=True).copy()
    feature_rows = features.reset_index(drop=True).copy()
    errors["predicted_is_fraud"] = y_pred
    errors["predicted_fraud_probability"] = scores
    errors["threshold"] = float(threshold)
    errors["error_type"] = np.select(
        [false_negative, false_positive],
        ["FALSE_NEGATIVE", "FALSE_POSITIVE"],
        default="",
    )
    errors = errors.loc[errors["error_type"] != ""].reset_index(drop=True)
    errors = pd.concat([errors, feature_rows.loc[errors.index].reset_index(drop=True)], axis=1)

    scenario_summary = {}
    fraud_audit = audit.loc[y_true == 1].reset_index(drop=True)
    fraud_predictions = y_pred[y_true == 1]
    for scenario, positions in fraud_audit.groupby("fraud_scenario", dropna=False).groups.items():
        indexes = np.asarray(list(positions), dtype=int)
        count = int(len(indexes))
        missed = int((fraud_predictions[indexes] == 0).sum())
        name = str(scenario) if pd.notna(scenario) and str(scenario).strip() else "UNSPECIFIED"
        scenario_summary[name] = {
            "fraud_transactions": count,
            "detected": count - missed,
            "missed": missed,
            "recall": float((count - missed) / count) if count else 0.0,
        }

    summary = {
        "purpose": "post-hoc error analysis on the existing chronological test split",
        "test_rows": int(len(audit)),
        "threshold": float(threshold),
        "true_positives": int(((y_true == 1) & (y_pred == 1)).sum()),
        "true_negatives": int(((y_true == 0) & (y_pred == 0)).sum()),
        "false_negatives": int(false_negative.sum()),
        "false_positives": int(false_positive.sum()),
        "error_rows": int(len(errors)),
        "fraud_scenarios": scenario_summary,
        "top_features": feature_importance[:10],
        "feature_importance_method": (
            "Random Forest impurity-based feature_importances_; one-hot encoded "
            "categorical importances are summed by original input feature"
        ),
        "limitations": [
            "Feature importance is global and descriptive; it does not explain an individual prediction or establish causation.",
            "Error analysis reuses the existing test split, so it is diagnostic rather than an independent final evaluation.",
            "Transactions and labels are synthetic; results do not establish real-world banking performance.",
            "Scenario metadata is used only for post-hoc grouping and is not a model input.",
        ],
    }
    return summary, errors
