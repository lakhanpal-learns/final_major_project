"""Train and evaluate the Sentinel fraud Random Forest baseline."""

import json
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from app.services.fraud_features import FEATURE_COLUMNS, TARGET_COLUMN


CATEGORICAL_COLUMNS = (
    "currency",
    "transaction_type",
    "channel",
    "sender_account_type",
    "receiver_account_type",
)
NUMERIC_COLUMNS = tuple(
    column for column in FEATURE_COLUMNS if column not in CATEGORICAL_COLUMNS
)
SPLIT_COLUMN = "dataset_split"
SPLIT_NAMES = ("train", "validation", "test")


def _load_and_validate_dataset(dataset_path: str | Path) -> pd.DataFrame:
    path = Path(dataset_path)
    if not path.is_file():
        raise FileNotFoundError(f"Prepared fraud dataset not found: {path}")

    frame = pd.read_csv(path)
    expected = set(FEATURE_COLUMNS) | {TARGET_COLUMN, SPLIT_COLUMN}
    actual = set(frame.columns)
    if actual != expected:
        raise ValueError(
            f"Dataset schema mismatch; missing={sorted(expected - actual)}, "
            f"unexpected={sorted(actual - expected)}."
        )
    if frame.empty:
        raise ValueError("Prepared fraud dataset is empty.")
    if frame[list(FEATURE_COLUMNS)].isnull().any().any():
        raise ValueError("Model features contain missing values.")

    for column in NUMERIC_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="raise")
        if not np.isfinite(frame[column].to_numpy(dtype=float)).all():
            raise ValueError(f"Feature {column} contains non-finite values.")

    frame[TARGET_COLUMN] = pd.to_numeric(frame[TARGET_COLUMN], errors="raise")
    if not set(frame[TARGET_COLUMN].unique()).issubset({0, 1}):
        raise ValueError("Fraud target must contain only binary values 0 and 1.")
    if not set(frame[SPLIT_COLUMN].unique()).issubset(SPLIT_NAMES):
        raise ValueError("Dataset contains an unknown dataset_split value.")
    if set(frame[SPLIT_COLUMN].unique()) != set(SPLIT_NAMES):
        raise ValueError("Dataset must contain train, validation and test partitions.")

    for split_name in SPLIT_NAMES:
        labels = frame.loc[frame[SPLIT_COLUMN] == split_name, TARGET_COLUMN]
        if labels.empty or labels.nunique() != 2:
            raise ValueError(
                f"The {split_name} split must contain both normal and fraud examples."
            )
    return frame


def _binary_metrics(y_true: np.ndarray, probabilities: np.ndarray, threshold: float) -> dict:
    predictions = (probabilities >= threshold).astype(int)
    matrix = confusion_matrix(y_true, predictions, labels=[0, 1])
    return {
        "threshold": float(threshold),
        "precision": float(precision_score(y_true, predictions, zero_division=0)),
        "recall": float(recall_score(y_true, predictions, zero_division=0)),
        "f1": float(f1_score(y_true, predictions, zero_division=0)),
        "pr_auc_average_precision": float(average_precision_score(y_true, probabilities)),
        "roc_auc": float(roc_auc_score(y_true, probabilities)),
        "confusion_matrix": {
            "true_negative": int(matrix[0, 0]),
            "false_positive": int(matrix[0, 1]),
            "false_negative": int(matrix[1, 0]),
            "true_positive": int(matrix[1, 1]),
        },
    }


def _select_validation_threshold(y_true: np.ndarray, probabilities: np.ndarray) -> float:
    """Choose the threshold with best validation F1; break ties by precision."""
    precision, recall, thresholds = precision_recall_curve(y_true, probabilities)
    if len(thresholds) == 0:
        raise ValueError("Validation predictions do not provide a usable threshold.")
    f1_values = (
        2 * precision[:-1] * recall[:-1]
        / np.maximum(precision[:-1] + recall[:-1], np.finfo(float).eps)
    )
    best_f1 = float(np.max(f1_values))
    best_indices = np.flatnonzero(np.isclose(f1_values, best_f1))
    best_index = max(best_indices, key=lambda index: (precision[index], thresholds[index]))
    return float(thresholds[best_index])


def train_fraud_model(
    dataset_path: str | Path,
    output_dir: str | Path,
    *,
    random_state: int = 42,
    n_estimators: int = 400,
) -> dict[str, Any]:
    """Fit on train, tune threshold on validation, and evaluate once on test."""
    if n_estimators < 1:
        raise ValueError("n_estimators must be at least 1.")

    frame = _load_and_validate_dataset(dataset_path)
    train = frame.loc[frame[SPLIT_COLUMN] == "train"]
    validation = frame.loc[frame[SPLIT_COLUMN] == "validation"]
    test = frame.loc[frame[SPLIT_COLUMN] == "test"]

    preprocessor = ColumnTransformer(
        transformers=[
            ("categorical", OneHotEncoder(handle_unknown="ignore"), list(CATEGORICAL_COLUMNS)),
            ("numeric", "passthrough", list(NUMERIC_COLUMNS)),
        ],
        remainder="drop",
    )
    pipeline = Pipeline([
        ("preprocessor", preprocessor),
        ("classifier", RandomForestClassifier(
            n_estimators=n_estimators,
            class_weight="balanced_subsample",
            min_samples_leaf=2,
            random_state=random_state,
            n_jobs=-1,
        )),
    ])

    x_train = train[list(FEATURE_COLUMNS)]
    y_train = train[TARGET_COLUMN].astype(int).to_numpy()
    pipeline.fit(x_train, y_train)

    x_validation = validation[list(FEATURE_COLUMNS)]
    y_validation = validation[TARGET_COLUMN].astype(int).to_numpy()
    validation_probabilities = pipeline.predict_proba(x_validation)[:, 1]
    threshold = _select_validation_threshold(y_validation, validation_probabilities)
    validation_metrics = _binary_metrics(y_validation, validation_probabilities, threshold)

    # The test partition is touched only after the threshold is fixed.
    x_test = test[list(FEATURE_COLUMNS)]
    y_test = test[TARGET_COLUMN].astype(int).to_numpy()
    test_probabilities = pipeline.predict_proba(x_test)[:, 1]
    test_metrics = _binary_metrics(y_test, test_probabilities, threshold)

    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    model_path = destination / "fraud_random_forest.joblib"
    metrics_path = destination / "fraud_model_metrics.json"
    joblib.dump(pipeline, model_path)

    summary = {
        "model": "RandomForestClassifier",
        "purpose": "synthetic fraud baseline",
        "random_state": random_state,
        "n_estimators": n_estimators,
        "class_weight": "balanced_subsample",
        "min_samples_leaf": 2,
        "feature_columns": list(FEATURE_COLUMNS),
        "categorical_columns": list(CATEGORICAL_COLUMNS),
        "numeric_columns": list(NUMERIC_COLUMNS),
        "target_column": TARGET_COLUMN,
        "split_policy": "precomputed chronological train/validation/test; no shuffle",
        "threshold_selection": "maximum validation F1; ties prefer higher precision",
        "selected_threshold": threshold,
        "split_counts": {
            name: {
                "rows": int(len(partition)),
                "normal": int((partition[TARGET_COLUMN] == 0).sum()),
                "fraud": int((partition[TARGET_COLUMN] == 1).sum()),
            }
            for name, partition in (
                ("train", train), ("validation", validation), ("test", test)
            )
        },
        "validation_metrics_at_selected_threshold": validation_metrics,
        "test_metrics_at_selected_threshold": test_metrics,
        "versions": {
            "scikit_learn": sklearn.__version__,
            "pandas": pd.__version__,
            "numpy": np.__version__,
        },
        "limitations": [
            "Evaluation uses rule-generated synthetic labels and transactions.",
            "Metrics do not establish performance on real banking data.",
            "The threshold is tuned on validation data, not on the test partition.",
        ],
        "model_file": model_path.name,
    }
    metrics_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return {
        "summary": summary,
        "model_path": model_path,
        "metrics_path": metrics_path,
    }
