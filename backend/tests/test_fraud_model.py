from __future__ import annotations

import csv

import pytest

from app.services.fraud_features import FEATURE_COLUMNS
from app.services.fraud_model import train_fraud_model


def _write_dataset(path):
    categorical = {
        "currency": "INR",
        "transaction_type": "TRANSFER",
        "channel": "UPI",
        "sender_account_type": "SAVINGS",
        "receiver_account_type": "CURRENT",
    }
    numeric = [column for column in FEATURE_COLUMNS if column not in categorical]
    fields = list(FEATURE_COLUMNS) + ["is_fraud", "dataset_split"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for split, count in (("train", 80), ("validation", 30), ("test", 30)):
            for index in range(count):
                fraud = int(index % 5 == 0)
                row = dict(categorical)
                row.update({column: float(index + (100 if fraud else 0)) for column in numeric})
                row.update({
                    "is_fraud": fraud,
                    "dataset_split": split,
                })
                writer.writerow(row)


def test_training_fits_pipeline_saves_model_and_reports_holdout_metrics(tmp_path):
    dataset = tmp_path / "dataset.csv"
    output = tmp_path / "model"
    _write_dataset(dataset)

    result = train_fraud_model(dataset, output, n_estimators=20)

    assert result["model_path"].is_file()
    assert result["metrics_path"].is_file()
    summary = result["summary"]
    assert summary["selected_threshold"] >= 0
    assert summary["split_policy"].startswith("precomputed chronological")
    assert summary["test_metrics_at_selected_threshold"]["pr_auc_average_precision"] >= 0
    assert set(summary["test_metrics_at_selected_threshold"]["confusion_matrix"]) == {
        "true_negative", "false_positive", "false_negative", "true_positive"
    }


def test_training_rejects_validation_split_without_both_classes(tmp_path):
    dataset = tmp_path / "dataset.csv"
    _write_dataset(dataset)
    rows = list(csv.DictReader(dataset.open(encoding="utf-8")))
    for row in rows:
        if row["dataset_split"] == "validation":
            row["is_fraud"] = "0"
    with dataset.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    with pytest.raises(ValueError, match="validation split"):
        train_fraud_model(dataset, tmp_path / "model", n_estimators=10)
