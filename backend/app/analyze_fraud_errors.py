"""CLI for post-hoc fraud model error analysis on the existing test split."""
import argparse
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from app.services.fraud_error_analysis import (
    aggregate_feature_importance,
    build_error_analysis,
)
from app.services.fraud_features import FEATURE_COLUMNS


def _parse_labels(values: pd.Series) -> pd.Series:
    mapping = {"true": 1, "1": 1, "false": 0, "0": 0}
    parsed = values.astype(str).str.strip().str.lower().map(mapping)
    if parsed.isna().any():
        raise ValueError("Raw is_fraud column contains invalid boolean values.")
    return parsed.astype(int)


def main() -> None:
    backend_dir = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description="Analyze false positives, false negatives and feature importance."
    )
    parser.add_argument("--raw-dir", default=str(backend_dir / "data" / "raw" / "synthetic"))
    parser.add_argument(
        "--dataset",
        default=str(backend_dir / "data" / "processed" / "fraud" / "fraud_dataset.csv"),
    )
    parser.add_argument("--model-dir", default=str(backend_dir / "models" / "fraud"))
    parser.add_argument(
        "--json-output",
        default=str(backend_dir / "models" / "fraud" / "fraud_error_analysis.json"),
    )
    parser.add_argument(
        "--csv-output",
        default=str(backend_dir / "models" / "fraud" / "fraud_error_transactions.csv"),
    )
    args = parser.parse_args()

    raw = pd.read_csv(Path(args.raw_dir) / "transactions.csv", dtype={"transaction_id": str})
    prepared = pd.read_csv(args.dataset)
    required_raw = {
        "transaction_id", "timestamp", "sender_account_id", "receiver_account_id",
        "amount", "currency", "transaction_type", "channel", "is_fraud", "fraud_scenario",
    }
    missing = required_raw - set(raw.columns)
    if missing:
        raise ValueError(f"Raw transactions missing columns: {sorted(missing)}")
    expected = set(FEATURE_COLUMNS) | {"is_fraud", "dataset_split"}
    if set(prepared.columns) != expected:
        raise ValueError("Prepared fraud dataset schema does not match current feature schema.")
    if len(raw) != len(prepared):
        raise ValueError("Raw and prepared datasets have different row counts.")

    raw["timestamp"] = pd.to_datetime(raw["timestamp"], utc=True, errors="raise")
    raw = raw.sort_values(["timestamp", "transaction_id"], kind="mergesort").reset_index(drop=True)
    raw["is_fraud"] = _parse_labels(raw["is_fraud"])
    if not np.array_equal(raw["is_fraud"].to_numpy(), prepared["is_fraud"].astype(int).to_numpy()):
        raise ValueError("Raw and prepared fraud labels/order do not align; refusing error analysis.")

    test_mask = prepared["dataset_split"].eq("test")
    if not test_mask.any():
        raise ValueError("Prepared dataset has no test rows.")
    test_features = prepared.loc[test_mask, list(FEATURE_COLUMNS)].reset_index(drop=True)
    test_audit = raw.loc[test_mask].reset_index(drop=True)
    model_dir = Path(args.model_dir)
    model = joblib.load(model_dir / "fraud_random_forest.joblib")
    metrics = json.loads((model_dir / "fraud_model_metrics.json").read_text(encoding="utf-8"))
    threshold = float(metrics["selected_threshold"])
    probabilities = model.predict_proba(test_features)[:, 1]
    predictions = (probabilities >= threshold).astype(int)

    preprocessor = model.named_steps["preprocessor"]
    classifier = model.named_steps["classifier"]
    transformed_names = preprocessor.get_feature_names_out()
    importance = aggregate_feature_importance(
        transformed_names,
        classifier.feature_importances_,
        FEATURE_COLUMNS,
    )
    summary, errors = build_error_analysis(
        test_audit,
        test_features,
        predictions,
        probabilities,
        threshold=threshold,
        feature_importance=importance,
    )

    json_path = Path(args.json_output)
    csv_path = Path(args.csv_output)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    errors.to_csv(csv_path, index=False)

    print("Fraud error analysis completed.")
    print(f"JSON report: {json_path}")
    print(f"Error transactions CSV: {csv_path}")
    print(f"Test rows: {summary['test_rows']}")
    print(f"False negatives: {summary['false_negatives']}")
    print(f"False positives: {summary['false_positives']}")
    print("Top 5 features:")
    for item in summary["top_features"][:5]:
        print(f"  {item['feature']}: {item['importance']:.6f}")


if __name__ == "__main__":
    main()
