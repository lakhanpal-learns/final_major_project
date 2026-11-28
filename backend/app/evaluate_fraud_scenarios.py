"""CLI for post-hoc, scenario-wise evaluation of the trained fraud model."""
import argparse
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from app.services.fraud_features import FEATURE_COLUMNS
from app.services.fraud_scenario_evaluation import build_scenario_report


def _bool_series(values: pd.Series) -> pd.Series:
    normalized = values.astype(str).str.strip().str.lower()
    mapping = {"true": 1, "1": 1, "false": 0, "0": 0}
    parsed = normalized.map(mapping)
    if parsed.isna().any():
        raise ValueError("Raw is_fraud column contains invalid boolean values.")
    return parsed.astype(int)


def main() -> None:
    backend_dir = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Evaluate fraud recall by synthetic scenario.")
    parser.add_argument("--raw-dir", default=str(backend_dir / "data" / "raw" / "synthetic"))
    parser.add_argument("--dataset", default=str(backend_dir / "data" / "processed" / "fraud" / "fraud_dataset.csv"))
    parser.add_argument("--model-dir", default=str(backend_dir / "models" / "fraud"))
    parser.add_argument("--output", default=str(backend_dir / "models" / "fraud" / "fraud_scenario_metrics.json"))
    parser.add_argument("--high-value-cutoff", type=float, default=50000.0)
    args = parser.parse_args()

    raw_path = Path(args.raw_dir) / "transactions.csv"
    dataset_path = Path(args.dataset)
    model_dir = Path(args.model_dir)
    raw = pd.read_csv(raw_path, dtype={"transaction_id": str})
    prepared = pd.read_csv(dataset_path)
    required_raw = {"transaction_id", "timestamp", "is_fraud", "fraud_scenario", "amount"}
    if not required_raw.issubset(raw.columns):
        raise ValueError(f"Raw transactions missing columns: {sorted(required_raw - set(raw.columns))}")
    if len(raw) != len(prepared):
        raise ValueError("Raw and prepared datasets have different row counts.")

    raw["timestamp"] = pd.to_datetime(raw["timestamp"], utc=True, errors="raise")
    raw = raw.sort_values(["timestamp", "transaction_id"], kind="mergesort").reset_index(drop=True)
    raw["is_fraud"] = _bool_series(raw["is_fraud"])
    if not np.array_equal(raw["is_fraud"].to_numpy(), prepared["is_fraud"].astype(int).to_numpy()):
        raise ValueError("Raw and prepared fraud labels/order do not align; refusing scenario evaluation.")

    test_mask = prepared["dataset_split"].eq("test")
    if not test_mask.any():
        raise ValueError("Prepared dataset has no test rows.")
    test_features = prepared.loc[test_mask, list(FEATURE_COLUMNS)]
    test_audit = raw.loc[test_mask, ["is_fraud", "fraud_scenario", "amount"]].reset_index(drop=True)
    model = joblib.load(model_dir / "fraud_random_forest.joblib")
    metrics = json.loads((model_dir / "fraud_model_metrics.json").read_text(encoding="utf-8"))
    threshold = float(metrics["selected_threshold"])
    probabilities = model.predict_proba(test_features)[:, 1]
    predictions = (probabilities >= threshold).astype(int)
    report = build_scenario_report(
        test_audit,
        predictions,
        probabilities,
        high_value_cutoff=args.high_value_cutoff,
    )
    report["model"] = metrics["model"]
    report["threshold"] = threshold
    report["test_split_policy"] = metrics["split_policy"]
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print("Scenario-wise fraud evaluation completed.")
    print(f"Report: {output}")
    print(f"Threshold: {threshold:.6f}")
    for name, result in report["fraud_scenarios"].items():
        print(f"{name}: recall={result['recall']:.4f}, detected={result['detected']}, missed={result['missed']}, total={result['fraud_transactions']}")
    hard = report["normal_high_value_transactions"]
    print(f"Normal high-value false-positive rate: {hard['false_positive_rate']}")


if __name__ == "__main__":
    main()
