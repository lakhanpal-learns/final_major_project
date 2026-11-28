"""CLI for training the Sentinel fraud Random Forest baseline."""

import argparse
from pathlib import Path

from app.services.fraud_model import train_fraud_model


def main() -> None:
    backend_dir = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(
        description="Train a Random Forest using prepared chronological fraud splits."
    )
    parser.add_argument(
        "--dataset",
        default=str(backend_dir / "data" / "processed" / "fraud" / "fraud_dataset.csv"),
        help="Prepared Task 11 CSV dataset.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(backend_dir / "models" / "fraud"),
        help="Directory for the serialized model and metrics JSON.",
    )
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--n-estimators", type=int, default=400)
    args = parser.parse_args()

    result = train_fraud_model(
        args.dataset,
        args.output_dir,
        random_state=args.random_state,
        n_estimators=args.n_estimators,
    )
    summary = result["summary"]
    metrics = summary["test_metrics_at_selected_threshold"]

    print("Fraud model training completed.")
    print(f"Model: {result['model_path']}")
    print(f"Metrics: {result['metrics_path']}")
    print(f"Validation-selected threshold: {summary['selected_threshold']:.6f}")
    print("Test metrics:")
    for name in ("precision", "recall", "f1", "pr_auc_average_precision", "roc_auc"):
        print(f"  {name}: {metrics[name]:.4f}")
    print(f"  confusion_matrix: {metrics['confusion_matrix']}")


if __name__ == "__main__":
    main()
