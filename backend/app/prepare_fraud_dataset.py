"""CLI for preparing fraud features from offline CSV or PostgreSQL."""

import argparse
from pathlib import Path

from app.services.fraud_dataset import (
    prepare_fraud_dataset_from_csv,
    write_fraud_dataset,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Prepare a leakage-controlled fraud dataset; does not train a model."
    )
    backend_dir = Path(__file__).resolve().parents[1]
    parser.add_argument(
        "--source",
        choices=("synthetic-csv", "postgres"),
        default="synthetic-csv",
        help="Input source. synthetic-csv is the default and does not require PostgreSQL.",
    )
    parser.add_argument(
        "--input-dir",
        default=str(backend_dir / "data" / "raw" / "synthetic"),
        help="Directory containing customers.csv, accounts.csv, transactions.csv and metadata.json.",
    )
    parser.add_argument(
        "--output-dir",
        default="data/processed/fraud",
        help="Directory for fraud_dataset.csv and fraud_dataset_metadata.json.",
    )
    args = parser.parse_args()

    if args.source == "synthetic-csv":
        rows, summary = prepare_fraud_dataset_from_csv(args.input_dir)
    else:
        # Keep the original database path available for comparison and legacy tests.
        from app.db.database import create_db_engine, create_session_factory

        engine = create_db_engine()
        try:
            session_factory = create_session_factory(engine)
            with session_factory() as session:
                from app.services.fraud_dataset import prepare_fraud_dataset
                rows, summary = prepare_fraud_dataset(session)
        finally:
            engine.dispose()

    csv_path, metadata_path = write_fraud_dataset(rows, summary, args.output_dir)
    print("Fraud dataset prepared.")
    print(f"Source: {summary['source_cohort']}")
    print(f"Rows: {summary['total_rows']}")
    print(f"Normal: {summary['normal']}")
    print(f"Fraud: {summary['fraud']}")
    print(f"CSV: {csv_path}")
    print(f"Metadata: {metadata_path}")
    for warning in summary["warnings"]:
        print(f"WARNING: {warning}")


if __name__ == "__main__":
    main()
