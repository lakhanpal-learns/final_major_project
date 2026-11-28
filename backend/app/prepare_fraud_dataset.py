"""CLI entry point for offline fraud feature engineering and dataset export."""

import argparse

from app.db.database import create_db_engine, create_session_factory
from app.services.fraud_dataset import prepare_fraud_dataset, write_fraud_dataset


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Prepare the labelled seed cohort for fraud model training."
    )
    parser.add_argument(
        "--output-dir",
        default="data/processed/fraud",
        help="Directory for fraud_dataset.csv and fraud_dataset_metadata.json.",
    )
    args = parser.parse_args()

    engine = create_db_engine()
    try:
        session_factory = create_session_factory(engine)
        with session_factory() as session:
            rows, summary = prepare_fraud_dataset(session)
        csv_path, metadata_path = write_fraud_dataset(rows, summary, args.output_dir)
        print("Fraud dataset prepared.")
        print(f"Rows: {summary['total_rows']}")
        print(f"Normal: {summary['normal']}")
        print(f"Fraud: {summary['fraud']}")
        print(f"CSV: {csv_path}")
        print(f"Metadata: {metadata_path}")
        for warning in summary["warnings"]:
            print(f"WARNING: {warning}")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
