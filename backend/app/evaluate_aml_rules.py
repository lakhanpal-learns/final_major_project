"""Evaluate persisted AML findings against synthetic labels in the processed cohort."""
from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import func, select

from app.db.database import create_db_engine, create_session_factory
from app.db.tables import AMLEventInboxRow, AMLFindingRow, TransactionRow
from app.services.aml_evaluation import build_aml_evaluation_report
from app.services.aml_rules import RULE_VERSION


def main() -> None:
    backend_dir = Path(__file__).resolve().parents[1]
    output_dir = backend_dir / "models" / "aml"
    output_dir.mkdir(parents=True, exist_ok=True)
    engine = create_db_engine()
    Session = create_session_factory(engine)
    try:
        with Session() as session:
            total_ledger = session.scalar(select(func.count()).select_from(TransactionRow)) or 0
            inbox_events = session.scalar(select(func.count()).select_from(AMLEventInboxRow)) or 0
            transactions = session.scalars(
                select(TransactionRow)
                .join(AMLEventInboxRow, AMLEventInboxRow.transaction_id == TransactionRow.transaction_id)
                .distinct()
                .order_by(TransactionRow.timestamp, TransactionRow.transaction_id)
            ).all()
            transaction_ids = [row.transaction_id for row in transactions]
            findings = session.scalars(
                select(AMLFindingRow).where(AMLFindingRow.transaction_id.in_(transaction_ids), AMLFindingRow.rule_version == RULE_VERSION)
            ).all() if transaction_ids else []

            transaction_data = [
                {
                    "transaction_id": row.transaction_id,
                    "is_aml_suspicious": row.is_aml_suspicious,
                    "aml_typology": row.aml_typology,
                }
                for row in transactions
            ]
            finding_data = [
                {"transaction_id": row.transaction_id, "typology": row.typology}
                for row in findings
            ]

        report, details = build_aml_evaluation_report(
            transaction_data,
            finding_data,
            total_ledger_transactions=total_ledger,
            inbox_event_count=inbox_events,
        )
        report["rule_version"] = RULE_VERSION
        report["generated_at_utc"] = datetime.now(timezone.utc).isoformat()
        report_path = output_dir / "aml_rule_evaluation.json"
        report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

        detail_path = output_dir / "aml_rule_evaluation_transactions.csv"
        fieldnames = [
            "transaction_id", "is_aml_suspicious", "aml_typology",
            "predicted_typologies", "predicted_any", "exact_typology_match",
        ]
        with detail_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(details)

        print(f"AML rule evaluation completed for {RULE_VERSION}.")
        print(f"  evaluated transactions: {report['evaluated_transactions']}")
        print(f"  total ledger transactions: {report['total_ledger_transactions']}")
        print(f"  ledger coverage: {report['ledger_transaction_coverage']:.2%}")
        print(f"  ground-truth AML positives: {report['ground_truth_aml_positive_transactions']}")
        print(f"  normal transactions flagged: {report['normal_transactions_flagged']}")
        print(f"  JSON report: {report_path}")
        print(f"  transaction details: {detail_path}")
        print("  per-typology metrics:")
        for typology, metrics in report["per_typology"].items():
            print(
                f"    {typology}: precision={metrics['precision']:.4f}, "
                f"recall={metrics['recall']:.4f}, f1={metrics['f1']:.4f}, "
                f"tp={metrics['tp']}, fp={metrics['fp']}, fn={metrics['fn']}"
            )
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
