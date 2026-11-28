"""Validation and chronological splitting for the offline fraud dataset."""

import math
from collections import Counter
from typing import Sequence

from app.services.fraud_features import FEATURE_COLUMNS, TARGET_COLUMN


SPLIT_COLUMN = "dataset_split"
SPLIT_NAMES = ("train", "validation", "test")
EXPECTED_COLUMNS = set(FEATURE_COLUMNS) | {TARGET_COLUMN, SPLIT_COLUMN}


def assign_chronological_splits(rows: Sequence[dict]) -> list[dict]:
    """Assign ordered rows to 70/15/15 train/validation/test partitions."""
    if len(rows) < 3:
        raise ValueError("At least three transactions are required for chronological splitting.")

    total = len(rows)
    train_end = max(1, int(total * 0.70))
    validation_end = max(train_end + 1, int(total * 0.85))
    validation_end = min(validation_end, total - 1)

    result = []
    for index, row in enumerate(rows):
        split = "train" if index < train_end else (
            "validation" if index < validation_end else "test"
        )
        result.append({**row, SPLIT_COLUMN: split})
    return result


def validate_fraud_dataset(rows: Sequence[dict]) -> dict:
    """Fail on schema/label/numeric corruption and summarize class distribution."""
    if not rows:
        raise ValueError("Fraud dataset is empty.")
    expected = EXPECTED_COLUMNS
    for row_number, row in enumerate(rows, start=1):
        if set(row) != expected:
            missing = sorted(expected - set(row))
            unexpected = sorted(set(row) - expected)
            raise ValueError(
                f"Row {row_number} has an invalid schema; missing={missing}, "
                f"unexpected={unexpected}."
            )
        if row[TARGET_COLUMN] not in (0, 1):
            raise ValueError(f"Row {row_number} has a non-binary fraud target.")
        if row[SPLIT_COLUMN] not in SPLIT_NAMES:
            raise ValueError(f"Row {row_number} has an unknown dataset split.")
        for feature in FEATURE_COLUMNS:
            value = row[feature]
            if isinstance(value, (int, float)) and not math.isfinite(float(value)):
                raise ValueError(f"Row {row_number} has a non-finite feature: {feature}.")

    split_counts = {}
    warnings = []
    for split_name in SPLIT_NAMES:
        split_rows = [row for row in rows if row[SPLIT_COLUMN] == split_name]
        labels = Counter(row[TARGET_COLUMN] for row in split_rows)
        split_counts[split_name] = {
            "rows": len(split_rows),
            "normal": labels.get(0, 0),
            "fraud": labels.get(1, 0),
        }
        if not split_rows:
            warnings.append(f"{split_name} split is empty.")
        elif labels.get(1, 0) == 0:
            warnings.append(f"{split_name} split contains no fraud-positive examples.")
        elif labels.get(0, 0) == 0:
            warnings.append(f"{split_name} split contains no normal examples.")

    total_labels = Counter(row[TARGET_COLUMN] for row in rows)
    return {
        "total_rows": len(rows),
        "normal": total_labels.get(0, 0),
        "fraud": total_labels.get(1, 0),
        "fraud_rate": total_labels.get(1, 0) / len(rows),
        "splits": split_counts,
        "warnings": warnings,
        "feature_columns": list(FEATURE_COLUMNS),
        "target_column": TARGET_COLUMN,
    }
