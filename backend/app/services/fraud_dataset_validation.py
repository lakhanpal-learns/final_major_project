"""Validation and chronological splitting for the offline fraud dataset."""

import math
from collections import Counter
from datetime import datetime, timezone
from typing import Sequence

from app.services.fraud_features import FEATURE_COLUMNS, TARGET_COLUMN


SPLIT_COLUMN = "dataset_split"
SPLIT_NAMES = ("train", "validation", "test")
EXPECTED_COLUMNS = set(FEATURE_COLUMNS) | {TARGET_COLUMN, SPLIT_COLUMN}


def assign_chronological_splits(
    rows: Sequence[dict],
    timestamps: Sequence[datetime] | None = None,
) -> list[dict]:
    """Split chronologically without separating events at an identical timestamp.

    Timestamps must align with the already ordered rows. If omitted, each row
    is treated as an independent time group for timestamp-free callers.
    """
    if len(rows) < 3:
        raise ValueError("At least three transactions are required for chronological splitting.")
    if timestamps is not None and len(timestamps) != len(rows):
        raise ValueError("Timestamp count must match the number of dataset rows.")

    total = len(rows)
    train_end = max(1, int(total * 0.70))
    validation_end = max(train_end + 1, int(total * 0.85))
    validation_end = min(validation_end, total - 1)

    if timestamps is None:
        time_keys = list(range(total))
    else:
        time_keys = []
        for value in timestamps:
            if not isinstance(value, datetime):
                raise ValueError("Split timestamps must be datetime values.")
            normalized = (
                value.replace(tzinfo=timezone.utc)
                if value.tzinfo is None
                else value.astimezone(timezone.utc)
            )
            time_keys.append(normalized)
        if any(left > right for left, right in zip(time_keys, time_keys[1:])):
            raise ValueError("Rows and timestamps must be in chronological order.")

    groups = []
    start = 0
    while start < total:
        end = start + 1
        while end < total and time_keys[end] == time_keys[start]:
            end += 1
        groups.append((start, end))
        start = end

    result = [None] * total
    for start, end in groups:
        midpoint = (start + end) / 2
        split = "train" if midpoint < train_end else (
            "validation" if midpoint < validation_end else "test"
        )
        for index in range(start, end):
            result[index] = {**rows[index], SPLIT_COLUMN: split}
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
