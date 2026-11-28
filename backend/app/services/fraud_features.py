"""Leakage-safe, point-in-time feature engineering for fraud transactions."""

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import math
from typing import Mapping, Sequence


FEATURE_COLUMNS = (
    "amount",
    "log_amount",
    "currency",
    "transaction_type",
    "channel",
    "transaction_hour",
    "day_of_week",
    "sender_account_type",
    "receiver_account_type",
    "sender_txn_count_5m",
    "sender_txn_count_15m",
    "sender_txn_count_1h",
    "sender_txn_count_24h",
    "sender_amount_sum_24h",
    "sender_unique_recipients_1h",
    "sender_unique_recipients_24h",
    "sender_max_recipient_txn_count_24h",
    "sender_recipient_repeat_ratio_24h",
    "receiver_txn_count_24h",
    "sender_avg_amount_30d",
)
TARGET_COLUMN = "is_fraud"
OUTPUT_COLUMNS = FEATURE_COLUMNS + (TARGET_COLUMN,)


def _utc(value: datetime) -> datetime:
    """Normalize database timestamps (including SQLite's naive values) to UTC."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _in_prior_window(event_time: datetime, target_time: datetime, hours: int) -> bool:
    """Use the open interval (target_time - window, target_time)."""
    return target_time - timedelta(hours=hours) < event_time < target_time


def _in_prior_minutes(event_time: datetime, target_time: datetime, minutes: int) -> bool:
    """Use the open interval (target_time - minutes, target_time)."""
    return target_time - timedelta(minutes=minutes) < event_time < target_time


def ordered_fraud_records(records: Sequence[Mapping]) -> list[Mapping]:
    """Return records in the exact order used by feature engineering."""
    return sorted(
        records,
        key=lambda item: (_utc(item["timestamp"]), str(item["transaction_id"])),
    )


def build_fraud_feature_rows(records: Sequence[Mapping]) -> list[dict]:
    """Build one feature row per transaction using strictly earlier events only.

    Records sharing the same timestamp are evaluated against the same prior
    history snapshot. IDs and labels other than the fraud target are never
    emitted as model features.
    """
    ordered = ordered_fraud_records(records)
    outgoing: dict[str, list[tuple[datetime, float, str]]] = defaultdict(list)
    incoming: dict[str, list[datetime]] = defaultdict(list)
    output: list[dict] = []

    index = 0
    while index < len(ordered):
        timestamp = _utc(ordered[index]["timestamp"])
        group_end = index
        while group_end < len(ordered) and _utc(ordered[group_end]["timestamp"]) == timestamp:
            group_end += 1

        for record in ordered[index:group_end]:
            amount = float(record["amount"])
            sender_history = outgoing[str(record["sender_account_id"])]
            receiver_history = incoming[str(record["receiver_account_id"])]

            prior_5m = [item for item in sender_history
                        if _in_prior_minutes(item[0], timestamp, 5)]
            prior_15m = [item for item in sender_history
                         if _in_prior_minutes(item[0], timestamp, 15)]
            prior_1h = [item for item in sender_history
                        if _in_prior_window(item[0], timestamp, 1)]
            prior_24h = [item for item in sender_history
                         if _in_prior_window(item[0], timestamp, 24)]
            prior_30d = [item for item in sender_history
                         if _in_prior_window(item[0], timestamp, 24 * 30)]
            incoming_24h = [item_time for item_time in receiver_history
                            if _in_prior_window(item_time, timestamp, 24)]

            recipient_counts = Counter(item[2] for item in prior_24h)
            max_recipient_count = max(recipient_counts.values(), default=0)

            output.append({
                "amount": amount,
                "log_amount": math.log1p(amount),
                "currency": str(record["currency"]),
                "transaction_type": str(record["transaction_type"]),
                "channel": str(record["channel"]),
                "transaction_hour": timestamp.hour,
                "day_of_week": timestamp.weekday(),
                "sender_account_type": str(record["sender_account_type"]),
                "receiver_account_type": str(record["receiver_account_type"]),
                "sender_txn_count_5m": len(prior_5m),
                "sender_txn_count_15m": len(prior_15m),
                "sender_txn_count_1h": len(prior_1h),
                "sender_txn_count_24h": len(prior_24h),
                "sender_amount_sum_24h": sum(item[1] for item in prior_24h),
                "sender_unique_recipients_1h": len({item[2] for item in prior_1h}),
                "sender_unique_recipients_24h": len(recipient_counts),
                "sender_max_recipient_txn_count_24h": max_recipient_count,
                "sender_recipient_repeat_ratio_24h": (
                    max_recipient_count / len(prior_24h) if prior_24h else 0.0
                ),
                "receiver_txn_count_24h": len(incoming_24h),
                "sender_avg_amount_30d": (
                    sum(item[1] for item in prior_30d) / len(prior_30d)
                    if prior_30d else 0.0
                ),
                TARGET_COLUMN: int(bool(record[TARGET_COLUMN])),
            })

        # Same-timestamp events become history only after every row is scored.
        for record in ordered[index:group_end]:
            sender_id = str(record["sender_account_id"])
            receiver_id = str(record["receiver_account_id"])
            outgoing[sender_id].append((timestamp, float(record["amount"]), receiver_id))
            incoming[receiver_id].append(timestamp)
        index = group_end

    return output
