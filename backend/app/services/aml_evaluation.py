"""Offline evaluation of AML rule findings against synthetic ground-truth labels.

Labels are read only for evaluation. They are never passed to the AML rule engine.
The report is explicitly scoped to transactions present in the AML consumer inbox.
"""
from __future__ import annotations

from collections import Counter
from typing import Any, Iterable

AML_TYPOLOGIES = (
    "STRUCTURING",
    "FAN_IN",
    "FAN_OUT",
    "LAYERING",
    "CIRCULAR_TRANSFER",
)


def _binary_metrics(tp: int, fp: int, fn: int, tn: int) -> dict[str, Any]:
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    specificity = tn / (tn + fp) if tn + fp else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "tp": tp, "fp": fp, "fn": fn, "tn": tn,
        "precision": precision, "recall": recall,
        "f1": f1, "specificity": specificity,
    }


def build_aml_evaluation_report(
    transactions: Iterable[dict[str, Any]],
    findings: Iterable[dict[str, Any]],
    *,
    total_ledger_transactions: int | None = None,
    inbox_event_count: int | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Compare rule findings with labels for the supplied processed transactions.

    A transaction is ground-truth positive for one typology only when
    is_aml_suspicious is true and aml_typology matches that typology.
    """
    transaction_rows = list(transactions)
    finding_rows = list(findings)
    by_id: dict[str, dict[str, Any]] = {}
    for row in transaction_rows:
        transaction_id = str(row["transaction_id"])
        if transaction_id in by_id:
            raise ValueError(f"duplicate evaluated transaction: {transaction_id}")
        by_id[transaction_id] = row

    predicted_by_transaction: dict[str, set[str]] = {
        transaction_id: set() for transaction_id in by_id
    }
    for finding in finding_rows:
        transaction_id = str(finding["transaction_id"])
        typology = str(finding["typology"])
        if transaction_id in predicted_by_transaction:
            predicted_by_transaction[transaction_id].add(typology)

    per_typology: dict[str, Any] = {}
    for typology in AML_TYPOLOGIES:
        tp = fp = fn = tn = 0
        for transaction_id, row in by_id.items():
            truth = bool(row["is_aml_suspicious"]) and row.get("aml_typology") == typology
            predicted = typology in predicted_by_transaction[transaction_id]
            if truth and predicted:
                tp += 1
            elif not truth and predicted:
                fp += 1
            elif truth and not predicted:
                fn += 1
            else:
                tn += 1
        per_typology[typology] = {
            "ground_truth_positive": tp + fn,
            "predicted_positive": tp + fp,
            **_binary_metrics(tp, fp, fn, tn),
        }

    overall_tp = overall_fp = overall_fn = overall_tn = 0
    details: list[dict[str, Any]] = []
    truth_typology_counts: Counter[str] = Counter()
    predicted_typology_counts: Counter[str] = Counter()
    any_rule_false_positives = 0
    exact_typology_matches = 0
    for transaction_id, row in by_id.items():
        truth_positive = bool(row["is_aml_suspicious"])
        truth_typology = row.get("aml_typology") if truth_positive else None
        predicted = sorted(predicted_by_transaction[transaction_id])
        predicted_positive = bool(predicted)
        if truth_positive and predicted_positive:
            overall_tp += 1
        elif not truth_positive and predicted_positive:
            overall_fp += 1
            any_rule_false_positives += 1
        elif truth_positive and not predicted_positive:
            overall_fn += 1
        else:
            overall_tn += 1

        if truth_typology:
            truth_typology_counts[str(truth_typology)] += 1
        predicted_typology_counts.update(predicted)
        matched = bool(truth_typology and truth_typology in predicted)
        if matched:
            exact_typology_matches += 1
        details.append({
            "transaction_id": transaction_id,
            "is_aml_suspicious": truth_positive,
            "aml_typology": truth_typology or "",
            "predicted_typologies": "|".join(predicted),
            "predicted_any": predicted_positive,
            "exact_typology_match": matched,
        })

    evaluated_count = len(by_id)
    total_count = total_ledger_transactions if total_ledger_transactions is not None else evaluated_count
    coverage = evaluated_count / total_count if total_count else 0.0
    report = {
        "evaluation_scope": "distinct transactions present in aml_event_inbox only",
        "evaluated_transactions": evaluated_count,
        "total_ledger_transactions": total_count,
        "ledger_transaction_coverage": coverage,
        "inbox_events": inbox_event_count if inbox_event_count is not None else evaluated_count,
        "ground_truth_aml_positive_transactions": sum(truth_typology_counts.values()),
        "ground_truth_typology_counts": dict(sorted(truth_typology_counts.items())),
        "predicted_typology_finding_counts": dict(sorted(predicted_typology_counts.items())),
        "overall_transaction_metrics": _binary_metrics(
            overall_tp, overall_fp, overall_fn, overall_tn
        ),
        "normal_transactions_flagged": any_rule_false_positives,
        "exact_typology_match_transactions": exact_typology_matches,
        "per_typology": per_typology,
        "limitations": [
            "Evaluation is limited to distinct transactions present in the AML inbox.",
            "Synthetic labels are generator-defined ground truth, not real-world confirmed laundering.",
            "A rule finding is an investigation lead, not proof of suspicious activity.",
            "The report measures this processed cohort and does not establish production performance.",
        ],
    }
    return report, details
