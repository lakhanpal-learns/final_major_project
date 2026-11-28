# Task 15 — AML Rule Evaluation

## Objective

Measure the five AML rules against the synthetic ground-truth labels already stored on canonical transactions. Labels are used only after rule execution for offline evaluation; they are not passed to the AML rule engine.

## Evaluation cohort

The evaluator uses distinct transactions joined to `aml_event_inbox`. This is deliberate: a transaction that has not yet been processed by the AML consumer must not be counted as a missed detection. The report also includes total ledger size and inbox coverage so partial Kafka catch-up is visible.

## Metrics

- Overall transaction-level precision, recall, F1 and specificity for any AML finding.
- Per-typology precision, recall, F1 and specificity.
- True positives, false positives, false negatives and true negatives.
- Ground-truth typology counts, predicted finding counts, normal transactions flagged, and exact typology matches.
- A transaction-level CSV for audit and manual inspection.

For a typology-specific score, the ground truth is positive only when `is_aml_suspicious` is true and `aml_typology` equals that typology. Predictions are based only on persisted AML findings.

## Run

With PostgreSQL running and the root `.env` loaded into the PowerShell process:

```powershell
cd backend
python -m app.evaluate_aml_rules
```

Outputs:

- `backend/models/aml/aml_rule_evaluation.json`
- `backend/models/aml/aml_rule_evaluation_transactions.csv`

## Interpretation limits

- Scores apply only to distinct transactions already present in the AML inbox at evaluation time. Re-run after Kafka catch-up for a larger cohort.
- Synthetic labels are generator-defined experimental labels, not verified real-world laundering outcomes.
- A finding is an investigation lead, not proof of suspicious activity.
- The fixed demonstration rules and thresholds are not regulatory definitions or validated bank policy.
- These metrics do not establish production effectiveness, generalization, or regulatory compliance.
