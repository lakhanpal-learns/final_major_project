# Task 12.1 — Fraud Scenario-wise Evaluation

## Purpose
Measure how the already-trained Random Forest behaves on each synthetic fraud scenario in the chronological test partition. This is post-hoc evaluation, not model training.

## What is measured
- Fraud transaction count, detected count, missed count and recall for each scenario label present in the test split.
- False-positive rate for normal-labelled test transactions with amount >= ₹50,000 (configurable proxy for high-value normal activity).
- The model threshold is read from the existing metrics JSON. It is not retuned.

## Leakage controls
- Scenario metadata and transaction IDs are never passed to `predict_proba`.
- Only the existing 15 model feature columns are passed to the model.
- Raw records are sorted by UTC timestamp and transaction ID, matching Task 11 feature row ordering.
- Evaluation aborts if raw labels do not align exactly with the prepared dataset labels.
- Only rows marked `test` are scored.

## Run
From `backend`:
```powershell
python -m app.evaluate_fraud_scenarios
Get-Content .\models\fraud\fraud_scenario_metrics.json
```

## Interpretation and limitations
Per-scenario recall indicates the fraction of generated positive transactions detected for each named scenario. It does not establish real-world typology detection. The normal high-value group uses an explicit amount cutoff because the generator does not currently export a dedicated hard-negative subtype label. Some scenarios may have few test examples; always report denominators alongside rates. The cross-domain overlap label is reported separately if it occurs in the test partition.
