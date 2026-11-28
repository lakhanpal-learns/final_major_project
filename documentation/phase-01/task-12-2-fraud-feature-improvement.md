# Task 12.2 — Short-Window Velocity and Recipient-Concentration Features

## Why
The first scenario evaluation showed weaker recall for rapid repeated transfers (50%), multi-recipient bursts (48.15%) and repeated-beneficiary bursts (46.15%). These results identify candidate feature gaps; they do not prove feature engineering alone caused the misses.

## Changes
The leakage-safe feature allowlist now includes:
- Sender transaction counts in the prior 5 and 15 minutes.
- Number of distinct recipients in the prior hour.
- Maximum number of prior-24-hour transfers to a single recipient.
- Largest recipient's share of the sender's prior-24-hour transfers.

These features are calculated only from transactions strictly earlier than the scored transaction. Same-timestamp events are still evaluated from a shared history snapshot. IDs, scenario labels and fraud labels are not model inputs.

## Expected use
Short-window velocity features may help characterize rapid transfer sequences. Recipient diversity may help characterize multi-recipient bursts. Recipient concentration may help characterize repeated-beneficiary behaviour. These are hypotheses to test, not guaranteed improvements.

## Run from backend
```powershell
python -m pytest
python -m app.prepare_fraud_dataset
python -m app.train_fraud_model
python -m app.evaluate_fraud_scenarios
```

## Evaluation discipline
The baseline metrics and original test report should be retained for comparison. The new model uses the same chronological split policy and validation-only threshold selection. Compare overall precision, recall, F1, PR-AUC and ROC-AUC, then compare per-scenario recall and normal high-value false-positive rate. Do not change the test set or tune on test results.

## Limitations
This is a feature experiment on rule-generated synthetic data. A better result would show improved fit to these simulator scenarios only; it would not establish real-world fraud detection capability. If the model fails to improve, investigate scenario construction, temporal distribution, label quality and account-history coverage before adding complexity.
