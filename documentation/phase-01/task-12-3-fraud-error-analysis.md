# Task 12.3 — Fraud Model Error Analysis

## Objective

Add a reproducible post-hoc diagnostic for the trained fraud Random Forest. The task identifies false-negative and false-positive test transactions, summarizes missed fraud by synthetic scenario, and reports global feature importance.

## Inputs

- Existing prepared fraud dataset and its precomputed chronological train/validation/test split.
- Existing trained model artifact and frozen validation-selected threshold.
- Raw synthetic transaction export, used only to restore transaction identifiers and audit metadata.

The CLI verifies row counts and fraud-label order between the raw and prepared datasets before scoring. Scenario metadata and identifiers are never passed to the model.

## Outputs

- `backend/models/fraud/fraud_error_analysis.json`: aggregate counts, scenario miss rates, and top original input features.
- `backend/models/fraud/fraud_error_transactions.csv`: row-level false positives and false negatives, with transaction audit fields, model probability, threshold, error type, and model feature values.

## Run

From the backend directory:

```powershell
python -m app.analyze_fraud_errors
```

## Interpretation limits

- Feature importance is Random Forest impurity-based global importance. It is not a per-transaction explanation, causal effect, or proof that a feature independently drives fraud.
- The analysis reuses the existing test partition and is diagnostic, not an independent final evaluation.
- Synthetic rule-generated labels cannot establish real-world fraud detection performance.
- Scenario names are used only after inference to group errors; they are not model inputs.
- False negatives and false positives are review candidates, not automatically confirmed banking incidents.
