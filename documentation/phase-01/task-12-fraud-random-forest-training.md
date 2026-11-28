# Task 12 — Fraud Random Forest Baseline Training

## Objective

Train a reproducible fraud-classification baseline from the Task 11 prepared CSV. This task does not yet connect model inference to Kafka or create operational fraud alerts.

## Model

- Scikit-learn `Pipeline` containing a `ColumnTransformer` and `RandomForestClassifier`.
- One-hot encode currency, transaction type, channel, sender account type and receiver account type.
- Pass numeric features through without scaling; tree-based Random Forest models do not require feature standardization.
- Use 400 trees by default, `class_weight="balanced_subsample"`, `min_samples_leaf=2`, and random state 42.
- Unknown categorical values at inference time are ignored by `OneHotEncoder(handle_unknown="ignore")`.

These are initial baseline settings, not proven optimal hyperparameters.

## Data split discipline

The trainer reads the precomputed `dataset_split` column from Task 11. It does not randomly split or shuffle records.

1. Fit preprocessing and classifier using only `train`.
2. Generate validation probabilities and select a threshold that maximizes validation F1. Ties prefer higher precision.
3. Freeze that threshold.
4. Evaluate the test partition once at the selected threshold.

The test partition is not used for fitting, preprocessing vocabulary selection, or threshold tuning. All three partitions must contain both target classes or training stops with a clear error.

## Metrics

The JSON report contains precision, recall, F1, average precision (PR-AUC), ROC-AUC and a confusion matrix for validation and test at the selected threshold. Accuracy is intentionally not the primary metric because the fraud target is imbalanced.

PR-AUC is reported as scikit-learn average precision; it is not trapezoidal integration of the precision-recall curve.

## Files and outputs

- `backend/app/services/fraud_model.py`: dataset validation, preprocessing, training, validation threshold selection, test evaluation and artifact writing.
- `backend/app/train_fraud_model.py`: command-line entry point.
- `backend/tests/test_fraud_model.py`: training and invalid-split regression tests.
- Default model artifact: `backend/models/fraud/fraud_random_forest.joblib`.
- Default metrics report: `backend/models/fraud/fraud_model_metrics.json`.

Model artifacts are local generated outputs and should not be committed as source code. The metrics JSON records model settings, split counts, library versions, selected threshold and evidence limitations.

## Run

From the backend directory, after Task 11 dataset preparation:

```powershell
python -m pip install -r requirements-dev.txt
python -m app.train_fraud_model
```

Optional settings:

```powershell
python -m app.train_fraud_model --n-estimators 400 --random-state 42
```

## Evidence boundary and limitations

The model is trained and evaluated against deterministic, rule-generated synthetic banking transactions. A strong score can show that the model learned patterns present in this generator; it does not establish real-bank accuracy, generalization, calibration, fairness, or production readiness. The current feature set is transfer-focused and excludes device, merchant, login, geolocation and KYC signals. Real-world validation requires independently labelled, representative bank data and a time-separated evaluation design.
