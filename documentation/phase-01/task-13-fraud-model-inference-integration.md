# Task 13 — Fraud Model Inference Integration

## Objective

Connect the frozen Fraud Random Forest V2 artifact to the existing Kafka fraud consumer. This task adds online scoring; it does not retrain the model, tune the threshold, or alter the V2 artifacts.

## Runtime flow

1. The fraud consumer receives a record from `bank.transactions.v1`.
2. The existing strict Pydantic event contract validates the payload.
3. The consumer checks that the transaction exists in the canonical PostgreSQL ledger and that event fields match the ledger row.
4. Point-in-time features are built from canonical ledger history strictly earlier than the transaction timestamp.
5. The frozen V2 pipeline calculates a fraud probability.
6. The threshold loaded from the preserved V2 metrics JSON is applied.
7. The validated inbox record and fraud prediction are committed in one PostgreSQL transaction.
8. The Kafka offset is committed only after that database transaction succeeds.

## Model artifact configuration

Default files are:

- `backend/models/fraud/fraud_random_forest_v2.joblib`
- `backend/models/fraud/fraud_model_metrics_v2.json`

Override paths with:

- `FRAUD_MODEL_PATH`
- `FRAUD_MODEL_METRICS_PATH`

The model and threshold are loaded once when the consumer starts. The consumer does not train or tune a model.

## Online feature contract

The online service emits the same ordered 20 fields in `app.services.fraud_features.FEATURE_COLUMNS`. It uses the current transaction payload plus canonical account types and prior completed transactions from PostgreSQL. History windows are strictly earlier than the current transaction timestamp; events sharing a timestamp therefore do not leak into each other's features.

The model input excludes transaction IDs, customer/account identifiers, fraud labels, AML labels, scenario/typology metadata, and post-transaction balance snapshots.

The current transaction must already exist in the canonical ledger. A missing or mismatched ledger row fails processing rather than scoring an unverified event.

## Persistence

The new `fraud_predictions` table stores:

- transaction ID and source event ID
- model version
- fraud probability
- frozen decision threshold
- binary model decision
- feature snapshot used for scoring
- prediction timestamp

The transaction ID is the primary key, so one prediction is stored per canonical transaction for this baseline.

## Delivery and failure behavior

Inbox insertion and inference persistence share one database transaction. If feature construction, model inference, or prediction persistence fails, the inbox insert rolls back and the Kafka offset is not committed. Kafka can redeliver the event after the consumer restarts.

This is at-least-once event delivery with idempotent persistence, not a claim of distributed exactly-once processing. Invalid events continue to be durably quarantined by the existing consumer path.

## Local run

From the `backend` directory, ensure PostgreSQL, Kafka, and the V2 model backup files are available, then run:

```powershell
python -m app.consume_fraud
```

## Validation scope and limitations

Automated tests use SQLite and a deterministic fake classifier to verify feature ordering, label exclusion, prediction persistence, duplicate handling, rollback behavior, and missing-ledger fail-closed behavior. These tests do not prove production throughput or real-world model performance.

The online feature implementation should be compared against offline feature generation on the same ordered canonical ledger cohort before any production claim. PostgreSQL integration and end-to-end Kafka inference must also be exercised locally.
