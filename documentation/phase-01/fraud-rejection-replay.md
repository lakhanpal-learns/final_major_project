# Fraud rejection replay

## Why this exists

The fraud consumer durably quarantines malformed Kafka messages in
`fraud_event_rejections` and commits their Kafka offsets after the quarantine
row is stored. Fixing an event parser therefore does not cause already
quarantined messages to be delivered again.

## Recovery procedure

1. Stop the fraud Kafka consumer to prevent concurrent processing.
2. Ensure the event schema fix is deployed locally and tests pass.
3. Load the normal project environment variables.
4. Run `python -m app.replay_fraud_rejections` from the `backend` directory.
5. Verify fraud inbox and predictions against the canonical transaction count.
6. Restart the fraud Kafka consumer.

The replay reads the stored raw payload and original Kafka source position,
then calls the same `process_fraud_message` path used by the live consumer.
Consequently, normal schema validation, inbox idempotency, canonical-ledger
checks, model inference, and prediction persistence remain in force.

## Audit and safety properties

- Rejection rows are never deleted or modified by the replay.
- Existing inbox events are not duplicated.
- Existing predictions are not duplicated.
- An inbox event without a prediction can be scored through the existing
  recovery path.
- A malformed payload remains quarantined and is counted as rejected again.
- Database/inference failures propagate and stop the replay; rerunning is safe
  because successful records are idempotent.

This utility does not reset Kafka offsets, republish events, retrain the model,
or change historical rejection evidence.
