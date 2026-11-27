# Sentinel — Data Flow and Processing Modes

## 1. Why hybrid processing?

Sentinel uses real-time, near-real-time and batch processing because its detection tasks have different latency and context requirements.

- **Real-time:** process an event as it arrives.
- **Near-real-time:** update state continuously, then evaluate a short rolling window or micro-batch.
- **Batch:** process a bounded historical dataset on a schedule.

These terms describe the target design. Actual latency must be measured after implementation.

## 2. End-to-end transaction flow

1. The continuous Synthetic Banking Engine creates a transaction using the shared canonical customer/account registry and current simulated account state.
2. One PostgreSQL transaction writes the transaction, updates relevant account state and inserts an outbox record.
3. The outbox publisher reads committed unpublished rows and publishes versioned events to Kafka.
4. Kafka delivers the event independently to the fraud, AML and data-lake consumer groups.
5. Each consumer persists its work before committing its offset, using idempotent handling for duplicates.

## 3. Fraud flow — per-event inference

For each transaction event:

1. Validate event schema and required fields.
2. Retrieve or calculate permitted account context.
3. Build model features from an explicit feature allowlist.
4. Exclude fraud/AML labels, scenario names, synthetic generator metadata and identifiers that could leak labels.
5. Run the loaded model.
6. Persist prediction, probability/score, model version and prediction timestamp.
7. Create or update a fraud alert according to configured decision rules.

The model does not train itself on each incoming event. Model training and evaluation use separate historical datasets and controlled jobs.

## 4. AML flow — near-real-time and batch

### Near-real-time path

1. Consume every transaction event.
2. Update account-level rolling state and graph edges.
3. Evaluate relevant short windows and typologies.
4. Persist findings with supporting transaction/account references.
5. Make findings available to the AML investigation workspace.

### Batch path

Scheduled jobs analyze longer historical windows, compute account/network aggregates, and support retrospective pattern discovery and model evaluation.

The batch path supplements streaming analysis; it does not replace the event consumer.

## 5. Data-lake flow

The independent data-lake consumer stores original event payloads and ingestion metadata in Bronze Parquet files. Scheduled transformations produce Silver and Gold datasets.

Lake writes must be recoverable. A Kafka offset must not be committed merely because an event was read; it should be committed only after the event has been safely written or durably staged. File rotation should use temporary files and atomic rename where supported, with a manifest/checkpoint strategy to identify completed files.

## 6. Independent consumer groups

| Consumer group | Reads | Main work | Writes |
|---|---|---|---|
| fraud-detector | `bank.transactions.v1` | Feature preparation and model inference | Fraud predictions/alerts in PostgreSQL |
| aml-detector | `bank.transactions.v1` | Account/network state and AML pattern checks | AML findings/alerts in PostgreSQL |
| data-lake-ingestor | `bank.transactions.v1` | Preserve raw event history | Bronze Parquet files |

A failure or backlog in one consumer group must not stop the others. Consumer lag should be monitored independently.

## 7. Initial schedules and tuning

| Work | Initial target |
|---|---|
| Fraud inference | Per event |
| AML graph/account state update | Per event |
| AML rolling-window evaluation | Every 1–5 minutes |
| Bronze writes | Small micro-batches, based on size or elapsed time |
| Bronze-to-Silver transformation | Every 5–15 minutes |
| Silver-to-Gold aggregation | Hourly |
| Longer historical AML analysis | Daily |

These are proposed starting settings for a moderate local demonstration. They are not measured optimal values. Configuration should expose intervals and batch-size thresholds. Tune using event throughput, end-to-end latency, consumer lag, job duration and disk consumption.

## 8. Reliability rules

- Stable unique event IDs.
- Idempotent database writes keyed by event ID and processing/model version as appropriate.
- Retry transient failures with bounded backoff.
- Quarantine poison events in a dead-letter topic or equivalent.
- Commit offsets only after durable processing.
- Track consumer lag and failed/retried event counts.
- Use event timestamps and ingestion timestamps distinctly.
- Define late-arriving event handling for AML windows and batch jobs.
- Preserve source events; do not silently mutate Bronze records.

## 9. Ground-truth and model-data separation

Synthetic fraud and AML labels are useful for controlled evaluation, but they must not be passed into production inference features. Scenario metadata and generator-only fields must also be excluded. Store labels in a restricted evaluation/training dataset or a separate label structure, and document how labels are generated.

Synthetic data can test system behavior and demonstrate pipeline mechanics. It cannot establish real-world detection accuracy, prevalence or fairness.
