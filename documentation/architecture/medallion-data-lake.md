# Sentinel — Local Medallion Data Lake

## 1. Purpose

The data lake preserves transaction-event history and provides reproducible datasets for analytics, reporting and model development. It is a separate analytical path from operational fraud and AML processing.

**Storage decision:** local filesystem.  
**File format:** Apache Parquet.  
**Initial processing:** Python and Pandas.  
**Layers:** Bronze, Silver and Gold.

This is a lightweight local implementation of a medallion-style data lake. It is not a claim that Sentinel has a distributed enterprise lakehouse.

## 2. Why three layers?

Each layer has a distinct data-quality contract:

| Layer | Meaning | Main purpose |
|---|---|---|
| Bronze | Raw, source-aligned events | Preserve what arrived and support audit/reprocessing |
| Silver | Validated, standardized records | Provide consistent reusable data |
| Gold | Curated aggregates/features | Support dashboards, reporting and selected model datasets |

Without layers, raw events, corrected records and business aggregates can become mixed together, making it difficult to know which dataset is suitable for which use.

## 3. Proposed local directory layout

```text
data_lake/
├── bronze/
│   └── transactions/
│       └── event_date=YYYY-MM-DD/
├── silver/
│   └── transactions/
│       └── event_date=YYYY-MM-DD/
├── gold/
│   ├── account_daily_activity/
│   ├── fraud_daily_metrics/
│   └── aml_network_metrics/
├── quarantine/
└── manifests/
```

Partition by event date initially. Avoid excessive partitioning by account ID because it can create many tiny files.

## 4. Bronze — raw event records

Bronze stores the original event payload as received from Kafka, with ingestion metadata such as:

- event ID
- event schema version
- source topic and partition
- Kafka offset
- event timestamp
- ingestion timestamp
- raw payload

Do not overwrite Bronze to correct data. Invalid records should be retained with failure details and routed to quarantine according to the data-quality policy.

**Reason:** preserving the original input allows later validation changes and historical reprocessing.

## 5. Silver — validated and standardized records

Silver transformations should:

- validate required fields and schema version;
- normalize timestamps to UTC while retaining original timezone context if present;
- validate canonical account references;
- detect duplicate event IDs;
- standardize numeric types and currency representation;
- validate transaction invariants and balance snapshots where applicable;
- separate ground-truth labels and generator metadata from model feature columns;
- record rejected records and reasons in quarantine.

Silver should not invent missing values or silently repair financial values. Any correction rule must be explicit and auditable.

**Reason:** downstream analysis should use a consistent, quality-checked dataset rather than independently cleaning raw data in every notebook/job.

## 6. Gold — curated analytical datasets

Initial Gold datasets:

| Dataset | Grain | Example contents | Purpose |
|---|---|---|---|
| account_daily_activity | Account + date | Incoming/outgoing totals, count, unique counterparties | Account behavior analysis |
| fraud_daily_metrics | Date + selected dimensions | Prediction/alert counts and evaluation metrics where labels are available | Operational reporting |
| aml_network_metrics | Account/network + window | Degree, in/out totals, fan-in/fan-out indicators | AML investigation and trend analysis |

Gold is not the source of truth for individual transactions. It is derived from Silver and should be reproducible.

## 7. Event and prediction data

The initial lake path ingests raw transaction events. Prediction and alert exports may be added as separate datasets later, with explicit model version and event linkage. Do not mix prediction outputs into raw transaction records or imply that a prediction is ground truth.

Ground-truth labels must be handled separately from model input features. For model evaluation, construct a controlled dataset with explicit label provenance and time-aware splitting.

## 8. File-writing and recovery requirements

- Write to a temporary file, then atomically rename to a completed Parquet file where the filesystem supports it.
- Maintain a manifest/checkpoint containing topic, partition, offset range, event count and file path.
- Do not commit Kafka offsets until the corresponding records are durably written or safely staged.
- Use stable event IDs for deduplication during recovery.
- Rotate files by configurable row count or elapsed time to avoid both tiny files and excessive memory use.
- Keep quarantine records and reasons.
- Define retention and cleanup policies before the lake grows without bound.

A local filesystem can be lost with the host machine. Backups are required if the data must survive machine failure.

## 9. Why not MinIO or cloud object storage now?

MinIO/S3-style object storage would provide an object API and make multi-service or multi-machine access easier. It also adds another service, credentials, configuration and operational failure modes.

For the current single-machine academic scope, local Parquet files demonstrate the data-lake concepts with fewer dependencies. If object-storage semantics or multi-machine deployment become real requirements, the same Parquet datasets can be moved behind an object-storage interface.

## 10. Why not Spark or Airflow now?

- **Spark:** distributed compute is not justified until measured data size or Pandas job duration exceeds local limits.
- **Airflow:** a small number of scheduled Python transformations can initially run through a simple scheduler. Adopt a workflow orchestrator only when dependencies, retries, backfills and monitoring become difficult to manage manually.

## 11. Initial schedule

| Job | Proposed initial schedule |
|---|---|
| Bronze ingestion | Continuous consumer, flush small micro-batches |
| Silver transformation | Every 5–15 minutes |
| Gold aggregation | Hourly |
| Longer AML historical aggregation | Daily |

These are configurable starting values, not proven optimal schedules. Measure job duration, event lag, memory and disk usage before tuning.
