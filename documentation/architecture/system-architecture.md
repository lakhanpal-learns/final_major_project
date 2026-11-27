# Sentinel — Approved System Architecture

**Status:** Approved design  
**Scope:** Target architecture for the academic project  
**Banking environment:** One continuously operating synthetic bank

## 1. Purpose

Sentinel is a downstream banking fraud-detection and anti-money-laundering (AML) intelligence system. It simulates banking activity, processes transaction events, produces fraud and AML findings, and supports investigation.

Sentinel is not a core banking system and does not authorize, settle or block real payments.

## 2. Architecture overview

```text
Continuous Synthetic Banking Engine
              |
              v
PostgreSQL operational ledger
  + account balance updates
  + transactional outbox
              |
              v
       Outbox publisher
              |
              v
       Kafka transaction topic
          /       |        \
         /        |         \
        v         v          v
 Fraud consumer  AML consumer  Data-lake consumer
        |         |          |
        v         v          v
 Fraud model   Account/graph  Bronze Parquet
 inference     state + AML        |
        |      pattern checks    v
        v         |          Silver datasets
 Fraud results   v                |
 and alerts   AML results         v
        |      and alerts     Gold aggregates
        \         /               |
         v       v                v
     PostgreSQL result stores / analytical views
                  |
                  v
          Sentinel dashboard
```

The three Kafka consumer groups are independent. The data-lake consumer does not sit between Kafka and either detection pipeline.

## 3. Component responsibilities

### 3.1 Continuous Synthetic Banking Engine

- Runs as a separate background worker, not inside an API request handler.
- Generates transactions continuously while its run is active.
- Maintains persistent simulated account state and transaction history.
- Supports controlled start, pause, resume and stop operations.
- Uses a configurable event rate. The initial moderate demonstration setting is **10 transactions per second**; this is a proposed default, not a measured capacity claim.

FastAPI provides control and monitoring endpoints. It should not perform long-running transaction generation inside HTTP request handlers.

### 3.2 PostgreSQL operational store

PostgreSQL is the operational system of record for canonical customers, accounts, transactions, account state and Sentinel results.

A transaction write must persist the transaction, relevant account balance changes and an outbox event atomically. This avoids the dual-write failure where the database commit succeeds but publishing to Kafka fails, or vice versa.

### 3.3 Transactional outbox publisher

A separate publisher reads committed outbox rows and publishes corresponding events to Kafka. It retries failures and marks records as published only after broker acknowledgement.

The publisher may publish an event more than once after a crash or uncertain acknowledgement. Consumers therefore must be idempotent.

### 3.4 Kafka

Kafka transports committed banking events to independent consumer groups. The initial transaction event topic is `bank.transactions.v1`.

Kafka is an event transport and replay buffer within its configured retention period; PostgreSQL remains the operational source of truth. Kafka retention is finite and must be configured deliberately.

### 3.5 Fraud consumer

- Consumes each transaction event.
- Builds model input from approved features and available account context.
- Runs the loaded fraud model through a model-serving component.
- Persists prediction history and alerts in PostgreSQL.
- Records model version and prediction time.
- Does not use ground-truth labels or scenario metadata as model features.

### 3.6 AML consumer

- Consumes the same transaction stream independently.
- Updates account/network state and rolling aggregates.
- Evaluates patterns over relevant transaction windows rather than assuming every AML typology can be decided from one isolated transaction.
- Persists AML findings, alerts and supporting evidence in PostgreSQL.

The initial design is near-real-time for rolling pattern checks, with scheduled batch analysis for broader historical windows. A graph neural network is conditional on having suitable data, labels and a valid evaluation design; it is not required merely to claim an AML pipeline.

### 3.7 Data-lake consumer and local data lake

The independent data-lake consumer writes incoming raw events into local Bronze Parquet files in small batches. Scheduled Python jobs transform Bronze to Silver and Silver to Gold.

The local data lake is for history, replay support within retained source availability, analytics and reproducible model-development datasets. It is not the operational database and is not a prerequisite for real-time inference.

### 3.8 Dashboard and investigation

The dashboard presents live operational status, fraud and AML findings, and historical analytical views. Fraud and AML investigation workspaces remain logically separate, while case management can be shared.

## 4. Event and data integrity rules

1. Every event has a stable unique event ID.
2. Canonical customer and account IDs are shared across fraud and AML.
3. Fraud and AML labels are independent ground-truth fields and must be excluded from inference features.
4. Consumers must handle duplicate delivery idempotently.
5. A consumer commits its Kafka offset only after its result or durable work record has been persisted successfully.
6. Repeatedly failing events require retry handling and a dead-letter topic or equivalent quarantine mechanism.
7. Prediction history records the model version and timestamp; new predictions should not silently overwrite the audit history.
8. Event schemas are versioned (initial topic/schema version: v1).

## 5. Failure isolation

- If fraud inference is unavailable, Kafka can retain events within configured retention while AML and data-lake consumers continue independently.
- If AML processing is delayed, it must not block the fraud consumer group.
- If local file writing fails, the data-lake consumer must not commit the affected offsets until the event is safely written or durably staged.
- If PostgreSQL is unavailable, the outbox publisher and result-writing consumers must retry safely; they must not acknowledge work that has not been persisted.
- Recovery and duplicate-processing behavior must be tested. Kafka alone does not make the full system exactly-once across PostgreSQL, model inference and local files.

## 6. Proposed starting settings

| Setting | Initial value | Status |
|---|---:|---|
| Generation rate | 10 transactions/second | Configurable starting default |
| Fraud inference | Per event | Design requirement |
| AML state update | Per event | Design requirement |
| AML rolling pattern evaluation | Every 1–5 minutes | Proposed, to be measured |
| Bronze file flush | Small micro-batches | Threshold/time configurable |
| Silver transformation | Every 5–15 minutes | Proposed, to be measured |
| Gold aggregation | Hourly | Proposed, to be measured |
| Historical AML analysis | Daily | Proposed, to be measured |

These values are not benchmark results or claims about optimal production settings. Tune them after measuring processing latency, throughput, consumer lag and local disk usage.

## 7. Implementation boundary

This document records the approved target design. Existing finite seed generation and PostgreSQL persistence do not mean the continuous worker, Kafka integration, model-serving consumer, AML consumer or data lake are already implemented. Each must be implemented and tested in its own project task.
