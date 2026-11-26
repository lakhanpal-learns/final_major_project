# SENTINEL
## Unified Banking Fraud Detection and Anti-Money Laundering Investigation System

**Document:** Software Requirements Specification (SRS)  
**Project level:** 7th-semester engineering major project  
**Status:** Approved project scope / implementation target  
**System type:** Full-stack banking intelligence application

---

## 1. Project Overview

Sentinel is a unified banking intelligence platform for detecting potentially fraudulent financial transactions and suspicious money-laundering activity in a simulated banking environment.

The system represents **one simulated bank** with one canonical registry of customers and bank accounts. A shared transaction ledger records financial activity between those accounts.

Two independent scenario-generation paths create labelled synthetic activity:

1. **PaySim-inspired fraud scenarios** for transaction-level fraud detection.
2. **AMLSim-inspired AML scenarios** for account-network and money-flow analysis.

Both paths must use the same canonical customer IDs and account IDs. PaySim and AMLSim identifiers must never be assumed to refer to the same entities; any imported/generated records must be explicitly mapped to Sentinel's canonical registry.

Transactions are stored in PostgreSQL and published continuously to Apache Kafka. Separate fraud and AML consumers process the events using their respective feature pipelines and detection methods.

The application provides two distinct investigation workspaces:
- **Fraud workspace:** transaction-centred investigation of a flagged payment and its connected transaction network.
- **AML workspace:** account-centred investigation of suspicious money-flow patterns and connected accounts.

The workspaces share customer/account identity and case-management infrastructure, but retain separate models, labels, alerts and investigation logic.

## 2. Problem Statement

Banks process large volumes of transactions between interconnected accounts. Detecting an unusual individual payment and identifying broader suspicious money flows are related but different analytical tasks.

Transaction-level fraud detection focuses on whether a particular payment may be fraudulent. AML monitoring focuses on patterns across multiple transactions and accounts, such as fan-in, fan-out, structuring, layering and circular transfers.

Sentinel addresses these tasks through one shared synthetic banking environment, continuous event streaming, independent fraud and AML detection pipelines, graph-based investigation and a unified case-management application.

## 3. Objectives

1. Generate synthetic customers, accounts and transactions for one simulated bank.
2. Maintain one canonical customer and account identity registry.
3. Generate normal, fraud-related and AML-related transaction scenarios.
4. Store banking records in PostgreSQL.
5. Stream transaction events through Apache Kafka.
6. Train and evaluate a Random Forest fraud classifier.
7. implement AML typology detection and a Graph Neural Network (GNN) where labelled graph data supports it.
8. Construct transaction-centred fraud graphs and account-centred AML graphs.
9. Provide separate fraud and AML investigation workspaces.
10. Implement shared case management, reporting and operational monitoring.
11. Measure model performance and end-to-end streaming performance.

## 4. Scope and Boundaries

### 4.1 In scope

- One simulated bank.
- Shared synthetic customers and accounts.
- One canonical transaction ledger.
- PaySim-inspired fraud scenario generation.
- AMLSim-inspired AML scenario generation.
- PostgreSQL persistence.
- Database-to-Kafka event publishing.
- Independent fraud and AML processing pipelines.
- Random Forest fraud detection.
- AML typology detection and GNN-based analysis.
- Interactive transaction and account graphs.
- Separate investigation workspaces.
- Shared case management.
- Reporting, exports and pipeline/model monitoring.

### 4.2 Out of scope for the first version

- Integration with real bank core systems.
- Real customer or personally identifiable banking data.
- Dependence on a third-party live banking transaction API.
- Multi-bank interbank payment-network simulation.
- Production-grade regulatory filing or automatic SAR submission.
- Claims of production banking accuracy or capacity without evidence.

Multiple banks may be considered as a future extension, but the initial project uses one bank with many interconnected accounts.

## 5. High-Level Architecture

The system consists of these layers:

1. **Synthetic Banking Data Generation:** Python-based shared customer, account and transaction generation, with separate fraud and AML scenario logic.
2. **Shared Database:** PostgreSQL stores canonical customers, accounts, transactions, labels, outbox events, predictions, alerts and cases.
3. **Event Publishing:** Outbox publishers publish committed transaction events to Kafka.
4. **Event Streaming:** Kafka distributes events to independent fraud and AML consumers.
5. **Fraud Pipeline:** Fraud consumer, feature engineering, Random Forest inference, transaction graph construction and fraud alerts.
6. **AML Pipeline:** AML consumer, account graph construction, typology detection/GNN inference and AML findings.
7. **Backend:** FastAPI exposes data, investigation, alert, case, report and health APIs.
8. **Frontend:** Next.js provides the Sentinel application and separate investigation workspaces.

### 5.1 Logical flow

```text
             ONE SIMULATED BANK
                      |
       Shared Customer and Account Registry
                      |
       Shared Transaction Generation/Ledger
                /             \
       Fraud Scenarios      AML Scenarios
       (PaySim-inspired)   (AMLSim-inspired)
                \             /
                 PostgreSQL
                      |
              Transactional Outbox
                      |
                    Kafka
                 /        \
          Fraud Consumer  AML Consumer
                |              |
          Feature Prep     Graph Building
                |              |
          Random Forest    Typology + GNN
                |              |
          Fraud Alerts     AML Findings
                \              /
                 FastAPI Backend
                      |
               Next.js Application
                      |
        Separate Fraud and AML Workspaces
             Shared Case Management
```

The diagram is logical. The actual implementation may use separate topics and consumer groups, but canonical identities and transaction IDs must remain consistent.

## 6. Shared Synthetic Banking Data Requirements

### 6.1 Customer generator

Generate synthetic customer records with:

- `customer_id` — canonical unique identifier.
- `full_name`.
- `date_of_birth` — synthetic value.
- `address` — synthetic value.
- `customer_type`.
- `registration_date`.

### 6.2 Account generator

Generate bank account records with:

- `account_id` — canonical unique identifier.
- `account_number` — unique display/account number.
- `customer_id` — foreign key to customer.
- `account_type`.
- `account_balance`.
- `currency`.
- `account_status`.
- `account_open_date`.

Every account must reference an existing customer. If joint accounts are supported later, the data model must explicitly represent multiple account holders rather than duplicating identities.

### 6.3 Transaction generator and canonical ledger

Generate transaction records with:

- `transaction_id` — canonical unique identifier.
- `sender_account_id`.
- `receiver_account_id`.
- `amount`.
- `currency`.
- `transaction_type`.
- `transaction_timestamp`.
- `channel`.
- `transaction_status`.
- `scenario_source` or scenario metadata.

Every sender and receiver must reference valid canonical accounts. Preserve transaction IDs and account IDs across database records, Kafka events, model outputs, graph edges, alerts and investigation evidence.

### 6.4 Fraud scenario generation

The PaySim-inspired generator must create normal and fraud-labelled transaction scenarios using shared Sentinel accounts.

Candidate scenarios/features include:

- Normal transfers.
- Unusual transaction amounts.
- Unusual transaction frequency.
- Rapid successive transfers.
- Suspicious balance movements.
- Fraudulent transfers.
- Connected suspicious transactions.

Potential model features:

- Transaction amount and type.
- Sender and receiver account references.
- Sender balance before and after.
- Receiver balance before and after, where available.
- Transaction time.
- Derived transaction frequency and behavioural features.

Target label: `is_fraud`.

The generator must define explicit scenario rules and labels. It must not assign fraud labels randomly merely to create a class balance.

### 6.5 AML scenario generation

The AMLSim-inspired generator must create connected account and transaction patterns using the same Sentinel accounts.

Required typologies:

| Typology | Description |
|---|---|
| Fan-in | Multiple accounts transfer funds to one account |
| Fan-out | One account transfers funds to multiple accounts |
| Circular transfers | Funds move through a sequence of accounts and return to an earlier account |
| Structuring | Activity is divided into smaller transactions to evade attention |
| Layering | Funds move through multiple accounts to obscure their origin |
| Rapid movement | Funds are transferred onward soon after receipt |

Required AML data:

- Origin and beneficiary account IDs.
- Transaction amount and timestamp.
- Transaction type.
- Account relationships and transaction history.
- Scenario/typology metadata.
- AML suspicious-activity label.

Target label: `is_aml_suspicious`, with a separate `aml_typology` where applicable.

The AML generator must create meaningful graph structures and known scenario labels. A GNN requires correctly constructed graph examples and labels matching its prediction target.

### 6.6 Independent labels

Fraud and AML are different targets. Store them independently.

A transaction may be normal, fraud-related, AML-related, both, or neither, according to explicitly defined generator rules. Do not copy PaySim fraud labels into AML labels or AML SAR labels into fraud labels.

A generated scenario label is ground truth for the synthetic experiment, not proof that a real-world crime occurred.

## 7. Synthetic Banking API

Build an internal FastAPI service to control generation and retrieve synthetic banking data.

| Endpoint | Function |
|---|---|
| `GET /customers` | List/search customers |
| `GET /accounts` | List/search accounts |
| `GET /transactions` | List/filter transactions |
| `GET /transactions/{id}` | Retrieve one transaction |
| `POST /simulate/fraud` | Generate fraud scenarios |
| `POST /simulate/aml` | Generate AML scenarios |
| `POST /simulate/start` | Start continuous generation |
| `POST /simulate/pause` | Pause generation |
| `POST /simulate/resume` | Resume generation |
| `POST /simulate/stop` | Stop generation |
| `GET /simulate/status` | Read generator status |

Generation rates must be configurable, for example 10, 50 or 100 transactions per second for local testing. These are test settings, not production capacity claims.

The API is an internal synthetic-data API; no external live banking API is required.

## 8. PostgreSQL Requirements

Recommended logical tables:

| Table | Purpose |
|---|---|
| `customers` | Canonical customer records |
| `accounts` | Canonical bank accounts |
| `transactions` | Shared transaction ledger |
| `transaction_labels` | Independent fraud and AML scenario labels |
| `outbox_events` | Events awaiting publication |
| `fraud_predictions` | Fraud model outputs |
| `aml_predictions` | AML model outputs |
| `fraud_alerts` | Fraud alerts |
| `aml_alerts` | AML findings/alerts |
| `investigation_cases` | Shared case records |
| `case_evidence` | Evidence attached to cases |
| `model_registry` | Model version and evaluation metadata |
| `pipeline_status` | Pipeline health and processing metadata |

Database requirements:

- Primary keys and foreign keys for canonical entities.
- Unique customer, account, transaction and event identifiers.
- Indexes for transaction time, sender, receiver, account and alert status.
- Independent fraud and AML labels and outputs.
- Transactional creation of a transaction and its outbox event.
- Retention of source/scenario metadata.
- No silent overwriting of original transaction data by model predictions.

## 9. Kafka Streaming Requirements

Recommended topics:

- `bank.transactions` — canonical transaction events.
- `fraud.events` — fraud processing events, if a separate routed topic is needed.
- `aml.events` — AML processing events, if a separate routed topic is needed.
- `fraud.alerts` — fraud detection results.
- `aml.alerts` — AML detection results.

The implementation may consume `bank.transactions` directly in two independent consumer groups instead of duplicating events into `fraud.events` and `aml.events`. Choose one approach and document it; avoid unnecessary duplicate routing.

Event envelope should include:

- `event_id`.
- `event_type`.
- `transaction_id`.
- `event_timestamp`.
- `schema_version`.
- `payload`.

Publishing workflow:

1. Insert the canonical transaction and outbox event in one PostgreSQL transaction.
2. Publisher reads unpublished outbox records.
3. Publisher sends the event to Kafka.
4. Publisher marks the outbox record published after acknowledgement.
5. Fraud and AML consumers process events independently.
6. Consumers persist predictions/findings and processing status.

Implement retries, dead-letter/error handling and idempotent consumers. Kafka and PostgreSQL do not form one automatic atomic transaction; design for at-least-once delivery and safe duplicate handling.

## 10. Fraud Detection System

### 10.1 Model

Implement a Scikit-learn Random Forest classifier.

Requirements:

- Reproducible training pipeline.
- Feature preprocessing fitted on training data only.
- Feature/schema validation at inference.
- Class-imbalance handling where justified.
- Fraud probability or score.
- Configurable alert threshold.
- Model version and training metadata.
- Persisted inference results.

### 10.2 Fraud investigation workspace

The workspace must support:

1. Fraud alert queue and filters.
2. Search by transaction ID, account number and holder name.
3. Transaction detail view.
4. Sender and receiver account/holder details.
5. Connected transaction exploration.
6. Incoming and outgoing transfer analysis.
7. Transaction-centred network graph.
8. Fraud model output and relevant features.
9. Case creation and evidence capture.

### 10.3 Fraud graph

- Nodes represent canonical bank accounts.
- Directed edges represent actual transactions.
- Edge direction represents movement of money.
- Edge details include amount, currency, timestamp and transaction ID.
- Selected suspicious transaction is visually highlighted.
- Investigator can expand connected accounts and filter by time/amount.

The graph must be constructed from stored transaction records, not invented links.

## 11. AML Detection and Investigation System

### 11.1 Detection engine

Use Python graph processing and PyTorch/PyTorch Geometric for GNN implementation.

Requirements:

- Construct account relationship graphs from canonical transactions.
- Define node and edge features.
- Define the prediction target (account, transaction, subgraph or scenario).
- Create labelled training/validation/test graph data.
- Implement AML typology detection.
- Train and evaluate a GNN if the selected labelled graph data supports it.
- Persist findings, scores, model version and supporting evidence.

Rule/typology detection and GNN prediction must be distinguishable in outputs. Do not claim GNN detection when only a rule has fired.

### 11.2 AML investigation workspace

The workspace must support:

1. Suspicious account and pattern queue.
2. Account-holder information.
3. Account transaction history.
4. Incoming/outgoing transfer analysis.
5. Fan-in and fan-out exploration.
6. Circular-flow detection and path inspection.
7. Structuring investigation.
8. Layering and multi-hop tracing.
9. Suspicious cluster exploration.
10. AML model/rule findings and evidence.
11. Case creation and investigator notes.

### 11.3 AML graph

- Nodes represent canonical bank accounts.
- Directed edges represent money transfers.
- Nodes display account numbers and holder names.
- Edges display transaction amounts, direction and timestamps.
- Suspicious paths and clusters are highlighted.
- Investigators can expand relationships and inspect source transactions.

## 12. Shared Case Management

Fraud and AML workspaces must create cases in one shared case-management module.

Required capabilities:

- Create a case from a fraud alert or AML finding.
- Preserve case type and source alert ID.
- Assign an investigator.
- Change case status.
- Add notes and evidence.
- Link relevant canonical accounts and transactions.
- Maintain an auditable case timeline.
- Record investigation conclusions.
- Export case reports.

Suggested statuses: Open, Under Investigation, Escalated, Resolved and Closed.

A shared case system does not mean that fraud and AML alerts or labels are merged.

## 13. Frontend Application

Build a complete Next.js/React application.

Required navigation:

1. Overview.
2. Transactions.
3. Fraud Investigation.
4. Fraud Alerts.
5. AML Investigation.
6. AML Alerts.
7. Case Management.
8. Reports.
9. Pipeline Monitoring.
10. Model Monitoring.

### 13.1 Overview

Display data-backed metrics:

- Total customers and accounts.
- Total transactions.
- Fraud alerts.
- AML findings/alerts.
- Open investigation cases.
- Current generation rate.
- Kafka processing status.
- Fraud and AML model status.

### 13.2 Transactions

Provide search/filter by:

- Transaction ID.
- Account number.
- Account-holder name.
- Date range.
- Amount range.
- Transaction type.
- Fraud/AML scenario indicators.

### 13.3 Pipeline monitoring

Show actual metrics from system health checks:

- Generator status.
- PostgreSQL connectivity.
- Kafka broker/topic status.
- Published and consumed event counts.
- Consumer lag.
- Processing failures.
- Last successful processing time.
- Dead-letter/error count, if implemented.

### 13.4 Model monitoring

Show:

- Model name and version.
- Model status.
- Last inference time.
- Evaluation metrics from recorded runs.
- Prediction and alert counts.
- Training/evaluation dataset version.

Do not display invented scores, hard-coded health statuses or sample metrics as live system measurements.

## 14. Reports and Exports

Support appropriate CSV/PDF exports for:

- Transaction search results.
- Account transaction history.
- Fraud investigation reports.
- AML investigation reports.
- Case reports.
- Model evaluation.
- Pipeline performance.

Reports must identify synthetic data and distinguish model predictions from investigator conclusions.

## 15. Technology Stack

| Layer | Technology |
|---|---|
| Data generation | Python |
| Internal API | FastAPI |
| Database | PostgreSQL |
| Event streaming | Apache Kafka |
| Publisher/consumer | Python Kafka client |
| Data processing | Python, Pandas |
| Fraud model | Scikit-learn Random Forest |
| Graph processing | NetworkX |
| AML model | PyTorch, PyTorch Geometric |
| Frontend | Next.js, React |
| Graph visualisation | Cytoscape.js or React Flow |
| Local deployment | Docker Compose |

Do not add Spark, Flink, Kubernetes or other infrastructure solely to enlarge the technology list. Add them only if implementation requirements justify them.

## 16. Functional Requirements

| ID | Requirement | Priority |
|---|---|---|
| FR-01 | Generate synthetic customers | Must |
| FR-02 | Generate synthetic bank accounts | Must |
| FR-03 | Maintain one shared canonical identity registry | Must |
| FR-04 | Generate normal transactions | Must |
| FR-05 | Generate labelled fraud scenarios | Must |
| FR-06 | Generate labelled AML scenarios | Must |
| FR-07 | Maintain one canonical transaction ledger | Must |
| FR-08 | Persist data in PostgreSQL | Must |
| FR-09 | Publish events from database to Kafka | Must |
| FR-10 | Implement Random Forest fraud inference | Must |
| FR-11 | Construct transaction-centred fraud graphs | Must |
| FR-12 | Implement AML typology detection | Must |
| FR-13 | Implement and evaluate GNN-based AML detection | Must |
| FR-14 | Construct account-centred AML graphs | Must |
| FR-15 | Generate independent fraud and AML findings | Must |
| FR-16 | Provide separate investigation workspaces | Must |
| FR-17 | Implement shared case management | Must |
| FR-18 | Provide account and transaction search | Must |
| FR-19 | Provide operational pipeline monitoring | Must |
| FR-20 | Generate investigation reports | Must |
| FR-21 | Support generator start/pause/resume/stop | Must |
| FR-22 | Provide model evaluation and version tracking | Should |

## 17. Non-Functional Requirements

- **Consistency:** Same customer/account IDs must resolve to the same canonical entities in both workspaces.
- **Reliability:** Publishers and consumers must retry safely and handle duplicate events.
- **Integrity:** Transactions must reference valid accounts.
- **Auditability:** Investigator actions and case status changes must be recorded.
- **Security:** Provide authentication, role-based access and safe handling of secrets.
- **Maintainability:** Keep fraud and AML pipelines modular and independently testable.
- **Performance:** Measure throughput, latency and consumer lag rather than making unsupported capacity claims.
- **Reproducibility:** Record generator configuration, random seeds where practical, data versions and model versions.
- **Transparency:** Clearly distinguish synthetic scenario labels, model predictions, rule findings and human investigation outcomes.

## 18. Evaluation and Testing

### 18.1 Fraud model

Evaluate using:

- Precision.
- Recall.
- F1-score.
- ROC-AUC.
- Confusion matrix.

### 18.2 AML model

Evaluate using:

- Precision.
- Recall.
- F1-score.
- PR-AUC where appropriate.
- Confusion matrix.
- Detection performance by AML typology.

### 18.3 Streaming pipeline

Measure:

- Events published per second.
- Events consumed per second.
- End-to-end processing latency.
- Consumer lag.
- Failed events.
- Duplicate-safe processing behaviour.

### 18.4 Application and data integrity

Test:

- Shared identity consistency across workspaces.
- Valid transaction/account references.
- Scenario label correctness.
- Kafka retry and replay behaviour.
- Graph edges matching stored transactions.
- Alert-to-case evidence traceability.
- API response times and workflow correctness.

Use independent training, validation and test data. Avoid leakage of related accounts or generated scenarios across splits; time-based or scenario-based evaluation is preferable. Compare ML models with simple rule-based baselines.

## 19. Implementation Phases

**Phase 1 — Shared banking foundation**
- Customer and account registry.
- Canonical transaction schema.
- PostgreSQL database and integrity checks.

**Phase 2 — Scenario generation**
- Normal transaction generation.
- PaySim-inspired fraud scenarios.
- AMLSim-inspired AML scenarios.
- Independent labels and shared account mapping.

**Phase 3 — Streaming**
- Kafka setup.
- Transactional outbox.
- Publishers and consumers.
- Retry and idempotency tests.

**Phase 4 — Fraud detection**
- Feature engineering.
- Random Forest training/evaluation.
- Streaming inference.
- Fraud alerts and transaction graphs.

**Phase 5 — AML detection**
- Account graph construction.
- Typology rules.
- GNN data preparation, training and evaluation.
- AML findings and account-network investigation.

**Phase 6 — Application**
- FastAPI endpoints.
- Next.js navigation and workspaces.
- Shared case management.
- Reporting and exports.

**Phase 7 — Integration and validation**
- End-to-end tests.
- Pipeline monitoring.
- Model evaluation.
- Performance measurements.
- Demonstration scenarios.

## 20. Acceptance Criteria

The project is complete when the following can be demonstrated:

- One simulated bank with shared customer and account registries.
- Multiple interconnected accounts and transactions.
- Independently generated fraud and AML scenarios using the same canonical accounts.
- One persistent transaction ledger in PostgreSQL.
- Continuous database-to-Kafka event publishing.
- Actual Random Forest fraud predictions from a trained model.
- Actual AML typology findings and GNN predictions from a trained/evaluated model.
- Fraud workspace that investigates a selected transaction and its connected network.
- AML workspace that investigates account-level money flows and typologies.
- Same account holder and account number visible consistently in both workspaces.
- Persisted alerts/findings and traceable supporting transactions.
- Shared case management.
- Functional reports and exports.
- Monitoring based on actual pipeline metrics.
- Reproducible model and pipeline evaluation.

## 21. Final Architecture Decisions

| Decision | Final choice |
|---|---|
| Simulated bank environment | One bank |
| Customer registry | One shared registry |
| Account registry | One shared registry |
| Transaction ledger | One canonical ledger |
| Fraud scenario reference | PaySim-inspired |
| AML scenario reference | AMLSim-inspired |
| External live transaction API | Not required |
| Internal generation API | FastAPI |
| Database | PostgreSQL |
| Streaming | Apache Kafka |
| Fraud model | Random Forest |
| AML detection | GNN + typology detection |
| Fraud investigation | Transaction-centred graph |
| AML investigation | Account-centred graph |
| Case management | Shared |
| Frontend | Next.js |

## 22. Expected Outcome

Sentinel will be a functional banking intelligence application that generates continuous synthetic banking transactions, stores them in a shared database, streams them through Kafka, detects potentially fraudulent transactions using machine learning, identifies suspicious money-flow patterns using graph-based AML analysis, and provides two independent investigation workspaces backed by the same canonical customer and account identities.

The project demonstrates the integration of data engineering, event streaming, machine learning, graph analytics, backend engineering and full-stack software development in one unified academic major project.

---

**Implementation integrity rule:** A component is considered implemented only when its code runs and its behaviour is tested. A planned technology, illustrative graph, placeholder score or hard-coded status must not be represented as a working integration or measured model result.
