# Sentinel — Technology Decisions and Alternatives

**Status:** Approved architectural choices  
**Purpose:** Explain why each technology or pattern is included, what alternatives exist, and why the project is not adopting every available tool.

A technology is included only when it has a defined responsibility in Sentinel. These decisions are based on project requirements and engineering trade-offs, not on a claim that the selected option is universally superior.

## Decision summary

| Area | Selected | Main reason | Alternative considered |
|---|---|---|---|
| Backend API | Python + FastAPI | API controls, validation and integration with Python ML stack | Flask |
| Operational database | PostgreSQL | Relational customers/accounts/transactions, constraints and transactions | MongoDB, SQLite |
| Event transport | Apache Kafka | Independent consumer groups, buffering and replay within retention | Direct API calls, RabbitMQ |
| Event publishing reliability | Transactional outbox | Atomic database write plus durable event intent | Direct DB write followed by Kafka publish |
| Continuous generation | Separate Python worker | Long-running generator does not block API requests | Generate inside FastAPI request handler |
| Fraud inference | Python model-serving consumer | Per-event inference and isolated failure handling | Batch-only scoring |
| AML processing | Python rolling-window and graph analysis | Account/network context plus historical pattern evaluation | Only transaction-by-transaction rules |
| Batch analytics | Python + Pandas | Adequate for the project's expected local academic workload | Apache Spark |
| Data lake storage | Local filesystem | No cloud account or extra storage service required | S3, MinIO |
| Analytical file format | Apache Parquet | Columnar format suited to analytical scans and typed datasets | CSV, JSON Lines |
| Data organization | Bronze/Silver/Gold | Separates raw, validated and curated datasets | One undifferentiated folder/table |
| Frontend | Next.js/React | Interactive dashboard and investigation workspaces | Server-rendered templates |
| Network visualization | Cytoscape.js or React Flow | Visual account relationships and investigation paths | Static graph images |
| Deployment for development | Docker Compose | Reproducible local PostgreSQL/Kafka services | Kubernetes |

## 1. Python and FastAPI

**Problem:** Sentinel needs API endpoints for generator controls, health checks and dashboard/backend integration, while its data generation and ML components are Python-based.

**Decision:** Use Python with FastAPI for the API layer. Run continuous generation and Kafka consumers as separate processes/workers.

**Why:** This keeps API responsibilities separate from long-running work and allows reuse of Python libraries for data engineering and machine learning.

**Alternative — Flask:** Flask is also capable of serving the required API. FastAPI is selected for its type-driven request validation and API schema support. This is a project fit decision, not proof that Flask cannot meet the requirements.

**Trade-off:** Multiple processes need configuration, lifecycle management and monitoring.

## 2. PostgreSQL as operational system of record

**Problem:** Customers own accounts; transactions refer to accounts; balance updates and transaction writes must remain consistent.

**Decision:** PostgreSQL stores canonical banking entities, the operational transaction ledger, account state, outbox records and detection/investigation results.

**Why:** Relational constraints and database transactions fit linked banking records and atomic updates.

**Alternatives:**
- **MongoDB:** Flexible document model, but Sentinel's core records have clear relationships and consistency requirements. It could work, but does not remove the need to model those relationships.
- **SQLite:** Useful for lightweight local tests, but not selected as the main multi-process operational database.
- **Separate fraud and AML databases:** Rejected initially because both pipelines use the same bank identities and shared ledger. Separate result tables/workspaces provide logical separation without duplicating the source of truth.

**Trade-off:** PostgreSQL needs schema migrations, backups and connection management.

## 3. Kafka for event transport

**Problem:** Fraud, AML and historical ingestion must consume the same committed transaction events independently. A slow AML analysis or data-lake write must not block fraud inference.

**Decision:** Publish transaction events to a versioned Kafka topic and use separate consumer groups for fraud, AML and data-lake ingestion.

**Why:** Consumer groups allow independent progress, scaling and offset tracking. Kafka also provides a configurable retention window for replay.

**Alternatives:**
- **Direct API call from generator to fraud model:** Simpler, but tightly couples generation to model availability and makes independent AML and lake ingestion harder.
- **RabbitMQ:** A capable message broker, especially for work queues and routing. Kafka is selected because this design is an event stream with multiple independent consumers and replay needs.
- **No broker:** Fewer moving parts, but the generator would need to coordinate multiple downstream services itself.

**Trade-off:** Kafka adds operational complexity: broker configuration, topic management, consumer offsets, retries, monitoring and schema evolution. It is justified only if the independent consumers are actually implemented.

## 4. Transactional outbox

**Problem:** Writing a transaction to PostgreSQL and publishing to Kafka are two separate operations. If one succeeds and the other fails, the system can lose or misrepresent an event.

**Decision:** Write the transaction, account balance changes and outbox row in one PostgreSQL transaction. A publisher later sends the outbox event to Kafka.

**Why:** The database commit atomically records both the banking event and the intent to publish it.

**Alternative:** Publish directly to Kafka after committing the database transaction. This is simpler but has a failure gap between the two operations.

**Trade-off:** The outbox publisher, retries and cleanup/retention need implementation. Duplicate publication remains possible, so consumers must be idempotent.

## 5. Separate continuous generator worker

**Problem:** The bank simulation must continue generating events, while the API remains responsive.

**Decision:** Run the generator as a separate worker. FastAPI exposes controls such as start, pause, resume, stop and rate configuration.

**Why:** Long-running generation does not occupy HTTP request handlers. Worker lifecycle and generation rate can be managed independently.

**Alternative:** Generate transactions inside an API endpoint. Rejected because a request handler is not an appropriate owner for an indefinite background loop.

**Trade-off:** The worker needs safe shutdown, persisted account state, rate limiting and restart behavior.

## 6. Hybrid real-time and batch processing

**Problem:** Fraud scoring can be performed for an individual event, while AML typologies may require rolling account history, multiple related transactions and historical network context. Historical analytics also has different latency requirements from operational detection.

**Decision:**
- Fraud: per-event inference.
- AML: per-event state/network updates plus near-real-time rolling-window evaluation.
- Batch: periodic AML historical analysis, aggregate reporting and model-development/evaluation datasets.

**Why:** The processing mode matches the information needed by each task. It avoids forcing all analytics into a synchronous per-event path.

**Alternatives:**
- **All real-time:** More complexity and unnecessary work for historical aggregates; some AML patterns still require windows.
- **All batch:** Delays transaction-level fraud findings and does not demonstrate streaming operation.
- **Spark/Flink streaming:** Powerful distributed processing engines, but unnecessary until measured volume or processing complexity requires them.

**Trade-off:** Hybrid processing creates multiple schedules and requires clear event-time/window semantics and duplicate handling.

## 7. AML graph processing

**Problem:** AML investigations often concern relationships and movement of funds across accounts, not only an isolated transaction.

**Decision:** Maintain account/transaction graph state and calculate relevant network features and typology rules. Use a GNN only if the project has defensible labels, sufficient examples and a leakage-safe evaluation plan.

**Why:** Graph context can represent fan-in, fan-out, paths and cycles.

**Alternatives:** SQL aggregation and NetworkX graph analysis are simpler and suitable initial choices. A GNN is not automatically better and must be justified by evaluation.

**Trade-off:** Graph state and time windows require careful memory, persistence and historical reconstruction design.

## 8. Local Parquet data lake and medallion layers

**Problem:** Operational tables alone are not an ideal layout for retaining raw event history and creating repeatable analytical datasets.

**Decision:** Store lake data as Parquet files on the local filesystem, organized into Bronze, Silver and Gold layers.

**Why:** Local storage avoids cloud credentials and an additional object-storage service. Parquet supports typed, columnar analytical datasets.

**Alternatives:**
- **CSV:** Easy to inspect, but weaker typing and generally less efficient for analytical scans.
- **JSON Lines:** Convenient for raw event exchange, but less efficient for repeated columnar analytics.
- **MinIO/S3:** Useful for object-storage semantics and larger deployments, but adds a service and configuration not currently needed.
- **Cloud object storage:** Not required for a local academic demonstration.

**Trade-off:** Local files are tied to the machine unless backed up. The project must implement safe file rotation, atomic writes and recovery around Kafka offsets. A local folder is a practical lake implementation for this project's scope, not a highly available enterprise lakehouse.

## 9. Python and Pandas for batch jobs

**Problem:** Bronze data needs validation, standardization and aggregation.

**Decision:** Use scheduled Python jobs and Pandas initially.

**Why:** The expected academic workload can be processed locally without operating a distributed compute cluster.

**Alternative — Apache Spark:** Appropriate when data volume, parallel processing or cluster execution justifies it. It is not included merely to increase the technology count.

**Trade-off:** Pandas is memory-bound for many workflows. If measured data volume exceeds local capacity, revisit the choice.

## 10. Frontend and graph visualization

**Decision:** Use Next.js/React for the dashboard and Cytoscape.js or React Flow for account-network exploration.

**Why:** The project needs interactive operational views and investigation workflows, not just static reports.

**Alternatives:** Basic server-rendered pages or static charts are simpler but offer less interaction for graph investigations.

**Trade-off:** Frontend state, API contracts and visualization performance need testing.

## 11. Docker Compose, not Kubernetes

**Decision:** Use Docker Compose for local development services such as PostgreSQL and Kafka.

**Why:** It provides repeatable local setup with a small operational footprint.

**Alternative — Kubernetes:** Useful for orchestration across larger deployments, but unnecessary for this single-machine academic project.

**Trade-off:** Compose is not a production high-availability deployment architecture.

## 12. Explicitly excluded for now

| Technology | Why it is not included now | Reconsider if |
|---|---|---|
| Apache Spark | No demonstrated scale requirement for distributed compute | Pandas jobs exceed memory/time limits |
| Apache Flink | Streaming consumers can meet initial event processing needs | Stateful stream processing complexity/throughput requires it |
| Airflow | A few scheduled Python jobs do not justify a workflow platform | Number/dependency/retry complexity of batch jobs grows |
| MinIO/S3 | Local Parquet storage meets current demonstration needs | Object storage, multi-machine access or larger retention is needed |
| Kubernetes | Excessive deployment complexity for current scope | Multi-node orchestration becomes a real requirement |
| Separate fraud and AML source databases | Both use one canonical bank ledger | A concrete isolation or scaling requirement is demonstrated |

## 13. Evidence and validation plan

Architectural reasoning is not a performance benchmark. Validate decisions with project evidence:

- Measure generated events/second and consumer lag at the configured rate.
- Verify each event reaches fraud, AML and Bronze independently.
- Test database outage and outbox retry.
- Test broker outage and publisher recovery.
- Test duplicate delivery and idempotent result writes.
- Test a consumer restart and offset recovery.
- Test local file-write failure without losing acknowledged lake events.
- Compare batch runtime and memory use as data grows.
- Record actual test output and measurements in the relevant task documentation.

Do not report a component as implemented or validated until the corresponding code and tests exist.
