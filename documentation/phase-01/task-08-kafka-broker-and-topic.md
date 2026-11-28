# Task 08 — Local Kafka Broker and Transaction Topic

## 1. Objective

Add a local Apache Kafka broker to the existing Docker Compose environment and create the versioned transaction event topic required by Sentinel's downstream consumers.

This task provisions the messaging infrastructure only. It does not yet publish PostgreSQL outbox events or run fraud, AML, or data-lake consumers.

## 2. Why Kafka is added here

Task 07 established a PostgreSQL transactional outbox. The next architectural step is a durable event transport so independent consumer groups can receive the same transaction event:

- fraud processing;
- AML monitoring;
- raw data-lake ingestion.

Kafka decouples the banking ledger from downstream processing. The outbox remains the durable handoff from PostgreSQL; Kafka is not the financial system of record.

## 3. Technology and topology

- Image: official Apache Kafka JVM image, pinned to `apache/kafka:4.4.0`.
- Mode: single-node KRaft (combined broker/controller) for local development.
- Topic: `bank.transactions.v1`.
- Partitions: 3.
- Replication factor: 1.
- Retention: 7 days (`604800000` ms).
- Automatic topic creation: disabled.
- Kafka data: named Docker volume `sentinel_kafka_data`.

The Apache Kafka documentation describes the official JVM Docker image and its KRaft defaults/configuration. The project explicitly configures listeners because both host-run tools and other Compose services need to reach the broker.

## 4. Listener addresses

| Client location | Bootstrap address | Purpose |
|---|---|---|
| Windows host | `localhost:29092` | Local Python process, CLI or developer tools |
| Another Docker Compose service | `kafka:9092` | Backend/publisher/consumer containers |
| Kafka controller | `kafka:9093` | Internal KRaft controller communication |

Do not configure a containerized backend to connect to `localhost:29092`: inside a container, localhost refers to that same container. Use `kafka:9092` from services on the Compose network.

The listeners use PLAINTEXT and are suitable only for this local development environment. They are not a production security configuration.

## 5. Topic design

`bank.transactions.v1` is the versioned topic for committed banking transaction events.

Three partitions provide a small amount of parallelism for independent consumer groups. A single broker and replication factor of one are intentionally used to keep the local project lightweight. This does not provide broker high availability or replica fault tolerance.

Kafka ordering is guaranteed only within a partition, not across the entire topic. The future publisher must make an explicit and documented partition-key choice; this task does not claim global transaction ordering.

The seven-day retention is a local transport-retention choice, not a ledger-retention policy. PostgreSQL remains the source of truth and the outbox is the durable publication backlog. Retention can be revised after measuring storage and replay needs.

## 6. Files changed

| File | Change |
|---|---|
| `docker-compose.yml` | Adds the Kafka broker, persistent volume, health check, and one-shot topic initialization service. |

The existing PostgreSQL service and `sentinel_postgres_data` volume are retained.

## 7. Start the services

From the repository root:

```powershell
docker compose up -d
```

This starts PostgreSQL and Kafka. The `kafka-init` one-shot service waits for Kafka health and creates the topic if it does not already exist.

Inspect service status:

```powershell
docker compose ps -a
```

Inspect the topic from inside the Kafka container:

```powershell
docker exec sentinel-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic bank.transactions.v1
```

Expected topic metadata:
- topic name: `bank.transactions.v1`;
- partition count: 3;
- replication factor: 1.

To list all topics:

```powershell
docker exec sentinel-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

For host-run Kafka clients, use bootstrap server `localhost:29092`. For clients running in Compose, use `kafka:9092`.

## 8. Persistence and safe operations

Kafka uses its own named volume, separate from PostgreSQL.

- Restarting containers does not intentionally remove either named volume.
- `docker compose down` removes containers and the Compose network but keeps named volumes.
- **Do not run `docker compose down -v`** unless you deliberately intend to delete both PostgreSQL and Kafka persisted data.

## 9. Limitations

- Single broker, replication factor one: no high availability.
- PLAINTEXT listeners: local development only.
- No producer, outbox polling publisher, retry loop, dead-letter topic, or consumers yet.
- Topic creation is infrastructure provisioning, not proof that application events are being published.
- The configured three partitions do not guarantee global event order.
- Kafka durability and replay behavior still need runtime verification on the user's machine.

## 10. Completion criteria

Task 08 is verified only after the user starts the Compose services and confirms:
1. `sentinel-kafka` is healthy;
2. `kafka-init` exits successfully;
3. `bank.transactions.v1` exists with three partitions and replication factor one.

Do not start implementing the outbox publisher until these checks pass.


## 11. Local verification record (2026-11-28)

The user verified the Compose deployment on Windows PowerShell from the repository root.

Observed results:

| Check | Observed result |
|---|---|
| sentinel-kafka | Up (healthy) |
| sentinel-postgres | Up (healthy); existing service remained running |
| sentinel-kafka-init | Exited (0) |
| Init container logs | Kafka CLI emitted only the topic-name metric warning; no command failure |
| Topic | bank.transactions.v1 exists |
| Partition count | 3 |
| Replication factor | 1 |
| Partition leaders | 1 for partitions 0, 1 and 2 |
| ISR | 1 for all three partitions |

The initialization service was first observed exiting with code 1 because its shell command was not passed to the Kafka CLI as intended. The Compose command was corrected to use a shell block with explicit line continuations. Recreating only kafka-init then produced exit code 0.

The topic had also been created manually during diagnosis before the corrected initialization service was rerun. Therefore, the successful --if-not-exists run proves the initialization command can complete against the existing topic; it does not independently prove that the topic configuration was changed by that run. The observed topic description listed min.insync.replicas=1 and did not display an explicit retention.ms override. Seven-day retention remains the intended configuration; verify or apply it separately if an explicit topic-level override is required.

This verification proves local broker health, topic existence, three partitions, replication factor one, and successful one-shot initialization. It does not prove producer delivery, consumer processing, broker high availability, or crash-recovery behavior.
