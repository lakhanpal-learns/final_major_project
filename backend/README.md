# Sentinel Backend

Initial FastAPI service scaffold for Sentinel. Sentinel is a downstream banking intelligence application; this API does not authorize or execute payments.

## Requirements

- Python 3.11 or newer

## Run locally (PowerShell)

    cd backend
    py -3.11 -m venv .venv
    .\.venv\Scripts\Activate.ps1
    python -m pip install -r requirements-dev.txt
    uvicorn app.main:app --reload

Open http://127.0.0.1:8000/health for the process health endpoint and http://127.0.0.1:8000/docs for interactive API documentation.

## Run tests

    python -m pytest

The current health endpoint confirms that the API process responds. Database, Kafka, and model dependency checks will be added when those components are implemented.
## Run the outbox publisher (PowerShell)

Run this in a separate terminal while PostgreSQL and Kafka are healthy:

    cd backend
    python -m pip install -r requirements-dev.txt
    $env:KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
    $env:KAFKA_TRANSACTION_TOPIC = "bank.transactions.v1"
    python -m app.publish_outbox

The publisher reads pending rows from PostgreSQL and waits for Kafka acknowledgements before marking them published. Stop it with Ctrl+C. Its initial implementation is at-least-once: duplicate Kafka records are possible after a crash between broker acknowledgement and the PostgreSQL commit. Do not treat the publisher as verified until the project test suite and a real PostgreSQL-to-Kafka integration check pass.

## Run the fraud event consumer (PowerShell)

Run this in another terminal while PostgreSQL and Kafka are healthy. The consumer uses its own Kafka group and does not compete with the AML or dashboard consumers that will be added separately.

    cd backend
    python -m pip install -r requirements-dev.txt
    $env:KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
    $env:KAFKA_TRANSACTION_TOPIC = "bank.transactions.v1"
    $env:KAFKA_FRAUD_CONSUMER_GROUP_ID = "fraud-detector-v1"
    python -m app.consume_fraud

The first run uses `auto_offset_reset=earliest` for this new consumer group, so it will read existing retained events. It validates the v1 event contract and stores valid events in `fraud_event_inbox`. Invalid messages are durably quarantined in `fraud_event_rejections`. Kafka offsets are manually committed only after the corresponding database write completes.

This task implements durable event ingestion only. It does not perform fraud model inference, create fraud predictions, or generate fraud alerts. Stop the consumer with Ctrl+C.
