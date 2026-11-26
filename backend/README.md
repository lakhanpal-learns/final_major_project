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
