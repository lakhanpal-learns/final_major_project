# Phase 01 — Task 01: Project Structure and Initial Backend Setup

**Status:** Implemented in repository; runtime verification pending  
**Date:** 2026-11-26  
**Scope:** Establish the initial repository conventions and a minimal FastAPI backend entry point.

## Objective

Create a clean starting structure for Sentinel so subsequent work can add the shared synthetic banking registry, transaction generator, persistence, Kafka consumers, and investigation APIs without mixing responsibilities.

## Repository structure added

    .
    ├── .gitignore
    ├── backend/
    │   ├── app/
    │   │   ├── __init__.py
    │   │   └── main.py
    │   ├── tests/
    │   │   └── test_health.py
    │   ├── pytest.ini
    │   ├── requirements.txt
    │   ├── requirements-dev.txt
    │   └── README.md
    └── documentation/
        ├── SOFTWARE_REQUIREMENTS_SPECIFICATION.md
        └── phase-01/
            └── task-01-project-structure-and-initial-backend-setup.md

## Implemented

- Added Python cache, virtual-environment, environment-file, editor and test-artifact exclusions in the root .gitignore.
- Added a FastAPI application named Sentinel Banking Intelligence API.
- Added GET /health, returning the service name, version and basic process status.
- Added runtime and development dependency files.
- Added a pytest health-endpoint test and pytest configuration.
- Added PowerShell setup, run and test commands in backend/README.md.

## Architecture decision

Sentinel is a downstream fraud-detection and AML-monitoring system. This initial API scaffold does not authorize payments, debit accounts, or execute transfers. The /health endpoint currently checks only that the API process responds; it does not claim that PostgreSQL, Kafka, or model services are healthy.

## Verification

- Repository writes: committed to the main branch.
- GitHub file existence: verified after commit.
- Local dependency installation and pytest execution: not run in this environment. Do not treat the test as passed until it is executed locally or in CI.

## Known limitations / next work

- No database connection or schema exists yet.
- No synthetic customers, accounts or transactions are generated yet.
- No Kafka producer or consumers exist yet.
- No fraud or AML model is integrated yet.
- No frontend is scaffolded in this task.

## Completion criteria

The task is complete when the initial files are present in the repository and the developer can install the dependencies, start the API, receive 200 OK from /health, and run the health test successfully. Repository file creation is complete; local runtime verification remains outstanding.
