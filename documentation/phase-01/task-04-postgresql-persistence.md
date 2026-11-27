# Phase 01 — Task 04: PostgreSQL Persistence Layer

**Status:** Implemented in repository; local test verification pending  
**Date:** 2026-11-27

## Objective

Move Sentinel's canonical customer, account and transaction records from process memory into a relational database. PostgreSQL is the intended runtime database. Both fraud and AML workflows will read the same persisted ledger.

## Implemented

- Added SQLAlchemy 2.x and Psycopg 3 dependencies.
- Added environment-based DATABASE_URL configuration with a local PostgreSQL development default.
- Added SQLAlchemy engine and session-factory helpers.
- Added schema bootstrap helper for local development and tests.
- Added relational mappings for customers, accounts and transactions.
- Added primary keys, unique account numbers, foreign keys and useful account/transaction indexes.
- Added repository operations to save and read customers, accounts and transactions.
- Added database tests using in-memory SQLite with foreign-key enforcement enabled.

## Files added or changed

- Updated backend/requirements.txt
- backend/app/db/__init__.py
- backend/app/db/database.py
- backend/app/db/tables.py
- backend/app/db/repository.py
- backend/tests/test_persistence.py

## Data integrity

- Each account references an existing customer.
- Each transaction references existing sender and receiver accounts.
- Account numbers are unique.
- Transaction IDs are primary keys.
- Fraud and AML labels are stored as separate columns on the same transaction row, preserving the shared-ledger design.

## Important verification boundary

Automated tests use SQLite in-memory to validate repository behavior and relational constraints without requiring a running PostgreSQL server. This does **not** prove that PostgreSQL is installed, reachable, configured, or successfully accepting connections. PostgreSQL integration must be verified separately after configuring DATABASE_URL and running the application against PostgreSQL.

The create_schema helper uses SQLAlchemy create_all for initial local development. A versioned Alembic migration workflow should replace this before schema changes become frequent or deployment becomes production-like.

## Verification

- Code and tests are committed to the main branch.
- Local test execution for this task is pending.
- Run python -m pytest from backend/ after installing the updated requirements.
- Then configure DATABASE_URL for PostgreSQL and verify schema creation and persistence against a real PostgreSQL instance.

## Known limitations / next work

- The registry generator is not yet wired to a startup/seed command or API endpoint.
- No PostgreSQL instance is provisioned by this task.
- No Alembic migrations exist yet.
- No Kafka outbox or event publisher is implemented yet.
- Repository functions flush but do not commit; transaction boundaries are controlled by the calling service/session.
