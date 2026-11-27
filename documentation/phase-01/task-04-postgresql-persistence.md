# Phase 01 — Task 04: PostgreSQL Persistence Layer

**Status:** Implemented in repository; local test verification pending  
**Date:** 2026-11-27

## Objective

Move Sentinel's canonical customer, account and transaction records from process memory into a relational database. PostgreSQL is the intended runtime database. Both fraud and AML workflows will read the same persisted ledger.

## What was added and where

| File | Change | Purpose |
|---|---|---|
| \`backend/requirements.txt\` | Updated | Adds SQLAlchemy for ORM/database access and Psycopg 3 as the PostgreSQL driver. |
| \`backend/app/db/__init__.py\` | Added | Marks \`app.db\` as a Python package and documents the database layer. It intentionally contains no runtime logic. |
| \`backend/app/db/database.py\` | Added | Reads the database URL, creates the SQLAlchemy engine and session factory, and creates the initial database schema. |
| \`backend/app/db/tables.py\` | Added | Defines the database tables and their columns, primary keys, foreign keys, uniqueness constraints and indexes. |
| \`backend/app/db/repository.py\` | Added | Implements functions to save the shared registry and transactions, and read them back as application models. |
| \`backend/tests/test_persistence.py\` | Added | Tests database round-tripping and checks that invalid foreign-key references are rejected. |

## Code added

The following is the code currently committed for Task 04.

### 1. \`backend/requirements.txt\`

**Purpose:** Declares the Python packages required by the backend. SQLAlchemy provides the database abstraction/ORM. Psycopg is the PostgreSQL database adapter.

~~~text
fastapi>=0.115,<1.0
uvicorn[standard]>=0.30,<1.0
SQLAlchemy>=2.0,<3.0
psycopg[binary]>=3.2,<4.0
~~~

### 2. \`backend/app/db/__init__.py\`

**Purpose:** Makes the \`db\` directory an importable Python package. The module currently contains only a package-level docstring.

~~~python
"""Database configuration and persistence layer."""
~~~

### 3. \`backend/app/db/database.py\`

**Purpose:** Centralizes database configuration and SQLAlchemy setup. \`DATABASE_URL\` can override the local development default. \`create_db_engine\` creates the engine, \`create_session_factory\` creates database sessions, and \`create_schema\` creates missing tables from the ORM metadata.

~~~python
"""SQLAlchemy engine, session factory and schema bootstrap helpers."""

import os

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

DEFAULT_DATABASE_URL = "postgresql+psycopg://sentinel:sentinel@localhost:5432/sentinel"


class Base(DeclarativeBase):
    """Base class for Sentinel relational tables."""


def get_database_url() -> str:
    return os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)


def create_db_engine(database_url: str | None = None) -> Engine:
    return create_engine(database_url or get_database_url(), pool_pre_ping=True)


def create_session_factory(engine: Engine) -> sessionmaker:
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def create_schema(engine: Engine) -> None:
    """Create missing tables for local development and tests."""
    from app.db import tables  # noqa: F401

    Base.metadata.create_all(bind=engine)
~~~

### 4. \`backend/app/db/tables.py\`

**Purpose:** Maps Sentinel's canonical data to relational tables. The \`customers\` table stores customer records; \`accounts\` stores accounts and links each account to a customer; \`transactions\` stores transfers and links sender and receiver accounts. Fraud and AML ground-truth labels are separate fields on the same transaction row, so both workflows use one ledger.

~~~python
"""Relational table mappings for the canonical banking ledger."""

from datetime import datetime
from decimal import Decimal

from sqlalchemy import Boolean, DateTime, ForeignKey, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db.database import Base


class CustomerRow(Base):
    __tablename__ = "customers"

    customer_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    full_name: Mapped[str] = mapped_column(String(160), nullable=False)
    customer_type: Mapped[str] = mapped_column(String(32), nullable=False)
    city: Mapped[str] = mapped_column(String(100), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class AccountRow(Base):
    __tablename__ = "accounts"

    account_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    account_number: Mapped[str] = mapped_column(String(32), nullable=False, unique=True)
    customer_id: Mapped[str] = mapped_column(
        String(32), ForeignKey("customers.customer_id"), nullable=False, index=True
    )
    account_type: Mapped[str] = mapped_column(String(32), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    balance: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class TransactionRow(Base):
    __tablename__ = "transactions"

    transaction_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    sender_account_id: Mapped[str] = mapped_column(
        String(32), ForeignKey("accounts.account_id"), nullable=False, index=True
    )
    receiver_account_id: Mapped[str] = mapped_column(
        String(32), ForeignKey("accounts.account_id"), nullable=False, index=True
    )
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    transaction_type: Mapped[str] = mapped_column(String(32), nullable=False)
    channel: Mapped[str] = mapped_column(String(20), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    is_fraud: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_aml_suspicious: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    fraud_scenario: Mapped[str | None] = mapped_column(String(80), nullable=True)
    aml_typology: Mapped[str | None] = mapped_column(String(80), nullable=True)
~~~

### 5. \`backend/app/db/repository.py\`

**Purpose:** Keeps database read/write operations separate from API routes and model-generation logic.

- \`save_registry\` inserts customers first, then accounts, respecting the foreign-key dependency.
- \`save_transactions\` inserts transactions after their referenced accounts exist.
- \`list_customers\`, \`list_accounts\` and \`list_transactions\` read ORM rows and convert them back into the existing Pydantic application models.
- The save functions call \`flush()\`, not \`commit()\`. The calling service controls the transaction boundary and decides when to commit or roll back.

~~~python
"""Persistence operations for the canonical customer/account/transaction ledger."""

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.tables import AccountRow, CustomerRow, TransactionRow
from app.models.banking import Account, Customer
from app.models.transaction import Transaction
from app.services.registry import BankingRegistry


def save_registry(session: Session, registry: BankingRegistry) -> None:
    """Insert canonical customers and accounts into the current transaction."""
    session.add_all([
        CustomerRow(
            customer_id=item.customer_id,
            full_name=item.full_name,
            customer_type=item.customer_type,
            city=item.city,
            created_at=item.created_at,
        )
        for item in registry.customers
    ])
    session.flush()
    session.add_all([
        AccountRow(
            account_id=item.account_id,
            account_number=item.account_number,
            customer_id=item.customer_id,
            account_type=item.account_type,
            currency=item.currency,
            balance=item.balance,
            status=item.status,
            opened_at=item.opened_at,
        )
        for item in registry.accounts
    ])
    session.flush()


def save_transactions(session: Session, transactions: list[Transaction]) -> None:
    """Insert transactions after their referenced accounts have been persisted."""
    session.add_all([
        TransactionRow(
            transaction_id=item.transaction_id,
            sender_account_id=item.sender_account_id,
            receiver_account_id=item.receiver_account_id,
            amount=item.amount,
            currency=item.currency,
            transaction_type=item.transaction_type,
            channel=item.channel,
            status=item.status,
            timestamp=item.timestamp,
            is_fraud=item.is_fraud,
            is_aml_suspicious=item.is_aml_suspicious,
            fraud_scenario=item.fraud_scenario,
            aml_typology=item.aml_typology,
        )
        for item in transactions
    ])
    session.flush()


def list_customers(session: Session) -> list[Customer]:
    rows = session.scalars(select(CustomerRow).order_by(CustomerRow.customer_id)).all()
    return [
        Customer(
            customer_id=row.customer_id,
            full_name=row.full_name,
            customer_type=row.customer_type,
            city=row.city,
            created_at=row.created_at,
        )
        for row in rows
    ]


def list_accounts(session: Session) -> list[Account]:
    rows = session.scalars(select(AccountRow).order_by(AccountRow.account_id)).all()
    return [
        Account(
            account_id=row.account_id,
            account_number=row.account_number,
            customer_id=row.customer_id,
            account_type=row.account_type,
            currency=row.currency,
            balance=row.balance,
            status=row.status,
            opened_at=row.opened_at,
        )
        for row in rows
    ]


def list_transactions(session: Session) -> list[Transaction]:
    rows = session.scalars(
        select(TransactionRow).order_by(TransactionRow.transaction_id)
    ).all()
    return [
        Transaction(
            transaction_id=row.transaction_id,
            sender_account_id=row.sender_account_id,
            receiver_account_id=row.receiver_account_id,
            amount=row.amount,
            currency=row.currency,
            transaction_type=row.transaction_type,
            channel=row.channel,
            status=row.status,
            timestamp=row.timestamp,
            is_fraud=row.is_fraud,
            is_aml_suspicious=row.is_aml_suspicious,
            fraud_scenario=row.fraud_scenario,
            aml_typology=row.aml_typology,
        )
        for row in rows
    ]
~~~

### 6. \`backend/tests/test_persistence.py\`

**Purpose:** Verifies that customer, account and transaction data can be saved and loaded, and that invalid references are rejected by database foreign-key constraints. Tests use an in-memory SQLite database with foreign-key enforcement enabled; they do not connect to PostgreSQL.

~~~python
import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.db.database import create_schema
from app.db.repository import (
    list_accounts,
    list_customers,
    list_transactions,
    save_registry,
    save_transactions,
)
from app.services.registry import build_registry
from app.services.transaction_generator import generate_transactions


@pytest.fixture
def session() -> Session:
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def enable_sqlite_foreign_keys(connection, _record) -> None:
        cursor = connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    create_schema(engine)
    with Session(engine) as db_session:
        yield db_session
    engine.dispose()


def test_registry_and_transactions_round_trip_through_database(session: Session) -> None:
    registry = build_registry(customer_count=20, seed=12)
    transactions = generate_transactions(
        registry, normal_count=30, fraud_count=4, aml_count=6, seed=13
    )

    save_registry(session, registry)
    save_transactions(session, transactions)
    session.commit()

    loaded_customers = list_customers(session)
    loaded_accounts = list_accounts(session)
    loaded_transactions = list_transactions(session)

    assert len(loaded_customers) == 20
    assert len(loaded_accounts) == 22
    assert len(loaded_transactions) == 40
    assert loaded_accounts[0].customer_id in {
        customer.customer_id for customer in loaded_customers
    }
    assert {item.transaction_id for item in loaded_transactions} == {
        item.transaction_id for item in transactions
    }


def test_database_rejects_account_with_unknown_customer(session: Session) -> None:
    from app.db.tables import AccountRow

    session.add(
        AccountRow(
            account_id="ACC99999999",
            account_number="999999999999",
            customer_id="MISSING_CUSTOMER",
            account_type="SAVINGS",
            currency="INR",
            balance=100,
            status="ACTIVE",
            opened_at=build_registry(1).customers[0].created_at,
        )
    )

    with pytest.raises(IntegrityError):
        session.commit()


def test_database_rejects_transaction_with_unknown_account(session: Session) -> None:
    from app.db.tables import TransactionRow

    session.add(
        TransactionRow(
            transaction_id="TXN9999999999",
            sender_account_id="MISSING_A",
            receiver_account_id="MISSING_B",
            amount=100,
            currency="INR",
            transaction_type="TRANSFER",
            channel="UPI",
            status="COMPLETED",
            timestamp=build_registry(1).customers[0].created_at,
            is_fraud=False,
            is_aml_suspicious=False,
        )
    )

    with pytest.raises(IntegrityError):
        session.commit()
~~~

## Data integrity

- Each account references an existing customer.
- Each transaction references existing sender and receiver accounts.
- Account numbers are unique.
- Transaction IDs are primary keys.
- Fraud and AML labels are stored as separate columns on the same transaction row, preserving the shared-ledger design.

## Important verification boundary

Automated tests use SQLite in-memory to validate repository behavior and relational constraints without requiring a running PostgreSQL server. This does **not** prove that PostgreSQL is installed, reachable, configured, or successfully accepting connections. PostgreSQL integration must be verified separately after configuring \`DATABASE_URL\` and running the application against PostgreSQL.

The \`create_schema\` helper uses SQLAlchemy \`create_all\` for initial local development. A versioned Alembic migration workflow should replace this before schema changes become frequent or deployment becomes production-like.

## Verification

- Code and tests are committed to the \`main\` branch.
- Local test execution for this task is pending.
- Run \`python -m pytest\` from \`backend/\` after installing the updated requirements.
- Then configure \`DATABASE_URL\` for PostgreSQL and verify schema creation and persistence against a real PostgreSQL instance.

## Known limitations / next work

- The registry generator is not yet wired to a startup/seed command or API endpoint.
- No PostgreSQL instance is provisioned by this task.
- No Alembic migrations exist yet.
- No Kafka outbox or event publisher is implemented yet.
- Repository functions flush but do not commit; transaction boundaries are controlled by the calling service/session.
