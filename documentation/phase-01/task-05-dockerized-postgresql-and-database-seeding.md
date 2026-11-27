# Task 05 — Dockerized PostgreSQL and Database Seeding

## 1. Objective

Provide a repeatable local PostgreSQL environment and a controlled way to insert Sentinel's shared synthetic banking dataset into it.

This task builds on Task 04's SQLAlchemy tables and repository functions. It does not add Kafka, migrations, a backend container, or public data API endpoints.

## 2. Files added

| File | Purpose |
|---|---|
| `docker-compose.yml` | Starts PostgreSQL 16 with a persistent named volume and readiness health check. |
| `backend/.env.example` | Documents the local `DATABASE_URL` connection string. |
| `backend/app/seed.py` | Generates one canonical customer/account registry and its transactions, then saves them in one database transaction. |
| `backend/tests/test_seed.py` | Tests seed counts, persistence, and protection against accidental duplicate seeding. |

No existing application source files were modified in this task.

## 3. Implementation

### 3.1 PostgreSQL service — `docker-compose.yml`

```yaml
services:
  postgres:
    image: postgres:16-alpine
    container_name: sentinel-postgres
    restart: unless-stopped
    environment:
      POSTGRES_DB: sentinel
      POSTGRES_USER: sentinel
      POSTGRES_PASSWORD: sentinel
    ports:
      - "5432:5432"
    volumes:
      - sentinel_postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U sentinel -d sentinel"]
      interval: 5s
      timeout: 5s
      retries: 10

volumes:
  sentinel_postgres_data:
```

**Purpose:** Runs PostgreSQL 16 in a container, publishes port 5432 to the host, stores database files in a named Docker volume, and reports readiness using `pg_isready`.

The credentials are for local development only. Do not reuse these credentials in a public or production deployment.

### 3.2 Environment template — `backend/.env.example`

```dotenv
# Local PostgreSQL connection used by the backend.
DATABASE_URL=postgresql+psycopg://sentinel:sentinel@localhost:5432/sentinel
```

**Purpose:** Shows the connection string expected by the existing `get_database_url()` function in `backend/app/db/database.py`.

The application currently reads environment variables; it does not automatically load a `.env` file. Set `DATABASE_URL` in the terminal before running the seed script.

### 3.3 Seed implementation — `backend/app/seed.py`

```python
"""Create the initial synthetic Sentinel dataset in the configured database."""

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.db.database import create_db_engine, create_schema, create_session_factory
from app.db.repository import save_registry, save_transactions
from app.db.tables import AccountRow, CustomerRow, TransactionRow
from app.services.data_quality import audit_ledger
from app.services.registry import build_registry
from app.services.transaction_generator import generate_transactions


def seed_database(
    session: Session,
    *,
    customer_count: int = 100,
    normal_count: int = 500,
    fraud_count: int = 25,
    aml_count: int = 25,
    registry_seed: int = 42,
    transaction_seed: int = 99,
) -> dict[str, int]:
    """Insert one deterministic dataset; refuse to append duplicate seed data."""
    existing_counts = (
        session.scalar(select(func.count()).select_from(CustomerRow)) or 0,
        session.scalar(select(func.count()).select_from(AccountRow)) or 0,
        session.scalar(select(func.count()).select_from(TransactionRow)) or 0,
    )
    if any(existing_counts):
        raise RuntimeError(
            "Database already contains banking data; refusing to seed over existing rows."
        )

    registry = build_registry(customer_count=customer_count, seed=registry_seed)
    transactions = generate_transactions(
        registry,
        normal_count=normal_count,
        fraud_count=fraud_count,
        aml_count=aml_count,
        seed=transaction_seed,
    )
    audit_ledger(registry, transactions)

    try:
        save_registry(session, registry)
        save_transactions(session, transactions)
        session.commit()
    except Exception:
        session.rollback()
        raise

    return {
        "customers": len(registry.customers),
        "accounts": len(registry.accounts),
        "transactions": len(transactions),
    }


def main() -> None:
    """Create the schema and seed the database configured by DATABASE_URL."""
    engine = create_db_engine()
    create_schema(engine)
    session_factory = create_session_factory(engine)
    try:
        with session_factory() as session:
            counts = seed_database(session)
        print("Sentinel database seeded successfully:")
        for table, count in counts.items():
            print(f"  {table}: {count}")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()

```

**Purpose and flow:**

1. Counts existing customer, account, and transaction rows.
2. Stops if any banking data already exists, preventing accidental duplicate seed runs.
3. Calls the existing canonical registry generator.
4. Generates normal, fraud-labelled, and AML-labelled transactions using the same account IDs.
5. Uses the existing repository functions to insert customers before accounts and transactions.
6. Commits the inserts together; rolls back if an insert fails.
7. The command-line entry point creates missing tables and connects using `DATABASE_URL`.

The fraud and AML flags remain synthetic generator labels, not trained model predictions.

### 3.4 Seed tests — `backend/tests/test_seed.py`

```python
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from app.db.database import create_schema
from app.db.repository import list_accounts, list_customers, list_transactions
from app.seed import seed_database


@pytest.fixture
def session() -> Session:
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    create_schema(engine)
    with Session(engine) as db_session:
        yield db_session
    engine.dispose()


def test_seed_database_persists_shared_banking_data(session: Session) -> None:
    counts = seed_database(
        session,
        customer_count=10,
        normal_count=12,
        fraud_count=2,
        aml_count=4,
    )

    assert counts == {"customers": 10, "accounts": 11, "transactions": 17}
    assert len(list_customers(session)) == 10
    assert len(list_accounts(session)) == 11
    assert len(list_transactions(session)) == 17


def test_seed_database_refuses_to_duplicate_existing_data(session: Session) -> None:
    seed_database(
        session,
        customer_count=5,
        normal_count=4,
        fraud_count=1,
        aml_count=0,
    )

    with pytest.raises(RuntimeError, match="already contains banking data"):
        seed_database(session)

    assert len(list_customers(session)) == 5
    assert len(list_transactions(session)) == 5
```

**Purpose:** Uses an in-memory SQLite database to verify the seed function's data counts and duplicate protection. These are unit/integration tests of the SQLAlchemy persistence logic; they do not prove connectivity to a running PostgreSQL server.

## 4. Run locally

From the repository root, start PostgreSQL:

```powershell
docker compose up -d postgres
docker compose ps
```

Wait until the PostgreSQL service reports healthy. Then open PowerShell in the `backend` directory and configure the connection:

```powershell
$env:DATABASE_URL = "postgresql+psycopg://sentinel:sentinel@localhost:5432/sentinel"
pip install -r requirements.txt
python -m app.seed
```

Expected default seed counts:

- Customers: 100
- Accounts: 110 (one per customer, plus a second account for every tenth customer)
- Transactions: 549 (500 normal-only + 25 fraud-positive + 25 AML-positive - 1 cross-labelled transaction)

Run the full test suite from `backend`:

```powershell
python -m pytest
```

The default seed script intentionally refuses to run again when banking rows already exist. To reset this local development database and remove its persisted volume, run from the repository root:

```powershell
docker compose down -v
docker compose up -d postgres
```

**Warning:** `docker compose down -v` deletes the PostgreSQL data volume. Use it only when you intend to erase this local database.

## 5. Verification status

Implementation files and tests have been added to the repository. The developer must run the commands above locally.

- SQLite seed tests: added; execution result is pending.
- PostgreSQL container startup: pending local verification.
- Real PostgreSQL seed and read-back: pending local verification.
- Full pytest result: pending local verification.

Do not describe PostgreSQL as verified until the container starts, `python -m app.seed` completes against PostgreSQL, and the persisted rows can be read back.

## 6. Limitations and next work

- This task creates only the PostgreSQL service; the FastAPI process still runs locally.
- `.env.example` is a template, not an automatically loaded configuration file.
- The seed script is a one-time initial-data loader, not a continuous transaction generator.
- Schema creation still uses SQLAlchemy `create_all`; Alembic migrations are not implemented.
- Local development credentials are intentionally simple and must be replaced for any shared deployment.
- Kafka streaming, outbox publishing, API endpoints for database reads, and model pipelines remain future tasks.

---

## 2026-11-27 update — New generator defaults and database schema

The redesigned generator defaults to one cross-labelled transaction when both fraud and AML counts are non-zero. The default total is therefore 549 rows: 500 normal-only + 25 fraud-positive + 25 AML-positive - 1 overlap.

The transaction table now includes four optional balance snapshot columns. If this database was created before the schema change, apply the four `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` statements documented in `task-04-postgresql-persistence.md` before running the updated application. SQLAlchemy `create_all()` does not alter an existing table.

Seed test expected count for the small fixture is now 17 rows (12 normal-only + 2 fraud-positive + 4 AML-positive - 1 overlap). Test execution and a real PostgreSQL seed/read-back remain pending.

### 2026-11-27 seed validation update

The seed command now calls the shared ledger audit before persistence. This prevents a generated dataset with invalid account references, inconsistent fraud/AML metadata, missing balance snapshots or overdraft arithmetic from being inserted by the seed workflow.

Updated source file: `backend/app/seed.py`. The full current implementation is shown in section 3.3 above. Local execution is still pending.
