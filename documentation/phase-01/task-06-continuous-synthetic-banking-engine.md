# Task 06 — Continuous Synthetic Banking Engine

## Status

Implementation committed to `main`. The previous backend baseline was **23 tests passed** (user-provided local result). Task 06 tests have been added but have **not yet been run in the user's local environment**. Do not treat the new tests as passing until the next local pytest run.

## Objective

Turn the finite synthetic transaction generator into a continuously running simulated bank activity source. The worker writes transactions to the same PostgreSQL operational ledger and updates the sender and receiver account balances in the same database transaction.

The existing `seed.py` remains a one-time initial-data operation. It is not used by the continuous worker.

## Scope and boundaries

- Configurable rate: `TRANSACTIONS_PER_SECOND`, default `10`.
- A dedicated background thread performs generation; HTTP requests only control its lifecycle.
- Controls: start, pause, resume, stop, and status.
- Every completed transfer inserts one ledger row and updates both account balances atomically.
- Transaction balance snapshots are recorded in the transaction row.
- The worker generates ordinary synthetic transfers with fraud and AML labels set to false. Controlled fraud/AML scenario generation remains in the existing labelled batch generator; downstream model inference and Kafka are later tasks.
- No Kafka publishing, data-lake writes, model inference, or case creation is implemented in this task.

## Data consistency design

1. The worker loads active accounts and selects two different accounts.
2. It calculates a positive transfer amount capped at 25% of the sender's observed balance.
3. The repository reloads both accounts in stable ID order using `SELECT ... FOR UPDATE` (effective on PostgreSQL) to lock rows against concurrent transfers.
4. It checks account existence, active status, and sufficient funds.
5. It updates both balances and inserts the transaction with before/after snapshots.
6. The worker commits once; any exception leaves the SQLAlchemy session context to roll back the uncommitted transaction.

The database transaction prevents the account projection and transaction ledger from diverging on a normal single-database commit. PostgreSQL row locks help serialize concurrent writers. SQLite ignores `FOR UPDATE`; SQLite tests validate sequential behavior, not PostgreSQL locking semantics.

## Rate control

`TRANSACTIONS_PER_SECOND=10` is a configurable starting rate, not a measured performance guarantee. The worker waits one interval after each attempt. If database latency is higher than the target interval, actual throughput will be lower; it deliberately does not generate catch-up bursts.

## API

| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/simulation/status` | Read state, configured rate, generated count, last error |
| POST | `/simulation/start` | Start the background worker |
| POST | `/simulation/pause` | Pause generation |
| POST | `/simulation/resume` | Resume a paused worker |
| POST | `/simulation/stop` | Stop and wait for current operation to finish |

The worker is process-local. Run one API process/worker instance for this local project stage. Multi-process coordination or distributed leader election is not included. Do not run multiple Uvicorn workers against the same simulation without adding a database-backed lease or another single-writer mechanism.

## Configuration

Add this to `backend/.env` or set it in the shell (the application currently reads environment variables directly; it does not automatically load dotenv files):

```powershell
$env:DATABASE_URL = "postgresql+psycopg://sentinel:sentinel@localhost:5432/sentinel"
$env:TRANSACTIONS_PER_SECOND = "10"
```

Install dependencies and start PostgreSQL using the existing Task 05 instructions. Seed an empty database once with `python -m app.seed`. Then start the API from the `backend` directory:

```powershell
uvicorn app.main:app --reload
```

Use the API docs at `http://127.0.0.1:8000/docs` to call the lifecycle endpoints. For a continuously running local process, avoid the auto-reload process restarting while the worker is active.

To stop the worker before shutting down the API, call `POST /simulation/stop`.

## Files changed or added

| File | Purpose |
|---|---|
| `backend/app/config.py` | Validates the configurable event rate |
| `backend/app/services/live_banking_engine.py` | Background worker and lifecycle state machine |
| `backend/app/db/repository.py` | Adds atomic transfer persistence with account balance updates |
| `backend/app/main.py` | Exposes worker lifecycle API endpoints |
| `backend/tests/test_live_banking_engine.py` | Tests transaction persistence, balance snapshots and lifecycle |
| `backend/tests/test_live_engine_config.py` | Tests default, custom and invalid event rates |
| `pytest.ini` (repository root) | Allows pytest invocation from repository root |
| `backend/.env.example` | Documents the simulator rate setting |

## Why this design

- **Background thread instead of doing work in API requests:** long-running generation must not block request handling.
- **PostgreSQL remains the system of record:** the worker writes directly to the operational ledger; the data lake and Kafka are separate downstream stages.
- **Atomic transfer operation:** both balance changes and the ledger row commit together.
- **Row locks and deterministic lock ordering:** protect account updates from conflicting concurrent transfers and reduce deadlock risk in PostgreSQL.
- **One process-local worker for now:** appropriate for the single-machine development setup; distributed coordination would add complexity before it is needed.
- **No new queue or scheduler:** Kafka will transport committed events in the later event-pipeline task; it is not needed to implement the source simulator itself.

## Limitations and known risks

- The worker currently creates normal transfers only; it does not emit fraud/AML scenario labels. The existing batch generator remains the labelled scenario source until a later streaming scenario design is implemented.
- API worker state and generated count are in memory and reset when the API process restarts.
- A single process-local worker is supported. Multiple API processes can each start their own worker; do not deploy this configuration as-is with multiple workers.
- No database-backed worker lease, retry queue, outbox, Kafka publisher, dead-letter queue, or event replay exists yet.
- The configured rate is a target interval, not a throughput SLA.
- Tests use SQLite in-memory. They do not prove PostgreSQL connectivity, PostgreSQL row-lock behavior, or Docker runtime behavior.
- The API currently reports the last worker exception in status and continues retrying on the next interval. Operational alerting is not implemented.
- Schema changes were not required for this task; existing Task 05 tables are reused.

## Test plan

Run from the repository root after the root-level `pytest.ini` has been pulled:

```powershell
python -m pytest
```

New tests cover:
- Persisting one transfer and reconciling both account balances with transaction snapshots.
- Rejecting a non-positive rate.
- Start, pause, resume and stop state transitions.
- Default rate of 10 transactions/second.
- Custom rate configuration.
- Invalid rate values.

Expected total after adding these tests is 31 tests, assuming the previous 23 tests remain unchanged. This is an expected collection count, not a claimed test result.

---

## Source code

The following are the complete source listings as committed for this task.

### `backend/app/config.py`

```python
"""Runtime settings for the Sentinel local development backend."""
import os


def get_transactions_per_second() -> float:
    """Read the configurable live simulator rate (default: 10 events/second)."""
    raw_value = os.getenv("TRANSACTIONS_PER_SECOND", "10")
    try:
        value = float(raw_value)
    except ValueError as exc:
        raise ValueError("TRANSACTIONS_PER_SECOND must be a positive number") from exc
    if value <= 0:
        raise ValueError("TRANSACTIONS_PER_SECOND must be a positive number")
    return value

```

### `backend/app/services/live_banking_engine.py`

```python
"""Continuous synthetic transfer engine backed by the operational ledger.

This worker creates ordinary synthetic bank transfers. Fraud/AML scenario labels
remain the responsibility of the controlled scenario generator and later
stream-processing stages; this worker never invents model predictions.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
import random
import threading
import time
import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from app.db.repository import persist_live_transfer
from app.db.tables import AccountRow


class LiveBankingEngine:
    """Single-process background worker with start/pause/resume/stop controls."""

    def __init__(
        self,
        session_factory: sessionmaker,
        transactions_per_second: float = 10.0,
        seed: int | None = None,
    ) -> None:
        if transactions_per_second <= 0:
            raise ValueError("transactions_per_second must be greater than zero")
        self._session_factory = session_factory
        self._interval = 1.0 / transactions_per_second
        self._rng = random.Random(seed)
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._resume_event = threading.Event()
        self._resume_event.set()
        self._thread: threading.Thread | None = None
        self._state = "stopped"
        self._generated = 0
        self._last_error: str | None = None
        self._rate = transactions_per_second

    def status(self) -> dict[str, object]:
        with self._lock:
            return {
                "state": self._state,
                "transactions_per_second": self._rate,
                "generated_transactions": self._generated,
                "last_error": self._last_error,
            }

    def start(self) -> dict[str, object]:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return self.status()
            self._stop_event.clear()
            self._resume_event.set()
            self._state = "running"
            self._last_error = None
            self._thread = threading.Thread(
                target=self._run, name="sentinel-banking-engine", daemon=True
            )
            self._thread.start()
        return self.status()

    def pause(self) -> dict[str, object]:
        with self._lock:
            if self._thread is None or not self._thread.is_alive():
                raise RuntimeError("engine is not running")
            self._resume_event.clear()
            self._state = "paused"
        return self.status()

    def resume(self) -> dict[str, object]:
        with self._lock:
            if self._thread is None or not self._thread.is_alive():
                raise RuntimeError("engine is not running")
            self._resume_event.set()
            self._state = "running"
        return self.status()

    def stop(self, timeout: float = 10.0) -> dict[str, object]:
        with self._lock:
            thread = self._thread
            if thread is None:
                self._state = "stopped"
                return self.status()
            self._stop_event.set()
            self._resume_event.set()
        thread.join(timeout=timeout)
        if thread.is_alive():
            raise TimeoutError("engine did not stop within the requested timeout")
        with self._lock:
            self._state = "stopped"
            self._thread = None
        return self.status()

    def generate_one(self) -> str | None:
        """Generate and commit one transfer; public for deterministic integration tests."""
        with self._session_factory() as session:
            accounts = session.scalars(
                select(AccountRow).where(AccountRow.status == "ACTIVE")
            ).all()
            if len(accounts) < 2:
                raise RuntimeError("at least two active accounts are required")
            sender, receiver = self._rng.sample(accounts, 2)
            available = Decimal(sender.balance)
            if available < Decimal("0.01"):
                return None
            requested = Decimal(self._rng.randint(100, 100_000))
            amount = min(requested, available * Decimal("0.25"))
            amount = amount.quantize(Decimal("0.01"), rounding=ROUND_DOWN)
            if amount <= 0:
                return None
            transaction_id = f"TXN{uuid.uuid4().hex[:24].upper()}"
            persist_live_transfer(
                session,
                transaction_id=transaction_id,
                sender_account_id=sender.account_id,
                receiver_account_id=receiver.account_id,
                amount=amount,
                timestamp=datetime.now(timezone.utc),
                channel=self._rng.choice(("UPI", "IMPS", "NEFT", "RTGS", "INTERNAL")),
            )
            session.commit()
            return transaction_id

    def _run(self) -> None:
        while not self._stop_event.is_set():
            self._resume_event.wait()
            if self._stop_event.is_set():
                break
            try:
                transaction_id = self.generate_one()
                if transaction_id is not None:
                    with self._lock:
                        self._generated += 1
                self._last_error = None
            except Exception as exc:  # keep worker alive; expose failure to operations API
                with self._lock:
                    self._last_error = f"{type(exc).__name__}: {exc}"
            # Delay after each committed attempt; do not burst to catch up after DB latency.
            self._stop_event.wait(self._interval)
        with self._lock:
            self._state = "stopped"

```

### `backend/app/db/repository.py`

```python
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
            sender_balance_before=item.sender_balance_before,
            sender_balance_after=item.sender_balance_after,
            receiver_balance_before=item.receiver_balance_before,
            receiver_balance_after=item.receiver_balance_after,
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
        select(TransactionRow).order_by(TransactionRow.timestamp, TransactionRow.transaction_id)
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
            sender_balance_before=row.sender_balance_before,
            sender_balance_after=row.sender_balance_after,
            receiver_balance_before=row.receiver_balance_before,
            receiver_balance_after=row.receiver_balance_after,
            is_fraud=row.is_fraud,
            is_aml_suspicious=row.is_aml_suspicious,
            fraud_scenario=row.fraud_scenario,
            aml_typology=row.aml_typology,
        )
        for row in rows
    ]

def persist_live_transfer(
    session: Session,
    *,
    transaction_id: str,
    sender_account_id: str,
    receiver_account_id: str,
    amount,
    timestamp,
    channel: str,
) -> None:
    """Atomically update account balances and insert one live ledger event.

    PostgreSQL row locks serialize concurrent updates to the same accounts.
    The caller owns the transaction and must commit or roll it back.
    """
    from decimal import Decimal

    from app.db.tables import TransactionRow

    if sender_account_id == receiver_account_id:
        raise ValueError("sender and receiver must be different accounts")
    transfer_amount = Decimal(amount).quantize(Decimal("0.01"))
    if transfer_amount <= 0:
        raise ValueError("transfer amount must be positive")

    account_ids = sorted((sender_account_id, receiver_account_id))
    rows = session.scalars(
        select(AccountRow)
        .where(AccountRow.account_id.in_(account_ids))
        .order_by(AccountRow.account_id)
        .with_for_update()
    ).all()
    by_id = {row.account_id: row for row in rows}
    if len(by_id) != 2:
        raise ValueError("sender and receiver accounts must both exist")

    sender = by_id[sender_account_id]
    receiver = by_id[receiver_account_id]
    if sender.status != "ACTIVE" or receiver.status != "ACTIVE":
        raise ValueError("both accounts must be active")
    sender_before = Decimal(sender.balance)
    receiver_before = Decimal(receiver.balance)
    if transfer_amount > sender_before:
        raise ValueError("insufficient sender balance")

    sender.balance = sender_before - transfer_amount
    receiver.balance = receiver_before + transfer_amount
    session.add(
        TransactionRow(
            transaction_id=transaction_id,
            sender_account_id=sender_account_id,
            receiver_account_id=receiver_account_id,
            amount=transfer_amount,
            currency=sender.currency,
            transaction_type="TRANSFER",
            channel=channel,
            status="COMPLETED",
            timestamp=timestamp,
            sender_balance_before=sender_before,
            sender_balance_after=sender.balance,
            receiver_balance_before=receiver_before,
            receiver_balance_after=receiver.balance,
            is_fraud=False,
            is_aml_suspicious=False,
            fraud_scenario=None,
            aml_typology=None,
        )
    )
    session.flush()


```

### `backend/app/main.py`

```python
"""Sentinel API application entry point and local simulation controls."""

from fastapi import FastAPI, HTTPException

from app.config import get_transactions_per_second
from app.db.database import create_db_engine, create_session_factory
from app.services.live_banking_engine import LiveBankingEngine

app = FastAPI(
    title="Sentinel Banking Intelligence API",
    description=(
        "Downstream fraud detection and AML monitoring platform "
        "for a simulated banking environment."
    ),
    version="0.2.0",
)

_live_engine: LiveBankingEngine | None = None


def _get_live_engine() -> LiveBankingEngine:
    """Create one process-local worker controller on first use."""
    global _live_engine
    if _live_engine is None:
        database_engine = create_db_engine()
        _live_engine = LiveBankingEngine(
            create_session_factory(database_engine),
            transactions_per_second=get_transactions_per_second(),
        )
    return _live_engine


@app.get("/health", tags=["Operations"])
async def health_check() -> dict[str, str]:
    """Return basic process health; dependency health is added in a later task."""
    return {"status": "ok", "service": "sentinel-api", "version": "0.2.0"}


@app.get("/simulation/status", tags=["Simulation"])
async def simulation_status() -> dict[str, object]:
    """Read the local synthetic banking worker state."""
    if _live_engine is None:
        return {
            "state": "stopped",
            "transactions_per_second": get_transactions_per_second(),
            "generated_transactions": 0,
            "last_error": None,
        }
    return _live_engine.status()


@app.post("/simulation/start", tags=["Simulation"])
async def start_simulation() -> dict[str, object]:
    """Start continuous transaction generation in a background thread."""
    try:
        return _get_live_engine().start()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.post("/simulation/pause", tags=["Simulation"])
async def pause_simulation() -> dict[str, object]:
    """Pause the running generator without terminating its worker thread."""
    if _live_engine is None:
        raise HTTPException(status_code=409, detail="engine is not running")
    try:
        return _live_engine.pause()
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/simulation/resume", tags=["Simulation"])
async def resume_simulation() -> dict[str, object]:
    """Resume a paused generator."""
    if _live_engine is None:
        raise HTTPException(status_code=409, detail="engine is not running")
    try:
        return _live_engine.resume()
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/simulation/stop", tags=["Simulation"])
async def stop_simulation() -> dict[str, object]:
    """Stop the worker and wait for its current database operation to finish."""
    if _live_engine is None:
        return {
            "state": "stopped",
            "transactions_per_second": get_transactions_per_second(),
            "generated_transactions": 0,
            "last_error": None,
        }
    try:
        return _live_engine.stop()
    except TimeoutError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

```

### `backend/tests/test_live_banking_engine.py`

```python
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app.db.database import create_schema
from app.db.repository import save_registry
from app.db.tables import AccountRow, TransactionRow
from app.services.live_banking_engine import LiveBankingEngine
from app.services.registry import build_registry


@pytest.fixture
def session_factory():
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    create_schema(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    with factory() as session:
        save_registry(session, build_registry(customer_count=10, seed=5))
        session.commit()
    yield factory
    engine.dispose()


def test_live_transfer_persists_event_and_updates_both_balances(session_factory):
    engine = LiveBankingEngine(session_factory, transactions_per_second=10, seed=7)
    with session_factory() as session:
        before = {
            row.account_id: Decimal(row.balance)
            for row in session.scalars(select(AccountRow)).all()
        }

    transaction_id = engine.generate_one()
    assert transaction_id is not None

    with session_factory() as session:
        transaction = session.get(TransactionRow, transaction_id)
        assert transaction is not None
        sender = session.get(AccountRow, transaction.sender_account_id)
        receiver = session.get(AccountRow, transaction.receiver_account_id)
        assert transaction.sender_balance_before == before[sender.account_id]
        assert transaction.sender_balance_after == sender.balance
        assert transaction.receiver_balance_after == receiver.balance
        assert sender.balance == transaction.sender_balance_before - transaction.amount
        assert receiver.balance == transaction.receiver_balance_before + transaction.amount


def test_engine_rejects_non_positive_rate(session_factory):
    with pytest.raises(ValueError):
        LiveBankingEngine(session_factory, transactions_per_second=0)


def test_engine_start_pause_resume_stop(session_factory):
    engine = LiveBankingEngine(session_factory, transactions_per_second=20, seed=9)
    assert engine.status()["state"] == "stopped"
    engine.start()
    assert engine.status()["state"] == "running"
    engine.pause()
    assert engine.status()["state"] == "paused"
    engine.resume()
    assert engine.status()["state"] == "running"
    result = engine.stop()
    assert result["state"] == "stopped"

```

### `backend/tests/test_live_engine_config.py`

```python
import pytest

from app.config import get_transactions_per_second


def test_default_live_rate_is_ten(monkeypatch):
    monkeypatch.delenv("TRANSACTIONS_PER_SECOND", raising=False)
    assert get_transactions_per_second() == 10.0


def test_live_rate_is_configurable(monkeypatch):
    monkeypatch.setenv("TRANSACTIONS_PER_SECOND", "2.5")
    assert get_transactions_per_second() == 2.5


@pytest.mark.parametrize("value", ["0", "-1", "fast"])
def test_invalid_live_rate_is_rejected(monkeypatch, value):
    monkeypatch.setenv("TRANSACTIONS_PER_SECOND", value)
    with pytest.raises(ValueError):
        get_transactions_per_second()

```
