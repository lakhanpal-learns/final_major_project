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
