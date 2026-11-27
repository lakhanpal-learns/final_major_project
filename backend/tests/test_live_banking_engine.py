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
