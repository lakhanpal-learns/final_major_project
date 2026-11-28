from decimal import Decimal

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db.database import create_schema
from app.db.repository import persist_live_transfer, save_registry
from app.db.tables import AccountRow, OutboxEventRow, TransactionRow
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


def test_live_transfer_creates_matching_outbox_event(session_factory):
    engine = LiveBankingEngine(session_factory, transactions_per_second=10, seed=7)
    transaction_id = engine.generate_one()
    assert transaction_id is not None

    with session_factory() as session:
        transaction = session.get(TransactionRow, transaction_id)
        events = session.scalars(
            select(OutboxEventRow).where(OutboxEventRow.aggregate_id == transaction_id)
        ).all()

        assert transaction is not None
        assert len(events) == 1
        event = events[0]
        assert event.aggregate_type == "transaction"
        assert event.event_type == "bank.transaction.created.v1"
        assert event.published_at is None
        assert event.attempts == 0
        assert event.payload["event_id"] == event.event_id
        assert event.payload["transaction"]["transaction_id"] == transaction_id
        assert event.payload["transaction"]["amount"] == str(transaction.amount)
        assert event.payload["transaction"]["sender_account_id"] == transaction.sender_account_id
        assert event.payload["transaction"]["receiver_account_id"] == transaction.receiver_account_id


def test_outbox_payload_does_not_contain_training_labels_or_scenario_metadata(session_factory):
    engine = LiveBankingEngine(session_factory, transactions_per_second=10, seed=11)
    transaction_id = engine.generate_one()
    assert transaction_id is not None

    with session_factory() as session:
        event = session.scalars(
            select(OutboxEventRow).where(OutboxEventRow.aggregate_id == transaction_id)
        ).one()
        serialized = str(event.payload).lower()
        for forbidden in ("is_fraud", "is_aml_suspicious", "fraud_scenario", "aml_typology"):
            assert forbidden not in serialized


def test_rollback_reverts_balances_transaction_and_outbox_together(session_factory):
    with session_factory() as session:
        accounts = session.scalars(
            select(AccountRow).order_by(AccountRow.account_id)
        ).all()
        sender, receiver = accounts[0], accounts[1]
        sender_before = Decimal(sender.balance)
        receiver_before = Decimal(receiver.balance)
        transaction_id = "TXNROLLBACK00000000000001"

        persist_live_transfer(
            session,
            transaction_id=transaction_id,
            sender_account_id=sender.account_id,
            receiver_account_id=receiver.account_id,
            amount=Decimal("1.00"),
            timestamp=__import__("datetime").datetime.now(__import__("datetime").timezone.utc),
            channel="UPI",
        )
        session.rollback()

    with session_factory() as session:
        assert session.get(TransactionRow, transaction_id) is None
        assert session.scalars(
            select(OutboxEventRow).where(OutboxEventRow.aggregate_id == transaction_id)
        ).all() == []
        assert Decimal(session.get(AccountRow, sender.account_id).balance) == sender_before
        assert Decimal(session.get(AccountRow, receiver.account_id).balance) == receiver_before


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
