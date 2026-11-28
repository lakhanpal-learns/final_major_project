import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db.database import create_schema
from app.db.tables import AccountRow, CustomerRow, TransactionRow, AMLFindingRow, AMLEventInboxRow
from app.models.events import TransactionCreatedEvent
from app.services.aml_rules import evaluate_aml_event, persist_aml_findings
from app.services.aml_event_consumer import process_aml_message


@pytest.fixture
def factory():
    engine = create_engine("sqlite+pysqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    create_schema(engine)
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    now = datetime(2026, 11, 28, 12, tzinfo=timezone.utc)
    with Session.begin() as session:
        session.add(CustomerRow(customer_id="CUS1", full_name="Test", customer_type="PERSON", city="Mohali", created_at=now))
        for account in ("A1", "A2", "A3", "A4", "A5"):
            session.add(AccountRow(account_id=account, account_number=f"000{account}", customer_id="CUS1",
                account_type="SAVINGS", currency="INR", balance=Decimal("1000000"), status="ACTIVE", opened_at=now))
    yield Session, now
    engine.dispose()


def add_tx(session, txid, sender, receiver, amount, ts):
    session.add(TransactionRow(transaction_id=txid, sender_account_id=sender, receiver_account_id=receiver,
        amount=Decimal(str(amount)), currency="INR", transaction_type="TRANSFER", channel="UPI",
        status="COMPLETED", timestamp=ts, sender_balance_before=None, sender_balance_after=None,
        receiver_balance_before=None, receiver_balance_after=None, is_fraud=False, is_aml_suspicious=False,
        fraud_scenario=None, aml_typology=None))


def make_event(txid, sender, receiver, amount, ts):
    return TransactionCreatedEvent.model_validate({
        "event_id": str(uuid4()), "event_type": "bank.transaction.created.v1", "event_version": 1,
        "occurred_at": ts.isoformat(), "transaction": {"transaction_id": txid, "sender_account_id": sender,
        "receiver_account_id": receiver, "amount": str(amount), "currency": "INR",
        "transaction_type": "TRANSFER", "channel": "UPI", "status": "COMPLETED", "timestamp": ts.isoformat()}
    })


def test_detects_fan_in_without_using_aml_labels(factory):
    Session, now = factory
    with Session.begin() as s:
        add_tx(s, "P1", "A1", "A4", 50000, now - timedelta(minutes=30))
        add_tx(s, "P2", "A2", "A4", 50000, now - timedelta(minutes=20))
        add_tx(s, "P3", "A3", "A4", 50000, now - timedelta(minutes=10))
        add_tx(s, "CURRENT", "A5", "A4", 50000, now)
    with Session() as s:
        event = make_event("CURRENT", "A5", "A4", 50000, now)
        found = evaluate_aml_event(s, event)
    assert "FAN_IN" in {item["typology"] for item in found}


def test_detects_fan_out(factory):
    Session, now = factory
    with Session.begin() as s:
        add_tx(s, "P1", "A1", "A2", 50000, now - timedelta(minutes=30))
        add_tx(s, "P2", "A1", "A3", 50000, now - timedelta(minutes=20))
        add_tx(s, "P3", "A1", "A4", 50000, now - timedelta(minutes=10))
        add_tx(s, "CURRENT", "A1", "A5", 50000, now)
    with Session() as s:
        found = evaluate_aml_event(s, make_event("CURRENT", "A1", "A5", 50000, now))
    assert "FAN_OUT" in {item["typology"] for item in found}


def test_detects_pass_through_layering(factory):
    Session, now = factory
    with Session.begin() as s:
        add_tx(s, "IN", "A1", "A2", 5000, now - timedelta(minutes=5))
        add_tx(s, "OUT", "A2", "A3", 4900, now)
    with Session() as s:
        found = evaluate_aml_event(s, make_event("OUT", "A2", "A3", 4900, now))
    assert "LAYERING" in {item["typology"] for item in found}


def test_detects_cycle_closure(factory):
    Session, now = factory
    with Session.begin() as s:
        add_tx(s, "P1", "A2", "A3", 1000, now - timedelta(minutes=20))
        add_tx(s, "P2", "A3", "A1", 1000, now - timedelta(minutes=10))
        add_tx(s, "CURRENT", "A1", "A2", 1000, now)
    with Session() as s:
        found = evaluate_aml_event(s, make_event("CURRENT", "A1", "A2", 1000, now))
    assert "CIRCULAR_TRANSFER" in {item["typology"] for item in found}


def test_does_not_use_same_timestamp_future_or_label_metadata(factory):
    Session, now = factory
    with Session.begin() as s:
        add_tx(s, "SAME1", "A1", "A4", 1000, now)
        add_tx(s, "SAME2", "A2", "A4", 1000, now)
        add_tx(s, "CURRENT", "A3", "A4", 1000, now)
    with Session() as s:
        found = evaluate_aml_event(s, make_event("CURRENT", "A3", "A4", 1000, now))
    assert "FAN_IN" not in {item["typology"] for item in found}


def test_consumer_persists_inbox_and_findings_idempotently(factory):
    Session, now = factory
    with Session.begin() as s:
        add_tx(s, "P1", "A1", "A4", 1000, now - timedelta(minutes=20))
        add_tx(s, "P2", "A2", "A4", 1000, now - timedelta(minutes=10))
        add_tx(s, "CURRENT", "A3", "A4", 1000, now)
    event = make_event("CURRENT", "A3", "A4", 1000, now)
    raw = json.dumps(event.model_dump(mode="json"))
    assert process_aml_message(Session, value=raw).startswith("processed:")
    assert process_aml_message(Session, value=raw) == "duplicate"
    with Session() as s:
        assert len(s.scalars(select(AMLEventInboxRow)).all()) == 1
        assert len(s.scalars(select(AMLFindingRow)).all()) >= 1


def test_event_must_match_canonical_ledger(factory):
    Session, now = factory
    with Session.begin() as s:
        add_tx(s, "CURRENT", "A1", "A2", 1000, now)
    with Session() as s:
        with pytest.raises(ValueError, match="does not match"):
            evaluate_aml_event(s, make_event("CURRENT", "A1", "A3", 1000, now))

 
def test_does_not_flag_layering_for_unrelated_amounts(factory):
    Session, now = factory
    with Session.begin() as s:
        add_tx(s, "IN", "A1", "A2", 10000, now - timedelta(minutes=5))
        add_tx(s, "OUT", "A2", "A3", 7796, now)
    with Session() as s:
        found = evaluate_aml_event(s, make_event("OUT", "A2", "A3", 7796, now))
    assert "LAYERING" not in {item["typology"] for item in found}


def test_detects_structuring_with_repeated_small_outgoing(factory):
    Session, now = factory
    with Session.begin() as s:
        add_tx(s, "S1", "A1", "A2", 30000, now - timedelta(hours=3))
        add_tx(s, "S2", "A1", "A3", 30000, now - timedelta(hours=2))
        add_tx(s, "S3", "A1", "A4", 30000, now - timedelta(hours=1))
        add_tx(s, "S4", "A1", "A5", 30000, now)
    with Session() as s:
        found = evaluate_aml_event(s, make_event("S4", "A1", "A5", 30000, now))
    assert "STRUCTURING" in {item["typology"] for item in found}
