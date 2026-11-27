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
