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

    assert counts == {"customers": 10, "accounts": 11, "transactions": 18}
    assert len(list_customers(session)) == 10
    assert len(list_accounts(session)) == 11
    assert len(list_transactions(session)) == 18


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
