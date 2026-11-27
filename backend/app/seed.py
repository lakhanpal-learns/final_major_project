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
