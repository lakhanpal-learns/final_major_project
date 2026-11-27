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
