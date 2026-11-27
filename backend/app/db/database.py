"""SQLAlchemy engine, session factory and schema bootstrap helpers."""

import os

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

DEFAULT_DATABASE_URL = "postgresql+psycopg://sentinel:sentinel@localhost:5432/sentinel"


class Base(DeclarativeBase):
    """Base class for Sentinel relational tables."""


def get_database_url() -> str:
    return os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)


def create_db_engine(database_url: str | None = None) -> Engine:
    return create_engine(database_url or get_database_url(), pool_pre_ping=True)


def create_session_factory(engine: Engine) -> sessionmaker:
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def create_schema(engine: Engine) -> None:
    """Create missing tables for local development and tests."""
    from app.db import tables  # noqa: F401

    Base.metadata.create_all(bind=engine)
