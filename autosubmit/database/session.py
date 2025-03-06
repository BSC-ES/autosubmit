from pathlib import Path
from sqlalchemy import Engine, NullPool, create_engine as sqlalchemy_create_engine
from typing import Optional

from autosubmitconfigparser.config.basicconfig import BasicConfig

_SQLITE_IN_MEMORY_URL = "sqlite://"


def _create_sqlite_engine(path: Optional[str] = None) -> Engine:
    # file-based, or in-memory database?
    sqlite_url = f"sqlite:///{Path(path).resolve()}" if path else _SQLITE_IN_MEMORY_URL
    return sqlalchemy_create_engine(sqlite_url, poolclass=NullPool)


def create_engine() -> Engine:
    """Create SQLAlchemy Core engine."""
    if BasicConfig.DATABASE_BACKEND == "postgres":
        return sqlalchemy_create_engine(BasicConfig.DATABASE_CONN_URL)
    else:
        return _create_sqlite_engine()


__all__ = ["create_engine"]
