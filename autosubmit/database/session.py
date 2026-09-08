# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
#
# This file is part of Autosubmit.
#
# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

"""Autosubmit database session."""

from enum import Enum
from pathlib import Path

from sqlalchemy import Engine, NullPool
from sqlalchemy import create_engine as sqlalchemy_create_engine

from autosubmit.config.basicconfig import BasicConfig

__all__ = ["get_engine"]


class DatabaseType(str, Enum):
    """Enum for the database engine."""

    SQLITE = "sqlite"
    """SQLite database."""

    POSTGRES = "postgres"
    """Postgres database."""


def _resolve_engine(connection_url: str) -> Engine:
    """Create SQLAlchemy Core engine and resolves the connection pool class based on the backend.

    :param connection_url: An SQLAlchemy connection URL.
    """
    if not connection_url:
        raise ValueError(f"Invalid SQLAlchemy connection URL: {connection_url}")

    is_sqlite = connection_url.startswith("sqlite")
    pool_class = NullPool if is_sqlite else None
    return sqlalchemy_create_engine(connection_url, poolclass=pool_class)


def get_engine(db_path: str | Path) -> Engine:
    """Get SQLAlchemy Core engine.

    This will resolve which backend to use based on the Autosubmit configuration.

    If the backend is Postgres, Autosubmit will load the settings from the configuration file
    and an engine will be created.

    If the backend is SQLite, it uses the provided database path and a ``NullPool``
    to reduce the number of open file descriptors. It supports the ``:memory:``
    value to return an in-memory SQLite database.

    For SQLite, if the parent directory does not exist, it is created with the permission
    ``755``. Likewise, if the database file does not exist, it is created the same way.

    :param db_path: Path to the database file, only used for SQLite.
    :return: SQLAlchemy Core engine.
    """
    db_backend = BasicConfig.DATABASE_BACKEND

    if db_backend not in (DatabaseType.SQLITE, DatabaseType.POSTGRES):
        raise ValueError(f"Unsupported database backend: {db_backend}")

    if db_backend == DatabaseType.POSTGRES:
        return _resolve_engine(BasicConfig.DATABASE_CONN_URL)

    if db_path == ":memory:":
        return _resolve_engine("sqlite:///:memory:")

    db_path = Path(db_path).resolve()

    if not db_path.parent.exists():
        db_path.parent.mkdir(parents=True)
        db_path.parent.chmod(0o775)

    if not db_path.exists():
        db_path.touch()
        db_path.chmod(0o775)

    return _resolve_engine(f"sqlite:///{db_path}")
