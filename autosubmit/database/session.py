# Copyright 2015-2025 Earth Sciences Department, BSC-CNS
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

from pathlib import Path
from typing import Union

from autosubmit.config.basicconfig import BasicConfig
from sqlalchemy import Engine, NullPool, create_engine as sqlalchemy_create_engine


def _resolve_engine(connection_url: str) -> Engine:
    """Create SQLAlchemy Core engine and resolves the connection pool class based on the backend.

    :param connection_url: A SQLAlchemy connection URL.
    """
    if not connection_url:
        raise ValueError(f"Invalid SQLAlchemy connection URL: {connection_url}")

    is_sqlite = connection_url.startswith("sqlite")
    pool_class = NullPool if is_sqlite else None
    return sqlalchemy_create_engine(connection_url, poolclass=pool_class)


class PostgreSQLEngineSingleton:
    """Singleton class to manage a single instance of the PostgreSQL engine."""

    _instance: Engine = None

    @classmethod
    def get_instance(cls) -> Engine:
        """Get the singleton instance of the PostgreSQL engine."""
        if cls._instance is None:
            connection_url = BasicConfig.DATABASE_CONN_URL
            if not connection_url:
                raise ValueError(
                    "PostgreSQL connection URL is not set in the configuration."
                )
            cls._instance = _resolve_engine(connection_url)
        return cls._instance


def get_engine(db_path: Union[str, Path]) -> Engine:
    """Get SQLAlchemy Core engine.
    This will resolve which backend to use based on the AS configuration.

    In case the backend is PostgreSQL, the connection URL will be read from the environment variable,
    and the engine will be reutilized from the global variable to use the same connection pool.

    In case the backend is SQLite, a new engine will be created for each call based on the provided database path
    with a NullPool to avoid accumulating open file descriptors.

    :param db_path: Path to the database file, only used for SQLite.
    """
    db_backend = BasicConfig.DATABASE_BACKEND
    db_path = Path(db_path) if isinstance(db_path, str) else db_path
    db_path = db_path.resolve()

    if db_backend == "sqlite":
        connection_url = f"sqlite:///{db_path}"
        return _resolve_engine(connection_url)
    elif db_backend == "postgres":
        # Get from singleton engine
        return PostgreSQLEngineSingleton.get_instance()
    else:
        raise ValueError(f"Unsupported database backend: {db_backend}")


__all__ = ["_resolve_engine", "get_engine"]
