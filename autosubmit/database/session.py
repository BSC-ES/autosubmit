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

import threading
from pathlib import Path

from sqlalchemy import Engine, NullPool, StaticPool
from sqlalchemy import create_engine as sqlalchemy_create_engine
from sqlalchemy.engine import URL, make_url

from autosubmit.config.basicconfig import BasicConfig

__all__ = ["get_engine"]

# SQLite waits up to this many seconds for a lock before raising
# "database is locked". The pysqlite default (5 seconds) is too low on the
# shared filesystems where experiment databases usually live.
_SQLITE_TIMEOUT_SECONDS = 30

# Recycle PostgreSQL connections older than this many seconds, so a connection
# silently dropped by the infrastructure is not reused.
_POSTGRES_POOL_RECYCLE_SECONDS = 1800

# Engines are cached so that repeated calls reuse the same connection pool.
# PostgreSQL is keyed by connection URL and SQLite by resolved file path.
_ENGINE_CACHE: dict[tuple[str, str], Engine] = {}
_ENGINE_CACHE_LOCK = threading.Lock()


def _is_in_memory(url: URL) -> bool:
    """Return whether a SQLite URL points to an in-memory database.

    :param url: A parsed SQLAlchemy URL.
    :return: ``True`` for in-memory SQLite databases.
    """
    database = url.database
    return not database or database.startswith(":memory:")


def _resolve_engine(connection_url: str | URL) -> Engine:
    """Create a SQLAlchemy Core engine and resolve the connection pool based on the backend.

    SQLite uses a ``NullPool`` so that connections do not accumulate open file
    descriptors, except for in-memory databases, which use a ``StaticPool`` so
    that every connection shares the same in-memory database. PostgreSQL uses
    the default pool with ``pool_pre_ping`` so stale connections are recycled.

    :param connection_url: A SQLAlchemy connection URL.
    :return: A configured SQLAlchemy engine.
    """
    if not connection_url:
        raise ValueError(f"Invalid SQLAlchemy connection URL: {connection_url}")

    url = make_url(connection_url)
    if url.get_backend_name() == "sqlite":
        if _is_in_memory(url):
            return sqlalchemy_create_engine(
                url,
                poolclass=StaticPool,
                connect_args={"check_same_thread": False, "timeout": _SQLITE_TIMEOUT_SECONDS},
            )
        return sqlalchemy_create_engine(
            url,
            poolclass=NullPool,
            connect_args={"timeout": _SQLITE_TIMEOUT_SECONDS},
        )
    return sqlalchemy_create_engine(
        url,
        pool_pre_ping=True,
        pool_recycle=_POSTGRES_POOL_RECYCLE_SECONDS,
    )


def _get_or_create_engine(cache_key: tuple[str, str], connection_url: str | URL) -> Engine:
    """Return the cached engine for ``cache_key``, creating it on first use.

    :param cache_key: The key that identifies the target database.
    :param connection_url: A SQLAlchemy connection URL.
    :return: The cached SQLAlchemy engine.
    """
    with _ENGINE_CACHE_LOCK:
        engine = _ENGINE_CACHE.get(cache_key)
        if engine is None:
            engine = _resolve_engine(connection_url)
            _ENGINE_CACHE[cache_key] = engine
        return engine


def _ensure_sqlite_file(db_path: Path) -> None:
    """Create the SQLite file and its parent directory if they do not exist.

    :param db_path: The resolved path to the SQLite database file.
    """
    if db_path.exists():
        return
    if not db_path.parent.exists():
        db_path.parent.mkdir(parents=True, exist_ok=True)
        db_path.parent.chmod(0o775)
    db_path.touch()
    db_path.chmod(0o775)


def get_engine(db_path: str | Path) -> Engine:
    """Get a SQLAlchemy Core engine.

    The backend is resolved from the AS configuration. PostgreSQL engines are
    cached by connection URL, and SQLite engines by resolved file path, so that
    repeated calls reuse the same connection pool.

    :param db_path: Path to the database file, only used for SQLite.
    :return: A SQLAlchemy engine for the configured backend.
    """
    db_backend = BasicConfig.DATABASE_BACKEND

    if db_backend == "sqlite":
        if str(db_path) == ":memory:":
            # In-memory databases are not cached: each call gets a fresh engine.
            return _resolve_engine("sqlite:///:memory:")
        db_path = Path(db_path).resolve()
        _ensure_sqlite_file(db_path)
        connection_url = URL.create("sqlite", database=str(db_path))
        return _get_or_create_engine(("sqlite", str(db_path)), connection_url)
    elif db_backend == "postgres":
        connection_url = BasicConfig.DATABASE_CONN_URL
        return _get_or_create_engine(("postgres", connection_url), connection_url)
    else:
        raise ValueError(f"Unsupported database backend: {db_backend}")
