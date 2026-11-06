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

"""Unit tests for ``autosubmit.database.session``."""

import pytest
from sqlalchemy import NullPool, QueuePool, StaticPool
from sqlalchemy.engine import URL

from autosubmit.database.session import _resolve_engine, get_engine


@pytest.mark.parametrize(
    "url,expected",
    [
        ("postgresql://user:pass@host:1984/db", "postgresql"),
        ("sqlite://", "sqlite"),
        (None, ValueError),
    ],
)
def test_resolve_engine(url: str, expected: str | Exception):
    if type(expected) is not str:
        with pytest.raises(expected):  # type: ignore
            _resolve_engine(connection_url=url)
    else:
        engine = _resolve_engine(connection_url=url)
        assert engine.name == expected


@pytest.mark.parametrize(
    "url,expected_pool",
    [
        ("sqlite://", StaticPool),
        ("sqlite:///:memory:", StaticPool),
        ("sqlite:////tmp/autosubmit-test.db", NullPool),
        ("postgresql://user:pass@host:1984/db", QueuePool),
    ],
)
def test_resolve_engine_pool(url: str, expected_pool: type):
    """SQLite uses NullPool (StaticPool in memory) and PostgreSQL the default pool."""
    engine = _resolve_engine(connection_url=url)

    assert isinstance(engine.pool, expected_pool)


def test_resolve_engine_accepts_url_object():
    """The engine factory accepts an already-parsed URL, not only a string."""
    engine = _resolve_engine(connection_url=URL.create("sqlite", database=":memory:"))

    assert engine.name == "sqlite"
    assert isinstance(engine.pool, StaticPool)


def test_resolve_engine_postgres_recycles_connections():
    """PostgreSQL engines pre-ping and recycle stale connections."""
    engine = _resolve_engine(connection_url="postgresql://user:pass@host:1984/db")

    assert engine.pool._pre_ping is True
    assert engine.pool._recycle == 1800


def test_get_engine_sqlite(mocker):
    mocker.patch("autosubmit.config.basicconfig.BasicConfig.DATABASE_BACKEND", "sqlite")
    mocker.patch("autosubmit.config.basicconfig.BasicConfig.DATABASE_CONN_URL", None)
    engine = get_engine(db_path=":memory:")
    assert engine.name == "sqlite"


def test_get_engine_sqlite_is_cached_by_path(mocker, tmp_path):
    """Repeated calls for the same file reuse the engine; different files do not."""
    mocker.patch("autosubmit.config.basicconfig.BasicConfig.DATABASE_BACKEND", "sqlite")
    db_path = tmp_path / "cache.db"

    first = get_engine(db_path=db_path)
    second = get_engine(db_path=db_path)
    other = get_engine(db_path=tmp_path / "other.db")

    assert first is second
    assert first is not other


def test_get_engine_sqlite_creates_file_and_sets_timeout(mocker, tmp_path):
    """The SQLite file is created and the busy timeout is configured (no WAL)."""
    mocker.patch("autosubmit.config.basicconfig.BasicConfig.DATABASE_BACKEND", "sqlite")
    db_path = tmp_path / "nested" / "data.db"

    engine = get_engine(db_path=db_path)

    assert db_path.exists()
    with engine.connect() as conn:
        assert conn.exec_driver_sql("PRAGMA busy_timeout").scalar() == 30000
        assert conn.exec_driver_sql("PRAGMA journal_mode").scalar() == "delete"


def test_get_engine_memory_is_not_cached_and_creates_no_file(mocker, tmp_path, monkeypatch):
    """In-memory databases are never cached and do not touch the filesystem."""
    mocker.patch("autosubmit.config.basicconfig.BasicConfig.DATABASE_BACKEND", "sqlite")
    monkeypatch.chdir(tmp_path)

    first = get_engine(db_path=":memory:")
    second = get_engine(db_path=":memory:")

    assert first is not second
    assert not (tmp_path / ":memory:").exists()


def test_get_engine_postgres_is_cached_by_url(mocker):
    """PostgreSQL engines are cached per connection URL."""
    mocker.patch("autosubmit.config.basicconfig.BasicConfig.DATABASE_BACKEND", "postgres")
    mocker.patch(
        "autosubmit.config.basicconfig.BasicConfig.DATABASE_CONN_URL",
        "postgresql://user:pass@localhost:5432/one",
    )
    first = get_engine(db_path="ignored")
    second = get_engine(db_path="ignored")

    mocker.patch(
        "autosubmit.config.basicconfig.BasicConfig.DATABASE_CONN_URL",
        "postgresql://user:pass@localhost:5432/two",
    )
    third = get_engine(db_path="ignored")

    assert first is second
    assert first is not third


@pytest.mark.postgres
def test_get_engine_postgres(mocker):
    mocker.patch(
        "autosubmit.config.basicconfig.BasicConfig.DATABASE_BACKEND",
        "postgres",
    )
    mocker.patch(
        "autosubmit.config.basicconfig.BasicConfig.DATABASE_CONN_URL",
        "postgresql://user:pass@localhost:5432/autosubmit",
    )

    engine = get_engine(db_path="dummy_path")

    assert engine.name == "postgresql"

    # Singleton behaviour: subsequent calls should return the same instance
    engine2 = get_engine(db_path="dummy_path")

    assert engine is engine2


def test_get_engine_unsupported_backend(mocker):
    mocker.patch(
        "autosubmit.config.basicconfig.BasicConfig.DATABASE_BACKEND", "unsupported"
    )
    with pytest.raises(ValueError):
        get_engine(db_path="dummy_path")
