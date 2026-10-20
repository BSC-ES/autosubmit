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

import pytest

# noinspection PyProtectedMember
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
    """Test that the engine is created for supported URLs and raises for invalid URLs.

    :param url: SQLAlchemy connection URL.
    :param expected: Expected engine name or exception type.
    :raises ValueError: If the connection URL is invalid.
    """
    if type(expected) is not str:
        with pytest.raises(expected):  # type: ignore
            _resolve_engine(connection_url=url)
    else:
        engine = _resolve_engine(connection_url=url)
        assert engine.name == expected


def test_get_engine_sqlite(tmp_path, mocker):
    """Test that ``get_engine`` creates a SQLite engine.

    :param tmp_path: Temporary directory provided by pytest.
    :param mocker: Pytest-mock fixture.
    """
    mocker.patch("autosubmit.config.basicconfig.BasicConfig.DATABASE_BACKEND", "sqlite")
    mocker.patch("autosubmit.config.basicconfig.BasicConfig.DATABASE_CONN_URL", None)

    database_path = tmp_path / "autosubmit.db"

    assert not database_path.exists()

    engine = get_engine(db_path=database_path)

    try:
        assert engine.name == "sqlite"
    finally:
        engine.dispose()


@pytest.mark.postgres
def test_get_engine_postgres(mocker):
    """Test that ``get_engine`` does not reuse the PostgreSQL engine for subsequent calls.

    The control of whether the same instance is re-used or not must be done by the
    user/caller.

    :param mocker: Pytest-mock fixture.
    """
    mocker.patch(
        "autosubmit.config.basicconfig.BasicConfig.DATABASE_BACKEND",
        "postgres",
    )
    mocker.patch(
        "autosubmit.config.basicconfig.BasicConfig.DATABASE_CONN_URL",
        "postgresql://user:pass@localhost:5432/autosubmit",
    )

    engine = get_engine(db_path="dummy_path")
    engine2 = None

    try:
        assert engine.name == "postgresql"

        engine2 = get_engine(db_path="dummy_path")

        assert engine is not engine2
    finally:
        engine.dispose(close=True)
        if engine2:
            engine2.dispose(close=True)



def test_get_engine_unsupported_backend(mocker):
    """Test that ``get_engine`` raises ``ValueError`` for an unsupported database backend.

    :param mocker: Pytest-mock fixture.
    :raises ValueError: If the database backend is unsupported.
    """
    mocker.patch(
        "autosubmit.config.basicconfig.BasicConfig.DATABASE_BACKEND", "unsupported"
    )
    with pytest.raises(ValueError):
        get_engine(db_path="dummy_path")


def test_get_engine_sqlite_creates_database_file(tmp_path, mocker):
    """Test that ``get_engine`` creates the SQLite database if it does not exist.

    :param tmp_path: Temporary directory provided by pytest.
    :param mocker: Pytest-mock fixture.
    """
    mocker.patch(
        "autosubmit.config.basicconfig.BasicConfig.DATABASE_BACKEND",
        "sqlite",
    )
    mocker.patch(
        "autosubmit.config.basicconfig.BasicConfig.DATABASE_CONN_URL",
        None,
    )

    database_path = tmp_path / "database" / "autosubmit.db"

    assert not database_path.parent.exists()
    assert not database_path.exists()

    engine = get_engine(db_path=database_path)

    try:
        assert database_path.parent.exists()
        assert database_path.exists()
        assert database_path.is_file()
        assert engine.name == "sqlite"
    finally:
        engine.dispose()


def test_get_engine_sqlite_existing_database(tmp_path, mocker):
    """Test that ``get_engine`` uses an existing SQLite database file.

    :param tmp_path: Temporary directory provided by pytest.
    :param mocker: Pytest-mock fixture.
    """
    mocker.patch(
        "autosubmit.config.basicconfig.BasicConfig.DATABASE_BACKEND",
        "sqlite",
    )
    mocker.patch(
        "autosubmit.config.basicconfig.BasicConfig.DATABASE_CONN_URL",
        None,
    )

    database_path = tmp_path / "autosubmit.db"
    database_path.touch()

    engine = get_engine(db_path=database_path)

    try:
        assert database_path.exists()
        assert database_path.is_file()
        assert engine.name == "sqlite"
    finally:
        engine.dispose()
