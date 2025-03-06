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

"""Integration tests for Autosubmit ``DbManager``."""
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database.db_common import get_autosubmit_version, _get_autosubmit_version, \
    _update_experiment_description_version, _get_autosubmit_version_sqlalchemy, _last_name_used_sqlalchemy, check_db_path
from autosubmit.database.db_common import get_connection_url, DbException, create_db, open_conn
from autosubmit.database.db_manager import DbManager
from autosubmit.database.tables import DBVersionTable
from autosubmit.log.log import AutosubmitCritical

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from _pytest._py.path import LocalPath


def _create_db_manager(db_path: Path):
    connection_url = get_connection_url(db_path=db_path)
    return DbManager(connection_url=connection_url)


def test_db_manager_has_made_correct_initialization(tmp_path: "LocalPath") -> None:
    db_manager = _create_db_manager(Path(tmp_path, f'{__name__}.db'))
    assert db_manager.engine.name.startswith('sqlite')


@pytest.mark.docker
@pytest.mark.postgres
def test_after_create_table_command_then_it_returns_1_row(tmp_path: "LocalPath", as_db: str):
    db_manager = _create_db_manager(Path(tmp_path, 'tests.db'))
    db_manager.create_table(DBVersionTable.name)
    count = db_manager.count(DBVersionTable.name)
    assert 1 == count


@pytest.mark.docker
@pytest.mark.postgres
def test_after_3_inserts_into_a_table_then_it_has_4_rows(tmp_path: "LocalPath", as_db: str):
    db_manager = _create_db_manager(Path(tmp_path, 'tests.db'))
    db_manager.create_table(DBVersionTable.name)
    # It already has the first version, so we are adding versions 2, 3, 4...
    for i in range(2, 5):
        db_manager.insert(DBVersionTable.name, {'version': str(i)})
    count = db_manager.count(DBVersionTable.name)
    assert 4 == count


@pytest.mark.docker
@pytest.mark.postgres
def test_select_first_where(tmp_path: "LocalPath", as_db: str):
    db_manager = _create_db_manager(Path(tmp_path, 'tests.db'))
    db_manager.create_table(DBVersionTable.name)
    # It already has the first version, so we are adding versions 2, 3, 4...
    for i in range(2, 5):
        db_manager.insert(DBVersionTable.name, {'version': str(i)})
    first_value = db_manager.select_first_where(DBVersionTable.name, where=None)
    # We are getting the first version, that was already in the database
    assert first_value[0] == 1

    assert 1 == first_value[0]

    last_value = db_manager.select_first_where(DBVersionTable.name, where={'version': '4'})
    assert last_value[0] == 4


def test_create_db_raises_dbexception(monkeypatch, tmp_path: "LocalPath") -> None:
    """
    Test that create_db raises AutosubmitCritical when DbException is raised during connection.
    """

    def mock_open_conn(_):
        raise DbException("Mocked connection error")

    monkeypatch.setattr("autosubmit.database.db_common.open_conn", mock_open_conn)
    with pytest.raises(AutosubmitCritical) as e:
        create_db("CREATE TABLE test (id INTEGER);")
        assert str(e.value).lower() == "could not establish a connection to database"


def test_create_db_sqlite_error(monkeypatch):
    """
    Test that create_db raises AutosubmitCritical when sqlite3.Error is raised.
    """

    class MockCursor:
        def executescript(self, _):
            raise sqlite3.Error("Mocked executescript error")

        def close(self):
            pass

    class MockConn:
        def commit(self):
            pass

        def close(self):
            pass

    def mock_open_conn(_):
        return MockConn(), MockCursor()

    monkeypatch.setattr("autosubmit.database.db_common.open_conn", mock_open_conn)
    with pytest.raises(AutosubmitCritical) as e:
        create_db("CREATE TABLE test (id INTEGER);")
        assert str(e.value).lower() == "database can not be created"


def test_open_conn_with_postgres_raises(monkeypatch) -> None:
    """
    Test that open_conn raises AutosubmitCritical when DATABASE_BACKEND is 'postgres'.

    :param monkeypatch: Pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    :return: None
    :rtype: None
    """
    monkeypatch.setattr("autosubmit.database.db_common.BasicConfig.DATABASE_BACKEND", "postgres")
    with pytest.raises(AutosubmitCritical) as e:
        open_conn("dummy")
        assert "for postgres databases, connections must be open and managed with sqlalchemy!" == str(e.value)


def test_open_conn_check_version_real_sqlite(monkeypatch, tmp_path: "LocalPath") -> None:
    """
    Test that open_conn raises DbException when the database version is not compatible
    using a real SQLite database file.

    :param monkeypatch: Pytest monkeypatch fixture.
    :type monkeypatch: pytest.MonkeyPatch
    :param tmp_path: Temporary path fixture.
    :type tmp_path: LocalPath
    :return: None
    :rtype: None
    """
    db_path = tmp_path / "test.db"

    # Create a real SQLite DB and seed an incompatible version
    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS db_version (version INTEGER NOT NULL);")
        cursor.execute("DELETE FROM db_version;")
        cursor.execute("INSERT INTO db_version (version) VALUES (?);", (9999,))
        conn.commit()
        cursor.close()
    finally:
        conn.close()

    monkeypatch.setattr("autosubmit.database.db_common.BasicConfig.DATABASE_BACKEND", "sqlite", raising=False)
    monkeypatch.setattr("autosubmit.database.db_common.BasicConfig.DB_PATH", str(db_path), raising=False)

    with pytest.raises(AutosubmitCritical) as e:
        open_conn(str(db_path))
        assert "not compatible" in str(e.value).lower()


@pytest.mark.parametrize(
    "scenario",
    [
        "db_version_incompatible_higher",
        "db_version_older",
        "operational_error_missing_tables",
    ],
)
def test_open_conn(monkeypatch: pytest.MonkeyPatch, tmp_path, scenario: str) -> None:
    """
    Parametrized test for open_conn covering all scenarios.
    """
    db_path = tmp_path / f"{scenario}.db"

    if scenario == "db_version_incompatible_higher":
        monkeypatch.setattr("autosubmit.database.db_common.BasicConfig.DATABASE_BACKEND", "sqlite")
        monkeypatch.setattr("autosubmit.database.db_common.BasicConfig.DB_PATH", str(db_path))
        conn = sqlite3.connect(str(db_path))
        try:
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE db_version (version INTEGER NOT NULL);")
            cursor.execute("INSERT INTO db_version (version) VALUES (9999);")
            conn.commit()
            cursor.close()
        finally:
            conn.close()
        with pytest.raises(AutosubmitCritical) as e:
            open_conn(str(db_path))
            assert "not compatible" in str(e.value).lower()
        return

    elif scenario == "db_version_older":
        monkeypatch.setattr("autosubmit.database.db_common.BasicConfig.DATABASE_BACKEND", "sqlite")
        monkeypatch.setattr("autosubmit.database.db_common.BasicConfig.DB_PATH", str(db_path))
        conn = sqlite3.connect(str(db_path))
        try:
            cursor = conn.cursor()
            cursor.execute("CREATE TABLE db_version (version INTEGER NOT NULL);")
            cursor.execute("INSERT INTO db_version (version) VALUES (0);")
            conn.commit()
            cursor.close()
        finally:
            conn.close()
        with pytest.raises(AutosubmitCritical) as e:
            open_conn(str(db_path))
            assert "not compatible" in str(e.value).lower()
        return

    elif scenario == "operational_error_missing_tables":
        monkeypatch.setattr("autosubmit.database.db_common.BasicConfig.DATABASE_BACKEND", "sqlite")
        monkeypatch.setattr("autosubmit.database.db_common.BasicConfig.DB_PATH", str(db_path))
        with pytest.raises(AutosubmitCritical) as e:
            open_conn(str(db_path))
            assert "database version does not match" in str(e.value).lower()

        return


def test_get_autosubmit_version(monkeypatch, tmp_path) -> None:
    """
    Test that get_autosubmit_version returns the correct version from a real SQLite database file.
    """
    db_path = tmp_path / "test.db"

    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE db_version (version INTEGER NOT NULL);")
        cursor.execute("INSERT INTO db_version (version) VALUES (?);", (3,))
        cursor.execute("CREATE TABLE experiment (name, autosubmit_version);")
        cursor.execute("INSERT INTO experiment (name, autosubmit_version) VALUES (?, ?);", ("test_experiment", 3))
        conn.commit()
        cursor.close()
    finally:
        conn.close()

    monkeypatch.setattr("autosubmit.database.db_common.BasicConfig.DATABASE_BACKEND", "sqlite", raising=False)
    monkeypatch.setattr("autosubmit.database.db_common.BasicConfig.DB_PATH", str(db_path), raising=False)
    monkeypatch.setattr("autosubmit.database.db_common.CURRENT_DATABASE_VERSION", 3, raising=False)
    version = _get_autosubmit_version("test_experiment")
    assert version == 3
    version = get_autosubmit_version("test_experiment")
    assert version == 3


@pytest.fixture
def setup_experiment_table(tmp_path, monkeypatch):
    """
    Sets up the experiment table in a temporary database.
    """
    db_path = tmp_path / "test_update_experiment.db"

    # Configure database path
    monkeypatch.setattr("autosubmit.database.db_common.BasicConfig.DATABASE_BACKEND", "sqlite")
    monkeypatch.setattr("autosubmit.database.db_common.BasicConfig.DB_PATH", str(db_path))

    # Create database and tables
    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.cursor()
        # Create db_version table required by check_db
        cursor.execute("CREATE TABLE db_version (version INTEGER NOT NULL);")
        cursor.execute("INSERT INTO db_version (version) VALUES (?);", (3,))  # Use a compatible version

        # Create experiment table
        cursor.execute("""
                       CREATE TABLE experiment
                       (
                           name               TEXT PRIMARY KEY,
                           description        TEXT,
                           autosubmit_version TEXT
                       );
                       """)

        # Insert test data
        cursor.execute(
            "INSERT INTO experiment (name, description, autosubmit_version) VALUES (?, ?, ?);",
            ("test_exp", "Initial description", "1.0.0")
        )
        conn.commit()
        cursor.close()
    finally:
        conn.close()

    monkeypatch.setattr("autosubmit.database.db_common.CURRENT_DATABASE_VERSION", 3)
    return db_path


def test_update_experiment_description_version_both_params(setup_experiment_table):
    """
    Test updating both description and version.
    """
    result = _update_experiment_description_version(
        "test_exp",
        description="Updated description",
        version="2.0.0"
    )
    assert result is True

    conn = sqlite3.connect(str(setup_experiment_table))
    cursor = conn.cursor()
    cursor.execute("SELECT description, autosubmit_version FROM experiment WHERE name=?", ("test_exp",))
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    assert row is not None
    assert row[0] == "Updated description"
    assert row[1] == "2.0.0"


def test_update_experiment_no_params(setup_experiment_table):
    """
    Test updating with neither description nor version provided.

    :param setup_experiment_table: Fixture that sets up the experiment table
    :type setup_experiment_table: Path
    """
    # Should raise an exception when neither description nor version is provided
    with pytest.raises(AutosubmitCritical) as e:
        _update_experiment_description_version("test_exp")

        assert "Not enough data to update" in str(e.value)


def test_update_experiment_nonexistent(setup_experiment_table):
    """
    Test updating a nonexistent experiment.

    :param setup_experiment_table: Fixture that sets up the experiment table
    :type setup_experiment_table: Path
    """
    # Should raise an exception when the experiment doesn't exist
    with pytest.raises(AutosubmitCritical) as e:
        _update_experiment_description_version("nonexistent_exp", description="New description")

        assert "Update on experiment nonexistent_exp failed" in str(e.value)


### SQLAlchemy specific tests below ###

def test_get_autosubmit_version_sqlalchemy(monkeypatch, tmp_path, as_db: str):
    """
    Test that get_autosubmit_version returns the correct version using DbManager (SQLAlchemy).
    """
    if as_db == "sqlite":
        pytest.skip("Skipping SQLAlchemy specific test when using sqlite")
    db_path = tmp_path / "tests.db"
    monkeypatch.setattr("autosubmit.database.db_common.CURRENT_DATABASE_VERSION", 3, raising=False)
    # monkey _get_sqlalchemy_conn
    # Create database and insert version
    db_manager = _create_db_manager(db_path)  # TODO: I think this forces to the db name be "tests.db"
    db_manager.create_table(DBVersionTable.name)
    db_manager.insert(DBVersionTable.name, {'version': 3})
    db_manager.create_table('experiment')
    db_manager.insert('experiment', {'name': 'test_experiment', 'autosubmit_version': 3,
                                     'description': 'Test experiment description'})
    version = _get_autosubmit_version_sqlalchemy("test_experiment")
    assert version == '3'


def test_last_name_used_sqlalchemy(monkeypatch, tmp_path, as_db):
    """
    Test that last_name_used returns the correct last name using DbManager (SQLAlchemy).
    """
    if as_db == "sqlite":
        pytest.skip("Skipping SQLAlchemy specific test when using sqlite")
    db_path = tmp_path / "tests.db"
    db_manager = _create_db_manager(db_path)
    db_manager.create_table('experiment')
    db_manager.insert('experiment', {'name': 'oexp1', 'autosubmit_version': 1, 'description': 'First experiment'})
    db_manager.insert('experiment', {'name': 'texp2', 'autosubmit_version': 1, 'description': 'Second experiment'})
    db_manager.insert('experiment', {'name': 'eexp3', 'autosubmit_version': 1, 'description': 'Third experiment'})
    db_manager.insert('experiment', {'name': 'xp4', 'autosubmit_version': 1, 'description': 'Fourth experiment'})

    last_name = _last_name_used_sqlalchemy(False, True, False)
    assert last_name == 'oexp1'
    last_name = _last_name_used_sqlalchemy(True, False, False)
    assert last_name == 'texp2'
    last_name = _last_name_used_sqlalchemy(False, False, True)
    assert last_name == 'eexp3'
    last_name = _last_name_used_sqlalchemy(False, False, False)
    assert last_name == 'xp4'


def test_delete_experiment_db(monkeypatch, tmp_path):
    """
    Test that delete_experiment works correctly using DbManager (SQLAlchemy).
    """
    db_path = tmp_path / "tests.db"
    db_manager = _create_db_manager(db_path)
    db_manager.create_table('experiment')
    db_manager.insert('experiment', {'name': 'test_experiment', 'autosubmit_version': 1,
                                     'description': 'Test experiment description'})
    count = db_manager.count('experiment')
    assert count == 1
    # Now delete the experiment
    db_manager.delete_where('experiment', {'name': 'test_experiment'})
    count = db_manager.count('experiment')
    assert count == 0


def test_check_db_sqlite(monkeypatch, tmp_path, as_db: str):
    """
    Test that check_db works correctly without DbManager (mainly sqlite)
    """
    if as_db == "postgres":
        pytest.skip("Skipping sqlite specific test when using postgres")
    db_manager = _create_db_manager(BasicConfig.DB_PATH)
    db_manager.create_table(DBVersionTable.name)
    db_manager.insert(DBVersionTable.name, {'version': 3})
    with pytest.raises(ValueError):
        check_db_path(Path("dummy"), must_exists=True)

    exists = check_db_path(Path("dummy"), must_exists=False)
    assert not exists

    exists = check_db_path(Path(BasicConfig.DB_PATH), must_exists=True)
    assert exists
