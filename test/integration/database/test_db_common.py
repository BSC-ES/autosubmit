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

import sqlite3
from pathlib import Path
from typing import Optional

import pytest
from autosubmitconfigparser.config.basicconfig import BasicConfig
from sqlalchemy import select, insert
from sqlalchemy.schema import Column, CreateTable, Table, MetaData

from autosubmit.database import db_common
from autosubmit.database.db_manager import DbManager
from autosubmit.database.tables import DBVersionTable, ExperimentTable, Integer, String
from log.log import AutosubmitCritical

_EXPID = 't999'


def test_create_db_open_conn_fails(mocker):
    mocker.patch('autosubmit.database.db_common.open_conn', side_effect=db_common.DbException('bla'))
    with pytest.raises(AutosubmitCritical) as cm:
        db_common.create_db('')

    assert 'Could not establish a connection to database' in str(cm.value.message)


def test_create_db_executescript_fails(mocker):
    cursor = mocker.MagicMock()
    cursor.executescript.side_effect = sqlite3.Error
    mocker.patch('autosubmit.database.db_common.open_conn', return_value=(mocker.MagicMock(), cursor))
    mocker.patch('autosubmit.database.db_common.close_conn')
    with pytest.raises(AutosubmitCritical) as cm:
        db_common.create_db('')

    assert 'Database can not be created' in str(cm.value.message)


def test_check_db_invalid_sqlite_db(monkeypatch):
    monkeypatch.setattr(BasicConfig, 'DB_PATH', 'you-cannot-find-me.lzw')
    with pytest.raises(AutosubmitCritical) as cm:
        db_common.check_db()

    assert 'DB path does not exist' in cm.value.message


@pytest.mark.parametrize(
    "db_engine",
    [
        # postgres
        pytest.param("postgres", marks=[pytest.mark.postgres]),
        # sqlite
        pytest.param("sqlite")
    ],
)
def test_db_common(db_engine: str, request, monkeypatch):
    """Regression tests for ``db_common.py``.
    Tests for regression issues in ``db_common.py`` functions, and
    for compatibility issues with the new functions for SQLAlchemy.
    The parameters allow the test to be run with different engine+options.
    You can also mark certain tests belonging to a group (e.g. postgres)
    so that they are skipped/executed selectively in CICD environments.
    """
    monkeypatch.setattr(db_common, 'TIMEOUT', 5)

    # Dynamically load the fixture for that DB,
    # ref: https://stackoverflow.com/a/64348247.
    request.getfixturevalue(f"as_db_{db_engine}")

    # The only differences in this test for SQLite and SQLAlchemy are
    # i) the SQL query used to create a DB, ii) we check that the sqlite
    # database file was created and iii) we load a different fixture for
    # sqlite and sqlalchemy (to mock ``BasicConfig`` and run a container
    # for sqlalchemy).
    is_sqlite = db_engine == "sqlite"
    if is_sqlite:
        assert Path(BasicConfig.DB_PATH).exists()

    # Test last name used
    assert "empty" == db_common.last_name_used()
    assert "empty" == db_common.last_name_used(test=True)
    assert "empty" == db_common.last_name_used(operational=True)

    new_exp = {
        "name": "a700",
        "description": "Description",
        "autosubmit_version": "4.0.0",
    }

    # Experiment doesn't exist yet
    with pytest.raises(Exception):
        db_common.check_experiment_exists(new_exp["name"])

    # Test save
    db_common.save_experiment(
        new_exp["name"], new_exp["description"], new_exp["autosubmit_version"]
    )
    assert db_common.check_experiment_exists(
        new_exp["name"], error_on_inexistence=False
    )
    assert db_common.last_name_used() == new_exp["name"]

    # Get version
    assert (
            db_common.get_autosubmit_version(new_exp["name"])
            == new_exp["autosubmit_version"]
    )
    new_version = "v4.1.0"
    db_common.update_experiment_description_version(new_exp["name"], version=new_version)
    assert db_common.get_autosubmit_version(new_exp["name"]) == new_version

    # Update description
    assert (
            db_common.get_experiment_description(new_exp["name"])[0][0]
            == new_exp["description"]
    )
    new_desc = "New Description"
    db_common.update_experiment_description_version(new_exp["name"], description=new_desc)
    assert db_common.get_experiment_description(new_exp["name"])[0][0] == new_desc

    # Update back both: description and version
    db_common.update_experiment_description_version(
        new_exp["name"],
        description=new_exp["description"],
        version=new_exp["autosubmit_version"],
    )
    assert (
            db_common.get_experiment_description(new_exp["name"])[0][0]
            == new_exp["description"]
            and db_common.get_autosubmit_version(new_exp["name"])
            == new_exp["autosubmit_version"]
    )

    # Delete experiment
    assert db_common.delete_experiment(new_exp["name"])
    with pytest.raises(AutosubmitCritical):
        assert (
                db_common.get_autosubmit_version(new_exp["name"])
                == new_exp["autosubmit_version"]
        )


def test_open_conn_postgres_not_valid(monkeypatch):
    """``open_conn`` is only ever to be called when using SQLite!"""
    monkeypatch.setattr(BasicConfig, 'DATABASE_BACKEND', 'postgres')

    with pytest.raises(AutosubmitCritical):
        db_common.open_conn()


def test_open_conn_no_db_version_table(as_db_sqlite):
    """Test that ``open_conn`` handles when there is no db_version table."""
    connection_url = db_common.get_connection_url(BasicConfig.DB_PATH)
    db_manager = DbManager(connection_url=connection_url)

    db_manager.drop_table(DBVersionTable.name)
    db_manager.drop_table(ExperimentTable.name)

    old_experiment = Table(
        'experiment',
        MetaData(),
        Column('id', Integer, primary_key=True)
    )

    with db_manager.engine.connect() as conn:
        conn.execute(CreateTable(old_experiment))
        conn.commit()

    conn, cursor = db_common.open_conn()

    assert conn is not None
    assert cursor is not None

    conn.close()

    test_as_id = 1984
    test_as_version = '1.0.0.'

    updated_experiment = Table(
        'experiment',
        MetaData(),
        Column('id', Integer, primary_key=True),
        Column('autosubmit_version', String)
    )

    # Now, even though ``FakeExperiment`` has only `id`, we must have ``autosubmit_version``,
    # with the entry ``autosubmit_version = "3.0.0b"`` created by ``open_conn``.
    with db_manager.engine.connect() as conn:
        conn.execute(insert(updated_experiment).values(id=test_as_id, autosubmit_version=test_as_version))
        conn.commit()
        row = conn.execute(select(updated_experiment)).fetchone()
        assert row.autosubmit_version == test_as_version
        assert row.id == test_as_id


@pytest.mark.parametrize(
    "db_engine",
    [
        # postgres
        pytest.param("postgres", marks=[pytest.mark.postgres]),
        # sqlite
        pytest.param("sqlite")
    ],
)
def test_check_experiment_exists(db_engine, request, monkeypatch, autosubmit_exp):
    request.getfixturevalue(f"as_db_{db_engine}")

    monkeypatch.setattr(db_common, 'TIMEOUT', 5)

    exp = autosubmit_exp(_EXPID)

    assert db_common.check_experiment_exists(exp.expid)


@pytest.mark.parametrize(
    "db_engine",
    [
        # postgres
        pytest.param("postgres", marks=[pytest.mark.postgres]),
        # sqlite
        pytest.param("sqlite")
    ],
)
def test_get_experiment_description(db_engine, request, monkeypatch, autosubmit_exp):
    request.getfixturevalue(f"as_db_{db_engine}")

    monkeypatch.setattr(db_common, 'TIMEOUT', 5)

    exp = autosubmit_exp(_EXPID)

    description = db_common.get_experiment_description(exp.expid)
    assert description
    assert description[0][0].startswith('Pytest')


@pytest.mark.parametrize(
    "db_engine",
    [
        # postgres
        pytest.param("postgres", marks=[pytest.mark.postgres]),
        # sqlite
        pytest.param("sqlite")
    ],
)
def test_get_experiment_description_invalid_id(db_engine, request, monkeypatch, autosubmit_exp):
    request.getfixturevalue(f"as_db_{db_engine}")

    monkeypatch.setattr(db_common, 'TIMEOUT', 5)

    autosubmit_exp(_EXPID)

    description = db_common.get_experiment_description('z999')
    assert len(description) == 0


@pytest.mark.parametrize(
    "db_engine",
    [
        # postgres
        pytest.param("postgres", marks=[pytest.mark.postgres]),
        # sqlite
        pytest.param("sqlite")
    ],
)
def test_get_experiment_id(db_engine, request, monkeypatch, autosubmit_exp):
    request.getfixturevalue(f"as_db_{db_engine}")

    monkeypatch.setattr(db_common, 'TIMEOUT', 5)

    exp = autosubmit_exp(_EXPID)

    experiment_id: int = db_common.get_experiment_id(exp.expid)
    assert experiment_id > 0


@pytest.mark.parametrize(
    "db_engine",
    [
        # postgres
        pytest.param("postgres", marks=[pytest.mark.postgres]),
        # sqlite
        pytest.param("sqlite")
    ],
)
def test_get_experiment_id_invalid(db_engine, request, monkeypatch, autosubmit_exp):
    request.getfixturevalue(f"as_db_{db_engine}")

    monkeypatch.setattr(db_common, 'TIMEOUT', 5)

    autosubmit_exp(_EXPID)

    with pytest.raises(AutosubmitCritical):
        db_common.get_experiment_id('z999')


@pytest.mark.parametrize(
    "db_engine",
    [
        # postgres
        pytest.param("postgres", marks=[pytest.mark.postgres]),
        # sqlite
        pytest.param("sqlite")
    ],
)
def test_delete_experiment_not_found(db_engine, request, monkeypatch, autosubmit_exp):
    """Test that Autosubmit does not complain when we try to delete an experiment that does not exist."""
    request.getfixturevalue(f"as_db_{db_engine}")

    monkeypatch.setattr(db_common, 'TIMEOUT', 5)

    # Needed to mock the BasicConfig settings...
    exp = autosubmit_exp(_EXPID)

    assert db_common.delete_experiment(f'no-such-experiment-{exp.expid}')


def test_delete_experiment_sqlite_open_conn_failure(monkeypatch, mocker, autosubmit_exp):
    monkeypatch.setattr(db_common, 'TIMEOUT', 5)

    # Needed to mock the BasicConfig settings...
    exp = autosubmit_exp(_EXPID)

    mocker.patch('autosubmit.database.db_common.open_conn', side_effect=db_common.DbException('bla'))

    with pytest.raises(AutosubmitCritical):
        db_common.delete_experiment(exp.expid)


@pytest.mark.parametrize(
    "db_engine",
    [
        # postgres
        pytest.param("postgres", marks=[pytest.mark.postgres]),
        # sqlite
        pytest.param("sqlite")
    ],
)
def test_delete_experiment(db_engine, request, monkeypatch, autosubmit_exp):
    request.getfixturevalue(f"as_db_{db_engine}")

    monkeypatch.setattr(db_common, 'TIMEOUT', 5)

    # Needed to mock the BasicConfig settings...
    exp = autosubmit_exp(_EXPID)

    assert db_common.delete_experiment(exp.expid)
    with pytest.raises(AutosubmitCritical):
        assert db_common.check_experiment_exists(exp.expid, error_on_inexistence=True)


@pytest.mark.parametrize(
    "db_engine,expid,test,operational,evaluation",
    [
        # postgres
        pytest.param("postgres", 't000', True, False, False, marks=[pytest.mark.postgres]),
        pytest.param("postgres", 'z000', False, False, False, marks=[pytest.mark.postgres]),
        pytest.param("postgres", 'a000', False, False, False, marks=[pytest.mark.postgres]),
        pytest.param("postgres", 'e000', False, False, True, marks=[pytest.mark.postgres]),
        pytest.param("postgres", 'o000', False, True, False, marks=[pytest.mark.postgres]),
        # sqlite
        pytest.param("sqlite", 't000', True, False, False),
        pytest.param("sqlite", 'z000', False, False, False),
        pytest.param("sqlite", 'a000', False, False, False),
        pytest.param("sqlite", 'e000', False, False, True),
        pytest.param("sqlite", 'o000', False, True, False),
    ],
)
def test_last_name_used(db_engine, expid: str, test: bool, operational: bool, evaluation: bool,
                        request, monkeypatch, autosubmit_exp):
    request.getfixturevalue(f"as_db_{db_engine}")

    monkeypatch.setattr(db_common, 'TIMEOUT', 5)

    exp = autosubmit_exp(expid, mock_last_name_used=False)

    last_name_used = db_common.last_name_used(test=test, operational=operational, evaluation=evaluation)
    assert last_name_used == exp.expid


@pytest.mark.parametrize(
    "db_engine",
    [
        # postgres
        pytest.param("postgres", marks=[pytest.mark.postgres]),
        # sqlite
        pytest.param("sqlite")
    ],
)
def test_last_name_used_empty(db_engine, request, monkeypatch, autosubmit_exp):
    request.getfixturevalue(f"as_db_{db_engine}")

    monkeypatch.setattr(db_common, 'TIMEOUT', 5)

    autosubmit_exp(_EXPID, mock_last_name_used=False)

    # ``_EXPID`` is a test experiment ID, but we are looking for an operational one... which hasn't been used!
    last_name_used = db_common.last_name_used(test=False, operational=True, evaluation=False)
    assert last_name_used == 'empty'


@pytest.mark.parametrize(
    "db_engine",
    [
        # postgres
        pytest.param("postgres", marks=[pytest.mark.postgres]),
        # sqlite
        pytest.param("sqlite")
    ],
)
def test_last_name_used_is_numeric(db_engine, request, monkeypatch, autosubmit_exp):
    """Apparently, from the comment in the code, AS used to returned numeric expid's in previous versions.
    This test verifies that the code handles that corner case."""
    request.getfixturevalue(f"as_db_{db_engine}")

    monkeypatch.setattr(db_common, 'TIMEOUT', 5)

    autosubmit_exp('42', mock_last_name_used=False)

    last_name_used = db_common.last_name_used(test=False, operational=True, evaluation=False)
    assert last_name_used == 'empty'


@pytest.mark.parametrize(
    "db_engine,description,version,error",
    [
        # postgres
        pytest.param("postgres", "Pytest updated", "New version", None, marks=[pytest.mark.postgres]),
        pytest.param("postgres", None, "New version", None, marks=[pytest.mark.postgres]),
        pytest.param("postgres", "Pytest updated", None, None, marks=[pytest.mark.postgres]),
        pytest.param("postgres", None, None, AutosubmitCritical, marks=[pytest.mark.postgres]),
        # sqlite
        pytest.param("sqlite", "Pytest updated", "New version", None),
        pytest.param("sqlite", None, "New version", None),
        pytest.param("sqlite", "Pytest updated", None, None),
        pytest.param("sqlite", None, None, AutosubmitCritical),
    ],
)
def test_update_experiment_description_version(
        db_engine: str,
        description: str,
        version: str,
        error: Optional[AutosubmitCritical],
        request,
        monkeypatch,
        autosubmit_exp
):
    request.getfixturevalue(f"as_db_{db_engine}")

    monkeypatch.setattr(db_common, 'TIMEOUT', 5)

    exp = autosubmit_exp(_EXPID, mock_last_name_used=False)

    if error is None:
        assert db_common.update_experiment_description_version(exp.expid, description=description, version=version)

        retrieved_description = db_common.get_experiment_description(expid=exp.expid)
        retrieved_version = db_common.get_autosubmit_version(expid=exp.expid)

        # The function updates either or both, so in this test we need to choose what to
        # compare and assert.
        if description is not None:
            assert description == retrieved_description[0][0]
        if version is not None:
            assert version == retrieved_version
    else:
        with pytest.raises(error):  # type: ignore
            db_common.update_experiment_description_version(exp.expid, description=description, version=version)


@pytest.mark.parametrize(
    "db_engine",
    [
        # postgres
        pytest.param("postgres", marks=[pytest.mark.postgres]),
        # sqlite
        pytest.param("sqlite")
    ]
)
def test_update_experiment_description_version_wrong_expid(
        db_engine: str,
        request,
        monkeypatch,
        autosubmit_exp
):
    request.getfixturevalue(f"as_db_{db_engine}")

    monkeypatch.setattr(db_common, 'TIMEOUT', 5)

    autosubmit_exp(_EXPID, mock_last_name_used=False)

    with pytest.raises(AutosubmitCritical) as cm:  # type: ignore
        db_common.update_experiment_description_version('0000', description="Not used", version="Not Used")

    assert '0000' in str(cm.value.message)


@pytest.mark.parametrize(
    "db_engine",
    [
        # postgres
        pytest.param("postgres", marks=[pytest.mark.postgres]),
        # sqlite
        pytest.param("sqlite")
    ]
)
def test_save_experiment(
        db_engine: str,
        request,
        monkeypatch,
        autosubmit_exp
):
    request.getfixturevalue(f"as_db_{db_engine}")

    monkeypatch.setattr(db_common, 'TIMEOUT', 5)

    # Needed to mock BasicConfig, ...
    autosubmit_exp(_EXPID, mock_last_name_used=False)

    new_expid = 'z000'
    new_description = 'Chough'
    new_version = 'v42'

    assert db_common.save_experiment(name=new_expid, description=new_description, version=new_version)

    retrieved_description = db_common.get_experiment_description(new_expid)
    retrieved_version = db_common.get_autosubmit_version(new_expid)

    assert new_description == retrieved_description[0][0]
    assert new_version == retrieved_version


@pytest.mark.parametrize(
    "db_engine",
    [
        # postgres
        pytest.param("postgres", marks=[pytest.mark.postgres]),
        # sqlite
        pytest.param("sqlite")
    ]
)
def test_save_experiment_error_expid_is_none(
        db_engine: str,
        request,
        monkeypatch,
        autosubmit_exp
):
    request.getfixturevalue(f"as_db_{db_engine}")

    monkeypatch.setattr(db_common, 'TIMEOUT', 5)

    # Needed to mock BasicConfig, ...
    autosubmit_exp(_EXPID, mock_last_name_used=False)

    new_description = 'Chough'
    new_version = 'v42'

    with pytest.raises(AutosubmitCritical):
        assert db_common.save_experiment(name=None, description=new_description, version=new_version)  # type: ignore


def test_get_connection_url_invalid_sqlite_db():
    with pytest.raises(ValueError):
        db_common.get_connection_url(None)


def test_get_connection_url_pg(monkeypatch):
    monkeypatch.setattr(BasicConfig, 'DATABASE_BACKEND', 'postgres')
    monkeypatch.setattr(BasicConfig, 'DATABASE_CONN_URL', 'postgres://user:pass@host:port/dbname')
    assert 'postgres' in db_common.get_connection_url()
