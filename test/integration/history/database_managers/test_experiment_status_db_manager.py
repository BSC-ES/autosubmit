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

"""Integration tests for the experiment status DB managers."""

from pathlib import Path
import sqlite3
from typing import cast, TYPE_CHECKING

import pytest
from sqlalchemy import inspect

from autosubmit.database.tables import ExperimentStatusTable
from autosubmit.history.database_managers.database_models import (
    ExperimentRow,
    ExperimentStatusRow,
)
from autosubmit.history.database_managers.experiment_status_db_manager import (
    ExperimentStatusDbManager,
    SqlAlchemyExperimentStatusDbManager,
    create_experiment_status_db_manager,
)

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from _pytest._py.path import LocalPath


def test_create_experiment_status_db_manager_invalid_value():
    with pytest.raises(ValueError):
        create_experiment_status_db_manager(None)  # type: ignore


@pytest.mark.docker
@pytest.mark.postgres
def test_experiment_status_db_manager(tmp_path: 'LocalPath', as_db: str, get_next_expid):
    expid = get_next_expid()
    options = {"expid": expid}
    tmp_test_dir = tmp_path / "test_status"
    tmp_test_dir.mkdir()

    clazz = SqlAlchemyExperimentStatusDbManager

    is_sqlalchemy = as_db == "sqlite"
    if is_sqlalchemy:
        clazz = ExperimentStatusDbManager

        options["db_dir_path"] = tmp_test_dir
        options["local_root_dir_path"] = tmp_test_dir
        options["main_db_name"] = "tests.db"

    # Assert type of database manager
    database_manager = create_experiment_status_db_manager(as_db, **options)  # type: clazz
    assert isinstance(database_manager, clazz)

    # Test initialization of the table (is possible is created by some previous test)
    if is_sqlalchemy:
        assert Path(cast(ExperimentStatusDbManager, database_manager)._as_times_file_path).exists()
    else:
        inspector = inspect(database_manager.engine)
        assert inspector.has_table(ExperimentStatusTable.name, schema="public")

    # Test methods
    # Create as RUNNING
    experiment = ExperimentRow(id=1, name=options["expid"], autosubmit_version="4.1.10", description="test")
    database_manager.create_experiment_status_as_running(experiment)

    exp_status: ExperimentStatusRow = (database_manager.get_experiment_status_row_by_exp_id(exp_id=experiment.id))
    assert exp_status.status == "RUNNING"

    # Update status
    database_manager.update_exp_status(experiment.name, "NOT RUNNING")
    exp_status: ExperimentStatusRow = (database_manager.get_experiment_status_row_by_exp_id(exp_id=experiment.id))
    assert exp_status.status == "NOT RUNNING"

    # Set back to RUNNING
    database_manager.set_existing_experiment_status_as_running(exp_status.name)
    exp_status: ExperimentStatusRow = (database_manager.get_experiment_status_row_by_exp_id(exp_id=experiment.id))
    assert exp_status.status == "RUNNING"


@pytest.mark.docker
@pytest.mark.postgres
def test_get_experiment_status_row_by_expid(
    tmp_path: "LocalPath", as_db: str, autosubmit_exp, get_next_expid
):
    expid = get_next_expid()
    options = {"expid": expid}

    is_sqlalchemy = as_db == "sqlite"
    if is_sqlalchemy:
        options["db_dir_path"] = tmp_path
        options["local_root_dir_path"] = tmp_path
        options["main_db_name"] = "tests.db"

    database_manager = create_experiment_status_db_manager(as_db, **options)

    # An error as there is no such experiment ID in the database
    with pytest.raises(ValueError):
        database_manager.get_experiment_status_row_by_expid(expid)

    # Create the experiment, it will have status 'NOT RUNNING' in the experiment_status table
    exp = autosubmit_exp(expid=expid, include_jobs=True)
    experiment_status_row = database_manager.get_experiment_status_row_by_expid(
        exp.expid
    )
    assert (
        experiment_status_row
        and experiment_status_row.status == "NOT RUNNING"
    )


def test_experiment_status_db_manager_adds_last_heartbeat_column_if_missing(
    tmp_path: "LocalPath",
):
    """Test that the manager adds last_heartbeat column into experiment_status table if it is missing."""
    db_dir = tmp_path / "db"
    db_dir.mkdir()
    local_root_dir = tmp_path / "local"
    local_root_dir.mkdir()

    as_times_path = db_dir / "as_times.db"
    with sqlite3.connect(as_times_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE experiment_status (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                status TEXT NOT NULL,
                created TIMESTAMP NOT NULL,
                modified TIMESTAMP NOT NULL
            )
            """
        )
        conn.commit()

    database_manager = ExperimentStatusDbManager(
        expid="a000",
        db_dir_path=str(db_dir),
        main_db_name="test.db",
        local_root_dir_path=str(local_root_dir),
    )

    with sqlite3.connect(as_times_path) as conn:
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(experiment_status)")
        columns = [row[1] for row in cursor.fetchall()]

    assert "last_heartbeat" in columns
    assert isinstance(database_manager, ExperimentStatusDbManager)


def test_set_exp_status_logs_warning_when_get_experiment_status_row_by_expid_fails(
    tmp_path: "LocalPath", mocker
):
    """Test that set_exp_status() logs a warning when get_experiment_status_row_by_expid() fails."""
    db_dir = tmp_path / "db"
    db_dir.mkdir()
    local_root_dir = tmp_path / "local"
    local_root_dir.mkdir()

    database_manager = ExperimentStatusDbManager(
        expid="a000",
        db_dir_path=str(db_dir),
        main_db_name="test.db",
        local_root_dir_path=str(local_root_dir),
    )

    # Mock
    warning_mock = mocker.patch(
        "autosubmit.history.database_managers.experiment_status_db_manager.Log.warning"
    )
    mocker.patch.object(
        database_manager,
        "get_experiment_status_row_by_expid",
        side_effect=ValueError("missing experiment status row"),
    )

    get_experiment_status_row_mock = mocker.patch.object(
        database_manager, "get_experiment_row_by_expid"
    )
    create_status_mock = mocker.patch.object(database_manager, "create_exp_status")

    # Act
    database_manager.set_exp_status("a000", "RUNNING")

    # Assert
    warning_mock.assert_called_once()
    # If warning is logged, it returns early
    get_experiment_status_row_mock.assert_not_called()
    create_status_mock.assert_not_called()


def test_set_exp_status_logs_warning_when_get_experiment_row_by_expid_fails(
    tmp_path: "LocalPath", mocker
):
    """Test that set_exp_status() logs a warning when get_experiment_row_by_expid() fails."""
    db_dir = tmp_path / "db"
    db_dir.mkdir()
    local_root_dir = tmp_path / "local"
    local_root_dir.mkdir()

    database_manager = ExperimentStatusDbManager(
        expid="a000",
        db_dir_path=str(db_dir),
        main_db_name="test.db",
        local_root_dir_path=str(local_root_dir),
    )

    # Mock
    warning_mock = mocker.patch(
        "autosubmit.history.database_managers.experiment_status_db_manager.Log.warning"
    )
    mocker.patch.object(
        database_manager, "get_experiment_status_row_by_expid", return_value=None
    )
    mocker.patch.object(
        database_manager,
        "get_experiment_row_by_expid",
        side_effect=ValueError("missing experiment row"),
    )

    create_status_mock = mocker.patch.object(database_manager, "create_exp_status")

    # Act
    database_manager.set_exp_status("a000", "RUNNING")

    # Assert
    warning_mock.assert_called_once()
    create_status_mock.assert_not_called()


def test_update_heartbeat_stores_last_heartbeat(tmp_path: "LocalPath"):
    """Test that update_heartbeat() stores the last heartbeat timestamp in the database."""
    db_dir = tmp_path / "db"
    db_dir.mkdir()
    local_root_dir = tmp_path / "local"
    local_root_dir.mkdir()

    # SQLite database initialization
    autosubmit_db_path = db_dir / "test.db"
    with sqlite3.connect(autosubmit_db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE experiment (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT NOT NULL,
                autosubmit_version TEXT
            )
            """
        )
        conn.execute(
            "INSERT INTO experiment (id, name, description, autosubmit_version) VALUES (1, 'a000', 'No description', '3.14.0')",
        )
        conn.commit()

    database_manager = ExperimentStatusDbManager(
        expid="a000",
        db_dir_path=str(db_dir),
        main_db_name="test.db",
        local_root_dir_path=str(local_root_dir),
    )

    experiment = ExperimentRow(
        id=1, name="a000", description="No description", autosubmit_version="3.14.0"
    )
    database_manager.create_experiment_status_as_running(experiment)

    before = database_manager.get_experiment_status_row_by_exp_id(1)
    assert before is not None
    assert before.last_heartbeat is None

    database_manager.update_heartbeat("a000")
    after = database_manager.get_experiment_status_row_by_exp_id(1)
    assert after is not None
    assert after.last_heartbeat is not None
