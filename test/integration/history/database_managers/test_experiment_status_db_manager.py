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


@pytest.mark.docker
@pytest.mark.postgres
def test_experiment_status_db_manager(tmp_path: 'LocalPath', as_db: str, get_next_expid):
    """Test that the experiment status DB manager can be created and performs basic operations (happy path)."""
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
    """Test that get_experiment_status_row_by_expid() retrieves the correct row for a given experiment ID, and raises an error if the experiment ID is not found."""
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
    assert experiment_status_row and experiment_status_row.status == "NOT RUNNING"


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


def test_update_heartbeat_stores_last_heartbeat(tmp_path: "LocalPath", mocker):
    """Test that update_heartbeat() stores the last heartbeat timestamp in the database."""
    db_dir = tmp_path / "db"
    db_dir.mkdir()
    local_root_dir = tmp_path / "local"
    local_root_dir.mkdir()

    # Ensure heartbeat timestamps are deterministic, ordering stable across diff runs
    timestamps = [
        "2026-05-08T10:00:00+00:00",
        "2026-05-08T10:00:01+00:00",
        "2026-05-08T10:00:02+00:00",
    ]
    mocker.patch(
        "autosubmit.history.database_managers.experiment_status_db_manager.HUtils.get_current_datetime",
        side_effect=timestamps,
    )

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
    # Act
    database_manager.create_experiment_status_as_running(experiment)
    before = database_manager.get_experiment_status_row_by_exp_id(1)
    # Assert
    assert before is not None
    assert before.last_heartbeat == timestamps[1]

    # Act
    database_manager.update_heartbeat("a000")
    after = database_manager.get_experiment_status_row_by_exp_id(1)
    # Assert
    assert after is not None
    assert after.last_heartbeat == timestamps[2]


@pytest.mark.parametrize(
    "exp_count,update_expids",
    [
        (1, ["a000", "a000"]),  # same experiment, concurrent updates
        (2, ["a000", "a001"]),  # different experiments, concurrent updates
    ],
    ids=["same_experiment", "different_experiments"],
)
def test_concurrent_heartbeat_updates(tmp_path: "LocalPath", mocker, exp_count, update_expids):
    """Test that concurrent heartbeat updates do not cause race conditions."""
    db_dir = tmp_path / "db"
    db_dir.mkdir()
    local_root_dir = tmp_path / "local"
    local_root_dir.mkdir()

    # Initialize SQLite database
    autosubmit_db_path = db_dir / "test.db"
    with sqlite3.connect(autosubmit_db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE experiment (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT NOT NULL,
                autosubmit_version TEXT
            )
            """)
        cursor.executemany(
            "INSERT INTO experiment (id, name, description, autosubmit_version) VALUES (?, ?, ?, ?)",
            [(i + 1, f"a00{i}", "No description", "3.14.0") for i in range(exp_count)],
        )
        conn.commit()

    database_manager = ExperimentStatusDbManager(
        expid="a000",
        db_dir_path=str(db_dir),
        main_db_name="test.db",
        local_root_dir_path=str(local_root_dir),
    )

    # Create status rows for experiments
    for i in range(exp_count):
        experiment = ExperimentRow(
            id=i + 1, name=f"a00{i}", description="No description", autosubmit_version="3.14.0"
        )
        database_manager.create_experiment_status_as_running(experiment)

    # Mock update_heartbeat to synchronize concurrent calls
    original_update_heartbeat = database_manager.update_heartbeat
    import threading
    start_barrier = threading.Barrier(2)

    def delayed_update_heartbeat(expid: str):
        start_barrier.wait()
        original_update_heartbeat(expid)

    mocker.patch.object(
        database_manager, "update_heartbeat", side_effect=delayed_update_heartbeat
    )

    # Execute concurrent updates
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=2) as executor:
        thread1 = executor.submit(database_manager.update_heartbeat, update_expids[0])
        thread2 = executor.submit(database_manager.update_heartbeat, update_expids[1])
        thread1.result()
        thread2.result()

    # Verify all experiments have heartbeats
    for i in range(exp_count):
        exp_status = database_manager.get_experiment_status_row_by_exp_id(i + 1)
        assert exp_status is not None
        assert exp_status.last_heartbeat is not None


def test_update_exp_status_updates_last_heartbeat_only_when_running(tmp_path: "LocalPath", mocker):
    """Test that RUNNING status creation and updates store a last_heartbeat value."""

    db_dir = tmp_path / "db"
    db_dir.mkdir()
    local_root_dir = tmp_path / "local"
    local_root_dir.mkdir()

    # Make it deterministic, ensure ordering is stable across diff runs
    timestamps = [
        "2026-05-08T10:00:00+00:00",
        "2026-05-08T10:00:01+00:00",
        "2026-05-08T10:00:02+00:00",
        "2026-05-08T10:00:03+00:00",
    ]
    mocker.patch(
        "autosubmit.history.database_managers.experiment_status_db_manager.HUtils.get_current_datetime",
        side_effect=timestamps,
    )

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
        cursor.execute(
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

    # Get initial status (RUNNING with last_heartbeat set)
    exp_status = database_manager.get_experiment_status_row_by_exp_id(1)
    assert exp_status is not None
    initial_heartbeat = exp_status.last_heartbeat
    assert initial_heartbeat == timestamps[1]

    # Update status to NOT RUNNING. Should not update last_heartbeat
    database_manager.set_exp_status("a000", "NOT RUNNING")

    after_not_running = database_manager.get_experiment_status_row_by_exp_id(1)
    assert after_not_running is not None
    assert after_not_running.status == "NOT RUNNING"
    assert after_not_running.last_heartbeat == initial_heartbeat

    # Update again with RUNNING status. Should update last_heartbeat
    database_manager.set_exp_status("a000", "RUNNING")

    after_running = database_manager.get_experiment_status_row_by_exp_id(1)
    assert after_running is not None
    assert after_running.status == "RUNNING"
    assert after_running.last_heartbeat == timestamps[3]


def test_set_exp_status_creates_running_with_heartbeat(tmp_path: "LocalPath", mocker):
    """Test that set_exp_status creates a new RUNNING status with heartbeat when status doesn't exist."""
    db_dir = tmp_path / "db"
    db_dir.mkdir()
    local_root_dir = tmp_path / "local"
    local_root_dir.mkdir()
    
    # Make it deterministic, ensure ordering is stable across diff runs
    timestamps = [
        "2026-05-08T10:00:00+00:00",  # create_exp_status (modified)
        "2026-05-08T10:00:01+00:00",  # update_heartbeat (called by set_exp_status)
    ]
    mocker.patch(
        "autosubmit.history.database_managers.experiment_status_db_manager.HUtils.get_current_datetime",
        side_effect=timestamps,
    )

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
        cursor.execute(
            "INSERT INTO experiment (id, name, description, autosubmit_version) VALUES (1, 'a000', 'No description', '3.14.0')",
        )
        conn.commit()

    database_manager = ExperimentStatusDbManager(
        expid="a000",
        db_dir_path=str(db_dir),
        main_db_name="test.db",
        local_root_dir_path=str(local_root_dir),
    )

    # Verify no status row exists
    initial_status = database_manager.get_experiment_status_row_by_exp_id(1)
    assert initial_status is None

    # Act
    database_manager.set_exp_status("a000", "RUNNING")

    # Assert
    final_status = database_manager.get_experiment_status_row_by_exp_id(1)
    assert final_status is not None
    assert final_status.status == "RUNNING"
    assert final_status.last_heartbeat == timestamps[1]
