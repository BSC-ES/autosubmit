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

import sqlite3
import sqlalchemy.exc
from pathlib import Path
from typing import cast, TYPE_CHECKING

import pytest
from sqlalchemy import create_engine, inspect, text

from autosubmit.config.basicconfig import BasicConfig
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
    
def _create_experiment_status_db_manager_and_rows(
    as_db: str,
    tmp_path: Path,
    expids: list[str],
    autosubmit_exp=None,
):
    """Create a status manager and the experiment rows for sqlite or postgres."""
    options = {"expid": expids[0]}

    if as_db == "sqlite":
        db_dir = tmp_path / "db"
        db_dir.mkdir()
        local_root_dir = tmp_path / "local"
        local_root_dir.mkdir()

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
            cursor.executemany(
                "INSERT INTO experiment (id, name, description, autosubmit_version) VALUES (?, ?, ?, ?)",
                [(index + 1, expid, "test", "4.1.10") for index, expid in enumerate(expids)],
            )
            conn.commit()

        options["db_dir_path"] = str(db_dir)
        options["local_root_dir_path"] = str(local_root_dir)
        options["main_db_name"] = "test.db"
    else:
        if autosubmit_exp is None:
            raise ValueError("autosubmit_exp is required when using postgres")

        for expid in expids:
            autosubmit_exp(expid=expid, include_jobs=True)

    database_manager = create_experiment_status_db_manager(as_db, **options)

    if as_db == "postgres":
        with database_manager.engine.begin() as conn:
            for expid in expids:
                conn.execute(text("DELETE FROM experiment_status WHERE name = :name"), {"name": expid})

    experiment_rows = [database_manager.get_experiment_row_by_expid(expid) for expid in expids]
    return database_manager, experiment_rows


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


@pytest.mark.docker
@pytest.mark.postgres
def test_experiment_status_name_is_unique(tmp_path: "LocalPath", as_db: str, autosubmit_exp, get_next_expid):
    """Test that experiment_status.name is enforced as unique."""
    database_manager, experiments = _create_experiment_status_db_manager_and_rows(
        as_db=as_db,
        tmp_path=tmp_path,
        expids=["a000"],
        autosubmit_exp=autosubmit_exp,
    )

    experiment = experiments[0]

    database_manager.create_exp_status(
        experiment.id,
        experiment.name,
        "RUNNING",
    )

    if as_db == "sqlite":
        with pytest.raises(sqlite3.IntegrityError) as exc_info:
            database_manager.create_exp_status(
                experiment.id,
                experiment.name,
                "RUNNING",
            )
        assert "UNIQUE constraint failed: experiment_status.exp_id" in str(exc_info.value)
    else:
        with pytest.raises(sqlalchemy.exc.IntegrityError) as exc_info:
            database_manager.create_exp_status(
                experiment.id,
                experiment.name,
                "RUNNING",
            )
        assert "duplicate key value violates unique constraint" in str(exc_info.value)


@pytest.mark.docker
@pytest.mark.postgres
def test_experiment_status_db_manager_adds_last_heartbeat_column_if_missing(
    tmp_path: "LocalPath", as_db: str
):
    """Test that the manager adds last_heartbeat column into experiment_status table if it is missing."""
    # SQLite
    if as_db == "sqlite":
        db_dir = tmp_path / "db"
        db_dir.mkdir()
        local_root_dir = tmp_path / "local"
        local_root_dir.mkdir()

        # Create the database and experiment_status table without last_heartbeat column
        as_times_db_path = db_dir / "as_times.db"
        with sqlite3.connect(as_times_db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE experiment_status (
                    exp_id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    seconds_diff INTEGER NOT NULL,
                    modified TEXT NOT NULL
                )
                """)
            conn.commit()

        # Initialize the manager
        database_manager = ExperimentStatusDbManager(
            expid="a000",
            db_dir_path=str(db_dir),
            main_db_name="as_times.db",
            local_root_dir_path=str(local_root_dir),
        )

        # Get the columns of the experiment_status table for later verification
        with sqlite3.connect(as_times_db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(experiment_status)")
            columns = [row[1] for row in cursor.fetchall()]

    # Postgres
    else:
        with create_engine(BasicConfig.DATABASE_CONN_URL).begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS experiment_status"))
            conn.execute(text("""
                    CREATE TABLE experiment_status (
                        exp_id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        status TEXT NOT NULL,
                        seconds_diff INTEGER NOT NULL,
                        modified TEXT NOT NULL
                    )
                    """))
        database_manager = create_experiment_status_db_manager("postgres")
        inspector = inspect(database_manager.engine)
        columns = [
            col["name"]
            for col in inspector.get_columns(
                ExperimentStatusTable.name, schema="public"
            )
        ]

    assert "last_heartbeat" in columns
    if as_db == "sqlite":
        assert isinstance(database_manager, ExperimentStatusDbManager)
    else:
        assert isinstance(database_manager, SqlAlchemyExperimentStatusDbManager)


@pytest.mark.docker
@pytest.mark.postgres
def test_update_heartbeat_stores_last_heartbeat(tmp_path: "LocalPath", as_db: str, autosubmit_exp, mocker):
    """Test that update_heartbeat() stores the last heartbeat timestamp in the database."""
    database_manager, experiments = _create_experiment_status_db_manager_and_rows(
        as_db=as_db,
        tmp_path=tmp_path,
        expids=["a000"],
        autosubmit_exp=autosubmit_exp,
    )

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

    experiment = experiments[0]
    exp_id = experiment.id
    # Act
    database_manager.create_experiment_status_as_running(experiment)
    before = database_manager.get_experiment_status_row_by_exp_id(exp_id)
    # Assert
    assert before is not None
    assert before.last_heartbeat == timestamps[1]

    # Act
    database_manager.update_heartbeat("a000")
    after = database_manager.get_experiment_status_row_by_exp_id(exp_id)
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
@pytest.mark.docker
@pytest.mark.postgres
def test_concurrent_heartbeat_updates(tmp_path: "LocalPath", as_db: str, autosubmit_exp, mocker, exp_count, update_expids):
    """Test that concurrent heartbeat updates do not cause race conditions."""
    unique_expids = list(dict.fromkeys(update_expids))
    database_manager, experiments = _create_experiment_status_db_manager_and_rows(
        as_db=as_db,
        tmp_path=tmp_path,
        expids=unique_expids,
        autosubmit_exp=autosubmit_exp,
    )

    # Create status rows for experiments
    for experiment in experiments:
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


@pytest.mark.docker
@pytest.mark.postgres
def test_update_exp_status_updates_last_heartbeat_only_when_running(
    tmp_path: "LocalPath", as_db: str, autosubmit_exp, mocker
):
    """Test that RUNNING status creation and updates store a last_heartbeat value."""
    database_manager, experiments = _create_experiment_status_db_manager_and_rows(
        as_db=as_db,
        tmp_path=tmp_path,
        expids=["a000"],
        autosubmit_exp=autosubmit_exp,
    )

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

    experiment = experiments[0]
    exp_id = experiment.id
    
    database_manager.create_experiment_status_as_running(experiment)

    # Get initial status (RUNNING with last_heartbeat set)
    exp_status = database_manager.get_experiment_status_row_by_exp_id(exp_id)
    assert exp_status is not None
    initial_heartbeat = exp_status.last_heartbeat
    assert initial_heartbeat == timestamps[1]

    # Update status to NOT RUNNING. Should not update last_heartbeat
    database_manager.update_exp_status("a000", "NOT RUNNING")

    after_not_running = database_manager.get_experiment_status_row_by_exp_id(exp_id)
    assert after_not_running is not None
    assert after_not_running.status == "NOT RUNNING"
    assert after_not_running.last_heartbeat == initial_heartbeat

    # Update again with RUNNING status. Should update last_heartbeat
    database_manager.update_exp_status("a000", "RUNNING")

    after_running = database_manager.get_experiment_status_row_by_exp_id(exp_id)
    assert after_running is not None
    assert after_running.status == "RUNNING"
    assert after_running.last_heartbeat == timestamps[3]


@pytest.mark.docker
@pytest.mark.postgres
def test_set_exp_status_creates_running_with_heartbeat(
    tmp_path: "LocalPath", as_db: str, autosubmit_exp, mocker
):
    """Test that set_exp_status creates a new RUNNING status with heartbeat when status doesn't exist."""
    database_manager, _ = _create_experiment_status_db_manager_and_rows(
        as_db=as_db,
        tmp_path=tmp_path,
        expids=["a000"],
        autosubmit_exp=autosubmit_exp,
    )

    # Make it deterministic, ensure ordering is stable across diff runs
    timestamps = [
        "2026-05-08T10:00:00+00:00",  # create_exp_status (modified)
        "2026-05-08T10:00:01+00:00",  # update_heartbeat (called by set_exp_status)
    ]
    mocker.patch(
        "autosubmit.history.database_managers.experiment_status_db_manager.HUtils.get_current_datetime",
        side_effect=timestamps,
    )

    experiment = database_manager.get_experiment_row_by_expid("a000")
    exp_id = experiment.id

    # Verify no status row exists
    initial_status = database_manager.get_experiment_status_row_by_exp_id(exp_id)
    assert initial_status is None

    # Act
    database_manager.set_exp_status("a000", "RUNNING")

    # Assert
    final_status = database_manager.get_experiment_status_row_by_exp_id(exp_id)
    assert final_status is not None
    assert final_status.status == "RUNNING"
    assert final_status.last_heartbeat == timestamps[1]


@pytest.mark.docker
@pytest.mark.postgres
def test_set_exp_status_logs_warning(
    tmp_path: "LocalPath", as_db: str, mocker
):
    """Test lookup failure behavior: direct lookup raises, status setter logs warning."""
    options = {"expid": "a000"}

    if as_db == "sqlite":
        options["db_dir_path"] = tmp_path
        options["local_root_dir_path"] = tmp_path
        options["main_db_name"] = "tests.db"

    database_manager = create_experiment_status_db_manager(as_db, **options)

    warning_mock = mocker.patch(
        "autosubmit.history.database_managers.experiment_status_db_manager.Log.warning"
    )

    # Calling directly get_exp_row_by_expid raises a ValueError
    with pytest.raises(ValueError):
        database_manager.get_experiment_row_by_expid("a000")

    # set_exp_status catches the ValueError and logs a warning
    database_manager.set_exp_status("a000", "RUNNING")

    warning_mock.assert_called_once()
    assert "Experiment a000 not found when trying to set status" in warning_mock.call_args[0][0]
