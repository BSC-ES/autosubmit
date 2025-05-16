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

"""Integration tests for the experiment history DB managers."""

import os
import pytest
import time
from pathlib import Path
from typing import Type

from autosubmit.history.data_classes.experiment_run import ExperimentRun
from autosubmit.history.data_classes.job_data import JobData
from autosubmit.history.database_managers.experiment_history_db_manager import (
    SqlAlchemyExperimentHistoryDbManager,
    ExperimentHistoryDbManager,
    create_experiment_history_db_manager,
)


@pytest.mark.parametrize(
    "db_engine,options,clazz",
    [
        # postgres
        pytest.param(
            "postgres",
            {"schema": "test_schema_history"},
            SqlAlchemyExperimentHistoryDbManager,
            marks=[pytest.mark.postgres],
        ),
        # sqlite
        (
            "sqlite",
            {"schema": "test_schema_history"},
            ExperimentHistoryDbManager,
        ),
    ],
)
def test_experiment_history_db_manager(
    tmp_path: Path,
    db_engine: str,
    options: dict,
    clazz: Type,
    request: pytest.FixtureRequest,
):
    """
    Test history database manager using the old (SQLite) and new (SQLAlchemy) implementations.
    """
    is_sqlalchemy = db_engine != "sqlite"
    tmp_test_dir = os.path.join(str(tmp_path), "test_experiment_history_db_manager")
    os.mkdir(tmp_test_dir)
    if not is_sqlalchemy:
        # N.B.: We do it here, as we don't know the temporary path name until the fixture exists,
        #       and because it's harmless to the Postgres test to have the tmp_path fixture.
        options["jobdata_dir_path"] = str(tmp_test_dir)

    # Dynamically load the fixture for that DB
    request.getfixturevalue(f"as_db_{db_engine}")

    # Assert type of database manager
    database_manager = create_experiment_history_db_manager(db_engine, **options)
    assert isinstance(database_manager, clazz)

    # Test initialization of the table
    # assert not database_manager.my_database_exists()
    database_manager.initialize()
    assert database_manager.my_database_exists()

    # Test that .db file was created or not depending on the database engine
    db_file_path = os.path.join(
        str(tmp_test_dir), "job_data_{0}.db".format(options["schema"])
    )
    if is_sqlalchemy:
        assert not os.path.exists(db_file_path)
    else:
        assert os.path.exists(db_file_path)

    # Test experiment run history methods
    ## Test run insertion
    assert database_manager.is_there_a_last_experiment_run() is False
    new_experiment_run = ExperimentRun(
        run_id=1,
        start=int(time.time()),
    )
    database_manager.register_experiment_run_dc(new_experiment_run)
    assert database_manager.is_there_a_last_experiment_run() is True

    ## Test last run retrieval
    last_experiment_run = database_manager.get_experiment_run_dc_with_max_id()
    assert last_experiment_run.run_id == new_experiment_run.run_id
    assert last_experiment_run.start == new_experiment_run.start

    ## Test run update
    new_experiment_run.finish = int(time.time())
    new_experiment_run.total = 1
    new_experiment_run.completed = 1
    database_manager.update_experiment_run_dc_by_id(new_experiment_run)

    last_experiment_run = database_manager.get_experiment_run_dc_with_max_id()
    assert last_experiment_run.run_id == new_experiment_run.run_id
    assert last_experiment_run.start == new_experiment_run.start
    assert last_experiment_run.finish == new_experiment_run.finish
    assert last_experiment_run.total == new_experiment_run.total
    assert last_experiment_run.completed == new_experiment_run.completed

    # Test job history methods
    # Test job insertion
    new_job = JobData(
        _id=0,  # Doesn't matter on insertion
        job_name="test_job",
        rowtype=2,
    )

    for i in range(10):
        new_job.run_id = i + 1
        new_job.counter = i
        submitted_job: JobData = database_manager.register_submitted_job_data_dc(
            new_job
        )
        assert submitted_job.run_id == new_job.run_id
        assert submitted_job.counter == new_job.counter
        assert submitted_job.job_name == new_job.job_name
        assert submitted_job.rowtype == new_job.rowtype
        assert submitted_job.last == 1

        all_jobs = database_manager.get_job_data_all()
        assert len(all_jobs) == i + 1
        count_lasts = 0
        for curr_job in all_jobs:
            count_lasts += curr_job.last
        assert count_lasts == 1

    # Test many job update
    all_jobs = database_manager.get_job_data_all()
    changes = []
    for i, curr_job in enumerate(all_jobs):
        changes.append(["2024-01-01-00:00:00", "COMPLETED", i, curr_job.id])

    database_manager.update_many_job_data_change_status(changes)

    all_jobs = database_manager.get_job_data_all()
    for i, curr_job in enumerate(all_jobs):
        assert curr_job.modified == "2024-01-01-00:00:00"
        assert curr_job.status == "COMPLETED"
        assert curr_job.rowstatus == i
