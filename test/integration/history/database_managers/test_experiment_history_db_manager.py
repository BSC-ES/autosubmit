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
import time
from pathlib import Path
from typing import cast, TYPE_CHECKING

import pytest

from autosubmit.history.data_classes.experiment_run import ExperimentRun
from autosubmit.history.data_classes.job_data import JobData
from autosubmit.history.database_managers import experiment_history_db_manager
from autosubmit.history.database_managers.experiment_history_db_manager import create_experiment_history_db_manager

if TYPE_CHECKING:
    from autosubmit.history.database_managers.experiment_history_db_manager import (
        ExperimentHistoryDatabaseManager, ExperimentHistoryDbManager
    )
    # noinspection PyProtectedMember
    from py._path.local import LocalPath  # type: ignore

_EXPID = 't0123'


@pytest.mark.docker
@pytest.mark.postgres
def test_experiment_history_db_manager(tmp_path: Path, as_db: str):
    """Test history database manager using the old (SQLite) and new (SQLAlchemy) implementations."""
    _EXPID = "test_schema_history"
    options = {"expid": _EXPID}
    is_sqlalchemy = as_db != "sqlite"
    tmp_test_dir = os.path.join(str(tmp_path), "test_experiment_history_db_manager")
    os.mkdir(tmp_test_dir)
    if not is_sqlalchemy:
        # N.B.: We do it here, as we don't know the temporary path name until the fixture exists,
        #       and because it's harmless to the Postgres test to have the tmp_path fixture.
        options["jobdata_dir_path"] = str(tmp_test_dir)

    # Assert type of database manager
    database_manager: 'ExperimentHistoryDatabaseManager' = create_experiment_history_db_manager(as_db, **options)

    # Test initialization of the table
    # assert not database_manager.my_database_exists()
    database_manager.initialize()
    assert database_manager.my_database_exists()

    # Test that .db file was created or not depending on the database engine
    db_file_path = Path(tmp_test_dir, f"job_data_{options['expid']}.db")
    if is_sqlalchemy:
        assert not Path(db_file_path).exists()
    else:
        assert Path(db_file_path).exists()

    # Test experiment run history methods
    # Test run insertion
    assert database_manager.is_there_a_last_experiment_run() is False
    new_experiment_run = ExperimentRun(
        run_id=1,
        start=int(time.time()),
    )
    database_manager.register_experiment_run_dc(new_experiment_run)
    assert database_manager.is_there_a_last_experiment_run() is True

    # Test last run retrieval
    last_experiment_run = database_manager.get_experiment_run_dc_with_max_id()
    assert last_experiment_run.run_id == new_experiment_run.run_id
    assert last_experiment_run.start == new_experiment_run.start

    # Test run update
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


def test_sqlite_initialize_no_db(autosubmit_exp, mocker, tmp_path):
    exp = autosubmit_exp(_EXPID, experiment_data={})
    wrong_folder = tmp_path / 'wrong-folder'
    wrong_folder.mkdir()
    db_manager = create_experiment_history_db_manager(
        'sqlite',
        schema=exp.expid,
        expid=exp.expid,
        jobdata_dir_path=str(wrong_folder)
    )
    create_historical_database_spy = mocker.spy(db_manager, "create_historical_database")
    update_historical_database_spy = mocker.spy(db_manager, "update_historical_database")

    db_manager.initialize()

    assert create_historical_database_spy.called
    assert not update_historical_database_spy.called


def test_sqlite_initialize_wrong_version(autosubmit_exp, mocker, tmp_path, monkeypatch):
    exp = autosubmit_exp(_EXPID, experiment_data={})
    data_folder = Path(tmp_path, 'metadata/data')
    db_file = data_folder / f'job_data_{_EXPID}.db'
    Path(db_file).touch()
    db_manager = create_experiment_history_db_manager(
        'sqlite',
        schema=exp.expid,
        expid=exp.expid,
        jobdata_dir_path=str(data_folder)
    )
    create_historical_database_spy = mocker.spy(db_manager, "create_historical_database")
    update_historical_database_spy = mocker.spy(db_manager, "update_historical_database")

    db_manager.initialize()

    assert not create_historical_database_spy.called
    assert not update_historical_database_spy.called

    monkeypatch.setattr(experiment_history_db_manager, 'CURRENT_DB_VERSION', -99)
    db_manager.initialize()

    assert not create_historical_database_spy.called
    assert update_historical_database_spy.called


def test_sqlite_initialize_db_exists(autosubmit_exp, mocker, tmp_path):
    exp = autosubmit_exp(_EXPID, experiment_data={})
    data_folder = Path(tmp_path, 'metadata/data')
    db_file = data_folder / f'job_data_{_EXPID}.db'
    Path(db_file).touch()
    db_manager: 'ExperimentHistoryDbManager' = cast('ExperimentHistoryDbManager', create_experiment_history_db_manager(
        'sqlite',
        schema=exp.expid,
        expid=exp.expid,
        jobdata_dir_path=str(data_folder)
    ))
    create_historical_database_spy = mocker.spy(db_manager, "create_historical_database")
    update_historical_database_spy = mocker.spy(db_manager, "update_historical_database")

    db_manager.initialize()

    assert not create_historical_database_spy.called
    assert not update_historical_database_spy.called


def test_sqlite_is_current_version_db_exists(autosubmit_exp, tmp_path):
    exp = autosubmit_exp(_EXPID, experiment_data={})
    data_folder = Path(tmp_path, 'metadata/data')
    db_file = data_folder / f'job_data_{_EXPID}.db'
    Path(db_file).touch()
    db_manager: 'ExperimentHistoryDbManager' = cast('ExperimentHistoryDbManager', create_experiment_history_db_manager(
        'sqlite',
        schema=exp.expid,
        expid=exp.expid,
        jobdata_dir_path=str(data_folder)
    ))

    assert db_manager.is_current_version()


def test_sqlite_is_current_version_no_db(autosubmit_exp, tmp_path):
    exp = autosubmit_exp(_EXPID, experiment_data={})
    data_folder = Path(tmp_path, 'metadata/data')
    db_manager: 'ExperimentHistoryDbManager' = cast('ExperimentHistoryDbManager', create_experiment_history_db_manager(
        'sqlite',
        schema=exp.expid,
        expid=exp.expid,
        jobdata_dir_path=str(data_folder / 'wrong-folder')
    ))

    assert not db_manager.is_current_version()


def test_sqlite_is_header_ready_db_version_db_exists(autosubmit_exp, tmp_path):
    exp = autosubmit_exp(_EXPID, experiment_data={})
    data_folder = Path(tmp_path, 'metadata/data')
    db_file = data_folder
    Path(db_file).touch()
    db_manager: 'ExperimentHistoryDbManager' = cast('ExperimentHistoryDbManager', create_experiment_history_db_manager(
        'sqlite',
        schema=exp.expid,
        expid=exp.expid,
        jobdata_dir_path=str(data_folder)
    ))

    assert db_manager.is_header_ready_db_version()


def test_sqlite_is_header_ready_db_version_no_db(autosubmit_exp, tmp_path):
    exp = autosubmit_exp(_EXPID, experiment_data={})
    data_folder = Path(tmp_path, 'metadata/data')
    db_manager: 'ExperimentHistoryDbManager' = cast('ExperimentHistoryDbManager', create_experiment_history_db_manager(
        'sqlite',
        expid=exp.expid,
        jobdata_dir_path=str(data_folder / 'wrong-folder')
    ))

    assert not db_manager.is_header_ready_db_version()


@pytest.mark.docker
@pytest.mark.postgres
def test_get_job_data_by_job_id_name(as_db: str, autosubmit_exp):
    exp = autosubmit_exp(_EXPID, experiment_data={})

    db_manager: 'ExperimentHistoryDbManager' = cast('ExperimentHistoryDbManager', create_experiment_history_db_manager(
        as_db,
        schema=exp.expid,
        expid=exp.expid,
        jobdata_dir_path=str(Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR, 'metadata', 'data'))
    ))
    db_manager.initialize()

    new_job = JobData(
        _id=0,  # Doesn't matter on insertion
        job_name="test_job",
        rowtype=2,
    )
    db_manager.register_submitted_job_data_dc(new_job)

    retrieved_job = db_manager.get_job_data_by_job_id_name(new_job.job_id, new_job.job_name)

    assert new_job._id != retrieved_job._id
    assert retrieved_job._id > 0
    assert new_job.job_name == retrieved_job.job_name
    assert new_job.rowtype == retrieved_job.rowtype


@pytest.mark.parametrize(
    "job_name,counters",
    [
        ['test_job', [42]],
        ['test_job', [42, 13]],
        ['test_job', []],
        ['', []],
        ['', [1]]
    ],
)
@pytest.mark.docker
@pytest.mark.postgres
def test_get_job_data_max_counter(as_db: str, job_name: str, counters: list[int], autosubmit_exp):
    """Persists the job data for the given optional job name, and its counters to verify the max counter."""
    exp = autosubmit_exp(_EXPID, experiment_data={})

    db_manager: 'ExperimentHistoryDbManager' = cast('ExperimentHistoryDbManager', create_experiment_history_db_manager(
        as_db,
        schema=exp.expid,
        expid=exp.expid,
        jobdata_dir_path=str(Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR, 'metadata', 'data'))
    ))
    db_manager.initialize()

    sum_counters = max(counters) if counters else 0
    for counter in counters:
        new_job = JobData(
            _id=0,  # Doesn't matter on insertion
            job_name=job_name,
            rowtype=2,
            counter=counter
        )
        db_manager.register_submitted_job_data_dc(new_job)

    max_counter = db_manager.get_job_data_max_counter(job_name=job_name)

    assert max_counter == sum_counters


@pytest.mark.parametrize(
    "lasts",
    [
        [1, 0]
    ],
)
@pytest.mark.docker
@pytest.mark.postgres
def test_get_all_last_job_data_dcs(as_db: str, lasts: list[bool], request, autosubmit_exp):
    """Persists the job data for the given optional job name, and its counters to verify the max counter."""
    exp = autosubmit_exp(_EXPID, experiment_data={})

    db_manager: 'ExperimentHistoryDbManager' = cast('ExperimentHistoryDbManager', create_experiment_history_db_manager(
        as_db,
        schema=exp.expid,
        expid=exp.expid,
        jobdata_dir_path=str(Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR, 'metadata', 'data'))
    ))
    db_manager.initialize()

    last_rows = lasts.count(True)
    for i, last in enumerate(lasts):
        new_job = JobData(
            _id=0,  # Doesn't matter on insertion
            job_name=f'test_job_{i}',
            rowtype=2,
            last=last
        )
        db_manager.register_submitted_job_data_dc(new_job)

    last_job_data_dcs = db_manager.get_all_last_job_data_dcs()

    assert len(last_job_data_dcs) == last_rows


@pytest.mark.parametrize(
    "wrapper_code,number_of_expected",
    [
        [2, 0],
        [10, 1]
    ],
)
@pytest.mark.docker
@pytest.mark.postgres
def test_get_job_data_dcs_last_by_wrapper_code(as_db: str, wrapper_code: int, number_of_expected: int, autosubmit_exp):
    """Tests that we retrieve the expected number of entries (only when ``wrapper_code`` is greater than 2)."""
    exp = autosubmit_exp(_EXPID, experiment_data={})

    db_manager: 'ExperimentHistoryDbManager' = cast('ExperimentHistoryDbManager', create_experiment_history_db_manager(
        as_db,
        schema=exp.expid,
        expid=exp.expid,
        jobdata_dir_path=str(Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR, 'metadata', 'data'))
    ))
    db_manager.initialize()

    new_job = JobData(
        _id=0,  # Doesn't matter on insertion
        job_name=f'test_job_{wrapper_code}',
        rowtype=wrapper_code,
        last=1
    )
    db_manager.register_submitted_job_data_dc(new_job)

    job_data_dcs = db_manager.get_job_data_dcs_last_by_wrapper_code(wrapper_code)

    assert len(job_data_dcs) == number_of_expected


@pytest.mark.postgres
def test_get_job_data_dc_unique_latest_by_job_name(as_db: str, autosubmit_exp):
    """Tests that we retrieve the expected number of entries (only when ``wrapper_code`` is greater than 2)."""
    exp = autosubmit_exp(_EXPID, experiment_data={})

    db_manager: 'ExperimentHistoryDbManager' = cast('ExperimentHistoryDbManager', create_experiment_history_db_manager(
        as_db,
        schema=exp.expid,
        expid=exp.expid,
        jobdata_dir_path=str(Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR, 'metadata', 'data'))
    ))
    db_manager.initialize()

    job_name = 'test_job'

    assert not db_manager.get_job_data_dc_unique_latest_by_job_name(job_name)

    new_job = JobData(
        _id=0,  # Doesn't matter on insertion
        job_name=job_name,
        rowtype=2
    )
    db_manager.register_submitted_job_data_dc(new_job)

    assert db_manager.get_job_data_dc_unique_latest_by_job_name(job_name)


@pytest.mark.docker
@pytest.mark.postgres
def test_update_job_data_dc_by_job_id_name(as_db: str, autosubmit_exp):
    """Tests that we retrieve the expected number of entries (only when ``wrapper_code`` is greater than 2)."""
    exp = autosubmit_exp(_EXPID, experiment_data={})

    db_manager: 'ExperimentHistoryDbManager' = cast('ExperimentHistoryDbManager', create_experiment_history_db_manager(
        as_db,
        schema=exp.expid,
        expid=exp.expid,
        jobdata_dir_path=str(Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR, 'metadata', 'data'))
    ))
    db_manager.initialize()

    job_name = 'test_job'

    new_job = JobData(
        _id=0,  # Doesn't matter on insertion
        job_name=job_name,
        rowtype=2
    )
    db_manager.register_submitted_job_data_dc(new_job)

    retrieved = db_manager.get_job_data_by_job_id_name(new_job.job_id, new_job.job_name)
    assert retrieved.job_id == new_job.job_id
    retrieved.job_id = 1984

    db_manager.update_job_data_dc_by_job_id_name(retrieved)

    retrieved = db_manager.get_job_data_by_job_id_name(retrieved.job_id, retrieved.job_name)
    assert retrieved.job_id == 1984


@pytest.mark.docker
@pytest.mark.postgres
def test_update_list_job_data_dc_by_each_id(as_db: str, autosubmit_exp):
    """Tests that we retrieve the expected number of entries (only when ``wrapper_code`` is greater than 2)."""
    exp = autosubmit_exp(_EXPID, experiment_data={})

    db_manager: 'ExperimentHistoryDbManager' = cast('ExperimentHistoryDbManager', create_experiment_history_db_manager(
        as_db,
        schema=exp.expid,
        expid=exp.expid,
        jobdata_dir_path=str(Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR, 'metadata', 'data'))
    ))
    db_manager.initialize()

    jobs = [
        JobData(
            _id=i + 1,
            job_name=f'test_job_{i}',
            rowtype=2,
            status='1'
        )
        for i in range(3)
    ]

    for job in jobs:
        db_manager.register_submitted_job_data_dc(job)

    jobs_row_statuses = [j.status for j in jobs]
    retrieved_jobs = db_manager.get_job_data_all()
    retrieved_jobs_statuses = [j.status for j in retrieved_jobs]

    assert retrieved_jobs_statuses == jobs_row_statuses

    new_status = '10'

    for job in jobs:
        job.status = new_status

    db_manager.update_list_job_data_dc_by_each_id(jobs)

    retrieved_jobs = db_manager.get_job_data_all()
    retrieved_jobs_statuses = [j.status for j in retrieved_jobs]

    assert retrieved_jobs_statuses == [new_status, new_status, new_status]


def test_sqlite_pragma_version(autosubmit_exp, tmp_path: 'LocalPath'):
    exp = autosubmit_exp(_EXPID, experiment_data={})
    data_folder = Path(tmp_path, 'metadata/data')
    db_file = data_folder / f'job_data_{_EXPID}.db'
    Path(db_file).touch()
    db_manager = cast('ExperimentHistoryDbManager', create_experiment_history_db_manager(
        'sqlite',
        schema=exp.expid,
        expid=exp.expid,
        jobdata_dir_path=str(data_folder)
    ))

    Path(db_file).unlink()
    Path(db_file).touch()

    db_manager.execute_statement_on_dbfile(db_manager.historicaldb_file_path, db_manager.create_table_header_query)
    db_manager.execute_statement_on_dbfile(db_manager.historicaldb_file_path, db_manager.create_table_query)
    db_manager.execute_statement_on_dbfile(db_manager.historicaldb_file_path, db_manager.create_index_query)
    # Skipped _set_historical_pragma_version

    with pytest.raises(Exception) as cm:
        db_manager.is_header_ready_db_version()

    assert 'pragma version' in str(cm.value)
