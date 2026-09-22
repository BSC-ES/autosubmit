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

import pytest
from sqlalchemy import delete, inspect, select, text
from sqlalchemy.schema import CreateSchema

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database.tables import JobDataTable, get_table_with_schema
from autosubmit.history.data_classes.experiment_run import ExperimentRun
from autosubmit.history.data_classes.job_data import JobData
from autosubmit.history.database_managers.experiment_history_db_manager import (
    SqlAlchemyExperimentHistoryDbManager,
)
from test._oldschema import old_experiment_run_table, old_job_data_table


def _create_db_manager(as_db: str, **options) -> SqlAlchemyExperimentHistoryDbManager:
    """Create the SQLAlchemy history manager (``as_db`` is kept for readability)."""
    jobdata_dir_path = options.get("jobdata_dir_path", BasicConfig.JOBDATA_DIR)
    job_data_file = options.get("jobdata_file", None)
    return SqlAlchemyExperimentHistoryDbManager(options["expid"], jobdata_dir_path, job_data_file)


@pytest.mark.docker
@pytest.mark.postgres
def test_experiment_history_db_manager(tmp_path: Path, as_db: str):
    """Test the SQLAlchemy history database manager on both backends."""
    expid = "test_schema_history"
    tmp_test_dir = os.path.join(str(tmp_path), "test_experiment_history_db_manager")
    os.mkdir(tmp_test_dir)

    database_manager = _create_db_manager(as_db, expid=expid, jobdata_dir_path=tmp_test_dir)

    # Test initialization of the table
    database_manager.initialize()
    assert database_manager.my_database_exists()
    # The manager tracks a portable schema version and creates the job_data index.
    assert database_manager.is_current_version() is True
    assert database_manager.is_header_ready_db_version() is True
    inspector = inspect(database_manager.engine)
    index_names = {
        index['name'] for index in inspector.get_indexes(JobDataTable.name, schema=database_manager.schema)
    }
    # Postgres schema-qualifies index names (e.g. ``ix_<schema>_job_data_job_name``).
    assert any(name.endswith('job_data_job_name') for name in index_names)

    # The SQLite backend keeps the data in a local .db file; Postgres does not.
    db_file_path = Path(tmp_test_dir, f"job_data_{expid}.db")
    if as_db == "postgres":
        assert not db_file_path.exists()
    else:
        assert db_file_path.exists()

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


@pytest.mark.docker
@pytest.mark.postgres
def test_sqlalchemy_schema_version_is_isolated_per_tenant(as_db: str):
    """Each experiment records its own schema version."""
    first = _create_db_manager(as_db, expid="test_iso_first")
    second = _create_db_manager(as_db, expid="test_iso_second")
    assert isinstance(first, SqlAlchemyExperimentHistoryDbManager)
    assert isinstance(second, SqlAlchemyExperimentHistoryDbManager)
    first.initialize()
    second.initialize()

    with first.engine.begin() as conn:
        conn.execute(delete(first._version_table))

    assert first.is_current_version() is False
    assert second.is_current_version() is True


@pytest.mark.docker
@pytest.mark.postgres
def test_get_job_data_by_job_id_name(as_db: str, autosubmit_exp):
    exp = autosubmit_exp(experiment_data={})

    db_manager = _create_db_manager(
        as_db,
        expid=exp.expid,
        jobdata_dir_path=str(Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR, 'metadata', 'data'))
    )
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
    exp = autosubmit_exp(experiment_data={})

    db_manager = _create_db_manager(
        as_db,
        expid=exp.expid,
        jobdata_dir_path=str(Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR, 'metadata', 'data'))
    )
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
        [1, 0],
        [1, 1],
        [0, 0],
        [1, 0, 1],
    ],
)
@pytest.mark.docker
@pytest.mark.postgres
def test_get_all_last_job_data_dcs(as_db: str, lasts: list[bool], autosubmit_exp):
    """Persists the job data for the given optional job name, and its counters to verify the max counter."""
    exp = autosubmit_exp(experiment_data={})

    db_manager = _create_db_manager(
        as_db,
        expid=exp.expid,
        jobdata_dir_path=str(Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR, 'metadata', 'data'))
    )
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
    exp = autosubmit_exp(experiment_data={})

    db_manager = _create_db_manager(
        as_db,
        expid=exp.expid,
        jobdata_dir_path=str(Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR, 'metadata', 'data'))
    )
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


@pytest.mark.docker
@pytest.mark.postgres
def test_get_job_data_dc_unique_latest_by_job_name(as_db: str, autosubmit_exp):
    """Tests that we retrieve the expected number of entries (only when ``wrapper_code`` is greater than 2)."""
    exp = autosubmit_exp(experiment_data={})

    db_manager = _create_db_manager(
        as_db,
        expid=exp.expid,
        jobdata_dir_path=str(Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR, 'metadata', 'data'))
    )
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
    exp = autosubmit_exp(experiment_data={})

    db_manager = _create_db_manager(
        as_db,
        expid=exp.expid,
        jobdata_dir_path=str(Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR, 'metadata', 'data'))
    )
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
    exp = autosubmit_exp(experiment_data={})

    db_manager = _create_db_manager(
        as_db,
        expid=exp.expid,
        jobdata_dir_path=str(Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR, 'metadata', 'data'))
    )
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


@pytest.mark.docker
@pytest.mark.postgres
def test_sqlalchemy_initialize_migration_postgres(as_db):
    """Migration adds missing columns on PostgreSQL."""
    if as_db != 'postgres':
        pytest.skip("Only relevant for the PostgreSQL backend")

    expid = "test_migration_add_cols"
    options = {"expid": expid}

    db_manager = _create_db_manager(as_db, **options)
    assert isinstance(db_manager, SqlAlchemyExperimentHistoryDbManager)
    schema = db_manager.schema

    with db_manager.engine.connect() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        conn.execute(CreateSchema(schema, if_not_exists=True))
        conn.commit()

    old_job = get_table_with_schema(schema, old_job_data_table)
    old_exp = get_table_with_schema(schema, old_experiment_run_table)
    old_job.create(db_manager.engine)
    old_exp.create(db_manager.engine)

    inspector = inspect(db_manager.engine)
    cols = {c['name'] for c in inspector.get_columns("job_data", schema=schema)}
    assert "split" not in cols
    assert "splits" not in cols
    assert "fail_count" not in cols

    db_manager.initialize()

    inspector = inspect(db_manager.engine)
    cols = {c['name'] for c in inspector.get_columns("job_data", schema=schema)}
    assert "split" in cols
    assert "splits" in cols
    assert "fail_count" in cols
    assert "out" in cols
    assert "err" in cols

    with db_manager.engine.connect() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        conn.commit()


@pytest.mark.docker
@pytest.mark.postgres
def test_sqlalchemy_initialize_migration_twice_postgres(as_db):
    """Running initialize() twice on an old-schema database does not raise on PostgreSQL."""
    if as_db != 'postgres':
        pytest.skip("Only relevant for the PostgreSQL backend")

    expid = "test_migration_twice"
    options = {"expid": expid}

    db_manager = _create_db_manager(as_db, **options)
    assert isinstance(db_manager, SqlAlchemyExperimentHistoryDbManager)
    schema = db_manager.schema

    with db_manager.engine.connect() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        conn.execute(CreateSchema(schema, if_not_exists=True))
        conn.commit()

    old_job = get_table_with_schema(schema, old_job_data_table)
    old_exp = get_table_with_schema(schema, old_experiment_run_table)
    old_job.create(db_manager.engine)
    old_exp.create(db_manager.engine)

    db_manager.initialize()

    inspector = inspect(db_manager.engine)
    cols = {c['name'] for c in inspector.get_columns("job_data", schema=schema)}
    assert "split" in cols

    with db_manager.engine.connect() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        conn.commit()


@pytest.mark.docker
@pytest.mark.postgres
def test_sqlalchemy_initialize_migration_preserves_data_postgres(as_db):
    """Existing data survives migration on PostgreSQL."""
    if as_db != 'postgres':
        pytest.skip("Only relevant for the PostgreSQL backend")

    expid = "test_migration_data"
    options = {"expid": expid}

    db_manager = _create_db_manager(as_db, **options)
    assert isinstance(db_manager, SqlAlchemyExperimentHistoryDbManager)
    schema = db_manager.schema

    with db_manager.engine.connect() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        conn.execute(CreateSchema(schema, if_not_exists=True))
        conn.commit()

    old_job = get_table_with_schema(schema, old_job_data_table)
    old_exp = get_table_with_schema(schema, old_experiment_run_table)
    old_job.create(db_manager.engine)
    old_exp.create(db_manager.engine)

    with db_manager.engine.connect() as conn:
        conn.execute(
            old_exp.insert().values(
                run_id=1, created='now', modified='now', start=0,
                chunk_unit='month', chunk_size=1, completed=0, total=1,
                failed=0, queuing=0, running=0, submitted=0,
            )
        )
        conn.execute(
            old_job.insert().values(
                id=1, counter=1, job_name='test_job', created='now', modified='now',
                submit=0, start=0, finish=0, status='COMPLETED', rowtype=0,
                ncpus=0, wallclock='00:00', qos='debug', energy=0, date='20200101',
                section='SIM', member='fc0', chunk=1, last=1, platform='LOCAL',
                job_id=1, extra_data='{}', out='', err='',
            )
        )
        conn.commit()

    db_manager.initialize()

    job_data_table = get_table_with_schema(schema, old_job_data_table)
    with db_manager.engine.connect() as conn:
        result = conn.execute(
            select(job_data_table).where(job_data_table.c.job_name == "test_job")
        ).fetchall()
    assert len(result) == 1
    row = result[0]
    assert row.job_name == "test_job"
    assert row.last == 1

    with db_manager.engine.connect() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
        conn.commit()
