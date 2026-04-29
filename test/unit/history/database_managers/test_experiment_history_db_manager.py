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

import pytest
from sqlalchemy import and_, insert, select

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database.tables import JobDataTable
from autosubmit.history.utils import get_current_datetime
from autosubmit.history.database_managers.experiment_history_db_manager import (
    DEFAULT_MAX_COUNTER,
    create_experiment_history_db_manager,
    ExperimentHistoryDbManager,
    SqlAlchemyExperimentHistoryDbManager,
)


def test_create_experiment_history_db_manager_invalid():
    with pytest.raises(ValueError):
        create_experiment_history_db_manager('banana')


def test_functions_not_implemented(mocker):
    """Confirm that we do not implement a few functions for Postgres."""
    mocker.patch('autosubmit.history.database_managers.experiment_history_db_manager.get_connection_url')
    mocker.patch('autosubmit.history.database_managers.experiment_history_db_manager.session')
    db_manager = SqlAlchemyExperimentHistoryDbManager(None, BasicConfig.JOBDATA_DIR)
    # NOTE: These are all parameter-less.
    for fn in [
        'is_header_ready_db_version',
        'is_current_version',
        'update_historical_database'
    ]:
        with pytest.raises(NotImplementedError):
            getattr(db_manager, fn)()


def test_select_jobs_data_regression_sqlite_variable_limit(tmp_path, monkeypatch):
    """Regression test: old IN-clause crashes above SQLite's variable-number limit.
        The default limit varies by Python version and SQlite build, it is often 999 or 32766
    """
    monkeypatch.setattr(BasicConfig, 'DATABASE_BACKEND', 'sqlite')

    with sqlite3.connect(":memory:") as raw:
        # Python 3.11+ exposes getlimit() default seems to be 250000; older builds default to unknown number (32766 fails in the CI/CD)
        sqlite_limit = raw.getlimit(9) if hasattr(raw, 'getlimit') else 250000
    n_jobs = sqlite_limit + 1

    db_manager = SqlAlchemyExperimentHistoryDbManager(
        schema="test_limit",
        jobdata_path=str(tmp_path),
        jobdata_file="job_data_test_limit.db",
    )
    db_manager.initialize()

    job_names = [f"test_limit_20200101_fc0_{i}_SIM" for i in range(1, n_jobs + 1)]
    job_data_table = db_manager.table_registry.get(JobDataTable.name)
    now = get_current_datetime()

    rows = [
        {
            "counter": 1, "job_name": name, "created": now, "modified": now,
            "submit": 0, "start": 0, "finish": 0, "status": "COMPLETED",
            "rowtype": 0, "ncpus": 0, "wallclock": "00:00", "qos": "debug",
            "energy": 0, "date": "20200101", "section": "SIM", "member": "fc0",
            "chunk": idx + 1, "last": 1, "platform": "LOCAL", "job_id": idx + 1,
            "extra_data": "{}", "nnodes": 0, "run_id": 1,
            "MaxRSS": 0.0, "AveRSS": 0.0, "out": "", "err": "",
            "rowstatus": 0, "children": None, "platform_output": None,
        }
        for idx, name in enumerate(job_names)
    ]

    with db_manager.engine.connect() as conn:
        conn.execute(insert(job_data_table), rows)
        conn.commit()

    # Old approach: IN-clause with N bound parameters must fail above the limit.
    old_query = select(job_data_table).where(
        and_(
            job_data_table.c.last == 1,
            job_data_table.c.job_name.in_(job_names),
        )
    )
    with db_manager.engine.connect() as conn:
        with pytest.raises(Exception, match="too many SQL variables|SQLITE_MAX_VARIABLE_NUMBER"):
            conn.execute(old_query).fetchall()

    # New approach
    result = db_manager.select_jobs_data(job_data_table, job_names)

    assert len(result) == n_jobs
    assert all(dict(row)["last"] == 1 for row in result)


def _base_row(job_name: str, counter: int, job_id: int, status: str = "COMPLETED") -> dict:
    """Return a minimal job_data row dict for use in both SQLite and SQLAlchemy tests."""
    now = get_current_datetime()
    return {
        "counter": counter,
        "job_name": job_name,
        "created": now,
        "modified": now,
        "submit": 0,
        "start": 0,
        "finish": 0,
        "status": status,
        "rowtype": 0,
        "ncpus": 1,
        "wallclock": "00:30",
        "qos": "debug",
        "energy": 0,
        "date": "20200101",
        "section": "SIM",
        "member": "fc0",
        "chunk": 1,
        "last": 1,
        "platform": "LOCAL",
        "job_id": job_id,
        "extra_data": "{}",
        "nnodes": 0,
        "run_id": 1,
        "MaxRSS": 0.0,
        "AveRSS": 0.0,
        "out": "",
        "err": "",
        "rowstatus": 0,
        "children": None,
        "platform_output": None,
    }


@pytest.fixture()
def sqlalchemy_db_manager(tmp_path, monkeypatch):
    """Return an initialised SqlAlchemyExperimentHistoryDbManager backed by SQLite."""
    monkeypatch.setattr(BasicConfig, "DATABASE_BACKEND", "sqlite")
    db_manager = SqlAlchemyExperimentHistoryDbManager(
        schema="t001",
        jobdata_path=str(tmp_path),
        jobdata_file="job_data_t001.db",
    )
    db_manager.initialize()
    return db_manager


def test_sqlalchemy_get_last_job_data_dc_returns_single_row(sqlalchemy_db_manager):
    """Return exactly one JobData when exactly one row matches."""
    job_data_table = sqlalchemy_db_manager.table_registry.get(JobDataTable.name)
    row = _base_row("t001_20200101_fc0_1_SIM", counter=1, job_id=10)
    with sqlalchemy_db_manager.engine.connect() as conn:
        conn.execute(insert(job_data_table), [row])
        conn.commit()

    result = sqlalchemy_db_manager.get_last_job_data_dc_by_job_name_and_counter(
        "t001_20200101_fc0_1_SIM", 1
    )

    assert result.job_name == "t001_20200101_fc0_1_SIM"
    assert result.counter == 1


def test_sqlalchemy_get_last_job_data_dc_returns_correct_row_among_multiple_counters(sqlalchemy_db_manager):
    """Return the row matching the requested counter when the job has multiple counter entries."""
    job_data_table = sqlalchemy_db_manager.table_registry.get(JobDataTable.name)
    job_name = "t001_20200101_fc0_3_SIM"
    rows = [
        {**_base_row(job_name, counter=1, job_id=20), "status": "FAILED"},
        {**_base_row(job_name, counter=2, job_id=22), "status": "COMPLETED"},
        {**_base_row(job_name, counter=3, job_id=23), "status": "RUNNING"},
    ]
    with sqlalchemy_db_manager.engine.connect() as conn:
        conn.execute(insert(job_data_table), rows)
        conn.commit()

    result = sqlalchemy_db_manager.get_last_job_data_dc_by_job_name_and_counter(job_name, 2)

    assert result.counter == 2
    assert result.status == "COMPLETED"
    assert result.job_id == 22


def test_sqlalchemy_get_last_job_data_dc_raises_when_not_found(sqlalchemy_db_manager):
    """Raise an exception when no row matches job_name and counter."""
    with pytest.raises(Exception, match="No job_data found"):
        sqlalchemy_db_manager.get_last_job_data_dc_by_job_name_and_counter(
            "nonexistent_job", 99
        )


@pytest.mark.parametrize("counter", [1, 2, 3])
def test_sqlalchemy_get_last_job_data_dc_only_matching_counter_is_returned(sqlalchemy_db_manager, counter):
    """Return only the row matching the requested counter value."""
    job_data_table = sqlalchemy_db_manager.table_registry.get(JobDataTable.name)
    job_name = f"t001_20200101_fc0_{counter}_SIM_counter_test"
    rows = [_base_row(job_name, counter=c, job_id=100 + c) for c in range(1, 4)]
    with sqlalchemy_db_manager.engine.connect() as conn:
        conn.execute(insert(job_data_table), rows)
        conn.commit()

    result = sqlalchemy_db_manager.get_last_job_data_dc_by_job_name_and_counter(
        job_name, counter
    )

    assert result.counter == counter


@pytest.fixture()
def sqlite_db_manager(tmp_path):
    """Return an initialised ExperimentHistoryDbManager backed by a temp SQLite file."""
    db_manager = ExperimentHistoryDbManager(
        expid="t002",
        jobdata_dir_path=str(tmp_path),
    )
    db_manager.create_historical_database()
    return db_manager


def _insert_sqlite_row(db_manager: ExperimentHistoryDbManager, row: dict) -> None:
    """Insert a single row dict into job_data via raw SQLite for test setup."""
    import sqlite3 as _sqlite3
    conn = _sqlite3.connect(db_manager.historicaldb_file_path)
    cols = ", ".join(row.keys())
    placeholders = ", ".join(["?"] * len(row))
    conn.execute(
        f"INSERT INTO job_data ({cols}) VALUES ({placeholders})",
        list(row.values()),
    )
    conn.commit()
    conn.close()


def test_sqlite_get_last_job_data_dc_returns_single_row(sqlite_db_manager):
    """Return exactly one JobData when exactly one row matches."""
    row = _base_row("t002_20200101_fc0_1_SIM", counter=1, job_id=10)
    _insert_sqlite_row(sqlite_db_manager, row)

    result = sqlite_db_manager.get_last_job_data_dc_by_job_name_and_counter(
        "t002_20200101_fc0_1_SIM", 1
    )

    assert result.job_name == "t002_20200101_fc0_1_SIM"
    assert result.counter == 1


def test_sqlite_get_last_job_data_dc_returns_correct_row_among_multiple_counters(sqlite_db_manager):
    """Return the row matching the requested counter when the job has multiple counter entries."""
    job_name = "t002_20200101_fc0_3_SIM"
    _insert_sqlite_row(sqlite_db_manager, {**_base_row(job_name, counter=1, job_id=20), "status": "FAILED"})
    _insert_sqlite_row(sqlite_db_manager, {**_base_row(job_name, counter=2, job_id=22), "status": "COMPLETED"})
    _insert_sqlite_row(sqlite_db_manager, {**_base_row(job_name, counter=3, job_id=23), "status": "RUNNING"})

    result = sqlite_db_manager.get_last_job_data_dc_by_job_name_and_counter(job_name, 2)

    assert result.counter == 2
    assert result.status == "COMPLETED"
    assert result.job_id == 22


def test_sqlite_get_last_job_data_dc_raises_when_not_found(sqlite_db_manager):
    """Raise an exception when no row matches job_name and counter."""
    with pytest.raises(Exception, match="No job_data found"):
        sqlite_db_manager.get_last_job_data_dc_by_job_name_and_counter(
            "nonexistent_job", 99
        )


@pytest.mark.parametrize("counter", [1, 2, 3])
def test_sqlite_get_last_job_data_dc_only_matching_counter_is_returned(sqlite_db_manager, counter):
    """Return only the row matching the requested counter value."""
    job_name = f"t002_20200101_fc0_{counter}_SIM_counter_test"
    for c in range(1, 4):
        _insert_sqlite_row(sqlite_db_manager, _base_row(job_name, counter=c, job_id=100 + c))

    result = sqlite_db_manager.get_last_job_data_dc_by_job_name_and_counter(
        job_name, counter
    )

    assert result.counter == counter


def test_sqlalchemy_get_last_job_data_dc_by_job_name_returns_highest_id(sqlalchemy_db_manager):
    """Return the row with the highest id when the job has multiple counter entries."""
    job_data_table = sqlalchemy_db_manager.table_registry.get(JobDataTable.name)
    job_name = "t001_20200101_fc0_10_SIM"
    rows = [
        {**_base_row(job_name, counter=1, job_id=50), "status": "FAILED"},
        {**_base_row(job_name, counter=2, job_id=51), "status": "COMPLETED"},
    ]
    with sqlalchemy_db_manager.engine.connect() as conn:
        conn.execute(insert(job_data_table), rows)
        conn.commit()

    result = sqlalchemy_db_manager.get_last_job_data_dc_by_job_name(job_name)

    assert result.job_name == job_name
    assert result.counter == 2
    assert result.job_id == 51


def test_sqlalchemy_get_last_job_data_dc_by_job_name_single_row(sqlalchemy_db_manager):
    """Return the only row when exactly one row exists for the job_name."""
    job_data_table = sqlalchemy_db_manager.table_registry.get(JobDataTable.name)
    job_name = "t001_20200101_fc0_11_SIM"
    with sqlalchemy_db_manager.engine.connect() as conn:
        conn.execute(insert(job_data_table), [_base_row(job_name, counter=1, job_id=60)])
        conn.commit()

    result = sqlalchemy_db_manager.get_last_job_data_dc_by_job_name(job_name)

    assert result.job_name == job_name
    assert result.counter == 1


def test_sqlalchemy_get_last_job_data_dc_by_job_name_raises_when_not_found(sqlalchemy_db_manager):
    """Raise an exception when no row exists for the given job_name."""
    with pytest.raises(Exception, match="No job_data found"):
        sqlalchemy_db_manager.get_last_job_data_dc_by_job_name("nonexistent_job_name")


def test_sqlite_get_last_job_data_dc_by_job_name_returns_highest_id(sqlite_db_manager):
    """Return the row with the highest id when the job has multiple counter entries."""
    job_name = "t002_20200101_fc0_10_SIM"
    _insert_sqlite_row(sqlite_db_manager, {**_base_row(job_name, counter=1, job_id=50), "status": "FAILED"})
    _insert_sqlite_row(sqlite_db_manager, {**_base_row(job_name, counter=2, job_id=51), "status": "COMPLETED"})

    result = sqlite_db_manager.get_last_job_data_dc_by_job_name(job_name)

    assert result.job_name == job_name
    assert result.counter == 2
    assert result.job_id == 51


def test_sqlite_get_last_job_data_dc_by_job_name_single_row(sqlite_db_manager):
    """Return the only row when exactly one row exists for the job_name."""
    job_name = "t002_20200101_fc0_11_SIM"
    _insert_sqlite_row(sqlite_db_manager, _base_row(job_name, counter=1, job_id=60))

    result = sqlite_db_manager.get_last_job_data_dc_by_job_name(job_name)

    assert result.job_name == job_name
    assert result.counter == 1


def test_sqlite_get_last_job_data_dc_by_job_name_raises_when_not_found(sqlite_db_manager):
    """Raise an exception when no row exists for the given job_name."""
    with pytest.raises(Exception, match="No job_data found"):
        sqlite_db_manager.get_last_job_data_dc_by_job_name("nonexistent_job_name")
