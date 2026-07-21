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
from sqlalchemy import and_, create_engine, inspect, insert, select

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database.tables import JobDataTable, get_table_with_schema
from autosubmit.history.utils import get_current_datetime
from autosubmit.history.database_managers.experiment_history_db_manager import (
    create_experiment_history_db_manager,
    SqlAlchemyExperimentHistoryDbManager,
)

from test._oldschema import old_job_data_table, old_experiment_run_table


def _create_old_schema(engine):
    old_job_data_table.create(engine)
    old_experiment_run_table.create(engine)


def test_sqlalchemy_initialize_migration(tmp_path, mocker):
    """Migration adds missing columns to an old-schema database."""
    mocker.patch.object(BasicConfig, 'DATABASE_BACKEND', 'sqlite')
    db_path = tmp_path / "job_data_old.db"

    engine = create_engine(f"sqlite:///{db_path}")
    _create_old_schema(engine)
    engine.dispose()

    engine = create_engine(f"sqlite:///{db_path}")
    inspector = inspect(engine)
    cols = {c['name'] for c in inspector.get_columns("job_data")}
    assert "split" not in cols
    assert "splits" not in cols
    assert "fail_count" not in cols
    engine.dispose()

    db_manager = SqlAlchemyExperimentHistoryDbManager("test", str(tmp_path), "job_data_old.db")
    db_manager.initialize()

    inspector = inspect(db_manager.engine)
    cols = {c['name'] for c in inspector.get_columns("job_data")}
    assert "split" in cols
    assert "splits" in cols
    assert "fail_count" in cols

    rows = db_manager.select_jobs_data(get_table_with_schema(None, JobDataTable), ["nonexistent"])
    assert rows == []


def test_sqlalchemy_initialize_migration_multiple_times_works(tmp_path, mocker):
    """Running initialize() twice on an old-schema database does not raise."""
    mocker.patch.object(BasicConfig, 'DATABASE_BACKEND', 'sqlite')
    db_path = tmp_path / "job_data_bla.db"

    engine = create_engine(f"sqlite:///{db_path}")
    _create_old_schema(engine)
    engine.dispose()

    db_manager = SqlAlchemyExperimentHistoryDbManager("test", str(tmp_path), "job_data_bla.db")
    db_manager.initialize()
    db_manager.initialize()

    inspector = inspect(db_manager.engine)
    cols = {c['name'] for c in inspector.get_columns("job_data")}
    assert "split" in cols


def test_sqlalchemy_initialize_migration_preserves_data(tmp_path, mocker):
    """Existing data survives migration and is queryable."""
    mocker.patch.object(BasicConfig, 'DATABASE_BACKEND', 'sqlite')
    db_path = tmp_path / "job_data.db"

    engine = create_engine(f"sqlite:///{db_path}")
    _create_old_schema(engine)
    with engine.connect() as conn:
        conn.execute(
            old_experiment_run_table.insert().values(
                run_id=1, created='now', modified='now', start=0,
                chunk_unit='month', chunk_size=1, completed=0, total=1,
                failed=0, queuing=0, running=0, submitted=0,
            )
        )
        conn.execute(
            old_job_data_table.insert().values(
                id=1, counter=1, job_name='test_job', created='now', modified='now',
                submit=0, start=0, finish=0, status='COMPLETED', rowtype=0,
                ncpus=0, wallclock='00:00', qos='debug', energy=0, date='20200101',
                section='SIM', member='fc0', chunk=1, last=1, platform='LOCAL',
                job_id=1, extra_data='{}', out='', err='',
            )
        )
        conn.commit()
    engine.dispose()

    db_manager = SqlAlchemyExperimentHistoryDbManager("test", str(tmp_path), "job_data.db")
    db_manager.initialize()

    job_data_table = get_table_with_schema(None, JobDataTable)
    with db_manager.engine.connect() as conn:
        result = conn.execute(
            select(job_data_table).where(job_data_table.c.job_name == "test_job")
        ).fetchall()
    assert len(result) == 1
    row = result[0]
    assert row.job_name == "test_job"
    assert row.last == 1
    assert row.split is None
    assert row.fail_count == 0


def test_create_experiment_history_db_manager_invalid():
    with pytest.raises(ValueError):
        create_experiment_history_db_manager('banana')


def test_functions_not_implemented(mocker):
    """Confirm that we do not implement a few functions for Postgres."""
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
    job_data_table = get_table_with_schema(db_manager.schema, JobDataTable)
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
