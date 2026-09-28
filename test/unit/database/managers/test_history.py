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
from sqlalchemy import and_, create_engine, delete, insert, inspect, select

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database.managers.history import (
    CURRENT_DB_VERSION,
    DB_EXPERIMENT_HEADER_SCHEMA_CHANGES,
    SqlAlchemyExperimentHistoryDbManager,
)
from autosubmit.database.models.tables import JobDataTable
from autosubmit.history.utils import get_current_datetime
from test._oldschema import old_experiment_run_table, old_job_data_table, with_schema


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

    rows = db_manager.select_jobs_data(["nonexistent"])
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

    job_data_table = with_schema(None, JobDataTable)
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


def _set_only_version(db_manager, version: int) -> None:
    """Reset the recorded migrations and leave only ``version`` (test helper)."""
    with db_manager.engine.begin() as conn:
        conn.execute(delete(db_manager._schema_migrations_table))
        db_manager._set_schema_version(conn, version)


def test_sqlalchemy_create_creates_index_and_records_version(sqlalchemy_db_manager) -> None:
    """A freshly initialized SQLAlchemy database records the current version and has the job_data index."""
    assert sqlalchemy_db_manager._get_schema_version(
        sqlalchemy_db_manager.engine, sqlalchemy_db_manager.schema
    ) == CURRENT_DB_VERSION
    assert sqlalchemy_db_manager.is_header_ready_db_version() is True

    inspector = inspect(sqlalchemy_db_manager.engine)
    indexes = {idx['name'] for idx in inspector.get_indexes(JobDataTable.name)}
    assert "ix_job_data_job_name" in indexes


@pytest.mark.parametrize(
    "stored_version, expected_header_ready",
    [
        (0, False),
        (DB_EXPERIMENT_HEADER_SCHEMA_CHANGES - 1, False),
        (DB_EXPERIMENT_HEADER_SCHEMA_CHANGES, True),
        (CURRENT_DB_VERSION, True),
    ],
)
def test_sqlalchemy_version_checks(
    sqlalchemy_db_manager, stored_version: int, expected_header_ready: bool
) -> None:
    """Report whether the recorded schema version is new enough for the completion trigger."""
    _set_only_version(sqlalchemy_db_manager, stored_version)

    assert sqlalchemy_db_manager.is_header_ready_db_version() is expected_header_ready


def test_sqlalchemy_no_database_reports_version_zero(tmp_path, monkeypatch) -> None:
    """A manager pointing to a non-existent database reports version 0."""
    monkeypatch.setattr(BasicConfig, "DATABASE_BACKEND", "sqlite")
    db_manager = SqlAlchemyExperimentHistoryDbManager(
        schema="t000",
        jobdata_path=str(tmp_path / "wrong-folder"),
        jobdata_file="job_data_t000.db",
    )

    assert db_manager._get_schema_version(db_manager.engine, db_manager.schema) == 0
    assert db_manager.is_header_ready_db_version() is False


def test_select_jobs_data_regression_sqlite_variable_limit(tmp_path, monkeypatch):
    """Regression test: a single IN-clause crashes above SQLite's variable-number limit.

    ``select_jobs_data`` splits the job names into chunks instead, so it works
    regardless of the backend limit (which varies by build, often 999 or 32766).
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

    # A single IN-clause with all the names must fail above the limit.
    old_query = select(job_data_table).where(
        and_(
            job_data_table.c.last == 1,
            job_data_table.c.job_name.in_(job_names),
        )
    )
    with db_manager.engine.connect() as conn:
        with pytest.raises(Exception, match="too many SQL variables|SQLITE_MAX_VARIABLE_NUMBER"):
            conn.execute(old_query).fetchall()

    # select_jobs_data chunks the names, so it succeeds.
    result = db_manager.select_jobs_data(job_names)

    assert len(result) == n_jobs
    assert all(row["last"] == 1 for row in result)


def _base_row(job_name: str, counter: int, job_id: int, status: str = "COMPLETED") -> dict:
    """Return a minimal job_data row dict for job-data tests."""
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


def test_sqlalchemy_get_job_data_by_job_id_name_raises_when_not_found(sqlalchemy_db_manager) -> None:
    """Raise an exception when no row matches the job id and name."""
    with pytest.raises(Exception, match="No job_data found"):
        sqlalchemy_db_manager.get_job_data_by_job_id_name(999, "nonexistent_job")


def test_sqlalchemy_get_jobs_data_last_row_projects_columns(sqlalchemy_db_manager) -> None:
    """get_jobs_data_last_row returns only the columns needed to recover logs."""
    job_data_table = sqlalchemy_db_manager.table_registry.get(JobDataTable.name)
    job_name = "t001_20200101_fc0_20_SIM"
    with sqlalchemy_db_manager.engine.connect() as conn:
        conn.execute(insert(job_data_table), [_base_row(job_name, counter=1, job_id=70)])
        conn.commit()

    result = sqlalchemy_db_manager.get_jobs_data_last_row([job_name])

    assert set(result[job_name].keys()) == set(
        sqlalchemy_db_manager._JOB_DATA_LAST_ROW_COLUMNS
    )
    assert result[job_name]["job_id"] == 70
