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

"""Unit tests for ``autosubmit.database.db_common``.

We cover mainly error and validation scenarios here. See
the ``test/integration/test_db_common.py`` for more tests.
"""

import inspect
import shutil
import sqlite3
import subprocess
from contextlib import closing
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple
from unittest.mock import MagicMock

import pytest

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database import db_common
from autosubmit.database.db_common import TIMEOUT, database_backup
from autosubmit.log.log import AutosubmitCritical

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

_EXPID = "a000"
_PREVIOUS_BACKUP = "-- previous good backup\n"
_DUMP_OUTPUT = "BEGIN TRANSACTION;\nCOMMIT;\n"
_JOB_DATA_ROWS = [("job list line\n" * 100,), ("single job",)]


class JobDataPaths(NamedTuple):
    """Paths of an experiment's jobs historical database and its backup files."""

    db: Path
    sql: Path
    tmp_sql: Path


@pytest.fixture
def job_data_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> JobDataPaths:
    """Use a temporary SQLite jobdata directory with backups enabled and an existing database."""
    monkeypatch.setattr(BasicConfig, "DATABASE_BACKEND", "sqlite")
    monkeypatch.setattr(BasicConfig, "DATABASE_BACKUP", True)
    monkeypatch.setattr(BasicConfig, "JOBDATA_DIR", str(tmp_path))
    paths = JobDataPaths(
        db=tmp_path / f"job_data_{_EXPID}.db",
        sql=tmp_path / f"job_data_{_EXPID}.sql",
        tmp_sql=tmp_path / f"job_data_{_EXPID}.sql.tmp",
    )
    paths.db.touch()
    return paths


@pytest.fixture
def mocked_log(mocker: "MockerFixture") -> MagicMock:
    """Mock the logger used by ``db_common``."""
    return mocker.patch("autosubmit.database.db_common.Log")


def _fake_dump(cmd: list[str], stdout, **_) -> subprocess.CompletedProcess:
    """Simulate a successful ``sqlite3 .dump`` writing to the given file."""
    stdout.write(_DUMP_OUTPUT)
    return subprocess.CompletedProcess(cmd, 0)


def _partial_dump_then_fail(cmd: list[str], stdout, **_) -> None:
    """Simulate ``sqlite3 .dump`` writing part of the output and then exiting with an error."""
    stdout.write("BEGIN TRANSACTION;\n-- truncated")
    raise subprocess.CalledProcessError(1, cmd, stderr="Error: database disk image is malformed")


@pytest.mark.parametrize(
    "fn",
    [
        "check_experiment_exists",
        "delete_experiment",
        "get_autosubmit_version",
        "get_experiment_description",
        "get_experiment_id",
        "last_name_used",
        "save_experiment",
        "update_experiment_description_version",
    ],
)
def test_db_common_sqlite_multiprocessing_queue_error(mocker, fn: str, tmp_path) -> None:
    """Test the queue timeout error path for ``db_common`` functions.

    ``db_common`` uses multiprocessing and a ``Queue`` to launch database operations (SQLite).
    This test simply iterates all the functions and confirms that upon an error in the
    ``Queue`` reading (like a timeout) the function will raise an ``AutosubmitCritical``
    error.

    TODO: In the future we should possibly drop that multiprocessing approach, and use
          simpler timeouts directly in SQLAlchemy or sqlite3.
    """
    mocked_queue = mocker.patch('multiprocessing.Queue')
    mocker.patch('multiprocessing.Queue', return_value=mocked_queue)
    mocker.patch('multiprocessing.Process')
    mocked_queue.get.side_effect = [Exception]

    db_common_fn = getattr(db_common, fn)
    sig = inspect.signature(db_common_fn)
    params = ['' for _ in range(len(sig.parameters))]
    with pytest.raises(AutosubmitCritical):
        db_common_fn(*params)

def test_save_experiment_sqlite_open_conn_error(monkeypatch, tmp_path, mocker):
    """Test the ``open_conn`` error path for ``db_common`` functions.

    Several functions in ``db_common`` (SQLite) follow the pattern of
    calling ``check_db`` and then try/catch the ``open_conn`` call.

    Here, we verify that ``open_conn`` failing results in the expected
    outcome."""
    monkeypatch.setattr(db_common, 'TIMEOUT', 1)
    monkeypatch.setattr(BasicConfig, 'DB_PATH', str(tmp_path))

    mocker.patch('autosubmit.database.db_common.open_conn', side_effect=db_common.DbException('bla'))

    for fn in [
        'get_experiment_id',
        'update_experiment_description_version',
        'last_name_used',
        'delete_experiment',
        'save_experiment',
        'get_experiment_description',
        'get_autosubmit_version'
    ]:
        with pytest.raises(AutosubmitCritical):
            db_common_fn = getattr(db_common, fn)
            sig = inspect.signature(db_common_fn)
            params = ['' for _ in range(len(sig.parameters))]
            db_common_fn(*params)

def test_save_experiment_integrity_error(monkeypatch, tmp_path, mocker):
    """Test the ``IntegrityError`` error path for ``db_common.save_experiment``.."""
    monkeypatch.setattr(db_common, 'TIMEOUT', 1)
    monkeypatch.setattr(BasicConfig, 'DB_PATH', str(tmp_path))

    from sqlalchemy.exc import IntegrityError

    mocker.patch('autosubmit.database.db_common.open_conn', side_effect=IntegrityError('bla', 'bla', 'bla'))

    with pytest.raises(AutosubmitCritical) as e:
        db_common.save_experiment('', '', '')
        assert "could not register experiment" in str(e.value)


def test_database_backup_disabled(
    job_data_paths: JobDataPaths, mocked_log: MagicMock, mocker: "MockerFixture", monkeypatch: pytest.MonkeyPatch
) -> None:
    """With backups disabled, no dump runs and an existing backup is left untouched."""
    monkeypatch.setattr(BasicConfig, "DATABASE_BACKUP", False)
    job_data_paths.sql.write_text(_PREVIOUS_BACKUP)
    mocked_run = mocker.patch("autosubmit.database.db_common.subprocess.run")

    database_backup(_EXPID)

    mocked_run.assert_not_called()
    assert job_data_paths.sql.read_text() == _PREVIOUS_BACKUP
    assert not job_data_paths.tmp_sql.exists()
    mocked_log.warning.assert_not_called()


def test_database_backup_success(
    job_data_paths: JobDataPaths, mocked_log: MagicMock, mocker: "MockerFixture"
) -> None:
    """A successful dump runs without a shell, replaces the backup and removes the temporary file."""
    job_data_paths.sql.write_text(_PREVIOUS_BACKUP)
    mocked_run = mocker.patch("autosubmit.database.db_common.subprocess.run", side_effect=_fake_dump)

    database_backup(_EXPID)

    args, kwargs = mocked_run.call_args
    assert args[0] == ["sqlite3", "-cmd", f".timeout {TIMEOUT * 1000}", str(job_data_paths.db), ".dump"]
    assert kwargs["check"] is True
    assert "shell" not in kwargs
    assert job_data_paths.sql.read_text() == _DUMP_OUTPUT
    assert not job_data_paths.tmp_sql.exists()
    mocked_log.warning.assert_not_called()


@pytest.mark.parametrize(
    "side_effect,expected_reason",
    [
        pytest.param(_partial_dump_then_fail, "malformed", id="sqlite3-error-after-partial-dump"),
        pytest.param(FileNotFoundError("sqlite3"), "sqlite3", id="sqlite3-not-installed"),
    ],
)
def test_database_backup_failure_keeps_previous_backup(
    job_data_paths: JobDataPaths,
    mocked_log: MagicMock,
    mocker: "MockerFixture",
    side_effect,
    expected_reason: str,
) -> None:
    """A failed dump logs a warning, keeps the previous backup and removes the temporary file."""
    job_data_paths.sql.write_text(_PREVIOUS_BACKUP)
    mocker.patch("autosubmit.database.db_common.subprocess.run", side_effect=side_effect)

    database_backup(_EXPID)

    assert job_data_paths.sql.read_text() == _PREVIOUS_BACKUP
    assert not job_data_paths.tmp_sql.exists()
    mocked_log.warning.assert_called_once()
    assert expected_reason in mocked_log.warning.call_args[0][0]


def test_database_backup_missing_database(
    job_data_paths: JobDataPaths, mocked_log: MagicMock, mocker: "MockerFixture"
) -> None:
    """A missing jobs historical database logs a warning without running ``sqlite3``."""
    job_data_paths.db.unlink()
    mocked_run = mocker.patch("autosubmit.database.db_common.subprocess.run")

    database_backup(_EXPID)

    mocked_run.assert_not_called()
    assert not job_data_paths.sql.exists()
    mocked_log.warning.assert_called_once()
    assert str(job_data_paths.db) in mocked_log.warning.call_args[0][0]


@pytest.mark.skipif(shutil.which("sqlite3") is None, reason="sqlite3 CLI not installed")
def test_database_backup_dump_can_be_restored(job_data_paths: JobDataPaths, mocked_log: MagicMock) -> None:
    """The real ``sqlite3`` CLI accepts the arguments and its dump restores the original data."""
    with closing(sqlite3.connect(job_data_paths.db)) as conn:
        conn.execute("CREATE TABLE job_data (id INTEGER PRIMARY KEY, platform_output TEXT)")
        conn.executemany("INSERT INTO job_data (platform_output) VALUES (?)", _JOB_DATA_ROWS)
        conn.commit()

    database_backup(_EXPID)

    mocked_log.warning.assert_not_called()
    assert not job_data_paths.tmp_sql.exists()
    with closing(sqlite3.connect(":memory:")) as restored:
        restored.executescript(job_data_paths.sql.read_text())
        rows = restored.execute("SELECT platform_output FROM job_data ORDER BY id").fetchall()
    assert rows == _JOB_DATA_ROWS

