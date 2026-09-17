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

We cover mainly error and validation scenarios here.
"""

import shutil
import sqlite3
import subprocess
from contextlib import closing
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import IntegrityError

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


def test_install_creates_schema_migrations(autosubmit_config) -> None:
    """install() creates the general database with a recorded schema version."""
    autosubmit_config("a000", {})

    conn = sqlite3.connect(BasicConfig.DB_PATH)
    try:
        rows = conn.execute("SELECT version FROM general_schema_migrations").fetchall()
    finally:
        conn.close()

    assert rows == [(db_common.CURRENT_DATABASE_VERSION,)]


@pytest.mark.parametrize(
    "fn,args",
    [
        ("check_db", ()),
        ("check_experiment_exists", ("a000",)),
        ("delete_experiment", ("a000",)),
        ("get_autosubmit_version", ("a000",)),
        ("get_experiment_description", ("a000",)),
        ("get_experiment_expids", ()),
        ("get_experiment_id", ("a000",)),
        ("last_name_used", ()),
        ("save_experiment", ("a000", "desc", "4.0.0")),
        ("update_experiment_description_version", ("a000",)),
    ],
)
def test_db_common_raises_without_database(monkeypatch, tmp_path, fn: str, args: tuple) -> None:
    """Every database operation raises when the general database is missing."""
    monkeypatch.setattr(BasicConfig, "DATABASE_BACKEND", "sqlite")
    monkeypatch.setattr(BasicConfig, "DB_PATH", str(tmp_path / "missing.db"))

    with pytest.raises(AutosubmitCritical):
        getattr(db_common, fn)(*args)


@pytest.mark.parametrize(
    "exception",
    [
        IntegrityError("stmt", {}, Exception("boom")),
        Exception("boom"),
    ],
    ids=["integrity-error", "generic-exception"],
)
def test_save_experiment_wraps_insert_errors(mocker, monkeypatch, tmp_path, exception) -> None:
    """``save_experiment`` wraps insert errors in an ``AutosubmitCritical``."""
    db_file = tmp_path / "autosubmit.db"
    db_file.touch()
    monkeypatch.setattr(BasicConfig, "DATABASE_BACKEND", "sqlite")
    monkeypatch.setattr(BasicConfig, "DB_PATH", str(db_file))
    mocker.patch("autosubmit.database.db_common._get_engine", side_effect=exception)

    with pytest.raises(AutosubmitCritical):
        db_common.save_experiment("a000", "desc", "4.0.0")


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


def test_create_db_wraps_errors(mocker, monkeypatch, tmp_path) -> None:
    """``create_db`` wraps creation errors in an ``AutosubmitCritical``."""
    monkeypatch.setattr(BasicConfig, "DATABASE_BACKEND", "sqlite")
    monkeypatch.setattr(BasicConfig, "DB_PATH", str(tmp_path / "autosubmit.db"))
    mocker.patch("autosubmit.database.db_common._get_engine", side_effect=Exception("boom"))

    with pytest.raises(AutosubmitCritical):
        db_common.create_db()


@pytest.mark.parametrize("error_on_inexistence, create_folder, expect_raises", [
    (True, False, True),
    (False, True, False),
])
def test_check_experiment_exists_missing(autosubmit_config, error_on_inexistence, create_folder, expect_raises) -> None:
    """A missing experiment either raises or is registered from the local filesystem."""
    autosubmit_config("a000", {})
    name = "a999"
    if create_folder:
        Path(BasicConfig.LOCAL_ROOT_DIR, name).mkdir(parents=True, exist_ok=True)

    if expect_raises:
        with pytest.raises(AutosubmitCritical):
            db_common.check_experiment_exists(name, error_on_inexistence=error_on_inexistence)
    else:
        assert db_common.check_experiment_exists(name, error_on_inexistence=error_on_inexistence) is True
