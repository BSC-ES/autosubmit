# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
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

"""Integration tests for ``autosubmit run`` command."""

import time
from contextlib import nullcontext as does_not_raise
from os import R_OK, W_OK
from typing import TYPE_CHECKING

import pytest

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.config.yamlparser import YAMLParserFactory
from autosubmit.database.db_common import (
    get_experiment_description,
    update_experiment_description_version,
)
from autosubmit.experiment.manage import create
from autosubmit.install import install
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_list import JobList, load_job_list
from autosubmit.job.job_utils import check_wrappers
from autosubmit.job.manage import set_status
from autosubmit.log.log import Log
from autosubmit.platforms.platform import Platform
from autosubmit.scheduler import Scheduler
from autosubmit.workflow.manage import _prepare_run, monitor, run, stop

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

    from test.integration.conftest import AutosubmitExperimentFixture


def _assert_log_contains(mock_error, message: str) -> None:
    """Assert that an error log contains the expected message.

    :param mock_error: Mocked ``Log.error`` method.
    :param message: Text expected to be present in one of the logged errors.
    """
    logged_messages = [
        str(call.args[0])
        for call in mock_error.call_args_list
        if call.args
    ]

    assert any(message in logged_message for logged_message in logged_messages), (
        f"Expected {message!r} in logged errors: {logged_messages!r}"
    )


def test__init_logs_config_file_not_found(autosubmit_exp, mocker, monkeypatch):
    """Test that an error is logged when the configuration file is missing.

    :param autosubmit_exp: Fixture used to initialise the Autosubmit environment.
    :param mocker: Pytest mock fixture.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    autosubmit_exp()

    args = mocker.MagicMock()
    args.logconsole = "DEBUG"
    args.logfile = "DEBUG"
    args.command = "describe"

    monkeypatch.setattr(BasicConfig, "CONFIG_FILE_FOUND", False)
    mock_error = mocker.patch.object(Log, "error")

    run(args)

    _assert_log_contains(mock_error, "No configuration file")


def test__init_logs_sqlite_db_path_not_found(
    autosubmit_exp,
    mocker,
    monkeypatch,
    tmp_path,
):
    """Test that an error is logged when the SQLite database cannot be found.

    :param autosubmit_exp: Fixture used to initialise the Autosubmit environment.
    :param mocker: Pytest mock fixture.
    :param monkeypatch: Pytest monkeypatch fixture.
    :param tmp_path: Temporary directory used for the missing database path.
    """
    exp = autosubmit_exp()

    args = mocker.MagicMock()
    args.expid = exp.expid
    args.logconsole = "DEBUG"
    args.logfile = "DEBUG"
    args.command = "describe"

    monkeypatch.setattr(BasicConfig, "DATABASE_BACKEND", "sqlite")
    monkeypatch.setattr(
        BasicConfig,
        "DB_PATH",
        str(tmp_path / "you-cannot-find-me.xz"),
    )

    mock_error = mocker.patch.object(Log, "error")

    run(args)

    _assert_log_contains(
        mock_error,
        "Experiments database not found in this filesystem",
    )


def test__init_logs_sqlite_db_not_readable(autosubmit_exp, mocker, monkeypatch):
    """Test that an error is logged when the SQLite database is not readable.

    :param autosubmit_exp: Fixture used to initialise the Autosubmit environment.
    :param mocker: Pytest mock fixture.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    exp = autosubmit_exp()

    args = mocker.MagicMock()
    args.expid = exp.expid
    args.logconsole = "DEBUG"
    args.logfile = "DEBUG"
    args.command = "describe"

    monkeypatch.setattr(BasicConfig, "DATABASE_BACKEND", "sqlite")

    def path_exists(_, perm):
        return perm != R_OK

    mocker.patch("os.access", side_effect=path_exists)
    mock_error = mocker.patch.object(Log, "error")

    run(args)

    _assert_log_contains(mock_error, "not readable")


def test__init_logs_sqlite_db_not_writable(autosubmit_exp, mocker, monkeypatch):
    """Test that an error is logged when the SQLite database is not writable.

    :param autosubmit_exp: Fixture used to initialise the Autosubmit environment.
    :param mocker: Pytest mock fixture.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    exp = autosubmit_exp()

    args = mocker.MagicMock()
    args.expid = exp.expid
    args.logconsole = "DEBUG"
    args.logfile = "DEBUG"
    args.command = "describe"

    monkeypatch.setattr(BasicConfig, "DATABASE_BACKEND", "sqlite")

    def path_exists(_, perm):
        return perm != W_OK

    mocker.patch("os.access", side_effect=path_exists)
    mock_error = mocker.patch.object(Log, "error")

    run(args)

    _assert_log_contains(mock_error, "not writable")


def test__init_logs_sqlite_exp_path_does_not_exist(
    autosubmit_exp,
    mocker,
    monkeypatch,
):
    """Test that an error is logged when a SQLite experiment does not exist.

    :param autosubmit_exp: Fixture used to initialise the Autosubmit environment.
    :param mocker: Pytest mock fixture.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    autosubmit_exp()

    args = mocker.MagicMock()
    args.expid = "0000"
    args.logconsole = "DEBUG"
    args.logfile = "DEBUG"
    args.command = "setstatus"

    monkeypatch.setattr(BasicConfig, "DATABASE_BACKEND", "sqlite")
    mock_error = mocker.patch.object(Log, "error")

    run(args)

    _assert_log_contains(mock_error, "Experiment does not exist")


def test__init_logs_postgres_exp_path_does_not_exist_no_yaml_data(
    autosubmit_exp,
    mocker,
    monkeypatch,
):
    """Test that a PostgreSQL experiment without YAML data logs an error.

    :param autosubmit_exp: Fixture used to initialise the Autosubmit environment.
    :param mocker: Pytest mock fixture.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    autosubmit_exp()

    args = mocker.MagicMock()
    args.expid = "0000"
    args.logconsole = "DEBUG"
    args.logfile = "DEBUG"
    args.command = "setstatus"

    monkeypatch.setattr(BasicConfig, "DATABASE_BACKEND", "postgres")
    mocker.patch("autosubmit.config.configcommon.AutosubmitConfig.reload")

    mock_error = mocker.patch.object(Log, "error")

    run(args)

    _assert_log_contains(mock_error, "has no yml data")


def test__init_logs_sqlite_mismatch_as_version_upgrade_it(autosubmit_exp, mocker):
    """Test that an invalid Autosubmit version can be explicitly updated.

    :param autosubmit_exp: Fixture used to initialise the Autosubmit environment.
    :param mocker: Pytest mock fixture.
    """
    exp = autosubmit_exp(
        experiment_data={
            "CONFIG": {
                "AUTOSUBMIT_VERSION": "bright-opera",
            }
        }
    )

    args = mocker.MagicMock()
    args.expid = exp.expid
    args.logconsole = "DEBUG"
    args.logfile = "DEBUG"
    args.command = "setstatus"
    args.update_version = True
    args.__contains__ = lambda x, y: True

    mocked_set_status = mocker.patch("autosubmit.workflow.manage.set_status")

    run(args)

    assert mocked_set_status.called


def test__init_logs_sqlite_mismatch_as_version(autosubmit_exp, mocker):
    """Test that an Autosubmit version mismatch is logged as an error.

    :param autosubmit_exp: Fixture used to initialise the Autosubmit environment.
    :param mocker: Pytest mock fixture.
    """
    exp = autosubmit_exp(
        experiment_data={
            "CONFIG": {
                "AUTOSUBMIT_VERSION": "bright-opera",
            }
        }
    )

    args = mocker.MagicMock()
    args.expid = exp.expid
    args.logconsole = "DEBUG"
    args.logfile = "DEBUG"
    args.command = "setstatus"

    mock_error = mocker.patch.object(Log, "error")

    run(args)

    _assert_log_contains(mock_error, "update the experiment version")


def test_install_sqlite_already_exists(monkeypatch, tmp_path, mocker):
    """Test that an existing SQLite database is reported as an error.

    :param monkeypatch: Pytest monkeypatch fixture.
    :param tmp_path: Temporary directory used for the database file.
    :param mocker: Pytest mock fixture.
    """
    monkeypatch.setattr(BasicConfig, "DATABASE_BACKEND", "sqlite")

    db_file = tmp_path / "test.db"
    db_file.touch()

    monkeypatch.setattr(BasicConfig, "DB_PATH", str(db_file))

    mock_error = mocker.patch.object(Log, "error")

    install()

    mock_error.assert_called_once_with("Database already exists.")


def test_install_sqlite_create_db_fails(monkeypatch, tmp_path, mocker):
    """Test that failure to create the SQLite database is logged.

    :param monkeypatch: Pytest monkeypatch fixture.
    :param tmp_path: Temporary directory used for the database file.
    :param mocker: Pytest mock fixture.
    """
    monkeypatch.setattr(BasicConfig, "DATABASE_BACKEND", "sqlite")

    db_file = tmp_path / "test.db"
    monkeypatch.setattr(BasicConfig, "DB_PATH", str(db_file))

    mocker.patch("autosubmit.install.create_db", return_value=False)
    mock_error = mocker.patch.object(Log, "error")

    install()

    _assert_log_contains(mock_error, "Can not write database file")


def test_install_sqlite_create_new_db(monkeypatch, tmp_path):
    """Test that a new SQLite database is created successfully.

    :param monkeypatch: Pytest monkeypatch fixture.
    :param tmp_path: Temporary directory used for the database file.
    """
    monkeypatch.setattr(BasicConfig, "DATABASE_BACKEND", "sqlite")

    db_file = tmp_path / "test.db"
    monkeypatch.setattr(BasicConfig, "DB_PATH", str(db_file))

    install()

    assert db_file.exists()


def test_install_postgres_create_db_fails(monkeypatch, mocker):
    """Test that failure to create the PostgreSQL database is logged.

    :param monkeypatch: Pytest monkeypatch fixture.
    :param mocker: Pytest mock fixture.
    """
    monkeypatch.setattr(BasicConfig, "DATABASE_BACKEND", "postgres")
    mocker.patch("autosubmit.install.create_db", return_value=False)

    mock_error = mocker.patch.object(Log, "error")

    install()

    _assert_log_contains(mock_error, "Failed to create Postgres database")


@pytest.mark.docker
@pytest.mark.postgres
def test_update_version(as_db: str, autosubmit_exp, mocker):
    """Test that an experiment with an outdated version can be updated.

    :param as_db: PostgreSQL database fixture.
    :param autosubmit_exp: Fixture used to create an Autosubmit experiment.
    :param mocker: Pytest mock fixture.
    """
    wrong_version = "bright-opera"

    exp = autosubmit_exp(
        experiment_data={
            "CONFIG": {
                "AUTOSUBMIT_VERSION": wrong_version,
            }
        }
    )

    args = mocker.MagicMock()
    args.expid = exp.expid
    args.logconsole = "DEBUG"
    args.logfile = "DEBUG"
    args.command = "setstatus"

    assert update_experiment_description_version(exp.expid)

    as_conf = AutosubmitConfig(
        exp.expid,
        BasicConfig,
        YAMLParserFactory(),
    )
    as_conf.reload(force_load=True)

    assert as_conf.get_version() != wrong_version


@pytest.mark.docker
@pytest.mark.postgres
def test_update_description(as_db: str, autosubmit_exp, mocker):
    """Test that an experiment description can be updated.

    :param as_db: PostgreSQL database fixture.
    :param autosubmit_exp: Fixture used to create an Autosubmit experiment.
    :param mocker: Pytest mock fixture.
    """
    wrong_version = "bright-opera"

    exp = autosubmit_exp(
        experiment_data={
            "CONFIG": {
                "AUTOSUBMIT_VERSION": wrong_version,
            }
        }
    )

    args = mocker.MagicMock()
    args.expid = exp.expid
    args.logconsole = "DEBUG"
    args.logfile = "DEBUG"
    args.command = "setstatus"

    new_description = "a new description arrived"

    assert update_experiment_description_version(
        exp.expid,
        new_description,
    )

    assert new_description == get_experiment_description(exp.expid)[0][0]


@pytest.mark.parametrize(
    "experiment_data,context_mgr",
    [
        (
            {
                "JOBS": {
                    "DQC": {
                        "FOR": {
                            "NAME": [
                                "BASIC",
                                "FULL",
                            ],
                            "WALLCLOCK": "00:40",
                        },
                    },
                },
            },
            pytest.raises(IndexError),
        ),
        (
            {
                "JOBS": {
                    "DQC": {
                        "FOR": {
                            "NAME": [
                                "BASIC",
                                "FULL",
                            ],
                        },
                        "WALLCLOCK": "00:40",
                    },
                },
            },
            does_not_raise(),
        ),
    ],
    ids=[
        "Missing WALLCLOCK in FOR",
        "Correct FOR",
    ],
)
def test_parse_data_loops(
    autosubmit_exp: "AutosubmitExperimentFixture",
    experiment_data: dict,
    context_mgr: "AbstractContextManager",
):
    """Test parsing of job loop configuration data.

    :param autosubmit_exp: Fixture used to create an Autosubmit experiment.
    :param experiment_data: Experiment configuration to parse.
    :param context_mgr: Expected exception context manager.
    """
    with context_mgr:
        autosubmit_exp(
            experiment_data=experiment_data,
            create=False,
            include_jobs=False,
        )


@pytest.mark.parametrize(
    "_exit,job_previous_status,expected_jobs_to_check",
    [
        (
            True,
            Status.FAILED,
            0,
        ),
        (
            True,
            Status.RUNNING,
            0,
        ),
        (
            False,
            Status.FAILED,
            0,
        ),
        (
            False,
            Status.RUNNING,
            1,
        ),
    ],
    ids=[
        "If exiting, no jobs are checked",
        "If exiting, no jobs are checked",
        "If not exiting, ignore failed jobs",
        "If not exiting, do NOT ignore running jobs",
    ],
)
def test_check_wrappers_and_as_exit(
    _exit,
    job_previous_status,
    expected_jobs_to_check,
    autosubmit_exp,
    mocker,
):
    """Test wrapper handling when the scheduler exit flag changes.

    :param _exit: Value assigned to ``Scheduler.exit``.
    :param job_previous_status: Previous status of the test job.
    :param expected_jobs_to_check: Expected number of jobs to inspect.
    :param autosubmit_exp: Fixture used to create an Autosubmit experiment.
    :param mocker: Pytest mock fixture.
    """
    exp = autosubmit_exp(experiment_data={})
    as_conf: AutosubmitConfig = exp.as_conf

    job = Job("1", "1", job_previous_status)

    job_list: JobList = mocker.MagicMock(spec=JobList)
    job_list.get_job_list.return_value = [job]
    job_list.job_package_map = {}

    platform = mocker.MagicMock(spec=Platform)
    platform.name = "test_platform"

    Scheduler.exit = _exit

    check_wrappers(as_conf, job_list, exp.expid)


def test_create_txt_output_writes_status_file(autosubmit_exp):
    """Test that text output creates status files.

    Both ``-o txt`` and ``-d`` should create a text file in the status directory.

    :param autosubmit_exp: Fixture used to create an Autosubmit experiment.
    """
    exp = autosubmit_exp(include_jobs=True)

    assert not list(exp.status_dir.glob("*.txt")), (
        "status/ should be empty before test"
    )

    create(
        exp.expid,
        noplot=False,
        hide=True,
        output="txt",
        force=True,
    )

    txt_files_after_txt = list(exp.status_dir.glob("*.txt"))

    assert len(txt_files_after_txt) == 1, (
        "Expected exactly one txt file in status/ for -o txt"
    )

    time.sleep(1)

    create(
        exp.expid,
        noplot=True,
        hide=True,
        output=None,
        detail=True,
        force=True,
    )

    txt_files_after_detail = list(exp.status_dir.glob("*.txt"))

    assert len(txt_files_after_detail) == 2, (
        "Expected a second txt file in status/ for -d"
    )


def test_prepare_run_returns_tuple(autosubmit_exp):
    """Test that ``prepare_run`` returns the expected tuple.

    :param autosubmit_exp: Fixture used to create an Autosubmit experiment.
    """
    exp = autosubmit_exp(include_jobs=True)

    result = _prepare_run(
        exp.expid,
        check_scripts=False,
    )

    assert len(result) == 7

    (
        job_list,
        submitter,
        exp_history,
        _host,
        _as_conf,
        _platforms_to_test,
        recover,
    ) = result

    assert job_list is not None
    assert submitter is not None
    assert exp_history is not None
    assert recover is False


def test_prepare_run_returns_tuple_with_recover(autosubmit_exp):
    """Test that ``prepare_run`` returns the expected tuple in recovery mode.

    :param autosubmit_exp: Fixture used to create an Autosubmit experiment.
    """
    exp = autosubmit_exp(include_jobs=True)

    result = _prepare_run(
        exp.expid,
        check_scripts=False,
        recover=True,
    )

    assert len(result) == 7

    (
        job_list,
        submitter,
        exp_history,
        _host,
        _as_conf,
        _platforms_to_test,
        recover,
    ) = result

    assert job_list is not None
    assert submitter is not None
    assert exp_history is None
    assert recover is True


def test_stop_sets_exit_flag(autosubmit_exp, mocker):
    """Test that ``stop`` sets the scheduler exit flag.

    :param autosubmit_exp: Fixture used to create an Autosubmit experiment.
    :param mocker: Pytest mock fixture.
    """
    exp = autosubmit_exp()

    mocker.patch("builtins.input", return_value="y")
    mocker.patch(
        "autosubmit.helpers.processes.process_id",
        return_value=0,
    )

    original = Scheduler.exit

    try:
        Scheduler.exit = False

        result = stop(
            exp.expid,
            force_yes=True,
        )

        assert result is True
    finally:
        Scheduler.exit = original


def test_monitor_with_check_wrapper(autosubmit_exp):
    """Test that ``monitor`` loads wrapper packages when requested.

    :param autosubmit_exp: Fixture used to create an Autosubmit experiment.
    """
    exp = autosubmit_exp(include_jobs=True)

    as_conf = AutosubmitConfig(
        exp.expid,
        BasicConfig,
        YAMLParserFactory(),
    )
    as_conf.check_conf_files(True)

    job_list = load_job_list(
        exp.expid,
        as_conf,
        monitor=True,
        new=False,
    )
    job_list.save_jobs()

    result = monitor(
        exp.expid,
        file_format="pdf",
        lst="",
        filter_chunks="",
        filter_status="",
        filter_section="",
        hide=True,
        check_wrapper=True,
    )

    assert result is True


def test_set_status_with_detail(autosubmit_exp):
    """Test that ``set_status`` prints the job list when detail is requested.

    :param autosubmit_exp: Fixture used to create an Autosubmit experiment.
    """
    exp = autosubmit_exp(include_jobs=True)

    as_conf = AutosubmitConfig(
        exp.expid,
        BasicConfig,
        YAMLParserFactory(),
    )
    as_conf.check_conf_files(True)

    job_list = load_job_list(
        exp.expid,
        as_conf,
        monitor=True,
        new=False,
    )

    job_name = job_list.get_job_list()[0].name
    job_list.save_jobs()

    result = set_status(
        exp.expid,
        noplot=True,
        save=True,
        final="WAITING",
        filter_list=job_name,
        filter_chunks="",
        filter_status="",
        filter_section="",
        filter_type_chunk="",
        filter_type_chunk_split="",
        hide=True,
        detail=True,
    )

    assert result is True
