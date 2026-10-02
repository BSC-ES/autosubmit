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
from autosubmit.helpers.version import get_version
from autosubmit.install import install
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_list import JobList, load_job_list
from autosubmit.job.job_utils import check_wrappers
from autosubmit.job.manage import set_status
from autosubmit.log.log import Log
from autosubmit.platforms.platform import Platform
from autosubmit.scheduler import Scheduler

# noinspection PyProtectedMember
from autosubmit.scripts.autosubmit import _autosubmit

# noinspection PyProtectedMember
from autosubmit.workflow.manage import _prepare_run, monitor, stop

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

    from test.integration.conftest import AutosubmitExperimentFixture


def _assert_log_contains(mock_error, message: str) -> None:
    """Assert that an error log contains the expected message.

    :param mock_error: Mocked ``Log.error`` method.
    :param message: Text expected to be present in one of the logged errors.
    """
    logged_messages = [
        str(call.args[0]) for call in mock_error.call_args_list if call.args
    ]

    assert any(message in logged_message for logged_message in logged_messages), (
        f"Expected {message!r} in logged errors: {logged_messages!r}"
    )


def test__init_logs_config_file_not_found(autosubmit_exp, mocker):
    """Test that an error is logged when the configuration file is missing."""
    mocked_basic_config = mocker.patch("autosubmit.scripts._validation.BasicConfig")
    mocked_basic_config.CONFIG_FILE_FOUND = False
    mocked_log = mocker.patch("autosubmit.scripts._validation.Log")

    autosubmit_exp()

    with pytest.raises(SystemExit):
        _autosubmit(["-lc", "DEBUG", "-lf", "DEBUG", "describe"])

    _assert_log_contains(
        mocked_log.error, 'Autosubmit configuration file "autosubmitrc" not found'
    )


def test__init_logs_sqlite_db_path_not_found(
    autosubmit_exp,
    mocker,
    tmp_path,
):
    """Test that an error is logged when the SQLite database cannot be found."""
    mocked_basic_config = mocker.patch("autosubmit.scripts._validation.BasicConfig")
    mocked_basic_config.DATABASE_BACKEND = "sqlite"
    mocked_basic_config.DB_PATH = str(tmp_path / "you-cannot-find-me.xz")

    mocked_log = mocker.patch("autosubmit.scripts._validation.Log")

    exp = autosubmit_exp()
    args = ["-lc", "DEBUG", "-lf", "DEBUG", "describe", exp.expid]

    with pytest.raises(SystemExit):
        _autosubmit(args)

    _assert_log_contains(
        mocked_log.error,
        "Experiments database not found",
    )


def test__init_logs_sqlite_db_not_readable(autosubmit_exp, mocker, tmp_path):
    """Test that an error is logged when the SQLite database is not readable."""
    mocked_basic_config = mocker.patch("autosubmit.scripts._validation.BasicConfig")
    mocked_basic_config.DATABASE_BACKEND = "sqlite"
    mocked_basic_config.DB_PATH = tmp_path / "database.db"
    mocked_basic_config.DB_PATH.touch()

    def path_exists(_, perm):
        return perm != R_OK

    mocker.patch("os.access", side_effect=path_exists)
    mocked_log = mocker.patch("autosubmit.scripts._validation.Log")

    exp = autosubmit_exp()
    args = ["-lc", "DEBUG", "-lf", "DEBUG", "describe", exp.expid]

    with pytest.raises(SystemExit):
        _autosubmit(args)

    _assert_log_contains(mocked_log.error, "not readable")


def test__init_logs_sqlite_db_not_writable(autosubmit_exp, mocker, tmp_path):
    """Test that an error is logged when the SQLite database is not writable."""
    mocked_basic_config = mocker.patch("autosubmit.scripts._validation.BasicConfig")
    mocked_basic_config.DATABASE_BACKEND = "sqlite"
    mocked_basic_config.DB_PATH = tmp_path / "database.db"
    mocked_basic_config.DB_PATH.touch()

    def path_exists(_, perm):
        return perm != W_OK

    mocker.patch("os.access", side_effect=path_exists)
    mocked_log = mocker.patch("autosubmit.scripts._validation.Log")

    exp = autosubmit_exp()
    args = ["-lc", "DEBUG", "-lf", "DEBUG", "describe", exp.expid]

    with pytest.raises(SystemExit):
        _autosubmit(args)

    _assert_log_contains(mocked_log.error, "not writable")


def test__init_logs_sqlite_exp_path_does_not_exist(
    autosubmit_exp,
    mocker,
    tmp_path,
):
    """Test that an error is logged when a SQLite experiment does not exist."""
    mocked_basic_config = mocker.patch("autosubmit.scripts._validation.BasicConfig")
    mocked_basic_config.DATABASE_BACKEND = "sqlite"
    mocked_basic_config.DB_PATH = tmp_path / "database.db"
    mocked_basic_config.DB_PATH.touch()

    mocked_log = mocker.patch("autosubmit.scripts._validation.Log")

    autosubmit_exp()
    args = ["-lc", "DEBUG", "-lf", "DEBUG", "setstatus", "yyyy", "-t", "WAITING"]

    with pytest.raises(SystemExit):
        _autosubmit(args)

    _assert_log_contains(mocked_log.error, "Experiment 'yyyy' was not found")


def test__init_logs_postgres_exp_path_does_not_exist_no_yaml_data(
    autosubmit_exp, mocker, tmp_path
):
    """Test that a PostgreSQL experiment without YAML data logs an error."""
    mocked_basic_config = mocker.patch("autosubmit.scripts._validation.BasicConfig")
    mocked_basic_config.DATABASE_BACKEND = "postgres"
    mocked_basic_config.DB_PATH = tmp_path / "database.db"
    mocked_basic_config.DB_PATH.touch()

    mocker.patch("autosubmit.scripts._validation.validate_required_files")
    mocked_log = mocker.patch("autosubmit.scripts._initialise.Log")

    as_exp = autosubmit_exp()
    args = ["-lc", "DEBUG", "-lf", "DEBUG", "clean", as_exp.expid]

    mocked_autosubmit_config = mocker.patch(
        "autosubmit.config.configcommon.AutosubmitConfig"
    )
    as_conf = mocker.MagicMock()
    mocked_autosubmit_config.return_value = as_conf
    as_conf.experiment_data = {}

    with pytest.raises(SystemExit):
        _autosubmit(args)

    _assert_log_contains(
        mocked_log.error, f"Experiment '{as_exp.expid}' contains no YAML configuration"
    )


def test__init_logs_sqlite_mismatch_as_version_upgrade_it(
    autosubmit_exp, mocker, tmp_path
):
    """Test that an invalid Autosubmit version can be explicitly updated."""
    mocked_basic_config = mocker.patch("autosubmit.scripts._validation.BasicConfig")
    mocked_basic_config.DATABASE_BACKEND = "sqlite"
    mocked_basic_config.DB_PATH = tmp_path / "database.db"
    mocked_basic_config.DB_PATH.touch()

    mocker.patch("autosubmit.job.manage.set_status", True)

    exp = autosubmit_exp(
        experiment_data={
            "CONFIG": {
                "AUTOSUBMIT_VERSION": "bright-opera",
            }
        }
    )
    args = [
        "-lc",
        "DEBUG",
        "-lf",
        "DEBUG",
        "setstatus",
        exp.expid,
        "-t",
        "WAITING",
        "-v",
    ]

    assert _autosubmit(args)


def test__init_logs_sqlite_mismatch_as_version(autosubmit_exp, mocker, tmp_path):
    """Test that an Autosubmit version mismatch is logged as an error."""
    mocked_basic_config = mocker.patch("autosubmit.scripts._validation.BasicConfig")
    mocked_basic_config.DATABASE_BACKEND = "sqlite"
    mocked_basic_config.DB_PATH = tmp_path / "database.db"
    mocked_basic_config.DB_PATH.touch()

    mocked_log = mocker.patch("autosubmit.scripts._initialise.Log")

    exp = autosubmit_exp(
        experiment_data={
            "CONFIG": {
                "AUTOSUBMIT_VERSION": "bright-opera",
            }
        }
    )
    args = ["-lc", "DEBUG", "-lf", "DEBUG", "setstatus", exp.expid, "-t", "WAITING"]

    with pytest.raises(SystemExit):
        _autosubmit(args)

    _assert_log_contains(
        mocked_log.error, "update the experiment version if you wish to continue"
    )


def test_install_sqlite_already_exists(monkeypatch, tmp_path, mocker):
    """Test that an existing SQLite database is reported as an error."""
    monkeypatch.setattr(BasicConfig, "DATABASE_BACKEND", "sqlite")

    db_file = tmp_path / "test.db"
    db_file.touch()

    monkeypatch.setattr(BasicConfig, "DB_PATH", str(db_file))

    mock_error = mocker.patch.object(Log, "error")

    install()

    mock_error.assert_called_once_with("Database already exists.")


def test_install_sqlite_create_db_fails(monkeypatch, tmp_path, mocker):
    """Test that failure to create the SQLite database is logged."""
    monkeypatch.setattr(BasicConfig, "DATABASE_BACKEND", "sqlite")

    db_file = tmp_path / "test.db"
    monkeypatch.setattr(BasicConfig, "DB_PATH", str(db_file))

    mocker.patch("autosubmit.install.create_db", return_value=False)
    mock_error = mocker.patch.object(Log, "error")

    install()

    _assert_log_contains(mock_error, "Can not write database file")


def test_install_sqlite_create_new_db(monkeypatch, tmp_path):
    """Test that a new SQLite database is created successfully."""
    monkeypatch.setattr(BasicConfig, "DATABASE_BACKEND", "sqlite")

    db_file = tmp_path / "test.db"
    monkeypatch.setattr(BasicConfig, "DB_PATH", str(db_file))

    install()

    assert db_file.exists()


def test_install_postgres_create_db_fails(monkeypatch, mocker):
    """Test that failure to create the PostgreSQL database is logged."""
    monkeypatch.setattr(BasicConfig, "DATABASE_BACKEND", "postgres")
    mocker.patch("autosubmit.install.create_db", return_value=False)

    mock_error = mocker.patch.object(Log, "error")

    install()

    _assert_log_contains(mock_error, "Failed to create Postgres database")


@pytest.mark.docker
@pytest.mark.postgres
def test_update_version(as_db: str, autosubmit_exp):
    """Test that an experiment with an outdated version can be updated."""
    wrong_version = "bright-opera"

    exp = autosubmit_exp(
        experiment_data={
            "CONFIG": {
                "AUTOSUBMIT_VERSION": wrong_version,
            }
        }
    )
    create(exp.expid, True, True)

    new_version = get_version()
    assert update_experiment_description_version(exp.expid, version=new_version)
    exp.as_conf.set_version(new_version)

    as_conf = AutosubmitConfig(
        exp.expid,
        BasicConfig,
        YAMLParserFactory(),
    )
    as_conf.reload(force_load=True)

    # TODO: We probably should test that the DB value is correct as well?
    assert as_conf.get_version() != wrong_version


@pytest.mark.docker
@pytest.mark.postgres
def test_update_description(as_db: str, autosubmit_exp):
    """Test that an experiment description can be updated."""
    wrong_version = "bright-opera"

    exp = autosubmit_exp(
        experiment_data={
            "CONFIG": {
                "AUTOSUBMIT_VERSION": wrong_version,
            }
        }
    )

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
    """Test parsing of job loop configuration data."""
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
    """
    exp = autosubmit_exp(include_jobs=True)

    assert not list(exp.status_dir.glob("*.txt")), "status/ should be empty before test"

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
    """Test that ``prepare_run`` returns the expected tuple."""
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
    """Test that ``prepare_run`` returns the expected tuple in recovery mode."""
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
    """Test that ``stop`` sets the scheduler exit flag."""
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
    """Test that ``monitor`` loads wrapper packages when requested."""
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
    """Test that ``set_status`` prints the job list when detail is requested."""
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
