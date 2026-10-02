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

from pathlib import Path
from subprocess import DEVNULL

import pytest

# noinspection PyProtectedMember
from autosubmit.log.catlog import _view_file, cat_log
from autosubmit.log.log import AutosubmitCritical

_EXPID = "a000"


# NOTE: this fixture is marked to be auto-used, so that all the tests here
#       use the mocked configuration directories.
@pytest.fixture(autouse=True)
def as_conf(autosubmit_config):
    return autosubmit_config(_EXPID, {})


@pytest.fixture
def exp_path(as_conf) -> Path:
    return Path(as_conf.basic_config.LOCAL_ROOT_DIR) / _EXPID


@pytest.fixture
def exp_logs_dir(exp_path, as_conf):
    exp_tmp_dir = exp_path / as_conf.basic_config.LOCAL_TMP_DIR
    exp_logs_dir = exp_tmp_dir / f"LOG_{_EXPID}"
    return exp_logs_dir


@pytest.fixture
def aslogs_dir(exp_path, as_conf):
    exp_tmp_dir = exp_path / as_conf.basic_config.LOCAL_TMP_DIR
    aslogs_dir = exp_tmp_dir / as_conf.basic_config.LOCAL_ASLOG_DIR
    return aslogs_dir


@pytest.fixture
def status_path(exp_path, as_conf):
    status_path = exp_path / "status"
    status_path.mkdir(exist_ok=True)
    return status_path


def test_invalid_file():
    def _fn():
        cat_log(None, "8", None)

    pytest.raises(AutosubmitCritical, _fn)


def test_invalid_mode():
    def _fn():
        cat_log(None, "o", "8")

    pytest.raises(AutosubmitCritical, _fn)


# -- workflow


def test_is_workflow_invalid_file():
    def _fn():
        cat_log(_EXPID, "j", None)

    pytest.raises(AutosubmitCritical, _fn)


def test_is_workflow_not_found(mocker):
    mocked_log = mocker.patch("autosubmit.log.catlog.Log")
    cat_log(_EXPID, "o", "c")
    assert mocked_log.info.called
    assert mocked_log.info.call_args[0][0] == "No logs found."


def test_is_workflow_log_is_dir(aslogs_dir):
    log_file_actually_dir = aslogs_dir / "log_run.log"
    log_file_actually_dir.mkdir()

    def _fn():
        cat_log(_EXPID, "o", "c")

    pytest.raises(AutosubmitCritical, _fn)


def test_is_workflow_out_cat(mocker, aslogs_dir):
    """Test that the workflow output log is displayed using ``cat``.

    The default log type is the workflow output log, and ``cat`` mode should
    invoke ``subprocess.run`` with the matching log path.
    """
    run = mocker.patch("autosubmit.log.catlog.subprocess.run")
    log_file = Path(aslogs_dir, "log_run.log")

    if log_file.is_dir():
        log_file.rmdir()

    log_file.write_text("as test")

    run.return_value.returncode = 0

    assert cat_log(_EXPID, file=None, mode="c") is True

    run.assert_called_once_with(
        ["cat", str(log_file)],
        stdin=DEVNULL,
    )


def test_is_workflow_status_tail(mocker, status_path):
    popen = mocker.patch("subprocess.Popen")
    log_file = Path(status_path, f"{_EXPID}_anything.txt")
    with open(log_file, "w") as f:
        f.write("as test")
        f.flush()
        cat_log(_EXPID, file="s", mode="t")
        assert popen.called
        args = popen.call_args[0][0]
        assert args[0] == "tail"
        assert str(args[-1]) == str(log_file)


# --- jobs


@pytest.mark.parametrize("file", ["s", "o"])
def test_is_workflow_log_not_found(mocker, file):
    """Test that no message is displayed when a workflow log is missing."""
    mocked_log = mocker.patch("autosubmit.log.catlog.Log")

    cat_log(_EXPID, file=file, mode="c")

    mocked_log.info.assert_called_once_with("No logs found.")


def test_is_workflow_job_log_invalid(mocker):
    """Test that requesting a job log for a workflow raises an error."""
    mocker.patch("autosubmit.log.catlog.Log")

    with pytest.raises(
        AutosubmitCritical,
        match="workflow logs only support",
    ):
        cat_log(_EXPID, file="j", mode="c")


def test_is_jobs_log_is_dir(exp_logs_dir):
    log_file_actually_dir = exp_logs_dir / f"{_EXPID}_INI.20000101.out"
    log_file_actually_dir.mkdir()

    def _fn():
        cat_log(f"{_EXPID}_INI", "o", "c")

    pytest.raises(AutosubmitCritical, _fn)


def test_is_jobs_out_tail(mocker, exp_logs_dir):
    popen = mocker.patch("subprocess.Popen")
    log_file = Path(exp_logs_dir, f"{_EXPID}_INI.20200101.out")
    if log_file.is_dir():  # dir is created in previous test
        log_file.rmdir()
    with open(log_file, "w") as f:
        f.write("as test")
        f.flush()
        cat_log(f"{_EXPID}_INI", file=None, mode="t")
        assert popen.called
        args = popen.call_args[0][0]
        assert args[0] == "tail"
        assert str(args[-1]) == str(log_file)


def test_is_workflow_error_not_found(mocker):
    """Test that no message is displayed when the workflow error log is missing."""
    mocked_log = mocker.patch("autosubmit.log.catlog.Log")

    cat_log(_EXPID, file="e", mode="c")

    mocked_log.info.assert_called_once_with("No logs found.")


def test_is_job_log_file(mocker, exp_logs_dir):
    """Test that the job command file is displayed."""
    run = mocker.patch("autosubmit.log.catlog.subprocess.run")
    log_file = exp_logs_dir / f"{_EXPID}_INI.cmd"
    log_file.write_text("as test")

    run.return_value.returncode = 0

    assert (
        cat_log(
            f"{_EXPID}_INI",
            file="j",
            mode="c",
        )
        is True
    )

    run.assert_called_once_with(
        ["cat", str(log_file)],
        stdin=DEVNULL,
    )


def test_is_job_status_file(mocker, exp_logs_dir):
    """Test that the job status file is displayed."""
    run = mocker.patch("autosubmit.log.catlog.subprocess.run")
    log_file = exp_logs_dir / f"{_EXPID}_INI_TOTAL_STATS"
    log_file.write_text("as test")

    run.return_value.returncode = 0

    assert (
        cat_log(
            f"{_EXPID}_INI",
            file="s",
            mode="c",
        )
        is True
    )

    run.assert_called_once_with(
        ["cat", str(log_file)],
        stdin=DEVNULL,
    )


def test_is_job_log_not_found(mocker):
    """Test that no message is displayed when no matching job output log exists."""
    mocked_log = mocker.patch("autosubmit.log.catlog.Log")

    cat_log(
        f"{_EXPID}_INI",
        file="o",
        mode="c",
    )

    mocked_log.info.assert_called_once_with("No logs found.")


def test_is_job_log_missing_explicit_file(mocker, exp_logs_dir):
    """Test that a missing explicit job log path is passed to the viewer."""
    run = mocker.patch("autosubmit.log.catlog.subprocess.run")
    log_file = exp_logs_dir / f"{_EXPID}_INI.cmd"

    run.return_value.returncode = 0

    assert (
        cat_log(
            f"{_EXPID}_INI",
            file="j",
            mode="c",
        )
        is True
    )

    run.assert_called_once_with(
        ["cat", str(log_file)],
        stdin=DEVNULL,
    )


def test_is_workflow_status_tail_keyboard_interrupt(mocker, status_path):
    """Test that interrupting workflow status tailing terminates the process."""
    popen = mocker.patch("autosubmit.log.catlog.subprocess.Popen")
    log_file = status_path / f"{_EXPID}_anything.txt"
    log_file.write_text("as test")

    process = popen.return_value
    process.wait.side_effect = [KeyboardInterrupt, 0]

    assert (
        cat_log(
            _EXPID,
            file="s",
            mode="t",
        )
        is True
    )

    process.terminate.assert_called_once_with()
    assert process.wait.call_count == 2


def test_view_file_invalid_mode():
    """Test that an invalid visualisation mode raises ValueError."""
    with pytest.raises(
        ValueError,
        match="Invalid cat-log visualisation mode",
    ):
        _view_file(
            Path("/tmp/test.log"),
            "invalid",  # type: ignore[arg-type]
        )


def test_is_job_command_file_missing(mocker, exp_logs_dir):
    """Test that a missing job command file is passed to the viewer."""
    run = mocker.patch("autosubmit.log.catlog.subprocess.run")
    log_file = exp_logs_dir / f"{_EXPID}_INI.cmd"

    run.return_value.returncode = 0

    assert (
        cat_log(
            f"{_EXPID}_INI",
            file="j",
            mode="c",
        )
        is True
    )

    run.assert_called_once_with(
        ["cat", str(log_file)],
        stdin=DEVNULL,
    )
