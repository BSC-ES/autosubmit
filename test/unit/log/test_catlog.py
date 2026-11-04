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

import gzip
from pathlib import Path
from subprocess import DEVNULL

import pytest

from autosubmit.config.basicconfig import BasicConfig

# noinspection PyProtectedMember
from autosubmit.log.catlog import ViewMode, _view_file, cat_log
from autosubmit.log.log import AutosubmitCritical
from autosubmit.platforms.platform import Platform

_EXPID = "a000"


# NOTE: this fixture is marked to be auto-used, so that all the tests here
#       use the mocked configuration directories.
@pytest.fixture(autouse=True)
def as_conf(autosubmit_config):
    return autosubmit_config(_EXPID, {})


@pytest.fixture
def exp_path(as_conf) -> Path:
    return Path(BasicConfig.LOCAL_ROOT_DIR) / _EXPID


@pytest.fixture
def exp_logs_dir(exp_path, as_conf):
    exp_tmp_dir = exp_path / BasicConfig.LOCAL_TMP_DIR
    exp_logs_dir = exp_tmp_dir / f"LOG_{_EXPID}"
    return exp_logs_dir


@pytest.fixture
def aslogs_dir(exp_path, as_conf):
    exp_tmp_dir = exp_path / BasicConfig.LOCAL_TMP_DIR
    aslogs_dir = exp_tmp_dir / BasicConfig.LOCAL_ASLOG_DIR
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


@pytest.mark.parametrize("file_type,extension", [("o", "out"), ("e", "err")])
def test_job_uncompressed_cat(mocker, exp_logs_dir, file_type, extension):
    """Test cat_log displaying uncompressed .out / .err file with mode=ViewMode.CAT."""
    run = mocker.patch("autosubmit.log.catlog.subprocess.run")
    log_file = exp_logs_dir / f"{_EXPID}_SIM.20261104.{extension}"
    log_file.write_text("uncompressed log line 1\nuncompressed log line 2\n")
    run.return_value.returncode = 0

    assert cat_log(f"{_EXPID}_SIM", file=file_type, mode=ViewMode.CAT) is True

    run.assert_called_once_with(
        ["cat", str(log_file)],
        stdin=DEVNULL,
    )


@pytest.mark.parametrize("file_type,extension", [("o", "out"), ("e", "err")])
def test_job_uncompressed_tail(mocker, exp_logs_dir, file_type, extension):
    """Test cat_log displaying uncompressed .out / .err file with mode=ViewMode.TAIL."""
    popen = mocker.patch("autosubmit.log.catlog.subprocess.Popen")
    log_file = exp_logs_dir / f"{_EXPID}_SIM.20261104.{extension}"
    log_file.write_text("uncompressed log line 1\nuncompressed log line 2\n")
    popen.return_value.wait.return_value = 0

    assert cat_log(f"{_EXPID}_SIM", file=file_type, mode=ViewMode.TAIL) is True

    popen.assert_called_once_with(
        [
            "tail",
            "--lines=+1",
            "--retry",
            "--follow=name",
            str(log_file),
        ],
        stdin=DEVNULL,
    )


@pytest.mark.parametrize("file_type,extension", [("o", "out"), ("e", "err")])
def test_job_compressed_cat(capsys, mocker, exp_logs_dir, file_type, extension):
    """Test cat_log displaying .out.gz / .err.gz file with mode=ViewMode.CAT (transparent decompression)."""
    run = mocker.patch("autosubmit.log.catlog.subprocess.run")
    log_file = exp_logs_dir / f"{_EXPID}_SIM.20261104.{extension}.gz"
    content = "line 1: compressed archive\nline 2: decompressed output\n"
    with gzip.open(log_file, "wt", encoding="utf-8") as gz:
        gz.write(content)

    assert cat_log(f"{_EXPID}_SIM", file=file_type, mode=ViewMode.CAT) is True

    run.assert_not_called()
    captured = capsys.readouterr()
    assert captured.out == content


@pytest.mark.parametrize("file_type,extension", [("o", "out"), ("e", "err")])
def test_job_compressed_tail(capsys, mocker, exp_logs_dir, file_type, extension):
    """Test cat_log displaying .out.gz / .err.gz file with mode=ViewMode.TAIL (last 20 lines and notice)."""
    popen = mocker.patch("autosubmit.log.catlog.subprocess.Popen")
    mock_log = mocker.patch("autosubmit.log.catlog.Log")
    log_file = exp_logs_dir / f"{_EXPID}_SIM.20261104.{extension}.gz"
    lines = [f"archive log line {i:02d}\n" for i in range(1, 31)]
    with gzip.open(log_file, "wt", encoding="utf-8") as gz:
        gz.writelines(lines)

    assert cat_log(f"{_EXPID}_SIM", file=file_type, mode=ViewMode.TAIL) is True

    popen.assert_not_called()
    captured = capsys.readouterr()
    expected_output = "".join(lines[-20:])
    assert captured.out == expected_output
    mock_log.info.assert_called_once_with(
        "Note: Live follow (-m t / tail -f) is not supported on compressed archives."
    )


def test_platform_remote_log_compression_defaults():
    """Test remote gzip compression default settings and config overrides on Platform."""
    # Default without PLATFORMS config
    platform_default = Platform(expid=_EXPID, name="marenostrum5", config={})
    assert platform_default.compress_remote_logs is True
    assert platform_default.compression_level == 6

    # Override via platform configuration
    custom_config = {
        "PLATFORMS": {
            "MARENOSTRUM5": {
                "COMPRESS_REMOTE_LOGS": False,
                "COMPRESSION_LEVEL": 9,
            }
        }
    }
    platform_custom = Platform(expid=_EXPID, name="marenostrum5", config=custom_config)
    assert platform_custom.compress_remote_logs is False
    assert platform_custom.compression_level == 9
