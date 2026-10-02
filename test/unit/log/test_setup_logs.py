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

"""Tests for Autosubmit log file setup."""

from pathlib import Path
from stat import S_IWGRP
from typing import TYPE_CHECKING

import pytest

import autosubmit.log as log_setup
from autosubmit.config.basicconfig import BasicConfig

# noinspection PyProtectedMember
from autosubmit.log import _global_log_name, setup_log_files
from autosubmit.log.log import Log

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


@pytest.fixture
def local_root(tmp_path: Path, mocker: "MockerFixture") -> Path:
    """Configure the local Autosubmit root directory for a test.

    :param tmp_path: Temporary directory provided by pytest.
    :param mocker: pytest-mock fixture used to patch ``BasicConfig``.
    :return: The configured local Autosubmit root directory.
    """
    mocker.patch.object(
        BasicConfig,
        "LOCAL_ROOT_DIR",
        str(tmp_path),
    )
    return tmp_path


@pytest.fixture
def global_log_dir(tmp_path: Path, mocker: "MockerFixture") -> Path:
    """Configure the global Autosubmit log directory for a test.

    :param tmp_path: Temporary directory provided by pytest.
    :param mocker: pytest-mock fixture used to patch ``BasicConfig``.
    :return: The configured global Autosubmit log directory.
    """
    path = tmp_path / "global_logs"

    mocker.patch.object(
        BasicConfig,
        "GLOBAL_LOG_DIR",
        str(path),
    )

    return path


@pytest.fixture
def experiment_path(local_root: Path) -> Path:
    """Create and return an experiment directory.

    :param local_root: Local Autosubmit root directory.
    :return: The path to the test experiment directory.
    """
    exp_path = local_root / "a001"
    exp_path.mkdir(parents=True)
    return exp_path


@pytest.mark.parametrize("command", ["configure", "install"])
def test_setup_log_files_installation_commands_do_nothing(
    command: str,
    mocker: "MockerFixture",
) -> None:
    """Test that installation commands do not configure log files.

    :param command: Autosubmit command being tested.
    :param mocker: pytest-mock fixture used to patch log setup functions.
    """
    set_log_files = mocker.patch.object(
        log_setup,
        "_set_log_files",
    )

    setup_log_files(command, [])

    set_log_files.assert_not_called()


@pytest.mark.parametrize("command", ["archive", "unarchive", "upgrade"])
def test_setup_log_files_global_commands_use_global_log_directory(
    command: str,
    global_log_dir: Path,
    mocker: "MockerFixture",
) -> None:
    """Test that global commands use the global log directory.

    :param command: Autosubmit command being tested.
    :param global_log_dir: Expected global log directory.
    :param mocker: pytest-mock fixture used to patch log setup functions.
    """
    set_log_files = mocker.patch.object(
        log_setup,
        "_set_log_files",
    )

    setup_log_files(command, [])

    set_log_files.assert_called_once_with(
        global_log_dir,
        command,
    )


def test_setup_log_files_without_expid_uses_global_log_directory(
    global_log_dir: Path,
    mocker: "MockerFixture",
) -> None:
    """Test that commands without an experiment ID use global logs.

    :param global_log_dir: Expected global log directory.
    :param mocker: pytest-mock fixture used to patch log setup functions.
    """
    set_log_files = mocker.patch.object(
        log_setup,
        "_set_log_files",
    )

    setup_log_files("clean", [])

    set_log_files.assert_called_once_with(
        global_log_dir,
        "clean",
    )


def test_setup_log_files_multiple_expids_use_global_log_directory(
    global_log_dir: Path,
    mocker: "MockerFixture",
) -> None:
    """Test that multiple experiment IDs use the global log directory.

    The experiment IDs are included in the generated global log name.

    :param global_log_dir: Expected global log directory.
    :param mocker: pytest-mock fixture used to patch log setup functions.
    """
    set_log_files = mocker.patch.object(
        log_setup,
        "_set_log_files",
    )

    setup_log_files("clean", ["a001", "a002"])

    set_log_files.assert_called_once_with(
        global_log_dir,
        "clean_a001_a002",
    )


def test_setup_log_files_star_expid_is_ignored(
    global_log_dir: Path,
    mocker: "MockerFixture",
) -> None:
    """Test that a special ``*`` experiment ID is ignored.

    The special ID must not be included when constructing global log names.

    :param global_log_dir: Expected global log directory.
    :param mocker: pytest-mock fixture used to patch log setup functions.
    """
    set_log_files = mocker.patch.object(
        log_setup,
        "_set_log_files",
    )

    setup_log_files("clean", ["*"])

    set_log_files.assert_called_once_with(
        global_log_dir,
        "clean",
    )


def test_setup_log_files_star_expid_does_not_count_as_expid(
    local_root: Path,
    mocker: "MockerFixture",
) -> None:
    """Test that ``*`` does not count as an experiment ID.

    A real experiment ID following ``*`` is still handled normally.

    :param local_root: Local Autosubmit root directory.
    :param mocker: pytest-mock fixture used to patch log setup functions.
    """
    exp_path = local_root / "a001"
    exp_path.mkdir(parents=True)

    get_owner = mocker.patch.object(
        log_setup,
        "get_experiment_owner",
        return_value=(None, None, True, None),
    )
    set_log_files = mocker.patch.object(
        log_setup,
        "_set_log_files",
    )

    setup_log_files("clean", ["*", "a001"])

    get_owner.assert_called_once_with("a001")

    set_log_files.assert_called_once_with(
        exp_path / "tmp" / "ASLOGS",
        "clean",
    )


def test_setup_log_files_owner_uses_aslogs_directory(
    experiment_path: Path,
    mocker: "MockerFixture",
) -> None:
    """Test that an experiment owner uses the experiment ASLOGS directory.

    :param experiment_path: Path to the test experiment.
    :param mocker: pytest-mock fixture used to patch log setup functions.
    """
    mocker.patch.object(
        log_setup,
        "get_experiment_owner",
        return_value=(None, None, True, None),
    )

    set_log_files = mocker.patch.object(
        log_setup,
        "_set_log_files",
    )

    setup_log_files("clean", ["a001"])

    set_log_files.assert_called_once_with(
        experiment_path / "tmp" / "ASLOGS",
        "clean",
    )


def test_setup_log_files_non_owner_with_writable_tmp_uses_local_logs(
    experiment_path: Path,
    mocker: "MockerFixture",
) -> None:
    """Test that a non-owner with writable tmp uses local logs.

    :param experiment_path: Path to the test experiment.
    :param mocker: pytest-mock fixture used to patch filesystem and log setup.
    """
    experiment_tmp = experiment_path / "tmp"

    mocker.patch.object(
        log_setup,
        "get_experiment_owner",
        return_value=(None, None, False, None),
    )

    mocker.patch.object(
        Path,
        "stat",
        autospec=True,
        return_value=mocker.Mock(st_mode=S_IWGRP),
    )

    set_log_files = mocker.patch.object(
        log_setup,
        "_set_log_files",
    )

    setup_log_files("clean", ["a001"])

    set_log_files.assert_called_once_with(
        experiment_tmp,
        "clean",
    )


def test_setup_log_files_non_owner_with_non_writable_tmp_uses_global_logs(
    experiment_path: Path,
    global_log_dir: Path,
    mocker: "MockerFixture",
) -> None:
    """Test that a non-owner with non-writable tmp uses global logs.

    :param experiment_path: Path to the test experiment.
    :param global_log_dir: Expected global log directory.
    :param mocker: pytest-mock fixture used to patch filesystem and log setup.
    """
    mocker.patch.object(
        log_setup,
        "get_experiment_owner",
        return_value=(None, None, False, None),
    )

    mocker.patch.object(
        Path,
        "stat",
        autospec=True,
        return_value=mocker.Mock(st_mode=0),
    )

    set_log_files = mocker.patch.object(
        log_setup,
        "_set_log_files",
    )

    setup_log_files("clean", ["a001"])

    set_log_files.assert_called_once_with(
        global_log_dir,
        "clean_a001",
    )


def test_setup_log_files_non_owner_with_non_writable_tmp_logs_warning(
    experiment_path: Path,
    global_log_dir: Path,
    mocker: "MockerFixture",
) -> None:
    """Test that a warning is logged when a non-owner cannot write to tmp.

    The warning identifies the affected directory, its permissions, and the
    global directory that will be used for logging.

    :param experiment_path: Path to the test experiment.
    :param global_log_dir: Expected global log directory.
    :param mocker: pytest-mock fixture used to patch filesystem and logging.
    """
    mocker.patch.object(
        log_setup,
        "get_experiment_owner",
        return_value=(None, None, False, None),
    )

    mocker.patch.object(
        Path,
        "stat",
        autospec=True,
        return_value=mocker.Mock(st_mode=0o755),
    )

    mocker.patch.object(
        log_setup,
        "_set_log_files",
    )

    log_info = mocker.patch.object(
        Log,
        "info",
    )

    setup_log_files("clean", ["a001"])

    log_info.assert_called_once_with(
        f"Permissions of {experiment_path / 'tmp'} are {oct(0o755)}. "
        f"The log is being written to {global_log_dir} instead. "
        "Please ask the owner to fix the permissions."
    )


def test_setup_log_files_owner_run_creates_status_log_files(
    experiment_path: Path,
    mocker: "MockerFixture",
) -> None:
    """Test that the ``run`` command creates both status log files.

    :param experiment_path: Path to the test experiment.
    :param mocker: pytest-mock fixture used to patch log setup functions.
    """
    mocker.patch.object(
        log_setup,
        "get_experiment_owner",
        return_value=(None, None, True, None),
    )

    mocker.patch.object(
        log_setup,
        "_set_log_files",
    )

    set_file = mocker.patch.object(
        Log,
        "set_file",
    )

    setup_log_files("run", ["a001"])

    aslogs_path = experiment_path / "tmp" / "ASLOGS"

    assert set_file.call_count == 2

    set_file.assert_any_call(
        str(aslogs_path / "jobs_active_status.log"),
        "status",
    )

    set_file.assert_any_call(
        str(aslogs_path / "jobs_failed_status.log"),
        "status_failed",
    )


def test_setup_log_files_owner_non_run_does_not_create_status_files(
    experiment_path: Path,
    mocker: "MockerFixture",
) -> None:
    """Test that non-``run`` commands do not create status log files.

    :param experiment_path: Path to the test experiment.
    :param mocker: pytest-mock fixture used to patch log setup functions.
    """
    mocker.patch.object(
        log_setup,
        "get_experiment_owner",
        return_value=(None, None, True, None),
    )

    mocker.patch.object(
        log_setup,
        "_set_log_files",
    )

    set_file = mocker.patch.object(
        Log,
        "set_file",
    )

    setup_log_files("clean", ["a001"])

    set_file.assert_not_called()


def test_setup_log_files_owner_run_removes_existing_status_files(
    experiment_path: Path,
    mocker: "MockerFixture",
) -> None:
    """Test that ``run`` removes existing status files before recreating them.

    :param experiment_path: Path to the test experiment.
    :param mocker: pytest-mock fixture used to patch log setup functions.
    """
    aslogs_path = experiment_path / "tmp" / "ASLOGS"
    aslogs_path.mkdir(parents=True)

    active_status = aslogs_path / "jobs_active_status.log"
    failed_status = aslogs_path / "jobs_failed_status.log"

    active_status.touch()
    failed_status.touch()

    mocker.patch.object(
        log_setup,
        "get_experiment_owner",
        return_value=(None, None, True, None),
    )

    mocker.patch.object(
        log_setup,
        "_set_log_files",
    )

    mocker.patch.object(
        Log,
        "set_file",
    )

    setup_log_files("run", ["a001"])

    assert not active_status.exists()
    assert not failed_status.exists()


def test_setup_log_files_owner_status_chmod_failure_is_ignored(
    experiment_path: Path,
    mocker: "MockerFixture",
) -> None:
    """Test that a chmod failure for the experiment status file is ignored.

    :param experiment_path: Path to the test experiment.
    :param mocker: pytest-mock fixture used to patch filesystem operations.
    """
    status_path = experiment_path / "status"

    mocker.patch.object(
        log_setup,
        "get_experiment_owner",
        return_value=(None, None, True, None),
    )

    mocker.patch.object(
        log_setup,
        "_set_log_files",
    )

    chmod = mocker.patch.object(
        Path,
        "chmod",
        autospec=True,
    )

    def chmod_side_effect(self: Path, mode: int) -> None:
        """Raise an error when chmod is called for the status file.

        :param self: Path whose permissions are being changed.
        :param mode: Permission mode passed to ``chmod``.
        :raises OSError: When the target is the experiment status file.
        """
        if self == status_path:
            raise OSError("permission denied")

    chmod.side_effect = chmod_side_effect

    setup_log_files("clean", ["a001"])

    chmod.assert_any_call(status_path, 0o775)


def test_set_log_files_uses_configured_file_log_level(
    mocker: "MockerFixture",
) -> None:
    """Test that the configured file log level is passed to the output log.

    :param mocker: pytest-mock fixture used to patch the log configuration.
    """
    set_file = mocker.patch.object(
        Log,
        "set_file",
    )

    mocker.patch.object(
        Log,
        "file_log_level",
        "DEBUG",
    )

    path = Path("/tmp/logs")

    log_setup._set_log_files(path, "clean")

    set_file.assert_any_call(
        str(path / "clean.log"),
        "out",
        Log.file_log_level,
    )

    set_file.assert_any_call(
        str(path / "clean_err.log"),
        "err",
    )


@pytest.mark.parametrize(
    ("command", "expids", "expected"),
    [
        ("stop", [], "stop"),
        ("stop", ["a001"], "stop_a001"),
        ("stop", ["a001", "a002"], "stop_a001_a002"),
        ("archive", ["a001"], "archive_a001"),
    ],
)
def test_global_log_name(
    command: str,
    expids: list[str],
    expected: str,
) -> None:
    """Test that global log names contain the expected command and IDs.

    :param command: Autosubmit command used to construct the log name.
    :param expids: Experiment IDs included in the log name.
    :param expected: Expected global log filename stem.
    """
    assert _global_log_name(command, expids) == expected
