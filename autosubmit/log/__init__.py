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

"""Autosubmit logging and exceptions."""

from contextlib import suppress
from pathlib import Path
from stat import S_IWGRP, S_IWOTH

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.experiment.utils import get_experiment_owner
from autosubmit.log.log import Log

__all__ = ["setup_log_files"]

_GLOBAL_LOG_COMMANDS = {"archive", "upgrade"}
"""List of commands that are considered "global", and do not have a single experiment associated."""

_INSTALLATION_COMMANDS = ["configure", "install"]
"""List of installation commands."""


def _set_log_files(path: Path, name: str, log_level: str) -> None:
    """Set up standard output and error log files."""
    Log.set_file(str(path / f"{name}.log"), "out", log_level)
    Log.set_file(str(path / f"{name}_err.log"), "err")


def _setup_experiment_log_files(
    command: str,
    expid: str,
    owner: bool,
    tmp_path: Path,
    aslogs_path: Path,
    exp_path: Path,
    log_level: str,
) -> None:
    """Set up log files for an experiment."""
    tmp_path.mkdir(mode=0o775, exist_ok=True)
    aslogs_path.mkdir(mode=0o775, exist_ok=True)

    if owner:
        tmp_path.chmod(0o775)

        with suppress(OSError):
            (exp_path / "status").chmod(0o775)

        _set_log_files(aslogs_path, command, log_level)

        if command == "run":
            for filename in (
                "jobs_active_status.log",
                "jobs_failed_status.log",
            ):
                with suppress(FileNotFoundError):
                    (aslogs_path / filename).unlink()

            Log.set_file(
                str(aslogs_path / "jobs_active_status.log"),
                "status",
            )
            Log.set_file(
                str(aslogs_path / "jobs_failed_status.log"),
                "status_failed",
            )
        return

    mode = tmp_path.stat().st_mode

    if mode & (S_IWGRP | S_IWOTH):
        _set_log_files(tmp_path, command, log_level)
        return

    global_log_dir = Path(BasicConfig.GLOBAL_LOG_DIR)
    _set_log_files(global_log_dir, f"{command}{expid}", log_level)

    permissions = oct(mode & 0o777)
    Log.printlog(
        f"Permissions of {tmp_path} are {permissions}. "
        f"The log is being written to {global_log_dir} instead. "
        "Please ask the owner to fix the permissions."
    )


def _global_log_name(command: str, expids: list[str]) -> str:
    """Return the filename stem for a global log.

    >>> _global_log_name("stop", [])
    'stop'
    >>> _global_log_name("stop", ["a001"])
    'stop_a001'
    >>> _global_log_name("stop", ["a001", "a002"])
    'stop_a001_a002'
    >>> _global_log_name("archive", ["a001"])
    'archive_a001'
    """
    suffix = f"_{'_'.join(expids)}" if expids else ""
    return f"{command}{suffix}"


def setup_log_files(
    command: str,
    expids: list[str],
    log_level: str,
    console_level: str = "DEBUG",
) -> None:
    """Set up log files and permissions for the given command and experiment.

    :param command: Name of the command being executed.
    :param expids: List of experiment IDs.
    :param log_level: Log level for file output.
    :param console_level: Log level for console output.
    """
    Log.set_console_level(console_level)

    # Commands that set up Autosubmit (no log files available until we install the tool).
    if command in _INSTALLATION_COMMANDS:
        return

    # Some experiment identifiers have special meanings in the CLI. We ignore those here.
    expids = [e for e in expids if e != "*"]

    # Commands that take an expid, but are handled differently, or commands without an expid.
    if command in _GLOBAL_LOG_COMMANDS or len(expids) != 1:
        suffix = f"_{'_'.join(expids)}" if expids else ""
        _set_log_files(
            Path(BasicConfig.GLOBAL_LOG_DIR),
            f"{command}{suffix}",
            log_level,
        )
        return

    expid = expids[0]

    exp_path = Path(BasicConfig.LOCAL_ROOT_DIR, expid)
    tmp_path = exp_path / "tmp"
    aslogs_path = tmp_path / "ASLOGS"

    # TBD: Maybe ``is_owner`` can be dropped? We probably already
    #      know the user is the owner because of the validators? For later...
    _, _, is_owner, _ = get_experiment_owner(expid)

    _setup_experiment_log_files(
        command,
        expid,
        is_owner,
        tmp_path,
        aslogs_path,
        exp_path,
        log_level,
    )
