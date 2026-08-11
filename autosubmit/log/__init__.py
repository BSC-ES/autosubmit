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

_GLOBAL_LOG_COMMANDS = {"archive", "unarchive", "upgrade"}
"""List of commands that are considered "global", and do not have a single experiment associated."""

_INSTALLATION_COMMANDS = ["configure", "install"]
"""List of installation commands."""


def _set_log_files(path: Path, name: str) -> None:
    """Set up standard output and error log files."""
    Log.set_file(str(path / f"{name}.log"), "out", Log.file_log_level)
    Log.set_file(str(path / f"{name}_err.log"), "err")


def _setup_experiment_log_files(
    command: str,
    expid: str,
    owner: bool,
    tmp_path: Path,
    aslogs_path: Path,
    exp_path: Path,
) -> None:
    """Set up log files for an experiment."""
    tmp_path.mkdir(mode=0o775, exist_ok=True)
    aslogs_path.mkdir(mode=0o775, exist_ok=True)

    if owner:
        tmp_path.chmod(0o775)

        with suppress(OSError):
            (exp_path / "status").chmod(0o775)

        _set_log_files(aslogs_path, command)

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
        _set_log_files(tmp_path, command)
        return

    global_log_dir = Path(BasicConfig.GLOBAL_LOG_DIR)
    _set_log_files(global_log_dir, f"{command}_{expid}")

    permissions = oct(mode & 0o777)
    Log.info(
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


def setup_log_files(command: str, expids: list[str]) -> None:
    """Set up log files and permissions for the given command and experiment.

    The ``log_level`` is the level used for logging only by the file. For
    the console logging level, it is set elsewhere. It is a difference from
    the old AS4 original code, which set up log files, permissions, and the
    console level -- now, this function only sets up log files (as the name
    suggests).

    The console log is now set right at the entry console script.
    Permissions are now set during installation, in the function
    ``create_required_directories`` of ``autosubmit.install`` module.
    Permissions are not changed here any more. It is up to a sysadmin to
    choose if the permissions must be changed, at their own risk.

    The log file logging level is defined earlier. The log level is specified
    by the user, only via the entry console script ``autosubmit``. There, the
    code defines a ``Log.file_log_level`` property with the file logging level.

    :param command: Name of the command being executed.
    :param expids: List of experiment IDs.
    """
    # Commands that set up Autosubmit (no log files available until we install the tool).
    if command in _INSTALLATION_COMMANDS:
        return

    # Some experiment identifiers have special meanings in the CLI. We ignore those here.
    expids = [e for e in expids if e != "*"]

    # Commands that take an expid, but are handled differently, or commands without an expid.
    if command in _GLOBAL_LOG_COMMANDS or len(expids) != 1:
        global_log_name = _global_log_name(command, expids)
        _set_log_files(
            Path(BasicConfig.GLOBAL_LOG_DIR),
            global_log_name,
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
    )
