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

_GLOBAL_LOG_COMMANDS = {"archive", "delete", "describe", "unarchive", "upgrade"}
"""List of commands that are considered "global", or do not have a single experiment associated.

The term "global" predates this script, and its meaning is not so intuitive.
So some examples here may help understand how this is used.

The archive command has a single experiment identifier associated. However, when
you execute archive, the experiment files (including logs) would eventually be moved
at some point. To avoid conflicts while running the command, we log the output of
archive in the "global" log files (i.e., log files that are not in the experiment
folder, but somewhere in the system, configured by sysadmins or ``~/autosubmit/logs``).

The stop command may have one or more experiment identifiers associated with its call.
It is NOT a global command, so that when you do an ``autosubmit stop a000``, the stop
logs are written to the experiment logs folder. But if you do an ``stop a000,a001``,
then it will write the logs to the global logs folder. It was designed like this initially
as it would be difficult to write logs in each experiment folder (with a single log handler).

The delete command may have one or more scripts. But different than the stop command, here
this is treated as a global for the reason that if we wrote the logs in the experiment
folder, then the logs would be immediately deleted with the experiment files, and the
logs would never be accessible again.

So the logic here is:

* A command takes multiple expids -> global logs folder
* The logs must exist with or without the experiment directory -> global logs folder
* The logs belong to an experiment -> experiment logs folder, not global
"""

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

    :param command: Name of the command being executed.
    :param expids: List of experiment IDs.
    :return: The filename stem for a global log.
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
