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

"""Validators and code used for validating sub-commands.

Validators are functions that receive the command and options used.

Validators validate/check conditions, constraints, system settings, etc.,
and print useful and meaningful error messages, then exit the command
execution with a non-zero exit code.

Do not print exception tracebacks here unless that is useful to an
end user.

Please, be consistent with the other validation code. Print messages
in a similar format and tone (check punctuation). Use ``warnings``
to inform users of deprecated usage, and combine that with Sphinx
docs for developers.
"""

import os
import platform
import warnings
from collections.abc import Callable
from pathlib import Path
from re import split
from sys import exit

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.experiment.utils import (
    check_ownership,
    create_required_folders,
    experiment_exists,
    is_valid_experiment_id,
)
from autosubmit.log.log import AutosubmitCritical, Log
from autosubmit.scripts._args import ExpidOptions, OptionsT

__all__ = [
    "Validator",
    "validate_expid",
    "validate_expid_required_args",
    "validate_host_prohibited_commands",
    "validate_required_files",
]

Validator = Callable[[str, OptionsT], None]
"""Type for a validator function."""


def validate_required_files(command: str, opts: OptionsT) -> None:
    """Validate the system contains the required files for Autosubmit to function.

    :param command: The Autosubmit sub-command name.
    :param opts: The object with options.
    """
    if command == "configure":
        return

    if not BasicConfig.CONFIG_FILE_FOUND:
        Log.error(
            'Autosubmit configuration file "autosubmitrc" not found. '
            'Please run "autosubmit configure" to create it.'
        )
        exit(1)

    if command == "install" or BasicConfig.DATABASE_BACKEND != "sqlite":
        return

    db_path = Path(BasicConfig.DB_PATH)

    if not db_path.exists():
        Log.error(
            "Experiments database not found. "
            'Please run "autosubmit install" to create it.'
        )
        exit(1)

    if not os.access(db_path, os.R_OK):
        Log.error(
            f'Experiments database "{db_path}" is not readable. '
            "Please check the file permissions."
        )
        exit(1)

    if not os.access(db_path, os.W_OK):
        Log.error(
            f'Experiments database "{db_path}" is not writable. '
            "Please check the file permissions."
        )
        exit(1)


def _get_host_restrictions(restrictions: str | dict, command: str) -> list[str]:
    """Return host restrictions for a command.

    Legacy configurations use an empty string when no restrictions are
    configured. Treat that as no restrictions.

    >>> _get_host_restrictions("", "clean")
    []
    >>> _get_host_restrictions({}, "clean")
    []
    >>> _get_host_restrictions({"clean": ["ranma", "all"]}, "clean")
    ['ranma', 'all']
    >>> _get_host_restrictions({"run": ["ranma"]}, "clean")
    []
    """
    if not isinstance(restrictions, dict):
        return []

    return restrictions.get(command, [])


def validate_host_prohibited_commands(command: str, opts: OptionsT) -> None:
    """Validate that a command is not prohibited on the current host.

    .. deprecated::
        Kept for backward compatibility. Will be removed in the next major release.

    :param command: The Autosubmit sub-command name.
    :param opts: The object with options.
    """
    fqdn = platform.node()
    host = fqdn.split(".", 1)[0].split(",", 1)[0]

    denied_hosts = _get_host_restrictions(BasicConfig.DENIED_HOSTS, command)
    allowed_hosts = _get_host_restrictions(BasicConfig.ALLOWED_HOSTS, command)

    host_denied = "all" in denied_hosts or host in denied_hosts or fqdn in denied_hosts
    host_not_allowed = (
        allowed_hosts
        and "all" not in allowed_hosts
        and host not in allowed_hosts
        and fqdn not in allowed_hosts
    )

    if not host_denied and not host_not_allowed:
        return

    if host_denied:
        Log.error(
            f"Command '{command}' is not allowed on host '{host}'.\n"
            "The command is explicitly denied on this host."
        )
    else:
        Log.error(
            f"Command '{command}' is not allowed on host '{host}'.\n"
            f"Allowed hosts: {', '.join(allowed_hosts)}."
        )

    warnings.warn(
        "Host-based command restrictions are deprecated and will be removed "
        "in the next major release.",
        FutureWarning,
        stacklevel=2,
    )
    exit(1)


def validate_expid(command: str, opts: ExpidOptions) -> None:
    """Validate the experiment IDs given to an Autosubmit sub-command.

    Each experiment ID must:

    * be valid (alphanumeric and at least 4 characters long);
    * exist on disk and/or in the database;
    * be owned by the current user.

    If any condition is not met, an error message is printed and the command
    exits with a non-zero status.

    :param command: The Autosubmit sub-command name.
    :param opts: The object with options containing the experiment identifier.
    """
    expid = opts.expid
    if command == "stop" and not expid:
        return

    if command == "describe" and expid == "*":
        return

    expids = split(r"[,\s]+", expid.strip())

    if not opts.accepts_multiple_expids and len(expids) > 1:
        Log.error(f"The command '{command}' does not accept multiple experiment IDs.")
        exit(1)

    for expid in expids:
        if not is_valid_experiment_id(expid):
            Log.error(f"Invalid experiment ID: '{expid}'")
            exit(1)

        if not experiment_exists(expid):
            if BasicConfig.DATABASE_BACKEND != "sqlite":
                # TODO: Non-SQLite experiments may not have their local directory structure yet.
                #       Create it as a compatibility workaround until experiment configuration can
                #       be loaded directly from version control. See #1352.
                create_required_folders(
                    expid,
                    Path(BasicConfig.LOCAL_ROOT_DIR, expid),
                )

            Log.error(f"Experiment '{expid}' was not found.")
            exit(1)

        try:
            check_ownership(expid)
        except AutosubmitCritical as error:
            Log.error(f"Experiment '{expid}' is owned by a different user: {error}")
            exit(1)


def validate_expid_required_args(command: str, opts: ExpidOptions) -> None:
    """Validate the arguments given when creating a new experiment.

    :param command: The Autosubmit sub-command name.
    :param opts: The object with options.
    """
    if opts.description is None or not opts.description.strip():
        Log.error("You must provide an experiment description (-d/--description)")
        exit(1)

    if opts.HPC is None or not opts.HPC.strip():
        Log.error("You must provide an HPC (-H/--HPC)")
        exit(1)
