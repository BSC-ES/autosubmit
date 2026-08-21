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

"""Utilities for logging command traceability information."""

import getpass
import os
import platform
import shlex
import socket
from collections.abc import Sequence
from datetime import datetime

from autosubmit.log.log import Log

SENSITIVE_OPTIONS = {
    "--password",
    "--passwd",
    "--token",
    "--secret",
    "--api-key",
    "--apikey",
    "--database-conn-url",
}
"""Options whose values should never be written to the log."""


def _redact_arguments(args: Sequence[str]) -> list[str]:
    """Redact values of sensitive command-line options.

    :param args: The command-line arguments.
    :return: The command-line arguments with sensitive option values redacted.
    """
    redacted: list[str] = []

    redact_next = False

    for arg in args:
        if redact_next:
            redacted.append("<REDACTED>")
            redact_next = False
            continue

        option = arg.split("=", 1)[0]

        if option in SENSITIVE_OPTIONS:
            redacted.append(option)
            if "=" in arg:
                redacted[-1] += "=<REDACTED>"
            else:
                redact_next = True
            continue

        redacted.append(arg)

    return redacted


def _format_command(args: Sequence[str]) -> str:
    """Format the command line for display in the log.

    :param args: The command-line arguments.
    :return: The formatted command line.
    """
    command = ["autosubmit", *_redact_arguments(args)]
    return shlex.join(command)


def log_command_context(args: Sequence[str]) -> None:
    """Log information useful for tracing an Autosubmit command execution.

    The command line is logged with sensitive option values redacted.

    :param args: The command-line arguments.
    """
    timezone = datetime.now().astimezone().tzname()
    timezone_offset = datetime.now().astimezone().strftime("%z")

    Log.info("Command traceability:")
    Log.info(f"  Command: {_format_command(args)}")
    Log.info(f"  Host: {socket.gethostname()}")
    Log.info(f"  User: {getpass.getuser()}")
    Log.info(f"  PID: {os.getpid()}")
    Log.info(f"  Python: {platform.python_version()}")
    Log.info(f"  Platform: {platform.platform()}")
    Log.info(f"  Timezone: {timezone} (UTC{timezone_offset[:3]}:{timezone_offset[3:]})")
    Log.info(f"  Working directory: {os.getcwd()}")
    Log.info("")
