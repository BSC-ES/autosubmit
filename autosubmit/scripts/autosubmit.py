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

"""autosubmit

Examples:

    # Display help for this autosubmit command
    $ autosubmit --help

    # Display help for a sub-command
    $ autosubmit clean --help

    # Run a sub-command
    $ autosubmit -lc DEBUG clean a000 --project
"""

from argparse import ArgumentParser
from sys import argv, exit

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.helpers.version import get_version
from autosubmit.log.log import Log
from autosubmit.scripts._args import create_argparse_parser
from autosubmit.scripts._entry_points import execute_cmd, get_commands, iter_commands
from autosubmit.scripts._terminal import print_contents, supports_color

if supports_color():
    CYAN = "\033[36m"
    ORANGE = "\033[33m"
    RESET = "\033[0m"
else:
    CYAN = ""
    ORANGE = ""
    RESET = ""


_HIDDEN_SUBCOMMANDS = ["manpages"]


def _get_header() -> str:
    """Command-line terminal header."""
    return (
        f"{CYAN}   ##{RESET}    {ORANGE}####{RESET}\n"
        f"{CYAN}  #  #{RESET}  {ORANGE}#   {RESET}     Autosubmit Experiment and Workflow Manager {get_version()}\n"
        f"{CYAN} ######{RESET}  {ORANGE}####{RESET}    Copyright (C) 2011-2026 Barcelona Supercomputing Center\n"
        f"{CYAN} #    #{RESET}  {ORANGE}    #{RESET}   (BSC) & Contributors\n"
        f"{CYAN} #    #{RESET} {ORANGE}#####{RESET}"
    )


def get_arg_parser() -> ArgumentParser:
    """Create the top-level argument parser.

    This parser intentionally only handles arguments belonging to
    ``autosubmit`` itself. Arguments belonging to a sub-command are
    forwarded untouched to that command.

    :return: The argument parser.
    """
    parser = create_argparse_parser(__doc__, add_help=False)
    parser.add_argument(
        "--help", "-h", action="store_true", default=False, dest="help_"
    )
    parser.add_argument(
        "--version", "-v", action="store_true", default=False, dest="version"
    )
    log_levels = ("NO_LOG", "INFO", "WARNING", "DEBUG", "ERROR")
    parser.add_argument(
        "-lf",
        "--logfile",
        choices=log_levels,
        default="DEBUG",
        type=str,
        help="sets the log level for the log file",
    )
    parser.add_argument(
        "-lc",
        "--logconsole",
        choices=log_levels,
        default="WARNING",
        type=str,
        help="sets the log level for the console",
    )
    return parser


def cli_version() -> None:
    """Print the version of Autosubmit."""
    Log.info(get_version())


def cli_help() -> None:
    """Print the ``autosubmit`` command help.

    Prints the list of Autosubmit sub-commands, found via the Python
    entry-point "autosubmit.command". See ``pyproject.toml``.
    """
    print()
    print(_get_header())
    print()
    print("Autosubmit is open-source software (GPL-3.0) maintained by the BSC-ES.")
    print()

    parser = get_arg_parser()
    print(parser.format_help())

    print("Available sub-commands:")
    print()

    commands = get_commands()
    commands = [
        (cmd, desc)
        for cmd, desc, _, _ in iter_commands(commands)
        if cmd not in _HIDDEN_SUBCOMMANDS
    ]
    print_contents(commands, indent=0, char=".")
    print()


def _split_command(
    command_line_args: list[str],
    commands: dict,
) -> tuple[str | None, list[str], list[str]]:
    """Split top-level arguments from the sub-command arguments.

    The first known Autosubmit command is used as the boundary between
    top-level arguments and sub-command arguments.

    For example::

        autosubmit -lc DEBUG clean a000

    it becomes::

        command = "clean"
        top_level_args = ["-lc", "DEBUG"]
        command_args = ["a000"]

    :param command_line_args: The command-line arguments.
    :param commands: Dictionary with the Autosubmit commands and entry-points.
    :return: A tuple containing the sub-command name, top-level arguments,
        and sub-command arguments.
    """
    for index, arg in enumerate(command_line_args):
        if arg in commands:
            return (
                arg,
                command_line_args[:index],
                command_line_args[index + 1 :],
            )

    return None, command_line_args, []


def _autosubmit(command_line_args: list[str]) -> int:
    """Run the autosubmit command.

    Return the exit code of the command.

    :param command_line_args: The command-line arguments.
    """
    # Parse ONLY top-level options, for ``autosubmit``.
    commands = get_commands()
    command, top_level_args, command_args = _split_command(command_line_args, commands)
    opts = get_arg_parser().parse_args(top_level_args)

    if opts.version:
        cli_version()
        return 0

    if opts.help_:
        cli_help()
        return 0

    if command is None:
        cli_help()
        return 0

    # TODO: AS4.x was doing it; is it really necessary? Can it be moved elsewhere?
    BasicConfig.read()

    entry_point = commands[command]
    # We are passing the rest of the command-line arguments to the sub-command! Good luck!
    return execute_cmd(entry_point, *command_args)


# noinspection PyProtectedMember
def main() -> None:
    """Main entry-point of the Autosubmit workflow manager.

    This function is called via a Python console script. This is the implementation of
    the ``autosubmit`` command.

    Global options are parsed here. Sub-command options are handled by
    the corresponding sub-command.

    Exits via ``sys.exit`` with the exit code of the sub-command or another appropriate
    error code.
    """
    ret = _autosubmit(argv[1:])
    exit(ret)
