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

examples:

    # display help for a sub-command
    $ autosubmit clean --help

    # run a sub-command
    $ autosubmit -lc DEBUG clean a000 --project
"""

from argparse import ArgumentParser
from collections import defaultdict
from sys import argv, exit

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.helpers.version import get_version
from autosubmit.log.log import Log
from autosubmit.scripts._args import (
    CommandGroup,
    cli_unknown_command,
    create_argparse_parser,
)
from autosubmit.scripts._entry_points import execute_cmd, get_commands, iter_commands
from autosubmit.scripts._terminal import print_contents, supports_color
from autosubmit.scripts._traceability import log_command_context

__all__ = ["main"]

if supports_color():
    CYAN = "\033[36m"
    ORANGE = "\033[33m"
    RESET = "\033[0m"
else:
    CYAN = ""
    ORANGE = ""
    RESET = ""


_COMPLETION_SUBCOMMAND = "__complete"
"""A hidden sub-command used by Bash to autocomplete user input."""


def _get_header() -> str:
    """Command-line terminal header."""
    return (
        f"{CYAN}   ##{RESET}    {ORANGE}####{RESET}\n"
        f"{CYAN}  #  #{RESET}  {ORANGE}#   {RESET}     Autosubmit Experiment and Workflow Manager {get_version()}\n"
        f"{CYAN} ######{RESET}  {ORANGE}####{RESET}    Copyright (C) 2011-2026 Barcelona Supercomputing Center\n"
        f"{CYAN} #    #{RESET}  {ORANGE}    #{RESET}   (BSC) & Contributors\n"
        f"{CYAN} #    #{RESET} {ORANGE}#####{RESET}"
    )


def _get_arg_parser() -> ArgumentParser:
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
        help="Set the log level for the log file.",
    )
    parser.add_argument(
        "-lc",
        "--logconsole",
        choices=log_levels,
        default="WARNING",
        type=str,
        help="Set the log level for the console.",
    )
    return parser


def _cli_version() -> None:
    """Print the version of Autosubmit."""
    Log.info(get_version())


def _cli_help() -> None:
    """Print the ``autosubmit`` command help.

    Prints the list of Autosubmit sub-commands, found via the Python
    entry-point "autosubmit.command". See ``pyproject.toml``.
    """
    print()
    print(_get_header())
    print()
    print("Autosubmit is open-source software (GPL-3.0) maintained by the BSC-ES.")
    print()

    parser = _get_arg_parser()
    print(parser.format_help())

    print("Available sub-commands:")
    print()

    commands = get_commands()

    grouped_commands: dict[CommandGroup, list[tuple[str, str | None]]] = defaultdict(
        list
    )

    # First collect all commands into their groups.
    for cmd, doc, module, entry_point in iter_commands(commands):
        if not doc:
            raise ValueError(f"Command {cmd} has no docstring!")
        group = getattr(entry_point, "command_group", CommandGroup.GENERAL)

        grouped_commands[group].append((cmd, doc.short_description))

    # Find the longest command across *all* groups so that the
    # description column is aligned globally.
    title_width = max(
        len(cmd)
        for group_commands in grouped_commands.values()
        for cmd, _ in group_commands
    )

    for group in CommandGroup:
        group_commands = grouped_commands[group]
        if not group_commands:
            continue

        print(f"{group.value}:")
        print_contents(
            sorted(group_commands),
            indent=2,
            char=".",
            title_width=title_width,
        )
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

    becomes::

        command = "clean"
        top_level_args = ["-lc", "DEBUG"]
        command_args = ["a000"]

    If no known command is found, the command is returned as ``None`` and
    all arguments are left in ``top_level_args`` so that the caller can
    report an unknown command.

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


def _cli_complete(args: list[str]) -> int:
    from autosubmit.scripts._completion import complete

    cursor = int(args[0])
    words = args[1:]

    for candidate in complete(
        words, cursor, top_level_parser=_get_arg_parser(), commands=get_commands()
    ):
        print(candidate)

    return 0


def _autosubmit(command_line_args: list[str]) -> int:
    """Run the autosubmit command.

    If the user requests the Autosubmit version, we print it and return
    0 exit code.

    If the user asks for help, we print the help text and return 0
    exit code.

    If the Autosubmit sub-command requested by the user is invalid,
    we will try to match it against the available options and inform the
    user. Or just tell them simply that the sub-command does not exist.
    Then, the exit code 2 is returned.

    If none of the above applies, this entry console script will:

    * Set the Autosubmit console logging level;
    * Print the Autosubmit version;
    * Print command traceability information;
    * Load the Autosubmit configuration (because it was done like this);
    * Load the Python entry-point code, and execute it.

    Return the exit code of the sub-command entry-point function.

    :param command_line_args: The command-line arguments.
    :return: The exit code of the sub-command entry-point function.
    """
    # Bash autocomplete.
    if command_line_args and command_line_args[0] == _COMPLETION_SUBCOMMAND:
        return _cli_complete(command_line_args[1:])

    commands = get_commands()
    command, top_level_args, command_args = _split_command(
        command_line_args,
        commands,
    )

    # Parse ONLY top-level options when a known command was found.
    #
    # If no known command was found, parse the complete command line so that
    # top-level options such as "-lc DEBUG" are still handled correctly while
    # leaving the unknown command for our own error message.
    if command is not None:
        opts = _get_arg_parser().parse_args(top_level_args)
        unknown_args = []
    else:
        opts, unknown_args = _get_arg_parser().parse_known_args(command_line_args)

    if opts.version:
        _cli_version()
        return 0

    if opts.help_:
        _cli_help()
        return 0

    if command is None:
        if not command_line_args:
            _cli_help()
            return 0

        unknown_command = unknown_args[0] if unknown_args else command_line_args[0]
        cli_unknown_command(unknown_command, commands)
        return 2

    # NOTE: We have two log files here, the ``logconsole`` and the ``logfile``.
    #       In AS4, the option used was ``logconsole``, so that behaviour is
    #       kept here. We also moved the file log level here, as otherwise that
    #       would require us to change the design of the sub-commands.
    Log.set_console_level(opts.logconsole)
    Log.file_log_level = opts.logfile

    autosubmit_version = get_version()
    Log.info(f"Autosubmit v{autosubmit_version}\n")

    # Display traceability information.
    log_command_context(argv=argv)

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
