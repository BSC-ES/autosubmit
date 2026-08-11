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

"""Man pages generation code.

Writes documentation in roff, a typesetting language commonly used for
Unix manual pages and books, including *The C Programming Language*.

See: https://linux.die.net/man/7/roff
"""

from argparse import ArgumentParser
from pathlib import Path
from typing import TYPE_CHECKING, Any

from autosubmit.log.log import Log
from autosubmit.scripts._entry_points import iter_commands

if TYPE_CHECKING:
    from types import ModuleType

__all__ = ["write_man_page", "write_man_pages"]


def _get_short_description(parser: ArgumentParser) -> str:
    """Return a short description suitable for the NAME section."""
    description = parser.description or ""
    return description.splitlines()[0] if description else "Autosubmit command"


def _get_synopsis(parser: ArgumentParser) -> str:
    """Return the synopsis for a manual page."""
    if parser.prog == "autosubmit":
        return "autosubmit [OPTIONS] COMMAND [COMMAND OPTIONS]"

    return parser.format_usage().strip().removeprefix("usage: ")


def write_man_page(
    parser: ArgumentParser, path: Path, synopsis: str | None = None
) -> None:
    """Write a manual page to disk.

    :param parser: ArgumentParser parser.
    :param path: Path to write the manual page to disk.
    :param synopsis: Optional custom command synopsis.
    """
    command = parser.prog

    # Backslashes in f-string expressions are not supported by Python 3.10.
    roff_command = command.replace(" ", "\\ ")

    synopsis = _get_synopsis(parser)
    short_description = _get_short_description(parser)

    lines = [
        f".TH {roff_command.upper()} 1",
        ".SH NAME",
        f"{command} \\- {short_description}",
        ".SH SYNOPSIS",
        synopsis,
        ".SH DESCRIPTION",
    ]

    description = parser.description or ""
    lines.extend(description.splitlines())

    lines.append(".SH OPTIONS")

    for action in parser._actions:
        if not action.option_strings:
            continue

        option = ", ".join(action.option_strings)

        if action.metavar:
            option += f" {action.metavar}"

        lines.extend(
            [
                ".TP",
                f".B {option}",
                action.help or "",
            ]
        )

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    Log.info(f'Created man pages for "{command}" in {path}')


def write_man_pages(
    commands: dict[str, Any],
    output_dir: Path,
) -> None:
    """Generate manual pages for all Autosubmit sub-commands.

    :param commands: Autosubmit command entry points.
    :param output_dir: Directory in which to write the manual pages.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    from autosubmit.scripts.autosubmit import get_arg_parser

    count = 0

    # Not quite perfect... in the ideal world we'd get this automatically...
    write_man_page(
        get_arg_parser(),
        output_dir / "autosubmit.1",
        synopsis="autosubmit [OPTIONS] COMMAND [COMMAND OPTIONS]",
    )

    count += 1

    command: str
    module: "ModuleType"
    for command, _, _, module in iter_commands(commands):
        parser = module.main.build_parser()
        parser.prog = f"autosubmit {command}"

        write_man_page(parser, output_dir / f"autosubmit-{command}.1")
        count += 1

    Log.info(f"Created {count} man pages in {output_dir}")
