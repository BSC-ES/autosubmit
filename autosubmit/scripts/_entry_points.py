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

"""Code for handling Python entry-points."""

import sys
from collections.abc import Generator
from typing import TYPE_CHECKING, Any

from autosubmit.scripts._args import CommandDocstring, parse_docstring

if TYPE_CHECKING:
    from types import ModuleType

__all__ = [
    "execute_cmd",
    "get_commands",
    "iter_commands",
    "iter_entry_points",
]


def _handle_missing_dependency(entry_point, err: ModuleNotFoundError) -> str:
    """Return a suitable error message for a missing optional dependency.

    :param entry_point: The entry point that was attempted to load but caused a ``ModuleNotFoundError``.
    :param err: The ``ModuleNotFoundError`` that was caught.
    :raises ModuleNotFoundError: If the given ``ModuleNotFoundError`` is unexpected.
    """
    msg = f'"autosubmit {entry_point.name}" requires "{entry_point.dist.name}'
    if entry_point.extras:
        msg += f"[{','.join(entry_point.extras)}]"
    msg += f'"\n\n{err.__class__.__name__}: {err}'
    return msg


def iter_commands(
    commands: dict[str, Any],
) -> Generator[tuple[str, CommandDocstring | None, "ModuleType", Any], None, None]:
    """Yield all sub-commands that are available.

    Skips sub-commands that require missing optional dependencies.

    :param commands: The entry-points dictionary.
    :return: Iterator of tuples containing the command name, parsed documentation,
        module, and loaded command.
    """
    for cmd, entry_point in sorted(commands.items()):
        try:
            module = __import__(entry_point.module, fromlist=[""])
            # We must load it so we can access the command groups.
            command = entry_point.load()
        except ModuleNotFoundError as exc:
            msg = _handle_missing_dependency(entry_point, exc)
            print(msg, file=sys.stderr)
            continue

        if getattr(module, "INTERNAL", False):
            # do not list internal commands
            continue

        doc: CommandDocstring | None = None

        if hasattr(module, "__doc__") and module.__doc__:
            doc = parse_docstring(module.__doc__)

        yield cmd, doc, module, command


def iter_entry_points(entry_point_name):
    """Iterate over entry points."""
    from importlib.metadata import entry_points

    yield from entry_points(group=entry_point_name)


def get_commands() -> dict:
    """Return the available Autosubmit sub-command entry-points."""
    return {
        entry_point.name: entry_point
        for entry_point in iter_entry_points("autosubmit.command")
    }


def execute_cmd(entry_point: Any, *args: str) -> int:
    """Execute a sub-command.

    :param entry_point: The Python entry-point function to execute.
    :param args: The arguments to pass to the command.
    :return: The exit code of the command. Zero indicates success.
    """
    try:
        return entry_point.load()(*args)
    except ModuleNotFoundError as exc:
        msg = _handle_missing_dependency(entry_point, exc)
        print(msg, file=sys.stderr)
        return 1
