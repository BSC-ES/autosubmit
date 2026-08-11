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

"""Argument parsing utilities."""

import traceback
from argparse import ArgumentParser, Namespace, RawDescriptionHelpFormatter
from contextlib import suppress
from enum import Enum
from inspect import cleandoc
from pathlib import Path
from re import split, sub
from typing import Any, TypeVar, cast, get_type_hints

from portalocker.exceptions import BaseLockException

from autosubmit.log.log import AutosubmitCritical, AutosubmitError, Log
from autosubmit.profiler.profiler import Profiler

__all__ = [
    "AutosubmitOptions",
    "DefaultOptions",
    "ExpidMode",
    "ExpidOptions",
    "OptionsT",
    "add_common_arguments",
    "create_argparse_parser",
    "exit_from_error",
    "get_expid_mode",
    "normalise_return_value",
    "parse_expids",
]


class AutosubmitOptions(Namespace):
    """Autosubmit options object."""


OptionsT = TypeVar("OptionsT", bound=AutosubmitOptions)
"""Generic type for options objects."""


class DefaultOptions(AutosubmitOptions):
    """Default options for Autosubmit commands."""

    update_version: bool
    """Whether to update the experiment version on disk or database."""
    profile: bool
    """Whether to profile the command execution."""
    # These are not passed via the command line.
    accepts_multiple_expids = False
    """Whether or not the sub-command accepts multiple expids. Default is no."""

    _profiler: Profiler | None = None
    """The profiler instance if set."""


class ExpidOptions(DefaultOptions):
    """Options that contain an experiment identifier."""

    expid: str
    """The experiment identifier."""


class ExpidMode(str, Enum):
    """String enum type for expid modes in Autosubmit sub-commands."""

    NONE = "no-expid"
    """The expid-less option."""
    OPTIONAL = "optional-expid"
    """The sub-command takes an expid argument, but it is optional."""
    REQUIRED = "requires-expid"
    """The sub-command requires an expid argument."""


def get_expid_mode(
    options_type: type[AutosubmitOptions],
) -> ExpidMode:
    """Get the expid mode for a given options type.

    Uses the ``expid`` attribute of the type hints to determine the expid mode.

    If the attribute is not present returns ``ExpidMode.NONE``.

    If it is present but may be ``str | None``, then returns ``ExpidMode.OPTIONAL``.

    Otherwise, it must be ``str``, and returns ``ExpidMode.REQUIRED``.

    If the developer accidentally specified ``expid`` with the wrong type, .e.g, ``expid: int``,
    then this function fails with ``TypeError``.

    :param options_type: The type of the options for the sub-command.
    :return: The expid mode.
    :raises TypeError: If the ``expid`` attribute is not a ``str`` or ``str | None``.
    """
    hints = get_type_hints(options_type)

    if "expid" not in hints:
        return ExpidMode.NONE

    annotation = hints["expid"]

    if annotation is str:
        return ExpidMode.REQUIRED

    if annotation == str | None:
        return ExpidMode.OPTIONAL

    raise TypeError(f"{options_type.__name__}.expid must be str or str | None")


def _has_argument(parser: ArgumentParser, name: str) -> bool:
    """Check if an argparse parser already contains an argument definition."""
    return any(
        name in action.option_strings or action.dest == name
        for action in parser._actions
    )


def add_common_arguments(
    parser: ArgumentParser,
    *,
    expid_mode: ExpidMode,
    options_type: type["AutosubmitOptions"],
) -> None:
    """Add common Autosubmit CLI arguments to the parser.

    :param parser: The argparse parser to add arguments to.
    :param expid_mode: The expid mode.
    :param options_type: The type of the options for the sub-command.
    """
    is_default_options = issubclass(options_type, DefaultOptions)
    has_expid = (
        is_default_options
        or hasattr(options_type, "expid")
        or "expid" in options_type.__annotations__
    )
    has_update_version = (
        is_default_options
        or hasattr(options_type, "update_version")
        or "update_version" in options_type.__annotations__
    )

    if has_expid and not _has_argument(parser, "expid"):
        if expid_mode is ExpidMode.REQUIRED:
            parser.add_argument("expid", type=str, help="experiment identifier")
        elif expid_mode is ExpidMode.OPTIONAL:
            # TODO: is optional required at all?
            parser.add_argument(
                "expid", type=str, nargs="?", help="experiment identifier"
            )

    if has_update_version and not _has_argument(parser, "update_version"):
        # Update the Autosubmit version of the experiment when running the command requested.
        parser.add_argument(
            "-v",
            "--update_version",
            action="store_true",
            default=False,
            help="update experiment version",
        )

    # Allow to run every Autosubmit sub-command with a profiler.
    parser.add_argument(
        "--profile",
        action="store_true",
        default=False,
        required=False,
        help="prints performance parameters of the execution of this command",
    )

    # TODO: add --debug here or --verbose later...


def clean_docstring(doc: str) -> str:
    """Prepare the docstring text for terminal display.

    :param doc: The docstring to prepare.
    :return: The cleaned docstring.
    """
    # Sphinx inline literals: ``text`` -> text
    doc = sub(r"``([^`]+)``", r"\1", doc)
    return cleandoc(doc)


def create_argparse_parser(docs: str | None, *, add_help=True) -> ArgumentParser:
    """Create an ArgumentParser from the docstring.

    Reads the first line as the title (do not add a blank space as the first line).

    Everything else is treated as the body.

    The body before ``Examples:`` is used as the description.
    The examples are used as the epilog.

    :param docs: The docstring of the function.
    :param add_help: Whether to add the help argument.
    :raises ValueError: If the docstring is missing.
    """
    if not docs:
        raise ValueError("Missing docstring")

    docs = clean_docstring(docs)

    lines = docs.splitlines()
    title = lines[0].strip()
    body = "\n".join(lines[1:]).strip()

    marker = "Examples:"
    if marker in body:
        description, examples = body.split(marker, 1)
        epilog = f"\n{marker}\n{examples}"
    else:
        description = body
        epilog = None

    return ArgumentParser(
        prog=title,
        description=description,
        epilog=epilog,
        formatter_class=RawDescriptionHelpFormatter,
        add_help=add_help,
    )


def _delete_lock_file(
    base_path: str = Log.file_path, lock_file: str = "autosubmit.lock"
) -> None:
    """Delete the lock file if it exists. Suppresses permission errors raised.

    :param base_path: Base path to locate the lock file. Defaults to the experiment ``tmp`` directory.
    :param lock_file: The name of the lock file. Defaults to ``autosubmit.lock``.
    :return: None
    """
    with suppress(PermissionError):
        Path(base_path, lock_file).unlink(missing_ok=True)


def exit_from_error(e: BaseException) -> int:
    """Called by ``Autosubmit`` when an exception is raised during a command execution.

    Prints the exception in ``CRITICAL`` if it is an ``AutosubmitCritical`` or an
    ``AutosubmitError`` exception, including any trace attached to the exception.

    Exceptions raised by ``porta-locker` library print a message informing the user
    about the locked experiment. Other exceptions raised cause the lock to be deleted.

    :param e: The exception being raised.
    """
    err_code = 1
    trace = traceback.format_exc()
    try:
        Log.critical(trace)
    except Exception:
        print(trace)

    is_portalocker_error = isinstance(e, BaseLockException)
    is_autosubmit_error = isinstance(e, (AutosubmitCritical, AutosubmitError))

    if isinstance(e, BaseLockException):
        Log.warning(
            "Another Autosubmit instance using the experiment\n. Stop other Autosubmit instances that are "
            "using the experiment or delete autosubmit.lock file located on the /tmp folder."
        )
    else:
        _delete_lock_file()

    if is_autosubmit_error:
        as_error: AutosubmitError | AutosubmitCritical = cast(
            AutosubmitError | AutosubmitCritical, e
        )
        if as_error.trace:
            Log.critical(f"Trace: {str(as_error.trace)}")
        Log.critical(f"{as_error.message} [eCode={as_error.code}]")
        err_code = as_error.code

    if not is_portalocker_error and not is_autosubmit_error:
        msg = "Unexpected error: {0}.\n Please report it to Autosubmit Developers through Git: https://github.com/BSC-ES/autosubmit/issues"
        args = [str(e)]
        Log.critical(msg.format(*args))
        err_code = 7000

    Log.info(
        "More info at https://autosubmit.readthedocs.io/en/master/troubleshooting/error-codes.html"
    )
    return err_code


def normalise_return_value(return_value: Any) -> int:
    """Convert a sub-command return value to a process exit code.

    ``None`` and ``True`` indicate success.

    ``False`` indicates failure.

    An integer is used directly as the process exit code.

    :param return_value: The value that was returned by the sub-command.
    :return: A process exit code.
    """
    if return_value is None:
        return 0

    # Check bool before int because bool is a subclass of int.
    if type(return_value) is bool:
        if return_value:
            return 0

        # TODO: Make sure we have it elsewhere?! Log.error("The command failed")
        return 1

    if type(return_value) is int:
        return return_value

    raise TypeError(
        f"CLI command must return None, bool, or int; got {type(return_value).__name__}"
    )


def parse_expids(value: str | None) -> list[str]:
    """Parse a comma- or whitespace-separated experiment ID string.

    Empty or missing values result in an empty list.

    >>> parse_expids(None)
    []
    >>> parse_expids("")
    []
    >>> parse_expids("a001")
    ['a001']
    >>> parse_expids("a001,a002")
    ['a001', 'a002']
    >>> parse_expids("a001, a002")
    ['a001', 'a002']
    >>> parse_expids("a001 a002")
    ['a001', 'a002']
    >>> parse_expids(" a001,  a002  ")
    ['a001', 'a002']
    """
    if not value:
        return []

    return [e for e in split(r"[,\s]+", value.strip()) if e != "*"]
