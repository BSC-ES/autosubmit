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

"""Bash completion support for the Autosubmit CLI.

This module implements the completion logic invoked by the Bash completion
script shipped with Autosubmit.

Bash programmable completion documentation:

https://www.gnu.org/s/bash/manual/html_node/Programmable-Completion.html
https://www.gnu.org/s/bash/manual/html_node/Programmable-Completion-Builtins.html
"""

from argparse import Action, ArgumentParser
from collections.abc import Iterable
from typing import Any

__all__ = ["complete"]


# Special completion candidate understood by the Bash completion function.
# It means that the current word is already a complete sub-command and Bash
# should keep it while inserting a trailing space.
_SPACE = "__SPACE__"


def _get_options(parser: ArgumentParser) -> Iterable[str]:
    """Return all option strings defined by an argument parser.

    :param parser: The argument parser from which to retrieve option strings.
    :return: An iterable containing the option strings defined by the parser.
    """
    for action in parser._actions:
        yield from action.option_strings


def _get_option_actions(parser: ArgumentParser) -> dict[str, Action]:
    """Map each option string to its argparse action.

    :param parser: The argument parser from which to retrieve option actions.
    :return: A dictionary mapping each option string to its corresponding
        argparse action.
    """
    return {
        option: action for action in parser._actions for option in action.option_strings
    }


def _complete_options(
    parser: ArgumentParser,
    current: str,
) -> list[str]:
    """Return options matching the current word.

    :param parser: The argument parser containing the available options.
    :param current: The partial option currently being completed.
    :return: Sorted option strings starting with ``current``.
    """
    return sorted(
        option for option in _get_options(parser) if option.startswith(current)
    )


def _complete_choices(
    action: Action,
    current: str,
) -> list[str]:
    """Return choices matching the current word.

    :param action: The argparse action whose choices should be completed.
    :param current: The partial value currently being completed.
    :return: Sorted choices starting with ``current``.
    """
    if action.choices is None:
        return []

    return sorted(
        str(choice) for choice in action.choices if str(choice).startswith(current)
    )


def _option_takes_value(action: Action) -> bool:
    """Return whether an argparse action expects a value.

    :param action: The argparse action to inspect.
    :return: ``True`` if the action expects one or more values, otherwise
        ``False``.
    """
    # Actions such as store_true/store_false have nargs == 0.
    return action.nargs != 0


def _find_command(
    words: list[str],
    commands: dict[str, Any],
    parser: ArgumentParser,
) -> tuple[str | None, int | None]:
    """Find the Autosubmit sub-command in a command line.

    The first known command that occurs outside a top-level option's value is
    considered the sub-command.

    For example::

        autosubmit -lc DEBUG clean a000

    returns::

        ("clean", 2)

    :param words: The command-line words, including the executable name at
        position 0.
    :param commands: Mapping of available sub-command names to entry points.
    :param parser: The top-level Autosubmit argument parser.
    :return: A tuple containing the command name and its position in
        ``words``. Both values are ``None`` if no command is found.
    """
    option_actions = _get_option_actions(parser)

    index = 1

    while index < len(words):
        word = words[index]

        if word in commands:
            return word, index

        action = option_actions.get(word)

        if action is not None and _option_takes_value(action):
            # Top-level options currently consume one argument. This covers
            # Autosubmit's options such as --logfile and --logconsole.
            index += 1

        index += 1

    return None, None


def _complete_top_level(
    words: list[str],
    cursor: int,
    parser: ArgumentParser,
    commands: dict[str, Any],
) -> list[str]:
    """Complete top-level options or sub-command names.

    If the current word starts with ``-``, top-level options are returned.
    Otherwise, available sub-command names are returned.

    :param words: The command-line words, including the executable name.
    :param cursor: The index of the word currently being completed.
    :param parser: The top-level Autosubmit argument parser.
    :param commands: Mapping of available sub-command names to entry points.
    :return: Sorted completion candidates.
    """
    current = words[cursor] if cursor < len(words) else ""

    if current.startswith("-"):
        return _complete_options(parser, current)

    return sorted(command for command in commands if command.startswith(current))


def _complete_parser(
    parser: ArgumentParser,
    words: list[str],
    cursor: int,
) -> list[str]:
    """Complete arguments for a sub-command parser.

    Currently, this supports:

    * option names;
    * values from ``argparse`` ``choices``.

    Positional arguments and dynamically generated values are intentionally
    not completed yet.

    :param parser: The argument parser for the sub-command.
    :param words: The command-line words belonging to the sub-command.
    :param cursor: The index of the word currently being completed.
    :return: Sorted completion candidates.
    """
    current = words[cursor] if 0 <= cursor < len(words) else ""
    previous = words[cursor - 1] if 0 < cursor <= len(words) else None

    option_actions = _get_option_actions(parser)

    # If the previous word is an option that takes a value, complete its
    # choices instead of suggesting more options.
    if previous is not None:
        action = option_actions.get(previous)

        if action is not None and _option_takes_value(action):
            return _complete_choices(action, current)

    # Complete option names when starting a new argument or when the
    # current word already starts with ``-``.
    if not current or current.startswith("-"):
        return _complete_options(parser, current)

    # We currently don't complete positional arguments.
    return []


def complete(
    words: list[str],
    cursor: int,
    *,
    top_level_parser: ArgumentParser,
    commands: dict[str, Any],
) -> list[str]:
    """Return completion candidates for an Autosubmit command line.

    The command is resolved from the available entry points. Once a
    sub-command is identified, its argparse parser is used to provide
    option and choice completions.

    When the cursor is positioned on an already-complete sub-command, the
    special ``_SPACE`` completion is returned. This tells the Bash completion
    function to preserve the command and insert a trailing space.

    Experiment IDs and other dynamically generated positional values are not
    completed.

    :param words: The command-line words, including the executable name, as
        provided by Bash's ``COMP_WORDS``.
    :param cursor: The index of the word currently being completed, as
        provided by Bash's ``COMP_CWORD``.
    :param top_level_parser: The top-level Autosubmit argument parser.
    :param commands: Mapping of available sub-command names to entry points.
    :return: Completion candidates.
    """
    command, command_index = _find_command(
        words,
        commands,
        top_level_parser,
    )

    if command is None or command_index is None:
        return _complete_top_level(
            words,
            cursor,
            top_level_parser,
            commands,
        )

    # The cursor is currently on the command itself.
    #
    # For:
    #
    #     autosubmit run<TAB>
    #
    # Bash has:
    #
    #     words = ["autosubmit", "run"]
    #     cursor = 1
    #     command_index = 1
    #
    # We must not hand this to the sub-command parser, because that would
    # make it look like we are completing a new argument. Instead, tell
    # Bash to preserve "run" and add a space.
    if cursor == command_index:
        return [_SPACE]

    entry_point = commands[command]
    command_main = entry_point.load()
    parser = command_main.build_parser()

    # Everything after the sub-command belongs to this parser.
    command_words = words[command_index + 1 :]
    command_cursor = cursor - command_index - 1

    # Bash does not include an empty word when the cursor is immediately
    # after the command. Add one so the sub-command parser sees a new
    # argument position.
    if command_cursor >= len(command_words):
        command_words.extend([""] * (command_cursor - len(command_words) + 1))

    return _complete_parser(
        parser,
        command_words,
        command_cursor,
    )
