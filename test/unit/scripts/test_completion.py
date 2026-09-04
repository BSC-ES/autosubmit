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

"""Unit tests for the ``autosubmit.scripts._completion`` module."""

from argparse import ArgumentParser

import pytest

# noinspection PyProtectedMember
from autosubmit.scripts._completion import (
    _complete_choices,
    _complete_options,
    _complete_parser,
    _complete_top_level,
    _find_command,
    _get_option_actions,
    _get_options,
    _option_takes_value,
    complete,
)


def test_get_options():
    """Test that all option strings are returned."""
    parser = ArgumentParser(add_help=False)
    parser.add_argument("-v", "--version", action="store_true")
    parser.add_argument("-f", "--file")

    assert sorted(_get_options(parser)) == [
        "--file",
        "--version",
        "-f",
        "-v",
    ]


def test_get_option_actions():
    """Test that option strings are mapped to their argparse actions."""
    parser = ArgumentParser(add_help=False)
    parser.add_argument("-v", "--version", action="store_true")
    parser.add_argument("-f", "--file")

    actions = _get_option_actions(parser)

    assert actions["-v"] is actions["--version"]
    assert actions["-f"] is actions["--file"]
    assert actions["-v"].nargs == 0
    assert actions["-f"].nargs is None


def test_complete_options():
    """Test that options matching the current word are returned sorted."""
    parser = ArgumentParser(add_help=False)
    parser.add_argument("-v", "--version", action="store_true")
    parser.add_argument("-f", "--file")

    assert _complete_options(parser, "--") == [
        "--file",
        "--version",
    ]

    assert _complete_options(parser, "-v") == ["-v"]


def test_complete_choices():
    """Test that argparse choices matching the current word are returned."""
    parser = ArgumentParser(add_help=False)
    action = parser.add_argument(
        "--loglevel",
        choices=["DEBUG", "ERROR", "INFO", "WARNING"],
    )

    assert _complete_choices(action, "") == [
        "DEBUG",
        "ERROR",
        "INFO",
        "WARNING",
    ]

    assert _complete_choices(action, "DE") == ["DEBUG"]
    assert _complete_choices(action, "X") == []


def test_complete_choices_without_choices():
    """Test that an argparse action without choices returns no candidates."""
    parser = ArgumentParser(add_help=False)
    action = parser.add_argument("--name")

    assert _complete_choices(action, "") == []


@pytest.mark.parametrize(
    ("nargs", "expected"),
    [
        (0, False),
        (None, True),
        (1, True),
        ("?", True),
        ("+", True),
        ("*", True),
    ],
)
def test_option_takes_value(mocker, nargs, expected):
    """Test whether argparse actions are correctly identified as value-taking."""
    action = mocker.Mock()
    action.nargs = nargs

    assert _option_takes_value(action) is expected


def test_find_command(mocker):
    """Test that a known sub-command is found."""
    parser = ArgumentParser(add_help=False)
    parser.add_argument("-lc", "--logconsole")

    commands = {
        "create": mocker.Mock(),
        "run": mocker.Mock(),
    }

    command, index = _find_command(
        ["autosubmit", "run", "a000"],
        commands,
        parser,
    )

    assert command == "run"
    assert index == 1


def test_find_command_after_top_level_option(mocker):
    """Test that a command after a top-level option value is found."""
    parser = ArgumentParser(add_help=False)
    parser.add_argument("-lc", "--logconsole")

    commands = {
        "create": mocker.Mock(),
        "run": mocker.Mock(),
    }

    command, index = _find_command(
        ["autosubmit", "-lc", "DEBUG", "run", "a000"],
        commands,
        parser,
    )

    assert command == "run"
    assert index == 3


def test_find_command_ignores_top_level_option_value(mocker):
    """Test that a command-like option value is not mistaken for a command."""
    parser = ArgumentParser(add_help=False)
    parser.add_argument("-lc", "--logconsole")

    commands = {
        "DEBUG": mocker.Mock(),
        "run": mocker.Mock(),
    }

    command, index = _find_command(
        ["autosubmit", "-lc", "DEBUG", "run"],
        commands,
        parser,
    )

    assert command == "run"
    assert index == 3


def test_find_command_returns_none_when_no_command_exists(mocker):
    """Test that no command is returned when no known command is present."""
    parser = ArgumentParser(add_help=False)
    parser.add_argument("-lc", "--logconsole")

    commands = {
        "create": mocker.Mock(),
        "run": mocker.Mock(),
    }

    command, index = _find_command(
        ["autosubmit", "-lc", "DEBUG"],
        commands,
        parser,
    )

    assert command is None
    assert index is None


def test_complete_top_level_options(mocker):
    """Test completion of top-level options."""
    parser = ArgumentParser(add_help=False)
    parser.add_argument("-v", "--version", action="store_true")
    parser.add_argument("-lc", "--logconsole")

    commands = {
        "create": mocker.Mock(),
        "run": mocker.Mock(),
    }

    assert _complete_top_level(
        ["autosubmit", "--v"],
        1,
        parser,
        commands,
    ) == ["--version"]


def test_complete_parser_options():
    """Test completion of sub-command options."""
    parser = ArgumentParser(add_help=False)
    parser.add_argument("-p", "--project")
    parser.add_argument("--verbose", action="store_true")

    assert _complete_parser(
        parser,
        ["--p"],
        0,
    ) == ["--project"]


def test_complete_parser_choices():
    """Test completion of choices for an option."""
    parser = ArgumentParser(add_help=False)
    parser.add_argument(
        "--loglevel",
        choices=["DEBUG", "ERROR", "INFO", "WARNING"],
    )

    assert _complete_parser(
        parser,
        ["--loglevel", "DE"],
        1,
    ) == ["DEBUG"]


def test_complete_parser_returns_options_after_completed_argument():
    """Test that a new argument position offers options."""
    parser = ArgumentParser(add_help=False)
    parser.add_argument("--project")
    parser.add_argument("--verbose", action="store_true")

    assert _complete_parser(
        parser,
        [""],
        0,
    ) == ["--project", "--verbose"]


def test_complete_parser_does_not_complete_positional_arguments():
    """Test that positional arguments are intentionally not completed."""
    parser = ArgumentParser(add_help=False)
    parser.add_argument("experiment")

    assert (
        _complete_parser(
            parser,
            ["a000"],
            0,
        )
        == []
    )


def test_complete_top_level_command(mocker):
    """Test completion of a top-level sub-command."""
    parser = ArgumentParser(add_help=False)

    commands = {
        "archive": mocker.Mock(),
        "create": mocker.Mock(),
        "run": mocker.Mock(),
    }

    assert complete(
        ["autosubmit", "ar"],
        1,
        top_level_parser=parser,
        commands=commands,
    ) == ["archive"]


def test_complete_top_level_option():
    """Test completion of a top-level option."""
    parser = ArgumentParser(add_help=False)
    parser.add_argument("-v", "--version", action="store_true")
    parser.add_argument("-h", "--help", action="store_true")

    assert complete(
        ["autosubmit", "--v"],
        1,
        top_level_parser=parser,
        commands={},
    ) == ["--version"]


def test_complete_completed_command_returns_space(mocker):
    """Test that an already-complete command requests a trailing space."""
    parser = ArgumentParser(add_help=False)

    commands = {
        "run": mocker.Mock(),
    }

    assert complete(
        ["autosubmit", "run"],
        1,
        top_level_parser=parser,
        commands=commands,
    ) == ["__SPACE__"]


def test_complete_subcommand_option(mocker):
    """Test completion of an option belonging to a sub-command."""
    parser = ArgumentParser(add_help=False)

    command = mocker.Mock()
    command_main = mocker.Mock()

    command_parser = ArgumentParser(add_help=False)
    command_parser.add_argument("--project")

    command_main.build_parser.return_value = command_parser
    command.load.return_value = command_main

    commands = {
        "run": command,
    }

    assert complete(
        ["autosubmit", "run", "--p"],
        2,
        top_level_parser=parser,
        commands=commands,
    ) == ["--project"]

    command.load.assert_called_once_with()
    command_main.build_parser.assert_called_once_with()


def test_complete_subcommand_choices(mocker):
    """Test completion of a sub-command option's choices."""
    parser = ArgumentParser(add_help=False)

    command = mocker.Mock()
    command_main = mocker.Mock()

    command_parser = ArgumentParser(add_help=False)
    command_parser.add_argument(
        "--loglevel",
        choices=["DEBUG", "ERROR", "INFO", "WARNING"],
    )

    command_main.build_parser.return_value = command_parser
    command.load.return_value = command_main

    commands = {
        "run": command,
    }

    assert complete(
        ["autosubmit", "run", "--loglevel", "DE"],
        3,
        top_level_parser=parser,
        commands=commands,
    ) == ["DEBUG"]

    command.load.assert_called_once_with()
    command_main.build_parser.assert_called_once_with()


def test_complete_after_subcommand(mocker):
    """Test that completion after a sub-command starts a new argument."""
    parser = ArgumentParser(add_help=False)

    command = mocker.Mock()
    command_main = mocker.Mock()

    command_parser = ArgumentParser(add_help=False)
    command_parser.add_argument("--project")

    command_main.build_parser.return_value = command_parser
    command.load.return_value = command_main

    commands = {
        "run": command,
    }

    assert complete(
        ["autosubmit", "run"],
        2,
        top_level_parser=parser,
        commands=commands,
    ) == ["--project"]

    command.load.assert_called_once_with()
    command_main.build_parser.assert_called_once_with()
