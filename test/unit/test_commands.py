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

import pytest

from autosubmit.scripts.autosubmit import _autosubmit, get_arg_parser


def test_invalid_top_level_argument():
    """Test that argparse rejects an unknown top-level argument."""
    parser = get_arg_parser()

    with pytest.raises(SystemExit) as exc_info:
        parser.parse_args(["--fail-this-command-please-sir"])

    assert exc_info.value.code == 2


def test_no_command_shows_help(mocker):
    """Test that running Autosubmit without a command displays help."""
    mock_help = mocker.patch("autosubmit.scripts.autosubmit.cli_help")

    status = _autosubmit([])

    assert status == 0
    mock_help.assert_called_once_with()


def test_invalid_subcommand(mocker):
    """Test that an unknown subcommand is rejected by the top-level parser."""
    mock_help = mocker.patch("autosubmit.scripts.autosubmit.cli_help")

    with pytest.raises(SystemExit) as exc_info:
        _autosubmit(["versioning"])

    assert exc_info.value.code == 2
    mock_help.assert_not_called()


def test_top_level_version(mocker):
    """Test that the top-level version option exits successfully."""
    mock_version = mocker.patch("autosubmit.scripts.autosubmit.cli_version")

    status = _autosubmit(["--version"])

    assert status == 0
    mock_version.assert_called_once_with()


def test_top_level_help(mocker):
    """Test that the top-level help option exits successfully."""
    mock_help = mocker.patch("autosubmit.scripts.autosubmit.cli_help")

    status = _autosubmit(["--help"])

    assert status == 0
    mock_help.assert_called_once_with()


@pytest.mark.parametrize(
    "arguments",
    [
        ["-h"],
        ["--help"],
    ],
    ids=[
        "short-help",
        "long-help",
    ],
)
def test_help_aliases(mocker, arguments):
    """Test that both help aliases are accepted."""
    mock_help = mocker.patch("autosubmit.scripts.autosubmit.cli_help")

    status = _autosubmit(arguments)

    assert status == 0
    mock_help.assert_called_once_with()


@pytest.mark.parametrize(
    "arguments",
    [
        ["-v"],
        ["--version"],
    ],
    ids=[
        "short-version",
        "long-version",
    ],
)
def test_version_aliases(mocker, arguments):
    """Test that both version aliases are accepted."""
    mock_version = mocker.patch("autosubmit.scripts.autosubmit.cli_version")

    status = _autosubmit(arguments)

    assert status == 0
    mock_version.assert_called_once_with()


def test_subcommand_arguments_are_forwarded(mocker):
    """Test that arguments after the subcommand are passed to the command."""
    entry_point = mocker.Mock()
    commands = {"create": entry_point}

    mocker.patch(
        "autosubmit.scripts.autosubmit.get_commands",
        return_value=commands,
    )
    mocker.patch(
        "autosubmit.scripts.autosubmit.BasicConfig.read",
    )
    execute_cmd = mocker.patch(
        "autosubmit.scripts.autosubmit.execute_cmd",
        return_value=0,
    )

    status = _autosubmit(
        [
            "create",
            "a000",
            "--project",
        ]
    )

    assert status == 0
    execute_cmd.assert_called_once_with(
        entry_point,
        "a000",
        "--project",
    )


def test_top_level_options_are_parsed_before_subcommand(mocker):
    """Test that global options can appear before the subcommand."""
    entry_point = mocker.Mock()
    commands = {"create": entry_point}

    mocker.patch(
        "autosubmit.scripts.autosubmit.get_commands",
        return_value=commands,
    )
    mocker.patch(
        "autosubmit.scripts.autosubmit.BasicConfig.read",
    )
    execute_cmd = mocker.patch(
        "autosubmit.scripts.autosubmit.execute_cmd",
        return_value=0,
    )

    status = _autosubmit(
        [
            "-lc",
            "DEBUG",
            "create",
            "a000",
        ]
    )

    assert status == 0
    execute_cmd.assert_called_once_with(
        entry_point,
        "a000",
    )


def test_subcommand_exit_code_is_returned(mocker):
    """Test that the subcommand exit code is propagated."""
    entry_point = mocker.Mock()
    commands = {"create": entry_point}

    mocker.patch(
        "autosubmit.scripts.autosubmit.get_commands",
        return_value=commands,
    )
    mocker.patch(
        "autosubmit.scripts.autosubmit.BasicConfig.read",
    )
    mocker.patch(
        "autosubmit.scripts.autosubmit.execute_cmd",
        return_value=42,
    )

    assert _autosubmit(["create"]) == 42


def test_invalid_subcommand_argument_is_handled_by_subcommand(mocker):
    """Test that subcommand-specific invalid arguments reach the subcommand."""
    entry_point = mocker.Mock()
    commands = {"create": entry_point}

    mocker.patch(
        "autosubmit.scripts.autosubmit.get_commands",
        return_value=commands,
    )
    mocker.patch(
        "autosubmit.scripts.autosubmit.BasicConfig.read",
    )

    execute_cmd = mocker.patch(
        "autosubmit.scripts.autosubmit.execute_cmd",
        side_effect=SystemExit(2),
    )

    with pytest.raises(SystemExit) as exc_info:
        _autosubmit(
            [
                "create",
                "--fail-this-command-please-sir",
            ]
        )

    assert exc_info.value.code == 2
    execute_cmd.assert_called_once_with(
        entry_point,
        "--fail-this-command-please-sir",
    )
