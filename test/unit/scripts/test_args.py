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

"""Unit tests for the ``autosubmit.scripts._args`` module."""

# noinspection PyProtectedMember
from autosubmit.scripts._args import cli_unknown_command


def test_cli_unknown_command_without_match(mocker):
    """Test that an unknown command without a close match reports only the error."""
    mock_error = mocker.patch("autosubmit.scripts._args.Log.error")

    cli_unknown_command(
        "completely-invalid-command",
        {
            "create": mocker.Mock(),
            "archive": mocker.Mock(),
        },
    )

    mock_error.assert_called_once_with(
        "Error: unknown autosubmit sub-command 'completely-invalid-command'."
    )


def test_cli_unknown_command_with_close_match(mocker):
    """Test that an unknown command with a close match includes a suggestion."""
    mock_error = mocker.patch("autosubmit.scripts._args.Log.error")

    cli_unknown_command(
        "archivo",
        {
            "archive": mocker.Mock(),
            "create": mocker.Mock(),
        },
    )

    assert mock_error.call_count == 2
    mock_error.assert_any_call("Error: unknown autosubmit sub-command 'archivo'.")
    mock_error.assert_any_call("Did you mean 'autosubmit archive'?")


def test_cli_unknown_command_does_not_suggest_distant_match(mocker):
    """Test that sufficiently different commands do not get a suggestion."""
    mock_error = mocker.patch("autosubmit.scripts._args.Log.error")

    cli_unknown_command(
        "xyz",
        {
            "archive": mocker.Mock(),
            "create": mocker.Mock(),
        },
    )

    mock_error.assert_called_once_with("Error: unknown autosubmit sub-command 'xyz'.")


def test_cli_unknown_command_suggests_only_one_match(mocker):
    """Test that at most one command suggestion is displayed."""
    mock_error = mocker.patch("autosubmit.scripts._args.Log.error")

    cli_unknown_command(
        "creat",
        {
            "create": mocker.Mock(),
            "creator": mocker.Mock(),
            "archive": mocker.Mock(),
        },
    )

    assert mock_error.call_count == 2
    mock_error.assert_any_call("Error: unknown autosubmit sub-command 'creat'.")

    suggestion_calls = [
        call for call in mock_error.call_args_list if "Did you mean" in str(call)
    ]

    assert len(suggestion_calls) == 1
