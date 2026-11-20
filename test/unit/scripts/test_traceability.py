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


"""Unit tests for the ``autosubmit.scripts._traceability`` module."""

import os

# noinspection PyProtectedMember
from autosubmit.scripts._traceability import _working_directory, log_command_context


def test_working_directory_returns_cwd_when_available(mocker):
    mocker.patch(
        "autosubmit.scripts._traceability.os.getcwd", return_value="/some/where"
    )
    assert _working_directory() == "/some/where"


def test_working_directory_falls_back_to_pwd_when_cwd_deleted(mocker):
    # os.getcwd() raises FileNotFoundError once the working directory is gone;
    # $PWD still holds the path the shell was in, so prefer it. See GH #3332.
    mocker.patch(
        "autosubmit.scripts._traceability.os.getcwd",
        side_effect=FileNotFoundError(2, "No such file or directory"),
    )
    mocker.patch.dict(os.environ, {"PWD": "/tmp/was_here"})
    assert _working_directory() == "/tmp/was_here"


def test_working_directory_falls_back_to_placeholder_without_pwd(mocker):
    mocker.patch(
        "autosubmit.scripts._traceability.os.getcwd",
        side_effect=FileNotFoundError(2, "No such file or directory"),
    )
    mocker.patch.dict(os.environ, clear=False)
    os.environ.pop("PWD", None)
    assert _working_directory() == "(unavailable - working directory was deleted)"


def test_log_command_context_survives_deleted_working_directory(mocker):
    # The whole command used to crash here (any command run from a deleted
    # folder, e.g. after a `refresh` removed `proj`); it must now log and
    # continue rather than raise. See GH #3332.
    mock_log = mocker.patch("autosubmit.scripts._traceability.Log")
    mocker.patch(
        "autosubmit.scripts._traceability.os.getcwd",
        side_effect=FileNotFoundError(2, "No such file or directory"),
    )
    mocker.patch.dict(os.environ, clear=False)
    os.environ.pop("PWD", None)

    log_command_context(argv=["autosubmit", "readme"])  # must not raise

    logged = " ".join(str(call) for call in mock_log.info.call_args_list)
    assert "Working directory:" in logged
    assert "unavailable" in logged
