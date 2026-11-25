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
from portalocker.exceptions import BaseLockException

from autosubmit.log.log import AutosubmitCritical, AutosubmitError, AutosubmitUserError
from autosubmit.scripts._args import exit_from_error


def test_exit_from_error_handles_trace_logging_failure(mocker):
    """Fall back to print when traceback logging fails."""
    exception = ValueError("test error")

    mocked_log = mocker.patch("autosubmit.scripts._args.Log")
    mocked_print = mocker.patch("autosubmit.scripts._args.print")

    mocked_log.critical.side_effect = [
        Exception("logging failed"),
        None,
    ]

    result = exit_from_error(exception)

    assert result == 7000
    assert mocked_log.critical.call_count == 2
    mocked_print.assert_called_once()


@pytest.mark.parametrize(
    "exception,expected_code,critical_calls,warning_called",
    [
        (
            ValueError("test error"),
            7000,
            2,
            False,
        ),
        (
            BaseLockException(),
            1,
            1,
            True,
        ),
        (
            AutosubmitCritical(),
            7000,
            2,
            False,
        ),
        (
            AutosubmitError(),
            6000,
            2,
            False,
        ),
    ],
    ids=[
        "normal_exception",
        "portalocker_exception",
        "autosubmit_critical",
        "autosubmit_error",
    ],
)
def test_exit_from_error(
    mocker,
    exception,
    expected_code,
    critical_calls,
    warning_called,
):
    """Return the expected code and handle each supported exception type."""
    mocked_log = mocker.patch("autosubmit.scripts._args.Log")

    result = exit_from_error(exception)

    assert result == expected_code
    assert mocked_log.critical.call_count == critical_calls
    assert mocked_log.warning.called is warning_called


def test_exit_from_error_logs_autosubmit_critical_trace(mocker):
    """Log the trace attached to an AutosubmitCritical exception."""
    exception = AutosubmitCritical()
    exception.trace = "a trace"

    mocked_log = mocker.patch("autosubmit.scripts._args.Log")

    result = exit_from_error(exception)

    assert result == exception.code
    assert mocked_log.critical.call_count == 3
    mocked_log.critical.assert_any_call("Trace: a trace")


def test_exit_from_error_logs_user_error_traceback_only_at_debug(mocker):
    mocked_log = mocker.patch("autosubmit.scripts._args.Log")

    try:
        raise AutosubmitUserError("Job not found", 7011)
    except AutosubmitUserError as error:
        assert exit_from_error(error) == 7011

    mocked_log.critical.assert_called_once_with("Job not found [eCode=7011]")
    mocked_log.debug.assert_called_once()
    assert "Traceback" in mocked_log.debug.call_args.args[0]


def test_exit_from_error_logs_portalocker_warning(mocker):
    """Warn about an experiment lock when a portalocker error occurs."""
    exception = BaseLockException()

    mocked_log = mocker.patch("autosubmit.scripts._args.Log")

    result = exit_from_error(exception)

    assert result == 1
    mocked_log.warning.assert_called_once()
