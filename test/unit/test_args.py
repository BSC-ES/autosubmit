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

import pytest
from portalocker.exceptions import BaseLockException

from autosubmit.log.log import AutosubmitCritical, AutosubmitError
from autosubmit.scripts._args import exit_from_error


def test_log_critical_raises_error(mocker):
    """Test that print is used when Log.critical raises an exception."""

    def _fn():
        raise ValueError

    try:
        _fn()
    except Exception as e:
        mocked_log = mocker.patch("autosubmit.scripts._args.Log")
        mocked_print = mocker.patch("autosubmit.scripts._args.print")

        mocked_log.critical.side_effect = Exception()

        with pytest.raises(Exception):
            exit_from_error(e)

        assert mocked_print.called


_TEST_EXCEPTION = AutosubmitCritical()
_TEST_EXCEPTION.trace = "a trace"


@pytest.mark.parametrize(
    "exception,expected_code,critical_calls",
    [
        (ValueError(), 7000, 2),
        (BaseLockException(), 1, 1),
        (AutosubmitCritical(), 7000, 2),
        (_TEST_EXCEPTION, 7000, 3),
        (AutosubmitError(), 6000, 2),
    ],
    ids=[
        "normal_exception",
        "portalocker_exception",
        "autosubmit_critical",
        "autosubmit_critical_with_trace",
        "autosubmit_error",
    ],
)
def test_exit_from_error(
    mocker,
    exception: Exception,
    expected_code: int,
    critical_calls: int,
):
    mocked_log = mocker.patch("autosubmit.scripts._args.Log")

    result = exit_from_error(exception)

    assert result == expected_code
    assert mocked_log.critical.call_count == critical_calls
