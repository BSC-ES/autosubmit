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
from autosubmit.scripts._args import _delete_lock_file, exit_from_error


def test_delete_lockfile(tmp_path):
    fake_lock = tmp_path / "autosubmit.lock"
    fake_lock.touch()

    _delete_lock_file(str(tmp_path), "not-found")

    assert fake_lock.exists()

    _delete_lock_file(str(tmp_path), fake_lock.name)

    assert not fake_lock.exists()


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
    "exception,expected_code,critical_calls,delete_called",
    [
        (ValueError(), 7000, 2, True),
        (BaseLockException(), 1, 1, False),
        (AutosubmitCritical(), 7000, 2, True),
        (_TEST_EXCEPTION, 7000, 3, True),
        (AutosubmitError(), 6000, 2, True),
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
    delete_called: bool,
):
    mocked_log = mocker.patch("autosubmit.scripts._args.Log")
    mocked_delete = mocker.patch("autosubmit.scripts._args._delete_lock_file")

    result = exit_from_error(exception)

    assert result == expected_code
    assert mocked_log.critical.call_count == critical_calls
    assert mocked_delete.called == delete_called
