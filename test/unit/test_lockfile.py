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

import argparse

import pytest
from portalocker.exceptions import BaseLockException

from autosubmit.log.log import AutosubmitCritical, AutosubmitError
from autosubmit.scripts.autosubmit import _owns_lock, delete_lock_file, exit_from_error, main


def test_delete_lockfile(tmp_path):
    fake_lock = tmp_path / 'autosubmit.lock'
    fake_lock.touch()

    delete_lock_file(str(tmp_path), 'not-found')

    assert fake_lock.exists()
    delete_lock_file(str(tmp_path), fake_lock.name)
    assert not fake_lock.exists()


def test_log_critical_raises_error(mocker):
    def _fn():
        raise ValueError

    try:
        _fn()
    except BaseException as e:
        mocked_log = mocker.patch('autosubmit.scripts.autosubmit.Log')
        mocked_print = mocker.patch('autosubmit.scripts.autosubmit.print')
        mocked_log.critical.side_effect = BaseException()
        with pytest.raises(BaseException):
            exit_from_error(e)

        assert mocked_print.called


_TEST_EXCEPTION = AutosubmitCritical()
_TEST_EXCEPTION.trace = 'a trace'


@pytest.mark.parametrize(
    'exception,lock_path_provided,delete_called',
    [
        (ValueError, True, True),
        (AutosubmitCritical, True, True),
        (_TEST_EXCEPTION, True, True),
        (AutosubmitError, True, True),
        (BaseLockException, True, False),
        (BaseLockException, False, False),
        (ValueError, False, False),
        (AutosubmitCritical, False, False),
        (AutosubmitError, False, False),
    ],
    ids=[
        'normal_exception_with_lock_path',
        'autosubmit_critical_with_lock_path',
        'autosubmit_critical_with_trace_with_lock_path',
        'autosubmit_error_with_lock_path',
        'portalocker_with_lock_path',
        'portalocker_without_lock_path',
        'normal_exception_without_lock_path',
        'autosubmit_critical_without_lock_path',
        'autosubmit_error_without_lock_path',
    ]
)
def test_exit_from_error(mocker, tmp_path, exception, lock_path_provided, delete_called):
    def _fn():
        raise exception

    try:
        _fn()
    except BaseException as e:
        mocker.patch('autosubmit.scripts.autosubmit.Log')
        mocked_delete = mocker.patch('autosubmit.scripts.autosubmit.delete_lock_file')
        lock_path = str(tmp_path) if lock_path_provided else None
        exit_from_error(e, lock_path)
        assert mocked_delete.called == delete_called


@pytest.mark.parametrize(
    'command,expected',
    [
        ('run', True),
        ('create', True),
        ('recovery', True),
        ('setstatus', True),
        ('pklfix', True),
        ('inspect', False),
        ('monitor', False),
        ('archive', False),
    ]
)
def test_owns_lock(command, expected):
    args = argparse.Namespace(command=command, expid='a000')
    assert _owns_lock(args) is expected


def test_owns_lock_handles_none_args():
    assert _owns_lock(None) is False
    assert _owns_lock(argparse.Namespace()) is False


@pytest.mark.parametrize(
    'command,should_delete',
    [
        ('run', True),
        ('create', True),
        ('inspect', False),
        ('monitor', False),
        ('archive', False),
    ]
)
def test_main_only_deletes_lock_for_restrictive_commands(mocker, command, should_delete):
    args = argparse.Namespace(command=command, expid='a000')
    mocker.patch('autosubmit.scripts.autosubmit.Autosubmit.parse_args', return_value=(0, args))
    mocker.patch('autosubmit.scripts.autosubmit.Autosubmit.run_command', return_value=0)
    mocked_delete = mocker.patch('autosubmit.scripts.autosubmit.delete_lock_file')

    main()

    assert mocked_delete.called is should_delete
