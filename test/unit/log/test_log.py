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

import logging
from contextlib import nullcontext as does_not_raise
from pathlib import Path

import pytest

from autosubmit.log.log import (
    AutosubmitError, AutosubmitCritical, LogFormatter, Log, StatusFilter, StatusFailedFilter
)


def test_autosubmit_error_default_values():
    as_error = AutosubmitError()

    assert as_error.message == "Unhandled Error"
    assert as_error.code == 6000
    assert as_error.trace is None

    assert str(as_error) == " "

    assert as_error.error_message == as_error.message


def test_autosubmit_error_constructor():
    as_error = AutosubmitError("abc", 6500, "test")

    assert as_error.message == "abc"
    assert as_error.code == 6500
    assert as_error.trace == "test"

    assert str(as_error) == " "

    assert as_error.error_message == f"test abc"


def test_autosubmit_critical_default_values():
    as_error = AutosubmitCritical()

    assert as_error.message == "Unhandled Error"
    assert as_error.code == 7000
    assert as_error.trace is None

    assert str(as_error) == " "


def test_autosubmit_critical_constructor():
    as_error = AutosubmitCritical("abc", 6500, "test")

    assert as_error.message == "abc"
    assert as_error.code == 6500
    assert as_error.trace == "test"

    assert str(as_error) == " "


@pytest.mark.parametrize('to_file', [False, True])
def test_log_formatter(to_file):
    formatter = LogFormatter(to_file=to_file)

    msg = 'abc'

    # The logger code uses ``Log`` levels that match the values in ``LogFormatter``,
    # so we capture those values here dynamically.
    levels = [
        level
        for level, value in Log.__dict__.items()
        if level.isupper() and hasattr(LogFormatter, level)
    ]
    assert levels

    for level in levels:
        # Create a dummy ``LogRecord`` object, and set the message and level we want to test.
        log_record = logging.LogRecord(
            name='',
            exc_info=None,
            lineno=0,
            pathname='',
            args=None,
            msg=msg,
            level=logging.INFO
        )
        log_record.levelno = getattr(Log, level)

        level_str = '' if level == 'RESULT' else f'[{level}] '

        # This is bad design in the tests, probably the code could be simplified to make
        # writing tests a bit simpler.
        if to_file:
            logged = formatter.format(log_record)

            assert logged.startswith(level_str)
            assert logged.endswith(msg)
        else:
            expected = f'{getattr(LogFormatter, level)}{level_str}{msg}{LogFormatter.DEFAULT}'

            logged = formatter.format(log_record)

            assert logged == expected


def test_filters():
    for status_filter in [StatusFilter(), StatusFailedFilter()]:
        recs = []
        for level in [Log.STATUS, Log.STATUS_FAILED, Log.INFO]:
            log_record = logging.LogRecord(
                name='',
                exc_info=None,
                lineno=0,
                pathname='',
                args=None,
                msg='bla',
                level=logging.INFO
            )
            log_record.levelno = level
            recs.append(log_record)

        assert len(recs) == 3
        filtered = list(filter(lambda r: not status_filter.filter(r), recs))
        assert len(filtered) == 2


def test_log_constructor():
    assert Log()


def test_log_init_variables():
    log = Log()
    log.init_variables("abc")

    assert log.file_path == "abc"


def test_log_shutdown_logger(mocker):
    mocked_logging = mocker.patch('autosubmit.log.log.logging')

    log = Log()
    log.shutdown_logger()

    assert mocked_logging.shutdown.called


def test_set_console_level():
    log = Log()

    log.set_console_level(42)
    assert log.console_handler.level == 42

    log.set_console_level('INFO')
    assert log.console_handler.level == Log.INFO

    with pytest.raises(AttributeError):
        log.set_console_level('CANNOT_FIND_IT')


@pytest.mark.parametrize(
    'level,msg,args,expected',
    [
        # No args.
        ('debug', 'john says %s', None, 'john says '),
        ('info', 'john says %s', None, 'john says '),
        ('result', 'john says %s', None, 'john says '),
        ('warning', 'john says %s', None, 'john says '),
        ('error', 'john says %s', None, 'john says '),
        ('critical', 'john says %s', None, 'john says '),
        ('status', 'john says %s', None, 'john says '),
        ('status_failed', 'john says %s', None, 'john says '),
        # Now repeat, with args.
        ('debug', 'john says %s', 'Hi!', 'john says Hi!'),
        ('info', 'john says %s', 'Hi!', 'john says  Hi!'),
        ('result', 'john says %s', 'Hi!', 'john says  Hi!'),
        ('warning', 'john says %s', 'Hi!', 'john says  Hi!'),
        ('error', 'john says %s', 'Hi!', 'john says  Hi!'),
        ('critical', 'john says %s', 'Hi!', 'john says  Hi!'),
        ('status', 'john says %s', 'Hi!', 'john says  Hi!'),
        ('status_failed', 'john says %s', 'Hi!', 'john says  Hi!'),
    ]
)
def test_log_at_certain_level(level, msg, args, expected, mocker):
    """Ensures calling ``Log.{level}`` function passing message and args results in a call to ``Log.log.log``."""
    mocked_log_log = mocker.patch('autosubmit.log.log.Log.log')

    fn = getattr(Log, level)
    if args:
        fn(msg, args)
    else:
        fn(msg)

    assert mocked_log_log.log.called
    assert mocked_log_log.log.call_args_list[0][0][0] == getattr(Log, level.upper())


@pytest.mark.parametrize(
    'code,fn',
    [
        (3000, 'warning'),
        (4000, 'info'),
        (5000, 'result'),
        (6000, 'error'),
        (7000, 'critical'),
        # TODO: Huh, that's interesting, below 7000 and not in the other groups, it's a critical;
        #       but above the other groups is an info?
        (1, 'critical'),
        (-1, 'critical'),
        (0, 'critical'),
        (8000, 'info')
    ]
)
def test_printlog(code, fn, mocker):
    mocked_log = mocker.patch('autosubmit.log.log.Log')

    Log.printlog(code=code)

    fn_obj = getattr(mocked_log, fn)
    assert fn_obj.called


def test_reset_status_file_dummy():
    """It's harmless to call ``Log.reset_status_file()`` with a random ``type``."""
    with does_not_raise():
        Log.reset_status_file(file_path='', type='SHEEP')


def test_reset_status_file_exceptions_are_ignored(mocker):
    """For some reason the old code ignores any exceptions..."""
    mocked_log = mocker.patch('autosubmit.log.log.Log')
    mocked_log.log.side_effect = ValueError

    with does_not_raise():
        Log.reset_status_file(file_path='', type='status')


def test_reset_status_file_filters(mocker, tmp_path):
    mocked_log = mocker.patch('autosubmit.log.log.Log.log')

    for status_filter in ['status', 'status_failed']:
        tmp_file = tmp_path / f'{status_filter}.tmp'
        tmp_file.touch()
        Log.reset_status_file(file_path=str(tmp_file), type=status_filter)

        assert mocked_log.addHandler.called


def test_reset_status_file_status_filter_more_than_three_handlers(mocker, tmp_path):
    """Another case where the old code limited the number of handlers to 3..."""
    mocked_log = mocker.patch('autosubmit.log.log.Log.log')
    mocked_log.handlers = [1, 2, 3, 4, 5, 6]

    assert len(mocked_log.handlers) == 6

    _type = 'status'
    tmp_file = tmp_path / f'{_type}.tmp'
    tmp_file.touch()
    Log.reset_status_file(file_path=str(tmp_file), type=_type)

    # Here the code will have reset the file and also limited the number of handlers...
    assert len(mocked_log.handlers) == 3

    assert mocked_log.addHandler.called


@pytest.mark.parametrize(
    '_type,handler_added',
    [
        ('out', True),
        ('err', True),
        ('status', True),
        ('status_failed', True),
        ('disney', False)
    ]
)
def test_set_file(_type, handler_added, tmp_path, mocker):
    date = '20100309_'
    mocked_log = mocker.patch('autosubmit.log.log.Log.log')
    mocker.patch('autosubmit.log.log.Log.date', date)

    tmp_file = tmp_path / 'test.tmp'

    # TODO: This is strange too, you want to set the file, but first you must have an
    #       existing log file, with the same name, but with the date. (What about the
    #       first ever call? Chicken or egg case?)".
    tmp_file_with_date = tmp_path / f'{date}test.tmp'
    tmp_file_with_date.touch()

    Log.set_file(file_path=str(tmp_file), type=_type)

    assert mocked_log.addHandler.called == handler_added


def test_set_file_more_than_10_files(tmp_path, mocker):
    tmp_file = tmp_path / 'test.tmp'

    # TODO: This is strange too, you want to set the file, but first you must have an
    #       existing log file, with the same name, but with the date. (What about the
    #       first ever call? Chicken or egg case?)".
    for i in range(0, 20):
        tmp_file_with_date = tmp_path / f'{i}_test.tmp'
        tmp_file_with_date.touch()

    assert len(list(tmp_path.iterdir())) == 20
    assert Path(tmp_path / '0_test.tmp').exists()
    assert not Path(tmp_path / '100_test.tmp').exists()

    mocked_log = mocker.patch('autosubmit.log.log.Log.log')
    mocker.patch('autosubmit.log.log.Log.date', '100_')

    Log.set_file(file_path=str(tmp_file), type='out')
    assert mocked_log.addHandler.called

    # Here we confirm the number of log files remains the same (because it's more than 10),
    # and that the new log file has been created, and the first in the sorted by name list
    # has been deleted.
    assert len(list(tmp_path.iterdir())) == 20
    assert not Path(tmp_path / '0_test.tmp').exists()
    assert Path(tmp_path / '100_test.tmp').exists()
