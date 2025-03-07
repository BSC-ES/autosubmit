import pytest
from portalocker.exceptions import BaseLockException

from autosubmit.scripts.autosubmit import delete_lock_file, exit_from_error
from log.log import AutosubmitCritical, AutosubmitError


def test_delete_lockfile(tmp_path):
    fake_lock = tmp_path / 'autosubmit.lock'
    fake_lock.touch()

    delete_lock_file(str(tmp_path), 'not-found')

    assert fake_lock.exists()
    delete_lock_file(str(tmp_path), fake_lock.name)
    assert not fake_lock.exists()


def test_log_debug_raises_error(mocker):
    """TODO: this probably should never happen?"""

    def _fn():
        raise ValueError

    try:
        _fn()
    except BaseException as e:
        mocker.patch('autosubmit.scripts.autosubmit._exit')  # mock this to avoid the system from exiting
        mocked_log = mocker.patch('autosubmit.scripts.autosubmit.Log')
        mocked_print = mocker.patch('autosubmit.scripts.autosubmit.print')

        mocked_log.debug.side_effect = BaseException()

        exit_from_error(e)

        assert mocked_print.called


_TEST_EXCEPTION = AutosubmitCritical()
_TEST_EXCEPTION.trace = 'a trace'


@pytest.mark.parametrize(
    'exception,debug_calls,critical_calls,delete_called',
    [
        (ValueError, 1, 1, True),
        (BaseLockException, 1, 0, False),
        (AutosubmitCritical, 1, 1, True),
        (_TEST_EXCEPTION, 2, 1, True),
        (AutosubmitError, 1, 1, True)
    ],
    ids=[
        'normal_exception',
        'portalocker_exception',
        'autosubmit_critical',
        'autosubmit_critical_with_trace',
        'autosubmit_error'
    ]
)
def test_exit_from_error(
        mocker,
        tmp_path,
        exception: BaseException,
        debug_calls: int,
        critical_calls: int,
        delete_called: bool
):
    def _fn():
        raise exception

    try:
        _fn()
    except BaseException as e:
        mocker.patch('autosubmit.scripts.autosubmit._exit')  # mock this to avoid the system from exiting
        mocked_log = mocker.patch('autosubmit.scripts.autosubmit.Log')
        mocked_delete = mocker.patch('autosubmit.scripts.autosubmit.delete_lock_file')

        exit_from_error(e)

        assert mocked_log.debug.call_count == debug_calls
        assert mocked_log.critical.call_count == critical_calls
        assert mocked_delete.called == delete_called
