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

"""Unit tests for ``autosubmit.experiment.lock``."""

import pytest
from portalocker.exceptions import BaseLockException

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database.session import DatabaseBackend
from autosubmit.exceptions import (
    ExperimentAlreadyLockedException,
    FailedToReleaseLockException,
)
from autosubmit.experiment import lock as lock_module
from autosubmit.log.log import AutosubmitCritical, AutosubmitError

# noinspection PyProtectedMember
from autosubmit.scripts._args import exit_from_error


@pytest.fixture
def postgres_connection(mocker):
    """Provide a mocked PostgreSQL engine and connection."""
    engine = mocker.MagicMock()
    conn = mocker.MagicMock()

    # Model engine.connect().execution_options().__enter__().
    execution_context = engine.connect.return_value.execution_options.return_value
    execution_context.__enter__.return_value = conn

    # Patch get_engine where it is used by the lock module.
    mocker.patch(
        "autosubmit.experiment.lock.get_engine",
        return_value=engine,
    )

    return engine, conn


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
            ExperimentAlreadyLockedException(),
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


def test_exit_from_error_logs_portalocker_warning(mocker):
    """Warn about an experiment lock when a portalocker error occurs."""
    exception = ExperimentAlreadyLockedException()

    mocked_log = mocker.patch("autosubmit.scripts._args.Log")

    result = exit_from_error(exception)

    assert result == 1
    mocked_log.warning.assert_called_once()


def test_experiment_lock_uses_postgres(mocker):
    """Use PostgreSQL locking when the database backend is PostgreSQL."""
    expid = "a123"

    mocker.patch.object(
        lock_module.BasicConfig,
        "DATABASE_BACKEND",
        DatabaseBackend.POSTGRES,
    )

    mocked_postgres_lock = mocker.patch(
        "autosubmit.experiment.lock.postgres_based_experiment_lock"
    )

    result = lock_module.experiment_lock(expid)

    mocked_postgres_lock.assert_called_once_with(expid)
    assert result == mocked_postgres_lock.return_value


def test_experiment_lock_uses_file_lock(mocker):
    """Use file-based locking when the backend is not PostgreSQL."""
    expid = "a123"

    mocker.patch.object(
        lock_module.BasicConfig,
        "DATABASE_BACKEND",
        DatabaseBackend.SQLITE,
    )

    mocked_file_lock = mocker.patch("autosubmit.experiment.lock.ExperimentFileLock")

    result = lock_module.experiment_lock(expid)

    mocked_file_lock.assert_called_once_with(expid)
    assert result == mocked_file_lock.return_value


def test_experiment_file_lock_acquire_success(mocker):
    """Acquire the file lock successfully."""
    expid = "a123"

    mocked_lock = mocker.patch("autosubmit.experiment.lock.TemporaryFileLock")

    file_lock = lock_module.ExperimentFileLock(expid)

    file_lock.__enter__()

    mocked_lock.return_value.__enter__.assert_called_once_with()


def test_experiment_file_lock_acquire_failure(mocker):
    """Raise ExperimentAlreadyLockedException when acquisition fails."""
    expid = "a123"

    mocked_lock = mocker.patch("autosubmit.experiment.lock.TemporaryFileLock")

    mocked_lock.return_value.__enter__.side_effect = BaseLockException("Already locked")

    file_lock = lock_module.ExperimentFileLock(expid)

    with pytest.raises(
        ExperimentAlreadyLockedException,
        match=f"Experiment {expid} is locked",
    ) as exc_info:
        file_lock.__enter__()

    assert isinstance(exc_info.value.__cause__, BaseLockException)


def test_experiment_file_lock_release_failure(mocker):
    """Raise FailedToReleaseLockException when releasing the file lock fails."""
    expid = "a123"

    mocked_lock = mocker.patch("autosubmit.experiment.lock.TemporaryFileLock")

    mocked_lock.return_value.__exit__.side_effect = BaseLockException("Release failed")

    file_lock = lock_module.ExperimentFileLock(expid)

    with pytest.raises(
        FailedToReleaseLockException,
        match=f"Failed to release lock for experiment {expid}",
    ) as exc_info:
        file_lock.__exit__(None, None, None)

    assert isinstance(exc_info.value.__cause__, BaseLockException)


def test_experiment_file_lock_release_success(mocker):
    """Release the file lock without raising an exception."""
    mocked_lock = mocker.patch("autosubmit.experiment.lock.TemporaryFileLock")

    file_lock = lock_module.ExperimentFileLock("a123")

    file_lock.__exit__(None, None, None)

    mocked_lock.return_value.__exit__.assert_called_once_with(None, None, None)


def test_postgres_lock_success(postgres_connection):
    """Acquire and release a PostgreSQL advisory lock successfully."""
    expid = "a123"
    decoded_expid = int(expid, 36)

    engine, conn = postgres_connection

    conn.execute.return_value.scalar_one.return_value = True

    with lock_module.postgres_based_experiment_lock(expid):
        pass

    engine.connect.assert_called_once()

    engine.connect.return_value.execution_options.assert_called_once_with(
        isolation_level="AUTOCOMMIT"
    )

    assert conn.execute.call_count == 2

    acquire_call = conn.execute.call_args_list[0]
    release_call = conn.execute.call_args_list[1]

    assert acquire_call.args[1] == {"key": decoded_expid}
    assert release_call.args[1] == {"key": decoded_expid}

    assert str(acquire_call.args[0]) == lock_module._QUERY_OBTAIN_LOCK
    assert str(release_call.args[0]) == lock_module._QUERY_RELEASE_LOCK

    conn.invalidate.assert_not_called()


def test_postgres_lock_retries_until_acquired(mocker, postgres_connection):
    """Retry acquiring the PostgreSQL lock until it becomes available."""
    _, conn = postgres_connection

    conn.execute.return_value.scalar_one.side_effect = [
        False,
        False,
        True,  # Acquisition succeeds.
        True,  # Release succeeds.
    ]

    mocked_sleep = mocker.patch("autosubmit.experiment.lock.time.sleep")

    with lock_module.postgres_based_experiment_lock("a123"):
        pass

    assert conn.execute.call_count == 4

    assert mocked_sleep.call_count == 2

    mocked_sleep.assert_called_with(
        lock_module.BasicConfig.EXPERIMENT_LOCK_RETRY_INTERVAL
    )

    conn.invalidate.assert_not_called()


def test_postgres_lock_timeout(mocker, postgres_connection):
    """Raise ExperimentAlreadyLockedException when the lock times out."""
    _, conn = postgres_connection

    conn.execute.return_value.scalar_one.return_value = False

    # First call sets the deadline (0 + 1 second).
    # Second call is before the deadline.
    # Third call is after the deadline.
    mocker.patch.object(BasicConfig, "EXPERIMENT_LOCK_TIMEOUT", 1.0)
    mocker.patch(
        "autosubmit.experiment.lock.time.monotonic",
        side_effect=[0, 0.5, 2, 2, 2],
    )

    mocked_sleep = mocker.patch("autosubmit.experiment.lock.time.sleep")

    with pytest.raises(
        ExperimentAlreadyLockedException,
        match="Experiment a123 is locked",
    ):
        with lock_module.postgres_based_experiment_lock("a123"):
            pass

    # Two acquisition attempts, both unsuccessful.
    assert conn.execute.call_count == 2

    # The lock was never acquired, so it must not be released.
    conn.invalidate.assert_not_called()

    # One retry interval before the second acquisition attempt.
    mocked_sleep.assert_called_once_with(
        lock_module.BasicConfig.EXPERIMENT_LOCK_RETRY_INTERVAL
    )


def test_postgres_lock_acquisition_database_error(postgres_connection):
    """Invalidate the connection when PostgreSQL lock acquisition fails."""
    _, conn = postgres_connection

    conn.execute.side_effect = RuntimeError("Database error")

    with pytest.raises(RuntimeError, match="Database error"):
        with lock_module.postgres_based_experiment_lock("a123"):
            pass

    conn.invalidate.assert_called_once()


def test_postgres_lock_already_released_logs_warning(mocker, postgres_connection):
    """Warn when the PostgreSQL session no longer holds the advisory lock."""
    _, conn = postgres_connection

    conn.execute.return_value.scalar_one.side_effect = [
        True,  # Lock successfully acquired.
        False,  # Lock already released.
    ]

    mocked_log = mocker.patch("autosubmit.experiment.lock.Log")

    with lock_module.postgres_based_experiment_lock("a123"):
        pass

    mocked_log.warning.assert_called_once_with(
        "The PostgreSQL session no longer holds the advisory lock "
        "for experiment a123. The lock may have been released "
        "before the command completed. Please check whether another "
        "Autosubmit process is modifying this experiment."
    )

    conn.invalidate.assert_not_called()


def test_postgres_lock_release_database_error(mocker, postgres_connection):
    """Raise FailedToReleaseLockException when PostgreSQL release raises an error."""
    _, conn = postgres_connection

    conn.execute.side_effect = [
        mocker.MagicMock(scalar_one=mocker.MagicMock(return_value=True)),
        RuntimeError("Database release error"),
    ]

    with pytest.raises(
        FailedToReleaseLockException,
        match="Failed to release lock for experiment a123",
    ) as exc_info:
        with lock_module.postgres_based_experiment_lock("a123"):
            pass

    assert isinstance(exc_info.value.__cause__, RuntimeError)
    assert str(exc_info.value.__cause__) == "Database release error"

    conn.invalidate.assert_called_once()


def test_postgres_lock_released_when_context_raises(postgres_connection):
    """Release the PostgreSQL lock even when the context body raises an exception."""
    _, conn = postgres_connection

    conn.execute.return_value.scalar_one.return_value = True

    with pytest.raises(ValueError, match="Command failed"):  # NOSONAR python:S5778
        with lock_module.postgres_based_experiment_lock("a123"):
            raise ValueError("Command failed")

    # First execution acquires the lock.
    # Second execution releases the lock.
    assert conn.execute.call_count == 2

    conn.invalidate.assert_not_called()


def test_experiment_file_lock_uses_configured_timeout_and_retry_interval(
    mocker, monkeypatch
):
    """Pass the configured timeout and retry interval to ``portalocker``."""
    expid = "a123"
    timeout = 5.0
    retry_interval = 0.5

    monkeypatch.setattr(
        lock_module.BasicConfig,
        "EXPERIMENT_LOCK_TIMEOUT",
        timeout,
    )
    monkeypatch.setattr(
        lock_module.BasicConfig,
        "EXPERIMENT_LOCK_RETRY_INTERVAL",
        retry_interval,
    )

    mocked_lock = mocker.patch("autosubmit.experiment.lock.TemporaryFileLock")

    lock_module.ExperimentFileLock(expid)

    mocked_lock.assert_called_once_with(
        str(lock_module.BasicConfig.expid_lock_file(expid)),
        timeout=timeout,
        check_interval=retry_interval,
        fail_when_locked=False,
    )


def test_postgres_lock_uses_configured_timeout_and_retry_interval(
    mocker, monkeypatch, postgres_connection
):
    """Use the configured timeout and retry interval for PostgreSQL locking."""
    _, conn = postgres_connection

    timeout = 5.0
    retry_interval = 0.5

    monkeypatch.setattr(
        lock_module.BasicConfig,
        "EXPERIMENT_LOCK_TIMEOUT",
        timeout,
    )
    monkeypatch.setattr(
        lock_module.BasicConfig,
        "EXPERIMENT_LOCK_RETRY_INTERVAL",
        retry_interval,
    )

    conn.execute.return_value.scalar_one.return_value = False

    # Deadline is 5 seconds.
    # First attempt: 1 second elapsed.
    # Second attempt: 6 seconds elapsed.
    mocker.patch(
        "autosubmit.experiment.lock.time.monotonic",
        side_effect=[0, 1, 6],
    )

    mocked_sleep = mocker.patch("autosubmit.experiment.lock.time.sleep")

    with pytest.raises(
        ExperimentAlreadyLockedException,
        match="Experiment a123 is locked",
    ):
        with lock_module.postgres_based_experiment_lock("a123"):
            pass

    assert conn.execute.call_count == 2
    mocked_sleep.assert_called_once_with(retry_interval)
    conn.invalidate.assert_not_called()


def test_postgres_lock_sleep_is_bounded_by_remaining_time(
    mocker, postgres_connection, monkeypatch
):
    """Never sleep longer than the remaining time before the deadline."""
    _, conn = postgres_connection

    monkeypatch.setattr(
        lock_module.BasicConfig,
        "EXPERIMENT_LOCK_TIMEOUT",
        1.0,
    )
    monkeypatch.setattr(
        lock_module.BasicConfig,
        "EXPERIMENT_LOCK_RETRY_INTERVAL",
        0.5,
    )

    conn.execute.return_value.scalar_one.return_value = False

    # Deadline = 10 + 1 = 11.
    # First attempt: 10.8, remaining = 0.2.
    # Second attempt: 11.1, timeout.
    mocker.patch(
        "autosubmit.experiment.lock.time.monotonic",
        side_effect=[10, 10.8, 11.1],
    )

    mocked_sleep = mocker.patch("autosubmit.experiment.lock.time.sleep")

    with pytest.raises(ExperimentAlreadyLockedException):
        with lock_module.postgres_based_experiment_lock("a123"):
            pass

    mocked_sleep.assert_called_once_with(pytest.approx(0.2))
    assert conn.execute.call_count == 2
    conn.invalidate.assert_not_called()


def test_postgres_lock_does_not_sleep_when_immediately_acquired(
    mocker, postgres_connection
):
    """Do not sleep when the advisory lock is immediately available."""
    _, conn = postgres_connection

    conn.execute.return_value.scalar_one.return_value = True

    mocked_sleep = mocker.patch("autosubmit.experiment.lock.time.sleep")

    with lock_module.postgres_based_experiment_lock("a123"):
        pass

    mocked_sleep.assert_not_called()
    assert conn.execute.call_count == 2
    conn.invalidate.assert_not_called()


def test_postgres_lock_zero_timeout_attempts_acquisition(
    mocker, postgres_connection, monkeypatch
):
    """Attempt acquisition once even when the timeout is zero."""
    _, conn = postgres_connection

    monkeypatch.setattr(
        lock_module.BasicConfig,
        "EXPERIMENT_LOCK_TIMEOUT",
        0,
    )

    conn.execute.return_value.scalar_one.return_value = False

    mocked_sleep = mocker.patch("autosubmit.experiment.lock.time.sleep")

    with pytest.raises(ExperimentAlreadyLockedException):
        with lock_module.postgres_based_experiment_lock("a123"):
            pass

    assert conn.execute.call_count == 1
    mocked_sleep.assert_not_called()
    conn.invalidate.assert_not_called()


def test_postgres_lock_release_failure_preserves_original_exception(
    mocker,
    postgres_connection,
):
    """Preserve the original exception when PostgreSQL lock release also fails.

    Tests the scenario where:

    - Postgres lock is acquired
    - ``yield`` transfers the control to the command code
    - the command code raises a ``ValueError("Command failed")``
    - then, we enter the ``finally`` block
    - ``pg_advisory_unlock()`` fails! And the clean-up code raises a ``FailedToReleaseLockException``

    We ensure the original exception is kept. It is not our responsibility to log, so
    we except that somewhere else, someone is capturing this error and logging it or
    handling it correctly.
    """
    _, conn = postgres_connection
    conn.execute.side_effect = [
        mocker.MagicMock(scalar_one=mocker.MagicMock(return_value=True)),
        RuntimeError("Database release error"),
    ]

    def failing_command():
        raise ValueError("Command failed")

    lock = lock_module.postgres_based_experiment_lock("a123")

    with pytest.raises(
        FailedToReleaseLockException,
        match="Failed to release lock for experiment a123",
    ) as exc_info:
        with lock:
            failing_command()

    # The database exception is explicitly chained.
    database_exception = exc_info.value.__cause__
    assert isinstance(database_exception, RuntimeError)
    assert str(database_exception) == "Database release error"

    # The original command exception is preserved as implicit context.
    original_exception = database_exception.__context__
    assert isinstance(original_exception, ValueError)
    assert str(original_exception) == "Command failed"

    conn.invalidate.assert_called_once()
