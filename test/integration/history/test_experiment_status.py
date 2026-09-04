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

"""Integration tests for ExperimentStatus and ExperimentHeartBeatMonitor."""

import pytest

from autosubmit.history.experiment_status import (
    ExperimentHeartBeatMonitor,
    ExperimentStatus,
)


def test_heartbeat_monitor_starts_and_stops_correctly(mocker):
    """Test __enter__ and __exit__ methods call start() and stop()."""
    status_tracker = mocker.Mock(expid="a000")
    monitor = ExperimentHeartBeatMonitor(status_tracker, interval_seconds=1)

    start_mock = mocker.patch.object(monitor, "start", return_value=True)
    stop_mock = mocker.patch.object(monitor, "stop")

    with monitor as returned_monitor:
        assert returned_monitor is monitor

    start_mock.assert_called_once_with()
    stop_mock.assert_called_once_with()


def test_heartbeat_monitor_run_calls_ping_at_intervals(mocker):
    """Test that _run() calls ping() at the specified intervals."""
    status_tracker = mocker.Mock(expid="a000")
    monitor = ExperimentHeartBeatMonitor(status_tracker, interval_seconds=0.1)
    ping_mock = mocker.patch.object(monitor, "ping", return_value=True)

    wait_mock = mocker.patch.object(
        monitor._stop_event,
        "wait",
        side_effect=[False, False, False, True],
    )

    # Act
    monitor._run()

    # Assert
    assert (
        ping_mock.call_count == 3
    )  # Last call should not happen because wait returns True
    assert wait_mock.call_count == 4


def test_heartbeat_monitor_ping_returns_false_when_update_fails(mocker):
    """Test ping() returns False and logs warning when update_heartbeat fails."""
    status_tracker = mocker.Mock(expid="a000")
    monitor = ExperimentHeartBeatMonitor(status_tracker, interval_seconds=1)

    status_tracker.update_heartbeat.side_effect = RuntimeError("DB error")
    warning_error = mocker.patch("autosubmit.history.experiment_status.Log.warning")

    assert monitor.ping() is False
    warning_error.assert_called_once()


def test_heartbeat_monitor_ping_blocks_concurrent_pings(mocker):
    """Test that ping() correctly uses the lock to prevent concurrent updates."""
    status_tracker = mocker.Mock(expid="a000")
    monitor = ExperimentHeartBeatMonitor(status_tracker, interval_seconds=1)

    # Simulate update_heartbeat taking some time to complete
    def slow_update():
        import time

        time.sleep(0.1)

    status_tracker.update_heartbeat.side_effect = slow_update

    # Start two threads that call ping() at the same time
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=2) as executor:
        thread_a = executor.submit(monitor.ping)
        thread_b = executor.submit(monitor.ping)

        result_a = thread_a.result()
        result_b = thread_b.result()

    # Assert
    assert result_a is True
    assert result_b is True
    assert status_tracker.update_heartbeat.call_count == 2


def test_heartbeat_monitor_start_when_thread_already_running(mocker):
    """Test that start() does not start a new thread if one is already running."""
    status_tracker = mocker.Mock(expid="a000")
    monitor = ExperimentHeartBeatMonitor(status_tracker, interval_seconds=1)

    # Mock the thread to simulate it being alive
    thread = mocker.Mock()
    thread.is_alive.return_value = True
    monitor._thread = thread

    # Act
    result = monitor.start()

    # Assert
    assert result is True
    thread.is_alive.assert_called_once_with()


def test_heartbeat_monitor_start_fails_when_first_ping_fails(mocker):
    """Tets that start() fails if the initial ping() fails."""
    status_tracker = mocker.Mock(expid="a000")
    monitor = ExperimentHeartBeatMonitor(status_tracker, interval_seconds=1)

    # Simulate ping() failure on first call
    ping_mock = mocker.patch.object(monitor, "ping", return_value=False)

    # Assert
    assert monitor.start() is False
    ping_mock.assert_called_once_with()
    assert monitor._thread is None
    assert monitor._stop_event.is_set()


def test_heartbeat_monitor_start_fails_when_thread_creation_fails(mocker):
    """Test that start() fails if thread creation raises an exception."""
    status_tracker = mocker.Mock(expid="a000")
    monitor = ExperimentHeartBeatMonitor(status_tracker, interval_seconds=1)

    # Simulate successful ping() but thread creation failure
    mocker.patch.object(monitor, "ping", return_value=True)
    thread_mock = mocker.patch(
        "threading.Thread", side_effect=RuntimeError("Thread error")
    )

    # Assert
    assert monitor.start() is False
    thread_mock.assert_called_once()
    assert monitor._thread is None
    assert monitor._stop_event.is_set()


def test_heartbeat_monitor_stop_clears_thread(mocker):
    """Test that stop() clears the thread reference."""
    status_tracker = mocker.Mock(expid="a000")
    monitor = ExperimentHeartBeatMonitor(status_tracker, interval_seconds=1)

    # Mock the thread to simulate being dead
    thread = mocker.Mock()
    thread.is_alive.return_value = False
    monitor._thread = thread

    # Act
    monitor.stop()

    # Assert
    thread.join.assert_called_once_with(timeout=10.0)  # default timeout
    assert monitor._thread is None


def test_heartbeat_monitor_stop_logs_warning_if_thread_does_not_stop(mocker):
    """Test that stop() logs a warning if the thread does not stop within the timeout."""
    status_tracker = mocker.Mock(expid="a000")
    monitor = ExperimentHeartBeatMonitor(status_tracker, interval_seconds=1)

    # Mock the thread to simulate it being alive after join
    thread = mocker.Mock()
    thread.is_alive.return_value = True
    monitor._thread = thread

    warning_mock = mocker.patch("autosubmit.history.experiment_status.Log.warning")

    # Act
    monitor.stop(timeout=0.1)  # Use a short timeout for the test

    # Assert
    thread.join.assert_called_once_with(timeout=0.1)
    warning_mock.assert_called_once()
    assert monitor._thread is None


def test_experiment_status_set_status_delegates_manager(mocker):
    """Test that set_status() delegates to the status manager."""
    manager = mocker.Mock()
    mocker.patch(
        "autosubmit.history.experiment_status.create_experiment_status_db_manager",
        return_value=manager,
    )
    experiment_status = ExperimentStatus("a000")

    # Act
    experiment_status.set_status("RUNNING")

    # Assert
    manager.set_exp_status.assert_called_once_with("a000", "RUNNING")


def test_experiment_status_init_raises_error_when_manager_creation_fails(mocker):
    """Test that ExperimentStatus __init__ raises an error if the manager creation fails."""
    logging_mock = mocker.patch("autosubmit.history.experiment_status.Logging")
    mocker.patch(
        "autosubmit.history.experiment_status.create_experiment_status_db_manager",
        side_effect=RuntimeError("Error creating manager"),
    )

    with pytest.raises(RuntimeError, match="Error creating manager"):
        ExperimentStatus("a000")

    logging_mock.return_value.log.assert_called_once()
