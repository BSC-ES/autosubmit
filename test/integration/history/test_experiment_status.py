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

from autosubmit.history.experiment_status import ExperimentHeartBeatMonitor

def test_heartbeat_monitor_starts_and_stops_correctly(mocker):
    """Test __enter__ and __exit__ methods call start() and stop()."""
    status_tracker = mocker.Mock(expid="a000")
    monitor = ExperimentHeartBeatMonitor(status_tracker, interval_seconds=1)

    start_mock = mocker.patch.object(monitor, 'start', return_value=True)
    stop_mock = mocker.patch.object(monitor, 'stop')

    with monitor as returned_monitor:
        assert returned_monitor is monitor
        
    start_mock.assert_called_once_with()
    stop_mock.assert_called_once_with()


def test_heartbeat_monitor_ping_returns_false_when_update_fails(mocker):
    """Test ping() returns False and logs warning when update_heartbeat fails."""
    status_tracker = mocker.Mock(expid="a000")
    status_tracker.update_heartbeat.side_effect = RuntimeError("DB error")
    monitor = ExperimentHeartBeatMonitor(status_tracker, interval_seconds=1)

    warning_error = mocker.patch('autosubmit.history.experiment_status.Log.warning')

    assert monitor.ping() is False
    warning_error.assert_called_once()

