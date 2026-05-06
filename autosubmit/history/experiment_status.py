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

import traceback
import threading
from typing import Optional

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.history.database_managers import database_models as Models
from autosubmit.history.database_managers.database_manager import (
    DEFAULT_LOCAL_ROOT_DIR,
    DEFAULT_HISTORICAL_LOGS_DIR,
)
from autosubmit.history.database_managers.experiment_status_db_manager import (
    create_experiment_status_db_manager,
)
from autosubmit.history.internal_logging import Logging
from autosubmit.log.log import Log


class ExperimentHeartBeatMonitor:
    """Keeps the experiment heartbeat active while the experiment is running."""

    def __init__(self, status_tracker: "ExperimentStatus", interval_seconds: int = 120):
        self._status_tracker = status_tracker
        self._interval_seconds = max(1, interval_seconds)
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._ping_lock = threading.Lock()

    def __enter__(self) -> "ExperimentHeartBeatMonitor":
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback) -> bool:
        self.stop()
        return False

    def _run(self) -> None:
        """Background thread method that updates the heartbeat at regular intervals."""
        while not self._stop_event.wait(self._interval_seconds):
            self.ping()

    def ping(self) -> bool:
        """Ping the heartbeat to update the last_heartbeat timestamp in the database."""
        # TODO: Not sure about locking here. Needs testing
        # Use lock to prevent concurrent updates of the heartbeat, prevent race conditions
        with self._ping_lock:
            try:
                self._status_tracker.update_heartbeat()
                return True
            except Exception as exc:
                Log.warning(
                    f"Failed to update heartbeat for experiment {self._status_tracker.expid}: {exc}"
                )
                return False

    def start(self) -> bool:
        """Start the background thread that updates the heartbeat.

        :return: True if the thread started successfully, False otherwise.
        :rtype: bool
        """
        # If the thread is already running, do nothing
        if self._thread is not None and self._thread.is_alive():
            return True

        # clear the stop event in case it was previously set
        self._stop_event.clear()
        if not self.ping():
            self._stop_event.set()
            return False
        try:
            # Start the background thread
            self._thread = threading.Thread(
                target=self._run,
                name=f"autosubmit-heartbeat-{self._status_tracker.expid}",
                daemon=True,
            )  # set thread as daemon to automatically stop when the parent process exits
            # start the watchdog thread
            self._thread.start()
            return True
        except Exception:
            self._thread = None
            self._stop_event.set()
            Log.warning(
                f"Failed to start the heartbeat thread for experiment {self._status_tracker.expid}."
            )
            return False

    def stop(self, timeout: float = 10.0) -> None:
        """Stop the background thread that updates the heartbeat.

        :param timeout: Maximum time to wait for the thread to stop in seconds.
        :type timeout: float
        """
        self._stop_event.set()
        thread = self._thread
        if thread is not None:
            # Wait for the thread to finish
            thread.join(timeout=timeout)
            if thread.is_alive():
                Log.warning(
                    f"Heartbeat thread for experiment {self._status_tracker.expid} did not stop within the timeout."
                )
        self._thread = None


class ExperimentStatus:
    """Represents the Experiment Status Mechanism that keeps track of currently active experiments."""

    def __init__(
        self,
        expid,
        local_root_dir_path=DEFAULT_LOCAL_ROOT_DIR,
        historiclog_dir_path=DEFAULT_HISTORICAL_LOGS_DIR,
    ):
        # type : (str) -> None
        self.expid = expid  # type : str
        BasicConfig.read()
        try:
            options = {
                "expid": self.expid,
                "db_dir_path": BasicConfig.DB_DIR,
                "main_db_name": BasicConfig.DB_FILE,
                "local_root_dir_path": BasicConfig.LOCAL_ROOT_DIR,
            }
            self.manager = create_experiment_status_db_manager(
                BasicConfig.DATABASE_BACKEND, **options
            )
        except Exception:
            message = "Error while trying to update {0} in experiment_status.".format(
                str(self.expid)
            )
            Logging(self.expid, BasicConfig.HISTORICAL_LOG_DIR).log(
                message, traceback.format_exc()
            )
            raise

    def set_status(self, status: str) -> None:
        """
        Sets the status of the experiment in experiment_status of as_times.db.
        Creates the database, table and row if necessary.

        :param status: The status to set for the experiment.
        :dtype status: str
        :return: None
        """
        self.manager.set_exp_status(self.expid, status)

    def set_as_running(self) -> None:
        """
        Set the status of the experiment in experiment_status of as_times.db as RUNNING.
        Creates the database, table and row if necessary.

        :param status: The status to set for the experiment.
        :dtype status: str
        :return: None
        """
        self.manager.set_exp_status(self.expid, Models.RunningStatus.RUNNING)

    def set_as_not_running(self) -> None:
        """
        Set the status of the experiment in experiment_status of as_times.db as NOT_RUNNING.
        Creates the database, table and row if necessary.

        :param status: The status to set for the experiment.
        :dtype status: str
        :return: None
        """
        self.manager.set_exp_status(self.expid, Models.RunningStatus.NOT_RUNNING)

    def set_as_deleted(self) -> None:
        """
        Set the status of the experiment in experiment_status of as_times.db as DELETED.
        Creates the database, table and row if necessary.

        :param status: The status to set for the experiment.
        :dtype status: str
        :return: None
        """
        self.manager.set_exp_status(self.expid, Models.RunningStatus.DELETED)

    def set_as_archived(self) -> None:
        """
        Set the status of the experiment in experiment_status of as_times.db as ARCHIVED.
        Creates the database, table and row if necessary.

        :param status: The status to set for the experiment.
        :dtype status: str
        :return: None
        """
        self.manager.set_exp_status(self.expid, Models.RunningStatus.ARCHIVED)
    
    def update_heartbeat(self) -> None:
        """
        Updates the heartbeat of the experiment in experiment_status of as_times.db.
        Creates the database, table and row if necessary.
        
        :return: None
        """
        self.manager.update_heartbeat(self.expid)
    
    def heartbeat_monitor(self, interval_seconds: int = 120) -> ExperimentHeartBeatMonitor:
        """ Creates an ExperimentHeartBeatMonitor for the experiment with the specified interval for updating the heartbeat.

        :param interval_seconds: The interval in seconds at which the heartbeat should be updated. Default is 120 seconds.
        :type interval_seconds: int
        :return: An instance of ExperimentHeartBeatMonitor for the experiment.
        :rtype: ExperimentHeartBeatMonitor
        """
        return ExperimentHeartBeatMonitor(self, interval_seconds)

