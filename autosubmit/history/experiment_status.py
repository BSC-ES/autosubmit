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

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.history.database_managers.database_manager import DEFAULT_LOCAL_ROOT_DIR, DEFAULT_HISTORICAL_LOGS_DIR
from autosubmit.history.database_managers.experiment_status_db_manager import create_experiment_status_db_manager
from autosubmit.history.internal_logging import Logging


class ExperimentStatus:
    """Represents the Experiment Status Mechanism that keeps track of currently active experiments."""

    def __init__(self, expid, local_root_dir_path=DEFAULT_LOCAL_ROOT_DIR,
                 historiclog_dir_path=DEFAULT_HISTORICAL_LOGS_DIR):
        # type : (str) -> None
        self.expid = expid  # type : str
        BasicConfig.read()
        try:
            options = {
                'expid': self.expid,
                'db_dir_path': BasicConfig.DB_DIR,
                'main_db_name': BasicConfig.DB_FILE,
                'local_root_dir_path': BasicConfig.LOCAL_ROOT_DIR,
            }
            self.manager = create_experiment_status_db_manager(BasicConfig.DATABASE_BACKEND, **options)
        except Exception:
            message = "Error while trying to update {0} in experiment_status.".format(str(self.expid))
            Logging(self.expid, BasicConfig.HISTORICAL_LOG_DIR).log(message, traceback.format_exc())
            self.manager = None
        
    def set_status(self, status: str) -> None:
        if self.manager:
            self.manager.set_exp_status(self.expid, status)

    def set_as_running(self):
        if self.manager:
            self.manager.set_exp_status(self.expid, "RUNNING")
    
    def set_as_not_running(self) -> None:
        if self.manager:
            self.manager.set_exp_status(self.expid, "NOT_RUNNING")
    
    def set_as_deleted(self) -> None:
        if self.manager:
            self.manager.set_exp_status(self.expid, "DELETED")
    
    def set_as_archived(self) -> None:
        if self.manager:
            self.manager.set_exp_status(self.expid, "ARCHIVED")
