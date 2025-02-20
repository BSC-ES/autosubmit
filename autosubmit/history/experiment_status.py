#!/usr/bin/env python3

# Copyright 2015-2020 Earth Sciences Department, BSC-CNS
# This file is part of Autosubmit.

# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

import traceback
from .database_managers.experiment_status_db_manager import create_experiment_status_db_manager
from .database_managers.database_manager import DEFAULT_LOCAL_ROOT_DIR, DEFAULT_HISTORICAL_LOGS_DIR
from .internal_logging import Logging
from autosubmitconfigparser.config.basicconfig import BasicConfig

class ExperimentStatus:
  """ Represents the Experiment Status Mechanism that keeps track of currently active experiments """
  def __init__(self, expid, local_root_dir_path=DEFAULT_LOCAL_ROOT_DIR, historiclog_dir_path=DEFAULT_HISTORICAL_LOGS_DIR):
    # type : (str) -> None
    self.expid = expid # type : str
    BasicConfig.read()
    try:
      options = {
        'expid': self.expid,
        'db_dir_path': BasicConfig.DB_DIR,
        'main_db_name': BasicConfig.DB_FILE,
        'local_root_dir_path': BasicConfig.LOCAL_ROOT_DIR,
      }
      self.manager = create_experiment_status_db_manager(BasicConfig.DATABASE_BACKEND, **options)
    except Exception as exp:
      message = "Error while trying to update {0} in experiment_status.".format(str(self.expid))      
      Logging(self.expid, BasicConfig.HISTORICAL_LOG_DIR).log(message, traceback.format_exc())
      self.manager = None
                            
  def set_as_running(self):
    # type : () -> None
    """ Set the status of the experiment in experiment_status of as_times.db as RUNNING. Creates the database, table and row if necessary."""
    if self.manager:
      exp_status_row = self.manager.get_experiment_status_row_by_expid(self.expid)
      if exp_status_row:
        self.manager.set_existing_experiment_status_as_running(exp_status_row.name)
      else:
        exp_row = self.manager.get_experiment_row_by_expid(self.expid)
        self.manager.create_experiment_status_as_running(exp_row)
