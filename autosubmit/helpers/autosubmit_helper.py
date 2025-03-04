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

import datetime
import sys
from time import sleep
from typing import List

from autosubmit.database.db_common import check_experiment_exists
from autosubmit.history.experiment_history import ExperimentHistory
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from log.log import AutosubmitCritical, Log


def handle_start_time(start_time: str) -> None:
    """ Wait until the supplied time. """
    if start_time:
        Log.info("User provided starting time has been detected.")
        # current_time = time()
        datetime_now = datetime.datetime.now()
        target_date = parsed_time = None
        try:
            # Trying first parse H:M:S
            parsed_time = datetime.datetime.strptime(start_time, "%H:%M:%S")
            target_date = datetime.datetime(datetime_now.year, datetime_now.month,
                                            datetime_now.day, parsed_time.hour, parsed_time.minute, parsed_time.second)
        except Exception as e:
            try:
                # Trying second parse y-m-d H:M:S
                target_date = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
            except Exception as e:
                target_date = None
                Log.critical(
                    "The string input provided as the starting time of your experiment must have the format 'H:M:S' or 'yyyy-mm-dd H:M:S'. Your input was '{0}'.".format(
                        start_time))
                return
        # Must be in the future
        if target_date < datetime.datetime.now():
            Log.critical(
                "You must provide a valid date into the future. Your input was interpreted as '{0}', which is considered past.\nCurrent time {1}.".format(
                    target_date.strftime("%Y-%m-%d %H:%M:%S"), datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            return
        # Starting waiting sequence
        Log.info("Your experiment will start execution on {0}\n".format(target_date.strftime("%Y-%m-%d %H:%M:%S")))
        # Check time every second
        while datetime.datetime.now() < target_date:
            elapsed_time = target_date - datetime.datetime.now()
            sys.stdout.write("\r{0} until execution starts".format(elapsed_time))
            sys.stdout.flush()
            sleep(1)


def handle_start_after(start_after, expid, BasicConfig):
    # type: (str, str, BasicConfig) -> None
    """ Wait until the start_after experiment has finished."""
    if start_after:
        Log.info("User provided expid completion trigger has been detected.")
        # The user tries to be tricky
        if str(start_after) == str(expid):
            Log.info(
                "Hey! What do you think is going to happen? In theory, your experiment will run again after it has been completed. Good luck!")
        # Check if experiment exists. If False or None, it does not exist
        if not check_experiment_exists(start_after):
            return None
        # Historical Database: We use the historical database to retrieve the current progress data of the supplied expid (start_after)
        exp_history = ExperimentHistory(start_after, jobdata_dir_path=BasicConfig.JOBDATA_DIR,
                                        historiclog_dir_path=BasicConfig.HISTORICAL_LOG_DIR)
        if exp_history.is_header_ready() is False:
            Log.critical(
                "Experiment {0} is running a database version which is not supported by the completion trigger function. An updated DB version is needed.".format(
                    start_after))
            return
        Log.info(
            "Autosubmit will start monitoring experiment {0}. When the number of completed jobs plus suspended jobs becomes equal to the total number of jobs of experiment {0}, experiment {1} will start. Querying every 60 seconds. Status format Completed/Queuing/Running/Suspended/Failed.".format(
                start_after, expid))
        while True:
            # Query current run
            current_run = exp_history.manager.get_experiment_run_dc_with_max_id()
            if current_run and current_run.finish > 0 and current_run.total > 0 and current_run.completed + current_run.suspended == current_run.total:
                break
            else:
                sys.stdout.write(
                    "\rExperiment {0} ({1} total jobs) status {2}/{3}/{4}/{5}/{6}".format(start_after,
                                                                                          current_run.total,
                                                                                          current_run.completed,
                                                                                          current_run.queuing,
                                                                                          current_run.running,
                                                                                          current_run.suspended,
                                                                                          current_run.failed))
                sys.stdout.flush()
            # Update every 60 seconds
            sleep(60)


def get_allowed_members(run_members: str, as_conf: AutosubmitConfig) -> dict:
    """Check if the members sent are allowed

   :param run_members: str
   :param as_conf: AutosubmitConfig

   :return: dict
    """
    if run_members is not None:
        allowed_members = run_members.split()
        rmember = [rmember for rmember in allowed_members if rmember not in as_conf.get_member_list()]
        if len(rmember) > 0:
            raise AutosubmitCritical(
                "Some of the members ({0}) in the list of allowed members you supplied do not exist in the current list " +
                "of members specified in the conf files.\nCurrent list of members: {1}".format(str(rmember),
                                                                                               str(as_conf.get_member_list())))
        if len(allowed_members) == 0:
            raise AutosubmitCritical("Not a valid -rom --run_only_members input: {0}".format(str(run_members)))
        return allowed_members
    return []
