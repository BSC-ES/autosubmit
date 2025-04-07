#!/usr/bin/env python3

# Copyright 2017-2020 Earth Sciences Department, BSC-CNS

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

from typing import Union
from autosubmit.platforms.slurmplatform import SlurmPlatform
from log.log import Log

class Slurm_ExamplePlatform(SlurmPlatform):
    """
    Class to manage slurm jobs
    """

    def __init__(self, expid: str, name: str, config: dict, auth_password: str=None):
        """
        Initialization of the Class ExamplePlatform

        :param expid: ID of the experiment which will instantiate the MaestroPlatform.
        :type expid: str
        :param name: Name of the platform to be instantiated.
        :type name: str
        :param config: Configuration of the platform, PATHS to Files and DB.
        :type config: dict
        :param auth_password: Authenticator's password.
        :type auth_password: str
        :return: None
        """
        SlurmPlatform.__init__(self, expid, name, config, auth_password = auth_password)
        # other

    def submit_job(self, job, script_name: str, hold: bool=False, export: str="none") -> Union[int, None]:
        """
        Submit a job from a given job object.

        :param job: Job object
        :type job: autosubmit.job.job.Job
        :param script_name: Name of the script of the job.
        :type script_name: str
        :param hold: Send job hold.
        :type hold: bool
        :param export: Set within the jobs.yaml, used to export environment script to use before the job is launched.
        :type export: str

        :return: job id for the submitted job.
        :rtype: int
        """
        Log.result(f"Job: {job.name}")
        Log.result(f"script_name: {script_name}")
        Log.result(f"hold: {hold}")
        Log.result(f"export: {export}")
        return None
