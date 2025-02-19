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

import os

from autosubmit.platforms.paramiko_platform import ParamikoPlatform
from log.log import Log, AutosubmitCritical

from autosubmit.platforms.headers.pbs10_header import Pbs10Header
from autosubmit.platforms.headers.pbs11_header import Pbs11Header
from autosubmit.platforms.headers.pbs12_header import Pbs12Header


class PBSPlatform(ParamikoPlatform):
    """
    Class to manage jobs to host using PBS scheduler

    :param expid: experiment's identifier
    :type expid: str
    :param version: scheduler version
    :type version: str
    """

    def submit_Script(self, hold=False):
        pass

    def get_checkAlljobs_cmd(self, jobs_id):
        pass

    def parse_Alljobs_output(self, output, job_id):
        pass

    def __init__(self, expid, name, config, version):
        ParamikoPlatform.__init__(self, expid, name, config)
        self._checkjob_cmd = None
        self._submit_command_name = None
        self._submit_cmd = None
        self.mkdir_cmd = None
        self.get_cmd = None
        self.put_cmd = None
        self._checkhost_cmd = None
        self.cancel_cmd = None
        self._version = version

        if str.startswith(version, '10'):
            self._header = Pbs10Header()
        elif str.startswith(version, '11'):
            self._header = Pbs11Header()
        elif str.startswith(version, '12'):
            self._header = Pbs12Header()
        else:
            Log.error('PBS version {0} not supported'.format(version))
            raise AutosubmitCritical('PBS version {0} not supported'.format(version))

        self.job_status = dict()
        self.job_status['COMPLETED'] = ['F', 'E', 'c', 'C']
        self.job_status['RUNNING'] = ['R']
        self.job_status['QUEUING'] = ['Q', 'H', 'S', 'T', 'W', 'U', 'M']
        self.job_status['FAILED'] = ['Failed', 'Node_fail', 'Timeout']
        self.update_cmds()

    def create_a_new_copy(self):
        return PBSPlatform(self.expid, self.name, self.config, self.config[self.name.upper()].get('VERSION', ''))

    def parse_queue_reason(self, output, job_id):
        pass

    def update_cmds(self):
        """
        Updates commands for platforms
        """
        self.root_dir = os.path.join(self.scratch, self.project_dir, self.user, self.expid)
        self.remote_log_dir = os.path.join(self.root_dir, "LOG_" + self.expid)
        self.cancel_cmd = "ssh " + self.host + " qdel"
        self._checkhost_cmd = "ssh " + self.host + " echo 1"
        self.put_cmd = "scp"
        self.get_cmd = "scp"
        self.mkdir_cmd = "ssh " + self.host + " mkdir -p " + self.remote_log_dir
        self._submit_cmd = "ssh " + self.host + " qsub -d " + self.remote_log_dir + " " + self.remote_log_dir + "/ "
        self._submit_command_name = "qsub"
        if str.startswith(self._version, '11'):
            self._checkjob_cmd = "ssh " + self.host + " qstat"

    def get_checkhost_cmd(self):
        return self._checkhost_cmd


    def get_remote_log_dir(self):
        return self.remote_log_dir

    def check_Alljobs(self, job_list, as_conf, retries=5):
        for job,prev_status in job_list:
            self.check_job(job)

    def get_mkdir_cmd(self):
        return self.mkdir_cmd

    def parse_job_output(self, output):
        return output

    def get_submitted_job_id(self, output, x11 = False):
        return output.split('.')[0]

    def jobs_in_queue(self):
        return ''.split()

    def get_submit_cmd(self, job_script, job,hold=False, export=""):
        if export == "none" or export == "None" or export is None or export == "":
            export = ""
        else:
            export += " ; "
        return export + self._submit_cmd + job_script

    def get_checkjob_cmd(self, job_id):
        if str.startswith(self._version, '11'):
            return self._checkjob_cmd + str(job_id)
        else:
            return "ssh " + self.host + " " + self.get_qstatjob(job_id)
