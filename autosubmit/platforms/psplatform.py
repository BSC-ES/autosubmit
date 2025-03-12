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
from xml.dom.minidom import parseString

from autosubmit.platforms.paramiko_platform import ParamikoPlatform
from autosubmit.platforms.headers.ps_header import PsHeader


class PsPlatform(ParamikoPlatform):
    """
    Class to manage jobs to host not using any scheduler

    :param expid: experiment's identifier
    :type expid: str
    """

    def get_checkAlljobs_cmd(self, jobs_id):
        pass

    def parse_Alljobs_output(self, output, job_id):
        pass

    def parse_queue_reason(self, output, job_id):
        pass

    def __init__(self, expid, name, config):
        ParamikoPlatform.__init__(self, expid, name, config)
        self.mkdir_checker = None
        self.remove_checker = None
        self.mkdir_cmd = None
        self.get_cmd = None
        self.put_cmd = None
        self._checkhost_cmd = None
        self.cancel_cmd = None
        self._header = PsHeader()
        self.job_status = dict()
        self.job_status['COMPLETED'] = ['1']
        self.job_status['RUNNING'] = ['0']
        self.job_status['QUEUING'] = []
        self.job_status['FAILED'] = []
        self.update_cmds()

    def create_a_new_copy(self):
        return PsPlatform(self.expid, self.name, self.config)

    def submit_Script(self, hold=False):
        pass

    def update_cmds(self):
        """
        Updates commands for platforms
        """
        self.root_dir = os.path.join(self.scratch, self.project_dir, self.user, self.expid)
        self.remote_log_dir = os.path.join(self.root_dir, "LOG_" + self.expid)
        self.cancel_cmd = "kill -SIGINT"
        self._checkhost_cmd = "echo 1"
        self.put_cmd = "scp"
        self.get_cmd = "scp"
        self.mkdir_cmd = "mkdir -p " + self.remote_log_dir
        self.remove_checker = "rm -rf " + os.path.join(self.scratch, self.project_dir,self.user,"ps_permission_checker_azxbyc")
        self.mkdir_checker = "mkdir -p " + os.path.join(self.scratch, self.project_dir,self.user,"ps_permission_checker_azxbyc")

    def get_checkhost_cmd(self):
        return self._checkhost_cmd

    def get_remote_log_dir(self):
        return self.remote_log_dir

    def get_mkdir_cmd(self):
        return self.mkdir_cmd

    def parse_job_output(self, output):
        return output

    def get_submitted_job_id(self, output, x11 = False):
        return output

    def jobs_in_queue(self):
        dom = parseString('')
        jobs_xml = dom.getElementsByTagName("JB_job_number")
        return [int(element.firstChild.nodeValue) for element in jobs_xml]

    def get_submit_cmd(self, job_script, job, hold=False, export=""):
        if export == "none" or export == "None" or export is None or export == "":
            export = ""
        else:
            export += " ; "
        return self.get_call(job_script, job, export=export, timeout=job.wallclock_in_seconds)

    def get_checkjob_cmd(self, job_id):
        return self.get_pscall(job_id)

    def check_Alljobs(self, job_list, as_conf, retries=5):
        for job,prev_status in job_list:
            self.check_job(job)

    def check_remote_permissions(self):
        try:
            try:
                self.send_command(self.remove_checker)
            except Exception as e:
                pass
            self.send_command(self.mkdir_checker)
            self.send_command(self.remove_checker)
            return True
        except Exception as e:
            return False
