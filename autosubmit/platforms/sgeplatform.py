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
import subprocess

from xml.dom.minidom import parseString

from autosubmit.platforms.paramiko_platform import ParamikoPlatform
from autosubmit.platforms.headers.sge_header import SgeHeader


class SgePlatform(ParamikoPlatform):
    """
    Class to manage jobs to host using SGE scheduler

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
        self.mkdir_cmd = None
        self.get_cmd = None
        self.put_cmd = None
        self._submit_command_name = None
        self._submit_cmd = None
        self._checkhost_cmd = None
        self.cancel_cmd = None
        self._header = SgeHeader()
        self.job_status = dict()
        self.job_status['COMPLETED'] = ['c']
        self.job_status['RUNNING'] = ['r', 't', 'Rr', 'Rt']
        self.job_status['QUEUING'] = ['qw', 'hqw', 'hRwq', 'Rs', 'Rts', 'RS', 'RtS', 'RT', 'RtT']
        self.job_status['FAILED'] = ['Eqw', 'Ehqw', 'EhRqw', 's', 'ts', 'S', 'tS', 'T', 'tT', 'dr', 'dt', 'dRr', 'dRt',
                                     'ds', 'dS', 'dT', 'dRs', 'dRS', 'dRT']
        self._pathdir = "\$HOME/LOG_" + self.expid
        self.update_cmds()

    def submit_Script(self, hold=False):
        pass

    def update_cmds(self):
        """
        Updates commands for platforms
        """
        self.root_dir = os.path.join(self.scratch, self.project_dir, self.user, self.expid)
        self.remote_log_dir = os.path.join(self.root_dir, "LOG_" + self.expid)
        self.cancel_cmd = "qdel"
        self._checkhost_cmd = "echo 1"
        self._submit_cmd = "qsub -wd " + self.remote_log_dir + " " + self.remote_log_dir + "/"
        self._submit_command_name = "qsub"
        self.put_cmd = "scp"
        self.get_cmd = "scp"
        self.mkdir_cmd = "mkdir -p " + self.remote_log_dir

    def get_checkhost_cmd(self):
        return self._checkhost_cmd

    def get_mkdir_cmd(self):
        return self.mkdir_cmd

    def get_remote_log_dir(self):
        return self.remote_log_dir

    def parse_job_output(self, output):
        return output

    def check_Alljobs(self, job_list, as_conf, retries=5):
        for job,prev_status in job_list:
            self.check_job(job)

    def get_submitted_job_id(self, output, x11 = False):
        return output.split(' ')[2]

    def jobs_in_queue(self):
        output = subprocess.check_output('qstat -xml'.format(self.host), shell=True)
        dom = parseString(output)
        jobs_xml = dom.getElementsByTagName("JB_job_number")
        return [int(element.firstChild.nodeValue) for element in jobs_xml]

    def get_submit_cmd(self, job_script, job, hold=False, export=""):
        if (export is None or export.lower() == "none") or len(export) == 0:
            export = ""
        else:
            export += " ; "
        return export + self._submit_cmd + job_script

    def get_checkjob_cmd(self, job_id):
        return self.get_qstatjob(job_id)

    def connect(self, as_conf, reconnect=False):
        """
        In this case, it does nothing because connection is established for each command

        :return: True
        :rtype: bool
        """
        self.connected = True
        self.spawn_log_retrieval_process(as_conf) # This platform may be deprecated, so ignore the change


    def restore_connection(self,as_conf):
        """
        In this case, it does nothing because connection is established for each command

        :return: True
        :rtype: bool
        """
        self.connected = True

    def test_connection(self,as_conf):
        """
        In this case, it does nothing because connection is established for each command

        :return: True
        :rtype: bool
        """
        self.connected = True
        self.connected(as_conf,True) # This platform may be deprecated, so ignore the change
