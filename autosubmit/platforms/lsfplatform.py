#!/usr/bin/env python

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
from autosubmit.platforms.headers.lsf_header import LsfHeader
from autosubmit.platforms.wrappers.wrapper_factory import LSFWrapperFactory


class LsfPlatform(ParamikoPlatform):
    """
    Class to manage jobs to host using LSF scheduler

    :param expid: experiment's identifier
    :type expid: str
    """
    def __init__(self, expid, name, config):
        ParamikoPlatform.__init__(self, expid, name, config)
        self._header = LsfHeader()
        self._wrapper = LSFWrapperFactory(self)
        self.job_status = dict()
        self.job_status['COMPLETED'] = ['DONE']
        self.job_status['RUNNING'] = ['RUN']
        self.job_status['QUEUING'] = ['PEND', 'FW_PEND']
        self.job_status['FAILED'] = ['SSUSP', 'USUSP', 'EXIT']
        self._allow_arrays = True
        self._allow_wrappers = True
        self.update_cmds()

    def update_cmds(self):
        """
        Updates commands for platforms
        """
        self.root_dir = os.path.join(self.scratch, self.project_dir, self.user, self.expid)
        self.remote_log_dir = os.path.join(self.root_dir, "LOG_" + self.expid)
        self.cancel_cmd = "bkill"
        self._checkjob_cmd = "bjobs "
        self._checkhost_cmd = "echo 1"
        self._submit_cmd = "bsub -cwd " + self.remote_log_dir + " < " + self.remote_log_dir + "/"
        self._submit_command_name = "bsub"
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
        job_state = output.split('\n')
        if len(job_state) > 1:
            job_state = job_state[1].split()
            if len(job_state) > 2:
                return job_state[2]
        # If we can not process output, assuming completed. Then we will look for completed files and status will
        # change to failed if COMPLETED file is not present.
        return 'DONE'

    def get_submitted_job_id(self, output, x11 = False):
        return output.split('<')[1].split('>')[0]

    def jobs_in_queue(self):
        return zip(*[line.split() for line in ''.split('\n')])[0][1:]

    def get_checkjob_cmd(self, job_id):
        return self._checkjob_cmd + str(job_id)

    def get_submit_cmd(self, job_script, job, export=""):
        if export == "none" or export == "None" or export is None or export == "":
            export = ""
        else:
            export += " ; "
        return export + self._submit_cmd + job_script

    @staticmethod
    def wrapper_header(filename, queue, project, wallclock, num_procs, dependency, directives):
        return """\
        #!/usr/bin/env python
        ###############################################################################
        #              {0}
        ###############################################################################
        #
        #BSUB -J {0}
        #BSUB -q {1}
        #BSUB -P {2}
        #BSUB -oo {0}.out
        #BSUB -eo {0}.err
        #BSUB -W {3}
        #BSUB -n {4}
        {5}
        {6}
        #
        ###############################################################################
        """.format(filename, queue, project, wallclock, num_procs, dependency,
                   '\n'.ljust(13).join(str(s) for s in directives))
    # def connect(self):
    #     """
    #     In this case, it does nothing because connection is established for each command
    #
    #     :return: True
    #     :rtype: bool
    #     """
    #     self.connected = True
    # def restore_connection(self):
    #     """
    #     In this case, it does nothing because connection is established for each command
    #
    #     :return: True
    #     :rtype: bool
    #     """
    #     self.connected = True
    # def test_connection(self):
    #     """
    #     In this case, it does nothing because connection is established for each command
    #
    #     :return: True
    #     :rtype: bool
    #     """
    #     self.connected = True
