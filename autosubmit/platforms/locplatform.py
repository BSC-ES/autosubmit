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
import locale
import os
from pathlib import Path
from xml.dom.minidom import parseString
import subprocess
from autosubmit.platforms.paramiko_platform import ParamikoPlatform
from autosubmit.platforms.headers.local_header import LocalHeader
from autosubmit.platforms.wrappers.wrapper_factory import LocalWrapperFactory
from autosubmitconfigparser.config.basicconfig import BasicConfig
from time import sleep
from log.log import Log, AutosubmitError

class LocalPlatform(ParamikoPlatform):
    """
    Class to manage jobs to localhost

    :param expid: experiment's identifier
    :type expid: str
    """

    def submit_Script(self, hold=False):
        pass

    def parse_Alljobs_output(self, output, job_id):
        pass

    def parse_queue_reason(self, output, job_id):
        pass

    def get_checkAlljobs_cmd(self, jobs_id):
        pass

    def __init__(self, expid, name, config, auth_password = None):
        ParamikoPlatform.__init__(self, expid, name, config, auth_password= auth_password)
        self.cancel_cmd = None
        self.mkdir_cmd = None
        self.del_cmd = None
        self.get_cmd = None
        self.put_cmd = None
        self._checkhost_cmd = None
        self.type = 'local'
        self._header = LocalHeader()
        self.job_status = dict()
        self.job_status['COMPLETED'] = ['1']
        self.job_status['RUNNING'] = ['0']
        self.job_status['QUEUING'] = []
        self.job_status['FAILED'] = []
        self._allow_wrappers = True
        self._wrapper = LocalWrapperFactory(self)

        self.update_cmds()

    def create_a_new_copy(self):
        return LocalPlatform(self.expid, self.name, self.config)

    def update_cmds(self):
        """
        Updates commands for platforms
        """
        self.root_dir = os.path.join(BasicConfig.LOCAL_ROOT_DIR, self.expid)
        self.remote_log_dir = os.path.join(self.root_dir, "tmp", 'LOG_' + self.expid)
        self.cancel_cmd = "kill -SIGINT"
        self._checkhost_cmd = "echo 1"
        self.put_cmd = "cp -p"
        self.get_cmd = "cp"
        self.del_cmd = "rm -f"
        self.mkdir_cmd = "mkdir -p " + self.remote_log_dir

    def get_checkhost_cmd(self):
        return self._checkhost_cmd

    def get_remote_log_dir(self):
        return self.remote_log_dir

    def get_mkdir_cmd(self):
        return self.mkdir_cmd

    def parse_job_output(self, output):
        return output[0]

    def get_submitted_job_id(self, output, x11 = False):
        return output

    def jobs_in_queue(self):
        dom = parseString('')
        jobs_xml = dom.getElementsByTagName("JB_job_number")
        return [int(element.firstChild.nodeValue) for element in jobs_xml]

    def get_submit_cmd(self, job_script, job, hold=False, export=""):
        if job:  # Not intuitive at all, but if it is not a job, it is a wrapper
            seconds = job.wallclock_in_seconds
        else:
            # TODO for another branch this, it is to add a timeout to the wrapped jobs even if the wallclock is 0, default to 2 days
            seconds = 60*60*24*2
        if export == "none" or export == "None" or export is None or export == "":
            export = ""
        else:
            export += " ; "
        command = self.get_call(job_script, job, export=export, timeout=seconds)
        return f"cd {self.remote_log_dir} ; {command}"

    def get_checkjob_cmd(self, job_id):
        return self.get_pscall(job_id)

    def connect(self, as_conf={}, reconnect=False):
        self.connected = True
        self.spawn_log_retrieval_process(as_conf)

    def test_connection(self,as_conf):
        if not self.connected:
            self.connect(as_conf)


    def restore_connection(self,as_conf):
        self.connected = True

    def check_Alljobs(self, job_list, as_conf, retries=5):
        for job,prev_job_status in job_list:
            self.check_job(job)

    def send_command(self, command, ignore_log=False, x11 = False):
        lang = locale.getlocale()[1]
        if lang is None:
            lang = locale.getdefaultlocale()[1]
            if lang is None:
                lang = 'UTF-8'
        try:
            output = subprocess.check_output(command.encode(lang), shell=True)
        except subprocess.CalledProcessError as e:
            if not ignore_log:
                Log.error('Could not execute command {0} on {1}'.format(e.cmd, self.host))
            return False
        self._ssh_output = output.decode(lang)
        Log.debug("Command '{0}': {1}", command, self._ssh_output)

        return True

    def send_file(self, filename: str, check: bool = True) -> bool:
        """
        Sends a file to a specified location using a command.

        Args:
            filename (str): The name of the file to send.
            check (bool): Unused in this platform

        Returns:
            bool: True if the file was sent successfully.
        """
        command = f'{self.put_cmd} {os.path.join(self.tmp_path, Path(filename).name)} {os.path.join(self.tmp_path, "LOG_" + self.expid, Path(filename).name)}; chmod 770 {os.path.join(self.tmp_path, "LOG_" + self.expid, Path(filename).name)}'
        try:
            subprocess.check_call(command, shell=True)
        except subprocess.CalledProcessError:
            Log.error('Could not send file {0} to {1}'.format(os.path.join(self.tmp_path, filename),
                                                              os.path.join(self.tmp_path, 'LOG_' + self.expid,
                                                                           filename)))
            raise
        return True

    def remove_multiple_files(self, filenames: str) -> str:
        """
        Creates a shell script to remove multiple files in the remote and sets the appropriate permissions.

        Args:
            filenames (str): A string containing the filenames to be removed.

        Returns:
            str: An empty string.
        """
        # This function is a copy of the slurm one
        log_dir = os.path.join(self.tmp_path, 'LOG_{0}'.format(self.expid))
        multiple_delete_previous_run = os.path.join(
            log_dir, "multiple_delete_previous_run.sh")
        if os.path.exists(log_dir):
            lang = locale.getlocale()[1]
            if lang is None:
                lang = 'UTF-8'
            open(multiple_delete_previous_run, 'wb+').write(("rm -f" + filenames).encode(lang))
            os.chmod(multiple_delete_previous_run, 0o770)
        return ""

    def get_file(self, filename, must_exist=True, relative_path='',ignore_log = False,wrapper_failed=False):
        local_path = os.path.join(self.tmp_path, relative_path)
        if not os.path.exists(local_path):
            os.makedirs(local_path)
        file_path = os.path.join(local_path, filename)
        if os.path.exists(file_path):
            os.remove(file_path)

        command = '{0} {1} {2}'.format(self.get_cmd, os.path.join(self.tmp_path, 'LOG_' + self.expid, filename),
                                       file_path)
        try:        
            subprocess.check_call(command, stdout=open(os.devnull, 'w'), stderr=open(os.devnull, 'w'), shell=True)                      
        except subprocess.CalledProcessError:
            if must_exist:
                raise Exception('File {0} does not exists'.format(filename))
            return False
        return True

    def check_remote_permissions(self):
        return True

    # Moves .err .out
    def check_file_exists(self, src: str, wrapper_failed: bool = False, sleeptime: int = 1, max_retries: int = 1) -> bool:
        """
        Checks if a file exists in the platform.

        Args:
            src (str): Source name.
            wrapper_failed (bool): Checks inner jobs files. Defaults to False.
            sleeptime (int): Time to sleep between retries. Defaults to 1.
            max_retries (int): Maximum number of retries. Defaults to 1.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        # This function has a short sleep as the files are locally
        sleeptime = 1
        for i in range(max_retries):
            if os.path.isfile(os.path.join(self.get_files_path(), src)):
                return True
            sleep(sleeptime)
        Log.warning("File {0} does not exist".format(src))
        return False


    def delete_file(self, filename,del_cmd  = False):
        if del_cmd:
            command = '{0} {1}'.format(self.del_cmd, os.path.join(self.tmp_path,"LOG_"+self.expid, filename))
        else:
            command = '{0} {1}'.format(self.del_cmd, os.path.join(self.tmp_path,"LOG_"+self.expid, filename))
            command += ' ; {0} {1}'.format(self.del_cmd, os.path.join(self.tmp_path, filename))
        try:
            subprocess.check_call(command, shell=True)
        except subprocess.CalledProcessError:
            Log.debug('Could not remove file {0}'.format(os.path.join(self.tmp_path, filename)))
            return False
        return True
    def move_file(self, src, dest, must_exist=False):
        """
        Moves a file on the platform (includes .err and .out)
        :param src: source name
        :type src: str
        :param dest: destination name
        :param must_exist: ignore if file exist or not
        :type dest: str
        """
        path_root = ""
        try:
            path_root = self.get_files_path()
            os.rename(os.path.join(path_root, src),os.path.join(path_root, dest))
            return True
        except IOError as e:
            if must_exist:
                raise AutosubmitError("File {0} does not exists".format(
                    os.path.join(path_root,src)), 6004, str(e))
            else:
                Log.debug("File {0} doesn't exists ".format(path_root))
                return False
        except Exception as e:
            if str(e) in "Garbage":
                raise AutosubmitError('File {0} does not exists'.format(
                    os.path.join(self.get_files_path(), src)), 6004, str(e))
            if must_exist:
                raise AutosubmitError("File {0} does not exists".format(
                    os.path.join(self.get_files_path(), src)), 6004, str(e))
            else:
                Log.printlog("Log file couldn't be moved: {0}".format(
                    os.path.join(self.get_files_path(), src)), 5001)
                return False
    def get_ssh_output(self):
        return self._ssh_output
    def get_ssh_output_err(self):
        return self._ssh_output_err
    def get_logs_files(self, exp_id, remote_logs):
        """
        Overriding the parent's implementation.
        Do nothing because the log files are already in the local platform (redundancy).

        :param exp_id: experiment id
        :type exp_id: str
        :param remote_logs: names of the log files
        :type remote_logs: (str, str)
        """
        return

    def check_completed_files(self, sections: str = None) -> str:
        """
        Checks for completed files in the remote log directory.
        This function is used to check inner_jobs of a wrapper.

        Args:
            sections[str]: Space-separated string of sections to check for completed files. Defaults to None.

        Returns:
            str: The output if the command is successful, None otherwise.
        """
        # Clone of the slurm one.
        command = "find %s " % self.remote_log_dir
        if sections:
            for i, section in enumerate(sections.split()):
                command += " -name *%s_COMPLETED" % section
                if i < len(sections.split()) - 1:
                    command += " -o "
        else:
            command += " -name *_COMPLETED"

        if self.send_command(command, True):
            return self._ssh_output
        else:
            return None
