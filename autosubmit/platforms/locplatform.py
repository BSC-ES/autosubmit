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
from xml.dom.minidom import parseString
import subprocess


from autosubmit.platforms.paramiko_platform import ParamikoPlatform
from autosubmit.platforms.headers.local_header import LocalHeader

from autosubmitconfigparser.config.basicconfig import BasicConfig
from time import sleep
from log.log import Log, AutosubmitError, AutosubmitCritical
import threading
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
        self.update_cmds()

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
        wallclock = self.parse_time(job.wallclock)
        seconds = int(wallclock.days * 86400 + wallclock.seconds * 60)
        if export == "none" or export == "None" or export is None or export == "":
            export = ""
        else:
            export += " ; "
        return self.get_call(job_script, job, export=export,timeout=seconds)

    def get_checkjob_cmd(self, job_id):
        return self.get_pscall(job_id)

    def connect(self, as_conf, reconnect=False):
        self.connected = True
        if not self.log_retrieval_process_active and (
                as_conf is None or str(as_conf.platforms_data.get(self.name, {}).get('DISABLE_RECOVERY_THREADS',"false")).lower() == "false"):
            self.log_retrieval_process_active = True
            if as_conf and as_conf.misc_data.get("AS_COMMAND","").lower() == "run":
                self.recover_job_logs()


    def test_connection(self,as_conf):
        self.main_process_id = os.getpid()
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

    def send_file(self, filename, check=True):
        self.check_remote_log_dir()
        self.delete_file(filename,del_cmd=True)
        command = '{0} {1} {2}'.format(self.put_cmd, os.path.join(self.tmp_path, filename),
                                       os.path.join(self.tmp_path, 'LOG_' + self.expid, filename))
        try:
            subprocess.check_call(command, shell=True)
        except subprocess.CalledProcessError:
            Log.error('Could not send file {0} to {1}'.format(os.path.join(self.tmp_path, filename),
                                                              os.path.join(self.tmp_path, 'LOG_' + self.expid,
                                                                           filename)))
            raise
        return True


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
    def check_file_exists(self, src, wrapper_failed=False, sleeptime=5, max_retries=3, first=True):
        """
        Moves a file on the platform
        :param src: source name
        :type src: str
        :param: wrapper_failed: if True, the wrapper failed.
        :type wrapper_failed: bool

        """
        file_exist = False
        remote_path = os.path.join(self.get_files_path(), src)
        retries = 0
        # Not first is meant for vertical_wrappers. There you have to download STAT_{MAX_LOGS} then STAT_{MAX_LOGS-1} and so on
        if not first:
            max_retries = 1
            sleeptime = 0
        while not file_exist and retries < max_retries:
            try:
                file_exist = os.path.isfile(os.path.join(self.get_files_path(),src))
                if not file_exist:  # File doesn't exist, retry in sleep-time
                    if first:
                        Log.debug("{2} File does not exist.. waiting {0}s for a new retry (retries left: {1})", sleeptime,
                                 max_retries - retries, remote_path)
                    if not wrapper_failed:
                        sleep(sleeptime)
                        sleeptime = sleeptime + 5
                        retries = retries + 1
                    else:
                        retries = 9999
            except BaseException as e:  # Unrecoverable error
                Log.printlog("File does not exist, logs {0} {1}".format(self.get_files_path(),src),6001)
                file_exist = False  # won't exist
                retries = 999  # no more retries
        return file_exist

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
            raise AutosubmitError('File {0} does not exists, something went wrong with the platform'.format(
                path_root), 6004, str(e))
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
