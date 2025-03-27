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
from typing import Any

from autosubmit.platforms.paramiko_platform import ParamikoPlatform, ParamikoPlatformException
from log.log import Log,AutosubmitError
from autosubmit.platforms.headers.ec_header import EcHeader
from autosubmit.platforms.headers.ec_cca_header import EcCcaHeader
from autosubmit.platforms.headers.slurm_header import SlurmHeader
from autosubmit.platforms.wrappers.wrapper_factory import EcWrapperFactory
from time import sleep
import locale
class EcPlatform(ParamikoPlatform):
    """
    Class to manage queues with ecaccess

    :param expid: experiment's identifier
    :type expid: str
    :param scheduler: scheduler to use
    :type scheduler: str (pbs, loadleveler)
    """

    def parse_Alljobs_output(self, output, job_id):
        pass

    def parse_queue_reason(self, output, job_id):
        pass

    def get_checkAlljobs_cmd(self, jobs_id):
        pass

    def submit_Script(self, hold=False):
        pass

    def __init__(self, expid, name, config, scheduler):
        ParamikoPlatform.__init__(self, expid, name, config)
        #version=scheduler
        if scheduler == 'pbs':
            self._header = EcCcaHeader()
        elif scheduler == 'loadleveler':
            self._header = EcHeader()
        elif scheduler == 'slurm':
            self._header = SlurmHeader()
        else:
            raise ParamikoPlatformException('ecaccess scheduler {0} not supported'.format(scheduler))
        self._wrapper = EcWrapperFactory(self)
        self.job_status = dict()
        self.job_status['COMPLETED'] = ['DONE']
        self.job_status['RUNNING'] = ['EXEC']
        self.job_status['QUEUING'] = ['INIT', 'RETR', 'STDBY', 'WAIT']
        self.job_status['FAILED'] = ['STOP']
        self._pathdir = "\$HOME/LOG_" + self.expid
        self._allow_arrays = False
        self._allow_wrappers = False # TODO
        self._allow_python_jobs = False
        self.root_dir = ""
        self.remote_log_dir = ""
        self.cancel_cmd = ""
        self._checkjob_cmd = ""
        self._checkhost_cmd = ""
        self._submit_cmd = ""
        self._submit_command_name = ""
        self.put_cmd = ""
        self.get_cmd = ""
        self.del_cmd = ""
        self.mkdir_cmd = ""
        self.check_remote_permissions_cmd = ""
        self.check_remote_permissions_remove_cmd = ""
        self.update_cmds()
        self.scheduler = scheduler

    def update_cmds(self):
        """
        Updates commands for platforms
        """
        self.root_dir = os.path.join(self.scratch, self.project, self.user, self.expid)
        self.remote_log_dir = os.path.join(self.root_dir, "LOG_" + self.expid)
        self.cancel_cmd = "ecaccess-job-delete"
        self._checkjob_cmd = "ecaccess-job-list "
        self._checkhost_cmd = "ecaccess-certificate-list"
        self._checkvalidcert_cmd = "ecaccess-gateway-connected"
        self._submit_command_name = "ecaccess-job-submit"
        self.put_cmd = "ecaccess-file-put"
        self.get_cmd = "ecaccess-file-get"
        self.del_cmd = "ecaccess-file-delete"
        self.mkdir_cmd = ("ecaccess-file-mkdir " + self.host + ":" + self.scratch + "/" + self.project + "/" +
                          self.user + "/" + self.expid + "; " + "ecaccess-file-mkdir " + self.host + ":" +
                          self.remote_log_dir)
        self.check_remote_permissions_cmd = "ecaccess-file-mkdir " + self.host+":"+os.path.join(self.scratch,self.project,self.user,"_permission_checker_azxbyc")
        self.check_remote_permissions_remove_cmd = "ecaccess-file-rmdir " + self.host+":"+os.path.join(self.scratch,self.project,self.user,"_permission_checker_azxbyc")

    def get_checkhost_cmd(self):
        return self._checkhost_cmd

    def get_remote_log_dir(self):
        return self.remote_log_dir

    def get_mkdir_cmd(self):
        return self.mkdir_cmd

    def set_submit_cmd(self,ec_queue):
        self._submit_cmd = ("ecaccess-job-submit -distant -queueName " + ec_queue + " " + self.host + ":" +
                            self.remote_log_dir + "/")

    def check_Alljobs(self, job_list, as_conf, retries=5):
        for job,prev_status in job_list:
            self.check_job(job)

    def parse_job_output(self, output):
        job_state = output.split('\n')
        if len(job_state) > 7:
            job_state = job_state[7].split()
            if len(job_state) > 1:
                return job_state[1]
        return 'DONE'

    def get_submitted_job_id(self, output, x11 = False):
        return output

    def jobs_in_queue(self):
        """
        Returns empty list because ecacces does not support this command

        :return: empty list
        :rtype: list
        """
        return ''.split()

    def get_checkjob_cmd(self, job_id):
        return self._checkjob_cmd + str(job_id)

    def get_submit_cmd(self, job_script, job, hold=False, export=""):
        self.set_submit_cmd(job.ec_queue)
        if export == "none" or export == "None" or export is None or export == "":
            export = ""
        else:
            export += " ; "
        return export + self._submit_cmd + job_script

    def connect(self, as_conf, reconnect=False):
        """
        In this case, it does nothing because connection is established for each command

        :return: True
        :rtype: bool
        """
        output = subprocess.check_output(self._checkvalidcert_cmd, shell=True).decode(locale.getlocale()[1])
        if not output:
            output = ""
        try:
            if output.lower().find("yes") != -1:
                self.connected = True
            else:
                self.connected = False
        except Exception:
            self.connected = False
        self.spawn_log_retrieval_process(as_conf)

    def create_a_new_copy(self):
        return EcPlatform(self.expid, self.name, self.config, self.scheduler)

    def restore_connection(self,as_conf):
        """
        In this case, it does nothing because connection is established for each command

        :return: True
        :rtype: bool
        """
        output = subprocess.check_output(self._checkvalidcert_cmd, shell=True).decode(locale.getlocale()[1])
        if not output:
            output = ""
        try:
            if output.lower().find("yes") != -1:
                self.connected = True
            else:
                self.connected = False
        except Exception:
            self.connected = False

    def test_connection(self, as_conf: Any) -> None:
        """
        Tests the connection using the provided configuration.

        Args:
            as_conf (AutosubmitConfig): The configuration to use for testing the connection.
        """
        self.connect(as_conf)

    def check_remote_permissions(self):
        try:
            try:
                subprocess.check_output(self.check_remote_permissions_remove_cmd, shell=False)
            except Exception as e:
                pass
            subprocess.check_output(self.check_remote_permissions_cmd, shell=True)
            subprocess.check_output(self.check_remote_permissions_remove_cmd, shell=True)
            return True
        except Exception as e:
            return False

    def send_command(self, command, ignore_log=False, x11 = False):
        lang = locale.getlocale()[1] or locale.getdefaultlocale()[1] or 'UTF-8'
        err_message = 'command not executed'
        for _ in range(3):
            try:
                self._ssh_output = subprocess.check_output(command, shell=True).decode(lang)
            except Exception as e:
                err_message = str(e)
                sleep(1)
            else:
                break
        else:  # if break was not called in the for, all attemps failed!
            raise AutosubmitError(f'Could not execute command {command} on {self.host}', 7500, str(err_message))
        return True



    def send_file(self, filename, check=True):
        self.check_remote_log_dir()
        self.delete_file(filename)
        command = f'{self.put_cmd} {os.path.join(self.tmp_path, filename)} {self.host}:{os.path.join(self.get_files_path(), os.path.basename(filename))}'
        try:
            subprocess.check_call(command, shell=True)
        except subprocess.CalledProcessError as e:
            raise AutosubmitError('Could not send file {0} to {1}'.format(os.path.join(self.tmp_path, filename),os.path.join(self.get_files_path(), filename)),6005,str(e))
        return True

    def move_file(self, src, dest, must_exist = False):
        command = "ecaccess-file-move {0}:{1} {0}:{2}".format(self.host,os.path.join(self.remote_log_dir,src) , os.path.join(self.remote_log_dir,dest))
        try:
            retries = 0
            sleeptime = 5
            process_ok = False
            FNULL = open(os.devnull, 'w')
            while not process_ok and retries < 5:
                process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE,stderr=FNULL)
                out, _ = process.communicate()
                out=out.decode(locale.getlocale()[1])
                if 'No such file' in out or process.returncode != 0:
                    retries = retries + 1
                    process_ok = False
                    sleeptime = sleeptime + 5
                    sleep(sleeptime)
                else:
                    process_ok = True
        except Exception as e:
            process_ok = False
        if not process_ok:
            Log.printlog("Log file don't recovered {0}".format(src), 6004)
        return process_ok


    def get_file(self, filename, must_exist=True, relative_path='',ignore_log = False,wrapper_failed=False):
        local_path = os.path.join(self.tmp_path, relative_path)
        if not os.path.exists(local_path):
            os.makedirs(local_path)

        file_path = os.path.join(local_path, filename)
        if os.path.exists(file_path):
            os.remove(file_path)

        command = '{0} {3}:{2} {1}'.format(self.get_cmd, file_path, os.path.join(self.get_files_path(), filename),self.host)
        try:
            retries = 0
            sleeptime = 5
            process_ok = False
            FNULL = open(os.devnull, 'w')
            while not process_ok and retries < 5:
                process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE,stderr=FNULL)
                out, _ = process.communicate()
                out = out.decode(locale.getlocale()[1])
                if 'No such file' in out or process.returncode != 0:
                    retries = retries + 1
                    process_ok = False
                    sleeptime = sleeptime + 5
                    sleep(sleeptime)
                else:
                    process_ok = True
        except Exception as e:
            process_ok = False
        if not process_ok and must_exist:
            Log.printlog("Completed/Stat File don't recovered {0}".format(filename), 6004)
        if not process_ok:
            Log.printlog("Log file don't recovered {0}".format(filename), 6004)
        return process_ok

    def delete_file(self, filename):
        command = '{0} {1}:{2}'.format(self.del_cmd, self.host, os.path.join(self.get_files_path(), filename))
        try:
            FNULL = open(os.devnull, 'w')
            subprocess.check_call(command, stdout=FNULL,stderr=FNULL, shell=True)
        except subprocess.CalledProcessError:
            Log.debug('Could not remove file {0}',os.path.join(self.get_files_path(), filename) )
            return False
        return True

    def get_ssh_output(self):
        return self._ssh_output
    def get_ssh_output_err(self):
        return self._ssh_output_err
    @staticmethod
    def wrapper_header(filename, queue, project, wallclock, num_procs, expid, dependency, rootdir, directives):
        return """\
        #!/bin/bash
        ###############################################################################
        #              {0}
        ###############################################################################
        #
        #PBS -N {0}
        #PBS -q {1}
        #PBS -l EC_billing_account={2}
        #PBS -o {7}/LOG_{5}/{0}.out
        #PBS -e {7}/LOG_{5}/{0}.err
        #PBS -l walltime={3}:00
        #PBS -l EC_total_tasks={4}
        #PBS -l EC_hyperthreads=1
        {6}
        {8}
        #
        ###############################################################################
        """.format(filename, queue, project, wallclock, num_procs, expid, dependency, rootdir,
                   '\n'.ljust(13).join(str(s) for s in directives))
