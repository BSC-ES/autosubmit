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
from contextlib import suppress
from time import sleep
from typing import List, Union

from autosubmit.job.job_common import Status
from autosubmit.platforms.paramiko_platform import ParamikoPlatform
from autosubmit.platforms.headers.pjm_header import PJMHeader
from autosubmit.platforms.wrappers.wrapper_factory import PJMWrapperFactory
from log.log import AutosubmitCritical, AutosubmitError, Log

import textwrap
class PJMPlatform(ParamikoPlatform):
    """
    Class to manage jobs to host using PJM scheduler

    :param expid: experiment's identifier
    :type expid: str
    """


    def __init__(self, expid, name, config):
        ParamikoPlatform.__init__(self, expid, name, config)
        self.mkdir_cmd = None
        self.get_cmd = None
        self.put_cmd = None
        self._submit_hold_cmd = None
        self._submit_command_name = None
        self._submit_cmd = None
        self._checkhost_cmd = None
        self.cancel_cmd = None
        self._header = PJMHeader()
        self._wrapper = PJMWrapperFactory(self)
        #https://software.fujitsu.com/jp/manual/manualfiles/m220008/j2ul2452/02enz007/j2ul-2452-02enz0.pdf page 16
        self.job_status = dict()
        self.job_status['COMPLETED'] = ['EXT']
        self.job_status['RUNNING'] = ['RNO','RNE','RUN']
        self.job_status['QUEUING'] = ['ACC','QUE', 'RNA', 'RNP','HLD'] # TODO NOT SURE ABOUT HOLD HLD
        self.job_status['FAILED'] = ['ERR','CCL','RJT']
        self._pathdir = "\$HOME/LOG_" + self.expid
        self._allow_arrays = False
        self._allow_wrappers = True # NOT SURE IF WE NEED WRAPPERS
        self.update_cmds()
        self.config = config
        exp_id_path = os.path.join(self.config.get("LOCAL_ROOT_DIR"), self.expid)
        tmp_path = os.path.join(exp_id_path, "tmp")
        self._submit_script_path = os.path.join(
            tmp_path, self.config.get("LOCAL_ASLOG_DIR"), "submit_" + self.name + ".sh")
        self._submit_script_base_name = os.path.join(
            tmp_path, self.config.get("LOCAL_ASLOG_DIR"), "submit_")

    def create_a_new_copy(self):
        return PJMPlatform(self.expid, self.name, self.config)

    def generate_new_name_submit_script_file(self):
        if os.path.exists(self._submit_script_path):
            os.remove(self._submit_script_path)
        self._submit_script_path = self._submit_script_base_name + os.urandom(16).hex() + ".sh"

    def submit_error(self,output):
        """
        Check if the output of the submit command contains an error message.
        :param output: output of the submit cmd
        :return: boolean
        """
        return not all(part.lower() in output.lower() for part in ["pjsub", "[INFO] PJM 0000"])



    def process_batch_ready_jobs(self,valid_packages_to_submit,failed_packages,error_message="",hold=False):
        """
        Retrieve multiple jobs identifiers.
        :param valid_packages_to_submit:
        :param failed_packages:
        :param error_message:
        :param hold:
        :return:
        """
        try:
            valid_packages_to_submit = [ package for package in valid_packages_to_submit ]
            if len(valid_packages_to_submit) > 0:
                try:
                    jobs_id = self.submit_Script(hold=hold)
                except AutosubmitError as e:
                    jobnames = [job.name for job in valid_packages_to_submit[0].jobs]
                    for jobname in jobnames:
                        jobid = self.get_jobid_by_jobname(jobname)
                        #cancel bad submitted job if jobid is encountered
                        for id_ in jobid:
                            self.cancel_job(id_)
                    jobs_id = None
                    self.connected = False
                    if e.trace is not None:
                        has_trace_bad_parameters = self.submit_error(e.trace)
                    else:
                        e.trace = ""
                        has_trace_bad_parameters = False
                    if e.message is not None:
                        has_message_bad_parameters = self.submit_error(e.message)
                    else:
                        e.message = ""
                        has_message_bad_parameters = False
                    if has_trace_bad_parameters or has_message_bad_parameters or e.message.lower().find("invalid partition") != -1 or e.message.lower().find("invalid qos") != -1 or e.message.lower().find("scheduler is not installed") != -1 or e.message.lower().find("failed") != -1 or e.message.lower().find("not available") != -1:
                        error_msg = ""
                        for package_tmp in valid_packages_to_submit:
                            for job_tmp in package_tmp.jobs:
                                if job_tmp.section not in error_msg:
                                    error_msg += job_tmp.section + "&"
                        if has_trace_bad_parameters:
                            error_message+="Check job and queue specified in your JOBS definition in YAML. Sections that could be affected: {0}".format(error_msg[:-1])
                        else:
                            error_message+="\ncheck that {1} platform has set the correct scheduler. Sections that could be affected: {0}".format(
                                    error_msg[:-1], self.name)

                        if e.trace is None:
                            e.trace = ""
                        raise AutosubmitCritical(error_message,7014,e.message+"\n"+str(e.trace))
                except IOError as e:
                    raise AutosubmitError(
                        "IO issues ", 6016, str(e))
                except BaseException as e:
                    if str(e).find("scheduler") != -1:
                        raise AutosubmitCritical("Are you sure that [{0}] scheduler is the correct type for platform [{1}]?.\n Please, double check that {0} is loaded for {1} before autosubmit launch any job.".format(self.type.upper(),self.name.upper()),7070)
                    raise AutosubmitError(
                        "Submission failed, this can be due a failure on the platform", 6015, str(e))
                if jobs_id is None or len(jobs_id) <= 0:
                    raise AutosubmitError(
                        "Submission failed, this can be due a failure on the platform", 6015,"Jobs_id {0}".format(jobs_id))
                i = 0
                if hold:
                    sleep(10)

                for package in valid_packages_to_submit:
                    package.process_jobs_to_submit(jobs_id[i], hold)
                    i += 1
            save = True
        except AutosubmitError as e:
            raise
        except AutosubmitCritical as e:
            raise
        except Exception as e:
            raise AutosubmitError("{0} submission failed".format(self.name), 6015, str(e))
        return save,valid_packages_to_submit

    def generate_submit_script(self):
        # remove file
        with suppress(FileNotFoundError):
            os.remove(self._submit_script_path)
        self.generate_new_name_submit_script_file()

    def get_submit_script(self):
        os.chmod(self._submit_script_path, 0o750)
        return os.path.join(self.config.get("LOCAL_ASLOG_DIR"), os.path.basename(self._submit_script_path))

    def submit_job(self, job, script_name, hold=False, export="none"):
        """
        Submit a job from a given job object.

        :param export:
        :param job: job object
        :type job: autosubmit.job.job.Job
        :param script_name: job script's name
        :rtype scriptname: str
        :param hold: send job hold
        :type hold: boolean
        :return: job id for the submitted job
        :rtype: int
        """
        self.get_submit_cmd(script_name, job, hold=hold, export=export)
        return None


    def submit_Script(self, hold=False):
        # type: (bool) -> Union[List[str], str]
        """
        Sends a Submit file Script, execute it  in the platform and retrieves the Jobs_ID of all jobs at once.

        :param hold: if True, the job will be held
        :type hold: bool
        :return: job id for  submitted jobs
        :rtype: list(str)
        """
        try:
            self.send_file(self.get_submit_script(), False)
            cmd = os.path.join(self.get_files_path(),
                               os.path.basename(self._submit_script_path))
            # remove file after submisison
            cmd = f"{cmd} ; rm {cmd}"
            try:
                self.send_command(cmd)
            except AutosubmitError as e:
                raise
            except AutosubmitCritical as e:
                raise
            except Exception as e:
                raise
            jobs_id = self.get_submitted_job_id(self.get_ssh_output())
            return jobs_id
        except IOError as e:
            raise AutosubmitError("Submit script is not found, retry again in next AS iteration", 6008, str(e))
        except AutosubmitError as e:
            raise
        except AutosubmitCritical as e:
            raise
        except Exception as e:
            raise AutosubmitError("Submit script is not found, retry again in next AS iteration", 6008, str(e))
    def check_remote_log_dir(self):
        """
        Creates log dir on remote host
        """

        try:
            # Test if remote_path exists
            self._ftpChannel.chdir(self.remote_log_dir)
        except IOError as e:
            try:
                if self.send_command(self.get_mkdir_cmd()):
                    Log.debug('{0} has been created on {1} .',
                              self.remote_log_dir, self.host)
                else:
                    raise AutosubmitError("SFTP session not active ", 6007, "Could not create the DIR {0} on HPC {1}'.format(self.remote_log_dir, self.host)".format(
                        self.remote_log_dir, self.host))
            except BaseException as e:
                raise AutosubmitError(
                    "SFTP session not active ", 6007, str(e))

    def update_cmds(self):
        """
        Updates commands for platforms
        """
        self.root_dir = os.path.join(
            self.scratch, self.project_dir, self.user, self.expid)
        self.remote_log_dir = os.path.join(self.root_dir, "LOG_" + self.expid)
        self.cancel_cmd = "pjdel "
        self._checkhost_cmd = "echo 1"
        self._submit_cmd = 'cd {0} ; pjsub  '.format(self.remote_log_dir)
        self._submit_command_name = "pjsub"
        self._submit_hold_cmd = 'cd {0} ; pjsub  '.format(self.remote_log_dir)
        self.put_cmd = "scp"
        self.get_cmd = "scp"
        self.mkdir_cmd = "mkdir -p " + self.remote_log_dir

    def hold_job(self, job):
        try:
            cmd = "pjrls {0} ; sleep 2 ; pjhold -R ASHOLD {0}".format(job.id)
            self.send_command(cmd)
            job_status = self.check_job(job, submit_hold_check=True)
            if job_status == Status.RUNNING:
                self.send_command("{0} {1}".format(self.cancel_cmd,job.id))
                return False
            elif job_status == Status.FAILED:
                return False
            cmd = self.get_queue_status_cmd(job.id)
            self.send_command(cmd)
        except BaseException as e:
            try:
                self.send_command("{0} {1}".format(self.cancel_cmd,job.id))
                raise AutosubmitError(
                    "Can't hold jobid:{0}, canceling job".format(job.id), 6000, str(e))
            except BaseException as e:
                raise AutosubmitError(
                    "Can't cancel the jobid: {0}".format(job.id), 6000, str(e))
            except AutosubmitError as e:
                raise

    def get_checkhost_cmd(self):
        return self._checkhost_cmd

    def get_mkdir_cmd(self):
        return self.mkdir_cmd

    def get_remote_log_dir(self):
        return self.remote_log_dir

    def parse_job_output(self, output):
        return output.strip().split()[1].strip().strip("\n")

    def parse_job_finish_data(self, output, packed):
        return 0, 0, 0, 0, 0, 0, dict(), False

    def queuing_reason_cancel(self, reason):
        try:
            if len(reason.split('(', 1)) > 1:
                reason = reason.split('(', 1)[1].split(')')[0]
                if 'Invalid' in reason or reason in ['ANOTHER JOB STARTED','DELAY','DEADLINE SCHEDULE STARTED','ELAPSE LIMIT EXCEEDED','FILE IO ERROR','GATE CHECK','IMPOSSIBLE SCHED','INSUFF CPU','INSUFF MEMORY','INSUFF NODE','INSUFF','INTERNAL ERROR','INVALID HOSTFILE','LIMIT OVER MEMORY','LOST COMM','NO CURRENT DIR','NOT EXIST','RSCGRP NOT EXIST','RSCGRP STOP','RSCUNIT','USER','EXCEED','WAIT SCHED']:
                    return True
            return False
        except Exception as e:
            return False
    def get_queue_status(self, in_queue_jobs, list_queue_jobid, as_conf):
        if not in_queue_jobs:
            return
        cmd = self.get_queue_status_cmd(list_queue_jobid)
        self.send_command(cmd)
        queue_status = self._ssh_output
        for job in in_queue_jobs:
            reason = self.parse_queue_reason(queue_status, job.id)
            if job.queuing_reason_cancel(reason):
                Log.printlog(f"Job {job.name} will be cancelled and set to FAILED as it was queuing due to {reason}",6000)
                self.send_command(self.cancel_cmd + " {0}".format(job.id))
                job.new_status = Status.FAILED
                job.update_status(as_conf)
            elif reason.find('ASHOLD') != -1:
                job.new_status = Status.HELD
                if not job.hold:
                    self.send_command("{0} {1}".format(self.cancel_cmd,job.id))
                    job.new_status = Status.QUEUING  # If it was HELD and was released, it should be QUEUING next.
    def parse_Alljobs_output(self, output, job_id):
        status = ""
        try:
            status = [x.split()[1] for x in output.splitlines()
                      if x.split()[0] == str(job_id)]
        except BaseException as e:
            pass
        if len(status) == 0:
            return status
        return status[0]

    def parse_joblist(self, job_list):
        """
        Convert a list of job_list to job_list_cmd
        :param job_list: list of jobs
        :type job_list: list
        :return: job status
        :rtype: str
        """
        job_list_cmd = ""
        for job, job_prev_status in job_list:
            if job.id is None:
                job_str = "0"
            else:
                job_str = str(job.id)
            job_list_cmd += job_str + "+"
        if job_list_cmd[-1] == "+":
            job_list_cmd = job_list_cmd[:-1]

        return job_list_cmd
    def _check_jobid_in_queue(self, ssh_output, job_list_cmd):
        for job in job_list_cmd.split('+'):
            if job not in ssh_output:
                return False
        return True

    def get_submitted_job_id(self, outputlines):
        try:
            jobs_id = []
            for output in outputlines.splitlines():
                if not self.submit_error(output):
                    jobs_id.append(int(output.split()[5]))

            return jobs_id
        except IndexError:
            raise AutosubmitCritical(
                "Submission failed. There are issues on your config file", 7014)

    def get_submit_cmd(self, job_script, job, hold=False, export=""):
        if (export is None or str(export).lower() == "none") or len(export) == 0:
            export = ""
        else:
            export += " ; "


        with suppress(BaseException):
            lang = locale.getlocale()[1]
            if lang is None:
                lang = locale.getdefaultlocale()[1]
                if lang is None:
                    lang = 'UTF-8'
            with open(self._submit_script_path, "ab") as submit_script_file:
                if not hold:
                    submit_script_file.write((export + self._submit_cmd + job_script + "\n").encode(lang))
                else:
                    submit_script_file.write((export + self._submit_hold_cmd + job_script + "\n").encode(lang))


    def get_checkAlljobs_cmd(self, jobs_id):
        # jobs_id = "jobid1+jobid2+jobid3"
        # -H == sacct
        if jobs_id[-1] == ",":
            jobs_id = jobs_id[:-1] # deletes comma
        return "pjstat -H -v --choose jid,st,ermsg --filter \"jid={0}\" > as_checkalljobs.txt ; pjstat -v --choose jid,st,ermsg --filter \"jid={0}\" >> as_checkalljobs.txt ; cat as_checkalljobs.txt ; rm as_checkalljobs.txt".format(jobs_id)

    def get_checkjob_cmd(self, job_id):
        return f"pjstat -H -v --choose st --filter \"jid={job_id}\" > as_checkjob.txt ; pjstat -v --choose st --filter \"jid={job_id}\" >> as_checkjob.txt ; cat as_checkjob.txt ; rm as_checkjob.txt"

        #return 'pjstat -v --choose jid,st,ermsg --filter \"jid={0}\"'.format(job_id)
    def get_queue_status_cmd(self, job_id):
        return self.get_checkAlljobs_cmd(job_id)

    def get_jobid_by_jobname_cmd(self, job_name):
        if job_name[-1] == ",":
            job_name = job_name[:-1]
        return 'pjstat -v --choose jid,st,ermsg --filter \"jnam={0}\"'.format(job_name)



    def cancel_job(self, job_id):
        return '{0} {1}'.format(self.cancel_cmd,job_id)

    #def get_job_energy_cmd(self, job_id):
    #    return 'sacct -n --jobs {0} -o JobId%25,State,NCPUS,NNodes,Submit,Start,End,ConsumedEnergy,MaxRSS%25,AveRSS%25'.format(job_id)

    def parse_queue_reason(self, output, job_id):
        # split() is used to remove the trailing whitespace but also \t and multiple spaces
        # split(" ") is not enough
        reason = [x.split()[2] for x in output.splitlines()
                  if x.split()[0] == str(job_id)]
        # In case of duplicates we take the first one
        if len(reason) > 0:
            return reason[0]
        return reason

    def wrapper_header(self, **kwargs):
        wr_header = textwrap.dedent(f"""
    ###############################################################################
    #              {kwargs["name"].split("_")[0] + "_Wrapper"}
    ###############################################################################
    """)
        if kwargs["wrapper_data"].het.get("HETSIZE", 1) <= 1:
            wr_header += textwrap.dedent(f"""
    ###############################################################################
    #                   %TASKTYPE% %DEFAULT.EXPID% EXPERIMENT
    ###############################################################################
    #
    #PJM -N {kwargs["name"]}
    #PJM -L elapse={kwargs["wallclock"]}:00
    {kwargs["queue"]}
    {kwargs["partition"]}
    {kwargs["dependency"]}
    {kwargs["threads"]}
    {kwargs["nodes"]}
    {kwargs["num_processors"]}
    {kwargs["tasks"]}
    {kwargs["exclusive"]}
    {kwargs["custom_directives"]}

    #PJM -g {kwargs["project"]}
    #PJM -o {kwargs["name"]}.out
    #PJM -e {kwargs["name"]}.err
    #
    ###############################################################################
    
    
    #
        """).ljust(13)
        else:
            wr_header = self.calculate_wrapper_het_header(kwargs["wrapper_data"])
        if kwargs["method"] == 'srun':
            language = kwargs["executable"]
            if language is None or len(language) == 0:
                language = "#!/bin/bash"
            return language + wr_header
        else:
            language = kwargs["executable"]
            if language is None or len(language) == 0 or "bash" in language:
                language = "#!/usr/bin/env python3"
            return language + wr_header


    @staticmethod
    def allocated_nodes():
        return """os.system("scontrol show hostnames $SLURM_JOB_NODELIST > node_list_{0}".format(node_id))"""

    def check_file_exists(self, filename, wrapper_failed=False, sleeptime=5, max_retries=3):
        file_exist = False
        retries = 0

        while not file_exist and retries < max_retries:
            try:
                # This return IOError if path doesn't exist
                self._ftpChannel.stat(os.path.join(
                    self.get_files_path(), filename))
                file_exist = True
            except IOError as e:  # File doesn't exist, retry in sleeptime
                if not wrapper_failed:
                    sleep(sleeptime)
                    sleeptime = sleeptime + 5
                    retries = retries + 1
                else:
                    retries = 9999
            except BaseException as e:  # Unrecoverable error
                if str(e).lower().find("garbage") != -1:
                    if not wrapper_failed:
                        sleep(sleeptime)
                        sleeptime = sleeptime + 5
                        retries = retries + 1
                else:
                    Log.printlog("remote logs {0} couldn't be recovered".format(filename), 6001)
                    file_exist = False  # won't exist
                    retries = 999  # no more retries
        return file_exist
