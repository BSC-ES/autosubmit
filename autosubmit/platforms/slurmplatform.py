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
from time import sleep
from time import mktime
from time import time
from datetime import datetime
from typing import List, Union
import traceback

from xml.dom.minidom import parseString

from autosubmit.job.job_common import Status, parse_output_number
from autosubmit.platforms.paramiko_platform import ParamikoPlatform
from autosubmit.platforms.headers.slurm_header import SlurmHeader
from autosubmit.platforms.wrappers.wrapper_factory import SlurmWrapperFactory
from log.log import AutosubmitCritical, AutosubmitError, Log


class SlurmPlatform(ParamikoPlatform):
    """
    Class to manage jobs to host using SLURM scheduler

    :param expid: experiment's identifier
    :type expid: str
    """

    def __init__(self, expid, name, config):
        ParamikoPlatform.__init__(self, expid, name, config)
        self._header = SlurmHeader()
        self._wrapper = SlurmWrapperFactory(self)
        self.job_status = dict()
        self.job_status['COMPLETED'] = ['COMPLETED']
        self.job_status['RUNNING'] = ['RUNNING']
        self.job_status['QUEUING'] = ['PENDING', 'CONFIGURING', 'RESIZING']
        self.job_status['FAILED'] = ['FAILED', 'CANCELLED', 'CANCELLED+', 'NODE_FAIL',
                                     'PREEMPTED', 'SUSPENDED', 'TIMEOUT', 'OUT_OF_MEMORY', 'OUT_OF_ME+', 'OUT_OF_ME']
        self._pathdir = "\$HOME/LOG_" + self.expid
        self._allow_arrays = False
        self._allow_wrappers = True
        self.update_cmds()
        self.config = config
        exp_id_path = os.path.join(config.LOCAL_ROOT_DIR, self.expid)
        tmp_path = os.path.join(exp_id_path, "tmp")
        self._submit_script_path = os.path.join(
            tmp_path, config.LOCAL_ASLOG_DIR, "submit_" + self.name + ".sh")
        self._submit_script_file = open(self._submit_script_path, 'w').close()

    def open_submit_script(self):
        self._submit_script_file = open(self._submit_script_path, 'w').close()
        self._submit_script_file = open(self._submit_script_path, 'a')

    def get_submit_script(self):
        self._submit_script_file.close()
        os.chmod(self._submit_script_path, 0o750)
        return os.path.join(self.config.LOCAL_ASLOG_DIR, os.path.basename(self._submit_script_path))

    def submit_Script(self, hold=False):
        # type: (bool) -> Union[List[str], str]
        """
        Sends a Submit file Script, execute it  in the platform and retrieves the Jobs_ID of all jobs at once.

        :param job: job object
        :type job: autosubmit.job.job.Job
        :return: job id for  submitted jobs
        :rtype: list(str)
        """
        try:
            self.send_file(self.get_submit_script(), False)
            cmd = os.path.join(self.get_files_path(),
                               os.path.basename(self._submit_script_path))
            try:
                self.send_command(cmd)
            except AutosubmitError as e:
                raise
            except Exception as e:
                raise
            jobs_id = self.get_submitted_job_id(self.get_ssh_output())
            return jobs_id
        except IOError as e:
            raise AutosubmitError("Submit script is not found, retry again in next AS iteration", 6008, e.message)
        except AutosubmitError as e:
            raise
        except AutosubmitCritical as e:
            raise
        except Exception as e:
            raise AutosubmitError("Submit script is not found, retry again in next AS iteration", 6008, str(e))

    def update_cmds(self):
        """
        Updates commands for platforms
        """
        self.root_dir = os.path.join(
            self.scratch, self.project_dir, self.user, self.expid)
        self.remote_log_dir = os.path.join(self.root_dir, "LOG_" + self.expid)
        self.cancel_cmd = "scancel"
        self._checkhost_cmd = "echo 1"
        self._submit_cmd = 'sbatch -D {1} {1}/'.format(
            self.host, self.remote_log_dir)
        self._submit_command_name = "sbatch"
        self._submit_hold_cmd = 'sbatch -H -D {1} {1}/'.format(
            self.host, self.remote_log_dir)
        # jobid =$(sbatch WOA_run_mn4.sh 2 > & 1 | grep -o "[0-9]*"); scontrol hold $jobid;
        self.put_cmd = "scp"
        self.get_cmd = "scp"
        self.mkdir_cmd = "mkdir -p " + self.remote_log_dir

    def hold_job(self, job):
        try:
            cmd = "scontrol release {0} ; sleep 2 ; scontrol hold {0} ".format(job.id)
            self.send_command(cmd)
            job_status = self.check_job(job, submit_hold_check=True)
            if job_status == Status.RUNNING:
                self.send_command("scancel {0}".format(job.id))
                return False
            elif job_status == Status.FAILED:
                return False
            cmd = self.get_queue_status_cmd(job.id)
            self.send_command(cmd)

            queue_status = self._ssh_output
            reason = str()
            reason = self.parse_queue_reason(queue_status, job.id)
            if reason == '(JobHeldUser)':
                return True
            else:
                self.send_command("scancel {0}".format(job.id))
                return False
        except BaseException as e:
            try:
                self.send_command("scancel {0}".format(job.id))
                raise AutosubmitError(
                    "Can't hold jobid:{0}, canceling job".format(job.id), 6000, e.message)
            except BaseException as e:
                raise AutosubmitError(
                    "Can't cancel the jobid: {0}".format(job.id), 6000, e.message)
            except AutosubmitError as e:
                raise

    def get_checkhost_cmd(self):
        return self._checkhost_cmd

    def get_mkdir_cmd(self):
        return self.mkdir_cmd

    def get_remote_log_dir(self):
        return self.remote_log_dir

    def parse_job_output(self, output):
        return output.strip().split(' ')[0].strip()

    def parse_job_finish_data(self, output, packed):
        """Parses the context of the sacct query to SLURM for a single job.
        Only normal jobs return submit, start, finish, joules, ncpus, nnodes.

        When a wrapper has finished, capture finish time.

        :param output: The sacct output
        :type output: str
        :param job_id: Id in SLURM for the job
        :type job_id: int
        :param packed: true if job belongs to package
        :type packed: bool
        :return: submit, start, finish, joules, ncpus, nnodes, detailed_data
        :rtype: int, int, int, int, int, int, json object (str)
        """
        try:
            # Setting up: Storing detail for posterity
            detailed_data = dict()
            steps = []
            # No blank spaces after or before
            output = output.strip() if output else None
            lines = output.split("\n") if output else []
            is_end_of_wrapper = False
            extra_data = None
            # If there is output, list exists
            if len(lines) > 0:
                # Collecting information from all output
                for line in lines:
                    line = line.strip().split()
                    if len(line) > 0:
                        # Collecting detailed data
                        name = str(line[0])
                        if packed:
                            # If it belongs to a wrapper
                            extra_data = {"ncpus": str(line[2] if len(line) > 2 else "NA"),
                                          "nnodes": str(line[3] if len(line) > 3 else "NA"),
                                          "submit": str(line[4] if len(line) > 4 else "NA"),
                                          "start": str(line[5] if len(line) > 5 else "NA"),
                                          "finish": str(line[6] if len(line) > 6 else "NA"),
                                          "energy": str(line[7] if len(line) > 7 else "NA"),
                                          "MaxRSS": str(line[8] if len(line) > 8 else "NA"),
                                          "AveRSS": str(line[9] if len(line) > 9 else "NA")}
                        else:
                            # Normal job
                            extra_data = {"submit": str(line[4] if len(line) > 4 else "NA"),
                                          "start": str(line[5] if len(line) > 5 else "NA"),
                                          "finish": str(line[6] if len(line) > 6 else "NA"),
                                          "energy": str(line[7] if len(line) > 7 else "NA"),
                                          "MaxRSS": str(line[8] if len(line) > 8 else "NA"),
                                          "AveRSS": str(line[9] if len(line) > 9 else "NA")}
                        # Detailed data will contain the important information from output
                        detailed_data[name] = extra_data
                        steps.append(name)
                submit = start = finish = energy = nnodes = ncpus = 0
                status = "UNKNOWN"
                # Take first line as source
                line = lines[0].strip().split()
                ncpus = int(line[2] if len(line) > 2 else 0)
                nnodes = int(line[3] if len(line) > 3 else 0)
                status = str(line[1])
                if packed == False:
                    # If it is not wrapper job, take first line as source
                    if status not in ["COMPLETED", "FAILED", "UNKNOWN"]:
                        # It not completed, then its error and send default data plus output
                        return (0, 0, 0, 0, ncpus, nnodes, detailed_data, False)
                else:
                    # If it is a wrapped job
                    # Check if the wrapper has finished
                    if status in ["COMPLETED", "FAILED", "UNKNOWN"]:
                        # Wrapper has finished
                        is_end_of_wrapper = True
                # Continue with first line as source
                if line:
                    try:
                        # Parse submit and start only for normal jobs (not packed)
                        submit = int(mktime(datetime.strptime(
                            line[4], "%Y-%m-%dT%H:%M:%S").timetuple())) if not packed else 0
                        start = int(mktime(datetime.strptime(
                            line[5], "%Y-%m-%dT%H:%M:%S").timetuple())) if not packed else 0
                        # Assuming the job has been COMPLETED
                        # If normal job or end of wrapper => Try to get the finish time from the first line of the output, else default to now.
                        finish = 0

                        if not packed:
                            # If normal job, take finish time from first line
                            finish = (int(mktime(datetime.strptime(line[6], "%Y-%m-%dT%H:%M:%S").timetuple(
                            ))) if len(line) > 6 and line[6] != "Unknown" else int(time()))
                            energy = parse_output_number(line[7]) if len(
                                line) > 7 and len(line[7]) > 0 else 0
                        else:
                            # If it is a wrapper job
                            # If end of wrapper, take data from first line
                            if is_end_of_wrapper == True:
                                finish = (int(mktime(datetime.strptime(line[6], "%Y-%m-%dT%H:%M:%S").timetuple(
                                ))) if len(line) > 6 and line[6] != "Unknown" else int(time()))
                                energy = parse_output_number(line[7]) if len(
                                    line) > 7 and len(line[7]) > 0 else 0
                            else:
                                # If packed but not end of wrapper, try to get info from current data.
                                if "finish" in extra_data.keys() and extra_data["finish"] != "Unknown":
                                    # finish data exists
                                    finish = int(mktime(datetime.strptime(
                                        extra_data["finish"], "%Y-%m-%dT%H:%M:%S").timetuple()))
                                else:
                                    # if finish date does not exist, query previous step.
                                    if len(steps) >= 2 and detailed_data.__contains__(steps[-2]):
                                        new_extra_data = detailed_data[steps[-2]]
                                        if "finish" in new_extra_data.keys() and new_extra_data["finish"] != "Unknown":
                                            # This might result in an job finish < start, need to handle that in the caller function
                                            finish = int(mktime(datetime.strptime(
                                                new_extra_data["finish"], "%Y-%m-%dT%H:%M:%S").timetuple()))
                                        else:
                                            finish = int(time())
                                    else:
                                        finish = int(time())
                                if "energy" in extra_data.keys() and extra_data["energy"] != "NA":
                                    # energy exists
                                    energy = parse_output_number(
                                        extra_data["energy"])
                                else:
                                    # if energy does not exist, query previous step
                                    if len(steps) >= 2 and detailed_data.__contains__(steps[-2]):
                                        new_extra_data = detailed_data[steps[-2]]
                                        if "energy" in new_extra_data.keys() and new_extra_data["energy"] != "NA":
                                            energy = parse_output_number(
                                                new_extra_data["energy"])
                                        else:
                                            energy = 0
                                    else:
                                        energy = 0
                    except Exception as exp:
                        # print(line)
                        # Log.info(traceback.format_exc())
                        Log.info(
                            "Parsing mishandling.")
                        # joules = -1
                        pass

                detailed_data = detailed_data if not packed or is_end_of_wrapper == True else extra_data
                return (submit, start, finish, energy, ncpus, nnodes, detailed_data, is_end_of_wrapper)

            return (0, 0, 0, 0, 0, 0, dict(), False)
        except Exception as exp:
            Log.warning(
                "Autosubmit couldn't parse SLURM energy output. From parse_job_finish_data: {0}".format(str(exp)))
            return (0, 0, 0, 0, 0, 0, dict(), False)

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

    def get_submitted_job_id(self, outputlines, x11 = False):
        try:
            if outputlines.find("failed") != -1:
                raise AutosubmitCritical(
                    "Submission failed. Command Failed", 7014)
            jobs_id = []
            for output in outputlines.splitlines():
                jobs_id.append(int(output.split(' ')[3]))
            if x11:
                return jobs_id[0]
            else:
                return jobs_id
        except IndexError:
            raise AutosubmitCritical(
                "Submission failed. There are issues on your config file", 7014)

    def jobs_in_queue(self):
        dom = parseString('')
        jobs_xml = dom.getElementsByTagName("JB_job_number")
        return [int(element.firstChild.nodeValue) for element in jobs_xml]

    def get_submit_cmd(self, job_script, job, hold=False, export=""):
        if export == "none" or export == "None" or export is None or export == "":
            export = ""
        else:
            export += " ; "
        if job is None:
            x11 = False
        else:
            x11 = job.x11

        if x11:
            if not hold:
                return export + self._submit_cmd + job_script
            else:
                return export + self._submit_hold_cmd + job_script
        else:
            if not hold:
                write_this = export + self._submit_cmd + job_script +"\n"
                self._submit_script_file.write(write_this)
            else:
                self._submit_script_file.write(
                    export + self._submit_hold_cmd + job_script + "\n")

    def get_checkjob_cmd(self, job_id):
        return 'sacct -n -X --jobs {1} -o "State"'.format(self.host, job_id)

    def get_checkAlljobs_cmd(self, jobs_id):
        return "sacct -n -X --jobs  {1} -o jobid,State".format(self.host, jobs_id)

    def get_queue_status_cmd(self, job_id):
        return 'squeue -j {0} -o %A,%R'.format(job_id)

    def get_jobid_by_jobname_cmd(self, job_name,minutes="5"):
        return "sacct --name {0} -o JobID -n -X --starttime=$(date -d '{1} minutes ago' +'%Y-%m-%dT%H:%M:%S')".format(job_name,minutes)


    def cancel_job(self, job_id):
        return 'scancel {0}'.format(job_id)

    def get_job_energy_cmd(self, job_id):
        return 'sacct -n --jobs {0} -o JobId%25,State,NCPUS,NNodes,Submit,Start,End,ConsumedEnergy,MaxRSS%25,AveRSS%25'.format(job_id)

    def parse_queue_reason(self, output, job_id):
        reason = [x.split(',')[1] for x in output.splitlines()
                  if x.split(',')[0] == str(job_id)]
        if len(reason) > 0:
            return reason[0]
        return reason

    def get_queue_status(self, in_queue_jobs, list_queue_jobid, as_conf):
        if not in_queue_jobs:
            return
        cmd = self.get_queue_status_cmd(list_queue_jobid)
        self.send_command(cmd)
        queue_status = self._ssh_output
        for job in in_queue_jobs:
            reason = self.parse_queue_reason(queue_status, job.id)
            if job.queuing_reason_cancel(reason): # this should be a platform method to be implemented
                Log.error(
                    "Job {0} will be cancelled and set to FAILED as it was queuing due to {1}", job.name, reason)
                self.send_command(
                    self.cancel_cmd + " {0}".format(job.id))
                job.new_status = Status.FAILED
                job.update_status(as_conf)
            elif reason == '(JobHeldUser)':
                if not job.hold:
                    # should be self.release_cmd or something like that but it is not implemented
                    self.send_command("scontrol release {0}".format(job.id))
                    job.new_status = Status.QUEUING  # If it was HELD and was released, it should be QUEUING next.
                else:
                    job.new_status = Status.HELD
    @staticmethod
    def wrapper_header(filename, queue, project, wallclock, num_procs, dependency, directives, threads, method="asthreads",language="python3"):
        if method == 'srun':
            language = "#!/bin/bash"
            return \
                language + """
###############################################################################
#              {0}
###############################################################################
#
#SBATCH -J {0}
{1}
#SBATCH -A {2}
#SBATCH --output={0}.out
#SBATCH --error={0}.err
#SBATCH -t {3}:00
#SBATCH -n {4}
#SBATCH --cpus-per-task={7}
{5}
{6}
#
###############################################################################
                """.format(filename, queue, project, wallclock, num_procs, dependency,
                           '\n'.ljust(13).join(str(s) for s in directives), threads)
        else:
            if language.lower() == "python" or language.lower() == "python3":
                language = "#!/usr/bin/env python3"
            else:
                language = "#!/usr/bin/env python2"
            return \
                language + """
###############################################################################
#              {0}
###############################################################################
#
#SBATCH -J {0}
{1}
#SBATCH -A {2}
#SBATCH --output={0}.out
#SBATCH --error={0}.err
#SBATCH -t {3}:00
#SBATCH --cpus-per-task={7}
#SBATCH -n {4}
{5}
{6}
#
###############################################################################
            """.format(filename, queue, project, wallclock, num_procs, dependency,
                       '\n'.ljust(13).join(str(s) for s in directives), threads)

    @staticmethod
    def allocated_nodes():
        return """os.system("scontrol show hostnames $SLURM_JOB_NODELIST > {0}".format(node_id))"""

    def check_file_exists(self, filename,wrapper_failed=False):
        file_exist = False
        sleeptime = 5
        retries = 0
        max_retries = 3
        while not file_exist and retries < max_retries:
            try:
                # This return IOError if path doesn't exist
                self._ftpChannel.stat(os.path.join(
                    self.get_files_path(), filename))
                file_exist = True
            except IOError as e:  # File doesn't exist, retry in sleeptime
                Log.debug("{2} File does not exist.. waiting {0}s for a new retry (retries left: {1})", sleeptime,
                          max_retries - retries, os.path.join(self.get_files_path(), filename))
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
