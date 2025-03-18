#!/usr/bin/env python3
from contextlib import suppress

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
from datetime import datetime
from time import mktime
from time import sleep
from time import time
from typing import List, Union
from xml.dom.minidom import parseString

from autosubmit.job.job_common import Status, parse_output_number
from autosubmit.platforms.headers.slurm_header import SlurmHeader
from autosubmit.platforms.paramiko_platform import ParamikoPlatform
from autosubmit.platforms.wrappers.wrapper_factory import SlurmWrapperFactory
from log.log import AutosubmitCritical, AutosubmitError, Log

class SlurmPlatform(ParamikoPlatform):
    """
    Class to manage jobs to host using SLURM scheduler

    :param expid: experiment's identifier
    :type expid: str
    """


    def __init__(self, expid, name, config, auth_password=None):
        ParamikoPlatform.__init__(self, expid, name, config, auth_password = auth_password)
        self.mkdir_cmd = None
        self.get_cmd = None
        self.put_cmd = None
        self._submit_hold_cmd = None
        self._submit_command_name = None
        self._submit_cmd = None
        self.x11_options = None
        self._submit_cmd_x11 = f'{self.remote_log_dir}'
        self._checkhost_cmd = None
        self.cancel_cmd = None
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
        exp_id_path = os.path.join(self.config.get("LOCAL_ROOT_DIR"), self.expid)
        tmp_path = os.path.join(exp_id_path, "tmp")
        self._submit_script_path = os.path.join(
            tmp_path, self.config.get("LOCAL_ASLOG_DIR"), "submit_" + self.name + ".sh")
        self._submit_script_base_name = os.path.join(
            tmp_path, self.config.get("LOCAL_ASLOG_DIR"), "submit_")

    def create_a_new_copy(self):
        return SlurmPlatform(self.expid, self.name, self.config)

    def get_submit_cmd_x11(self, args, script_name, job):
        """
        Returns the submit command for the platform
        """

        cmd = f'salloc {args} {self._submit_cmd_x11}/{script_name}'
        Log.debug(f"Salloc command: {cmd}")
        return cmd

    def generate_new_name_submit_script_file(self):
        if os.path.exists(self._submit_script_path):
            os.remove(self._submit_script_path)
        self._submit_script_path = self._submit_script_base_name + os.urandom(16).hex() + ".sh"

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
            valid_packages_to_submit = [ package for package in valid_packages_to_submit if package.x11 != True]
            if len(valid_packages_to_submit) > 0:
                duplicated_jobs_already_checked = False
                platform = valid_packages_to_submit[0].jobs[0].platform
                try:
                    jobs_id = self.submit_Script(hold=hold)
                except AutosubmitError as e:
                    jobnames = []
                    duplicated_jobs_already_checked = True
                    try:
                        for package_ in valid_packages_to_submit:
                            if hasattr(package_,"name"):
                                jobnames.append(package_.name) # wrapper_name
                            else:
                                jobnames.append(package_.jobs[0].name) # job_name
                        Log.error(f'TRACE:{e.trace}\n{e.message} JOBS:{jobnames}')
                        for jobname in jobnames:
                            jobid = self.get_jobid_by_jobname(jobname)
                            #cancel bad submitted job if jobid is encountered
                            for id_ in jobid:
                                self.send_command(self.cancel_job(id_))
                    except:
                        pass
                    jobs_id = None
                    self.connected = False
                    if e.trace is not None:
                        has_trace_bad_parameters = str(e.trace).lower().find("bad parameters") != -1
                    else:
                        has_trace_bad_parameters = False
                    if has_trace_bad_parameters or e.message.lower().find("invalid partition") != -1 or e.message.lower().find(" invalid qos") != -1 or e.message.lower().find("scheduler is not installed") != -1 or e.message.lower().find("failed") != -1 or e.message.lower().find("not available") != -1:
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

                        raise AutosubmitCritical(error_message, 7014, e.error_message)
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
                if hold:
                    sleep(10)
                jobid_index = 0
                for package in valid_packages_to_submit:
                    current_package_id = str(jobs_id[jobid_index])
                    if hold:
                        retries = 5
                        package.jobs[0].id = current_package_id
                        try:
                            can_continue = True
                            while can_continue and retries > 0:
                                cmd = package.jobs[0].platform.get_queue_status_cmd(current_package_id)
                                package.jobs[0].platform.send_command(cmd)
                                queue_status = package.jobs[0].platform._ssh_output
                                reason = package.jobs[0].platform.parse_queue_reason(queue_status, current_package_id)
                                if reason == '(JobHeldAdmin)':
                                    can_continue = False
                                elif reason == '(JobHeldUser)':
                                    can_continue = True
                                else:
                                    can_continue = False
                                    sleep(5)
                                retries = retries - 1
                            if not can_continue:
                                package.jobs[0].platform.send_command(package.jobs[0].platform.cancel_cmd + " {0}".format(current_package_id))
                                jobid_index += 1
                                continue
                            if not self.hold_job(package.jobs[0]):
                                jobid_index += 1
                                continue
                        except Exception as e:
                            failed_packages.append(current_package_id)
                            continue
                    package.process_jobs_to_submit(current_package_id, hold)
                    # Check if there are duplicated jobnames
                    if not duplicated_jobs_already_checked:
                        job_name = package.name if hasattr(package, "name") else package.jobs[0].name
                        jobid = self.get_jobid_by_jobname(job_name)
                        if len(jobid) > 1: # Cancel each job that is not the associated
                            ids_to_check = [package.jobs[0].id]
                            if package.jobs[0].het:
                                for i in range(1,package.jobs[0].het.get("HETSIZE",1)):
                                    ids_to_check.append(str(int(ids_to_check[0]) + i))
                            for id_ in [ jobid for jobid in jobid if jobid not in ids_to_check]:
                                self.send_command(self.cancel_job(id_)) # This can be faster if we cancel all jobs at once but there is no cancel_all_jobs call right now so todo in future
                                Log.debug(f'Job {id_} with the assigned name: {job_name} has been cancelled')
                            Log.debug(f'Job {package.jobs[0].id} with the assigned name: {job_name} has been submitted')
                    jobid_index += 1
                if len(failed_packages) > 0:
                    for job_id in failed_packages:
                        platform.send_command(platform.cancel_cmd + " {0}".format(job_id))
                    raise AutosubmitError("{0} submission failed, some hold jobs failed to be held".format(self.name), 6015)
            save = True
        except AutosubmitError as e:
            raise
        except AutosubmitCritical as e:
            raise
        except AttributeError:
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
        return self._submit_script_path

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
        if job is None or not job:
            x11 = False
        else:
            x11 = job.x11
        if not x11:
            self.get_submit_cmd(script_name, job, hold=hold, export=export)
            return None
        else:
            cmd = self.get_submit_cmd(script_name, job, hold=hold, export=export)
            if cmd is None:
                return None
            if self.send_command(cmd, x11=x11):
                job_id = self.get_submitted_job_id(self.get_ssh_output(), x11=x11)
                if job:
                    Log.result(f"Job: {job.name} submitted with job_id: {str(job_id).strip()} and workflow commit: {job.workflow_commit}")
                return int(job_id)
            else:
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
        self.cancel_cmd = "scancel"
        self._checkhost_cmd = "echo 1"
        self._submit_cmd = 'sbatch --no-requeue -D {1} {1}/'.format(
            self.host, self.remote_log_dir)
        self._submit_command_name = "sbatch"
        self._submit_hold_cmd = 'sbatch -H -D {1} {1}/'.format(
            self.host, self.remote_log_dir)
        # jobid =$(sbatch WOA_run_mn4.sh 2 > & 1 | grep -o "[0-9]*"); scontrol hold $jobid;
        self.put_cmd = "scp"
        self.get_cmd = "scp"
        self.mkdir_cmd = "mkdir -p " + self.remote_log_dir
        self._submit_cmd_x11 = f'{self.remote_log_dir}'


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
            reason = self.parse_queue_reason(queue_status, job.id)
            self.send_command(self.get_estimated_queue_time_cmd(job.id))
            estimated_time = self.parse_estimated_time(self._ssh_output)
            if reason == '(JobHeldAdmin)':  # Job is held by the system
                self.send_command("scancel {0}".format(job.id))
                return False
            else:
                Log.info(
                    f"The {job.name} will be eligible to run the day {estimated_time.get('date', 'Unknown')} at {estimated_time.get('time', 'Unknown')}\nQueuing reason is: {reason}")
                return True
        except BaseException as e:
            try:
                self.send_command("scancel {0}".format(job.id))
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
        return output.strip().split(' ')[0].strip()

    def parse_job_finish_data(self, output, packed):
        """Parses the context of the sacct query to SLURM for a single job.
        Only normal jobs return submit, start, finish, joules, ncpus, nnodes.

        When a wrapper has finished, capture finish time.

        :param output: The sacct output
        :type output: str
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
                # Take first line as source
                line = lines[0].strip().split()
                ncpus = int(line[2] if len(line) > 2 else 0)
                nnodes = int(line[3] if len(line) > 3 else 0)
                status = str(line[1])
                if packed is False:
                    # If it is not wrapper job, take first line as source
                    if status not in ["COMPLETED", "FAILED", "UNKNOWN"]:
                        # It is not completed, then its error and send default data plus output
                        return 0, 0, 0, 0, ncpus, nnodes, detailed_data, False
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
                            if is_end_of_wrapper is True:
                                finish = (int(mktime(datetime.strptime(line[6], "%Y-%m-%dT%H:%M:%S").timetuple(
                                ))) if len(line) > 6 and line[6] != "Unknown" else int(time()))
                                energy = parse_output_number(line[7]) if len(
                                    line) > 7 and len(line[7]) > 0 else 0
                            else:
                                # If packed but not end of wrapper, try to get info from current data.
                                if "finish" in list(extra_data.keys()) and extra_data["finish"] != "Unknown":
                                    # finish data exists
                                    finish = int(mktime(datetime.strptime(
                                        extra_data["finish"], "%Y-%m-%dT%H:%M:%S").timetuple()))
                                else:
                                    # if finish date does not exist, query previous step.
                                    if len(steps) >= 2 and detailed_data.__contains__(steps[-2]):
                                        new_extra_data = detailed_data[steps[-2]]
                                        if "finish" in list(new_extra_data.keys()) and new_extra_data["finish"] != "Unknown":
                                            # This might result in a job finish < start, need to handle that in the caller function
                                            finish = int(mktime(datetime.strptime(
                                                new_extra_data["finish"], "%Y-%m-%dT%H:%M:%S").timetuple()))
                                        else:
                                            finish = int(time())
                                    else:
                                        finish = int(time())
                                if "energy" in list(extra_data.keys()) and extra_data["energy"] != "NA":
                                    # energy exists
                                    energy = parse_output_number(
                                        extra_data["energy"])
                                else:
                                    # if energy does not exist, query previous step
                                    if len(steps) >= 2 and detailed_data.__contains__(steps[-2]):
                                        new_extra_data = detailed_data[steps[-2]]
                                        if "energy" in list(new_extra_data.keys()) and new_extra_data["energy"] != "NA":
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

                detailed_data = detailed_data if not packed or is_end_of_wrapper is True else extra_data
                return submit, start, finish, energy, ncpus, nnodes, detailed_data, is_end_of_wrapper

            return 0, 0, 0, 0, 0, 0, dict(), False
        except Exception as exp:
            Log.warning(
                "Autosubmit couldn't parse SLURM energy output. From parse_job_finish_data: {0}".format(str(exp)))
            return 0, 0, 0, 0, 0, 0, dict(), False

    def parse_Alljobs_output(self, output, job_id):
        status = ""
        try:
            status = [x.split()[1] for x in output.splitlines()
                      if x.split()[0][:len(str(job_id))] == str(job_id)]
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
            if x11:
                return int(outputlines.splitlines()[0])
            else:
                jobs_id = []
                for output in outputlines.splitlines():
                    jobs_id.append(int(output.split(' ')[3]))
                return jobs_id
        except IndexError:
            raise AutosubmitCritical(
                "Submission failed. There are issues on your config file", 7014)

    def jobs_in_queue(self):
        dom = parseString('')
        jobs_xml = dom.getElementsByTagName("JB_job_number")
        return [int(element.firstChild.nodeValue) for element in jobs_xml]

    def get_submit_cmd(self, job_script, job, hold=False, export=""):
        if (export is None or export.lower() == "none") or len(export) == 0:
            export = ""
        else:
            export += " ; "
        if job is None or not job:
            x11 = False
        else:
            x11 = job.x11

        if x11:
            return export + self.get_submit_cmd_x11(job.x11_options.strip(""), job_script.strip(""), job)
        else:
            try:
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
            except BaseException as e:
                pass

    def get_checkjob_cmd(self, job_id):
        return 'sacct -n -X --jobs {1} -o "State"'.format(self.host, job_id)

    def get_checkAlljobs_cmd(self, jobs_id):
        return "sacct -n -X --jobs {1} -o jobid,State".format(self.host, jobs_id)
    def get_estimated_queue_time_cmd(self, job_id):
        return f"scontrol -o show JobId {job_id} | grep -Po '(?<=EligibleTime=)[0-9-:T]*'"

    def get_queue_status_cmd(self, job_id):
        return 'squeue -j {0} -o %A,%R'.format(job_id)

    def get_jobid_by_jobname_cmd(self, job_name):
        return 'squeue -o %A,%.50j -n {0}'.format(job_name)


    def cancel_job(self, job_id):
        return 'scancel {0}'.format(job_id)

    def get_job_energy_cmd(self, job_id):
        return 'sacct -n --jobs {0} -o JobId%25,State,NCPUS,NNodes,Submit,Start,End,ConsumedEnergy,MaxRSS%25,AveRSS%25'.format(job_id)

    def parse_queue_reason(self, output, job_id):
        """
        Parses the queue reason from the output of the command
        :param output: output of the command
        :param job_id: job id
        :return: queue reason
        :rtype: str
        """
        reason = [x.split(',')[1] for x in output.splitlines()
                  if x.split(',')[0] == str(job_id)]
        if isinstance(reason,list):
            # convert reason to str
            return ''.join(reason)
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

    def wrapper_header(self,**kwargs):
        return self._header.wrapper_header(**kwargs)

    @staticmethod
    def allocated_nodes():
        return """os.system("scontrol show hostnames $SLURM_JOB_NODELIST > node_list_{0}".format(node_id))"""

    def check_file_exists(self, filename: str, wrapper_failed: bool = False, sleeptime: int = 5, max_retries: int = 3) -> bool:
        """
        Checks if a file exists on the FTP server.

        Args:
            filename (str): The name of the file to check.
            wrapper_failed (bool): Whether the wrapper has failed. Defaults to False.
            sleeptime (int): Time to sleep between retries in seconds. Defaults to 5.
            max_retries (int): Maximum number of retries. Defaults to 3.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        # Todo in a future refactor, check the sleeptime retrials of these function, previously it was waiting a lot of time
        file_exist = False
        retries = 0
        while not file_exist and retries < max_retries:
            try:
                # This return IOError if a path doesn't exist
                self._ftpChannel.stat(os.path.join(
                    self.get_files_path(), filename))
                file_exist = True
            except IOError as e:  # File doesn't exist, retry in sleeptime
                if not wrapper_failed:
                    sleep(sleeptime)
                    retries = retries + 1
                else:
                    sleep(2)
                    retries = retries + 1
            except BaseException as e:  # Unrecoverable error
                if str(e).lower().find("garbage") != -1:
                    sleep(2)
                    retries = retries + 1
                else:
                    file_exist = False  # won't exist
                    retries = 999  # no more retries
        if not file_exist:
            Log.warning("File {0} couldn't be found".format(filename))
        return file_exist
