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
from pathlib import Path
from time import sleep
from typing import Union, TYPE_CHECKING, Optional

from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.job.job_common import Status
from autosubmit.log.log import AutosubmitCritical, Log, AutosubmitError
from autosubmit.platforms.headers.pbs_header import PBSHeader
from autosubmit.platforms.paramiko_platform import ParamikoPlatform

if TYPE_CHECKING:
    # Avoid circular imports
    from autosubmit.job.job import Job


class PBSPlatform(ParamikoPlatform):
    """Class to manage jobs to host using PBS scheduler."""

    def __init__(self, expid: str, name: str, config: dict, auth_password: str = None) -> None:
        """Initialization of the Class PBSPlatform.

        :param expid: ID of the experiment which will instantiate the PBSPlatform.
        :type expid: str
        :param name: Name of the platform to be instantiated.
        :type name: str
        :param config: Configuration of the platform, PATHS to Files and DB.
        :type config: dict
        :param auth_password: Authenticator's password.
        :type auth_password: str
        :rtype: None
        """
        ParamikoPlatform.__init__(self, expid, name, config, auth_password=auth_password)
        self.mkdir_cmd = None
        self.get_cmd = None
        self.put_cmd = None
        self._submit_hold_cmd = None
        self._submit_command_name = None
        self._submit_cmd = None
        self.x11_options = None
        self._submit_cmd_x11 = f'{self.remote_log_dir}'
        self.cancel_cmd = None
        self.type = 'PBS'
        self._header = PBSHeader()
        self.job_status: dict = {'COMPLETED': ['FINISH'], 'RUNNING': ['RUNNING'],
                                 'QUEUING': ['QUEUED', 'BEGUN', 'HELD'],
                                 'FAILED': ['EXITING']}
        self._pathdir = "\$HOME/LOG_" + self.expid
        self._allow_arrays: bool = False
        self.update_cmds()
        self.config: dict = config
        exp_id_path: Path = Path(self.config.get("LOCAL_ROOT_DIR"), self.expid)
        tmp_path: Path = Path(exp_id_path, "tmp")
        self._submit_script_path: Path = Path(
            tmp_path, self.config.get("LOCAL_ASLOG_DIR"), "submit_" + self.name + ".sh")
        self._submit_script_base_name: Path = Path(
            tmp_path, self.config.get("LOCAL_ASLOG_DIR"), "submit_")

    def create_a_new_copy(self):
        """Return a copy of a PBSPlatform object with the same
        expid, name and config as the original.

        :return: A new platform type PBS
        :rtype: PBSPlatform
        """
        return PBSPlatform(self.expid, self.name, self.config)

    def get_header(self, job: 'Job', parameters: dict) -> str:
        """Gets the header to be used by the job.

        :param job: The job.
        :param parameters: Parameters dictionary.
        :return: Job header.
        """
        header = self.header.HEADER

        header = header.replace('%OUT_LOG_DIRECTIVE%', f"{job.name}.cmd.out.{job.fail_count}")
        header = header.replace('%ERR_LOG_DIRECTIVE%', f"{job.name}.cmd.err.{job.fail_count}")

        if job.het.get("HETSIZE", 0) <= 1:
            if hasattr(self.header, 'get_queue_directive'):
                header = header.replace(
                    '%QUEUE_DIRECTIVE%', self.header.get_queue_directive(job, parameters))
            if hasattr(self.header, 'get_tasks_per_node'):
                header = header.replace(
                    '%TASKS_PER_NODE_DIRECTIVE%', self.header.get_tasks_per_node(job, parameters))
            if hasattr(self.header, 'get_threads_per_task'):
                header = header.replace(
                    '%THREADS_PER_TASK_DIRECTIVE%', self.header.get_threads_per_task(job, parameters))
            if hasattr(self.header, 'get_custom_directives'):
                header = header.replace(
                    '%CUSTOM_DIRECTIVES%', self.header.get_custom_directives(job, parameters))
            if hasattr(self.header, 'get_account_directive'):
                header = header.replace(
                    '%ACCOUNT_DIRECTIVE%', self.header.get_account_directive(job, parameters))
            if hasattr(self.header, 'get_nodes_directive'):
                header = header.replace(
                    '%NODES_DIRECTIVE%', self.header.get_nodes_directive(job, parameters))
            if hasattr(self.header, 'get_reservation_directive'):
                header = header.replace(
                    '%RESERVATION_DIRECTIVE%', self.header.get_reservation_directive(job, parameters))
            if hasattr(self.header, 'get_memory_directive'):
                header = header.replace(
                    '%MEMORY_DIRECTIVE%', self.header.get_memory_directive(job, parameters))
            if hasattr(self.header, 'get_memory_per_task_directive'):
                header = header.replace(
                    '%MEMORY_PER_TASK_DIRECTIVE%', self.header.get_memory_per_task_directive(job, parameters))
        return header

    def generate_new_name_submit_script_file(self) -> None:
        """
        Delete the current file and generates a new one with a new name.

        :rtype: None
        """
        self._submit_script_path.unlink(missing_ok=True)
        self._submit_script_path = Path(str(self._submit_script_base_name) + os.urandom(16).hex() + ".sh")

    def process_batch_ready_jobs(self, valid_packages_to_submit, failed_packages: list[str],
                                 error_message: str = "", hold: bool = False) -> tuple[bool, list]:
        """
        Retrieve multiple jobs identifiers.

        :param valid_packages_to_submit: List of valid Job Packages to be processes
        :type valid_packages_to_submit: List[JobPackageBase]
        :param failed_packages: List of packages that have failed to be submitted
        :type failed_packages: list[str]
        :param error_message: concatenated error message
        :type error_message: str
        :param hold: if True, the job will be held for 5 retries
        :type hold: bool

        :return: retrieve the ID of the Jobs
        :rtype: tuple[bool, list[JobPackageBase]]
        """
        try:
            valid_packages_to_submit = [package for package in valid_packages_to_submit if package.x11 is not True]
            if len(valid_packages_to_submit) > 0:
                duplicated_jobs_already_checked = False
                platform = valid_packages_to_submit[0].jobs[0].platform
                try:
                    jobs_id = self.submit_script(hold=hold)
                except AutosubmitError as e:
                    job_names = []
                    duplicated_jobs_already_checked = True
                    with suppress(Exception):
                        for package_ in valid_packages_to_submit:
                            if hasattr(package_, "name"):
                                job_names.append(package_.name)  # wrapper_name
                            else:
                                job_names.append(package_.jobs[0].name)  # job_name
                        Log.error(f'TRACE:{e.trace}\n{e.message} JOBS:{job_names}')
                        for job_name in job_names:
                            jobs_id = self.get_jobs_id_by_job_name(job_name)
                            # cancel bad submitted job if jobs_id is encountered
                            for id_ in jobs_id:
                                self.send_command(self.cancel_job(id_))
                    jobs_id = None
                    self.connected = False
                    if e.trace is not None:
                        has_trace_bad_parameters = (e.trace.lower().find("violates resource limits") != -1 or
                                e.trace.lower().find("Illegal attribute or resource value") != -1)
                    else:
                        has_trace_bad_parameters = False
                    if (has_trace_bad_parameters or e.message.lower().find("violates resource limits") != -1 or
                            e.message.lower().find("Illegal attribute or resource value") != -1 or
                            e.message.lower().find("Unknown Resource") != -1 or
                            e.message.lower().find("Job violates queue") != -1 or
                            e.message.lower().find("scheduler is not installed") != -1 or
                            e.message.lower().find("failed") != -1 or e.message.lower().find("not available") != -1):
                        error_msg = ""
                        for package_tmp in valid_packages_to_submit:
                            for job_tmp in package_tmp.jobs:
                                if job_tmp.section not in error_msg:
                                    error_msg += job_tmp.section + "&"
                        if has_trace_bad_parameters:
                            error_message += (
                                f"Check job and queue specified in your JOBS definition in YAML. Sections "
                                f"that could be affected: {error_msg[:-1]}"
                            )
                        else:
                            error_message += (
                                f"\ncheck that {self.name} platform has set the correct scheduler. "
                                f"Sections that could be affected: {error_msg[:-1]}"
                            )

                        raise AutosubmitCritical(error_message, 7014, e.error_message) from e
                except IOError as e:
                    raise AutosubmitError("IO issues ", 6016, str(e)) from e
                except BaseException as e:
                    if str(e).find("scheduler") != -1:
                        raise AutosubmitCritical(
                            f"Are you sure that [{self.type.upper()}] scheduler is the "
                            f"correct type for platform [{self.name.upper()}]?.\n Please, double check that "
                            f"{self.type.upper()} is loaded for {self.name.upper()} before "
                            f"autosubmit launch any job.", 7070
                        ) from e
                    raise AutosubmitError(
                        "Submission failed, this can be due a failure on the platform", 6015, str(e)) from e
                if jobs_id is None or len(jobs_id) <= 0:
                    raise AutosubmitError(
                        "Submission failed, this can be due a failure on the platform",
                        6015, f"Jobs_id {jobs_id}")
                if hold:
                    sleep(10)
                jobs_id_index = 0
                for package in valid_packages_to_submit:
                    current_package_id = str(jobs_id[jobs_id_index])
                    if hold:
                        retries = 5
                        package.jobs[0].id = current_package_id
                        try:
                            while retries > 0:
                                cmd = package.jobs[0].platform.get_queue_status_cmd(current_package_id)
                                package.jobs[0].platform.send_command(cmd)
                                sleep(5)
                                retries = retries - 1
                                package.jobs[0].platform.send_command(
                                    package.jobs[0].platform.cancel_cmd + f" {current_package_id}")
                                jobs_id_index += 1
                                continue
                        except Exception:
                            failed_packages.append(current_package_id)
                            continue
                    package.process_jobs_to_submit(current_package_id, hold)
                    # Check if there are duplicated job_name
                    if not duplicated_jobs_already_checked:
                        job_name = package.name if hasattr(package, "name") else package.jobs[0].name
                        jobs_id = self.get_jobs_id_by_job_name(job_name)
                        if len(jobs_id) > 1:  # Cancel each job that is not the associated
                            ids_to_check = [package.jobs[0].id]
                            if package.jobs[0].het:
                                for i in range(1, package.jobs[0].het.get("HETSIZE", 1)):  # noqa
                                    ids_to_check.append(str(int(ids_to_check[0]) + i))
                            # TODO to optimize cancel all jobs at once
                            for id_ in [jobs_id for jobs_id in jobs_id if jobs_id not in ids_to_check]:
                                self.send_command(self.cancel_job(id_))
                                Log.debug(f'Job {id_} with the assigned name: {job_name} has been cancelled')
                            Log.debug(f'Job {package.jobs[0].id} with the assigned name: {job_name} has been submitted')
                    jobs_id_index += 1
                if len(failed_packages) > 0:
                    for job_id in failed_packages:
                        platform.send_command(platform.cancel_cmd + f" {job_id}")
                    raise AutosubmitError(f"{self.name} submission failed, some hold jobs failed to be held", 6015)
            save = True
        except AutosubmitError:
            raise
        except AutosubmitCritical:
            raise
        except AttributeError:
            raise
        except Exception as e:
            raise AutosubmitError(f"{self.name} submission failed", 6015, str(e)) from e
        return save, valid_packages_to_submit

    def generate_submit_script(self) -> None:
        """
        Delete the current file and generates a new one with a new name.

        :rtype: None
        """
        # remove file
        self._submit_script_path.unlink(missing_ok=True)
        self.generate_new_name_submit_script_file()

    def get_submit_script(self) -> str:
        """
        Change file permissions to 0o750 and return the path of the file.

        :return: Path to the file
        :rtype: str
        """
        self._submit_script_path.chmod(mode=0o750)
        return str(self._submit_script_path)

    def submit_job(self, job: 'Job', script_name: str, hold: bool = False, export: str = "none") -> Optional[
        list[Optional[int]]]:
        """
        Submit a job from a given job object.

        :param job: Job object
        :type job: autosubmit.job.job.Job
        :param script_name: Name of the script of the job.
        :type script_name: str
        :param hold: Send job hold.
        :type hold: bool
        :param export: Set within the jobs.yaml, used to export environment script to use before the job is launched.
        :type export: str

        :return: job id for the submitted job.
        :rtype: int
        """
        cmd = self.get_submit_cmd(script_name, job, hold=hold, export=export)
        if cmd is None:
            return None
        if self.send_command(cmd):
            job_id = self.get_submitted_job_id(self.get_ssh_output())
            if job and len(job_id) > 0:
                Log.result(f"Job: {job.name} submitted with job_id: {str(job_id).strip()} and workflow commit: "
                           f"{job.workflow_commit}")
            return job_id
        return None

    def submit_script(self, hold: bool = False) -> Union[list[int], int]:
        """
        Sends a Submit file Script with sbatch instructions, execute it in the platform and
        retrieves the Jobs_ID of all jobs at once.

        :param hold: Submit a job in held status. Held jobs will only earn priority status if the
            remote machine allows it.
        :type hold: bool
        :return: job id for submitted jobs.
        :rtype: Union[List[int], int]
        """
        try:
            self.send_file(self.get_submit_script(), False)
            cmd = os.path.join(self.get_files_path(),
                               os.path.basename(self._submit_script_path))
            # remove file after submission
            cmd = f"{cmd} ; rm {cmd}"
            try:
                self.send_command(cmd)
            except Exception:
                raise
            jobs_id = self.get_submitted_job_id(self.get_ssh_output())
            return jobs_id
        except IOError as e:
            raise AutosubmitError("Submit script is not found, retry again in next AS iteration", 6008, str(e)) from e
        except AutosubmitError:
            raise
        except AutosubmitCritical:
            raise
        except Exception as e:
            raise AutosubmitError("Submit script is not found, retry again in next AS iteration", 6008, str(e)) from e

    def check_remote_log_dir(self) -> None:
        """
        Creates log dir on remote host.

        :rtype: None
        """

        try:
            # Test if remote_path exists
            self._ftpChannel.chdir(self.remote_log_dir)
        except IOError as io_err:
            try:
                if self.send_command(self.get_mkdir_cmd()):
                    Log.debug(f'{self.remote_log_dir} has been created on {self.host}.')
                else:
                    raise AutosubmitError(
                        "SFTP session not active ", 6007,
                        f"Could not create the DIR {self.remote_log_dir} on HPC {self.host}"
                    ) from io_err
            except BaseException as e:
                raise AutosubmitError("SFTP session not active ", 6007, str(e)) from e

    def update_cmds(self) -> None:
        """
        Updates commands for platforms.

        :rtype: None
        """
        self.root_dir = os.path.join(
            self.scratch, self.project_dir, self.user, self.expid)
        self.remote_log_dir = os.path.join(self.root_dir, "LOG_" + self.expid)
        self.cancel_cmd = "qdel"
        self._submit_cmd = f'qsub {self.remote_log_dir}/'
        self._submit_command_name = "qsub"
        self._submit_hold_cmd = 'qhold '  # Needs the JOB_ID to hold a JOB
        self.put_cmd = "scp"
        self.get_cmd = "scp"
        self.mkdir_cmd = "mkdir -p " + self.remote_log_dir
        self._submit_cmd_x11 = f'{self.remote_log_dir}'

    def get_mkdir_cmd(self) -> str:
        """
        Get the variable mkdir_cmd that stores the mkdir command.

        :return: Mkdir command
        :rtype: str
        """
        return self.mkdir_cmd

    def get_remote_log_dir(self) -> str:
        """
        Get the variable remote_log_dir that stores the directory of the Log of the experiment.

        :return: The remote_log_dir variable.
        :rtype: str
        """
        return self.remote_log_dir

    def parse_job_output(self, output: str) -> str:
        """
        Parses check job command output, so it can be interpreted by autosubmit.

        :param output: output to parse.
        :type output: str

        :return: job status.
        :rtype: str
        """
        return output.strip().split(' ')[0].strip()

    def parse_all_jobs_output(self, output: str, job_id: str) -> Union[list[str], str]:  # noqa
        """
        Filter one or more status of a specific Job ID.

        :param output: Output of the status of the jobs.
        :type output: str
        :param job_id: job ID.
        :type job_id: int

        :return: All status related to a Job.
        :rtype: Union[list[str], str]
        """
        with suppress(Exception):
            output_lines = output.lower().split('\n')
            for output_line in output_lines:
                if 'job_id status' in output_line or 'no job' in output_line or 'miyabi stop' in output_line or \
                        output_line.strip() == '':
                    continue
                id, status = output_line.split(' ')
                if id == job_id:
                    return status.upper()
        return ''

    def get_submitted_job_id(self, output_lines: str, x11: bool = False) -> list[int]:
        """

        :param output_lines: Output of the ssh command.
        :type output_lines: str
        :param x11: Enable x11 forwarding, to enable graphical jobs.
        :type x11: bool

        :return: List of job ids that got submitted and had an output.
        :rtype: Union[list[int], int]
        """
        try:
            output_lines = output_lines.lower()
            if output_lines.find("failed") != -1:
                raise AutosubmitCritical(
                    "Submission failed. Command Failed", 7014)
            jobs_id = []
            for output in output_lines.splitlines():
                jobs_id.append(int(output.split('.')[0]))
            return jobs_id
        except IndexError as exc:
            raise AutosubmitCritical("Submission failed. There are issues on your config file", 7014) from exc

    def get_submit_cmd(self, job_script: str, job, hold: bool = False, export: str = "") -> str:
        """
        :param job_script: Name of the script of the job.
        :type job_script: str
        :param job: Job object.
        :type job: autosubmit.job.job.Job
        :param hold: Send job hold.
        :type hold: bool
        :param export: Set within the jobs.yaml, used to export environment script to use before the job is launched.
        :type export: str
        :return: Submit command for the script.
        :rtype: str
        """
        with suppress(Exception):
            lang = locale.getlocale()[1]
            if lang is None:
                lang = locale.getdefaultlocale()[1]
                if lang is None:
                    lang = 'UTF-8'

            if not hold:
                cmd = (export + self._submit_cmd + job_script).encode(lang)
            else:
                cmd = (export + self._submit_hold_cmd + job_script).encode(lang)

            with open(self._submit_script_path, "ab") as submit_script_file:
                submit_script_file.write(cmd)
        return str(cmd)

    def get_check_job_cmd(self, job_id: str) -> list[str]:  # noqa
        """
        Generates qstat command to the job selected.

        :param job_id: ID of a job.
        :param job_id: str

        :return: Generates the qstat command to be executes.
        :rtype: str
        """
        job_id = job_id.replace('{', '').replace('}', '').replace(',', ' ')
        return [f"qstat {job_id} | awk " + "'{print $3}'", f"qstat -H {job_id} | awk " + "'{print $3}'"]

    def get_check_all_jobs_cmd(self, jobs_id: str) -> str:  # noqa
        """
        Generates qstat command to all the jobs passed down.

        :param jobs_id: ID of one or more jobs.
        :param jobs_id: str

        :return: qstat command to all jobs.
        :rtype: str
        """
        jobs_id = jobs_id.replace('{', '').replace('}', '').replace(',', ' ')
        return f"qstat {jobs_id} | awk" + " '{print $1, $3}' &&" + f"qstat -H {jobs_id} | awk" + " '{print $1, $3}'"

    def get_jobs_id_by_job_name(self, job_name, retries=2):
        """
        Get job id by job name

        :param job_name:
        :param retries: retries
        :type retries: int
        :return: job id
        """
        # sleep(5)
        job_ids = ""
        cmd = self.get_jobs_id_by_job_name_cmd(job_name)
        self.send_command(cmd)
        job_id_name = self.get_ssh_output()
        while len(job_id_name) <= 0 < retries:
            self.send_command(cmd)
            job_id_name = self.get_ssh_output()
            retries -= 1
            sleep(2)
        if retries >= 0:
            # get id last line
            job_ids_names = job_id_name.split('\n')[:-1]
            # get all ids by job-name
            job_ids = [job_id.split(' ')[0] for job_id in job_ids_names]
        return job_ids

    def get_estimated_queue_time_cmd(self, job_id: str) -> str:
        """
        Gets an estimated queue time to the job selected.

        :param job_id: ID of a job.
        :param job_id: str

        :return: Gets estimated queue time.
        :rtype: str
        """
        job_id = job_id.replace('{', '').replace('}', '').replace(',', ' ')
        return f"qstat -f {job_id} | grep 'eligible_time = [0-9:0-9:0-9]*' && echo \"BREAK\" && " + f"qstat -H -f {job_id} | grep 'eligible_time = [0-9:0-9:0-9]*'"

    def get_queue_status_cmd(self, job_id: str) -> str:
        """
        Get queue generating qstat command to the job selected.

        :param job_id: ID of a job.
        :param job_id: str

        :return: Gets estimated queue time.
        :rtype: str
        """
        job_id = job_id.replace('{', '').replace('}', '').replace(',', ' ')
        return f"qstat {job_id} && echo \"BREAK\" && " + f"qstat -H {job_id}"

    def get_jobs_id_by_job_name_cmd(self, job_name: str) -> str:  # noqa
        """
        Looks for a job based on its name.

        :param job_name: Name given to a job
        :param job_name: str

        :return: Command to look for a job in the queue.
        :rtype: str
        """
        return "qstat -l | awk '{print $1, $2}'" + f" | grep -E {job_name}"

    @staticmethod
    def cancel_job(job_id: str) -> str:
        """
        Command to cancel a job.

        :param job_id: ID of a job.
        :param job_id: str

        :return: Cancel job command.
        :rtype: str
        """
        # job_id = str(job_id).replace('{','').replace('}','').replace(',', ' ')
        return f'qdel {job_id}'

    def parse_queue_reason(self, output: str, job_id: str) -> str:
        """Parses the queue reason from the output of the command.

        :param output: output of the command.
        :param job_id: job id

        :return: queue reason.
        :rtype: str
        """
        reason = [x.split(',')[1] for x in output.splitlines()
                  if x.split(',')[0] == str(job_id)]
        if isinstance(reason, list):
            # convert reason to str
            return ''.join(reason)
        return reason  # noqa F501

    def get_queue_status(self, in_queue_jobs: list['Job'], list_queue_jobs_id: str, as_conf: AutosubmitConfig) -> None:
        """
        get_queue_status

        :param in_queue_jobs: List of Job.
        :type in_queue_jobs: list[Job]
        :param list_queue_jobs_id: List of Job IDs concatenated.
        :type list_queue_jobs_id: str
        :param as_conf: experiment configuration.
        :type as_conf: autosubmit.config.AutosubmitConfig

        :rtype:None
        """
        if not in_queue_jobs:
            return
        cmd = self.get_queue_status_cmd(list_queue_jobs_id)
        self.send_command(cmd)
        queue_status = self._ssh_output
        for job in in_queue_jobs:
            reason = self.parse_queue_reason(queue_status, job.id)
            if job.queuing_reason_cancel(reason):  # this should be a platform method to be implemented
                Log.error(
                    f"Job {job.name} will be cancelled and set to FAILED as it was queuing due to {reason}")
                self.send_command(
                    self.cancel_cmd + f" {job.id}")
                job.new_status = Status.FAILED
                job.update_status(as_conf)
            elif reason == '(JobHeldUser)':
                if not job.hold:
                    # should be self.release_cmd or something like that, but it is not implemented
                    self.send_command(f"qrls {job.id}")
                    job.new_status = Status.QUEUING  # If it was HELD and was released, it should be QUEUING next.
                else:
                    job.new_status = Status.HELD

    def check_file_exists(self, src: str, wrapper_failed: bool = False, sleeptime: int = 5,
                          max_retries: int = 3) -> bool:
        """
        Checks if a file exists on the FTP server.
        :param src: The name of the file to check.
        :type src: str
        :param wrapper_failed: Whether the wrapper has failed. Defaults to False.
        :type wrapper_failed: bool
        :param sleeptime: Time to sleep between retries in seconds. Defaults to 5.
        :type sleeptime: int
        :param max_retries: Maximum number of retries. Defaults to 3.
        :type max_retries: int

        :return: True if the file exists, False otherwise
        :rtype: bool
        """
        # noqa TODO check the sleeptime retrials of these function, previously it was waiting a lot of time
        file_exist = False
        retries = 0
        while not file_exist and retries < max_retries:
            try:
                # This return IOError if a path doesn't exist
                self._ftpChannel.stat(os.path.join(
                    self.get_files_path(), src))
                file_exist = True
            except IOError:  # File doesn't exist, retry in sleeptime
                sleep(sleeptime)
                retries = retries + 1
            except BaseException as e:  # Unrecoverable error
                if str(e).lower().find("garbage") != -1:
                    sleep(2)
                    retries = retries + 1
                else:
                    file_exist = False  # won't exist
                    retries = 999  # no more retries
        if not file_exist:
            Log.warning(f"File {src} couldn't be found")
        return file_exist
