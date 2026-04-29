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
from contextlib import suppress
from pathlib import Path
from time import sleep
from typing import TYPE_CHECKING

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

    def check_remote_log_dir(self) -> None:
        """Creates log dir on remote host.

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
        """Updates commands for platforms.

        :rtype: None
        """
        self.root_dir = os.path.join(
            self.scratch, self.project_dir, self.user, self.expid)
        self.remote_log_dir = os.path.join(self.root_dir, "LOG_" + self.expid)
        self.cancel_cmd = "qdel"
        self._submit_cmd = 'qsub'
        self._submit_command_name = "qsub"
        self._submit_hold_cmd = 'qhold '  # Needs the JOB_ID to hold a JOB
        self.put_cmd = "scp"
        self.get_cmd = "scp"
        self.mkdir_cmd = "mkdir -p " + self.remote_log_dir
        self._submit_cmd_x11 = f'{self.remote_log_dir}'

    def _construct_final_call(self, script_name: str, pre: str, post: str, x11_options: str) -> str:
        """Build the PBS qsub submission command for a single script.

        :param script_name: Absolute path to the script to submit.
        :param pre: Command prefix (e.g. export, timeout).
        :param post: Command suffix (e.g. output redirection).
        :param x11_options: X11 forwarding options; unused for PBS.
        :return: Complete qsub submission command.
        :rtype: str
        """
        return f"{pre} {self._submit_cmd} {script_name} {post}"

    def _check_for_unrecoverable_errors(self) -> None:
        """Check PBS command output for transient and permanent errors."""

        err = self._ssh_output_err or ""
        err_lower = err.lower()
        if not err_lower.strip():
            return

        transient_patterns: list[str] = [
            "not active",
            "socket timed out",
            "socket error",
            "connection timed out",
            "connection refused",
            "connection reset by peer",
            "broken pipe",
            "network is unreachable",
            "unable to connect to pbs server",
            "communication failure",
            "communication error",
            "temporary failure in name resolution",
        ]

        for pattern in transient_patterns:
            if pattern in err_lower:
                raise AutosubmitError(f"Transient PBS error: {err}", 6016)

        critical_patterns: list[str] = [
            "violates resource limits",
            "illegal attribute or resource value",
            "unknown resource",
            "job violates queue",
            "invalid queue",
            "not authorized",
            "bad uid",
            "not allowed to submit",
            "scheduler is not installed",
            "qsub: error",
            "command not found",
            "syntax error",
        ]

        for pattern in critical_patterns:
            if pattern in err_lower:
                raise AutosubmitCritical(f"Permanent PBS error: {err}", 7014)

    def cancel_jobs(self, job_ids: list[str]) -> None:
        """Cancel PBS jobs by their IDs.

        :param job_ids: List of job IDs to cancel.
        :type job_ids: list[str]
        """
        if job_ids:
            cancel_by_space = " ".join(str(job_id) for job_id in job_ids)
            self.send_command(f"{self.cancel_cmd} {cancel_by_space}")

    def _get_job_names_cmd(self, job_names: list[str]) -> str:
        """Return a command that groups PBS job IDs by job name.

        The command produces one line per job name in the format
        ``JobName:id1,id2`` so that the parent's ``_parse_job_names`` can
        process it for duplicate detection.

        :param job_names: Job names to query.
        :type job_names: list[str]
        :return: Shell command that groups matching job IDs by job name.
        :rtype: str
        """
        if not job_names:
            return ""

        names_pattern = "|".join(job_names)
        return (
            f"qstat -l | awk '{{print $1, $2}}' | grep -E '{names_pattern}' "
            "| awk '{split($1,a,\".\"); jobs[$2] = jobs[$2] ? jobs[$2] \",\" a[1] : a[1]} "
            "END {for (name in jobs) print name \":\" jobs[name]}'"
        )

    def get_submitted_jobs_by_name(self, script_names: list[str]) -> list[int]:
        """Return submitted PBS job IDs by script name using a single scheduler query.

        All names are batched into one ``qstat``

        :param script_names: Submitted script filenames.
        :type script_names: list[str]
        :return: Matching PBS job IDs in submission order.
        :rtype: list[int]
        """
        job_names = [Path(s).stem for s in script_names]
        names_pattern = "|".join(job_names)
        self.send_command(f"qstat -l | awk '{{print $1, $2}}' | grep -E '{names_pattern}'")

        name_to_ids: dict[str, list[int]] = {}
        for line in self.get_ssh_output().splitlines():
            parts = line.split()
            if len(parts) >= 2:
                jid = int(parts[0].split('.')[0])
                jnam = parts[1]
                name_to_ids.setdefault(jnam, []).append(jid)

        submitted_job_ids: list[int] = []
        for job_name in job_names:
            ids = name_to_ids.get(job_name, [])
            if not ids:
                return []
            submitted_job_ids.append(max(ids))

        return submitted_job_ids

    def get_mkdir_cmd(self) -> str:
        """Get the variable mkdir_cmd that stores the mkdir command.

        :return: Mkdir command
        :rtype: str
        """
        return self.mkdir_cmd

    def get_remote_log_dir(self) -> str:
        """Get the variable remote_log_dir that stores the directory of the Log of the experiment.

        :return: The remote_log_dir variable.
        :rtype: str
        """
        return self.remote_log_dir

    def parse_job_output(self, output: str) -> str:
        """Parse check job command output so it can be interpreted by autosubmit.

        :param output: output to parse.
        :type output: str
        :return: job status.
        :rtype: str
        """
        return output.strip().split(' ')[0].strip()

    def parse_all_jobs_output(self, output: str, job_id: str) -> str:  # noqa
        """Filter one or more status of a specific Job ID.

        :param output: Output of the status of the jobs.
        :type output: str
        :param job_id: job ID.
        :type job_id: int
        :return: All status related to a Job.
        :rtype: str
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
        """Iterate through jobs that didn't fail the submission and retrieve their ID.

        :param output_lines: Output of the ssh command.
        :type output_lines: str
        :param x11: Enable x11 forwarding, to enable graphical jobs.
        :type x11: bool
        :return: List of job ids that got submitted and had an output.
        :rtype: list[int]
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

    def get_check_job_cmd(self, job_id: str) -> str:  # noqa
        """Generate qstat command for the selected job.

        :param job_id: ID of a job.
        :param job_id: str

        :return: Generates the qstat command to be executed.
        :rtype: str
        """
        job_id = job_id.replace('{', '').replace('}', '').replace(',', ' ')
        return f"qstat {job_id} | awk " + "'{print $3}' && " + f"qstat -H {job_id} | awk " + "'{print $3}'"

    def get_check_all_jobs_cmd(self, jobs_id: str) -> str:  # noqa
        """Generate qstat command for all the jobs passed down.

        :param jobs_id: ID of one or more jobs.
        :param jobs_id: str

        :return: qstat command to all jobs.
        :rtype: str
        """
        jobs_id = jobs_id.replace('{', '').replace('}', '').replace(',', ' ')
        return f"qstat {jobs_id} | awk" + " '{print $1, $3}' && " + f"qstat -H {jobs_id} | awk" + " '{print $1, $3}'"

    def get_estimated_queue_time_cmd(self, job_id: str) -> str:
        """Get an estimated queue time to the job selected.

        :param job_id: ID of a job.
        :param job_id: str
        :return: Gets estimated queue time.
        :rtype: str
        """
        job_id = job_id.replace('{', '').replace('}', '').replace(',', ' ')
        return f"qstat -f {job_id} | grep 'eligible_time = [0-9:0-9:0-9]*' && echo \"BREAK\" && " + f"qstat -H -f {job_id} | grep 'eligible_time = [0-9:0-9:0-9]*'"



    def parse_queue_reason(self, output: str, job_id: str) -> str:
        """Parse the queue reason from the output of the command.

        :param output: output of the command.
        :type output: str
        :param job_id: job id
        :type job_id: str
        :return: queue reason.
        :rtype: str
        """
        reason = [x.split(',')[1] for x in output.splitlines()
                  if x.split(',')[0] == str(job_id)]
        if isinstance(reason, list):
            # convert reason to str
            return ''.join(reason)
        return reason  # noqa F501



    def wrapper_header(self, **kwargs: dict) -> str:
        """Generate the header of the wrapper configuring it to execute the Experiment.

        :param kwargs: Key arguments associated to the Job/Experiment to configure the wrapper.
        :type kwargs: Any
        :return: a sequence of PBS commands.
        :rtype: str
        """
        return self._header.wrapper_header(**kwargs)

    @staticmethod
    def allocated_nodes() -> None:
        """Set the allocated nodes of the wrapper.

        :return: A command that changes the num of Node per job
        :rtype: str
        """
        Log.warning("Permission denied: Not enough permission to execute the command that sets the allocated nodes of the wrapper")

    def check_file_exists(self, src: str, wrapper_failed: bool = False, sleeptime: int = 5,
                          max_retries: int = 3) -> bool:
        """Check if a file exists on the FTP server.

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
