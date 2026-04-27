# Copyright 2015-2025 Earth Sciences Department, BSC-CNS
#
# This file is part of Autosubmit.
#
# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

"""Slurm Platform.

This file contains code that interfaces between Autosubmit and a Slurm Platform.
"""

import os
import re
from contextlib import suppress
from pathlib import Path
from time import sleep
from typing import Any, Optional, Union, TYPE_CHECKING

from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.job.job_common import Status
from autosubmit.log.log import AutosubmitCritical, AutosubmitError, Log
from autosubmit.platforms.headers.slurm_header import SlurmHeader
from autosubmit.platforms.paramiko_platform import ParamikoPlatform
from autosubmit.platforms.wrappers.wrapper_factory import SlurmWrapperFactory

if TYPE_CHECKING:
    # Avoid circular imports
    from autosubmit.job.job import Job


# Compiled patterns that identify valid stdout from any Slurm command.
_SLURM_EXPECTED_OUTPUT: tuple[re.Pattern, ...] = (
    # sbatch (one or more "Submitted batch job N" lines in batched submission).
    re.compile(r"submitted batch job \d+", re.IGNORECASE),
    # squeue / sacct tabular output: any row that starts with a numeric job ID
    re.compile(r"^\s*\d+\b", re.MULTILINE),
    # sacct state-only output (-o "State"): matches any known job-state word
    re.compile(
        r"\b(completed|running|pending|configuring|resizing|failed|cancelled|"
        r"timeout|node_fail|preempted|suspended|out_of_memory)\b",
        re.IGNORECASE,
    ),
    # scontrol show output: key=value pairs present in single or multi-job output.
    re.compile(r"JobId=\d+", re.IGNORECASE),
    # squeue / sacct header-only output: command ran successfully but returned
    # The "JOBID" column header is always present in squeue (without -h) and
    # in sacct (without -n).
    re.compile(r"\bJOBID\b", re.IGNORECASE),
    # sacct column-separator line ("------------ ---------- ...") that appears
    # when sacct is called without -n and finds no matching job records.
    re.compile(r"^-{3,}", re.MULTILINE),
)


class SlurmPlatform(ParamikoPlatform):
    """Class to manage jobs to host using SLURM scheduler."""

    def __init__(self, expid: str, name: str, config: dict,
                 auth_password: Optional[Union[str, list[str]]] = None) -> None:
        """Initialization of the Class SlurmPlatform.

        :param expid: ID of the experiment which will instantiate the SlurmPlatform.
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
        self._submit_cmd_x11 = None
        self.cancel_cmd = None
        self.type = 'slurm'
        self._header = SlurmHeader()
        self._wrapper = SlurmWrapperFactory(self)
        self.job_status = dict()
        self.job_status['COMPLETED'] = ['COMPLETED']
        self.job_status['RUNNING'] = ['RUNNING']
        self.job_status['QUEUING'] = ['PENDING', 'CONFIGURING', 'RESIZING']
        self.job_status['FAILED'] = ['FAILED', 'CANCELLED', 'CANCELLED+', 'NODE_FAIL',
                                     'PREEMPTED', 'SUSPENDED', 'TIMEOUT', 'OUT_OF_MEMORY', 'OUT_OF_ME+', 'OUT_OF_ME']
        self._pathdir = f"$HOME/LOG_{self.expid}"
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
        """Return a copy of a SlurmPlatform object with the same
        expid, name and config as the original.

        :return: A new platform type slurm
        :rtype: SlurmPlatform
        """
        return SlurmPlatform(self.expid, self.name, self.config)

    def check_remote_log_dir(self) -> None:
        """Creates log dir on remote host."""

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
        """Updates commands for platforms. """
        self.root_dir = os.path.join(
            self.scratch, self.project_dir, self.user, self.expid)
        self.remote_log_dir = os.path.join(self.root_dir, "LOG_" + self.expid)
        self.cancel_cmd = "scancel"
        self._submit_cmd = f'sbatch --no-requeue -D {self.remote_log_dir} '
        self._submit_command_name = "sbatch"
        self._submit_hold_cmd = f'sbatch -H -D {self.remote_log_dir} '
        # jobid =$(sbatch WOA_run_mn4.sh 2 > & 1 | grep -o "[0-9]*"); scontrol hold $jobid;
        self.put_cmd = "scp"
        self.get_cmd = "scp"
        self.mkdir_cmd = "mkdir -p " + self.remote_log_dir

    def _construct_final_call(self, script_name: str, pre: str, post: str, x11_options: str):
        """Gets the command to submit a job, for the current platform, with the given parameters.
         This needs to be adapted to each scheduler, the default assumes that is being launched directly.

        :param script_name: name of the script to submit
        :param pre: command part to be placed before the script name, e.g. timeout, export, executable
        :param post: command part to be placed after the script name, e.g. redirection of stdout and stderr
        :param x11_options: x11 options to run the script, if any
        :return: command to submit a job
        """
        return f"cd {self.remote_log_dir} ; {pre} salloc {x11_options} {script_name} {post}" if x11_options else f"{pre} {self._submit_cmd} {script_name} {post}"

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
        """Parses check job command output, so it can be interpreted by autosubmit.

        :param output: output to parse.
        :type output: str
        :return: job status.
        :rtype: str
        """
        return output.strip().split(' ')[0].strip()

    def parse_all_jobs_output(self, output: str, job_id: int) -> Union[list[str], str]:
        status = ""
        with suppress(Exception):
            status = [
                x.split()[1]
                for x in output.splitlines()
                if x.split()[0][:len(str(job_id))] == str(job_id)
            ]
        if len(status) == 0:
            return status
        return status[0]

    def get_submitted_job_id(self, output: str, x11: bool = False) -> list[str]:
        """Parses the output of the submit command to get the job ID.

        :param output: output of the submit command.
        :param x11: whether the job is an x11 job, which has a different output format.
        :return: job ID of the submitted job.
        """
        jobs_id = []
        try:
            if "failed" in output.lower():
                raise AutosubmitCritical("Submission failed. Command Failed", 7014)
            if x11:
                return int(output.splitlines()[0])
            for line in output.splitlines():
                jobs_id.append(int(line.split(' ')[3]))
            return jobs_id
        except IndexError as exc:
            raise AutosubmitCritical("Submission failed. There are issues on your config file", 7014) from exc

    def get_submitted_jobs_by_name(self, script_names: list[str]) -> list[int]:
        """Return submitted Slurm job IDs by script name.

        This fallback is used when the batched submit command does not return
        one recoverable job identifier per submitted script.

        :param script_names: Submitted script filenames.
        :type script_names: list[str]
        :return: Matching Slurm job IDs in submission order.
        :rtype: list[int]
        """
        submitted_job_ids: list[int] = []

        for script_name in script_names:
            job_name = Path(script_name).stem

            matched_ids = [int(job_id) for job_id in self.get_job_id_by_job_name(job_name)]

            if not matched_ids:
                return []

            submitted_job_ids.append(max(matched_ids))

        return submitted_job_ids

    def get_check_job_cmd(self, job_id: str) -> str:
        """Generates sacct command to the job selected.

        :param job_id: ID of a job.
        :return: Generates the sacct command to be executes.
        """
        return f'sacct -n -X --jobs {job_id} -o "State"'

    def get_check_all_jobs_cmd(self, jobs_id: str):
        """Generates sacct command to all the jobs passed down.

        :param jobs_id: ID of one or more jobs.
        :return: sacct command to all jobs.
        :rtype: str
        """
        return f"sacct -n -X --jobs {jobs_id} -o jobid,State"

    def get_estimated_queue_time_cmd(self, job_id: str):
        """Gets an estimated queue time to the job selected.

        :param job_id: ID of a job.
        :param job_id: str
        :return: Gets estimated queue time.
        :rtype: str
        """
        return f"scontrol -o show JobId {job_id} | grep -Po '(?<=EligibleTime=)[0-9-:T]*'"

    def get_queue_status_cmd(self, job_id: str) -> str:
        """Get queue generating squeue command to the job selected.

        :param job_id: ID of a job.
        :param job_id: str
        :return: Gets estimated queue time.
        :rtype: str
        """
        return f'squeue -j {job_id} -o %A,%R'

    def get_job_id_by_job_name_cmd(self, job_name: str) -> str:
        """Looks for a job based on its name.

        :param job_name: Name given to a job
        :param job_name: str
        :return: Command to look for a job in the queue.
        :rtype: str
        """
        return f'squeue -o %A,%.50j -n {job_name}'

    def get_job_energy_cmd(self, job_id: str) -> str:
        """Generates a command to get data from a job
        JobId, State, NCPUS, NNodes, Submit, Start, End, ConsumedEnergy, MaxRSS, AveRSS%25.

        :param job_id: ID of a job.
        :param job_id: str
        :return: Command to get job energy.
        :rtype: str
        """
        return (f'sacct -n --jobs {job_id} -o JobId%25,State,NCPUS,NNodes,Submit,'
                f'Start,End,ConsumedEnergy,MaxRSS%25,AveRSS%25')

    def parse_queue_reason(self, output: str, job_id: str) -> str:
        """Parses the queue reason from the output of the command.

        :param output: output of the command.
        :param job_id: job id
        :return: queue reason.
        """
        return ''.join([
            x.split(',')[1]
            for x in output.splitlines()
            if x.split(',')[0] == str(job_id)
        ])

    def get_queue_status(self, in_queue_jobs: list['Job'], list_queue_jobid: str, as_conf: AutosubmitConfig) -> None:
        """get_queue_status.

        :param in_queue_jobs: List of Job.
        :type in_queue_jobs: list[Job]
        :param list_queue_jobid: List of Job IDs concatenated.
        :type list_queue_jobid: str
        :param as_conf: experiment configuration.
        :type as_conf: autosubmit.config.AutosubmitConfig
        """
        if not in_queue_jobs:
            return
        cmd = self.get_queue_status_cmd(list_queue_jobid)
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
                    self.send_command(f"scontrol release {job.id}")
                    job.new_status = Status.QUEUING  # If it was HELD and was released, it should be QUEUING next.
                else:
                    job.new_status = Status.HELD

    def wrapper_header(self, **kwargs: Any) -> str:
        """It generates the header of the wrapper configuring it to execute the Experiment.

        :param kwargs: Key arguments associated to the Job/Experiment to configure the wrapper.
        :type kwargs: Any
        :return: a sequence of slurm commands.
        :rtype: str
        """
        return self._header.wrapper_header(**kwargs)

    @staticmethod
    def allocated_nodes() -> str:
        """It sets the allocated nodes of the wrapper

        :return: A command that changes the num of Node per job
        :rtype: str
        """
        return """os.system("scontrol show hostnames $SLURM_JOB_NODELIST > node_list_{0}".format(node_id))"""

    def check_file_exists(self, src: str, wrapper_failed: bool = False, sleeptime: int = 5,
                          max_retries: int = 3) -> bool:
        """Checks if a file exists on the FTP server.

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
        # TODO check the sleeptime retrials of these function, previously it was waiting a lot of time
        file_exist = False
        retries = 0
        while not file_exist and retries < max_retries:
            try:
                # This return IOError if a path doesn't exist
                self._ftpChannel.stat(os.path.join(
                    self.get_files_path(), src))
                file_exist = True
            except IOError:  # File doesn't exist, retry in sleeptime
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
            Log.warning(f"File {src} couldn't be found")
        return file_exist

    def _get_job_names_cmd(self, job_names: list[str]) -> str:
        """Return a command that groups Slurm job IDs by job name.

        The command returns one line per job name using the format
        ``JobName:id,id2,id3``.

        :param job_names: Job names to query.
        :type job_names: list[str]
        :return: Shell command that groups matching job IDs by job name.
        :rtype: str
        """
        return (
            f"squeue -h -o '%j:%A' -n {','.join(job_names)} "
            "| awk -F':' '{jobs[$1] = jobs[$1] ? jobs[$1] \",\" $2 : $2} "
            "END {for (name in jobs) print name \":\" jobs[name]}'"
        )

    def cancel_jobs(self, job_ids: list[str]) -> None:
        """Cancel jobs by their IDs.

        :param job_ids: List of job IDs to cancel.
        :type job_ids: list[str]
        """
        if job_ids:
            cancel_by_comma = ",".join(str(job_id) for job_id in job_ids)
            self.send_command(f"{self.cancel_cmd} {cancel_by_comma}")

    def _check_for_unrecoverable_errors(self) -> None:
        """Check Slurm command output for recoverable and unrecoverable errors."""
        out = self._ssh_output or ""
        err = self._ssh_output_err or ""

        # Fast-exit: any match in stdout (single or multi-line) confirms the
        if any(pat.search(out) for pat in _SLURM_EXPECTED_OUTPUT):
            return

        # stdout does not contain the expected Slurm output; inspect stderr.
        err_lower = err.lower()
        if not err_lower.strip():
            return

        transient_patterns: list[str] = [
            "not active",
            "git clone",
            "socket timed out",
            "socket error",
            "connection timed out",
            "connection refused",
            "connection reset by peer",
            "broken pipe",
            "network is unreachable",
            "unable to connect to slurm daemon",
            "slurm_persist_conn_open_without_init",
            "slurmdbd",
            "slurmctld",
            "communication failure",
            "communication error",
            "temporary failure in name resolution",
        ]

        for pattern in transient_patterns:
            if pattern in err_lower:
                raise AutosubmitError(
                    f"Transient Slurm error: {err}",
                    6016,
                )

        critical_patterns: list[str] = [
            "invalid partition",
            "invalid qos",
            "invalid account",
            "invalid constraint",
            "invalid time specification",
            "invalid --time",
            "invalid --mem",
            "invalid --nodes",
            "invalid --ntasks",
            "invalid --cpus-per-task",
            "invalid --partition",
            "invalid --qos",
            "invalid --account",
            "maxwalldurationperjoblimit",
            "unrecognized option",
            "batch job submission failed",
            "not submitted",
            "violates accounting/qos policy",
            "violates accounting policy",
            "requested node configuration is not available",
            "not allowed to submit",
            "user/account not found",
            "job exceeds",
            "account has insufficient",
            "syntax error",
            "command not found",
            "salloc: error",
            "salloc: unrecognized option",
            "unknown option",
            "error: cpu count per node",
            "sbatch: error:",
        ]

        for pattern in critical_patterns:
            if pattern in err_lower:
                raise AutosubmitCritical(
                    f"Permanent Slurm error: {err}",
                    7014,
                )
