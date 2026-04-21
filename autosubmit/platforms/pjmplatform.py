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

import os
import re
import textwrap
from pathlib import Path
from time import sleep
from typing import TYPE_CHECKING

from autosubmit.job.job_common import Status
from autosubmit.log.log import AutosubmitCritical, AutosubmitError, Log
from autosubmit.platforms.headers.pjm_header import PJMHeader
from autosubmit.platforms.paramiko_platform import ParamikoPlatform
from autosubmit.platforms.wrappers.wrapper_factory import PJMWrapperFactory

if TYPE_CHECKING:
    # Avoid circular imports
    from autosubmit.job.job import Job

# Compiled patterns that identify valid stdout from any PJM command.
_PJM_EXPECTED_OUTPUT: tuple[re.Pattern, ...] = (
    # pjsub success: "[INFO] PJM 0000 pjsub Job <id> submitted."
    # pjdel success: "[INFO] PJM 0000 pjdel Job <id> deleted."
    re.compile(r"\[INFO] PJM 0000", re.IGNORECASE),
    # pjstat tabular output: any row that starts with a numeric job ID
    re.compile(r"^\s*\d+\b", re.MULTILINE),
    # pjstat known job-state abbreviations (all states, including failed ones,
    re.compile(r"\b(ext|rno|rne|run|acc|que|rna|rnp|hld|err|ccl|rjt)\b", re.IGNORECASE),
    # pjstat header-only output: column headers present when no jobs match.
    re.compile(r"\bJOBID\b|\bJID\b", re.IGNORECASE),
)


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
        # https://software.fujitsu.com/jp/manual/manualfiles/m220008/j2ul2452/02enz007/j2ul-2452-02enz0.pdf page 16
        self.job_status = dict()
        self.job_status['COMPLETED'] = ['EXT']
        self.job_status['RUNNING'] = ['RNO', 'RNE', 'RUN']
        self.job_status['QUEUING'] = ['ACC', 'QUE', 'RNA', 'RNP', 'HLD']  # TODO NOT SURE ABOUT HOLD HLD
        self.job_status['FAILED'] = ['ERR', 'CCL', 'RJT']
        self._pathdir = "\\$HOME/LOG_" + self.expid
        self._allow_arrays = False
        self._allow_wrappers = True  # NOT SURE IF WE NEED WRAPPERS
        self.update_cmds()
        self.config = config
        exp_id_path = os.path.join(self.config.get("LOCAL_ROOT_DIR"), self.expid)
        tmp_path = os.path.join(exp_id_path, "tmp")
        self._submit_script_path = os.path.join(
            tmp_path, self.config.get("LOCAL_ASLOG_DIR"), "submit_" + self.name + ".sh")
        self._submit_script_base_name = os.path.join(
            tmp_path, self.config.get("LOCAL_ASLOG_DIR"), "submit_")
        self.type = "pjm"

    def create_a_new_copy(self):
        return PJMPlatform(self.expid, self.name, self.config)

    def submit_error(self, output):
        """Check if the output of the submit command contains an error message.

        :param output: output of the submit cmd
        :return: boolean
        """
        return not all(part.lower() in output.lower() for part in ["pjsub", "[INFO] PJM 0000"])

    def check_remote_log_dir(self):
        """Creates log dir on remote host"""

        try:
            # Test if remote_path exists
            self._ftpChannel.chdir(self.remote_log_dir)
        except IOError:
            try:
                if self.send_command(self.get_mkdir_cmd()):
                    Log.debug(f'{self.remote_log_dir} has been created on {self.host} .')
                else:
                    raise AutosubmitError("SFTP session not active ", 6007,
                                          f"Could not create the DIR {self.remote_log_dir} on HPC {self.host}'.format(self.remote_log_dir, self.host)")
            except BaseException as e:
                raise AutosubmitError(
                    "SFTP session not active ", 6007, str(e))

    def update_cmds(self):
        """Update commands for platforms."""
        self.root_dir = os.path.join(
            self.scratch, self.project_dir, self.user, self.expid)
        self.remote_log_dir = os.path.join(self.root_dir, "LOG_" + self.expid)
        self.cancel_cmd = "pjdel"
        self._checkhost_cmd = "echo 1"
        self._submit_cmd = f'cd {self.remote_log_dir} ; pjsub'
        self._submit_command_name = "pjsub"
        self._submit_hold_cmd = f'cd {self.remote_log_dir} ; pjsub'
        self.put_cmd = "scp"
        self.get_cmd = "scp"
        self.mkdir_cmd = "mkdir -p " + self.remote_log_dir

    def get_mkdir_cmd(self):
        return self.mkdir_cmd

    def get_remote_log_dir(self):
        return self.remote_log_dir

    def parse_job_output(self, output):
        return output.strip().split()[1].strip().strip("\n")

    def queuing_reason_cancel(self, reason):
        try:
            if len(reason.split('(', 1)) > 1:
                reason = reason.split('(', 1)[1].split(')')[0]
                if 'Invalid' in reason or reason in ['ANOTHER JOB STARTED', 'DELAY', 'DEADLINE SCHEDULE STARTED',
                                                     'ELAPSE LIMIT EXCEEDED', 'FILE IO ERROR', 'GATE CHECK',
                                                     'IMPOSSIBLE SCHED', 'INSUFF CPU', 'INSUFF MEMORY', 'INSUFF NODE',
                                                     'INSUFF', 'INTERNAL ERROR', 'INVALID HOSTFILE',
                                                     'LIMIT OVER MEMORY', 'LOST COMM', 'NO CURRENT DIR', 'NOT EXIST',
                                                     'RSCGRP NOT EXIST', 'RSCGRP STOP', 'RSCUNIT', 'USER', 'EXCEED',
                                                     'WAIT SCHED']:
                    return True
            return False
        except Exception:
            return False

    def get_queue_status(self, in_queue_jobs: list['Job'], list_queue_jobid, as_conf):
        if not in_queue_jobs:
            return
        cmd = self.get_queue_status_cmd(list_queue_jobid)
        self.send_command(cmd)
        queue_status = self._ssh_output
        for job in in_queue_jobs:
            reason = self.parse_queue_reason(queue_status, job.id)
            if job.queuing_reason_cancel(reason):
                Log.printlog(f"Job {job.name} will be cancelled and set to FAILED as it was queuing due to {reason}",
                             6000)
                self.send_command(f"{self.cancel_cmd} {job.id}")
                job.new_status = Status.FAILED
                job.update_status(as_conf)

    def parse_all_jobs_output(self, output, job_id):
        status = ""
        try:
            status = [x.split()[1] for x in output.splitlines()
                      if x.split()[0] == str(job_id)]
        except BaseException:
            pass
        if len(status) == 0:
            return status
        return status[0]

    def parse_job_list(self, job_list: list[list['Job']]) -> str:
        """Convert a list of job_list to job_list_cmd.

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

    def get_submitted_job_id(self, output: str, x11: bool = False) -> list[int]:
        """Parse the output of the submit command and return PJM job IDs.

        :param output: Output of the submit command.
        :type output: str
        :param x11: Unused for PJM, kept for API compatibility.
        :type x11: bool
        :return: Parsed PJM job IDs.
        :rtype: list[int]
        """
        jobs_id: list[int] = []

        for line in output.splitlines():
            if self.submit_error(line):
                continue

            parts = line.split()
            if len(parts) >= 6 and parts[5].isdigit():
                jobs_id.append(int(parts[5]))

        return jobs_id

    def get_check_all_jobs_cmd(self, jobs_id):
        # jobs_id = "jobid1+jobid2+jobid3"
        # -H == sacct
        if jobs_id[-1] == ",":
            jobs_id = jobs_id[:-1]  # deletes comma
        return f"pjstat -H -v --choose jid,st,ermsg --filter \"jid={jobs_id}\" > as_checkalljobs.txt ; pjstat -v --choose jid,st,ermsg --filter \"jid={jobs_id}\" >> as_checkalljobs.txt ; cat as_checkalljobs.txt ; rm as_checkalljobs.txt"

    def get_check_job_cmd(self, job_id):
        return f"pjstat -H -v --choose st --filter \"jid={job_id}\" > as_checkjob.txt ; pjstat -v --choose st --filter \"jid={job_id}\" >> as_checkjob.txt ; cat as_checkjob.txt ; rm as_checkjob.txt"
        # return 'pjstat -v --choose jid,st,ermsg --filter \"jid={0}\"'.format(job_id)

    def get_queue_status_cmd(self, job_id):
        return self.get_check_all_jobs_cmd(job_id)

    def get_job_id_by_job_name_cmd(self, job_name):
        if job_name[-1] == ",":
            job_name = job_name[:-1]
        return f'pjstat -v --choose jid,st,ermsg --filter \"jnam={job_name}\"'

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

    def check_file_exists(self, filename: str, wrapper_failed: bool = False, sleeptime: int = 5, max_retries: int = 3):
        file_exist = False
        retries = 0

        while not file_exist and retries < max_retries:
            try:
                # This return IOError if path doesn't exist
                self._ftpChannel.stat(os.path.join(
                    self.get_files_path(), filename))
                file_exist = True
            except IOError:  # File doesn't exist, retry in sleeptime
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
                    Log.printlog(f"remote logs {filename} couldn't be recovered", 6001)
                    file_exist = False  # won't exist
                    retries = 999  # no more retries
        return file_exist

    def get_submitted_jobs_by_name(self, script_names: list[str]) -> list[int]:
        """Get submitted PJM job IDs by script name.

        This is a fallback used when the submit command output does not contain
        enough information to recover all submitted job IDs directly.

        :param script_names: Submitted script filenames.
        :type script_names: list[str]
        :return: Matching PJM job IDs in the same order as ``script_names``.
        :rtype: list[int]
        """
        submitted_job_ids: list[int] = []

        for script_name in script_names:
            job_name = Path(script_name).stem
            command = f'pjstat -v --choose jid,jnam --filter "jnam={job_name}"'
            self.send_command(command)

            matched_ids: list[int] = []
            for line in self.get_ssh_output().splitlines():
                parts = line.split()
                if parts and parts[0].isdigit():
                    matched_ids.append(int(parts[0]))

            if not matched_ids:
                return []

            submitted_job_ids.append(max(matched_ids))

        return submitted_job_ids

    def _get_job_names_cmd(self, job_names: list[str]) -> str:
        """Return a command that groups PJM job IDs by job name.

        The command returns one line per job name using the format
        ``JobName:id,id2,id3``.

        :param job_names: Job names to query.
        :type job_names: list[str]
        :return: Shell command that groups matching job IDs by job name.
        :rtype: str
        """
        if not job_names:
            return ""

        commands = " ; ".join(
            f'pjstat -v --choose jid,jnam --filter "jnam={job_name}"'
            for job_name in job_names
        )

        return (
            f"{commands} | "
            "awk '$1 ~ /^[0-9]+$/ {"
            "job_name = $NF; "
            "sub(/\\.cmd$/, \"\", job_name); "
            "jobs[job_name] = jobs[job_name] ? jobs[job_name] \",\" $1 : $1"
            "} END {"
            "for (name in jobs) print name \":\" jobs[name]"
            "}'"
        )

    def cancel_jobs(self, job_ids: list[str]) -> None:
        """Cancel PJM jobs by their IDs.

        :param job_ids: List of PJM job IDs to cancel.
        :type job_ids: list[str]
        :rtype: None
        """
        if not job_ids:
            return
        ids = " ".join(str(job_id) for job_id in job_ids)
        self.send_command(f"{self.cancel_cmd} {ids}")

    def _check_for_unrecoverable_errors(self) -> None:
        """Check PJM command output for recoverable and unrecoverable errors."""
        out = self._ssh_output or ""
        err = self._ssh_output_err or ""

        # Fast-exit: any match in stdout confirms valid PJM command output.
        if any(pat.search(out) for pat in _PJM_EXPECTED_OUTPUT):
            return

        err_lower = err.lower()
        if not err_lower.strip():
            return

        transient_patterns: list[tuple[str, str]] = [
            ("not active", "SSH session not active"),
            ("socket timed out", "Socket timed out communicating with PJM"),
            ("socket error", "Socket error communicating with PJM"),
            ("connection timed out", "Connection to PJM server timed out"),
            ("connection refused", "Connection to PJM server refused"),
            ("connection reset by peer", "Connection reset by PJM server"),
            ("broken pipe", "Broken pipe while communicating with PJM"),
            ("network is unreachable", "Network unreachable; cannot reach PJM server"),
            ("cannot connect to server", "Cannot connect to PJM server"),
            ("server not responding", "PJM server not responding"),
            ("communication failure", "PJM communication failure"),
            ("communication error", "PJM communication error"),
            ("temporary failure in name resolution", "DNS resolution failed temporarily"),
        ]

        for pattern, message in transient_patterns:
            if pattern in err_lower:
                raise AutosubmitError(
                    f"Transient PJM error: {message}",
                    6016,
                    err,
                )

        critical_patterns: list[tuple[str, str]] = [
            ("[err.] pjm", "PJM reported an error; check directives or resource group"),
            ("invalid resource group", "Invalid PJM resource group specified"),
            ("resource group does not exist", "PJM resource group does not exist"),
            ("resource group stop", "PJM resource group is stopped"),
            ("no permission", "No permission to submit to this PJM resource group"),
            ("exceeds limit", "Job exceeds a configured PJM resource limit"),
            ("not submitted", "Job was not submitted to PJM; check pjsub directives"),
            ("submission failed", "PJM submission failed; check directives or resource group"),
            ("invalid job name", "Invalid PJM job name"),
            ("invalid elapse", "Invalid wallclock time specification"),
            ("unrecognized option", "Unrecognised pjsub option"),
            ("unknown option", "Unknown pjsub option in the job script"),
            ("syntax error", "Syntax error in the job script or directives"),
            ("command not found", "PJM command not found on the remote host"),
        ]

        for pattern, message in critical_patterns:
            if pattern in err_lower:
                raise AutosubmitCritical(
                    f"Permanent PJM error: {message}",
                    7014,
                    err,
                )
