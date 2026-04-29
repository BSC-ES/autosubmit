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

from autosubmit.log.log import AutosubmitCritical, AutosubmitError, Log
from autosubmit.platforms.headers.ps_header import PsHeader
from autosubmit.platforms.paramiko_platform import ParamikoPlatform


class PsPlatform(ParamikoPlatform):
    """Class to manage jobs to host not using any scheduler."""

    def __init__(self, expid: str, name: str, config: dict):
        ParamikoPlatform.__init__(self, expid, name, config)
        self.mkdir_checker = None
        self.remove_checker = None
        self.mkdir_cmd = None
        self.get_cmd = None
        self.put_cmd = None
        self._checkhost_cmd = None
        self.type = 'ps'
        self.cancel_cmd = None
        self._header = PsHeader()
        self.job_status = dict()
        self.job_status['COMPLETED'] = ['1']
        self.job_status['RUNNING'] = ['0']
        self.job_status['QUEUING'] = []
        self.job_status['FAILED'] = []
        self.update_cmds()
        self.has_scheduler = False

    def get_check_all_jobs_cmd(self, jobs_id: str) -> str:
        """Return a shell command that checks the status of all given process IDs.

        For each PID, the command outputs a line ``<pid> <status>`` where status
        is ``0`` if the process is running (non-zombie) and ``1`` otherwise.

        :param jobs_id: Comma-separated list of process IDs to check.
        :return: Shell command outputting ``<pid> <status>`` per line.
        """
        pids = jobs_id.replace(',', ' ')
        return (
            f"for pid in {pids}; do "
            f"echo \"$pid $(ps -o stat= -p $pid 2>/dev/null | grep -qv '^Z' && echo 0 || echo 1)\"; "
            f"done"
        )

    def parse_all_jobs_output(self, output: str, job_id: int) -> str:
        """Parse process-status output for the given job ID.

        :param output: Output of :meth:`get_check_all_jobs_cmd`.
        :param job_id: Process ID to look up.
        :return: ``'0'`` if running, ``'1'`` if not running or not found.
        """
        for line in output.splitlines():
            parts = line.split()
            if parts and parts[0] == str(job_id):
                return parts[1] if len(parts) > 1 else '1'
        return '1'

    def parse_queue_reason(self, output, job_id):
        pass  # pragma: no cover

    def create_a_new_copy(self):
        return PsPlatform(self.expid, self.name, self.config)

    def update_cmds(self):
        """Updates commands for platforms."""
        self.root_dir = os.path.join(self.scratch, self.project_dir, self.user, self.expid)
        self.remote_log_dir = os.path.join(self.root_dir, "LOG_" + self.expid)
        self.cancel_cmd = "kill -2"
        self._checkhost_cmd = "echo 1"
        self.put_cmd = "scp"
        self.get_cmd = "scp"
        self.mkdir_cmd = "mkdir -p " + self.remote_log_dir
        self.remove_checker = "rm -rf " + os.path.join(self.scratch, self.project_dir, self.user,
                                                       "ps_permission_checker_azxbyc")
        self.mkdir_checker = "mkdir -p " + os.path.join(self.scratch, self.project_dir, self.user,
                                                         "ps_permission_checker_azxbyc")


    def get_remote_log_dir(self):
        return self.remote_log_dir

    def get_mkdir_cmd(self):
        return self.mkdir_cmd

    def parse_job_output(self, output):
        return output

    def get_submitted_job_id(self, raw_output: str, x11: bool = False) -> list[str]:
        """Parses the output of the submit command to get the job ID.

        :param raw_output: output of the submit command.
        :param x11: whether the job is an x11 job, which has a different output format.
        :return: job ID of the submitted job.
        """
        return [line for line in (line.strip() for line in raw_output.splitlines()) if line.isdigit()]

    def get_check_job_cmd(self, job_id):
        return self.get_pscall(job_id)

    def check_remote_permissions(self) -> bool:
        try:
            try:
                self.send_command(self.remove_checker)
            except Exception as e:
                Log.debug(f'Failed to send command to remove checker files: {e}')
            self.send_command(self.mkdir_checker)
            self.send_command(self.remove_checker)
            return True
        except Exception as e:
            Log.debug(f'Failed to check remote dependencies for PS platform: {e}')

        return False

    def cancel_jobs(self, job_ids: list[str]) -> None:
        """Cancel remote processes by their PIDs.

        :param job_ids: List of remote process IDs to cancel.
        :type job_ids: list[str]
        """
        if not job_ids:
            return
        pids = " ".join(str(job_id) for job_id in job_ids)
        self.send_command(f"{self.cancel_cmd} {pids}")

    def _get_job_names_cmd(self, job_names: list) -> str:
        """Gets command to check for duplicated job names on remote platforms (UNIX)
        It receives a list of job names, and returns a command that:

        1) Checks if there is a job name already present on the remote platform.
        2) If there any, it returns the oldest ID ( process or job_id) entry for each duplicated job name, separated by commas.

        :param job_names: List of job names to check for duplicates.
        :type job_names: list[str]
        :return: Command to check for duplicated job names on remote platforms.
        :rtype: str
        """
        # check processes with the same name, and return the oldest one (if any)
        return f"ps -eo pid,cmd | grep -E '({'|'.join(job_names)})' | awk '{{print $1}}' | sort -n | uniq -d"

    def _get_process_list_output(self) -> str:
        """Return the remote ``ps -eo pid,cmd`` output via SSH.

        :return: Raw process list output, or an empty string on failure.
        :rtype: str
        """
        if self.send_command("ps -eo pid,cmd", ignore_log=True):
            return self.get_ssh_output()
        return ""

    def _check_for_unrecoverable_errors(self) -> None:
        """Check process-platform output for recoverable and unrecoverable errors."""
        out = self._ssh_output or ""
        err = self._ssh_output_err or ""

        # Fast-exit: stdout is a single digit (process status from get_pscall)
        # or contains lines starting with numeric PIDs (ps output).
        out_stripped = out.strip()
        if out_stripped in ("0", "1"):
            return
        if any(line.strip() and line.strip()[0].isdigit() for line in out.splitlines()):
            return

        err_lower = err.lower()
        if not err_lower.strip():
            return

        transient_patterns: list[str] = [
            "socket timed out",
            "socket error",
            "connection timed out",
            "connection refused",
            "connection reset by peer",
            "broken pipe",
            "network is unreachable",
            "not active",
            "temporary failure in name resolution",
        ]

        for pattern in transient_patterns:
            if pattern in err_lower:
                raise AutosubmitError(
                    f"Transient PS platform error: {err}",
                    6016,
                )

        # Permanent / unrecoverable patterns → AutosubmitCritical (7014).
        critical_patterns: list[str] = [
            "command not found",
            "kill: invalid signal",
            "no such process",
            "permission denied",
        ]

        for pattern in critical_patterns:
            if pattern in err_lower:
                raise AutosubmitCritical(
                    f"Permanent PS platform error: {err}",
                    7014,
                )
