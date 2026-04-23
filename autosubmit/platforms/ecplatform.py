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

import locale
import os
import re
import subprocess
from contextlib import suppress
from pathlib import Path
from time import sleep
from typing import TYPE_CHECKING, Optional

from autosubmit.log.log import Log, AutosubmitError, AutosubmitCritical
from autosubmit.platforms.headers.ec_cca_header import EcCcaHeader
from autosubmit.platforms.headers.ec_header import EcHeader
from autosubmit.platforms.headers.slurm_header import SlurmHeader
from autosubmit.platforms.paramiko_platform import ParamikoPlatform, ParamikoPlatformException
from autosubmit.platforms.wrappers.wrapper_factory import EcWrapperFactory

if TYPE_CHECKING:
    from autosubmit.config.configcommon import AutosubmitConfig

_EC_EXPECTED_OUTPUT: tuple[re.Pattern, ...] = (
    # ecaccess-job-submit: returns a bare numeric job ID on success.
    re.compile(r"^\s*\d+\s*$", re.MULTILINE),
    # ecaccess-job-list: known ecaccess job-state words in the tabular output.
    re.compile(r"\b(done|exec|init|retr|stdby|wait|stop)\b", re.IGNORECASE),
    # ecaccess-job-list: column-header row ("JOB Id ... Status ...").
    re.compile(r"\bStatus\b|\bJOB\s+Id\b", re.IGNORECASE),
    # ecaccess-file-dir: directory listing contains completion markers or logs.
    re.compile(r"_COMPLETED|\.out\b|\.err\b", re.IGNORECASE),
)


class EcPlatform(ParamikoPlatform):
    """
    Class to manage queues with ecaccess

    :param expid: experiment's identifier
    :type expid: str
    :param scheduler: scheduler to use
    :type scheduler: str (pbs, loadleveler)
    """

    def parse_all_jobs_output(self, output, job_id):
        pass  # pragma: no cover

    def parse_queue_reason(self, output, job_id):
        pass  # pragma: no cover

    def get_check_all_jobs_cmd(self, jobs_id):
        pass  # pragma: no cover

    def __init__(self, expid, name, config, scheduler):
        ParamikoPlatform.__init__(self, expid, name, config)
        # version=scheduler
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
        self._pathdir = "\\$HOME/LOG_" + self.expid
        self._allow_arrays = False
        self._allow_wrappers = False  # TODO
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
        self._uses_local_api = True
        # Pre-submission snapshot used by get_submitted_jobs_by_name to exclude
        # stale jobs from previous runs.
        self._pre_submission_ids: dict[str, set[int]] = {}
        self.has_scheduler = False

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
        self.check_remote_permissions_cmd = "ecaccess-file-mkdir " + self.host + ":" + os.path.join(self.scratch,
                                                                                                    self.project,
                                                                                                    self.user,
                                                                                                    "_permission_checker_azxbyc")
        self.check_remote_permissions_remove_cmd = "ecaccess-file-rmdir " + self.host + ":" + os.path.join(self.scratch,
                                                                                                           self.project,
                                                                                                           self.user,
                                                                                                           "_permission_checker_azxbyc")

    def get_remote_log_dir(self):
        return self.remote_log_dir

    def get_mkdir_cmd(self):
        return self.mkdir_cmd

    def _set_submit_cmd(self, ec_queue: str):
        """Set the ecaccess-job-submit command for the given queue.

        The ``-retry 3w0`` flag tells ecaccess to retry the **initial SSL handshake**
        up to 30 times (one attempt per ~5 s) before giving up. This guards against
        transient SSL failures that occasionally occur when first connecting to the
        ECMWF gateway; without it, a single bad handshake would abort the submission
        entirely. It does **not** affect job-level retries or any subsequent requests
        made after the connection is established.

        See: https://confluence.ecmwf.int/display/ECAC/ecaccess-job-submit

        :param ec_queue: Queue to submit the job to.
        """
        self._submit_cmd = f"{self._submit_command_name} -retry 30 -distant -queueName {ec_queue} {self.host}:"

    def _construct_final_call(self, script_name: str, pre: str, post: str, x11_options: str):
        """Gets the command to submit a job, for the current platform, with the given parameters.
         This needs to be adapted to each scheduler, the default assumes that is being launched directly.

        :param script_name: name of the script to submit
        :param pre: command part to be placed before the script name, e.g. timeout, export, executable
        :param post: command part to be placed after the script name, e.g. redirection of stdout and stderr
        :param x11_options: x11 options to run the script, if any
        :return: command to submit a job
        """

        if x11_options:
            raise AutosubmitCritical(
                "X11 options are not supported for ecaccess jobs, as they need to be launched within an iterative node , which is not compatible with the current submission method. Please remove x11 options from your job configuration.")

        return f"{pre} {self._submit_cmd}{script_name} {post}"

    def check_all_jobs(self, job_list, as_conf, retries=5):
        for job, prev_status in job_list:
            self.check_job(job)

    def parse_job_output(self, output):
        job_state = output.split('\n')
        if len(job_state) > 7:
            job_state = job_state[7].split()
            if len(job_state) > 1:
                return job_state[1]
        return 'DONE'

    def get_submitted_job_id(self, output: str, x11: bool = False) -> list[str]:
        """Parses the output of the submit command to get the job ID.

        :param output: output of the submit command.
        :param x11: whether the job is an x11 job, which has a different output format.
        :return: job ID of the submitted job.
        """

        return [out.strip() for out in output.splitlines()]

    def get_check_job_cmd(self, job_id):
        return self._checkjob_cmd + str(job_id)

    def connect(self, as_conf: 'AutosubmitConfig', reconnect: bool = False, log_recovery_process: bool = False) -> None:
        """Establishes an SSH connection to the host.

        :param as_conf: The Autosubmit configuration object.
        :param reconnect: Indicates whether to attempt reconnection if the initial connection fails.
        :param log_recovery_process: Specifies if the call is made from the log retrieval process.
        :return: None
        """
        output = subprocess.check_output(self._checkvalidcert_cmd, shell=True).decode(locale.getlocale()[1])
        if not output:
            output = ""
        try:
            if output.lower().find("yes") != -1:
                self.connected = True
            else:
                Log.warning(
                    f"Connection to {self.host} could not be established. Please remember to weekly renew your ecaccess-certificate if you are using one.")
                self.connected = False
        except Exception:
            self.connected = False
        if not log_recovery_process:
            self.spawn_log_retrieval_process(as_conf)

    def create_a_new_copy(self):
        return EcPlatform(self.expid, self.name, self.config, self.scheduler)

    def restore_connection(self, as_conf: 'AutosubmitConfig', log_recovery_process: bool = False) -> None:
        """
        Restores the SSH connection to the platform.

        :param as_conf: The Autosubmit configuration object used to establish the connection.
        :type as_conf: AutosubmitConfig
        :param log_recovery_process: Indicates that the call is made from the log retrieval process.
        :type log_recovery_process: bool
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

    def test_connection(self, as_conf: 'AutosubmitConfig') -> None:
        """
        Tests the connection using the provided configuration.

        :param as_conf: The configuration to use for testing the connection.
        :type as_conf: AutosubmitConfig
        """
        self.connect(as_conf)

    def check_remote_permissions(self) -> bool:
        """Checks if the necessary permissions are in place on the remote host.
         There is no mkdir -p equivalent in ecaccess-file-mkdir.
         So we need to check permissions for each level of the path separately."""
        with suppress(Exception):
            subprocess.check_output(self.check_remote_permissions_remove_cmd, shell=True)
        with suppress(Exception):
            subprocess.check_output(f"{self.host}:{self.scratch}", shell=True)
        with suppress(Exception):
            subprocess.check_output(f"{self.host}:{self.scratch}/{self.project}", shell=True)
        with suppress(Exception):
            subprocess.check_output(f"{self.host}:{self.scratch}/{self.project}/{self.user}", shell=True)
        with suppress(Exception):
            subprocess.check_output(f"{self.host}:{self.scratch}/{self.project}/{self.user}/{self.expid}", shell=True)
        try:
            subprocess.check_output(self.check_remote_permissions_cmd, shell=True)
            subprocess.check_output(self.check_remote_permissions_remove_cmd, shell=True)
            return True
        except Exception as e:
            Log.warning(f"Remote permissions check failed: {e}")
            return False

    def send_command(self, command, ignore_log=False, x11=False) -> bool:
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
        self._check_for_unrecoverable_errors()
        return True

    def send_file(self, filename, check=True) -> bool:
        self.check_remote_log_dir()
        self.delete_file(filename)
        command = f'{self.put_cmd} {os.path.join(self.tmp_path, filename)} {self.host}:{os.path.join(self.get_files_path(), os.path.basename(filename))}'
        try:
            subprocess.check_call(command, shell=True)
        except subprocess.CalledProcessError as e:
            raise AutosubmitError('Could not send file {0} to {1}'.format(os.path.join(self.tmp_path, filename),
                                                                          os.path.join(self.get_files_path(),
                                                                                       filename)), 6005, str(e))
        return True

    def move_file(self, src, dest, must_exist=False):
        command = (f"ecaccess-file-move {self.host}:{os.path.join(self.remote_log_dir, src)} "
                   f"{self.host}:{os.path.join(self.remote_log_dir, dest)}")
        try:
            retries = 0
            sleeptime = 5
            process_ok = False
            FNULL = open(os.devnull, 'w')
            while not process_ok and retries < 5:
                process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=FNULL)
                out, _ = process.communicate()
                out = out.decode(locale.getlocale()[1])
                if 'No such file' in out or process.returncode != 0:
                    retries = retries + 1
                    process_ok = False
                    sleeptime = sleeptime + 5
                    sleep(sleeptime)
                else:
                    process_ok = True
        except Exception:
            process_ok = False
        if not process_ok:
            Log.printlog("Log file don't recovered {0}".format(src), 6004)
        return process_ok

    def get_file(self, filename, must_exist=True, relative_path='', ignore_log=False, wrapper_failed=False):
        local_path = os.path.join(self.tmp_path, relative_path)
        if not os.path.exists(local_path):
            os.makedirs(local_path)

        file_path = os.path.join(local_path, filename)
        if os.path.exists(file_path):
            os.remove(file_path)

        command = '{0} {3}:{2} {1}'.format(self.get_cmd, file_path, os.path.join(self.get_files_path(), filename),
                                           self.host)
        try:
            retries = 0
            sleeptime = 5
            process_ok = False
            FNULL = open(os.devnull, 'w')
            while not process_ok and retries < 5:
                process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=FNULL)
                out, _ = process.communicate()
                out = out.decode(locale.getlocale()[1])
                if 'No such file' in out or process.returncode != 0:
                    retries = retries + 1
                    process_ok = False
                    sleeptime = sleeptime + 5
                    sleep(sleeptime)
                else:
                    process_ok = True
        except Exception:
            process_ok = False
        if not process_ok and must_exist:
            Log.printlog("Completed/Stat File don't recovered {0}".format(filename), 6004)
        if not process_ok:
            Log.printlog("Log file don't recovered {0}".format(filename), 6004)
        return process_ok

    def delete_file(self, filename: str) -> bool:
        command = '{0} {1}:{2}'.format(self.del_cmd, self.host, os.path.join(self.get_files_path(), filename))
        try:
            FNULL = open(os.devnull, 'w')
            subprocess.check_call(command, stdout=FNULL, stderr=FNULL, shell=True)
        except subprocess.CalledProcessError:
            Log.debug('Could not remove file {0}', os.path.join(self.get_files_path(), filename))
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

    def get_completed_job_names(self, job_names: Optional[list[str]] = None) -> list[str]:
        """Retrieve the names of all files ending with '_COMPLETED' from the remote log directory using SSH.

        Uses ``ecaccess-file-dir`` to inspect the remote directory and filters
        results locally. If ``job_names`` is provided, only those names are checked.

        :param job_names: Optional job names to restrict the lookup.
        :return: Job names whose ``_COMPLETED`` marker exists remotely.
        """
        completed_job_names = []

        if self.expid in str(self.remote_log_dir):
            expected_files = (
                {f"{name}_COMPLETED" for name in job_names}
                if job_names
                else None
            )

            cmd = f"ecaccess-file-dir {self.host}:{self.remote_log_dir}"
            self.send_command(cmd)
            output = self.get_ssh_output()

            for line in output.splitlines():
                file_name = Path(line.strip().split()[-1].rstrip("/")).name if line.strip() else ""
                if not file_name.endswith("_COMPLETED") or (expected_files and file_name not in expected_files):
                    continue

                completed_job_names.append(file_name.removesuffix("_COMPLETED"))

        return completed_job_names

    def cancel_jobs(self, job_ids: list[str]) -> None:
        """Cancel ecaccess jobs by their IDs.

        :param job_ids: List of ecaccess job IDs to cancel.
        :type job_ids: list[str]
        """
        if not job_ids:
            return
        command = " ; ".join(f"{self.cancel_cmd} {job_id}" for job_id in job_ids)
        self.send_command(command)

    def _check_and_cancel_duplicated_job_names(self, scripts_to_submit: dict) -> None:
        """Check for duplicated job names in the submitted packages.

        :param scripts_to_submit: Package script names and their info.
        :type scripts_to_submit: dict
        """
        # There isen't a reliable way to check for duplicated job names in ecaccess, as the job list command doesn't return all the information needed to identify them,
        pass  # pragma: no cover

    def _check_for_unrecoverable_errors(self) -> None:
        """Check ecaccess command output for recoverable and unrecoverable errors.

        :raises AutosubmitError: For transient errors.
        :raises AutosubmitCritical: For permanent errors.
        """
        out = self._ssh_output or ""
        err = self._ssh_output_err or ""

        # Fast-exit: stdout matches a known ecaccess success pattern.
        if any(pat.search(out) for pat in _EC_EXPECTED_OUTPUT):
            return

        # ecaccess errors appear in stdout, not stderr; search both together.
        combined = (out + "\n" + err).lower()
        if not combined.strip():
            return

        # Transient / recoverable patterns → AutosubmitError (6016).
        transient_patterns: list[tuple[str, str]] = [
            ("not active", "SSH session not active"),
            ("git clone", "Git clone failed during submission; likely a transient network issue"),
            ("no gateway", "ecaccess gateway not available"),
            ("connection refused", "Connection to ecaccess gateway refused"),
            ("connection timed out", "Connection to ecaccess gateway timed out"),
            ("socket timed out", "Socket timed out communicating with ecaccess"),
            ("network is unreachable", "Network unreachable; cannot reach ecaccess gateway"),
            ("ssl", "ecaccess SSL or certificate issue"),
            ("temporary failure in name resolution", "DNS resolution failed temporarily"),
        ]

        for pattern, message in transient_patterns:
            if pattern in combined:
                raise AutosubmitError(
                    f"Transient ecaccess error: {message}",
                    6016,
                    (out + "\n" + err).strip(),
                )

        # Permanent / unrecoverable patterns → AutosubmitCritical (7014).
        critical_patterns: list[tuple[str, str]] = [
            ("invalid queue", "Invalid ecaccess queue specified"),
            ("certificate not found", "ecaccess certificate not found or expired"),
            ("queue does not exist", "ecaccess queue does not exist"),
            ("no permission", "No permission to submit to this ecaccess queue"),
            ("not authorized", "Not authorised for this ecaccess operation"),
            ("access denied", "Access denied by ecaccess gateway"),
            ("job not found", "ecaccess job not found"),
            ("invalid job", "Invalid ecaccess job specification"),
            ("not submitted", "Job was not submitted to ecaccess; check directives"),
            ("submission failed", "ecaccess submission failed; check directives"),
            ("command not found", "ecaccess command not found on the gateway host"),
        ]

        for pattern, message in critical_patterns:
            if pattern in combined:
                raise AutosubmitCritical(
                    f"Permanent ecaccess error: {message}",
                    7014,
                )

    def _snapshot_job_ids_before_submission(self, script_names: list[str]) -> None:
        """Snapshot currently active ecaccess job IDs for the given script names.

        :param script_names: Script filenames about to be submitted.
        :type script_names: list[str]
        """

        with suppress(Exception):
            self.send_command("ecaccess-job-list")
            output = self.get_ssh_output()
            pre: dict[str, set[int]] = {}
            for line in output.splitlines():
                parts = line.split()
                if len(parts) >= 2 and parts[0].isdigit() and parts[-1] in script_names:
                    pre.setdefault(parts[-1], set()).add(int(parts[0]))
            self._pre_submission_ids = pre

    def _pre_submission_snapshot(self, script_names: list[str]) -> None:
        """Snapshot currently active ecaccess job IDs before a submission batch.

        Overrides the base to populate `_pre_submission_ids` with
        ecaccess job IDs so that `get_submitted_jobs_by_name` can
        distinguish freshly submitted jobs from those of a previous run.

        :param script_names: Script filenames about to be submitted.
        :type script_names: list[str]
        """
        self._pre_submission_ids = {}
        self._snapshot_job_ids_before_submission(script_names)

    def get_submitted_jobs_by_name(self, script_names: list[str]) -> list[int]:
        """Return submitted ecaccess job IDs by script name.

        This fallback is used when the batched submit command does not return
        one recoverable job identifier per submitted script.

        :param script_names: Submitted script filenames.
        :type script_names: list[str]
        :return: Matching ecaccess job IDs in submission order, one per script.
            Returns an empty list if any script name has no newly submitted job.
        :rtype: list[int]
        """
        self.send_command("ecaccess-job-list")
        output = self.get_ssh_output()

        name_to_ids: dict[str, set[int]] = {}
        for line in output.splitlines():
            parts = line.split()
            if len(parts) >= 2 and parts[0].isdigit():
                name_to_ids.setdefault(parts[-1], set()).add(int(parts[0]))

        submitted_job_ids: list[int] = []
        for script_name in script_names:
            job_name = Path(script_name).stem
            all_ids = name_to_ids.get(job_name, set())
            new_ids = all_ids - self._pre_submission_ids.get(job_name, set())
            if not new_ids:
                return []
            submitted_job_ids.append(max(new_ids))

        return submitted_job_ids
