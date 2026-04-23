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

"""Unit tests for _check_for_unrecoverable_errors across all platform types.

Each platform's method follows the same contract:

1. If stdout matches a known success pattern  → return silently (fast-exit).
2. If stdout is unexpected but stderr is empty → return silently (no-op).
3. If stderr contains a transient pattern      → raise AutosubmitError (6016).
4. If stderr contains a permanent pattern      → raise AutosubmitCritical (7014).

EcPlatform is the exception: its send_command uses subprocess, so
_ssh_output_err is never populated; errors are looked up in the combined
stdout + stderr string instead.
"""

import pytest

from autosubmit.log.log import AutosubmitCritical, AutosubmitError


def _set_output(platform, stdout: str, stderr: str = "") -> None:
    platform._ssh_output = stdout
    platform._ssh_output_err = stderr


@pytest.mark.parametrize("stdout", [
    "Submitted batch job 1001\n",
    "Submitted batch job 1001\nSubmitted batch job 1002\nSubmitted batch job 1003\n",
    "     1001      COMPLETED\n     1002      RUNNING\n     1003      PENDING\n",
    "COMPLETED\nRUNNING\nFAILED\n",
    "JOBID,REASON\n1001,(None)\n1002,(Priority)\n",
    "             JOBID PARTITION     NAME     USER ST       TIME  NODES NODELIST(REASON)\n",
    "JobID           JobName  Partition    Account  AllocCPUS      State ExitCode \n"
    "------------ ---------- ---------- ---------- ---------- ---------- -------- \n",
    "JobId=1001 JobName=my_job UserId=user ...\n",
    "1001       FAILED\n",
    "CANCELLED\n",
    "OUT_OF_MEMORY\n",
    "CONFIGURING\n",
    "RESIZING\n",
    "NODE_FAIL\n",
    "PREEMPTED\n",
    "SUSPENDED\n",
])
def test_slurm_fast_exit_on_expected_stdout(slurm_platform, stdout):
    """Return silently when stdout matches a known Slurm success pattern."""
    _set_output(slurm_platform, stdout, stderr="sbatch: error: some unrelated warning")
    slurm_platform._check_for_unrecoverable_errors()  # must not raise


def test_slurm_no_exception_when_stderr_empty(slurm_platform):
    """Return silently when stdout is unexpected but stderr is empty."""
    _set_output(slurm_platform, stdout="", stderr="")
    slurm_platform._check_for_unrecoverable_errors()


@pytest.mark.parametrize("stderr", [
    "sbatch: error: Socket timed out on send/recv operation\n",
    "sbatch: error: slurm_persist_conn_open_without_init: ...\n",
    "slurmctld: error: reached\n",
    "slurmdbd: error: ...\n",
    "sbatch: error: Connection refused\n",
    "sbatch: error: Broken pipe\n",
    "sbatch: error: Communication failure\n",
    "Temporary failure in name resolution\n",
    "SSH session not active\n",
    "git clone failed: network error\n",
    "sbatch: error: Socket error on connection\n",
    "sbatch: error: Connection timed out\n",
    "sbatch: error: Connection reset by peer\n",
    "sbatch: error: Network is unreachable\n",
    "sbatch: error: Unable to connect to slurm daemon\n",
    "sbatch: error: Communication error\n",
])
def test_slurm_raises_autosubmit_error_for_transient(slurm_platform, stderr):
    """Raise AutosubmitError (6016) for transient connectivity issues."""
    _set_output(slurm_platform, stdout="", stderr=stderr)
    with pytest.raises(AutosubmitError) as exc_info:
        slurm_platform._check_for_unrecoverable_errors()
    assert exc_info.value.code == 6016


@pytest.mark.parametrize("stderr", [
    "sbatch: error: Invalid partition name specified\n",
    "sbatch: error: Batch job submission failed: Invalid QOS\n",
    "sbatch: error: Batch job submission failed: Invalid account\n",
    "sbatch: error: Unrecognized option '--bla-flag'\n",
    "sbatch: error: Batch job submission failed: ...\n",
    "sbatch: error: Requested node configuration is not available\n",
    "sbatch: error: Job violates accounting/qos policy\n",
    "sbatch: error: User is not allowed to submit\n",
    "sbatch: error: Invalid --time specification\n",
    "sacct: error: syntax error\n",
    "sbatch: command not found\n",
    "sbatch: error: Job exceeds limit\n",
    "sbatch: error:\n",
    "sbatch: error: Invalid constraint specified\n",
    "sbatch: error: Invalid --time value\n",
    "sbatch: error: Invalid --mem value\n",
    "sbatch: error: Invalid --nodes specification\n",
    "sbatch: error: Invalid --ntasks specification\n",
    "sbatch: error: Invalid --cpus-per-task value\n",
    "sbatch: error: Job not submitted to the scheduler\n",
    "sbatch: error: Job violates accounting policy\n",
    "sbatch: error: user/account not found on this system\n",
    "sbatch: error: account has insufficient quota\n",
    "salloc: error: too many CPUs requested\n",
    "sbatch: error: unknown option 'bla'\n",
    "sbatch: error: cpu count per node too high\n",
    "sbatch: error: Invalid --partition bla\n",
    "sbatch: error: Invalid --qos bla\n",
    "sbatch: error: Invalid --account bla\n",
])
def test_slurm_raises_autosubmit_critical_for_permanent(slurm_platform, stderr):
    """Raise AutosubmitCritical (7014) for permanent configuration errors."""
    _set_output(slurm_platform, stdout="", stderr=stderr)
    with pytest.raises(AutosubmitCritical) as exc_info:
        slurm_platform._check_for_unrecoverable_errors()
    assert exc_info.value.code == 7014


def test_slurm_transient_wins_over_critical(slurm_platform):
    """Transient patterns are checked first, even if critical keywords also present."""
    stderr = "sbatch: error: Connection refused\nsbatch: error: Invalid partition\n"
    _set_output(slurm_platform, stdout="", stderr=stderr)
    with pytest.raises(AutosubmitError):
        slurm_platform._check_for_unrecoverable_errors()


@pytest.mark.parametrize("stdout", [
    "[INFO] PJM 0000 pjsub Job 12345 submitted.\n",
    "[INFO] PJM 0000 pjsub Job 12345 submitted.\n[INFO] PJM 0000 pjsub Job 12346 submitted.\n",
    "[INFO] PJM 0000 pjdel Job 12345 deleted.\n",
    "  12345  RUN  -\n  12346  QUE  -\n  12347  EXT  COMPLETED\n",
    "EXT\n",
    "RNO\nRNE\nRUN\n",
    "ERR\nCCL\nRJT\n",
    "JOBID  ST  REASON\n",
    "JID  ST\n",
    "ACC\n",
    "RNA\nRNP\n",
    "HLD\n",
])
def test_pjm_fast_exit_on_expected_stdout(pjm_platform, stdout):
    """Return silently when stdout matches a known PJM success pattern."""
    _set_output(pjm_platform, stdout, stderr="[ERR.] PJM some warning")
    pjm_platform._check_for_unrecoverable_errors()


def test_pjm_no_exception_when_stderr_empty(pjm_platform):
    """Return silently when stdout is unexpected but stderr is empty."""
    _set_output(pjm_platform, stdout="", stderr="")
    pjm_platform._check_for_unrecoverable_errors()


@pytest.mark.parametrize("stderr", [
    "pjsub: Cannot connect to server\n",
    "pjsub: Connection timed out\n",
    "pjsub: Connection refused\n",
    "pjsub: Connection reset by peer\n",
    "pjsub: Broken pipe\n",
    "pjsub: Network is unreachable\n",
    "pjsub: Server not responding\n",
    "pjsub: Communication failure\n",
    "Temporary failure in name resolution\n",
    "pjsub: SSH session not active\n",
    "pjsub: Socket timed out\n",
    "pjsub: Socket error\n",
    "pjsub: Communication error\n",
])
def test_pjm_raises_autosubmit_error_for_transient(pjm_platform, stderr):
    """Raise AutosubmitError (6016) for transient PJM connectivity issues."""
    _set_output(pjm_platform, stdout="", stderr=stderr)
    with pytest.raises(AutosubmitError) as exc_info:
        pjm_platform._check_for_unrecoverable_errors()
    assert exc_info.value.code == 6016


@pytest.mark.parametrize("stderr", [
    "[ERR.] PJM 0007 pjsub Resource group does not exist\n",
    "pjsub: Invalid resource group specified\n",
    "pjsub: Resource group does not exist\n",
    "pjsub: Resource group stop\n",
    "pjsub: No permission to this resource group\n",
    "pjsub: Exceeds limit of elapse time\n",
    "pjsub: Invalid job name\n",
    "pjsub: Invalid elapse\n",
    "pjsub: Unrecognized option '--bad'\n",
    "pjsub: Unknown option\n",
    "pjsub: Syntax error\n",
    "pjsub: command not found\n",
    "pjsub: Job not submitted to the scheduler\n",
    "pjsub: Submission failed; check directives\n",
])
def test_pjm_raises_autosubmit_critical_for_permanent(pjm_platform, stderr):
    """Raise AutosubmitCritical (7014) for permanent PJM configuration errors."""
    _set_output(pjm_platform, stdout="", stderr=stderr)
    with pytest.raises(AutosubmitCritical) as exc_info:
        pjm_platform._check_for_unrecoverable_errors()
    assert exc_info.value.code == 7014


def test_pjm_transient_wins_over_critical(pjm_platform):
    """Transient patterns are checked first, even if critical keywords also present."""
    stderr = "pjsub: Connection refused\npjsub: Invalid resource group specified\n"
    _set_output(pjm_platform, stdout="", stderr=stderr)
    with pytest.raises(AutosubmitError):
        pjm_platform._check_for_unrecoverable_errors()


@pytest.mark.parametrize("stdout", [
    # ecaccess-job-submit: bare numeric job ID
    "12345678\n",
    # ecaccess-job-list: state words
    "JOB Id  User  Status  Queue  Name\n------  ----  ------  -----  ----\n12345   user  EXEC    hpc    myjob\n",
    "DONE\n",
    "EXEC\n",
    "INIT\n",
    "RETR\n",
    "STDBY\n",
    "WAIT\n",
    "STOP\n",
    # ecaccess-job-list header row (no jobs currently active)
    "JOB Id  User  Status  Queue  Name\n",
    "Status: DONE\n",
    # ecaccess-file-dir: completion marker files
    "t000_INI_COMPLETED\nt000_SIM_COMPLETED\n",
    "job_12345.out\n",
    "job_12345.err\n",
])
def test_ec_fast_exit_on_expected_stdout(ec_platform, stdout):
    """Return silently when stdout matches a known ecaccess success pattern."""
    _set_output(ec_platform, stdout, stderr="")
    ec_platform._check_for_unrecoverable_errors()


def test_ec_no_exception_when_all_empty(ec_platform):
    """Return silently when both stdout and stderr are empty."""
    _set_output(ec_platform, stdout="", stderr="")
    ec_platform._check_for_unrecoverable_errors()


@pytest.mark.parametrize("stdout", [
    "ecaccess: No gateway available\n",
    "ecaccess: Connection refused\n",
    "ecaccess: Connection timed out\n",
    "ecaccess: Socket timed out\n",
    "ecaccess: Network is unreachable\n",
    "ecaccess: SSL handshake failed\n",
    "Temporary failure in name resolution\n",
    "ecaccess: SSH session not active\n",
    "git clone failed: network error\n",
])
def test_ec_raises_autosubmit_error_for_transient(ec_platform, stdout):
    """Raise AutosubmitError (6016) for transient ecaccess gateway issues."""
    _set_output(ec_platform, stdout=stdout, stderr="")
    with pytest.raises(AutosubmitError) as exc_info:
        ec_platform._check_for_unrecoverable_errors()
    assert exc_info.value.code == 6016


@pytest.mark.parametrize("stdout", [
    "ecaccess-job-submit: Invalid queue specified\n",
    "ecaccess-job-submit: Queue does not exist\n",
    "ecaccess-job-submit: No permission to submit\n",
    "ecaccess-job-submit: Not authorized\n",
    "ecaccess-job-submit: Access denied\n",
    "ecaccess-job-list: Job not found\n",
    "ecaccess-job-submit: Invalid job specification\n",
    "ecaccess-job-submit: command not found\n",
    "ecaccess-job-submit: Job not submitted to the gateway\n",
    "ecaccess-job-submit: Submission failed; check directives\n",
])
def test_ec_raises_autosubmit_critical_for_permanent(ec_platform, stdout):
    """Raise AutosubmitCritical (7014) for permanent ecaccess configuration errors."""
    _set_output(ec_platform, stdout=stdout, stderr="")
    with pytest.raises(AutosubmitCritical) as exc_info:
        ec_platform._check_for_unrecoverable_errors()
    assert exc_info.value.code == 7014


def test_ec_checks_stderr_as_fallback(ec_platform):
    """Errors found in stderr are also classified (paramiko path fallback)."""
    _set_output(ec_platform, stdout="", stderr="ecaccess: SSL error\n")
    with pytest.raises(AutosubmitError) as exc_info:
        ec_platform._check_for_unrecoverable_errors()
    assert exc_info.value.code == 6016


def test_ec_transient_wins_over_critical(ec_platform):
    """Transient patterns are checked first, even if critical keywords also present."""
    stdout = "ecaccess: Connection refused\necaccess-job-submit: Invalid queue specified\n"
    _set_output(ec_platform, stdout=stdout, stderr="")
    with pytest.raises(AutosubmitError):
        ec_platform._check_for_unrecoverable_errors()


@pytest.mark.parametrize("stdout", [
    "0\n",
    "1\n",
    "  1234 bash my_script.sh\n  5678 python worker.py\n",
    "9999\n",
])
def test_ps_fast_exit_on_expected_stdout(ps_platform, stdout):
    """Return silently when stdout contains valid process-status output."""
    _set_output(ps_platform, stdout, stderr="some stderr noise")
    ps_platform._check_for_unrecoverable_errors()


def test_ps_no_exception_when_stderr_empty(ps_platform):
    """Return silently when stdout is unexpected and stderr is empty."""
    _set_output(ps_platform, stdout="", stderr="")
    ps_platform._check_for_unrecoverable_errors()


@pytest.mark.parametrize("stderr", [
    "SSH connection timed out\n",
    "Connection refused\n",
    "Socket timed out\n",
    "Network is unreachable\n",
    "Broken pipe\n",
    "Connection reset by peer\n",
    "Temporary failure in name resolution\n",
    "SSH session not active\n",
])
def test_ps_raises_autosubmit_error_for_transient(ps_platform, stderr):
    """Raise AutosubmitError (6016) for transient SSH connectivity issues."""
    _set_output(ps_platform, stdout="unexpected text", stderr=stderr)
    with pytest.raises(AutosubmitError) as exc_info:
        ps_platform._check_for_unrecoverable_errors()
    assert exc_info.value.code == 6016


@pytest.mark.parametrize("stderr", [
    "bash: kill: command not found\n",
    "kill: invalid signal specification: SIGBAD\n",
    "kill: No such process\n",
    "bash: permission denied\n",
])
def test_ps_raises_autosubmit_critical_for_permanent(ps_platform, stderr):
    """Raise AutosubmitCritical (7014) for permanent process-platform errors."""
    _set_output(ps_platform, stdout="unexpected text", stderr=stderr)
    with pytest.raises(AutosubmitCritical) as exc_info:
        ps_platform._check_for_unrecoverable_errors()
    assert exc_info.value.code == 7014


@pytest.mark.parametrize("stdout,stderr", [
    ("", ""),
    ("some unexpected output", "command not found"),
    ("", "connection refused"),
    ("Submitted batch job 1\n", ""),
])
def test_local_always_returns_without_raising(local_platform, stdout, stderr):
    """LocalPlatform._check_for_unrecoverable_errors."""
    _set_output(local_platform, stdout, stderr)
    local_platform._check_for_unrecoverable_errors()  # must never raise


def test_pbs_no_exception_when_stderr_empty(pbs_platform):
    """Return silently when stderr is empty."""
    _set_output(pbs_platform, stdout="", stderr="")
    pbs_platform._check_for_unrecoverable_errors()


@pytest.mark.parametrize("stderr", [
    "not active\n",
    "qsub: Socket timed out on send/recv operation\n",
    "qsub: Socket error\n",
    "qsub: Connection timed out\n",
    "qsub: Connection refused\n",
    "qsub: Connection reset by peer\n",
    "qsub: Broken pipe\n",
    "qsub: Network is unreachable\n",
    "qsub: Unable to connect to PBS server\n",
    "PBS: Communication failure\n",
    "PBS: Communication error\n",
    "Temporary failure in name resolution\n",
])
def test_pbs_raises_autosubmit_error_for_transient(pbs_platform, stderr):
    """Raise AutosubmitError (6016) for transient PBS connectivity issues."""
    _set_output(pbs_platform, stdout="", stderr=stderr)
    with pytest.raises(AutosubmitError) as exc_info:
        pbs_platform._check_for_unrecoverable_errors()
    assert exc_info.value.code == 6016


@pytest.mark.parametrize("stderr", [
    "qsub: Job violates resource limits\n",
    "qsub: Illegal attribute or resource value\n",
    "qsub: Unknown resource\n",
    "qsub: Job violates queue constraints\n",
    "qsub: Invalid queue specified\n",
    "qsub: Not authorized to submit to this queue\n",
    "qsub: Bad UID for job execution\n",
    "qsub: Not allowed to submit\n",
    "PBS scheduler is not installed\n",
    "qsub: error: Syntax error in job script\n",
    "qsub: command not found\n",
    "PBS syntax error in directives\n",
])
def test_pbs_raises_autosubmit_critical_for_permanent(pbs_platform, stderr):
    """Raise AutosubmitCritical (7014) for permanent PBS configuration errors."""
    _set_output(pbs_platform, stdout="", stderr=stderr)
    with pytest.raises(AutosubmitCritical) as exc_info:
        pbs_platform._check_for_unrecoverable_errors()
    assert exc_info.value.code == 7014


def test_pbs_transient_wins_over_critical(pbs_platform):
    """Transient patterns are checked first, even if critical keywords also present."""
    stderr = "qsub: Connection refused\nqsub: Invalid queue specified\n"
    _set_output(pbs_platform, stdout="", stderr=stderr)
    with pytest.raises(AutosubmitError):
        pbs_platform._check_for_unrecoverable_errors()
