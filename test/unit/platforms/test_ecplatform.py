#!/usr/bin/env python3

# Copyright 2015-2020 Earth Sciences Department, BSC-CNS

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

"""Unit tests for EcPlatform."""

import datetime
import subprocess
from pathlib import Path
from typing import Optional, Union

import pytest
from _pytest._py.path import LocalPath

from autosubmit.job.job_common import Status
from autosubmit.log.log import AutosubmitCritical, AutosubmitError
from autosubmit.platforms.ecplatform import EcPlatform
from autosubmit.platforms.headers.ec_cca_header import EcCcaHeader
from autosubmit.platforms.headers.ec_header import EcHeader
from autosubmit.platforms.headers.slurm_header import SlurmHeader
from autosubmit.platforms.paramiko_platform import ParamikoPlatformException
from autosubmit.platforms.platform_type import PlatformType

_EXPID = "t000"
"""Test expid."""

@pytest.fixture
def ec_platform(tmp_path: 'LocalPath'):
    """Create a minimal EcPlatform for unit tests."""
    config = {"LOCAL_ROOT_DIR": str(tmp_path), "LOCAL_TMP_DIR": "tmp"}
    from autosubmit.platforms.ecplatform import EcPlatform
    yield EcPlatform(expid=_EXPID, name='pytest-slurm', config=config, scheduler='slurm')


@pytest.mark.parametrize("config_retry,expected", [
    (None, 100),
    (50, 50),
    (0, 0),
])
def test_ec_retry_count_from_config(
    tmp_path: 'LocalPath',
    config_retry: int,
    expected: int,
) -> None:
    """Verify _ec_retry_count reads from PLATFORMS.<NAME>.ECACCESS_RETRIES, defaults to 100."""
    platforms = {}
    if config_retry is not None:
        platforms["TEST_ECMWF"] = {"ECACCESS_RETRIES": config_retry}
    config = {"LOCAL_ROOT_DIR": str(tmp_path), "LOCAL_TMP_DIR": "tmp", "PLATFORMS": platforms}
    platform = EcPlatform(expid=_EXPID, name='TEST_ECMWF', config=config, scheduler='slurm')
    assert platform._ec_retry_count == expected
    assert platform._ec_retry_flag == f"-retry {expected}"


@pytest.mark.parametrize("invalid_value", [
    "abc",
    "12.5",
    [1, 2, 3],
    {"key": "val"},
])
def test_ec_retry_count_fallback(
    tmp_path: 'LocalPath',
    invalid_value: object,
) -> None:
    """Verify _ec_retry_count falls back to 100 when ECACCESS_RETRIES is invalid."""
    platforms = {"TEST_ECMWF": {"ECACCESS_RETRIES": invalid_value}}
    config = {"LOCAL_ROOT_DIR": str(tmp_path), "LOCAL_TMP_DIR": "tmp", "PLATFORMS": platforms}
    platform = EcPlatform(expid=_EXPID, name='TEST_ECMWF', config=config, scheduler='slurm')
    assert platform._ec_retry_count == 100
    assert platform._ec_retry_flag == "-retry 100"


def test_file_read_size_and_send(ec_platform: EcPlatform, mocker):
    path = ec_platform.config.get("LOCAL_ROOT_DIR")
    assert isinstance(path, str)
    random_file = Path(path) / "random_file"
    assert isinstance(random_file, Path)
    with open(random_file, "w") as f:
        f.write("a" * 100)

    with pytest.raises(AutosubmitError):
        assert ec_platform.send_file(str(random_file))

    mocked_check_call = mocker.patch('autosubmit.platforms.ecplatform.subprocess')
    mocked_check_call.check_output.return_value = b""  # ensures decode() returns a str, not MagicMock
    mocked_check_call.check_call.return_value = True
    assert ec_platform.send_file(str(random_file))


# ecaccess-job-list tabular output used across several tests.
_JOB_LIST_OUTPUT = (
    "JOB Id  User  Status  Queue  Name\n"
    "------  ----  ------  -----  ----\n"
    "10001   user  EXEC    hpc    a000_INI\n"
    "10002   user  INIT    hpc    a000_SIM\n"
    "10003   user  EXEC    hpc    a000_INI\n"
)


def test_get_submitted_jobs_by_name_returns_latest_id_per_script(
    ec_platform: EcPlatform,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return the newest matching job ID for each submitted script.

    :param ec_platform: EcPlatform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    def _send_command(command: str, **_) -> bool:
        ec_platform._ssh_output = _JOB_LIST_OUTPUT
        return True

    monkeypatch.setattr(ec_platform, "send_command", _send_command)

    result = ec_platform.get_submitted_jobs_by_name(["a000_INI.cmd", "a000_SIM.cmd"])

    assert result == [10003, 10002]


def test_get_submitted_jobs_by_name_returns_empty_when_any_job_missing(
    ec_platform: EcPlatform,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return an empty list when any submitted script is absent from the job list.

    :param ec_platform: EcPlatform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    def _send_command(command: str, **_) -> bool:
        ec_platform._ssh_output = _JOB_LIST_OUTPUT
        return True

    monkeypatch.setattr(ec_platform, "send_command", _send_command)

    result = ec_platform.get_submitted_jobs_by_name(["a000_INI.cmd", "a000_MISSING.cmd"])

    assert result == []


@pytest.mark.parametrize("output", [
    # Header only (no active jobs).
    "JOB Id  User  Status  Queue  Name\n------  ----  ------  -----  ----\n",
    # Completely empty output.
    "",
])
def test_get_submitted_jobs_by_name_returns_empty_when_job_list_is_empty(
    ec_platform: EcPlatform,
    monkeypatch: pytest.MonkeyPatch,
    output: str,
) -> None:
    """Return an empty list when the job list contains no matching entries.

    :param ec_platform: EcPlatform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    :param output: Simulated ecaccess-job-list output.
    """
    def _send_command(command: str, **_) -> bool:
        ec_platform._ssh_output = output
        return True

    monkeypatch.setattr(ec_platform, "send_command", _send_command)

    result = ec_platform.get_submitted_jobs_by_name(["a000_INI.cmd"])

    assert result == []


def test_get_submitted_jobs_by_name_queries_ecaccess_job_list(
    ec_platform: EcPlatform,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify that exactly one ecaccess-job-list command is issued for any number of scripts.

    :param ec_platform: EcPlatform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    sent_commands: list[str] = []

    def _send_command(command: str, **_) -> bool:
        sent_commands.append(command)
        ec_platform._ssh_output = _JOB_LIST_OUTPUT
        return True

    monkeypatch.setattr(ec_platform, "send_command", _send_command)

    ec_platform.get_submitted_jobs_by_name(["a000_INI.cmd", "a000_SIM.cmd"])

    assert sent_commands == [f"ecaccess-job-list {ec_platform._ec_retry_flag}"]


def test_get_submitted_jobs_by_name_filters_pre_existing_job_ids(
    ec_platform: EcPlatform,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exclude job IDs that were already present before submission.

    Simulates a stale job (10001) from a previous run that is still active
    when a new submission adds job 10003.  Only the new ID must be returned.

    :param ec_platform: EcPlatform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    # Job 10001 existed before submission (stale, from a previous run).
    ec_platform._pre_submission_ids = {"a000_INI": {10001}}

    def _send_command(command: str, **_) -> bool:
        ec_platform._ssh_output = _JOB_LIST_OUTPUT
        return True

    monkeypatch.setattr(ec_platform, "send_command", _send_command)

    result = ec_platform.get_submitted_jobs_by_name(["a000_INI.cmd", "a000_SIM.cmd"])

    # 10001 must be filtered; 10003 is the freshly submitted replacement.
    assert result == [10003, 10002]


def test_get_submitted_jobs_by_name_returns_empty_when_no_new_job_submitted(
    ec_platform: EcPlatform,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return an empty list when every matching ID predates the submission.

    This covers the case where submission silently failed: ecaccess-job-list
    still shows the old jobs, but no new IDs appeared after the attempt.

    :param ec_platform: EcPlatform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    # All currently visible IDs for a000_INI existed before the submission.
    ec_platform._pre_submission_ids = {"a000_INI": {10001, 10003}}

    def _send_command(command: str, **_) -> bool:
        ec_platform._ssh_output = _JOB_LIST_OUTPUT
        return True

    monkeypatch.setattr(ec_platform, "send_command", _send_command)

    result = ec_platform.get_submitted_jobs_by_name(["a000_INI.cmd", "a000_SIM.cmd"])

    assert result == []


def test_snapshot_resets_and_captures_pre_existing_ids(
    ec_platform: EcPlatform,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify that _snapshot_job_ids_before_submission populates _pre_submission_ids.

    The snapshot must reset any stale state from a prior call and record only
    IDs matching the given script stems.

    :param ec_platform: EcPlatform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    # Seed a leftover snapshot from a previous cycle.
    ec_platform._pre_submission_ids = {"a000_OLD": {999}}

    def _send_command(command: str, **_) -> bool:
        ec_platform._ssh_output = _JOB_LIST_OUTPUT
        return True

    monkeypatch.setattr(ec_platform, "send_command", _send_command)

    ec_platform._snapshot_job_ids_before_submission(["a000_INI", "a000_SIM"])

    # Old entry must be gone; only the stems present in the job list remain.
    assert ec_platform._pre_submission_ids == {
        "a000_INI": {10001, 10003},
        "a000_SIM": {10002},
    }


@pytest.mark.parametrize("ssh_output,ssh_output_err", [
    # Empty or whitespace output – no error possible.
    ("", ""),
    ("   \n  ", ""),
    # Bare numeric job ID – ecaccess-job-submit success.
    ("12345", ""),
    ("  10001  \n", ""),
    # Known ecaccess job-state words (any case).
    ("EXEC", ""),
    ("done", ""),
    ("INIT", ""),
    ("STOP", ""),
    # Column-header row from ecaccess-job-list.
    ("JOB Id  User  Status  Queue  Name", ""),
    ("Status: EXEC", ""),
    # Completion markers from ecaccess-file-dir.
    ("some_job_COMPLETED", ""),
    ("job.out", ""),
    ("errors.err", ""),
])
def test_check_for_unrecoverable_errors_no_exception_for_valid_output(
    ec_platform: EcPlatform,
    ssh_output: Optional[str],
    ssh_output_err: Optional[str],
) -> None:
    """Verify that no exception is raised for known-valid ecaccess output.

    :param ec_platform: EcPlatform under test.
    :param ssh_output: Value to assign to ``_ssh_output``.
    :param ssh_output_err: Value to assign to ``_ssh_output_err``.
    """
    ec_platform._ssh_output = ssh_output
    ec_platform._ssh_output_err = ssh_output_err
    ec_platform._check_for_unrecoverable_errors()  # must not raise


@pytest.mark.parametrize("ssh_output,ssh_output_err", [
    (None, None),
])
def test_check_for_unrecoverable_errors_none_exception_expected(
    ec_platform: EcPlatform,
    ssh_output: Optional[str],
    ssh_output_err: Optional[str],
) -> None:
    """Verify that no exception is raised for known-valid ecaccess output.

    :param ec_platform: EcPlatform under test.
    :param ssh_output: Value to assign to ``_ssh_output``.
    :param ssh_output_err: Value to assign to ``_ssh_output_err``.
    """
    ec_platform._ssh_output = ssh_output
    ec_platform._ssh_output_err = ssh_output_err
    with pytest.raises(TypeError) as te:
        ec_platform._check_for_unrecoverable_errors()
    assert "expected string or bytes-like object" in te.value.args[0]


@pytest.mark.parametrize("output", [
    "SSH session not active",
    "git clone failed during submission",
    "no gateway available",
    "Connection refused to gateway",
    "Connection timed out",
    "Socket timed out connecting to ecaccess",
    "Network is unreachable",
    "SSL handshake failed",
    "Temporary failure in name resolution",
])
def test_check_for_unrecoverable_errors_raises_transient_error(
    ec_platform: EcPlatform,
    output: str,
) -> None:
    """Verify that transient ecaccess errors raise AutosubmitError.

    :param ec_platform: EcPlatform under test.
    :param output: Simulated ecaccess stdout containing a transient-error keyword.
    """
    ec_platform._ssh_output = output
    ec_platform._ssh_output_err = ""
    with pytest.raises(AutosubmitError):
        ec_platform._check_for_unrecoverable_errors()


@pytest.mark.parametrize("output", [
    "invalid queue specified for this job",
    "certificate not found on the gateway",
    "queue does not exist on this system",
    "no permission to submit to this queue",
    "not authorized for this ecaccess operation",
    "access denied by ecaccess gateway",
    "ecaccess job not found in the system",
    "invalid job specification provided",
    "job was not submitted to the queue",
    "submission failed for this job",
    "ecaccess command not found on gateway host",
])
def test_check_for_unrecoverable_errors_raises_critical_error(
    ec_platform: EcPlatform,
    output: str,
) -> None:
    """Verify that permanent ecaccess errors raise AutosubmitCritical.

    :param ec_platform: EcPlatform under test.
    :param output: Simulated ecaccess stdout containing a critical-error keyword.
    """
    ec_platform._ssh_output = output
    ec_platform._ssh_output_err = ""
    with pytest.raises(AutosubmitCritical):
        ec_platform._check_for_unrecoverable_errors()


def test_check_for_unrecoverable_errors_detects_error_in_stderr_only(
    ec_platform: EcPlatform,
) -> None:
    """Verify that an error present only in stderr is still detected.

    :param ec_platform: EcPlatform under test.
    """
    ec_platform._ssh_output = ""
    ec_platform._ssh_output_err = "Connection refused to ecaccess gateway"
    with pytest.raises(AutosubmitError):
        ec_platform._check_for_unrecoverable_errors()


def test_check_for_unrecoverable_errors_success_pattern_wins_over_stderr_error(
    ec_platform: EcPlatform,
) -> None:
    """Verify that a success pattern in stdout silences an error in stderr.

    When stdout contains a recognised ecaccess response (e.g. a job-state
    word), the command is considered successful and stderr content is ignored.

    :param ec_platform: EcPlatform under test.
    """
    ec_platform._ssh_output = "EXEC"  # valid ecaccess job-state → fast-exit
    ec_platform._ssh_output_err = "Connection refused"
    ec_platform._check_for_unrecoverable_errors()  # must not raise


def test_check_for_unrecoverable_errors_unknown_output_does_not_raise(
    ec_platform: EcPlatform,
) -> None:
    """Verify that unrecognised output matching no known pattern does not raise.

    :param ec_platform: EcPlatform under test.
    """
    ec_platform._ssh_output = "some unrecognised ecaccess output with no known keywords"
    ec_platform._ssh_output_err = ""
    ec_platform._check_for_unrecoverable_errors()  # must not raise


def test_pre_submission_snapshot_clears_and_captures_ids(
    ec_platform: EcPlatform,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify _pre_submission_snapshot clears stale state then delegates to the inner snapshot.

    ParamikoPlatform.submit_multiple_jobs calls ``self._pre_submission_snapshot``
    before issuing the submission command.  EcPlatform overrides this to
    reset ``_pre_submission_ids`` and record current job IDs so that
    ``get_submitted_jobs_by_name`` can exclude stale entries afterwards.

    :param ec_platform: EcPlatform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    ec_platform._pre_submission_ids = {"stale_job": {999}}

    snapshot_calls: list[list[str]] = []

    def _inner_snapshot(script_names: list) -> None:
        snapshot_calls.append(list(script_names))

    monkeypatch.setattr(ec_platform, "_snapshot_job_ids_before_submission", _inner_snapshot)

    ec_platform._pre_submission_snapshot(["a.cmd", "b.cmd"])

    assert ec_platform._pre_submission_ids == {}
    assert snapshot_calls == [["a.cmd", "b.cmd"]]


def test_check_remote_log_dir_creates_all_path_levels(
    ec_platform: EcPlatform,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify check_remote_log_dir calls ecaccess-file-mkdir for every path level.

    :param ec_platform: EcPlatform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    ec_platform.host = "hpc"
    ec_platform.scratch = "/scratch"
    ec_platform.project = "proj"
    ec_platform.user = "user1"
    ec_platform.expid = _EXPID
    ec_platform.remote_log_dir = "/scratch/proj/user1/t000/LOG_t000"

    import subprocess as sp
    called: list[str] = []

    def _check_output(cmd: str, **_) -> bytes:
        called.append(cmd)
        return b""

    monkeypatch.setattr(sp, "check_output", _check_output)

    ec_platform.check_remote_log_dir()

    expected_paths = [
        "hpc:/scratch",
        "hpc:/scratch/proj",
        "hpc:/scratch/proj/user1",
        "hpc:/scratch/proj/user1/t000",
        "hpc:/scratch/proj/user1/t000/LOG_t000",
    ]
    for expected in expected_paths:
        assert any(expected in c for c in called), (
            f"Expected ecaccess-file-mkdir for {expected!r} but got: {called}"
        )


def test_check_remote_log_dir_does_not_raise_when_dir_already_exists(
    ec_platform: EcPlatform,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify check_remote_log_dir treats a non-zero exit on the LOG dir as success.

    :param ec_platform: EcPlatform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    ec_platform.host = "hpc"
    ec_platform.scratch = "/scratch"
    ec_platform.project = "proj"
    ec_platform.user = "user1"
    ec_platform.expid = _EXPID
    ec_platform.remote_log_dir = "/scratch/proj/user1/t000/LOG_t000"

    import subprocess as sp

    def _check_output(cmd: str, **_) -> bytes:
        if "LOG_t000" in cmd:
            raise sp.CalledProcessError(1, cmd)
        return b""

    monkeypatch.setattr(sp, "check_output", _check_output)

    # Must not raise even when the final mkdir returns non-zero (already exists).
    ec_platform.check_remote_log_dir()


def test_check_remote_permissions_uses_ecaccess_file_mkdir(
    ec_platform: EcPlatform,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify check_remote_permissions uses ecaccess-file-mkdir for each path level.

    :param ec_platform: EcPlatform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    ec_platform.host = "hpc"
    ec_platform.scratch = "/scratch"
    ec_platform.project = "proj"
    ec_platform.user = "user1"
    ec_platform.expid = _EXPID
    ec_platform.check_remote_permissions_cmd = "ecaccess-file-mkdir hpc:/scratch/proj/user1/_permission_checker_azxbyc"
    ec_platform.check_remote_permissions_remove_cmd = "ecaccess-file-rmdir hpc:/scratch/proj/user1/_permission_checker_azxbyc"

    import subprocess as sp
    called: list[str] = []

    def _check_output(cmd: str, **_) -> bytes:
        called.append(cmd)
        return b""

    monkeypatch.setattr(sp, "check_output", _check_output)

    ec_platform.check_remote_permissions()

    mkdir_calls = [c for c in called if not c.startswith("ecaccess-file-rmdir")]
    for cmd in mkdir_calls:
        assert cmd.startswith("ecaccess-file-mkdir"), (
            f"Expected 'ecaccess-file-mkdir' prefix but got: {cmd!r}"
        )
        assert "hpc:/" in cmd, f"Host path missing in: {cmd!r}"


def test_delete_previous_run_files_by_job_names_calls_del_cmd(
    ec_platform: EcPlatform,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify delete_previous_run_files_by_job_names issues one del_cmd per suffix.

    :param ec_platform: EcPlatform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    ec_platform.expid = _EXPID
    ec_platform.remote_log_dir = "/scratch/t000/LOG_t000"
    ec_platform.host = "hpc"
    ec_platform.del_cmd = "ecaccess-file-delete"

    called: list[str] = []

    def _check_call(cmd: str, **_) -> int:
        called.append(cmd)
        return 0

    import subprocess as sp
    monkeypatch.setattr(sp, "check_call", _check_call)

    ec_platform.delete_previous_run_files_by_job_names(["t000_INI", "t000_SIM"])

    assert len(called) == 4  # 2 jobs × 2 suffixes (_COMPLETED, _FAILED)
    assert any("t000_INI_COMPLETED" in c for c in called)
    assert any("t000_INI_FAILED" in c for c in called)
    assert any("t000_SIM_COMPLETED" in c for c in called)
    assert any("t000_SIM_FAILED" in c for c in called)


def test_delete_previous_run_files_by_job_names_skips_when_expid_not_in_log_dir(
    ec_platform: EcPlatform,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify no commands are issued when expid is absent from remote_log_dir.

    :param ec_platform: EcPlatform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    ec_platform.expid = _EXPID
    ec_platform.remote_log_dir = "/scratch/other/LOG_other"

    called: list[str] = []
    import subprocess as sp
    monkeypatch.setattr(sp, "check_call", lambda cmd, **_: called.append(cmd))

    ec_platform.delete_previous_run_files_by_job_names(["t000_INI"])

    assert called == []


def test_delete_previous_stat_files_by_job_names_removes_matching_stat_files(
    ec_platform: EcPlatform,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify delete_previous_stat_files_by_job_names removes matched STAT files.

    :param ec_platform: EcPlatform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    ec_platform.expid = _EXPID
    ec_platform.remote_log_dir = "/scratch/t000/LOG_t000"
    ec_platform.host = "hpc"
    ec_platform.del_cmd = "ecaccess-file-delete"

    # ecaccess-file-dir output format: filename|size  NNNN
    dir_listing = (
        "t000_INI_STAT_0|size  5550\n"
        "t000_INI_STAT_1|size  5550\n"
        "t000_SIM_STAT_0|size  5550\n"
        "t000_OTHER_COMPLETED|size  5550\n"
    )

    def _send_command(command: str, **_) -> bool:
        ec_platform._ssh_output = dir_listing
        return True

    monkeypatch.setattr(ec_platform, "send_command", _send_command)

    called: list[str] = []
    import subprocess as sp
    monkeypatch.setattr(sp, "check_call", lambda cmd, **_: called.append(cmd))

    ec_platform.delete_previous_stat_files_by_job_names(["t000_INI"])

    assert len(called) == 2
    assert any("t000_INI_STAT_0" in c for c in called)
    assert any("t000_INI_STAT_1" in c for c in called)
    assert not any("t000_SIM" in c for c in called)
    assert not any("t000_OTHER" in c for c in called)


def test_pre_submission_snapshot_called_with_empty_list(
    ec_platform: EcPlatform,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify _pre_submission_snapshot still clears state for an empty script list.

    :param ec_platform: EcPlatform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    ec_platform._pre_submission_ids = {"stale": {1}}

    snapshot_calls: list[list[str]] = []
    monkeypatch.setattr(
        ec_platform,
        "_snapshot_job_ids_before_submission",
        lambda names: snapshot_calls.append(list(names)),
    )

    ec_platform._pre_submission_snapshot([])

    assert ec_platform._pre_submission_ids == {}
    assert snapshot_calls == [[]]


@pytest.mark.parametrize("cmd_attr,command_name", [
    ("cancel_cmd", "ecaccess-job-delete"),
    ("_checkjob_cmd", "ecaccess-job-list"),
    ("_checkhost_cmd", "ecaccess-certificate-list"),
    ("_checkvalidcert_cmd", "ecaccess-gateway-connected"),
    ("_submit_command_name", "ecaccess-job-submit"),
    ("put_cmd", "ecaccess-file-put"),
    ("get_cmd", "ecaccess-file-get"),
    ("del_cmd", "ecaccess-file-delete"),
], ids=[
    "cancel_cmd",
    "checkjob_cmd",
    "checkhost_cmd",
    "checkvalidcert_cmd",
    "submit_command_name",
    "put_cmd",
    "get_cmd",
    "del_cmd",
])
def test_update_cmds_includes_retry_in_command(
    ec_platform: EcPlatform,
    cmd_attr: str,
    command_name: str,
) -> None:
    """Verify each command variable from update_cmds contains the retry flag."""
    value = getattr(ec_platform, cmd_attr)
    assert ec_platform._ec_retry_flag in value, (
        f"{cmd_attr}={value!r} missing {ec_platform._ec_retry_flag!r}"
    )
    assert value.startswith(command_name), (
        f"{cmd_attr}={value!r} should start with {command_name!r}"
    )


@pytest.mark.parametrize("cmd_attr", [
    "mkdir_cmd",
    "check_remote_permissions_cmd",
    "check_remote_permissions_remove_cmd",
])
def test_update_cmds_includes_retry_in_path_commands(
    ec_platform: EcPlatform,
    cmd_attr: str,
) -> None:
    """Verify path-bearing command variables contain the retry flag in every ecaccess call."""
    value = getattr(ec_platform, cmd_attr)
    # Count how many separate ecaccess commands are in this string
    ecaccess_calls = [part for part in value.split(";") if "ecaccess-" in part]
    for call in ecaccess_calls:
        assert ec_platform._ec_retry_flag in call, (
            f"{cmd_attr} call {call!r} missing {ec_platform._ec_retry_flag!r}"
        )


@pytest.mark.parametrize("retry_count,expected_flag", [
    (30, "-retry 30"),
    (100, "-retry 100"),
])
def test_set_submit_cmd_uses_configured_retry(
    tmp_path: 'LocalPath',
    retry_count: int,
    expected_flag: str,
) -> None:
    """Verify _set_submit_cmd includes the configured retry flag via _submit_command_name."""
    config = {
        "LOCAL_ROOT_DIR": str(tmp_path), "LOCAL_TMP_DIR": "tmp",
        "PLATFORMS": {"TEST_ECMWF": {"ECACCESS_RETRIES": retry_count}},
    }
    platform = EcPlatform(expid=_EXPID, name='TEST_ECMWF', config=config, scheduler='slurm')
    platform._set_submit_cmd("hpc")
    assert expected_flag in platform._submit_cmd
    assert "-queueName hpc" in platform._submit_cmd


# -- Batch checking (check_all_jobs) tests

_JOB_LIST_TABLE = (
    "JOB Id  User  Status  Queue  Name\n"
    "------  ----  ------  -----  ----\n"
    "10001   user  EXEC    hpc    a000_INI\n"
    "10002   user  DONE    hpc    a000_SIM\n"
    "10003   user  STOP    hpc    a000_CLEAN\n"
)


def test_get_check_all_jobs_cmd_returns_ecaccess_job_list(
    ec_platform: EcPlatform,
) -> None:
    """Verify get_check_all_jobs_cmd returns the batch ecaccess list command."""
    assert ec_platform.get_check_all_jobs_cmd("10001,10002") == f"ecaccess-job-list {ec_platform._ec_retry_flag}"


def test_parse_all_jobs_output_finds_status_by_job_id(
    ec_platform: EcPlatform,
) -> None:
    """Verify parse_all_jobs_output extracts the correct status word from the table."""
    assert ec_platform.parse_all_jobs_output(_JOB_LIST_TABLE, 10001) == "EXEC"
    assert ec_platform.parse_all_jobs_output(_JOB_LIST_TABLE, 10002) == "DONE"
    assert ec_platform.parse_all_jobs_output(_JOB_LIST_TABLE, 10003) == "STOP"


def test_parse_all_jobs_output_returns_empty_for_missing_job(
    ec_platform: EcPlatform,
) -> None:
    """Verify parse_all_jobs_output returns '' when the job ID is absent."""
    assert ec_platform.parse_all_jobs_output(_JOB_LIST_TABLE, 99999) == ""
    assert ec_platform.parse_all_jobs_output("", 10001) == ""


def test_check_jobid_in_queue_always_returns_true(
    ec_platform: EcPlatform,
) -> None:
    """Verify _check_jobid_in_queue bypasses retries for ecaccess."""
    assert ec_platform._check_jobid_in_queue("any output", "10001,") is True
    assert ec_platform._check_jobid_in_queue("", "10001,") is True


def test_confirm_done_jobs_via_stat_downloads_and_reads_stat_files(
    ec_platform: EcPlatform,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Verify confirm_done_jobs_via_stat uses ecaccess-file-dir and ecaccess-file-get."""
    ec_platform.host = "hpc"
    ec_platform.remote_log_dir = "/scratch/t000/LOG_t000"
    ec_platform.tmp_path = str(tmp_path)
    ec_platform.get_cmd = "ecaccess-file-get"

    # Mock send_command for ecaccess-file-dir
    # ecaccess-file-dir output format: filename|size  NNNN
    def _send_command(cmd: str, **_) -> bool:
        ec_platform._ssh_output = (
            "t000_INI_STAT_0|size  5550\n"
            "t000_SIM_STAT_0|size  5550\n"
            "t000_OTHER_COMPLETED|size  5550\n"
        )
        return True

    monkeypatch.setattr(ec_platform, "send_command", _send_command)

    # Mock subprocess.check_output for ecaccess-file-get
    downloaded: list[str] = []

    def _check_output(cmd: str, **_) -> bytes:
        downloaded.append(cmd)
        # Create the local file with STAT content
        local_file = cmd.split()[-1]
        Path(local_file).write_text("COMPLETED\n")
        return b""

    monkeypatch.setattr(subprocess, "check_output", _check_output)

    # Create mock jobs
    class MockJob:
        def __init__(self, name: str, fail_count: int):
            self.name = name
            self.fail_count = fail_count

    job_list = [MockJob("t000_INI", 0), MockJob("t000_SIM", 0), MockJob("t000_MISSING", 0)]
    result = ec_platform.confirm_done_jobs_via_stat(job_list)

    # Only the first two jobs have STAT files
    assert result["t000_INI"] == Status.COMPLETED
    assert result["t000_SIM"] == Status.COMPLETED
    assert "t000_MISSING" not in result

    # Verify ecaccess-file-get was called for the existing STAT files
    assert any("t000_INI_STAT_0" in c for c in downloaded)
    assert any("t000_SIM_STAT_0" in c for c in downloaded)
    assert not any("t000_MISSING_STAT_0" in c for c in downloaded)


def test_set_start_time_from_remote_stat_file_downloads_and_parses_epoch(
    ec_platform: EcPlatform,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Verify set_start_time_from_remote_stat_file uses ecaccess-file-dir and ecaccess-file-get."""
    ec_platform.host = "hpc"
    ec_platform.remote_log_dir = "/scratch/t000/LOG_t000"
    ec_platform.tmp_path = str(tmp_path)
    ec_platform.get_cmd = "ecaccess-file-get"

    # Mock send_command for ecaccess-file-dir
    # ecaccess-file-dir output format: filename|size  NNNN
    def _send_command(cmd: str, **_) -> bool:
        ec_platform._ssh_output = (
            "t000_INI_STAT_0|size  5550\n"
            "t000_SIM_STAT_0|size  5550\n"
        )
        return True

    monkeypatch.setattr(ec_platform, "send_command", _send_command)

    # Mock subprocess.check_output for ecaccess-file-get
    downloaded: list[str] = []

    def _check_output(cmd: str, **_) -> bytes:
        downloaded.append(cmd)
        local_file = cmd.split()[-1]
        # Write an epoch timestamp as the first line
        Path(local_file).write_text("1715769600\n")
        return b""

    monkeypatch.setattr(subprocess, "check_output", _check_output)

    # Create mock jobs
    class MockJob:
        def __init__(self, name: str, fail_count: int):
            self.name = name
            self.fail_count = fail_count
            self.start_time_timestamp = None

    job_ini = MockJob("t000_INI", 0)
    job_sim = MockJob("t000_SIM", 0)
    job_missing = MockJob("t000_MISSING", 0)
    ec_platform.set_start_time_from_remote_stat_file([job_ini, job_sim, job_missing])

    # start_time_timestamp should be set for jobs that have STAT files
    expected_timestamp = datetime.datetime.fromtimestamp(1715769600).strftime("%Y%m%d%H%M%S")
    assert job_ini.start_time_timestamp == expected_timestamp
    assert job_sim.start_time_timestamp == expected_timestamp
    assert job_missing.start_time_timestamp is None

    # Verify ecaccess-file-get was called for the existing STAT files
    assert any("t000_INI_STAT_0" in c for c in downloaded)
    assert any("t000_SIM_STAT_0" in c for c in downloaded)
    assert not any("t000_MISSING_STAT_0" in c for c in downloaded)


@pytest.mark.parametrize(
    'scheduler,expected',
    [
        (PlatformType.PBS, EcCcaHeader),
        (PlatformType.LOAD_LEVELER, EcHeader),
        (PlatformType.SLURM, SlurmHeader),
        (PlatformType.LOCAL, ParamikoPlatformException)
    ]
)
def test_ecplatform_header_selected(
        scheduler: str,
        expected: type[Union[EcCcaHeader, EcHeader, SlurmHeader, Exception]],
        ec_platform: EcPlatform,
        tmp_path: Path
):
    """Test that ``EcPlatform`` correctly selects its wrapped platform header."""
    config = {
        "LOCAL_ROOT_DIR": str(tmp_path),
        "LOCAL_TMP_DIR": "tmp",
        "PLATFORMS": {},
    }
    if issubclass(expected, Exception):
        with pytest.raises(expected):
            EcPlatform(expid=_EXPID, name="test_select", config=config, scheduler=scheduler)
    else:
        platform = EcPlatform(
            expid=_EXPID, name="test_select", config=config, scheduler=scheduler
        )
        assert platform._header


def test_get_remote_log_dir(ec_platform):
    """The remote log dir will have the value specified in the constructor."""
    assert ec_platform.get_remote_log_dir() == f'{_EXPID}/LOG_{_EXPID}'


def test_check_remote_log_dir_failed_mkdir(ec_platform, mocker):
    """Test that ``ecaccess-file-mkdir`` raised error is captured."""
    mocker.patch("autosubmit.platforms.ecplatform.subprocess.check_output", side_effect=FileNotFoundError)

    with pytest.raises(AutosubmitError):
        ec_platform.check_remote_log_dir()

def test_get_mkdir_cmd(ec_platform):
    """Test the ``mkdir`` command for the ECaccess platform."""
    assert 'ecaccess-file-mkdir' in ec_platform.get_mkdir_cmd()

