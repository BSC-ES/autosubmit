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

"""Unit tests for EcPlatform."""

from pathlib import Path
from typing import Optional

import pytest
from _pytest._py.path import LocalPath

from autosubmit.log.log import AutosubmitCritical, AutosubmitError
from autosubmit.platforms.ecplatform import EcPlatform
from autosubmit.platforms.paramiko_platform import ParamikoPlatform


@pytest.fixture
def ec_platform(tmp_path: 'LocalPath'):
    """Create a minimal EcPlatform for unit tests."""
    config = {"LOCAL_ROOT_DIR": str(tmp_path), "LOCAL_TMP_DIR": "tmp"}
    yield EcPlatform(expid='t000', name='pytest-slurm', config=config, scheduler='slurm')


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

    assert sent_commands == ["ecaccess-job-list"]


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

    ec_platform._snapshot_job_ids_before_submission(["a000_INI.cmd", "a000_SIM.cmd"])

    # Old entry must be gone; only the stems present in the job list remain.
    assert ec_platform._pre_submission_ids == {
        "a000_INI": {10001, 10003},
        "a000_SIM": {10002},
    }


@pytest.mark.parametrize("ssh_output,ssh_output_err", [
    # Empty or whitespace output – no error possible.
    ("", ""),
    (None, None),
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


def test_submit_multiple_jobs_clears_and_snapshots_before_parent(
    ec_platform: EcPlatform,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reset state, call snapshot, then delegate to ParamikoPlatform.

    Verifies that _pre_submission_ids is cleared to an empty dict at the
    start of the call, that _snapshot_job_ids_before_submission receives the
    correct script stem list, and that the parent return value is forwarded.

    :param ec_platform: EcPlatform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    ec_platform._pre_submission_ids = {"stale_job": {999}}

    snapshot_calls: list[list[str]] = []

    def _snapshot(script_names: list) -> None:
        snapshot_calls.append(list(script_names))

    parent_calls: list[list[str]] = []

    def _parent_submit(self, scripts: dict) -> list[int]:
        parent_calls.append(list(scripts.keys()))
        return [42]

    monkeypatch.setattr(ec_platform, "_snapshot_job_ids_before_submission", _snapshot)
    monkeypatch.setattr(ParamikoPlatform, "submit_multiple_jobs", _parent_submit)

    result = ec_platform.submit_multiple_jobs({"a.cmd": None, "b.cmd": None})

    assert ec_platform._pre_submission_ids == {}
    assert snapshot_calls == [["a.cmd", "b.cmd"]]
    assert parent_calls == [["a.cmd", "b.cmd"]]
    assert result == [42]


def test_submit_multiple_jobs_empty_input_returns_empty(
    ec_platform: EcPlatform,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return an empty list immediately when no scripts are provided.

    :param ec_platform: EcPlatform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    snapshot_calls: list = []
    monkeypatch.setattr(
        ec_platform,
        "_snapshot_job_ids_before_submission",
        lambda names: snapshot_calls.append(names),
    )

    parent_calls: list = []

    def _parent_submit(self, scripts: dict) -> list[int]:
        parent_calls.append(scripts)
        return []

    monkeypatch.setattr(ParamikoPlatform, "submit_multiple_jobs", _parent_submit)

    result = ec_platform.submit_multiple_jobs({})

    assert result == []
    assert snapshot_calls == [[]]
