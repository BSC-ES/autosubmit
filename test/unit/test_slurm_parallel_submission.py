# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
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

"""Regression tests for Slurm parallel submission helpers."""

from collections import OrderedDict
from pathlib import Path
from typing import Callable

import pytest

from autosubmit.platforms.slurmplatform import SlurmPlatform


@pytest.fixture
def slurm_platform(autosubmit_config: Callable) -> SlurmPlatform:
    """Create a Slurm platform configured for unit tests.

    :param autosubmit_config: Factory that builds Autosubmit configurations.
    :return: Configured Slurm platform instance.
    """
    expid = "a000"
    as_conf = autosubmit_config(expid, experiment_data={})
    exp_path = Path(as_conf.basic_config.LOCAL_ROOT_DIR, expid)
    aslogs_dir = exp_path / as_conf.basic_config.LOCAL_TMP_DIR / as_conf.basic_config.LOCAL_ASLOG_DIR
    aslogs_dir.mkdir(parents=True, exist_ok=True)
    (aslogs_dir / "submit_local.sh").touch()
    return SlurmPlatform(expid=expid, name="local", config=as_conf.experiment_data)


@pytest.mark.parametrize(
    "output, expected_job_ids",
    [
        ("Submitted batch job 123\nSubmitted batch job 456\n", [123, 456]),
    ],
    ids=["multiple-submissions"],
)
def test_get_submitted_job_id_parses_batched_output(
        slurm_platform: SlurmPlatform,
        output: str,
        expected_job_ids: list[int],
) -> None:
    """Parse all Slurm job IDs from a batched submission output.

    :param slurm_platform: Slurm platform under test.
    :param output: Raw submit command output.
    :param expected_job_ids: Expected parsed job identifiers.
    """
    assert slurm_platform.get_submitted_job_id(output) == expected_job_ids


def test_get_submitted_jobs_by_name_returns_latest_id_per_script(
        slurm_platform: SlurmPlatform,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return the newest matching job ID for each submitted script.

    :param slurm_platform: Slurm platform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    job_ids_by_name = {
        "job_a": ["101", "103"],
        "job_b": ["202"],
    }

    def _get_jobid_by_jobname(job_name: str) -> list[str]:
        return job_ids_by_name[job_name]

    monkeypatch.setattr(slurm_platform, "get_jobid_by_jobname", _get_jobid_by_jobname)

    assert slurm_platform.get_submitted_jobs_by_name(["job_a.cmd", "job_b.cmd"]) == [103, 202]


def test_get_submitted_jobs_by_name_returns_empty_when_any_job_is_missing(
        slurm_platform: SlurmPlatform,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return an empty result when any submitted script cannot be recovered.

    :param slurm_platform: Slurm platform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    job_ids_by_name = {
        "job_a": ["101"],
        "job_b": [],
    }

    def _get_jobid_by_jobname(job_name: str) -> list[str]:
        return job_ids_by_name[job_name]

    monkeypatch.setattr(slurm_platform, "get_jobid_by_jobname", _get_jobid_by_jobname)

    assert slurm_platform.get_submitted_jobs_by_name(["job_a.cmd", "job_b.cmd"]) == []


def test_cancel_jobs_uses_scheduler_comma_separated_syntax(
        slurm_platform: SlurmPlatform,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Send a single Slurm cancel command with comma-separated job IDs.

    :param slurm_platform: Slurm platform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    sent_commands: list[str] = []

    def _send_command(command: str, ignore_log: bool = False, x11: bool = False) -> bool:
        sent_commands.append(command)
        return True

    monkeypatch.setattr(slurm_platform, "send_command", _send_command)

    slurm_platform.cancel_jobs(["101", "202", "303"])

    assert sent_commands == ["scancel 101,202,303"]


def test_process_ready_jobs_accepts_integer_job_ids(
        slurm_platform: SlurmPlatform,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Assign package identifiers correctly when the scheduler returns integers.

    :param slurm_platform: Slurm platform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    assigned_job_ids: list[str] = []

    class DummyPackage:
        """Capture the package job identifier assigned by the platform."""

        def process_jobs_to_submit(self, job_id: str) -> None:
            """Store the assigned job identifier.

            :param job_id: Submitted job identifier.
            """
            assigned_job_ids.append(job_id)

    scripts_to_submit = OrderedDict(
        [
            ("job_a.cmd", DummyPackage()),
            ("job_b.cmd", DummyPackage()),
        ]
    )

    monkeypatch.setattr(slurm_platform, "submit_multiple_jobs", lambda _: [101, 202])
    monkeypatch.setattr(slurm_platform, "_check_and_cancel_duplicated_job_names", lambda _: None)

    slurm_platform.process_ready_jobs(scripts_to_submit)

    assert assigned_job_ids == ["101", "202"]


def test_get_job_names_cmd_contains_expected_components(
        slurm_platform: SlurmPlatform,
) -> None:
    """The generated command must query squeue and group IDs by job name.

    :param slurm_platform: Slurm platform under test.
    """
    cmd = slurm_platform._get_job_names_cmd(["job_a", "job_b"])
    assert "job_a" in cmd
    assert "job_b" in cmd


def test_cancel_jobs_empty_list_sends_no_command(
        slurm_platform: SlurmPlatform,
) -> None:
    """Send no command when the list of job IDs to cancel is empty.
    :param slurm_platform: Slurm platform under test.
    """
    slurm_platform.cancel_jobs([])


def test_check_and_cancel_duplicated_job_names_no_duplicates(
        slurm_platform: SlurmPlatform,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Do not cancel any jobs when every job name appears only once.

    :param slurm_platform: Slurm platform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    monkeypatch.setattr(slurm_platform, "send_command", lambda cmd, **_: None)
    slurm_platform._ssh_output = "job_a:1001\n"

    cancelled: list[str] = []
    monkeypatch.setattr(slurm_platform, "cancel_jobs", lambda ids: cancelled.extend(ids))

    slurm_platform._check_and_cancel_duplicated_job_names({"job_a.cmd": None})

    assert cancelled == []


def test_check_and_cancel_duplicated_job_names_with_duplicates(
        slurm_platform: SlurmPlatform,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancel the oldest (lowest-sorted) ID when a job name appears more than once.

    :param slurm_platform: Slurm platform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    monkeypatch.setattr(slurm_platform, "send_command", lambda cmd, **_: None)
    slurm_platform._ssh_output = "job_a:1001,1002\n"

    cancelled: list[str] = []
    monkeypatch.setattr(slurm_platform, "cancel_jobs", lambda ids: cancelled.extend(ids))

    slurm_platform._check_and_cancel_duplicated_job_names({"job_a.cmd": None})

    assert cancelled == ["1001"]
