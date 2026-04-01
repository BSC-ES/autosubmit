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

"""Unit tests for the ``cancel_jobs`` method across all platform implementations.
"""

import pytest

from autosubmit.platforms.ecplatform import EcPlatform
from autosubmit.platforms.locplatform import LocalPlatform
from autosubmit.platforms.psplatform import PsPlatform
from autosubmit.platforms.slurmplatform import SlurmPlatform


def test_slurm_cancel_jobs_empty_list_sends_no_command(
        slurm_platform: SlurmPlatform,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Send no command when the list of Slurm job IDs is empty."""
    sent: list[str] = []
    monkeypatch.setattr(slurm_platform, "send_command", lambda cmd, **_: sent.append(cmd))

    slurm_platform.cancel_jobs([])

    assert sent == []


def test_slurm_cancel_jobs_single_id(
        slurm_platform: SlurmPlatform,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Send a single scancel command for one Slurm job ID."""
    sent: list[str] = []
    monkeypatch.setattr(slurm_platform, "send_command", lambda cmd, **_: sent.append(cmd))

    slurm_platform.cancel_jobs(["42"])

    assert sent == ["scancel 42"]


@pytest.mark.parametrize("job_ids,expected_command", [
    (["1", "2", "3"], "scancel 1,2,3"),
    (["100", "200"], "scancel 100,200"),
])
def test_slurm_cancel_jobs_multiple_ids_uses_comma_join(
        slurm_platform: SlurmPlatform,
        monkeypatch: pytest.MonkeyPatch,
        job_ids: list[str],
        expected_command: str,
) -> None:
    """Send one comma-joined scancel command for multiple Slurm job IDs."""
    sent: list[str] = []
    monkeypatch.setattr(slurm_platform, "send_command", lambda cmd, **_: sent.append(cmd))

    slurm_platform.cancel_jobs(job_ids)

    assert len(sent) == 1
    assert sent[0] == expected_command


def test_ec_cancel_jobs_empty_list_sends_no_command(
        ec_platform: EcPlatform,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Send no command when the list of ecaccess job IDs is empty."""
    sent: list[str] = []
    monkeypatch.setattr(ec_platform, "send_command", lambda cmd, **_: sent.append(cmd))

    ec_platform.cancel_jobs([])

    assert sent == []


def test_ec_cancel_jobs_single_id(
        ec_platform: EcPlatform,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Send a single ecaccess-job-delete command for one job ID."""
    sent: list[str] = []
    monkeypatch.setattr(ec_platform, "send_command", lambda cmd, **_: sent.append(cmd))

    ec_platform.cancel_jobs(["9001"])

    assert sent == ["ecaccess-job-delete 9001"]


@pytest.mark.parametrize("job_ids", [
    ["9001", "9002", "9003"],
    ["1", "2"],
])
def test_ec_cancel_jobs_multiple_ids_uses_semicolon_join(
        ec_platform: EcPlatform,
        monkeypatch: pytest.MonkeyPatch,
        job_ids: list[str],
) -> None:
    """Send one semicolon-joined ecaccess-job-delete command per job ID."""
    sent: list[str] = []
    monkeypatch.setattr(ec_platform, "send_command", lambda cmd, **_: sent.append(cmd))

    ec_platform.cancel_jobs(job_ids)

    assert len(sent) == 1
    for job_id in job_ids:
        assert f"ecaccess-job-delete {job_id}" in sent[0]
    assert " ; " in sent[0]


def test_local_cancel_jobs_empty_list_sends_no_command(
        local_platform: LocalPlatform,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Send no command when the list of local PIDs is empty."""
    sent: list[str] = []
    monkeypatch.setattr(local_platform, "send_command", lambda cmd, **_: sent.append(cmd))

    local_platform.cancel_jobs([])

    assert sent == []


def test_local_cancel_jobs_single_pid(
        local_platform: LocalPlatform,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Send a single kill -SIGINT command for one local PID."""
    sent: list[str] = []
    monkeypatch.setattr(local_platform, "send_command", lambda cmd, **_: sent.append(cmd))

    local_platform.cancel_jobs(["1234"])

    assert sent == ["kill -SIGINT 1234"]


@pytest.mark.parametrize("job_ids,expected_command", [
    (["100", "200", "300"], "kill -SIGINT 100 200 300"),
    (["10", "20"], "kill -SIGINT 10 20"),
])
def test_local_cancel_jobs_multiple_pids_uses_space_join(
        local_platform: LocalPlatform,
        monkeypatch: pytest.MonkeyPatch,
        job_ids: list[str],
        expected_command: str,
) -> None:
    """Send one space-joined kill command for multiple local PIDs."""
    sent: list[str] = []
    monkeypatch.setattr(local_platform, "send_command", lambda cmd, **_: sent.append(cmd))

    local_platform.cancel_jobs(job_ids)

    assert len(sent) == 1
    assert sent[0] == expected_command


def test_ps_cancel_jobs_empty_list_sends_no_command(
        ps_platform: PsPlatform,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Send no command when the list of remote PIDs is empty."""
    sent: list[str] = []
    monkeypatch.setattr(ps_platform, "send_command", lambda cmd, **_: sent.append(cmd))

    ps_platform.cancel_jobs([])

    assert sent == []


def test_ps_cancel_jobs_single_pid(
        ps_platform: PsPlatform,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Send a single kill -SIGINT command for one remote PID."""
    sent: list[str] = []
    monkeypatch.setattr(ps_platform, "send_command", lambda cmd, **_: sent.append(cmd))

    ps_platform.cancel_jobs(["5678"])

    assert sent == ["kill -SIGINT 5678"]


@pytest.mark.parametrize("job_ids,expected_command", [
    (["10", "20", "30"], "kill -SIGINT 10 20 30"),
    (["11", "22"], "kill -SIGINT 11 22"),
])
def test_ps_cancel_jobs_multiple_pids_uses_space_join(
        ps_platform: PsPlatform,
        monkeypatch: pytest.MonkeyPatch,
        job_ids: list[str],
        expected_command: str,
) -> None:
    """Send one space-joined kill command for multiple remote PIDs."""
    sent: list[str] = []
    monkeypatch.setattr(ps_platform, "send_command", lambda cmd, **_: sent.append(cmd))

    ps_platform.cancel_jobs(job_ids)

    assert len(sent) == 1
    assert sent[0] == expected_command
