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

"""Unit tests for the ``cancel_jobs`` method across all platform implementations."""

import pytest


@pytest.mark.parametrize("platform_fixture", [
    "slurm_platform",
    "ec_platform",
    "local_platform",
    "ps_platform",
])
def test_cancel_jobs_empty_list_sends_no_command(
        platform_fixture: str,
        monkeypatch: pytest.MonkeyPatch,
        request: pytest.FixtureRequest,
) -> None:
    """Send no command when the job ID list is empty."""
    platform = request.getfixturevalue(platform_fixture)
    sent: list[str] = []
    monkeypatch.setattr(platform, "send_command", lambda cmd, **_: sent.append(cmd))

    platform.cancel_jobs([])

    assert sent == []


@pytest.mark.parametrize("platform_fixture,job_id,expected_command", [
    ("slurm_platform", "42", "scancel 42"),
    ("ec_platform", "9001", "ecaccess-job-delete 9001"),
    ("local_platform", "1234", "kill -SIGINT 1234"),
    ("ps_platform", "5678", "kill -SIGINT 5678"),
])
def test_cancel_jobs_single_id(
        platform_fixture: str,
        job_id: str,
        expected_command: str,
        monkeypatch: pytest.MonkeyPatch,
        request: pytest.FixtureRequest,
) -> None:
    """Send one cancel command for a single job ID."""
    platform = request.getfixturevalue(platform_fixture)
    sent: list[str] = []
    monkeypatch.setattr(platform, "send_command", lambda cmd, **_: sent.append(cmd))

    platform.cancel_jobs([job_id])

    assert sent == [expected_command]


@pytest.mark.parametrize("platform_fixture,job_ids,expected_command", [
    ("slurm_platform", ["1", "2", "3"], "scancel 1,2,3"),
    ("slurm_platform", ["100", "200"], "scancel 100,200"),
    ("local_platform", ["100", "200", "300"], "kill -SIGINT 100 200 300"),
    ("local_platform", ["10", "20"], "kill -SIGINT 10 20"),
    ("ps_platform", ["10", "20", "30"], "kill -SIGINT 10 20 30"),
    ("ps_platform", ["11", "22"], "kill -SIGINT 11 22"),
])
def test_cancel_jobs_multiple_ids(
        platform_fixture: str,
        job_ids: list[str],
        expected_command: str,
        monkeypatch: pytest.MonkeyPatch,
        request: pytest.FixtureRequest,
) -> None:
    """Send one correctly-joined cancel command for multiple job IDs."""
    platform = request.getfixturevalue(platform_fixture)
    sent: list[str] = []
    monkeypatch.setattr(platform, "send_command", lambda cmd, **_: sent.append(cmd))

    platform.cancel_jobs(job_ids)

    assert len(sent) == 1
    assert sent[0] == expected_command


@pytest.mark.parametrize("job_ids", [
    ["9001", "9002", "9003"],
    ["1", "2"],
])
def test_ec_cancel_jobs_multiple_ids_uses_semicolon_join(
        ec_platform,
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
