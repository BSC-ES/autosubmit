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

import time
from dataclasses import dataclass

import pytest

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_packager import JobPackager
from autosubmit.job.job_packages import JobPackageVertical


@dataclass
class JobSpec:
    section: str
    status: Status
    parents: tuple = ()


@pytest.fixture
def setup(autosubmit_config, tmpdir, mocker):
    job1 = Job("SECTION1", 1, Status.READY, 0)
    job2 = Job("SECTION1", 1, Status.READY, 0)
    job3 = Job("SECTION1", 1, Status.READY, 0)
    wrapper_jobs = [job1, job2, job3]
    packages = [mocker.MagicMock(spec=JobPackageVertical)]
    packages[0].jobs = wrapper_jobs
    yield packages, wrapper_jobs


def test_propagate_inner_jobs_ready_date(setup):
    packages, wrapper_jobs = setup
    current_time = time.time()
    wrapper_jobs[0].ready_date = current_time
    JobPackager._propagate_inner_jobs_ready_date(packages)
    for job in wrapper_jobs:
        assert job.ready_date == current_time


@pytest.fixture
def packager(mocker):
    """Create a JobPackager with mocked dependencies for testing is_deadlock."""
    as_conf = mocker.MagicMock()
    as_conf.experiment_data = {
        "WRAPPERS": {
            "WRAPPER_A": {"JOBS_IN_WRAPPER": ["SECTION_A"]},
            "WRAPPER_B": {"JOBS_IN_WRAPPER": ["SECTION_B"]},
        }
    }
    as_conf.get_wrapper_type.return_value = "vertical"
    as_conf.get_wrapper_policy.return_value = "strict"
    as_conf.get_wrapper_method.return_value = "asthread"
    as_conf.get_wrapper_jobs.side_effect = [["SECTION_A"], ["SECTION_B"]]
    as_conf.get_extensible_wallclock.return_value = 0

    job_list = mocker.MagicMock()
    job_list._job_list = []
    job_list.get_in_queue.return_value = []
    job_list.get_ready.return_value = []
    job_list.get_prepared.return_value = []

    platform = mocker.MagicMock()
    platform.name = "test_platform"
    platform.max_wallclock = "24:00"

    mocker.patch.object(JobPackager, "calculate_job_limits", return_value=None)
    packager = JobPackager(as_conf, platform, job_list)

    return packager


def _build_jobs(config: list[JobSpec]) -> list[Job]:
    """Build a list of Job objects from a parametrized config."""
    jobs = []
    for spec in config:
        job = Job(f"J{len(jobs)}", len(jobs), spec.status, 0)
        job.section = spec.section
        if spec.parents:
            job.parents = {jobs[i] for i in spec.parents}
        jobs.append(job)
    return jobs


@pytest.mark.parametrize("jobs_config, expected", [
    ([JobSpec("SECTION_A", Status.READY)], False),
    ([JobSpec("EXTRA", Status.READY)], True),
    ([JobSpec("EXTRA", Status.RUNNING)], True),
    ([JobSpec("EXTRA", Status.QUEUING)], True),
    ([JobSpec("EXTRA", Status.SUBMITTED)], True),
    ([JobSpec("EXTRA", Status.READY), JobSpec("EXTRA", Status.WAITING, parents=(0,))], True),
    ([JobSpec("SECTION_A", Status.READY), JobSpec("EXTRA", Status.WAITING, parents=(0,))], False),
    ([JobSpec("SECTION_A", Status.READY), JobSpec("EXTRA", Status.COMPLETED),
      JobSpec("EXTRA", Status.WAITING, parents=(0, 1))], False),
    ([JobSpec("EXTRA", Status.COMPLETED)], False),
    ([JobSpec("EXTRA", Status.WAITING)], False),
    ([JobSpec("EXTRA", Status.COMPLETED), JobSpec("SECTION_A", Status.READY)], False),
], ids=[
    "all jobs wrappable",
    "simple READY",
    "simple RUNNING",
    "simple QUEUING",
    "simple SUBMITTED",
    "simple WAITING + simple parent READY",
    "simple WAITING + wrappable parent only",
    "simple WAITING + wrappable + simple COMPLETED",
    "all simple COMPLETED",
    "simple WAITING no parents",
    "simple COMPLETED + wrappable READY",
])
def test_has_blocking_non_wrapped_jobs(packager, jobs_config, expected):
    packager._jobs_list._job_list = _build_jobs(jobs_config)
    assert packager._has_blocking_non_wrapped_jobs() is expected


@pytest.mark.parametrize("any_simple, queue_len, jobs_config, not_wrappable, built, expected", [
    (True, 0, [], [1], [1], False),
    (False, 1, [], [1], [1], False),
    (False, 0, [JobSpec("EXTRA", Status.READY)], [1], [1], False),
    (False, 0, [], [], [1], False),
    (False, 0, [], [1], [1], True),
    (False, 0, [JobSpec("SECTION_A", Status.READY), JobSpec("EXTRA", Status.WAITING, parents=(0,))], [1], [1], True),
], ids=[
    "any_simple_packages=True",
    "jobs in queue",
    "simple READY blocks",
    "not all unwrappable",
    "all deadlock conditions met",
    "genuine deadlock: SIMPLE WAITING blocked by wrappable parent",
])
def test_is_deadlock(packager, mocker, any_simple, queue_len, jobs_config, not_wrappable, built, expected):
    if queue_len > 0:
        packager._jobs_list.get_in_queue.return_value = [mocker.MagicMock()]
    packager._jobs_list._job_list = _build_jobs(jobs_config)
    assert packager.is_deadlock(
        any_simple_packages=any_simple,
        not_wrappeable_package_info=not_wrappable,
        built_packages_tmp=built,
    ) is expected


class _FakeWrappedPackage:
    """Minimal stand-in for a built wrapper package carrying the static limit flag."""

    def __init__(self, jobs, static_limit_reached=False, static_limit_reason=None, policy="flexible"):
        self.jobs = jobs
        self.jobs_lists = [jobs]
        self.wrapper_policy = policy
        self.wrapper_type = "vertical"
        self.static_limit_reached = static_limit_reached
        self.static_limit_reason = static_limit_reason


_WRAPPER_LIMITS = {"real_min": 10, "min_v": 1, "min_h": 1}


@pytest.mark.parametrize("num_jobs, static_limit, policy, failed, remaining_blocked, expected_submitted", [
    (6, True, "flexible", False, False, True),
    (3, False, "flexible", False, False, False),
    (6, True, "strict", False, False, False),
    (6, True, "mixed", False, False, False),
    (6, True, "strict", True, False, False),
    (12, False, "flexible", False, False, True),
    (4, False, "flexible", False, True, True),
], ids=[
    "flexible-below-min-static-limit-submits",
    "flexible-below-min-waits",
    "strict-below-min-static-limit-waits",
    "mixed-below-min-static-limit-waits",
    "strict-below-min-failed-innerjobs-waits",
    "meets-min-submits",
    "flexible-below-min-remaining-blocked-submits",
])
def test_check_packages_respect_wrapper_policy_static_limit(
        packager, mocker, num_jobs, static_limit, policy, failed, remaining_blocked, expected_submitted):
    packager.current_wrapper_section = "WRAPPER_A"
    jobs = [Job(f"job{i}", i, Status.READY, 0) for i in range(num_jobs)]
    for job in jobs:
        job.section = "SECTION_A"
        job.fail_count = 1 if failed else 0
    package = _FakeWrappedPackage(jobs, static_limit, "MAX_WALLCLOCK" if static_limit else None, policy=policy)
    packager._jobs_list.get_jobs_by_section.return_value = []
    mocker.patch.object(JobPackager, "check_real_package_wrapper_limits", return_value=(len(jobs), 1, True))
    mocker.patch.object(JobPackager, "_remaining_blocked_by_package", return_value=remaining_blocked)
    mocker.patch.object(JobPackager, "is_deadlock", return_value=False)
    packages, _ = packager.check_packages_respect_wrapper_policy(
        [package], [], 5, _WRAPPER_LIMITS)
    assert (len(packages) == 1) is expected_submitted


def test_below_min_static_limit_logs_warning(packager, mocker):
    packager.current_wrapper_section = "WRAPPER_A"
    jobs = [Job(f"job{i}", i, Status.READY, 0) for i in range(6)]
    for job in jobs:
        job.section = "SECTION_A"
    package = _FakeWrappedPackage(jobs, True, "MAX_WALLCLOCK")
    packager._jobs_list.get_jobs_by_section.return_value = []
    mocker.patch.object(JobPackager, "check_real_package_wrapper_limits", return_value=(6, 1, True))
    mocker.patch.object(JobPackager, "_remaining_blocked_by_package", return_value=False)
    mocker.patch.object(JobPackager, "is_deadlock", return_value=False)
    warn = mocker.patch("autosubmit.log.log.Log.warning")
    packager.check_packages_respect_wrapper_policy([package], [], 5, _WRAPPER_LIMITS)
    warn.assert_called_once()
    message = warn.call_args[0][0]
    assert "WRAPPER_A" in message
    assert "MAX_WALLCLOCK" in message
    assert "below the configured minimum of 10" in message
