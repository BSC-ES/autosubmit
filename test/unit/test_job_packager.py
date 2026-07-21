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

import time
from dataclasses import dataclass

import pytest
import json

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_packager import JobPackager
from autosubmit.job.job_packages import JobPackageDelegated, JobPackageVertical


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

@pytest.fixture
def delegated_package(slurm_platform, autosubmit_config, mocker):
    """
    Returns a delegated package with a complex dependency graph, including an external uncompleted
    parent and a multi-level dependency. Also returns the packager instance for further testing.
    """
    wrapper_jobs = list()

    for i in range(1, 4):
        job = Job(f"job{i}", 1, Status.READY, 0)
        job.section = "SECTION1"
        wrapper_jobs.append(job)
    
    for i in range(4, 7):
        job = Job(f"job{i}", 1, Status.READY, 0)
        job.section = "SECTION2"
        job.parents = [wrapper_jobs[1], wrapper_jobs[2]]
        wrapper_jobs.append(job)

    wrapper_jobs[1].children = wrapper_jobs[3:6]
    wrapper_jobs[2].children = wrapper_jobs[3:6]

    # Attempt to add an external uncompleted parent to the package. Later on, tests will check it
    external_uncompleted_parent = Job("job_external", 1, Status.READY, 0)
    external_uncompleted_parent.section = "SECTION3"
    child_external = Job("child_external", 1, Status.READY, 0)
    child_external.section = "SECTION2"

    child_external.parents = [wrapper_jobs[4], wrapper_jobs[5], external_uncompleted_parent]
    wrapper_jobs[1].children.append(child_external)
    wrapper_jobs[4].children = [child_external]
    wrapper_jobs[5].children = [child_external]
    external_uncompleted_parent.children = [child_external]
    external_uncompleted_parent.parents = [wrapper_jobs[0]]
    wrapper_jobs[0].children = [external_uncompleted_parent]

    # A complex case with a multi-level dependency, which also does not fit in the package
    multi_level_job = Job("multi_level_job", 1, Status.READY, 0)
    multi_level_job.section = "SECTION2"
    multi_level_job.parents = [wrapper_jobs[1], wrapper_jobs[5]]
    wrapper_jobs[1].children.append(multi_level_job)
    wrapper_jobs[5].children.append(multi_level_job)

    # A last job depending on job6
    last_job = Job("last_job", 1, Status.READY, 0)
    last_job.section = "SECTION2"
    last_job.parents = [wrapper_jobs[5]]
    wrapper_jobs[5].children.append(last_job)

    custom_config = {
        "JOBS": {
            "SECTION1": {
                "WALLCLOCK": "00:01",
                "PROCESSORS": 1,
            },
            "SECTION2": {
                "WALLCLOCK": "00:01",
                "PROCESSORS": 1,
            },
            "SECTION3": {
                "WALLCLOCK": "00:01",
                "PROCESSORS": 1,
            }
        },
        "WRAPPERS": {
            "WRAPPER": {
                "POLICY": "flexible",
                "TYPE": "delegated",
                "MIN_WRAPPED": 1,
                "MAX_WRAPPED": 6,
                "JOBS_IN_WRAPPER": "SECTION1 SECTION2",
            },
            "CUSTOM_ENV_SETUP": "echo 'Hello World'",
            "METHOD": "flux"
        }
    }

    as_conf = autosubmit_config(expid="a000", experiment_data=custom_config)
    section_list = ['SECTION1', 'SECTION2']
    wrapper_limits = {'min': 1, 'max': 6, 'min_v': 1, 'max_v': 2, 'min_h': 1, 'max_h': 3, 'max_by_section': {'SECTION1': 3, 'SECTION2': 3}, 'real_min': 1}
    wrapper_info = ['delegated', 'flexible', 'flux', section_list, 0, as_conf]
    slurm_platform.max_processors = 50

    packager = JobPackager(as_conf, slurm_platform, mocker.MagicMock())
    packager.current_wrapper_section = 'WRAPPER'
    package = packager._build_delegated_package(jobs_list=wrapper_jobs[:3], wrapper_limits=wrapper_limits, section_list=section_list, wrapper_info=wrapper_info)
    return packager, package[0]

def test_check_real_package_wrapper_limits_delegated(delegated_package: tuple[JobPackager, JobPackageDelegated]):
    packager, package = delegated_package
    min_v, min_h, balanced = packager.check_real_package_wrapper_limits(package)
    assert min_v == 2
    assert min_h == 3
    assert balanced

def test_build_delegated_package(delegated_package: tuple[JobPackager, JobPackageDelegated]):
    _, package = delegated_package
    assert package.max_height == 2
    assert package.max_width == 3
    assert len(package.jobs) == 6

def test_serialize_delegated_package(delegated_package: tuple[JobPackager, JobPackageDelegated]):
    """Ensure interface consistency"""
    _, package = delegated_package
    serialized: dict = json.loads(package._subworkflow)
    assert serialized.get('directed')
    assert not serialized.get('multigraph')

    tasks: list[dict] = serialized.get('tasks')
    assert len(tasks) == 6

    dependencies: list[dict] = serialized.get('dependencies')
    assert len(dependencies) == 6

    graph = serialized.get('graph')
    assert graph is not None

    wrapper_defaults = graph.get('wrapper_defaults')
    assert wrapper_defaults is not None

    assert wrapper_defaults.get('cwd') is not None

    assert "id" in tasks[0]
    assert "nodes" in tasks[0]
    assert "threads" in tasks[0]
    assert "processors" in tasks[0]
    assert "wallclock" in tasks[0]
    assert "exclusive" in tasks[0]
    assert "weight" in dependencies[0]
    assert "from" in dependencies[0]
    assert "to" in dependencies[0]
