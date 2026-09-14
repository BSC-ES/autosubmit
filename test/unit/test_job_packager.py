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
from autosubmit.job.job_packager import JobPackager, JobPackagerVertical
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
    job_list.job_list = []
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
    packager._jobs_list.job_list = _build_jobs(jobs_config)
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
    packager._jobs_list.job_list = _build_jobs(jobs_config)
    assert packager.is_deadlock(
        any_simple_packages=any_simple,
        not_wrappeable_package_info=not_wrappable,
        built_packages_tmp=built,
    ) is expected


def _vertical_job(name, status, parents=(), wallclock="00:10", chunk=None):
    """Build a Job for vertical packaging tests, wiring its static dependencies."""
    job = Job(name, 0, status, 0)
    job.section = "SIM"
    job.wallclock = wallclock
    job.chunk = chunk
    for parent in parents:
        job.add_parent(parent)
    return job


def _vertical_wrapper_limits():
    return {"max": 9999, "max_v": 9999, "max_by_section": {"SIM": 9999}}


def _build_vertical_chain(seed, candidates, mocker):
    """Seed a vertical package over the given (date, member) candidates."""
    dict_jobs = {"d1": {"m1": [seed] + candidates}}
    wrapper_info = [mocker.MagicMock()]
    packager = JobPackagerVertical(dict_jobs, seed, [seed], "00:00", 100, _vertical_wrapper_limits(), "",
                                   wrapper_info)
    mocker.patch("autosubmit.job.job.Job.update_parameters", return_value={})
    return packager.build_vertical_package(seed, wrapper_info)


def _make_parallel_branches(n_branches, chain_len):
    """Build ``n_branches`` independent (date, member) lineages of ``chain_len`` jobs."""
    completed = _vertical_job("PREV_DONE", Status.COMPLETED)
    branches = []
    seeds = []
    candidates = []
    for branch in range(n_branches):
        root = _vertical_job(f"SIM_{branch}_0", Status.READY, parents=[completed], chunk=branch + 1)
        branches.append([root])
        seeds.append(root)
        candidates.append(root)
        previous = root
        for index in range(1, chain_len):
            job = _vertical_job(f"SIM_{branch}_{index}", Status.WAITING, parents=[previous], chunk=branch + 1)
            branches[-1].append(job)
            candidates.append(job)
            previous = job
    return branches, seeds, candidates


def test_vertical_chain_only_absorbs_dependent_descendants(mocker):
    """A vertical wrapper must be a single dependent lineage: an independent
    READY job whose parents are only COMPLETED never joins the chain."""
    completed = _vertical_job("PREV_DONE", Status.COMPLETED)
    seed = _vertical_job("SIM_1", Status.READY, parents=[completed])
    child = _vertical_job("SIM_2", Status.WAITING, parents=[seed])
    independent = _vertical_job("SIM_INDEP", Status.READY, parents=[completed])

    result = _build_vertical_chain(seed, [child, independent], mocker)

    assert result == [seed, child]
    assert independent not in result
    assert independent.packed_during_building is False


def test_vertical_chain_wraps_serialized_lineage(mocker):
    """A serialized chain of dependent jobs (static DB dependencies) is still
    wrapped together in a single vertical package."""
    completed = _vertical_job("PREV_DONE", Status.COMPLETED)
    seed = _vertical_job("SIM_1", Status.READY, parents=[completed])
    child2 = _vertical_job("SIM_2", Status.WAITING, parents=[seed])
    child3 = _vertical_job("SIM_3", Status.WAITING, parents=[child2])
    child4 = _vertical_job("SIM_4", Status.WAITING, parents=[child3])

    result = _build_vertical_chain(seed, [child2, child3, child4], mocker)

    assert result == [seed, child2, child3, child4]


@pytest.mark.parametrize("candidate_chunk, candidate_parent, expected_in_chain", [
    (2, "seed", True),        # different chunk, but a parent is in the chain: joins
    (2, "completed", False),  # different chunk, parents only COMPLETED: excluded
], ids=["cross-chunk-dependent", "cross-chunk-independent"])
def test_vertical_chain_chunk_is_not_a_boundary(mocker, candidate_chunk, candidate_parent, expected_in_chain):
    """Chunk is not an isolation boundary for vertical wrappers: cross-chunk
    jobs join only when the static dependency links them to the chain."""
    completed = _vertical_job("PREV_DONE", Status.COMPLETED)
    seed = _vertical_job("SIM_1", Status.READY, parents=[completed], chunk=1)
    parents = [seed] if candidate_parent == "seed" else [completed]
    candidate = _vertical_job("SIM_X", Status.WAITING, parents=parents, chunk=candidate_chunk)

    result = _build_vertical_chain(seed, [candidate], mocker)

    assert result == ([seed, candidate] if expected_in_chain else [seed])


@pytest.mark.parametrize("n_siblings", [2, 3])
def test_vertical_chain_absorbs_sibling_descendants(mocker, n_siblings):
    """Jobs that are descendants of the seed (even parallel siblings sharing an
    in-chain parent) belong to the same lineage and join the wrapper."""
    completed = _vertical_job("PREV_DONE", Status.COMPLETED)
    seed = _vertical_job("SIM_1", Status.READY, parents=[completed])
    siblings = [_vertical_job(f"SIM_SIBLING_{index}", Status.WAITING, parents=[seed]) for index in range(n_siblings)]

    result = _build_vertical_chain(seed, siblings, mocker)

    assert result == [seed, *siblings]


@pytest.mark.parametrize("n_branches, chain_len", [(1, 1), (2, 3), (3, 4)])
def test_build_vertical_packages_one_per_lineage(packager, mocker, n_branches, chain_len):
    """``_build_vertical_packages`` produces one package per independent lineage,
    never merging parallel branches that share a (date, member) bucket."""
    branches, seeds, candidates = _make_parallel_branches(n_branches, chain_len)
    packager.current_wrapper_section = "WRAPPER_A"
    packager._jobs_list.get_ordered_jobs_by_date_member.return_value = {"d1": {"m1": candidates}}
    packager._platform.max_wallclock = "48:00"
    mocker.patch("autosubmit.job.job.Job.update_parameters", return_value={})
    mocker.patch("autosubmit.job.job_packager.JobPackageVertical", side_effect=lambda jobs, **kwargs: list(jobs))

    packages = packager._build_vertical_packages(
        seeds, _vertical_wrapper_limits(), wrapper_info=[mocker.MagicMock()])

    assert len(packages) == n_branches
    for package, branch in zip(packages, branches):
        assert package == branch
