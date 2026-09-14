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


def _build_vertical_chain(seed, candidates):
    """Seed a vertical package over the given (date, member) candidates."""
    dict_jobs = {"d1": {"m1": [seed] + candidates}}
    packager = JobPackagerVertical(dict_jobs, [seed], _vertical_wrapper_limits(), "")
    return packager.build_vertical_package(seed, [])


def _make_parallel_branches(n_branches, chain_len):
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


def test_vertical_chain_only_absorbs_dependent_descendants():
    """A vertical wrapper must be a single dependent lineage: an independent
    READY job whose parents are only COMPLETED never joins the chain."""
    completed = _vertical_job("PREV_DONE", Status.COMPLETED)
    seed = _vertical_job("SIM_1", Status.READY, parents=[completed])
    child = _vertical_job("SIM_2", Status.WAITING, parents=[seed])
    independent = _vertical_job("SIM_INDEP", Status.READY, parents=[completed])

    result = _build_vertical_chain(seed, [child, independent])

    assert result == [seed, child]
    assert independent not in result
    assert independent.packed_during_building is False


def test_vertical_chain_wraps_serialized_lineage():
    """A serialized chain of dependent jobs (static DB dependencies) is still
    wrapped together in a single vertical package."""
    completed = _vertical_job("PREV_DONE", Status.COMPLETED)
    seed = _vertical_job("SIM_1", Status.READY, parents=[completed])
    child2 = _vertical_job("SIM_2", Status.WAITING, parents=[seed])
    child3 = _vertical_job("SIM_3", Status.WAITING, parents=[child2])
    child4 = _vertical_job("SIM_4", Status.WAITING, parents=[child3])

    result = _build_vertical_chain(seed, [child2, child3, child4])

    assert result == [seed, child2, child3, child4]


def test_vertical_chain_handles_backward_dependency():
    """A job may depend on a job placed later in the candidate order. The Kahn
    frontier does not rely on positional scanning, so it is still wrapped."""
    completed = _vertical_job("PREV_DONE", Status.COMPLETED)
    seed = _vertical_job("SIM_1", Status.READY, parents=[completed], chunk=1)
    late = _vertical_job("SIM_9", Status.WAITING, parents=[seed], chunk=9)
    early = _vertical_job("SIM_2", Status.WAITING, parents=[late], chunk=2)

    # `early` is placed before `late` in the candidate list on purpose.
    result = _build_vertical_chain(seed, [early, late])

    assert result == [seed, late, early]


def test_vertical_chain_spans_dates_members_and_splits():
    """The chain may cross date/member/split boundaries when dependencies link the
    jobs, because the candidate set is not restricted to a single bucket."""
    completed = _vertical_job("PREV_DONE", Status.COMPLETED)
    first = _vertical_job("SIM_1", Status.READY, parents=[completed])
    first.date = "d1"
    first.member = "m1"
    first.split = 1
    second = _vertical_job("SIM_2", Status.WAITING, parents=[first])
    second.date = "d2"
    second.member = "m2"
    second.split = 2

    dict_jobs = {"d1": {"m1": [first]}, "d2": {"m2": [second]}}
    # The candidate pool concatenates every bucket in order and can exclude the seed.
    assert JobPackagerVertical.flatten_jobs(dict_jobs) == [first, second]
    assert JobPackagerVertical.flatten_jobs(dict_jobs, [first]) == [second]
    assert JobPackagerVertical.flatten_jobs({}) == []

    packager = JobPackagerVertical(dict_jobs, [first], _vertical_wrapper_limits(), "")

    result = packager.build_vertical_package(first, [])

    assert result == [first, second]


@pytest.mark.parametrize("candidate_chunk, candidate_parent, expected_in_chain", [
    (2, "seed", True),
    (2, "completed", False),
], ids=["cross-chunk-dependent", "cross-chunk-independent"])
def test_vertical_chain_chunk_is_not_a_boundary(candidate_chunk, candidate_parent, expected_in_chain):
    """Chunk is not an isolation boundary for vertical wrappers: cross-chunk
    jobs join only when the static dependency links them to the chain."""
    completed = _vertical_job("PREV_DONE", Status.COMPLETED)
    seed = _vertical_job("SIM_1", Status.READY, parents=[completed], chunk=1)
    parents = [seed] if candidate_parent == "seed" else [completed]
    candidate = _vertical_job("SIM_X", Status.WAITING, parents=parents, chunk=candidate_chunk)

    result = _build_vertical_chain(seed, [candidate])

    assert result == ([seed, candidate] if expected_in_chain else [seed])


@pytest.mark.parametrize("n_siblings", [2, 3], ids=["two-siblings", "three-siblings"])
def test_vertical_chain_absorbs_sibling_descendants(n_siblings):
    """Descendants of the seed (including parallel siblings sharing an in-chain
    parent) belong to the same lineage and are wrapped sequentially in the same
    wrapper (one job runs at a time)."""
    completed = _vertical_job("PREV_DONE", Status.COMPLETED)
    seed = _vertical_job("SIM_1", Status.READY, parents=[completed])
    siblings = [_vertical_job(f"SIM_SIBLING_{index}", Status.WAITING, parents=[seed]) for index in range(n_siblings)]

    result = _build_vertical_chain(seed, siblings)

    assert result == [seed, *siblings]


def test_vertical_chain_joins_in_pool_parents():
    """A job with two in-wrapper parents joins once both parents are in the chain."""
    completed = _vertical_job("PREV_DONE", Status.COMPLETED)
    seed = _vertical_job("SIM_1", Status.READY, parents=[completed])
    parent = _vertical_job("SIM_2", Status.WAITING, parents=[seed])
    join = _vertical_job("SIM_3", Status.WAITING, parents=[seed, parent])

    result = _build_vertical_chain(seed, [parent, join])

    assert result == [seed, parent, join]


def test_vertical_chain_skips_blocked_child_from_added_parent():
    """A child blocked by an out-of-pool parent is not offered again by another parent."""
    completed = _vertical_job("PREV_DONE", Status.COMPLETED)
    external = _vertical_job("EXTERNAL", Status.WAITING)
    seed = _vertical_job("SIM_1", Status.READY, parents=[completed])
    parent = _vertical_job("SIM_2", Status.WAITING, parents=[seed])
    blocked_child = _vertical_job("SIM_3", Status.WAITING, parents=[seed, external, parent])

    result = _build_vertical_chain(seed, [parent, blocked_child])

    assert result == [seed, parent]


def test_vertical_chain_skips_already_packed_child():
    """A descendant already packed by another wrapper is not added again."""
    completed = _vertical_job("PREV_DONE", Status.COMPLETED)
    seed = _vertical_job("SIM_1", Status.READY, parents=[completed])
    packed = _vertical_job("SIM_2", Status.WAITING, parents=[seed])
    packed.packed_during_building = True

    result = _build_vertical_chain(seed, [packed])

    assert result == [seed]


def test_vertical_chain_skips_non_wrappable_child():
    """A child that is already SUBMITTED is not a wrappable candidate."""
    completed = _vertical_job("PREV_DONE", Status.COMPLETED)
    seed = _vertical_job("SIM_1", Status.READY, parents=[completed])
    submitted = _vertical_job("SIM_2", Status.SUBMITTED, parents=[seed])

    result = _build_vertical_chain(seed, [submitted])

    assert result == [seed]


def test_vertical_chain_initialises_loaded_child(mocker):
    """A child loaded lazily from the database has its runtime attributes unset.

    The packager must resolve them with ``update_parameters`` before reading the
    wallclock used by the limits, otherwise jobs without a persisted wallclock
    crash the wrapper construction.
    """
    completed = _vertical_job("PREV_DONE", Status.COMPLETED)
    seed = _vertical_job("SIM_1", Status.READY, parents=[completed])
    loaded_child = _vertical_job("SIM_2", Status.WAITING, parents=[seed], wallclock=None)
    assert loaded_child.wallclock is None
    assert loaded_child.processors is None

    def resolve_runtime_parameters(self, as_conf, set_attributes=False, reset_logs=False):
        if self.wallclock is None:
            self.wallclock = "00:20"
        if self.processors is None:
            self.processors = "4"
        self.updated = True
        return {}

    mocker.patch.object(Job, "update_parameters", autospec=True, side_effect=resolve_runtime_parameters)

    dict_jobs = {"d1": {"m1": [seed, loaded_child]}}
    packager = JobPackagerVertical(dict_jobs, [seed], _vertical_wrapper_limits(), "")

    result = packager.build_vertical_package(seed, [object()])

    assert result == [seed, loaded_child]
    assert loaded_child.wallclock == "00:20"
    assert loaded_child.processors == "4"
    assert packager.total_wallclock == "00:30"


def test_vertical_chain_counts_seed_in_section_limit():
    """The per-section MAX_WRAPPED includes the seed job already in the wrapper."""
    completed = _vertical_job("PREV_DONE", Status.COMPLETED)
    seed = _vertical_job("SIM_1", Status.READY, parents=[completed])
    child2 = _vertical_job("SIM_2", Status.WAITING, parents=[seed])
    child3 = _vertical_job("SIM_3", Status.WAITING, parents=[child2])
    limits = _vertical_wrapper_limits()
    limits["max_by_section"] = {"SIM": 2}
    dict_jobs = {"d1": {"m1": [seed, child2, child3]}}
    packager = JobPackagerVertical(dict_jobs, [seed], limits, "")

    result = packager.build_vertical_package(seed, [])

    assert result == [seed, child2]


@pytest.mark.parametrize("n_branches, chain_len", [(1, 1), (2, 3), (3, 4)], ids=["1x1", "2x3", "3x4"])
def test_build_vertical_packages_one_per_lineage(packager, mocker, n_branches, chain_len):
    """``_build_vertical_packages`` produces one package per independent lineage,
    never merging parallel branches that share a (date, member) bucket."""
    branches, seeds, candidates = _make_parallel_branches(n_branches, chain_len)
    packager.current_wrapper_section = "WRAPPER_A"
    packager._jobs_list.get_ordered_jobs_by_date_member.return_value = {"d1": {"m1": candidates}}
    packager._platform.max_wallclock = "48:00"
    mocker.patch("autosubmit.job.job_packager.JobPackageVertical", side_effect=lambda jobs, **kwargs: list(jobs))

    packages = packager._build_vertical_packages(
        seeds, _vertical_wrapper_limits(), wrapper_info=[])

    assert len(packages) == n_branches
    for package, branch in zip(packages, branches):
        assert package == branch


def test_build_vertical_packages_without_wrapper_info(packager, mocker):
    """The vertical builder still works when called without wrapper parameters."""
    seed = _vertical_job("SIM_1", Status.READY)
    packager.current_wrapper_section = "WRAPPER_A"
    packager._jobs_list.get_ordered_jobs_by_date_member.return_value = {"d1": {"m1": [seed]}}
    packager._platform.max_wallclock = "48:00"
    mocker.patch("autosubmit.job.job_packager.JobPackageVertical", side_effect=lambda jobs, **kwargs: list(jobs))

    packages = packager._build_vertical_packages(
        [seed], _vertical_wrapper_limits(), wrapper_info=None)

    assert packages == [[seed]]


def test_build_vertical_horizontal_package_uses_pool(packager, mocker):
    """The vertical-horizontal hybrid expands each horizontal job from the wrapper pool."""
    seed = _vertical_job("SIM_1", Status.READY)
    packager.current_wrapper_section = "WRAPPER_A"
    packager._jobs_list.get_ordered_jobs_by_date_member.return_value = {"d1": {"m1": [seed]}}
    horizontal_packager = mocker.MagicMock()
    horizontal_packager.build_horizontal_package.return_value = [seed]
    horizontal_packager.total_processors = 1
    horizontal_packager.wrapper_limits = {"max_by_section": {"SIM": 5}, "max": 5}
    mocker.patch(
        "autosubmit.job.job_packager.JobPackageVerticalHorizontal",
        side_effect=lambda jobs, *args, **kwargs: jobs,
    )

    result = packager._build_vertical_horizontal_package(horizontal_packager, {}, [])

    assert result == [[seed]]


def test_build_horizontal_packages_without_wrapper_info(packager):
    """The horizontal builder accepts the default wrapper_info with no jobs."""
    packager.current_wrapper_section = "WRAPPER_A"
    assert packager._build_horizontal_packages([], _vertical_wrapper_limits(), wrapper_info=None) == []


def test_build_hybrid_package_without_wrapper_info(packager, mocker):
    """The hybrid builder accepts the default wrapper_info and dispatches to its builder."""
    packager.current_wrapper_section = "WRAPPER_A"
    packager.wrapper_type["WRAPPER_A"] = "vertical-horizontal"
    mocker.patch("autosubmit.job.job_packager.JobPackagerHorizontal")
    expected = object()
    mocker.patch.object(packager, "_build_vertical_horizontal_package", return_value=expected)

    result = packager._build_hybrid_package([], _vertical_wrapper_limits(), ["SIM"], wrapper_info=None)

    assert result == [expected]
