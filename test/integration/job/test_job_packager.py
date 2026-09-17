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

from typing import Protocol

import pytest

from autosubmit.config.yamlparser import YAMLParserFactory
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_list import JobList
from autosubmit.job.job_packager import JobPackager


class CreatePackagerFixture(Protocol):

    def __call__(
            self,
            experiment_data: dict | None = None,
            total_jobs: int | None = 20
    ) -> JobPackager:
        ...


@pytest.fixture
def create_packager(autosubmit_exp, local) -> CreatePackagerFixture:
    def _job_packager(experiment_data: dict | None, total_jobs: int | None = 20) -> JobPackager:
        local.total_jobs = total_jobs

        exp = autosubmit_exp(experiment_data=experiment_data)
        as_conf = exp.as_conf
        parameters = as_conf.load_parameters()

        job_list = JobList(exp.expid, exp.as_conf, YAMLParserFactory())

        job_list.generate(
            as_conf,
            as_conf.get_date_list(),
            as_conf.get_member_list(),
            as_conf.get_num_chunks(),
            as_conf.get_chunk_ini(),
            parameters,
            '',
            as_conf.get_retrials(),
            as_conf.get_default_job_type(),
            {},
            full_load=True)
        for job in job_list.get_job_list():
            job.update_parameters(as_conf, set_attributes=True)

        return JobPackager(exp.as_conf, local, job_list)

    return _job_packager


def test_check_if_packages_are_ready_to_build_simple_job_is_ready(create_packager: CreatePackagerFixture):
    """Test that a simple package is ready to be built."""
    job_packager = create_packager(
        experiment_data={
            'JOBS': {
                'A': {
                    'running': 'once',
                    'platform': 'local',
                    'script': '"sleep 0"'
                }
            }
        }
    )
    jobs, flag = job_packager.check_if_packages_are_ready_to_build()

    assert jobs
    assert flag


def test_check_if_packages_are_ready_to_build_simple_job_not_ready_if_hold(create_packager: CreatePackagerFixture):
    """Test that a simple package is NOT ready when ``hold`` is ``True``."""
    job_packager = create_packager(
        experiment_data={
            'JOBS': {
                'A': {
                    'running': 'once',
                    'platform': 'local',
                    'script': '"sleep 0"'
                }
            }
        }
    )
    # Hold will make the packager to get prepared jobs, instead of ready.
    # And as ``A`` is ``READY``, it must not return it.
    job_packager.hold = True
    jobs, flag = job_packager.check_if_packages_are_ready_to_build()

    assert not jobs
    assert not flag


def test_check_if_packages_are_ready_hold(create_packager: CreatePackagerFixture):
    """Test that the packager handles prepared jobs when ``hold`` is ``True``."""
    job_packager = create_packager(
        experiment_data={
            'JOBS': {
                'A': {
                    'running': 'once',
                    'platform': 'local',
                    'script': '"sleep 0"',
                    'max_waiting_jobs': '1984'
                }
            }
        }
    )

    for job in job_packager._jobs_list.get_job_list():
        job.status = Status.PREPARED

    job_packager.hold = True
    jobs, flag = job_packager.check_if_packages_are_ready_to_build()

    assert jobs
    assert flag


def test_check_if_packages_are_ready_to_build_empty_job_list(create_packager: CreatePackagerFixture):
    """Test that an empty job list results in no jobs and a flag indicating it is not ready."""
    job_packager = create_packager(
        experiment_data={
            'JOBS': {}
        }
    )
    jobs, flag = job_packager.check_if_packages_are_ready_to_build()

    assert not jobs
    assert not flag


def test_check_if_packages_are_ready_to_build_max_waiting_time(create_packager: CreatePackagerFixture):
    """Test that a platform with total jobs 0 results in packages not being ready to be built."""
    job_packager = create_packager(
        experiment_data={
            'JOBS': {
                'A': {
                    'running': 'once',
                    'platform': 'local',
                    'script': '"sleep 0"',
                    'max_waiting_jobs': '1984'
                }
            }
        },
        total_jobs=0
    )

    jobs, flag = job_packager.check_if_packages_are_ready_to_build()

    assert jobs
    assert not flag


@pytest.mark.parametrize(
    "total_jobs, max_jobs_to_submit",
    [
        (None, float('inf')),
        (-10, float('inf')),
        (-1, float('inf')),
        (20, 20),
    ]
)
def test_calculate_job_limits_total_jobs_inf(create_packager: CreatePackagerFixture, total_jobs: int, max_jobs_to_submit: float):
    """Test that the job limits are calculated correctly based on total jobs."""
    job_packager = create_packager(
        experiment_data={
            'JOBS': {
                'A': {
                    'running': 'once',
                    'platform': 'local',
                    'script': '"sleep 0"',
                }
            }
        },
        total_jobs=total_jobs
    )
    job_packager.calculate_job_limits(job_packager._platform)
    assert job_packager._max_jobs_to_submit == max_jobs_to_submit


@pytest.mark.parametrize(
    "max_waiting_jobs, expected_max_waiting_jobs",
    [
        (None, float('inf')),
        (-10, float('inf')),
        (-1, float('inf')),
        (20, 20),
    ]
)
def test_calculate_job_limits_max_waiting_jobs(create_packager: CreatePackagerFixture, max_waiting_jobs: int, expected_max_waiting_jobs: float):
    """Test that the job limits are calculated correctly based on max waiting jobs."""
    job_packager = create_packager(
        experiment_data={
            'JOBS': {
                'A': {
                    'running': 'once',
                    'platform': 'local',
                    'script': '"sleep 0"',
                }
            }
        }
    )
    job_packager._platform.max_waiting_jobs = max_waiting_jobs
    job_packager.calculate_job_limits(job_packager._platform)
    assert job_packager._max_wait_jobs_to_submit == expected_max_waiting_jobs


_VERTICAL_WRAPPER_EXPERIMENT = {
    "EXPERIMENT": {
        "DATELIST": "19900101",
        "MEMBERS": "fc0",
        "NUMCHUNKS": "1",
    },
    "PLATFORMS": {
        "TEST_SLURM": {
            "ADD_PROJECT_TO_HOST": "False",
            "HOST": "127.0.0.1",
            "MAX_WALLCLOCK": "00:10",
            "PROJECT": "group",
            "QUEUE": "gp_debug",
            "SCRATCH_DIR": "/tmp/scratch/",
            "TEMP_DIR": "",
            "TYPE": "slurm",
            "USER": "root",
            "MAX_PROCESSORS": "4",
            "PROCESSORS_PER_NODE": "4",
        },
    },
    "JOBS": {
        "OPERATOR": {
            "PLATFORM": "TEST_SLURM",
            "RUNNING": "chunk",
            "SPLITS": "4",
            "SCRIPT": 'echo "operator"',
            "WALLCLOCK": "00:01",
        },
        "APPLICATION": {
            "PLATFORM": "TEST_SLURM",
            "RUNNING": "chunk",
            "SPLITS": "4",
            "SCRIPT": 'echo "application"',
            "WALLCLOCK": "00:01",
            "DEPENDENCIES": {
                "OPERATOR": {
                    "SPLITS_FROM": {
                        "ALL": {
                            "SPLITS_TO": "[1:4]*\\1",
                        },
                    },
                },
            },
        },
    },
    "WRAPPERS": {
        "WRAPPER_APPLICATION": {
            "JOBS_IN_WRAPPER": "APPLICATION",
            "TYPE": "vertical",
            "POLICY": "strict",
            "MIN_WRAPPED": "4",
            "MAX_WRAPPED": "4",
        },
    },
}


def _jobs_in_section(job_list: JobList, section: str) -> list[Job]:
    """Return the jobs that belong to ``section`` (case-insensitive)."""
    return [job for job in job_list.get_job_list() if job.section.upper() == section]


def _package_sizes(packages: list, section: str) -> list[int]:
    """Return the job counts of the packages made only of ``section`` jobs."""
    return [
        len(package.jobs)
        for package in packages
        if package.jobs and all(job.section.upper() == section for job in package.jobs)
    ]


@pytest.fixture
def wrappable_packager(create_packager: CreatePackagerFixture) -> JobPackager:
    """Return a JobPackager for an experiment with one strict vertical wrapper."""
    job_packager = create_packager(experiment_data=_VERTICAL_WRAPPER_EXPERIMENT)
    jobs = job_packager._jobs_list.get_job_list()
    assert {job.platform_name for job in jobs} == {"TEST_SLURM"}
    job_packager._platform = jobs[0].platform
    job_packager.calculate_job_limits(job_packager._platform)
    return job_packager


@pytest.mark.parametrize("parent_completed_in_db", [
    pytest.param(True, id="parent-saved"),
    pytest.param(False, id="parent-only-in-memory"),
])
def test_build_packages_loads_wrappable_child_with_stale_edge(
    wrappable_packager: JobPackager, parent_completed_in_db: bool
):
    """A vertical wrapper must not hit a false deadlock while a child is only in the DB.

    Regression test for the DestinE vertical-wrapper race: the parent is
    COMPLETED in memory, but its edge ``completion_status`` is still ``WAITING``
    in the database (``update_list`` runs after the packager). When
    ``parent_completed_in_db`` is False the parent status is stale too, which is
    the exact state that produced the ``strict`` policy 7014 in CI. The child
    must be loaded from the DB so the wrapper can reach ``MIN_WRAPPED``.
    """
    job_list = wrappable_packager._jobs_list

    # Keep the persisted edge completion status stale, as update_list has not run.
    job_list.save_edges()

    operators = _jobs_in_section(job_list, "OPERATOR")
    applications = sorted(
        _jobs_in_section(job_list, "APPLICATION"),
        key=lambda job: int(job.split or 0),
    )
    assert len(operators) == 4
    assert len(applications) == 4

    for job in applications[:3]:
        job.status = Status.READY
    applications[3].status = Status.WAITING

    parent_db_status = Status.COMPLETED if parent_completed_in_db else Status.WAITING
    for job in operators:
        job.status = parent_db_status
    job_list.save_jobs()
    for job in operators:
        job.status = Status.COMPLETED

    # Simulate the last application split not being loaded in memory yet.
    job_list._remove_job_from_graph(applications[3].name)

    packages = wrappable_packager.build_packages()

    assert not wrappable_packager.wrappers_with_error, next(
        iter(wrappable_packager.wrappers_with_error.values()), "unexpected strict policy error"
    )
    package_sizes = _package_sizes(packages, "APPLICATION")
    assert 4 in package_sizes, f"Expected a 4-job APPLICATION wrapper, got sizes {package_sizes}"
