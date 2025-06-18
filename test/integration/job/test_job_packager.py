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

from typing import Optional, Protocol

import pytest

from autosubmit.config.yamlparser import YAMLParserFactory
from autosubmit.job.job_common import Status
from autosubmit.job.job_list import JobList
from autosubmit.job.job_packager import JobPackager

_EXPID = 't000'


class CreatePackagerFixture(Protocol):

    def __call__(
            self,
            experiment_data: Optional[dict] = None,
            total_jobs: Optional[int] = 20
    ) -> JobPackager:
        ...


@pytest.fixture
def create_packager(autosubmit_exp, autosubmit, local) -> CreatePackagerFixture:
    def _job_packager(experiment_data: Optional[dict], total_jobs: Optional[int] = 20) -> JobPackager:
        local.total_jobs = total_jobs

        exp = autosubmit_exp(_EXPID, experiment_data=experiment_data)
        as_conf = exp.as_conf
        parameters = as_conf.load_parameters()

        job_list_persistence = autosubmit._get_job_list_persistence(_EXPID, as_conf)
        job_list = JobList(_EXPID, exp.as_conf, YAMLParserFactory(), job_list_persistence)

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
            run_only_members=[],
            force=False,
            create=True)

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


def test_check_if_packages_are_ready_to_build_jobs_to_run_first(create_packager: CreatePackagerFixture):
    """TODO: Looks like this might be removed when two_way_step is removed? Old auto-monarch was using that?"""
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

    jobs = job_packager._jobs_list.get_job_list()
    job_packager._jobs_list.jobs_to_run_first = jobs

    jobs, flag = job_packager.check_if_packages_are_ready_to_build()

    assert jobs
    assert flag
