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


import pytest
from mock import MagicMock

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_list import JobList
from autosubmit.job.job_list_persistence import JobListPersistenceDb
from autosubmit.job.job_packages import JobPackageSimple, JobPackageVertical
from autosubmit.job.job_packages import jobs_in_wrapper_str
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory


@pytest.fixture
def platform(mocker):
    platform = mocker.MagicMock()
    platform.queue = "debug"
    platform.partition = "debug"
    platform.serial_platform = platform
    platform.serial_platform.max_wallclock = '24:00'
    platform.serial_queue = "debug-serial"
    platform.serial_partition = "debug-serial"
    platform.max_waiting_jobs = 100
    platform.total_jobs = 100
    return platform


@pytest.fixture
def jobs(platform) -> list[Job]:
    jobs = [Job('dummy1', 0, Status.READY, 0),
            Job('dummy2', 0, Status.READY, 0)]
    for job in jobs:
        job._init_runtime_parameters()

    jobs[0].wallclock = "00:00"
    jobs[0]._threads = "1"
    jobs[0].tasks = "1"
    jobs[0].exclusive = True
    jobs[0].queue = "debug"
    jobs[0].partition = "debug"
    jobs[0].custom_directives = "dummy_directives"
    jobs[0].processors = "9"
    jobs[0]._processors = "9"
    jobs[0].retrials = 0
    jobs[1].wallclock = "00:00"
    jobs[1].tasks = "1"
    jobs[1].exclusive = True
    jobs[1].queue = "debug2"
    jobs[1].partition = "debug2"
    jobs[1].custom_directives = "dummy_directives2"

    jobs[0]._platform = jobs[1]._platform = platform

    return jobs


@pytest.fixture
def as_conf(autosubmit_config):
    return autosubmit_config('a000', {
        'JOBS': {},
        'PLATFORMS': {},
        'WRAPPERS': {}
    })


@pytest.fixture
def create_job_package_wrapper(jobs, as_conf):
    def fn(options: dict) -> JobPackageVertical:
        as_conf.experiment_data['WRAPPERS']['WRAPPERS'] = options
        wrapper_type = options.get('TYPE', 'vertical')
        wrapper_policy = options.get('POLICY', 'flexible')
        wrapper_method = options.get('METHOD', 'ASThread')
        jobs_in_wrapper = options.get('JOBS_IN_WRAPPER', 'None')
        extensible_wallclock = options.get('EXTEND_WALLCLOCK', 0)
        return JobPackageVertical(
            jobs,
            configuration=as_conf,
            wrapper_info=[
                wrapper_type,
                wrapper_policy,
                wrapper_method,
                jobs_in_wrapper,
                extensible_wallclock
            ])

    return fn


@pytest.fixture
def joblist(tmp_path, as_conf):
    job_list = JobList('a000', as_conf, YAMLParserFactory(),
                       JobListPersistenceDb(str(tmp_path)))
    job_list._ordered_jobs_by_date_member["WRAPPERS"] = dict()
    return job_list


def test_default_parameters(create_job_package_wrapper):
    options = {
        'TYPE': "vertical",
        'JOBS_IN_WRAPPER': "None",
        'METHOD': "ASThread",
        'POLICY': "flexible",
        'EXTEND_WALLCLOCK': 0,
    }
    job_package_wrapper = create_job_package_wrapper(options)
    assert job_package_wrapper.wrapper_type == "vertical"
    assert job_package_wrapper.jobs_in_wrapper == "None"
    assert job_package_wrapper.wrapper_method == "ASThread"
    assert job_package_wrapper.wrapper_policy == "flexible"
    assert job_package_wrapper.extensible_wallclock == 0

    assert job_package_wrapper.exclusive is True
    assert job_package_wrapper.inner_retrials == 0
    assert job_package_wrapper.queue == "debug"
    assert job_package_wrapper.partition == "debug"
    assert job_package_wrapper._threads == "1"
    assert job_package_wrapper.tasks == "1"

    options_slurm = {
        'EXCLUSIVE': False,
        'QUEUE': "bsc32",
        'PARTITION': "bsc32",
        'THREADS': "30",
        'TASKS': "40",
        'INNER_RETRIALS': 30,
        'CUSTOM_DIRECTIVES': "['#SBATCH --mem=1000']"
    }
    job_package_wrapper = create_job_package_wrapper(options_slurm)
    assert job_package_wrapper.exclusive is False
    assert job_package_wrapper.inner_retrials == 30
    assert job_package_wrapper.queue == "bsc32"
    assert job_package_wrapper.partition == "bsc32"
    assert job_package_wrapper._threads == "30"
    assert job_package_wrapper.tasks == "40"
    assert job_package_wrapper.custom_directives == ['#SBATCH --mem=1000']


def test_job_package_default_init():
    with pytest.raises(Exception):
        JobPackageSimple([])


def test_job_package_different_platforms_init(jobs):
    jobs[0]._platform = MagicMock()
    jobs[1]._platform = MagicMock()
    with pytest.raises(Exception):
        JobPackageSimple(jobs)


def test_job_package_none_platforms_init(jobs):
    jobs[0]._platform = None
    jobs[1]._platform = None
    with pytest.raises(Exception):
        JobPackageSimple(jobs)


def test_job_package_length(jobs):
    job_package = JobPackageSimple(jobs)
    assert 2 == len(job_package)


def test_job_package_jobs_getter(jobs):
    job_package = JobPackageSimple(jobs)
    assert jobs == job_package.jobs


def test_job_package_platform_getter(jobs, platform):
    job_package = JobPackageSimple(jobs)
    assert platform == job_package.platform


def test_jobs_in_wrapper_str(autosubmit_config):
    as_conf = autosubmit_config('a000', {
        "WRAPPERS": {
            "current_wrapper": {
                "JOBS_IN_WRAPPER": "job1 job2 job3"
            }
        }
    })
    # Arrange
    current_wrapper = "current_wrapper"
    result = jobs_in_wrapper_str(as_conf, current_wrapper)
    assert result == "job1_job2_job3"


def test_job_package_submission(mocker, local):
    # N.B.: AS only calls ``_create_scripts`` if you have less jobs than threads.
    # So we simply set threads to be greater than the amount of jobs.
    jobs = [
        Job("job1", "1", Status.READY, 0),
        Job("job2", "2", Status.READY, 0),
        Job("job3", "3", Status.READY, 0)
    ]
    for job in jobs:
        job.platform = local

    mocker.patch('multiprocessing.cpu_count', return_value=len(jobs) + 1)
    mocker.patch("autosubmit.job.job.Job.update_parameters", return_value={})
    mocker.patch('autosubmit.job.job.Job._get_paramiko_template', return_value="empty")
    for job in jobs:
        job._tmp_path = MagicMock()
        job.file = "fake-file"
        job.custom_directives = []

    job_package = JobPackageSimple(jobs)

    job_package._create_scripts = MagicMock()
    job_package._send_files = MagicMock()
    job_package._do_submission = MagicMock()
    configuration = MagicMock()
    configuration.get_project_dir = MagicMock()
    configuration.get_project_dir.return_value = "fake-proj-dir"
    # act
    job_package.submit(configuration, 'fake-params')
    # assert
    for job in jobs:
        # Should be called once for each job, but currently it needs two calls (for additional files) to change the code
        job.update_parameters.assert_called()  # type: ignore

    job_package._create_scripts.is_called_once_with()
    job_package._send_files.is_called_once_with()
    job_package._do_submission.is_called_once_with()
