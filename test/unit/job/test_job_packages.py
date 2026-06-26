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

from autosubmit.config.yamlparser import YAMLParserFactory
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_list import JobList
from autosubmit.job.job_list_persistence import JobListPersistenceDb
from autosubmit.job.job_packages import (
    JobPackageSimple,
    JobPackageVertical,
    jobs_in_wrapper_str,
)
from autosubmit.log.log import AutosubmitCritical, AutosubmitError
from autosubmit.platforms.locplatform import LocalPlatform
from autosubmit.platforms.pjmplatform import PJMPlatform
from autosubmit.platforms.slurmplatform import SlurmPlatform


@pytest.fixture
def create_platform(mocker):
    def _create_platform():
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

    return _create_platform


@pytest.fixture
def jobs(create_platform, local) -> list[Job]:
    jobs = [Job('dummy1', 0, Status.READY, 0),
            Job('dummy2', 0, Status.READY, 0)]
    for job in jobs:
        # TODO: Why not call this in the constructor of Job?
        job._init_runtime_parameters()

    jobs[0].wallclock = "00:00"
    jobs[0]._threads = "1"
    jobs[0].tasks = "1"
    jobs[0].exclusive = True
    jobs[0].queue = "debug"
    jobs[0].partition = "debug"
    jobs[0].custom_directives = "dummy_directives"
    jobs[0].processors = "9"
    jobs[0].retrials = 0
    jobs[1].wallclock = "00:00"
    jobs[1].tasks = "1"
    jobs[1].exclusive = True
    jobs[1].queue = "debug2"
    jobs[1].partition = "debug2"
    jobs[1].custom_directives = "dummy_directives2"

    jobs[0].platform = jobs[1].platform = local

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
    job_list = JobList('a000', as_conf, YAMLParserFactory(), JobListPersistenceDb(as_conf.expid))
    # TODO: Check why we can't make ordered jobs public, or if there is a better way of setting wrappers as dict
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


def test_job_package_platform_getter(jobs):
    job_package = JobPackageSimple(jobs)
    target_platform = jobs[0].platform
    assert job_package.platform == target_platform


def test_jobs_in_wrapper_str(autosubmit_config):
    as_conf = autosubmit_config('a000', {
        "WRAPPERS": {
            "current_wrapper": {
                "JOBS_IN_WRAPPER": ["job1", "job2", "job3"]
            }
        }
    })
    # Arrange
    current_wrapper = "current_wrapper"
    result = jobs_in_wrapper_str(as_conf, current_wrapper)
    assert result == "job1_job2_job3"


@pytest.mark.parametrize(
    "error,target_function",
    [
        (None, None),
        (AutosubmitError, "check_job_files_exists"),
        (AutosubmitCritical, "check_job_files_exists"),
        (AutosubmitError, "build_scripts"),
        (AutosubmitCritical, "build_scripts"),
    ],
    ids=[
        "no_error",
        "autosubmit_error_on_check_job_files_exists",
        "autosubmit_critical_on_check_job_files_exists",
        "autosubmit_error_on_build_scripts",
        "autosubmit_critical_on_build_scripts",
    ],
)
def test_job_package_generate_scripts(mocker, local, tmp_path, error, target_function) -> None:
    jobs = [
        Job("job1", "1", Status.READY, 0),
        Job("job2", "2", Status.READY, 0),
        Job("job3", "3", Status.READY, 0),
    ]
    for job in jobs:
        job.platform = local
        job._tmp_path = tmp_path
        job.file = tmp_path / "fake-file"
        job.custom_directives = []
        job.file.write_text("echo 'Hello World'")
        job.wallclock = '00:05'

    job_package = JobPackageSimple(jobs)
    mock_clean_previous_run = mocker.patch.object(job_package, "_clean_previous_run")
    mock_check_job_files_exists = mocker.patch.object(job_package, "check_job_files_exists")
    mock_build_scripts = mocker.patch.object(job_package, "build_scripts")

    method_mocks = {
        "check_job_files_exists": mock_check_job_files_exists,
        "build_scripts": mock_build_scripts,
    }
    configuration = mocker.MagicMock()

    if error is not None and target_function is not None:
        method_mocks[target_function].side_effect = error("boom", 7000) if error is AutosubmitError else error("boom", 7000)

        with pytest.raises(error):  # type: ignore
            job_package.generate_scripts(configuration)

        mock_clean_previous_run.assert_called_once()
        if target_function == "build_scripts":
            mock_check_job_files_exists.assert_called_once_with(configuration, False)
        if target_function == "check_job_files_exists":
            mock_build_scripts.assert_not_called()
    else:
        job_package.generate_scripts(configuration)

        mock_clean_previous_run.assert_called_once()
        mock_check_job_files_exists.assert_called_once_with(configuration, False)
        mock_build_scripts.assert_called_once_with(configuration)


def test_check_job_files_exists_no_additional_job_skip_check(local, mocker, tmp_path, jobs):
    """Test that checking a job with invalid additional jobs results in a warning.

    You must check for additional files, but the additional file must not exist or be
    ``None``. The ``only_generate`` argument of ``.check_job_files_exists`` must be ``True``.
    """
    job_package = JobPackageSimple(jobs)

    # Mocking as we only require to mock one attribute -- if you require more, we
    # are probably better using the ``autosubmit_config`` fixture here.
    configuration = mocker.MagicMock()
    empty_tmp_path = tmp_path / "empty-tmp-path"
    configuration.get_project_dir.return_value = empty_tmp_path

    jobs[0].additional_files = ['extra.inp']

    mocked_log = mocker.patch('autosubmit.job.job_packages.Log')
    job_package.check_job_files_exists(configuration, True)

    assert mocked_log.warning.called
    assert 'file: extra.inp does not exist' in mocked_log.warning.call_args[0][0]


def test_job_package_send_files_uses_current_public_api(mocker, local, tmp_path) -> None:
    """Send package scripts through the current ``send_files`` method."""
    jobs = [
        Job("job1", "1", Status.READY, 0),
        Job("job2", "2", Status.READY, 0),
    ]
    for job in jobs:
        job.platform = local
        job.wallclock = '00:05'
        job._tmp_path = tmp_path
        job.additional_files = []

    job_package = JobPackageSimple(jobs)
    job_package._job_scripts = {
        "job1": "job1.cmd",
        "job2": "job2.cmd",
    }
    mocked_send_file = mocker.patch.object(local, "send_file")

    job_package.send_files()

    assert mocked_send_file.call_args_list == [
        mocker.call("job1.cmd"),
        mocker.call("job2.cmd"),
    ]


def test_build_scripts_with_empty_variables_warning(jobs, mocker):
    """Test that building scripts that contain empty variables show a warning to users."""
    jobs = jobs[0:1]
    jobs[0].script = 'anything.sh'
    job_package = JobPackageSimple(jobs)
    mocked_log = mocker.patch('autosubmit.job.job_packages.Log')
    with (
        mocker.patch.object(Job, "check_script", return_value=False),
        mocker.patch.object(job_package, "_create_scripts", return_value=None)
    ):
        job_package.build_scripts(mocker.MagicMock())

    assert mocked_log.warning.called
    assert f'Job {jobs[0].name} script or file has empty variables' in mocked_log.warning.call_args[0][0]
        

def test_job_package_properties(jobs):
    """Test basic properties created during the simple package instantiation."""
    job_package = JobPackageSimple(jobs)
    assert job_package.num_processors == '0'
    assert job_package.name == jobs[0].name  # name of the first job...


@pytest.mark.parametrize(
    "platform_class,expected_timeout",
    [
        (LocalPlatform, 6),
        (PJMPlatform, None),
        (SlurmPlatform, None)
    ],
    ids=[
        "Local triggers the timeout to be set (1 as local will have 1 second in the test)",
        "PJM manages the wallclock, so timeout in the job is None",
        "Slurm manages the wallclock, so timeout in the job is None"
    ]
)
def test_job_package_timeout(platform_class , expected_timeout, create_platform):
    """Test that the ``timeout`` is calculated correctly.

    The ``timeout`` is calculated ONLY in ``ps`` and ``local`` platforms
    as the ``max`` of ``job.wallclock_in_seconds``. It receives a job
    list, which is assumed to be non-empty.

    We use a fixed list of four jobs, with wallclocks of 1, 2, 3, and 4
    seconds. Jobs get assigned a platform from the ``list_of_platforms``.

    This means that if you have one platform that enables timeout, then
    you will have the ``timeout`` equals 1, if you have two, then the
    ``timeout`` will be 2, and so on.
    """
    jobs = [Job(f"job{i}", f"{i}", Status.READY, 0) for i in range(4)]
    # job wallclock is a property, that checks and initialises the ``wallclock_in_seconds``
    platform = create_platform()
    serial_platform = create_platform()
    for job in jobs:
        serial_platform.type = platform_class.TYPE
        serial_platform.name = platform_class.TYPE
        platform.serial_platform = serial_platform
        job.platform = platform
        # ATTENTION: Internally, Autosubmit increases 30% the wall-time, so we will have 6 seconds (floor).
        job.wallclock = '00:00:05'

    job_packages = JobPackageSimple(jobs)
    if expected_timeout is None:
        assert job_packages.timeout is None
    else:
        assert job_packages.timeout == expected_timeout
