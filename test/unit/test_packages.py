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

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_packages import JobPackageSimple, JobPackageVertical, JobPackageHorizontal


@pytest.fixture
def create_packages(mocker, autosubmit_config):
    exp_data = {
        "WRAPPERS": {
            "WRAPPERS": {
                "JOBS_IN_WRAPPER": "dummysection"
            }
        }
    }
    as_conf = autosubmit_config("a000", exp_data)
    jobs = [Job("dummy-1", 1, Status.SUBMITTED, 0), Job("dummy-2", 2, Status.SUBMITTED, 0),
            Job("dummy-3", 3, Status.SUBMITTED, 0)]
    platform = mocker.MagicMock()
    platform.name = 'dummy'
    platform.serial_platform = mocker.MagicMock()
    platform.serial_platform.max_wallclock = '24:00'
    for job in jobs:
        job._platform = platform
        job.processors = 2
        job.section = "dummysection"
        job._init_runtime_parameters()
        job.wallclock = "00:01"
    packages = [
        JobPackageSimple([jobs[0]]),
        JobPackageVertical(jobs, configuration=as_conf),
        JobPackageHorizontal(jobs, configuration=as_conf),
    ]
    for package in packages:
        if not isinstance(package, JobPackageSimple):
            package._name = "wrapped"
    return packages


def test_process_jobs_to_submit(create_packages):
    packages = create_packages
    jobs_id = [1, 2, 3]
    for i, package in enumerate(
            packages):  # Equivalent to valid_packages_to_submit but without the ghost jobs check etc.
        package.process_jobs_to_submit(jobs_id[i])
        for job in package.jobs:  # All jobs inside a package must have the same id.
            assert job.id == jobs_id[i]
            assert job.status == Status.SUBMITTED


def test_init_validates_single_platform(mocker):
    """Test __init__ raises when jobs have different platforms."""
    from autosubmit.job.job_packages import JobPackageBase

    platform_a = mocker.MagicMock()
    platform_a.name = "platform_a"
    platform_b = mocker.MagicMock()
    platform_b.name = "platform_b"

    job_a = Job(name="job_a", job_id=1, status=Status.READY, priority=0)
    job_a._platform = platform_a
    job_a.platform = platform_a

    job_b = Job(name="job_b", job_id=2, status=Status.READY, priority=0)
    job_b._platform = platform_b
    job_b.platform = platform_b

    with pytest.raises(Exception, match="Only one valid platform per package"):
        JobPackageBase([job_a, job_b])


def test_init_raises_on_empty_jobs():
    """Test __init__ raises when no jobs are given."""
    from autosubmit.job.job_packages import JobPackageBase

    with pytest.raises(ValueError):
        JobPackageBase([])


def test_init_sets_fail_count_from_first_job(mocker):
    """Test __init__ copies fail_count from the first job."""
    from autosubmit.job.job_packages import JobPackageBase

    platform = mocker.MagicMock()
    platform.name = "fake"
    platform.type = "PS"

    job = Job(name="job1", job_id=1, status=Status.READY, priority=0)
    job._platform = platform
    job.platform = platform
    job.fail_count = 5

    package = JobPackageBase([job])

    assert package.fail_count == 5


def test_init_sets_sections_from_jobs(mocker):
    """Test __init__ builds sections string from jobs."""
    from autosubmit.job.job_packages import JobPackageBase

    platform = mocker.MagicMock()
    platform.name = "fake"
    platform.type = "PS"

    job1 = Job(name="job1", job_id=1, status=Status.READY, priority=0)
    job1._platform = platform
    job1.platform = platform
    job1.section = "section_b"

    job2 = Job(name="job2", job_id=2, status=Status.READY, priority=0)
    job2._platform = platform
    job2.platform = platform
    job2.section = "section_a"

    package = JobPackageBase([job1, job2])

    assert package.sections == "section_a&section_b"


@pytest.mark.parametrize("ec_queue_value,expected", [
    ([1, 2, 3], [1, 2, 3]),
    (None, None),
])
def test_init_ec_queue(mocker, ec_queue_value, expected):
    """Test __init__ sets ec_queue from the job when present, else None."""
    from collections import deque
    from autosubmit.job.job_packages import JobPackageBase

    platform = mocker.MagicMock()
    platform.name = "fake"
    platform.type = "PS"

    job = Job(name="job1", job_id=1, status=Status.READY, priority=0)
    job._platform = platform
    job.platform = platform
    if ec_queue_value is not None:
        job.ec_queue = deque(ec_queue_value)

    package = JobPackageBase([job])

    if expected is None:
        assert package.ec_queue is None
    else:
        assert list(package.ec_queue) == expected



@pytest.mark.parametrize("project_type", [None, "None"])
def test_check_job_files_exists_returns_early_for_no_project(mocker, project_type):
    """Test check_job_files_exists returns early when project type is absent."""
    platform = mocker.MagicMock()
    platform.name = "fake"
    platform.type = "PS"

    job = Job(name="job1", job_id=1, status=Status.READY, priority=0)
    job._platform = platform
    job.platform = platform

    as_conf = mocker.MagicMock()
    as_conf.get_project_type.return_value = project_type

    package = JobPackageSimple([job])

    package.check_job_files_exists(as_conf, only_generate=False)


@pytest.mark.parametrize("only_generate,raises", [
    (False, True),
    (True, False),
])
def test_check_job_files_exists_missing_job_file(mocker, tmp_path, only_generate, raises):
    """Test check_job_files_exists raises or warns depending on only_generate."""
    from autosubmit.log.log import AutosubmitCritical

    platform = mocker.MagicMock()
    platform.name = "fake"
    platform.type = "PS"

    job = Job(name="job1", job_id=1, status=Status.READY, priority=0)
    job._platform = platform
    job.platform = platform
    job.file = "nonexistent_script.sh"

    as_conf = mocker.MagicMock()
    as_conf.get_project_type.return_value = "git"
    as_conf.get_project_dir.return_value = str(tmp_path)

    package = JobPackageSimple([job])

    if raises:
        with pytest.raises(AutosubmitCritical, match="does not exists"):
            package.check_job_files_exists(as_conf, only_generate=only_generate)
    else:
        package.check_job_files_exists(as_conf, only_generate=only_generate)


def test_check_job_files_raises_when_additional_file_missing(mocker, tmp_path):
    """Test raises when additional file does not exist."""
    from autosubmit.log.log import AutosubmitCritical

    platform = mocker.MagicMock()
    platform.name = "fake"
    platform.type = "PS"

    job = Job(name="job1", job_id=1, status=Status.READY, priority=0)
    job._platform = platform
    job.platform = platform
    job.additional_files = ["missing_additional.txt"]

    as_conf = mocker.MagicMock()
    as_conf.get_project_type.return_value = "git"
    as_conf.get_project_dir.return_value = str(tmp_path)

    package = JobPackageSimple([job])

    with pytest.raises(AutosubmitCritical, match="Additional file:.*does not exists"):
        package.check_job_files_exists(as_conf, only_generate=False)



@pytest.mark.parametrize("only_generate,expect_clean_called", [
    (False, True),
    (True, False),
])
def test_generate_scripts_clean_behaviour(mocker, only_generate, expect_clean_called):
    """Test generate_scripts calls or skips _delete_previous_run_files based on only_generate."""
    platform = mocker.MagicMock()
    platform.name = "fake"
    platform.type = "PS"

    job = Job(name="job1", job_id=1, status=Status.READY, priority=0)
    job._platform = platform
    job.platform = platform
    job.additional_files = []
    job.custom_directives = []

    as_conf = mocker.MagicMock()
    as_conf.get_project_type.return_value = None

    package = JobPackageSimple([job])
    package._delete_previous_run_files = mocker.MagicMock()
    package._create_scripts = mocker.MagicMock()

    package.generate_scripts(as_conf, only_generate=only_generate)

    if expect_clean_called:
        package._delete_previous_run_files.assert_called_once()
    else:
        package._delete_previous_run_files.assert_not_called()



def test_deletes_files_only_for_jobs_with_zero_fail_count(mocker):
    """Test only jobs with fail_count==0 are passed to delete methods."""
    platform = mocker.MagicMock()
    platform.name = "fake"
    platform.type = "PS"

    job1 = Job(name="job1", job_id=1, status=Status.READY, priority=0)
    job1._platform = platform
    job1.platform = platform
    job1.fail_count = 0

    job2 = Job(name="job2", job_id=2, status=Status.READY, priority=0)
    job2._platform = platform
    job2.platform = platform
    job2.fail_count = 3

    package = JobPackageSimple([job1, job2])
    package._delete_previous_run_files()

    platform.delete_previous_run_files_by_job_names.assert_called_once_with(["job1"])
    platform.delete_previous_stat_files_by_job_names.assert_called_once_with(["job1"])



def test_create_scripts_populates_job_scripts(mocker):
    """Test _create_scripts calls job.create_script for each job."""
    platform = mocker.MagicMock()
    platform.name = "fake"
    platform.type = "PS"

    job = Job(name="job1", job_id=1, status=Status.READY, priority=0)
    job._platform = platform
    job.platform = platform

    mock_create_script = mocker.patch.object(Job, 'create_script', return_value="/path/to/job1.cmd")
    as_conf = mocker.MagicMock()

    package = JobPackageSimple([job])
    package._create_scripts(as_conf)

    mock_create_script.assert_called_once_with(as_conf)
    assert package._job_scripts["job1"] == "/path/to/job1.cmd"



def test_simple_wrapped_creates_both_script_types(mocker):
    """Test _create_scripts creates both regular and wrapped scripts."""
    from autosubmit.job.job_packages import JobPackageSimpleWrapped

    platform = mocker.MagicMock()
    platform.name = "fake"
    platform.type = "PS"

    job = Job(name="job1", job_id=1, status=Status.READY, priority=0)
    job._platform = platform
    job.platform = platform

    mock_create_script = mocker.patch.object(Job, 'create_script', return_value="/path/to/job1.cmd")
    mock_create_wrapped = mocker.patch.object(Job, 'create_wrapped_script', return_value="/path/to/job1_wrapped.cmd")
    as_conf = mocker.MagicMock()

    package = JobPackageSimpleWrapped([job])
    package._create_scripts(as_conf)

    mock_create_script.assert_called_once()
    mock_create_wrapped.assert_called_once()
    assert package._job_scripts["job1"] == "/path/to/job1.cmd"
    assert package._job_wrapped_scripts["job1"] == "/path/to/job1_wrapped.cmd"


@pytest.mark.parametrize("nodes,processors,num_processors,expected_queue", [
    ("abc", "1", "1", "serial_q"),
    ("0", "0", "0", "serial_q"),
    ("2", "4", "4", "main_q"),
])
def test_thread_queue_property(mocker, autosubmit_config, nodes, processors, num_processors, expected_queue):
    """Test JobPackageThread.queue returns the correct queue based on nodes and processors."""
    from autosubmit.job.job_packages import JobPackageThread

    platform = mocker.MagicMock()
    platform.name = "fake"
    platform.type = "slurm"
    platform.serial_platform = mocker.MagicMock()
    platform.serial_platform.serial_queue = "serial_q"
    platform.wrapper = mocker.MagicMock()

    exp_data = {"WRAPPERS": {"WRAPPERS": {"JOBS_IN_WRAPPER": "dummy"}}}
    as_conf = autosubmit_config("a000", exp_data)

    job = Job(name="job1", job_id=1, status=Status.READY, priority=0)
    job._platform = platform
    job.platform = platform
    job.nodes = nodes
    job.processors = processors
    job.threads = "1"
    job.queue = "main_q"
    job.partition = None
    job.tasks = None
    job.exclusive = None
    job.custom_directives = []
    job.reservation = None
    job.het = {"HETSIZE": 1}
    job.memory = None
    job.memory_per_task = None
    job.retrials = 0

    package = JobPackageThread([job], configuration=as_conf)
    package._num_processors = num_processors

    assert package.queue == expected_queue


@pytest.mark.parametrize("nodes,processors,num_processors,expected_queue", [
    ("1", "0", "0", "serial_q"),
    ("1", "1", "1", "serial_q"),
    ("2", "4", "4", "main_q"),
])
def test_thread_wrapped_queue_property(mocker, autosubmit_config, nodes, processors, num_processors, expected_queue):
    """Test JobPackageThreadWrapped.queue returns the correct queue."""
    from autosubmit.job.job_packages import JobPackageThreadWrapped

    platform = mocker.MagicMock()
    platform.name = "fake"
    platform.type = "slurm"
    platform.serial_platform = mocker.MagicMock()
    platform.serial_platform.serial_queue = "serial_q"
    platform.queue = "main_q"
    platform.wrapper = mocker.MagicMock()

    exp_data = {"WRAPPERS": {"WRAPPERS": {"JOBS_IN_WRAPPER": "dummy"}}}
    as_conf = autosubmit_config("a000", exp_data)

    job = Job(name="job1", job_id=1, status=Status.READY, priority=0)
    job._platform = platform
    job.platform = platform
    job.nodes = nodes
    job.processors = processors
    job.threads = "1"
    job.queue = "main_q"
    job.partition = None
    job.tasks = None
    job.exclusive = None
    job.custom_directives = []
    job.reservation = None
    job.het = {"HETSIZE": 1}
    job.memory = None
    job.memory_per_task = None
    job.retrials = 0

    package = JobPackageThreadWrapped([job], configuration=as_conf)
    package._num_processors = num_processors

    assert package.queue == expected_queue



@pytest.mark.parametrize("experiment_data,wrapper_key,expected", [
    (
        {"WRAPPERS": {"my_wrapper": {"JOBS_IN_WRAPPER": ["sim", "post", "plot"]}}},
        "my_wrapper",
        "sim_post_plot",
    ),
    (
        {"WRAPPERS": {}},
        "nonexistent",
        "",
    ),
    (
        {"WRAPPERS": {"my_wrapper": {}}},
        "my_wrapper",
        "",
    ),
    (
        {"WRAPPERS": {"my_wrapper": {"JOBS_IN_WRAPPER": []}}},
        "my_wrapper",
        "",
    ),
    (
        {"WRAPPERS": {"my_wrapper": {"JOBS_IN_WRAPPER": ["only_job"]}}},
        "my_wrapper",
        "only_job",
    ),
])
def test_jobs_in_wrapper_str(mocker, experiment_data, wrapper_key, expected):
    """Test jobs_in_wrapper_str returns the correct joined string."""
    from autosubmit.job.job_packages import jobs_in_wrapper_str

    as_conf = mocker.MagicMock()
    as_conf.experiment_data = experiment_data

    assert jobs_in_wrapper_str(as_conf, wrapper_key) == expected
