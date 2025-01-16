from datetime import datetime, timedelta

import pytest

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_list import JobList
from autosubmit.job.job_list_persistence import JobListPersistencePkl
from autosubmit.job.job_packages import JobPackageSimple, JobPackageVertical, JobPackageHorizontal
from autosubmit.platforms.slurmplatform import SlurmPlatform
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory

exp_data = {
    "WRAPPERS": {
        "WRAPPERS": {
            "JOBS_IN_WRAPPER": "dummysection"
        }
    }
}

@pytest.fixture
def setup_as_conf(autosubmit_config, tmpdir, prepare_basic_config):
    as_conf = autosubmit_config("random-id", exp_data)
    return as_conf


@pytest.fixture
def new_job_list(setup_as_conf, tmpdir, prepare_basic_config):
    job_list = JobList("random-id", prepare_basic_config, YAMLParserFactory(),
                       JobListPersistencePkl(), setup_as_conf)

    return job_list


@pytest.fixture
def new_platform(mocker):
    dummy_platform = mocker.MagicMock(autospec=SlurmPlatform)
    # Add here as many attributes as needed
    dummy_platform.name = 'dummy_platform'
    dummy_platform.max_wallclock = "02:00"

    # When proc = 1, the platform used will be serial, so just nest the defined platform.
    dummy_platform.serial_platform = dummy_platform
    return dummy_platform


def new_packages(as_conf, dummy_jobs):
    packages = [
        JobPackageSimple([dummy_jobs[0]]),
        JobPackageVertical(dummy_jobs, configuration=as_conf),
        JobPackageHorizontal(dummy_jobs, configuration=as_conf),
    ]
    for package in packages:
        if not isinstance(package, JobPackageSimple):
            package._name = "wrapped"
    return packages


def setup_jobs(dummy_jobs, new_platform):
    for job in dummy_jobs:
        job._platform = new_platform
        job.processors = 2
        job.section = "dummysection"
        job._init_runtime_parameters()
        job.wallclock = "00:01"
        job.start_time = datetime.now() - timedelta(minutes=1)


def test_check_wrapper_stored_status(setup_as_conf, new_job_list, new_platform):
    dummy_jobs = [Job("dummy-1", 1, Status.SUBMITTED, 0), Job("dummy-2", 2, Status.SUBMITTED, 0), Job("dummy-3", 3, Status.SUBMITTED, 0)]
    setup_jobs(dummy_jobs, new_platform)
    new_job_list.jobs = dummy_jobs
    packages = new_packages(setup_as_conf, dummy_jobs)
    new_job_list.packages_dict = packages


def test_is_over_wallclock(new_platform):
    job = Job("dummy-1", 1, Status.SUBMITTED, 0)
    setup_jobs([job], new_platform)
    job.wallclock = "00:01"
    assert job.is_over_wallclock() is False
    job.start_time = datetime.now() - timedelta(minutes=2)
    assert job.is_over_wallclock() is True
