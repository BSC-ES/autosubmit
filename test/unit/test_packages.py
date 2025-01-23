import mock
import pytest
from autosubmit.job.job_common import Status
from autosubmit.job.job_packages import JobPackageSimple, JobPackageVertical, JobPackageHorizontal
from autosubmit.job.job import Job


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
    jobs = [Job("dummy-1", 1, Status.SUBMITTED, 0), Job("dummy-2", 2, Status.SUBMITTED, 0), Job("dummy-3", 3, Status.SUBMITTED, 0)]
    platform = mocker.MagicMock()
    platform.name = 'dummy'
    platform.serial_platform = mock.MagicMock()
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
    for i, package in enumerate(packages):  # Equivalent to valid_packages_to_submit but without the ghost jobs check etc.
        package.process_jobs_to_submit(jobs_id[i], False)
        for job in package.jobs:  # All jobs inside a package must have the same id.
            assert job.hold is False
            assert job.id == str(jobs_id[i])
            assert job.status == Status.SUBMITTED
            if not isinstance(package, JobPackageSimple):
                assert job.wrapper_name == "wrapped"
            else:
                assert job.wrapper_name is None
