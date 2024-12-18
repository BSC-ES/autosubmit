import time
import pytest

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_packages import JobPackageVertical
from autosubmit.job.job_packager import JobPackager


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
