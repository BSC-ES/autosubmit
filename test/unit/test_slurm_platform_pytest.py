import pytest
from fontTools.misc.cython import returns

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_packages import JobPackageSimple, JobPackageVertical, JobPackageHorizontal
from autosubmit.platforms.slurmplatform import SlurmPlatform


@pytest.fixture
def as_conf(autosubmit_config, tmpdir):
    exp_data = {
        "WRAPPERS": {
            "WRAPPERS": {
                "JOBS_IN_WRAPPER": "dummysection"
            }
        },
        "PLATFORMS": {
            "pytest-slurm": {
                "type": "slurm",
                "host": "localhost",
                "user": "user",
                "project": "project",
                "scratch_dir": "/scratch",
                "QUEUE": "queue",
                "ADD_PROJECT_TO_HOST": False,
                "MAX_WALLCLOCK": "00:01",
                "TEMP_DIR": "",
                "MAX_PROCESSORS": 99999,
            },
        },
        "LOCAL_ROOT_DIR": str(tmpdir),
        "LOCAL_TMP_DIR": str(tmpdir),
        "LOCAL_PROJ_DIR": str(tmpdir),
        "LOCAL_ASLOG_DIR": str(tmpdir),
    }
    as_conf = autosubmit_config("dummy-expid", exp_data)
    return as_conf


@pytest.fixture
def slurm_platform(as_conf):
    platform = SlurmPlatform(expid="dummy-expid", name='pytest-slurm', config=as_conf.experiment_data)
    return platform


@pytest.fixture
def create_packages(as_conf, slurm_platform):
    simple_jobs = [Job("dummy-1", 1, Status.SUBMITTED, 0)]
    vertical_jobs = [Job("dummy-1", 1, Status.SUBMITTED, 0), Job("dummy-2", 2, Status.SUBMITTED, 0), Job("dummy-3", 3, Status.SUBMITTED, 0)]
    horizontal_jobs = [Job("dummy-1", 1, Status.SUBMITTED, 0), Job("dummy-2", 2, Status.SUBMITTED, 0), Job("dummy-3", 3, Status.SUBMITTED, 0)]
    for job in simple_jobs + vertical_jobs + horizontal_jobs:
        job._platform = slurm_platform
        job._platform.name = slurm_platform.name
        job.platform_name = slurm_platform.name
        job.processors = 2
        job.section = "dummysection"
        job._init_runtime_parameters()
        job.wallclock = "00:01"
    packages = [
        JobPackageSimple(simple_jobs),
        JobPackageVertical(vertical_jobs, configuration=as_conf),
        JobPackageHorizontal(horizontal_jobs, configuration=as_conf),
    ]
    for package in packages:
        if not isinstance(package, JobPackageSimple):
            package._name = "wrapped"
    return packages


def test_process_batch_ready_jobs_valid_packages_to_submit(mocker, slurm_platform, as_conf, create_packages):
    valid_packages_to_submit = create_packages
    failed_packages = []
    slurm_platform.get_jobid_by_jobname = mocker.MagicMock()
    slurm_platform.send_command = mocker.MagicMock()
    slurm_platform.submit_Script = mocker.MagicMock()
    jobs_id = [1, 2, 3]
    slurm_platform.submit_Script.return_value = jobs_id
    slurm_platform.process_batch_ready_jobs(valid_packages_to_submit, failed_packages)
    for i, package in enumerate(valid_packages_to_submit):
        for job in package.jobs:
            assert job.hold is False
            assert job.id == str(jobs_id[i])
            assert job.status == Status.SUBMITTED
            if not isinstance(package, JobPackageSimple):
                assert job.wrapper_name == "wrapped"
            else:
                assert job.wrapper_name is None
    assert failed_packages == []


def test_submit_job(mocker, slurm_platform):
    slurm_platform.get_submit_cmd = mocker.MagicMock(returns="dummy")
    slurm_platform.send_command = mocker.MagicMock(returns="dummy")
    slurm_platform._ssh_output = "10000"
    job = Job("dummy", 10000, Status.SUBMITTED, 0)
    job._platform = slurm_platform
    job.platform_name = slurm_platform.name
    jobs_id = slurm_platform.submit_job(job, "dummy")
    assert not jobs_id
    job.x11 = True
    jobs_id = slurm_platform.submit_job(job, "dummy")
    assert jobs_id == 10000
    job.workflow_commit = "dummy"
    jobs_id = slurm_platform.submit_job(job, "dummy")
    assert jobs_id == 10000
    slurm_platform._ssh_output = "10000\n"
    jobs_id = slurm_platform.submit_job(job, "dummy")
    assert jobs_id == 10000
