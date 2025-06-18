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


from pathlib import Path

import pytest

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_packages import JobPackageSimple, JobPackageVertical, JobPackageHorizontal
from autosubmit.platforms.slurmplatform import SlurmPlatform
from log.log import AutosubmitCritical, AutosubmitError

"""Unit tests for the Slurm platform."""


@pytest.fixture
def platform(autosubmit_config):
    expid = 'a000'
    as_conf = autosubmit_config(expid, experiment_data={})
    exp_path = Path(as_conf.basic_config.LOCAL_ROOT_DIR, expid)
    aslogs_dir = exp_path / as_conf.basic_config.LOCAL_TMP_DIR / as_conf.basic_config.LOCAL_ASLOG_DIR
    submit_platform_script = aslogs_dir / 'submit_local.sh'
    Path(submit_platform_script).touch()
    return SlurmPlatform(expid='a000', name='local', config=as_conf.experiment_data)


def test_properties(platform):
    props = {
        'name': 'foo',
        'host': 'localhost1',
        'user': 'sam',
        'project': 'proj1',
        'budget': 100,
        'reservation': 1,
        'exclusivity': True,
        'hyperthreading': True,
        'type': 'SuperSlurm',
        'scratch': '/scratch/1',
        'project_dir': '/proj1',
        'root_dir': '/root_1',
        'partition': 'inter',
        'queue': 'prio1'
    }
    for prop, value in props.items():
        setattr(platform, prop, value)
    for prop, value in props.items():
        assert value == getattr(platform, prop)


def test_slurm_platform_submit_script_raises_autosubmit_critical_with_trace(mocker, platform):
    package = mocker.MagicMock()
    package.jobs.return_value = []
    valid_packages_to_submit = [
        package
    ]

    ae = AutosubmitError(message='invalid partition', code=123, trace='ERR!')
    platform.submit_Script = mocker.MagicMock(side_effect=ae)

    # AS will handle the AutosubmitError above, but then raise an AutosubmitCritical.
    # This new error won't contain all the info from the upstream error.
    with pytest.raises(AutosubmitCritical) as cm:
        platform.process_batch_ready_jobs(
            valid_packages_to_submit=valid_packages_to_submit,
            failed_packages=[]
        )

    # AS will handle the error and then later will raise another error message.
    # But the AutosubmitError object we created will have been correctly used
    # without raising any exceptions (such as AttributeError).
    assert cm.value.message != ae.message


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
    vertical_jobs = [Job("dummy-1", 1, Status.SUBMITTED, 0), Job("dummy-2", 2, Status.SUBMITTED, 0),
                     Job("dummy-3", 3, Status.SUBMITTED, 0)]
    horizontal_jobs = [Job("dummy-1", 1, Status.SUBMITTED, 0), Job("dummy-2", 2, Status.SUBMITTED, 0),
                       Job("dummy-3", 3, Status.SUBMITTED, 0)]
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
