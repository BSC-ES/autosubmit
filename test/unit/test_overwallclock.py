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

from datetime import datetime, timedelta

import pytest

from autosubmit.autosubmit import Autosubmit
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_list import JobList
from autosubmit.job.job_list_persistence import JobListPersistencePkl
from autosubmit.job.job_packages import JobPackageSimple, JobPackageVertical, JobPackageHorizontal
from autosubmit.platforms.psplatform import PsPlatform
from autosubmit.platforms.slurmplatform import SlurmPlatform
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory


@pytest.fixture
def setup_as_conf(autosubmit_config, tmpdir):
    exp_data = {
        "WRAPPERS": {
            "WRAPPERS": {
                "JOBS_IN_WRAPPER": "dummysection"
            }
        },
        "LOCAL_ROOT_DIR": f"{tmpdir.strpath}",
        "LOCAL_TMP_DIR": f'{tmpdir.strpath}',
        "LOCAL_ASLOG_DIR": f"{tmpdir.strpath}",
        "PLATFORMS": {
            "PYTEST-UNSUPPORTED": {
                "TYPE": "unknown",
                "host": "",
                "user": "",
                "project": "",
                "scratch_dir": "",
                "MAX_WALLCLOCK": "",
                "DISABLE_RECOVERY_THREADS": True
            }
        },

    }
    as_conf = autosubmit_config("random-id", exp_data)
    return as_conf


@pytest.fixture
def new_job_list(setup_as_conf, tmpdir):
    job_list = JobList("random-id", setup_as_conf, YAMLParserFactory(),
                       JobListPersistencePkl())

    return job_list


@pytest.fixture
def new_platform_mock(mocker, tmpdir):
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


def setup_jobs(dummy_jobs, new_platform_mock):
    for job in dummy_jobs:
        job._platform = new_platform_mock
        job.processors = 2
        job.section = "dummysection"
        job._init_runtime_parameters()
        job.wallclock = "00:01"
        job.start_time = datetime.now() - timedelta(minutes=1)


@pytest.mark.parametrize(
    "initial_status, expected_status",
    [
        (Status.SUBMITTED, Status.SUBMITTED),
        (Status.QUEUING, Status.QUEUING),
        (Status.RUNNING, Status.RUNNING),
        (Status.FAILED, Status.FAILED),
        (Status.COMPLETED, Status.COMPLETED),
        (Status.HELD, Status.HELD),
        (Status.UNKNOWN, Status.UNKNOWN),
    ],
    ids=["Submitted", "Queuing", "Running", "Failed", "Completed", "Held", "No packages"]
)
def test_check_wrapper_stored_status(setup_as_conf, new_job_list, new_platform_mock, initial_status, expected_status):
    dummy_jobs = [Job("dummy-1", 1, initial_status, 0), Job("dummy-2", 2, initial_status, 0),
                  Job("dummy-3", 3, initial_status, 0)]
    setup_jobs(dummy_jobs, new_platform_mock)
    new_job_list.jobs = dummy_jobs
    if dummy_jobs[0].status != Status.UNKNOWN:
        new_job_list.packages_dict = {"dummy_wrapper": dummy_jobs}
    new_job_list = Autosubmit.check_wrapper_stored_status(setup_as_conf, new_job_list, "03:30")
    assert new_job_list is not None
    if dummy_jobs[0].status != Status.UNKNOWN:
        assert new_job_list.job_package_map[dummy_jobs[0].id].status == expected_status


def test_parse_time(new_platform_mock):
    job = Job("dummy-1", 1, Status.SUBMITTED, 0)
    setup_jobs([job], new_platform_mock)
    assert job.parse_time("0000") is None
    assert job.parse_time("00:01") == timedelta(seconds=60)


def test_is_over_wallclock(new_platform_mock):
    job = Job("dummy-1", 1, Status.SUBMITTED, 0)
    setup_jobs([job], new_platform_mock)
    job.wallclock = "00:01"
    assert job.is_over_wallclock() is False
    job.start_time = datetime.now() - timedelta(minutes=2)
    assert job.is_over_wallclock() is True


@pytest.mark.parametrize(
    "platform_class, platform_name",
    [(SlurmPlatform, "Slurm"), (PsPlatform, "PS"), (PsPlatform, "PJM")],
    ids=["SlurmPlatform", "PsPlatform", "PjmPlatform"]
)
def test_platform_job_is_over_wallclock(setup_as_conf, new_platform_mock, platform_class, platform_name, mocker):
    platform_instance = platform_class("dummy", f"{platform_name}-dummy", setup_as_conf.experiment_data)
    job = Job("dummy-1", 1, Status.RUNNING, 0)
    setup_jobs([job], platform_instance)
    job.wallclock = "00:01"
    job_status = platform_instance.job_is_over_wallclock(job, Status.RUNNING)
    assert job_status == Status.RUNNING
    job.start_time = datetime.now() - timedelta(minutes=2)
    job_status = platform_instance.job_is_over_wallclock(job, Status.RUNNING)
    assert job_status == Status.FAILED
    # check platform_instance is called
    platform_instance.send_command = mocker.MagicMock()
    job_status = platform_instance.job_is_over_wallclock(job, Status.RUNNING, True)
    assert job_status == Status.FAILED
    platform_instance.send_command.assert_called_once()
    platform_instance.cancel_cmd = None
    platform_instance.send_command = mocker.MagicMock()
    platform_instance.job_is_over_wallclock(job, Status.RUNNING, True)
    platform_instance.send_command.assert_not_called()


@pytest.mark.parametrize(
    "platform_class, platform_name",
    [(SlurmPlatform, "Slurm"), (PsPlatform, "PS"), (PsPlatform, "PJM")],
    ids=["SlurmPlatform", "PsPlatform", "PjmPlatform"]
)
def test_platform_job_is_over_wallclock_force_failure(setup_as_conf, new_platform_mock, platform_class, platform_name,
                                                      mocker):
    platform_instance = platform_class("dummy", f"{platform_name}-dummy", setup_as_conf.experiment_data)
    job = Job("dummy-1", 1, Status.RUNNING, 0)
    setup_jobs([job], platform_instance)
    job.start_time = datetime.now() - timedelta(minutes=2)
    job.platform.get_completed_files = mocker.MagicMock(side_effect=Exception("Error"))
    job_status = platform_instance.job_is_over_wallclock(job, Status.RUNNING, True)
    assert job_status == Status.FAILED
