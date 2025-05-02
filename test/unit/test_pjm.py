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
from autosubmit.platforms.pjmplatform import PJMPlatform
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory

_EXPECTED_COMPLETED_JOBS = ["167727"]
_EXPECTED_OUTPUT = """JOB_ID     ST  REASON                         
167727     EXT COMPLETED               
167728     RNO -               
167729     RNE -               
167730     RUN -               
167732     ACC -               
167733     QUE -               
167734     RNA -               
167735     RNP -               
167736     HLD ASHOLD               
167737     ERR -               
167738     CCL -               
167739     RJT -  
"""


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
def pjm_platform(as_conf):
    platform = PJMPlatform(expid="dummy-expid", name='pytest-slurm', config=as_conf.experiment_data)
    return platform


@pytest.fixture
def create_packages(as_conf, pjm_platform):
    simple_jobs = [Job("dummy-1", 1, Status.SUBMITTED, 0)]
    vertical_jobs = [Job("dummy-1", 1, Status.SUBMITTED, 0), Job("dummy-2", 2, Status.SUBMITTED, 0),
                     Job("dummy-3", 3, Status.SUBMITTED, 0)]
    horizontal_jobs = [Job("dummy-1", 1, Status.SUBMITTED, 0), Job("dummy-2", 2, Status.SUBMITTED, 0),
                       Job("dummy-3", 3, Status.SUBMITTED, 0)]
    for job in simple_jobs + vertical_jobs + horizontal_jobs:
        job._platform = pjm_platform
        job._platform.name = pjm_platform.name
        job.platform_name = pjm_platform.name
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


@pytest.fixture
def remote_platform(autosubmit_config, autosubmit):
    as_conf = autosubmit_config("a000", {
        'DEFAULT': {
            'HPCARCH': 'ARM'
        }
    })

    yml_file = Path(__file__).resolve().parents[1] / "files/fake-jobs.yml"
    factory = YAMLParserFactory()
    parser = factory.create_parser()
    parser.data = parser.load(yml_file)
    as_conf.experiment_data.update(parser.data)
    yml_file = Path(__file__).resolve().parents[1] / "files/fake-platforms.yml"
    factory = YAMLParserFactory()
    parser = factory.create_parser()
    parser.data = parser.load(yml_file)
    as_conf.experiment_data.update(parser.data)

    submitter = autosubmit._get_submitter(as_conf)
    submitter.load_platforms(as_conf)
    return submitter.platforms['ARM']


def test_parse_all_jobs_output(remote_platform):
    """Test parsing of all jobs output."""
    running_jobs = ["167728", "167729", "167730"]
    queued_jobs = ["167732", "167733", "167734", "167735", "167736"]
    failed_jobs = ["167737", "167738", "167739"]
    jobs_that_arent_listed = ["3442432423", "238472364782", "1728362138712"]
    for job_id in _EXPECTED_COMPLETED_JOBS:
        assert remote_platform.parse_Alljobs_output(_EXPECTED_OUTPUT, job_id) in remote_platform.job_status[
            "COMPLETED"]
    for job_id in failed_jobs:
        assert remote_platform.parse_Alljobs_output(_EXPECTED_OUTPUT, job_id) in remote_platform.job_status[
            "FAILED"]
    for job_id in queued_jobs:
        assert remote_platform.parse_Alljobs_output(_EXPECTED_OUTPUT, job_id) in remote_platform.job_status[
            "QUEUING"]
    for job_id in running_jobs:
        assert remote_platform.parse_Alljobs_output(_EXPECTED_OUTPUT, job_id) in remote_platform.job_status[
            "RUNNING"]
    for job_id in jobs_that_arent_listed:
        assert remote_platform.parse_Alljobs_output(_EXPECTED_OUTPUT, job_id) == []


def test_get_submitted_job_id(remote_platform):
    """Test parsing of submitted job id."""
    submitted_ok = "[INFO] PJM 0000 pjsub Job 167661 submitted."
    submitted_fail = "[ERR.] PJM 0057 pjsub node=32 is greater than the upper limit (24)."
    output = remote_platform.get_submitted_job_id(submitted_ok)
    assert output == [167661]
    output = remote_platform.get_submitted_job_id(submitted_fail)
    assert output == []


def test_parse_queue_reason(remote_platform):
    """Test parsing of queue reason."""
    output = remote_platform.parse_queue_reason(_EXPECTED_OUTPUT, _EXPECTED_COMPLETED_JOBS[0])
    assert output == "COMPLETED"


def test_process_batch_ready_jobs_valid_packages_to_submit(mocker, pjm_platform, as_conf, create_packages):
    valid_packages_to_submit = create_packages
    failed_packages = []
    pjm_platform.get_jobid_by_jobname = mocker.MagicMock()
    pjm_platform.send_command = mocker.MagicMock()
    pjm_platform.submit_Script = mocker.MagicMock()
    jobs_id = [1, 2, 3]
    pjm_platform.submit_Script.return_value = jobs_id
    pjm_platform.process_batch_ready_jobs(valid_packages_to_submit, failed_packages)
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
