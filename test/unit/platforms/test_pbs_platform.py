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
from autosubmit.job.job_packages import JobPackageSimple
from autosubmit.log.log import AutosubmitCritical, AutosubmitError
from autosubmit.platforms.pbsplatform import PBSPlatform

"""Unit tests for the PBS platform."""


@pytest.fixture
def platform(autosubmit_config):
    expid = 'a000'
    as_conf = autosubmit_config(expid, experiment_data={})
    exp_path = Path(as_conf.basic_config.LOCAL_ROOT_DIR, expid)
    aslogs_dir = exp_path / as_conf.basic_config.LOCAL_TMP_DIR / as_conf.basic_config.LOCAL_ASLOG_DIR
    submit_platform_script = aslogs_dir / 'submit_local.sh'
    Path(submit_platform_script).touch()
    return PBSPlatform(expid='a000', name='local', config=as_conf.experiment_data)


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
        'type': 'SuperPBS',
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


def test_pbs_platform_submit_script_raises_autosubmit_critical_with_trace(mocker, platform):
    package = mocker.MagicMock()
    package.jobs.return_value = []
    valid_packages_to_submit = [
        package
    ]

    ae = AutosubmitError(message='violates resource limits', code=123, trace='ERR!')
    platform.submit_script = mocker.MagicMock(side_effect=ae)

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
        "PLATFORMS": {
            "pytest-pbs": {
                "type": "pbs",
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
def pbs_platform(as_conf):
    platform = PBSPlatform(expid="dummy-expid", name='pytest-pbs', config=as_conf.experiment_data)
    return platform


@pytest.fixture
def create_packages(as_conf, pbs_platform):
    simple_jobs_1 = [Job("dummy-1", 1, Status.SUBMITTED, 0)]
    simple_jobs_2 = [Job("dummy-1", 1, Status.SUBMITTED, 0),
                     Job("dummy-2", 2, Status.SUBMITTED, 0),
                     Job("dummy-3", 3, Status.SUBMITTED, 0)]
    simple_jobs_3 = [Job("dummy-1", 1, Status.SUBMITTED, 0),
                     Job("dummy-2", 2, Status.SUBMITTED, 0),
                     Job("dummy-3", 3, Status.SUBMITTED, 0)]
    for job in simple_jobs_1 + simple_jobs_2 + simple_jobs_3:
        job._platform = pbs_platform
        job._platform.name = pbs_platform.name
        job.platform_name = pbs_platform.name
        job.processors = 2
        job.section = "dummysection"
        job._init_runtime_parameters()
        job.wallclock = "00:01"
    packages = [
        JobPackageSimple(simple_jobs_1),
        JobPackageSimple(simple_jobs_2),
        JobPackageSimple(simple_jobs_3),
    ]
    return packages


def test_process_batch_ready_jobs_valid_packages_to_submit(mocker, pbs_platform, as_conf, create_packages):
    valid_packages_to_submit = create_packages
    failed_packages = []
    pbs_platform.get_jobs_id_by_job_name = mocker.MagicMock()
    pbs_platform.send_command = mocker.MagicMock()
    pbs_platform.submit_script = mocker.MagicMock()
    jobs_id = [1, [1, 2, 3], [1, 2, 3]]
    pbs_platform.get_jobs_id_by_job_name.return_value = jobs_id
    pbs_platform.submit_script.return_value = jobs_id
    pbs_platform.process_batch_ready_jobs(valid_packages_to_submit, failed_packages)
    for i, package in enumerate(valid_packages_to_submit):
        for job in package.jobs:
            assert job.hold is False
            assert job.id == str(jobs_id[i])
            assert job.status == Status.SUBMITTED
            assert job.wrapper_name is None
    assert failed_packages == []


def test_submit_job(mocker, pbs_platform):
    pbs_platform.get_submit_cmd = mocker.MagicMock(returns="dummy")
    pbs_platform.send_command = mocker.MagicMock(returns="dummy")
    pbs_platform._ssh_output = "10000"
    job = Job("dummy", 10000, Status.SUBMITTED, 0)
    job._platform = pbs_platform
    job.platform_name = pbs_platform.name
    jobs_id = pbs_platform.submit_job(job, "dummy")
    assert not jobs_id
    job.x11 = True
    jobs_id = pbs_platform.submit_job(job, "dummy")
    assert jobs_id == 10000
    job.workflow_commit = "dummy"
    jobs_id = pbs_platform.submit_job(job, "dummy")
    assert jobs_id == 10000
    pbs_platform._ssh_output = "10000\n"
    jobs_id = pbs_platform.submit_job(job, "dummy")
    assert jobs_id == 10000


def test_get_header(mocker, pbs_platform):
    job = Job("dummy", 10000, Status.SUBMITTED, 0)

    job.het = dict()
    job.het["HETSIZE"] = 0

    parameters = dict()

    parameters['TASKS'] = '0'
    parameters['NODES'] = '0'
    parameters['MEMORY'] = ''
    parameters['NUMTHREADS'] = '0'
    parameters['RESERVATION'] = ''
    parameters['CURRENT_QUEUE'] = ''
    parameters['CURRENT_PROJ'] = ''
    parameters['MEMORY_PER_TASK'] = ''
    parameters['CUSTOM_DIRECTIVES'] = ''

    pbs_platform.header.SERIAL = '%OUT_LOG_DIRECTIVE%%ERR_LOG_DIRECTIVE%%QUEUE_DIRECTIVE%%TASKS_PER_NODE_DIRECTIVE%%THREADS_PER_TASK_DIRECTIVE%%CUSTOM_DIRECTIVES%%ACCOUNT_DIRECTIVE%%NODES_DIRECTIVE%%RESERVATION_DIRECTIVE%%MEMORY_DIRECTIVE%%MEMORY_PER_TASK_DIRECTIVE%'
    assert pbs_platform.get_header(job, parameters) == 'dummy.cmd.outdummy.cmd.err'

    parameters['TASKS'] = '2'
    parameters['NODES'] = '2'
    parameters['MEMORY'] = '100kb'
    parameters['NUMTHREADS'] = '2'
    parameters['RESERVATION'] = 'x'
    parameters['CURRENT_QUEUE'] = 'debug'
    parameters['CURRENT_PROJ'] = 'project'
    parameters['MEMORY_PER_TASK'] = '100kb'
    parameters['CUSTOM_DIRECTIVES'] = 'custom'

    pbs_platform.header.SERIAL = '%OUT_LOG_DIRECTIVE%%ERR_LOG_DIRECTIVE%%QUEUE_DIRECTIVE%%TASKS_PER_NODE_DIRECTIVE%%THREADS_PER_TASK_DIRECTIVE%%CUSTOM_DIRECTIVES%%ACCOUNT_DIRECTIVE%%NODES_DIRECTIVE%%RESERVATION_DIRECTIVE%%MEMORY_DIRECTIVE%%MEMORY_PER_TASK_DIRECTIVE%'
    assert pbs_platform.get_header(job, parameters) == 'dummy.cmd.outdummy.cmd.errPBS -q debug:mpiprocs=2:ompthreads=2c\nu\ns\nt\no\nmPBS -W group_list=projectPBS -l select=2PBS -W x=x:mem=100kb:vmem=100kb'



# def test_submit_script(mocker, tmp_path, pbs_platform):
#     pbs_platform._ftpChannel = mocker.MagicMock(returns="dummy")
#     pbs_platform.transport = mocker.MagicMock(returns="dummy")
#
#     pbs_platform._ssh_output = tmp_path / 'test/script.sh'
#     pbs_platform.root_dir = tmp_path
#
#     pbs_platform.remote_log_dir = tmp_path / 'test/'
#     pbs_platform.remote_log_dir.mkdir()
#
#     pbs_platform._submit_script_path = tmp_path / 'test/script.sh'
#     pbs_platform._submit_script_path.touch()
#
#     mocker.patch('autosubmit.platforms.paramiko_platform.select', side_effect=[[], '', ''])
#
#     assert pbs_platform.submit_script()

