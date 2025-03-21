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

import os
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.platforms.paramiko_platform import ParamikoPlatform
from autosubmit.platforms.psplatform import PsPlatform
from log.log import AutosubmitError


@pytest.fixture
def paramiko_platform():
    local_root_dir = TemporaryDirectory()
    config = {
        "LOCAL_ROOT_DIR": local_root_dir.name,
        "LOCAL_TMP_DIR": 'tmp'
    }
    platform = ParamikoPlatform(expid='a000', name='local', config=config)
    platform.job_status = {
        'COMPLETED': [],
        'RUNNING': [],
        'QUEUING': [],
        'FAILED': []
    }
    yield platform
    local_root_dir.cleanup()


@pytest.fixture
def ps_platform(tmpdir):
    tmp_path = Path(tmpdir)
    tmpdir.owner = tmp_path.owner()
    config = {
        "LOCAL_ROOT_DIR": str(tmpdir),
        "LOCAL_TMP_DIR": 'tmp',
        "PLATFORMS": {
            "pytest-ps": {
                "type": "ps",
                "host": "127.0.0.1",
                "user": tmpdir.owner,
                "project": "whatever",
                "scratch_dir": f"{Path(tmpdir).name}",
                "MAX_WALLCLOCK": "48:00",
                "DISABLE_RECOVERY_THREADS": True
            }
        }
    }
    platform = PsPlatform(expid='a000', name='local-ps', config=config)
    platform.host = '127.0.0.1'
    platform.user = tmpdir.owner
    platform.root_dir = Path(tmpdir) / "remote"
    platform.root_dir.mkdir(parents=True, exist_ok=True)
    yield platform, tmpdir


def test_paramiko_platform_constructor(paramiko_platform):
    platform = paramiko_platform
    assert platform.name == 'local'
    assert platform.expid == 'a000'
    assert platform.config["LOCAL_ROOT_DIR"] == platform.config["LOCAL_ROOT_DIR"]
    assert platform.header is None
    assert platform.wrapper is None
    assert len(platform.job_status) == 4


def test_check_all_jobs_send_command1_raises_autosubmit_error(mocker, paramiko_platform):
    mocker.patch('autosubmit.platforms.paramiko_platform.Log')
    mocker.patch('autosubmit.platforms.paramiko_platform.sleep')

    platform = paramiko_platform
    platform.get_checkAlljobs_cmd = mocker.Mock()
    platform.get_checkAlljobs_cmd.side_effect = ['ls']
    platform.send_command = mocker.Mock()
    ae = AutosubmitError(message='Test', code=123, trace='ERR!')
    platform.send_command.side_effect = ae
    as_conf = mocker.Mock()
    as_conf.get_copy_remote_logs.return_value = None
    job = mocker.Mock()
    job.id = 'TEST'
    job.name = 'TEST'
    with pytest.raises(AutosubmitError) as cm:
        platform.check_Alljobs(
            job_list=[(job, None)],
            as_conf=as_conf,
            retries=-1)
    assert cm.value.message == 'Some Jobs are in Unknown status'
    assert cm.value.code == 6008
    assert cm.value.trace is None


def test_check_all_jobs_send_command2_raises_autosubmit_error(mocker, paramiko_platform):
    mocker.patch('autosubmit.platforms.paramiko_platform.sleep')

    platform = paramiko_platform
    platform.get_checkAlljobs_cmd = mocker.Mock()
    platform.get_checkAlljobs_cmd.side_effect = ['ls']
    platform.send_command = mocker.Mock()
    ae = AutosubmitError(message='Test', code=123, trace='ERR!')
    platform.send_command.side_effect = [None, ae]
    platform._check_jobid_in_queue = mocker.Mock(return_value=False)
    as_conf = mocker.Mock()
    as_conf.get_copy_remote_logs.return_value = None
    job = mocker.Mock()
    job.id = 'TEST'
    job.name = 'TEST'
    job.status = Status.UNKNOWN
    platform.get_queue_status = mocker.Mock(side_effect=None)

    with pytest.raises(AutosubmitError) as cm:
        platform.check_Alljobs(
            job_list=[(job, None)],
            as_conf=as_conf,
            retries=1)
    assert cm.value.message == ae.error_message
    assert cm.value.code == 6000
    assert cm.value.trace is None


def test_ps_get_submit_cmd(ps_platform):
    platform, _ = ps_platform
    job = Job('TEST', 'TEST', Status.WAITING, 1)
    job.wallclock = '00:01'
    job.processors = 1
    job.section = 'dummysection'
    job.platform_name = 'pytest-ps'
    job.platform = platform
    job.script_name = "echo hello world"
    job.fail_count = 0
    command = platform.get_submit_cmd(job.script_name, job)
    assert job.wallclock_in_seconds == 60 * 1.3
    assert f"{job.script_name}" in command
    assert f"timeout {job.wallclock_in_seconds}" in command


def add_ssh_config_file(tmpdir, user, content):
    if not tmpdir.join(".ssh").exists():
        tmpdir.mkdir(".ssh")
    if user:
        ssh_config_file = tmpdir.join(f".ssh/config_{user}")
    else:
        ssh_config_file = tmpdir.join(".ssh/config")
    ssh_config_file.write(content)


@pytest.fixture(scope="function")
def generate_all_files(tmpdir):
    ssh_content = """
Host mn5-gpp
    User %change%
    HostName glogin1.bsc.es
    ForwardAgent yes
"""
    for user in [os.environ["USER"], "dummy-one"]:
        ssh_content_user = ssh_content.replace("%change%", user)
        add_ssh_config_file(tmpdir, user, ssh_content_user)
    return tmpdir


@pytest.mark.parametrize("user, env_ssh_config_defined",
                         [(os.environ["USER"], False),
                          ("dummy-one", True),
                          ("dummy-one", False),
                          ("not-exists", True),
                          ("not_exists", False)],
                         ids=["OWNER",
                              "SUDO USER(exists) + AS_ENV_CONFIG_SSH_PATH(defined)",
                              "SUDO USER(exists) + AS_ENV_CONFIG_SSH_PATH(not defined)",
                              "SUDO USER(not exists) + AS_ENV_CONFIG_SSH_PATH(defined)",
                              "SUDO USER(not exists) + AS_ENV_CONFIG_SSH_PATH(not defined)"])
def test_map_user_config_file(tmpdir, autosubmit_config, mocker, generate_all_files, user, env_ssh_config_defined):
    experiment_data = {
        "ROOTDIR": str(tmpdir),
        "PROJDIR": str(tmpdir),
        "LOCAL_TMP_DIR": str(tmpdir),
        "LOCAL_ROOT_DIR": str(tmpdir),
        "AS_ENV_CURRENT_USER": user,
    }
    if env_ssh_config_defined:
        experiment_data["AS_ENV_SSH_CONFIG_PATH"] = str(tmpdir.join(f".ssh/config_{user}"))
    as_conf = autosubmit_config(expid='a000', experiment_data=experiment_data)
    mocker.patch('autosubmitconfigparser.config.configcommon.AutosubmitConfig.is_current_real_user_owner',
                 os.environ["USER"] == user)
    platform = ParamikoPlatform(expid='a000', name='ps', config=experiment_data)
    platform._ssh_config = mocker.MagicMock()
    mocker.patch('os.path.expanduser',
                 side_effect=lambda x: x)  # Easier to test, and also not mess with the real user's config
    platform.map_user_config_file(as_conf)
    if not env_ssh_config_defined or not tmpdir.join(f".ssh/config_{user}").exists():
        assert platform._user_config_file == "~/.ssh/config"
    else:
        assert platform._user_config_file == str(tmpdir.join(f".ssh/config_{user}"))


def test_submit_job(mocker, autosubmit_config, tmpdir):
    experiment_data = {
        "ROOTDIR": str(tmpdir),
        "PROJDIR": str(tmpdir),
        "LOCAL_TMP_DIR": str(tmpdir),
        "LOCAL_ROOT_DIR": str(tmpdir),
        "AS_ENV_CURRENT_USER": "dummy",
    }
    platform = ParamikoPlatform(expid='a000', name='local', config=experiment_data)
    platform._ssh_config = mocker.MagicMock()
    platform.get_submit_cmd = mocker.MagicMock(returns="dummy")
    platform.send_command = mocker.MagicMock(returns="dummy")
    platform.get_submitted_job_id = mocker.MagicMock(return_value="10000")
    platform._ssh_output = "10000"
    job = Job("dummy", 10000, Status.SUBMITTED, 0)
    job._platform = platform
    job.platform_name = platform.name
    jobs_id = platform.submit_job(job, "dummy")
    assert jobs_id == 10000
