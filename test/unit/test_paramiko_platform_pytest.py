import pytest

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.platforms.paramiko_platform import ParamikoPlatform
import os
import autosubmitconfigparser.config.configcommon

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
    mocker.patch('autosubmitconfigparser.config.configcommon.AutosubmitConfig.is_current_real_user_owner', os.environ["USER"] == user)
    platform = ParamikoPlatform(expid='a000', name='ps', config=experiment_data)
    platform._ssh_config = mocker.MagicMock()
    mocker.patch('os.path.expanduser', side_effect=lambda x: x)  # Easier to test, and also not mess with the real user's config
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
