import pytest
from tempfile import TemporaryDirectory

from pathlib import Path

from autosubmit.job.job_common import Status
from autosubmit.job.job import Job
from autosubmit.platforms.paramiko_platform import ParamikoPlatform
from autosubmit.platforms.psplatform import PsPlatform
from log.log import AutosubmitError
import paramiko


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
    tmpdir.owner = Path(tmpdir).owner()
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


def test_check_Alljobs_send_command1_raises_autosubmit_error(mocker, paramiko_platform):
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


def test_check_Alljobs_send_command2_raises_autosubmit_error(mocker, paramiko_platform):
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


@pytest.mark.skip(reason="Skipping this test until Github transition is complete")
@pytest.mark.parametrize('filename, check', [
    ('test1', True),
    ('anotherdir/test2', True)
], ids=['filename', 'filename_long_path'])
def test_send_file(mocker, ps_platform, filename, check):
    platform, tmp_dir = ps_platform
    remote_dir = Path(platform.root_dir) / f'LOG_{platform.expid}'
    remote_dir.mkdir(parents=True, exist_ok=True)
    Path(platform.tmp_path).mkdir(parents=True, exist_ok=True)
    # generate file
    if "/" in filename:
        filename_dir = Path(filename).parent
        (Path(platform.tmp_path) / filename_dir).mkdir(parents=True, exist_ok=True)
        filename = Path(filename).name
    with open(Path(platform.tmp_path) / filename, 'w') as f:
        f.write('test')
    _ssh = paramiko.SSHClient()
    _ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    _ssh.connect(hostname=platform.host, username=platform.user)
    platform._ftpChannel = paramiko.SFTPClient.from_transport(_ssh.get_transport(), window_size=pow(4, 12),
                                                          max_packet_size=pow(4, 12))
    platform._ftpChannel.get_channel().settimeout(120)
    platform.connected = True
    platform.get_send_file_cmd = mocker.Mock()
    platform.get_send_file_cmd.return_value = 'ls'
    platform.send_command = mocker.Mock()
    platform.send_file(filename)
    assert check == (remote_dir / filename).exists()


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
