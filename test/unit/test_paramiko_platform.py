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
from io import BytesIO, TextIOWrapper
from pathlib import Path
from queue import Queue
from tempfile import TemporaryDirectory

import paramiko
import pytest

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_package_persistence import JobPackagePersistence
from autosubmit.job.job_packages import JobPackageSimple, JobPackageVertical, JobPackageHorizontal
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


class DummyInBuffer(Queue):
    """A dummy input buffer that extends the Queue class."""

    def __len__(self):
        """Return the size of the queue."""
        return self.qsize()


class DummyTransport:
    """A dummy transport class to simulate an SSH transport."""

    @staticmethod
    def is_active() -> bool:
        return True

    @staticmethod
    def open_session() -> "DummyChannel":
        return DummyChannel()

    @staticmethod
    def close() -> None:
        pass


class DummyChannel:
    """A dummy channel class to simulate an SSH channel."""

    def __init__(self, stdout_bytes: bytes = b"hello\n", stderr_bytes: bytes = b"") -> None:
        """
        Initialize the dummy channel.

        :param stdout_bytes: Bytes to simulate standard output.
        :param stderr_bytes: Bytes to simulate standard error.
        """
        self._r, self._w = os.pipe()
        self._stdout_buf = BytesIO(stdout_bytes)
        self._stderr_buf = BytesIO(stderr_bytes)
        self.in_buffer = DummyInBuffer()
        if stdout_bytes:
            self.in_buffer.put(stdout_bytes)

        self.timeout = None
        self.closed = False

    def fileno(self) -> int:
        """Return the file descriptor for the channel."""
        return self._r

    def recv_ready(self) -> bool:
        """Check if there is data ready to be received."""
        return self._stdout_buf.tell() < len(self._stdout_buf.getvalue())

    def recv(self, n: int) -> bytes:
        """Receive data from the channel."""
        return self._stdout_buf.read(n)

    def recv_stderr_ready(self) -> bool:
        """Check if there is stderr data ready to be received."""
        return self._stderr_buf.tell() < len(self._stderr_buf.getvalue())

    def recv_stderr(self, n: int) -> bytes:
        """Receive stderr data from the channel."""
        return self._stderr_buf.read(n)

    @staticmethod
    def get_pty() -> None:
        """Simulate getting a pseudo-terminal."""
        print("[dummy] get_pty()")

    @staticmethod
    def update_environment(env: dict) -> None:
        """Update the environment variables."""
        print(f"[dummy] update_environment({env!r})")

    def settimeout(self, t: float) -> None:
        """Set the timeout for the channel."""
        print(f"[dummy] settimeout({t})")
        self.timeout = t

    def exec_command(self, command: str) -> None:
        """Execute a command on the channel."""
        print(f"[dummy] exec_command({command!r})")
        out = f"{command}".encode()
        os.write(self._w, out)
        os.close(self._w)
        self._stdout_buf = BytesIO(out)
        self._stderr_buf = BytesIO(b"")
        self.closed = True

    def makefile_stdin(self, mode: str = "wb", bufsize: int = -1) -> BytesIO:
        """Create a file-like object for stdin."""
        f = BytesIO()
        f.channel = self
        return f

    def makefile(self, mode: str = "r", bufsize: int = -1) -> TextIOWrapper:
        """Create a file-like object for stdout."""
        self._stdout_buf.seek(0)
        f = TextIOWrapper(self._stdout_buf, encoding="utf-8")
        f.channel = self
        return f

    def makefile_stderr(self, mode: str = "r", bufsize: int = -1) -> TextIOWrapper:
        """Create a file-like object for stderr."""
        self._stderr_buf.seek(0)
        f = TextIOWrapper(self._stderr_buf, encoding="utf-8")
        f.channel = self
        return f

    @staticmethod
    def shutdown_write() -> None:
        """Simulate shutting down the write side of the channel."""
        pass

    def close(self) -> None:
        """Close the channel."""
        try:
            os.close(self._r)
        except OSError:
            pass
        self._stdout_buf.close()
        self._stderr_buf.close()
        self.closed = True


class DummySFTPClient:
    """A dummy SFTP client to simulate SFTP operations."""

    def __init__(self) -> None:
        print("[dummy sftp] session opened")

    @staticmethod
    def listdir(path: str) -> list[str]:
        print(f"[dummy sftp] listdir({path!r})")
        return ["file1.txt", "file2.log", "dir_a"]

    @staticmethod
    def open(filename: str, mode: str = "r") -> BytesIO:
        print(f"[dummy sftp] open({filename!r}, mode={mode!r})")
        data = b"dummy content of " + filename.encode()
        return BytesIO(data)

    @staticmethod
    def get(remotepath: str, localpath: str) -> None:
        print(f"[dummy sftp] get({remotepath!r} -> {localpath!r})")
        with open(localpath, "wb") as f:
            f.write(b"[dummy data]")

    @staticmethod
    def put(localpath: str, remotepath: str) -> None:
        print(f"[dummy sftp] put({localpath!r} -> {remotepath!r})")

    @staticmethod
    def remove(path: str) -> None:
        print(f"[dummy sftp] remove({path!r})")

    @staticmethod
    def mkdir(path: str) -> None:
        print(f"[dummy sftp] mkdir({path!r})")

    @staticmethod
    def rmdir(path: str) -> None:
        print(f"[dummy sftp] rmdir({path!r})")

    @staticmethod
    def close() -> None:
        print("[dummy sftp] session closed")


@pytest.fixture
def paramiko_connected_platform(paramiko_platform):
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client._transport = DummyTransport()
    paramiko_platform._ssh = client
    paramiko_platform.transport = client._transport
    paramiko_platform._ftpChannel = DummySFTPClient()
    paramiko_platform.connected = True
    return paramiko_platform


def test_send_command_success(paramiko_connected_platform, mocker):
    platform = paramiko_connected_platform
    platform._prepare_channel = mocker.Mock()
    result = platform.send_command("hello", ignore_log=False, x11=False)
    assert result
    assert platform._ssh_output == "hello"


@pytest.fixture
def create_packages(autosubmit_config, paramiko_connected_platform):
    as_conf = autosubmit_config(expid='test')
    simple_jobs = [Job("dummy-1", 1, Status.SUBMITTED, 0)]
    vertical_jobs = [Job("dummy-1", 1, Status.SUBMITTED, 0), Job("dummy-2", 2, Status.SUBMITTED, 0),
                     Job("dummy-3", 3, Status.SUBMITTED, 0)]
    horizontal_jobs = [Job("dummy-1", 1, Status.SUBMITTED, 0), Job("dummy-2", 2, Status.SUBMITTED, 0),
                       Job("dummy-3", 3, Status.SUBMITTED, 0)]
    for job in simple_jobs + vertical_jobs + horizontal_jobs:
        job._platform = paramiko_connected_platform
        job._platform.name = paramiko_connected_platform.name
        job.platform_name = paramiko_connected_platform.name
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

def test_submit_ready_jobs(paramiko_connected_platform, autosubmit_config, create_job_list, create_packages):
    as_conf = autosubmit_config(expid='test')
    job_list = create_job_list
    packages_persistence = JobPackagePersistence("test")
    packages_to_submit = create_packages
    paramiko_connected_platform.submit_ready_jobs(as_conf, job_list, packages_persistence, packages_to_submit, False, False, False)
