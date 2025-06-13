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
from getpass import getuser
from pathlib import Path
from random import randrange
from tempfile import TemporaryDirectory, gettempdir

import paramiko
import pytest
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.sftp import DockerContainer

from autosubmit.platforms.paramiko_platform import ParamikoPlatform
from autosubmit.platforms.psplatform import PsPlatform

"""Integration tests for the paramiko platform.

Note that tests will start and destroy an SSH server. For unit tests, see ``paramiko_platform.py``
in the ``test/unit`` directory."""


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
    platform.remote_log_dir = platform.root_dir / 'LOG_a000'
    yield platform, tmpdir


@pytest.mark.docker
@pytest.mark.parametrize('filename, check', [
    ('test1', True),
    ('sub/test2', True)
], ids=['filename', 'filename_long_path'])
def test_send_file(mocker, filename, ps_platform, check):
    """This test opens an SSH connection (via sftp) and sends a file to the remote location.

    It launches a Docker Image using testcontainers library.
    """
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

    # NOTE: because the test will run inside a container, with a different UID and GID,
    #       sftp would not be able to write to the folder in the temporary directory
    #       created by another user uid/gid (inside the container the user will be nobody).
    from_env = os.environ.get("PYTEST_DEBUG_TEMPROOT")
    temproot = Path(from_env or gettempdir()).resolve()
    user = getuser() or "unknown"
    rootdir = temproot / f"pytest-of-{user}"

    # To write in the /tmp (sticky bit, different uid/gid), reset it later (default pytest is 700)
    os.system(f'chmod 777 -R {str(rootdir)}')

    ssh_port = randrange(2500, 3000)

    try:
        image = 'lscr.io/linuxserver/openssh-server:latest'
        with DockerContainer(image=image, remove=True, hostname='openssh-server') \
                .with_env('TZ', 'Etc/UTC') \
                .with_env('SUDO_ACCESS', 'false') \
                .with_env('USER_NAME', user) \
                .with_env('USER_PASSWORD', 'password') \
                .with_env('PASSWORD_ACCESS', 'true') \
                .with_bind_ports(2222, ssh_port) \
                .with_volume_mapping('/tmp', '/tmp', mode='rw') as container:
            wait_for_logs(container, 'sshd is listening on port 2222')
            _ssh = paramiko.SSHClient()
            _ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            _ssh.connect(hostname=platform.host, username=platform.user, password='password', port=ssh_port)
            platform._ftpChannel = paramiko.SFTPClient.from_transport(_ssh.get_transport(), window_size=pow(4, 12),
                                                                      max_packet_size=pow(4, 12))
            platform._ftpChannel.get_channel().settimeout(120)
            platform.connected = True
            platform.get_send_file_cmd = mocker.Mock()
            platform.get_send_file_cmd.return_value = 'ls'
            platform.send_command = mocker.Mock()

            platform.send_file(filename)
            assert check == (remote_dir / filename).exists()
    finally:
        os.system(f'chmod 700 -R {str(rootdir)}')
