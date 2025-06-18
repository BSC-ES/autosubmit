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
from tempfile import gettempdir

import pytest
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.sftp import DockerContainer

from autosubmit.platforms.psplatform import PsPlatform
from log.log import AutosubmitError

"""Integration tests for the paramiko platform.

Note that tests will start and destroy an SSH server. For unit tests, see ``paramiko_platform.py``
in the ``test/unit`` directory."""

_DOCKER_IMAGE = 'lscr.io/linuxserver/openssh-server:latest'
_DOCKER_PASSWORD = 'password'


@pytest.fixture(scope='module', autouse=True)
def ssh_config() -> None:
    # Paramiko platform relies on parsing the SSH config file, failing if it does not exist.
    ssh_config = Path('~/.ssh/config').expanduser()
    delete_ssh_config = False
    if not ssh_config.exists():
        ssh_config.parent.mkdir(parents=True, exist_ok=True)
        ssh_config.touch(exist_ok=False)
        delete_ssh_config = True
    yield ssh_config
    # Now we remove so that the user can create one if s/he so desires.
    if delete_ssh_config:
        ssh_config.unlink(missing_ok=True)


@pytest.mark.docker
@pytest.mark.parametrize('filename, check', [
    ('test1', True),
    ('sub/test2', True)
], ids=['filename', 'filename_long_path'])
def test_send_file(filename: str, check: bool, mocker, tmp_path, ps_platform: PsPlatform, make_ssh_client):
    """This test opens an SSH connection (via sftp) and sends a file to the remote location.

    It launches a Docker Image using testcontainers library.
    """
    remote_dir = Path(ps_platform.root_dir) / f'LOG_{ps_platform.expid}'
    remote_dir.mkdir(parents=True, exist_ok=True)
    Path(ps_platform.tmp_path).mkdir(parents=True, exist_ok=True)
    # generate file
    if "/" in filename:
        filename_dir = Path(filename).parent
        (Path(ps_platform.tmp_path) / filename_dir).mkdir(parents=True, exist_ok=True)
        filename = Path(filename).name
    with open(Path(ps_platform.tmp_path) / filename, 'w') as f:
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
        with DockerContainer(image=_DOCKER_IMAGE, remove=True, hostname='openssh-server') \
                .with_env('TZ', 'Etc/UTC') \
                .with_env('SUDO_ACCESS', 'false') \
                .with_env('USER_NAME', user) \
                .with_env('USER_PASSWORD', _DOCKER_PASSWORD) \
                .with_env('PASSWORD_ACCESS', 'true') \
                .with_bind_ports(2222, ssh_port) \
                .with_volume_mapping('/tmp', '/tmp', mode='rw') as container:
            wait_for_logs(container, 'sshd is listening on port 2222')

            ssh_client = make_ssh_client(ssh_port, _DOCKER_PASSWORD)
            mocker.patch('autosubmit.platforms.paramiko_platform._create_ssh_client', return_value=ssh_client)

            ps_platform.connect(None, reconnect=False, log_recovery_process=False)

            ps_platform.get_send_file_cmd = mocker.Mock()
            ps_platform.get_send_file_cmd.return_value = 'ls'
            ps_platform.send_command = mocker.Mock()

            ps_platform.send_file(filename)
            assert check == (remote_dir / filename).exists()
    finally:
        os.system(f'chmod 700 -R {str(rootdir)}')


@pytest.mark.parametrize(
    'cmd,error',
    [
        ('whoami', None),
        ('parangaricutirimicuaro', AutosubmitError)
    ]
)
@pytest.mark.docker
def test_send_command(cmd, error, ps_platform: PsPlatform, mocker, tmp_path, make_ssh_client):
    """This test opens an SSH connection (via sftp) and sends a command."""
    user = getuser() or "unknown"

    ssh_port = randrange(2000, 4000)

    with DockerContainer(image=_DOCKER_IMAGE, remove=True, hostname='openssh-server') \
            .with_env('TZ', 'Etc/UTC') \
            .with_env('SUDO_ACCESS', 'false') \
            .with_env('USER_NAME', user) \
            .with_env('USER_PASSWORD', _DOCKER_PASSWORD) \
            .with_env('PASSWORD_ACCESS', 'true') \
            .with_bind_ports(2222, ssh_port) \
            .with_volume_mapping('/tmp', '/tmp', mode='rw') as container:
        wait_for_logs(container, 'sshd is listening on port 2222')

        ssh_client = make_ssh_client(ssh_port, _DOCKER_PASSWORD)
        mocker.patch('autosubmit.platforms.paramiko_platform._create_ssh_client', return_value=ssh_client)

        ps_platform.connect(None, reconnect=False, log_recovery_process=False)

        if error:
            with pytest.raises(error):
                ps_platform.send_command(cmd, ignore_log=False, x11=False)
        else:
            assert ps_platform.send_command(cmd, ignore_log=False, x11=False)


@pytest.mark.docker
def test_exec_command(ps_platform: PsPlatform, mocker, make_ssh_client):
    """This test opens an SSH connection (via sftp) and executes a command."""
    user = getuser() or "unknown"

    ssh_port = randrange(2000, 4000)

    with DockerContainer(image=_DOCKER_IMAGE, remove=True, hostname='openssh-server') \
            .with_env('TZ', 'Etc/UTC') \
            .with_env('SUDO_ACCESS', 'false') \
            .with_env('USER_NAME', user) \
            .with_env('USER_PASSWORD', _DOCKER_PASSWORD) \
            .with_env('PASSWORD_ACCESS', 'true') \
            .with_bind_ports(2222, ssh_port) \
            .with_volume_mapping('/tmp', '/tmp', mode='rw') as container:
        wait_for_logs(container, 'sshd is listening on port 2222')

        ssh_client = make_ssh_client(ssh_port, _DOCKER_PASSWORD)
        mocker.patch('autosubmit.platforms.paramiko_platform._create_ssh_client', return_value=ssh_client)

        ps_platform.connect(None, reconnect=False, log_recovery_process=False)

        stdin, stdout, stderr = ps_platform.exec_command('whoami')
        assert stdin is not False
        assert stderr is not False
        # The stdout contents should be [b"user_name\n"]; thus the ugly list comprehension + extra code.
        assert user == str(''.join([x.decode('UTF-8').strip() for x in stdout.readlines()]))
