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

"""Fixtures for integration tests."""

import multiprocessing
import os
import uuid
from getpass import getuser
from pathlib import Path
from pwd import getpwnam
from subprocess import check_output
from tempfile import TemporaryDirectory
from typing import Generator, Iterator, Optional, Protocol, Union, TYPE_CHECKING

import paramiko
import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from autosubmit.platforms.paramiko_platform import ParamikoPlatform
# noinspection PyProtectedMember
from autosubmit.platforms.paramiko_platform import _create_ssh_client
from test.integration.test_utils.networking import get_free_port

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from py._path.local import LocalPath  # type: ignore

_SSH_DOCKER_IMAGE = 'lscr.io/linuxserver/openssh-server:latest'
_SSH_DOCKER_PASSWORD = 'password'

_SLURM_DOCKER_IMAGE = 'autosubmit/slurm-openssh-container:25-05-0-1'


class MakeSSHClientFixture(Protocol):
    def __call__(
            self,
            ssh_port: int,
            password: Optional[str],
            key: Optional[Union['Path', str]]) -> paramiko.SSHClient:
        ...


@pytest.fixture
def make_ssh_client() -> MakeSSHClientFixture:
    """Creates the SSH client

    It modifies the list of arguments so that the port is always
    the Docker container port.

    Once the list of arguments is patched, we call the original
    function to connect to the SSH server.

    :return: A normal Paramiko SSH Client, but that used the Docker SSH port and password to connect.
    """

    def _make_ssh_client(ssh_port: int, password: Optional[str], key: Union['Path', str]) -> paramiko.SSHClient:
        ssh_client = _create_ssh_client()

        orig_ssh_client_connect = ssh_client.connect

        def _ssh_connect(*args, **kwargs):
            """Mock call.

            The SSH port is always set to the Docker container port, discarding
            any values provided by the user.

            If the user does not provide a kwarg password, we set the password to the
            Docker password.
            """
            if 'port' in kwargs:
                del kwargs['port']
                kwargs['port'] = ssh_port
            if 'password' not in kwargs:
                kwargs['password'] = password
            if len(args) > 1:
                # tuple to list, and then replace the port...
                args = [x for x in args]
                args[1] = ssh_port

            if key is not None:
                kwargs['key_filename'] = str(key)

            ssh_timeout = 180  # 3 minutes
            for timeout in ['banner_timeout', 'auth_timeout', 'channel_timeout']:
                kwargs[timeout] = ssh_timeout

            return orig_ssh_client_connect(*args, **kwargs)

        ssh_client.connect = _ssh_connect
        return ssh_client

    return _make_ssh_client


@pytest.fixture
def paramiko_platform() -> Iterator[ParamikoPlatform]:
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


@pytest.fixture(scope="function")
def git_server(tmp_path) -> Generator[tuple[DockerContainer, Path, str], None, None]:
    # Start a container to server it -- otherwise, we would have to use
    # `git -c protocol.file.allow=always submodule ...`, and we cannot
    # change how Autosubmit uses it in `autosubmit create` (due to bad
    # code design choices).

    git_repos_path = tmp_path / 'git_repos'
    git_repos_path.mkdir(exist_ok=True, parents=True)

    http_port = get_free_port()

    image = 'githttpd/githttpd:latest'
    with DockerContainer(image=image, remove=True) \
            .with_bind_ports(80, http_port) \
            .with_volume_mapping(str(git_repos_path), '/opt/git-server', mode='rw') as container:
        wait_for_logs(container, "Command line: 'httpd -D FOREGROUND'")

        # The docker image ``githttpd/githttpd`` creates an HTTP server for Git
        # repositories, using the volume bound onto ``/opt/git-server`` as base
        # for any subdirectory, the Git URL becoming ``git/{subdirectory-name}}``.
        yield container, git_repos_path, f'http://localhost:{http_port}/git'


@pytest.fixture()
def ssh_server(mocker, tmp_path, make_ssh_client, request):
    ssh_port = get_free_port()

    user = getuser() or "unknown"
    user_pw = getpwnam(user)
    uid = user_pw.pw_uid
    gid = user_pw.pw_gid

    with DockerContainer(image=_SSH_DOCKER_IMAGE, remove=True, hostname='openssh-server') \
            .with_env('TZ', 'Etc/UTC') \
            .with_env('SUDO_ACCESS', 'false') \
            .with_env('USER_NAME', user) \
            .with_env('USER_PASSWORD', 'password') \
            .with_env('PUID', str(uid)) \
            .with_env('PGID', str(gid)) \
            .with_env('UMASK', '000') \
            .with_env('PASSWORD_ACCESS', 'true') \
            .with_bind_ports(2222, ssh_port) as container:
        wait_for_logs(container, 'sshd is listening on port 2222')

        ssh_client = make_ssh_client(ssh_port, _SSH_DOCKER_PASSWORD, None)
        mocker.patch('autosubmit.platforms.paramiko_platform._create_ssh_client', return_value=ssh_client)

        yield container


@pytest.fixture()
def slurm_server(mocker, tmp_path: 'LocalPath', make_ssh_client: MakeSSHClientFixture, request):
    ssh_port = get_free_port()
    container_name = f'slurm-server-{uuid.uuid4()}'

    docker_args = {
        'cgroupns': 'host',
        'privileged': True
    }

    docker_container = DockerContainer(
            image=_SLURM_DOCKER_IMAGE,
            remove=True,
            hostname='slurmctld',
            name=container_name,
            **docker_args
    )

    # TODO: GH needs --volume /sys/fs/cgroup:/sys/fs/cgroup:rw
    if 'GITHUB_ACTION' in os.environ:
        docker_container = docker_container.with_volume_mapping('/sys/fs/cgroup', '/sys/fs/cgroup', mode='rw')

    with docker_container \
            .with_env('TZ', 'Etc/UTC') \
            .with_bind_ports(2222, ssh_port) as container:
        # TODO: or maybe wait for 'debug:  sched: Running job scheduler for full queue.'?
        wait_for_logs(container, 'No fed_mgr state file')

        container.exec('sinfo')

        # What we had in ci.yaml for the old Slurm Docker service:
        # $ docker cp slurm-container:/root/.ssh/container_root_pubkey /tmp/container_root_pubkey
        # $ chmod 600 /tmp/container_root_pubkey
        # Translated into the code below:
        ssh_key = tmp_path / 'container_root_pubkey'
        check_output(
            [
                'docker',
                'cp',
                f'{container_name}:/root/.ssh/container_root_pubkey',
                str(ssh_key)
            ]
        )
        Path(ssh_key).chmod(0o600)

        ssh_client = make_ssh_client(ssh_port, password=None, key=ssh_key)
        mocker.patch('autosubmit.platforms.paramiko_platform._create_ssh_client', return_value=ssh_client)

        # Pytest does NOT patch when using spawn context.
        mocker.patch(
            'autosubmit.platforms.platform.Platform.get_mp_context',
            return_value=multiprocessing.get_context('fork')
        )

        yield container
