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

"""Utilities for Docker."""

from getpass import getuser
from os import environ
from pathlib import Path
from pwd import getpwnam
from time import sleep, time
from typing import TYPE_CHECKING

# noinspection PyProtectedMember
from docker import from_env
from portalocker import Lock, LOCK_EX
from testcontainers.core.container import DockerContainer  # type: ignore
from testcontainers.core.waiting_utils import wait_for_logs  # type: ignore

from test.integration.test_utils.networking import get_free_port
from test.integration.test_utils.ssh import create_ssh_keypair_and_config, wait_for_ssh_port

if TYPE_CHECKING:
    from docker.models.containers import Container, ExecResult

__all__ = [
    'SSH_DOCKER_PASSWORD',
    'get_container_by_id',
    'get_git_container',
    'get_slurm_container',
    'get_ssh_container',
    'stop_test_containers'
]

_LOCK_TIMEOUT_SECONDS = 60
"""Timeout used when using file locks."""

_SSH_DOCKER_IMAGE = 'lscr.io/linuxserver/openssh-server:latest'
"""This is the vanilla image from LinuxServer.io, with OpenSSH. About 39MB."""
_SSH_DOCKER_IMAGE_X11_MFA = 'autosubmit/linuxserverio-ssh-2fa-x11:latest'
"""This is our test image, built on top of LinuxServer.io's, but with MFA and X11. About 395MB."""
SSH_DOCKER_PASSWORD = 'password'
"""Common password used in SSH containers; we mock the SSH Client of Paramiko to avoid hassle with keys."""

_SLURM_DOCKER_IMAGE = 'autosubmit/slurm-openssh-container:25-05-0-1'
"""The Slurm Docker image. About 600 MB. It contains 2 cores, 1 node."""

_GIT_DOCKER_IMAGE = 'githttpd/githttpd:latest'
"""The Git image used for tests where Autosubmit needs to clone a repository."""

_AS_SLURM_CONTAINER_LABEL = "pytest.slurm.singleton"
"""Docker container label key for Slurm singleton."""
_AS_GIT_CONTAINER_LABEL = "pytest.git.singleton"
"""Docker container label key for Git singleton."""
_AS_SSH_CONTAINER_LABEL = "pytest.ssh.singleton"
"""Docker container label key for SSH singleton."""
_AS_SSH_X11_CONTAINER_LABEL = "pytest.ssh_x11.singleton"
"""Docker container label key for SSH singleton (with X11 configured)."""
_AS_SSH_X11_MFA_CONTAINER_LABEL = "pytest.ssh_x11_mfa.singleton"
"""Docker container label key for SSH singleton (with X11 and MFA configured)."""
_AS_SINGLETON_CONTAINER_VALUE = "true"
"""Docker container label value for singletons."""


def get_container_by_id(container_id: str) -> 'Container':
    """Gets a Docker container by its ID.

    :param: container_id: The ID of the container.
    :return: A Docker container.
    """
    client = from_env()
    return client.containers.get(container_id)


def _start_git_container(git_repos_path: Path, http_port: int) -> DockerContainer:
    """Start a Docker container with Git.

    The repository will be available with the base name of the path given,
    with the TCP/80 port mapped to the host ``http_port``.
    """
    docker_args = {
        'labels': {
            _AS_GIT_CONTAINER_LABEL: 'true',
        }
    }

    docker_container = DockerContainer(
        image=_GIT_DOCKER_IMAGE,
        remove=True,
        **docker_args
    )

    container = docker_container \
        .with_bind_ports(80, http_port) \
        .with_volume_mapping(str(git_repos_path), '/opt/git-server', mode='rw')
    container.start()

    wait_for_logs(container, "Command line: 'httpd -D FOREGROUND'")

    container.exec('whoami')

    # The docker image ``githttpd/githttpd`` creates an HTTP server for Git
    # repositories, using the volume bound onto ``/opt/git-server`` as base
    # for any subdirectory, the Git URL becoming ``git/{subdirectory-name}}``.
    return container


def get_git_container(lock_path: Path, git_repos_path: Path) -> tuple['Container', int]:
    """Get a running Git container and its HTTP port."""
    client = from_env()

    with Lock(filename=str(lock_path), flags=LOCK_EX, timeout=_LOCK_TIMEOUT_SECONDS):
        containers = client.containers.list(
            filters={
                "label": f"{_AS_GIT_CONTAINER_LABEL}=true"
            }
        )

        if containers:
            container = containers[0]
            container_instance = get_container_by_id(container.id)
            http_port = int(container_instance.ports['80/tcp'][0]['HostPort'])  # type: ignore
        else:
            # Create the container exactly once
            http_port = get_free_port()
            # noinspection PyProtectedMember
            container_instance = _start_git_container(git_repos_path, http_port)._container

    return container_instance, http_port


def _start_slurm_container(ssh_port: int) -> DockerContainer:
    """Create and start a Slurm container with TestContainers.

    The container is created with a label that can be used to later retrieve
    the container without needing its ID. It is designed so that only one
    container instance is created per test session (a singleton).

    Do not repeat Autosubmit experiment IDs. Do not reuse experiment folders.
    Doing any of these, will result in pytest failures that i. do not contain
    any meaningful information in the logs, ii. nothing useful in the ASLOGS or
    experiment temporary logs, iii. you will have to figure out how to set a
    breakpoint and inspect what is inside the Slurm server.

    Avoiding these risks will save you & other developers time debugging
    issues like this.

    :param ssh_port: The SSH port.
    :return: an instance of a TestContainers container, with a docker container wrapped.
    """
    docker_args = {
        'cgroupns': 'host',
        'privileged': True,
        'labels': {
            _AS_SLURM_CONTAINER_LABEL: 'true',
        }
    }

    docker_container = DockerContainer(
        image=_SLURM_DOCKER_IMAGE,
        remove=True,
        hostname='slurmctld',
        **docker_args
    )

    # TODO: GH needs --volume /sys/fs/cgroup:/sys/fs/cgroup:rw
    if 'GITHUB_ACTION' in environ:
        docker_container = docker_container.with_volume_mapping('/sys/fs/cgroup', '/sys/fs/cgroup', mode='rw')

    container = docker_container \
        .with_env('TZ', 'Etc/UTC') \
        .with_bind_ports(2222, ssh_port)
    container.start()

    # TODO: or maybe wait for 'debug:  sched: Running job scheduler for full queue.'?
    wait_for_logs(container, lambda logs: 'No fed_mgr state file' in logs)

    container.exec('sinfo')

    return container


def get_slurm_container(lock_path: Path) -> tuple['Container', int]:
    """Get a running Slurm container and its SSH port."""
    client = from_env()

    with Lock(filename=str(lock_path), flags=LOCK_EX, timeout=_LOCK_TIMEOUT_SECONDS):
        containers = client.containers.list(
            filters={
                "label": f"{_AS_SLURM_CONTAINER_LABEL}=true"
            }
        )

        if containers:
            container = containers[0]
            container_instance = get_container_by_id(container.id)
            ssh_port = int(container.ports['2222/tcp'][0]['HostPort'])  # type: ignore
        else:
            # Create the container exactly once
            ssh_port = get_free_port()
            # noinspection PyProtectedMember
            container_instance = _start_slurm_container(ssh_port=ssh_port)._container

    return container_instance, ssh_port


def _write_authorized_keys(container: 'Container', public_key: Path, authorized_keys: Path) -> 'ExecResult':
    """Write an SSH public key into an ``authorized_keys`` directory inside a container."""
    key_content = public_key.read_text()
    # escape single quotes for shell
    safe_key = key_content.replace("'", "'\"'\"'")
    return container.exec_run(
        f"sh -c 'echo \"{safe_key}\" > {authorized_keys} && chmod 600 {authorized_keys}'"
    )


def _start_ssh_container(ssh_port: int, priv_pub_key: tuple[Path, Path], mfa=False, x11=False) -> DockerContainer:
    """Create and start a Docker SSH container for SSH."""
    user = getuser()
    user_pw = getpwnam(user)
    uid = user_pw.pw_uid
    gid = user_pw.pw_gid

    ssh_image = _SSH_DOCKER_IMAGE_X11_MFA if mfa or x11 else _SSH_DOCKER_IMAGE
    label = _AS_SSH_X11_MFA_CONTAINER_LABEL if mfa or x11 else _AS_SSH_CONTAINER_LABEL

    docker_args = {
        'labels': {
            label: _AS_SINGLETON_CONTAINER_VALUE,
        }
    }

    docker_container = DockerContainer(
        image=ssh_image,
        remove=True,
        hostname='openssh-server',
        **docker_args
    ).with_env('TZ', 'Etc/UTC') \
        .with_env('SUDO_ACCESS', 'false') \
        .with_env('USER_NAME', user) \
        .with_env('USER_PASSWORD', 'password') \
        .with_env('PUID', str(uid)) \
        .with_env('PGID', str(gid)) \
        .with_env('UMASK', '000') \
        .with_env('PASSWORD_ACCESS', 'true') \
        .with_env('MFA', str(mfa).lower()) \
        .with_bind_ports(2222, ssh_port)
    docker_container.start()

    # This verifies that the server printed the line, not necessarily the port is available
    wait_for_logs(docker_container, 'sshd is listening on port 2222')

    # noinspection PyProtectedMember
    container = docker_container._container

    exec_result = docker_container.exec('whoami')
    if exec_result.exit_code != 0:
        raise RuntimeError(f'Failed to run whoami on test container {container.id}')

    # _, public_key = create_ssh_keypair(ssh_port, home_dir)
    public_key = priv_pub_key[1]
    exec_result = _write_authorized_keys(container, public_key, Path('/config/.ssh/authorized_keys'))
    exit_code = exec_result.exit_code

    if exit_code != 0:
        raise RuntimeError(f'Failed to write authorized_keys to test container {container.id}')

    wait_for_ssh_port('localhost', ssh_port, timeout=30)

    return docker_container


def get_ssh_container(
        lock_path: Path,
        ssh_path: Path,
        /,
        mfa: bool,
        x11: bool,
        singleton=True
) -> tuple['Container', int, Path]:
    """Get a running SSH container, its port, and an SSH config.

    NOTE: Different from its sibling functions in this module, this function
          may return different instances. That's because we have three possible
          combinations: SSH vanilla container, SSH with X11 and MFA disabled,
          and SSH with X11 and MFA enabled.
    """
    if not mfa and not x11:
        label = _AS_SSH_CONTAINER_LABEL
    elif not mfa and x11:
        label = _AS_SSH_X11_CONTAINER_LABEL
    else:
        label = _AS_SSH_X11_MFA_CONTAINER_LABEL

    client = from_env()

    with Lock(filename=str(lock_path), flags=LOCK_EX, timeout=120):
        containers = client.containers.list(
            filters={
                "label": f"{label}=true"
            }
        )

        if containers and singleton:
            container = containers[0]
            container_instance = get_container_by_id(container.id)
            ssh_config = ssh_path / 'config'
            ssh_port = int(container.ports['2222/tcp'][0]['HostPort'])  # type: ignore
        else:
            # Create the container exactly once
            ssh_port = get_free_port()
            priv, pubkey, ssh_config = create_ssh_keypair_and_config(ssh_port, ssh_path)

            # noinspection PyProtectedMember
            container_instance = _start_ssh_container(ssh_port, (priv, pubkey), mfa=mfa, x11=x11)._container
            # NOTE: In the call above, the ``DockerContainer`` returned by the start
            #       function has the correct ports, but the ``Container`` object
            #       wrapped doesn't. Can't tell if by design in TestContainers or a bug,
            #       which is why the signature of the get_container functions return
            #       the port used.

    return container_instance, ssh_port, ssh_config


def stop_test_containers(stop_timeout=1, stop_all_timeout=30) -> None:
    """Stops the singleton containers created for Autosubmit tests.

    We use custom labels to mark our test containers. They start with
    the text "pytest.", and contain other parts like "ssh", "git". This
    way if you query existing containers and labels, you can get a good
    picture of what's using your system resources.

    This function ignores any errors as the TestContainers sidekick
    container (riuk?) can also kill the singleton once it's not used
    or needs to be stopped, or the container may stop by itself right
    when we are about to terminate it -- and we do not care if it fails
    as this function responsibility is to stop them, no matter how/who.

    # TODO: figure out a way to send a kill force to these containers?

    :param stop_timeout: Timeout in seconds passed to Docker API to stop a container.
    :param stop_all_timeout: Timeout in seconds wait for all containers to have stopped.
    """
    labels = [
        _AS_SSH_CONTAINER_LABEL,
        _AS_SSH_X11_CONTAINER_LABEL,
        _AS_SSH_X11_MFA_CONTAINER_LABEL,
        _AS_GIT_CONTAINER_LABEL,
        _AS_SLURM_CONTAINER_LABEL
    ]

    # Loop and call stop on all containers. Even with the timeout,
    # the containers may take some seconds to really stop.
    for label in labels:
        try:
            # from_env().containers.prune(
            #     filters={"label": f"{label}={_AS_SINGLETON_CONTAINER_VALUE}"}
            # )
            containers: list['Container'] = from_env().containers.list(
                filters={
                    "status": "running",
                    "label": f"{label}=true"
                }
            )

            if containers:
                for container in containers:
                    try:
                        container.stop(timeout=stop_timeout)
                    except Exception as e:
                        print(f'Failed to stop container {container.id}: {str(e)}')
        except Exception as e:
            print(f'Failed to list containers with label {label}: {str(e)}')

    # Loop to wait for all containers to have really stopped.
    start = time()
    for label in labels:
        try:
            # from_env().containers.prune(
            #     filters={"label": f"{label}={_AS_SINGLETON_CONTAINER_VALUE}"}
            # )
            containers: list['Container'] = from_env().containers.list(
                filters={
                    "status": "running",
                    "label": f"{label}=true"
                }
            )
            if not containers:
                continue

            now = time()
            if now - start > stop_all_timeout:
                raise RuntimeError(f'Failed to stop all Docker containers after {stop_all_timeout} seconds')

            sleep(1)
        except Exception as e:
            print(f'Failed to list containers with label {label}: {str(e)}')
