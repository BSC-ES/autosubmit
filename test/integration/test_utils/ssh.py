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

"""Utilities for SSH."""

import time
from getpass import getuser
from pathlib import Path
from subprocess import check_output
from textwrap import dedent
from typing import Any, Optional, Protocol, Union, TYPE_CHECKING

import paramiko.ssh_exception
from cryptography.hazmat.primitives import asymmetric
from cryptography.hazmat.primitives import serialization

# noinspection PyProtectedMember
from autosubmit.platforms.paramiko_platform import _create_ssh_client
from test.integration.test_utils.networking import wait_for_tcp_port

if TYPE_CHECKING:
    from paramiko import SSHClient
    from pytest_mock import MockerFixture

__all__ = [
    'MakeSSHClientFixture',
    'make_ssh_client',
    'mock_ssh_config_and_client',
    'create_ssh_keypair_and_config',
    'copy_ssh_key_from_container',
    'wait_for_ssh_port'
]


class MakeSSHClientFixture(Protocol):
    def __call__(
            self,
            ssh_port: int,
            password: Optional[str],
            key: Optional[Union['Path', str]]) -> 'SSHClient':
        ...


# noinspection PyUnusedLocal
def make_ssh_client(ssh_port: int, password: Optional[str], key: Optional[Union['Path', str]]) -> 'SSHClient':
    """Creates the SSH client

    It modifies the list of arguments so that the port is always
    the Docker container port.

    Once the list of arguments is patched, we call the original
    function to connect to the SSH server.

    :return: A normal Paramiko SSH Client, but that used the Docker SSH port and password to connect.
    """
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
            kwargs['look_for_keys'] = False
            kwargs['allow_agent'] = False
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


def mock_ssh_config_and_client(ssh_config_path: Path, ssh_port: int, password: Optional[str], mocker: 'MockerFixture') -> Any:
    ssh_config = paramiko.SSHConfig()
    with open(ssh_config_path, 'r') as f:
        ssh_config.parse(f)
    if password:
        ssh_client = make_ssh_client(ssh_port, password, None)
        mocker.patch('autosubmit.platforms.paramiko_platform._create_ssh_client', return_value=ssh_client)
    return mocker.patch('autosubmit.platforms.paramiko_platform._load_ssh_config', return_value=ssh_config)


def _generate_ssh_keypair(path: Path):
    """Generates an ed25519 private/public key pair and saves them as ``path{|.pub}``."""

    if path.exists() and path.with_suffix(path.suffix + ".pub").exists():
        return path, path.with_suffix(path.suffix + ".pub")

    # From: https://github.com/paramiko/paramiko/issues/1136#issuecomment-1160771520
    c_ed25519key = asymmetric.ed25519.Ed25519PrivateKey.generate()  # type: ignore
    private_key = c_ed25519key.private_bytes(encoding=serialization.Encoding.PEM,
                                             format=serialization.PrivateFormat.OpenSSH,
                                             encryption_algorithm=serialization.NoEncryption())
    # print(private_key.decode())
    path.touch()
    path.write_text(private_key.decode('utf-8'))
    path.chmod(0o600)
    # import io
    # private_key_obj = io.StringIO(private_key.decode())
    # p_ed25519key = paramiko.ed25519key.Ed25519Key.from_private_key(private_key_obj)
    # print(p_ed25519key.get_base64())

    pub = c_ed25519key.public_key()
    openssh_pub = pub.public_bytes(encoding=serialization.Encoding.OpenSSH,
                                   format=serialization.PublicFormat.OpenSSH)
    pub_file = path.with_suffix(path.suffix + ".pub")
    pub_file.touch()
    pub_file.write_text(openssh_pub.decode('utf-8'))
    pub_file.chmod(0o644)

    return path, pub_file


def create_ssh_keypair_and_config(
        ssh_port: int,
        ssh_dir: Path,
        config_filename='config'
) -> tuple[Path, Path, Path]:
    """Sets up ``$HOME/.ssh/config`` file for SSH.

    Previously, Autosubmit fixtures for containers would mock calls in
    Autosubmit with ``mocker`` or ``session_mocker``.

    Mocking in general can be bad or dangerous. But in this
    case it is even riskier, since we have pytest-xdist, and
    we are trying to have fewer containers running - i.e. we
    share the container across multiple tests and pytest-xdist
    processes.

    Mocking a session in one process that is using Slurm
    and SSH can lead to some times working by chance, or
    intermittent failures.

    We removed mocks, and instead now set up an SSH configuration
    file ``.ssh/config`` located inside the current test directory.
    This is done **only** when one of the fixtures above is used,
    which is why this fixture is located here - for developer
    convenience.

    :return: A tuple with private, public keys, and SSH config file.
    """
    ssh_key_path = Path(ssh_dir, "test_key")
    private_key, public_key = _generate_ssh_keypair(ssh_key_path)

    ssh_config = Path(ssh_dir, config_filename)
    if not ssh_config.exists():
        ssh_config.write_text(
            dedent(f"""\
                Host localhost
                    Hostname localhost
                    User {getuser()}
                    ForwardX11 yes
                    Port {ssh_port}
                    StrictHostKeyChecking no
                    IdentityFile {str(private_key.resolve())}
                Host 127.0.0.1
                    Hostname localhost
                    User {getuser()}
                    ForwardX11 yes
                    Port {ssh_port}
                    StrictHostKeyChecking no
                    IdentityFile {str(private_key.resolve())}
                """)
        )
        ssh_config.chmod(0o600)  # only user can read/write

    return private_key, public_key, ssh_config


def copy_ssh_key_from_container(tmp_path_factory, container_id):
    """Utility to copy the key from the container to the worker's local tmp."""
    worker_tmp = tmp_path_factory.getbasetemp()
    ssh_key = worker_tmp / 'container_root_pubkey'

    check_output([
        'docker', 'cp',
        f'{container_id}:/root/.ssh/container_root_pubkey',
        str(ssh_key)
    ])
    ssh_key.chmod(0o600)
    return ssh_key


def wait_for_ssh_port(host, port, timeout=30):
    """Wait for SSH.

    Testing the TCP port is not enough as the SSH service may not have
    fully initiated yet.

    Testing the SSH banner also may give a false positive.

    1. TCP port open
    2. SSH banner available
    3. Successful SSH command execution
    """
    start = time.time()
    while True:
        transport = None
        # noinspection PyBroadException
        try:
            wait_for_tcp_port(host, port, timeout=timeout)
            transport = paramiko.Transport((host, port))
            transport.start_client(timeout=5)
            return
        except Exception:
            if time.time() - start > timeout:
                raise TimeoutError(f"SSH not ready at {host}:{port}")
            time.sleep(1)
        finally:
            if transport:
                transport.close()
