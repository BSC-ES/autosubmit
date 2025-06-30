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

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Callable, Iterator, TYPE_CHECKING

import paramiko
import pytest

from autosubmit.platforms.paramiko_platform import ParamikoPlatform
# noinspection PyProtectedMember
from autosubmit.platforms.paramiko_platform import _create_ssh_client
from autosubmit.platforms.psplatform import PsPlatform

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from py._path.local import LocalPath  # type: ignore


@pytest.fixture
def make_ssh_client() -> Callable[[int, str], paramiko.SSHClient]:
    """Creates the SSH client

    It modifies the list of arguments so that the port is always
    the Docker container port.

    Once the list of arguments is patched, we call the original
    function to connect to the SSH server.

    :return: A normal Paramiko SSH Client, but that used the Docker SSH port and password to connect.
    """

    def _make_ssh_client(ssh_port: int, password) -> paramiko.SSHClient:
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


@pytest.fixture
def ps_platform(tmp_path: Path) -> PsPlatform:
    config = {
        "LOCAL_ROOT_DIR": str(tmp_path),
        "LOCAL_TMP_DIR": 'tmp',
        "PLATFORMS": {
            "pytest-ps": {
                "type": "ps",
                "host": "127.0.0.1",
                "user": tmp_path.owner(),
                "project": "whatever",
                "scratch_dir": f"{tmp_path.name}",
                "MAX_WALLCLOCK": "48:00",
                "DISABLE_RECOVERY_THREADS": True
            }
        }
    }
    platform = PsPlatform(expid='a000', name='local-ps', config=config)
    platform.host = '127.0.0.1'
    platform.user = tmp_path.owner()
    platform.root_dir = tmp_path / "remote"
    platform.root_dir.mkdir(parents=True, exist_ok=True)
    return platform