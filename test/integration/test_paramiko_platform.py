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

"""Integration tests for the paramiko platform.

Note that tests will start and destroy an SSH server. For unit tests, see ``test_paramiko_platform.py``
in the ``test/unit`` directory."""

import socket
from dataclasses import dataclass
from getpass import getuser
from pathlib import Path
from typing import Optional, TYPE_CHECKING

import paramiko
import pytest

from autosubmit.log.log import AutosubmitError
from autosubmit.platforms.paramiko_submitter import ParamikoSubmitter

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from _pytest._py.path import LocalPath
    from testcontainers.sftp import DockerContainer
    from test.conftest import AutosubmitExperiment
    from autosubmit.platforms.psplatform import PsPlatform

_EXPID = 't000'
_PLATFORM_NAME = 'TEST_PS_PLATFORM'
_PLATFORM_REMOTE_DIR = '/app/'
_PLATFORM_PROJECT = 'test'


@dataclass
class ExperimentPlatformServer:
    """Data holder for fixture objects."""
    experiment: 'AutosubmitExperiment'
    platform: 'PsPlatform'
    ssh_server: 'DockerContainer'


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


@pytest.fixture()
def exp_platform_server(autosubmit_exp, ssh_server: 'DockerContainer') -> ExperimentPlatformServer:
    """Fixture that returns an Autosubmit experiment, a platform, and the (Docker) server used."""
    user = getuser()
    exp = autosubmit_exp(_EXPID, experiment_data={
        'PLATFORMS': {
            _PLATFORM_NAME: {
                'TYPE': 'ps',
                'HOST': ssh_server.get_docker_client().host(),
                'PROJECT': _PLATFORM_PROJECT,
                'USER': user,
                'SCRATCH_DIR': _PLATFORM_REMOTE_DIR,
                'ADD_PROJECT_TO_HOST': 'False',
                'MAX_WALLCLOCK': '48:00',
                'DISABLE_RECOVERY_THREADS': 'True'
            }
        },
        'JOBS': {
            # FIXME: This is poorly designed. First, to load platforms you need an experiment
            #        (even if you are in test/code mode). Then, platforms only get the user
            #        populated by a submitter. This is strange, as the information about the
            #        user is in the ``AutosubmitConfig``, and the platform has access to the
            #        ``AutosubmitConfig``. It is just never accessing the user (expid, yes).
            'BECAUSE_YOU_NEED_AT_LEAST_ONE_JOB_USING_THE_PLATFORM': {
                'RUNNING': 'once',
                'SCRIPT': "sleep 0",
                'PLATFORM': _PLATFORM_NAME
            }
        }
    })

    # We load the platforms with the submitter so that the platforms have all attributes.
    # NOTE: The set up of platforms is done partially in the platform constructor and
    #       partially by a submitter (i.e., they are tightly coupled, which makes it hard
    #       to maintain and test).
    submitter = ParamikoSubmitter()
    submitter.load_platforms(asconf=exp.as_conf, retries=0)

    ps_platform: 'PsPlatform' = submitter.platforms[_PLATFORM_NAME]

    return ExperimentPlatformServer(exp, ps_platform, ssh_server)


@pytest.mark.docker
@pytest.mark.parametrize(
    'filename',
    [
        'test1',
        'sub/test2'
    ],
    ids=['filename', 'filename_long_path']
)
def test_send_file(filename: str, exp_platform_server: ExperimentPlatformServer):
    """This test opens an SSH connection (via sftp) and sends a file to the remote location.

    It launches a Docker Image using testcontainers library.
    """
    user = getuser()

    exp = exp_platform_server.experiment
    ps_platform = exp_platform_server.platform
    ssh_server = exp_platform_server.ssh_server

    ps_platform.connect(as_conf=exp.as_conf, reconnect=False, log_recovery_process=False)
    assert ps_platform.check_remote_permissions()

    # generate the file
    if "/" in filename:
        filename_dir = Path(filename).parent
        Path(ps_platform.tmp_path, filename_dir).mkdir(parents=True, exist_ok=True)
        filename = Path(filename).name
    with open(str(Path(ps_platform.tmp_path, filename)), 'w') as f:
        f.write('test')

    assert ps_platform.send_file(filename)

    file = f'{_PLATFORM_REMOTE_DIR}/{_PLATFORM_PROJECT}/{user}/{exp.expid}/LOG_{exp.expid}/{filename}'
    result = ssh_server.exec(f'ls {file}')
    assert result.exit_code == 0


@pytest.mark.parametrize(
    'cmd,error',
    [
        ('whoami', None),
        ('parangaricutirimicuaro', AutosubmitError)
    ]
)
@pytest.mark.docker
def test_send_command(cmd: str, error: Optional, exp_platform_server: ExperimentPlatformServer):
    """This test opens an SSH connection (via sftp) and sends a command."""
    exp_platform_server.platform.connect(None, reconnect=False, log_recovery_process=False)

    if error:
        with pytest.raises(error):
            exp_platform_server.platform.send_command(cmd, ignore_log=False, x11=False)
    else:
        assert exp_platform_server.platform.send_command(cmd, ignore_log=False, x11=False)


@pytest.mark.parametrize(
    'cmd,timeout',
    [
        ('rsync --version', None),
        ('rm --help', 60),
        ('whoami', 120)
    ]
)
@pytest.mark.docker
def test_send_command_timeout_error_exec_command(
        cmd: str, timeout: Optional[int], exp_platform_server: 'AutosubmitExperiment', mocker):
    """Test that the correct timeout is used, and that ``exec_command`` raises ``AutosubmitError``."""
    exp_platform_server.platform.connect(None, reconnect=False, log_recovery_process=False)

    # Capture platform log.
    mocked_log = mocker.patch('autosubmit.platforms.paramiko_platform.Log')
    # Simulate an error occurred, and retrying did not fix it.
    mocker.patch.object(exp_platform_server.platform, 'exec_command', return_value=(False, False, False))

    with pytest.raises(AutosubmitError) as cm:
        exp_platform_server.platform.send_command(command=cmd, ignore_log=False, x11=False)

    assert mocked_log.debug.called
    assert f'send_command timeout used: {str(timeout)}' in mocked_log.debug.call_args[0][0]

    assert 'Failed to send' in str(cm.value.message)
    assert 6005 == cm.value.code


@pytest.mark.docker
def test_exec_command(exp_platform_server: 'ExperimentPlatformServer'):
    """This test opens an SSH connection (via sftp) and executes a command."""
    user = getuser() or "unknown"
    exp_platform_server.platform.connect(None, reconnect=False, log_recovery_process=False)

    stdin, stdout, stderr = exp_platform_server.platform.exec_command('whoami')
    assert stdin is not False
    assert stderr is not False
    # The stdout contents should be [b"user_name\n"]; thus the ugly list comprehension + extra code.
    assert user == str(''.join([x.decode('UTF-8').strip() for x in stdout.readlines()]))


@pytest.mark.docker
def test_exec_command_after_a_reset(exp_platform_server: 'ExperimentPlatformServer'):
    """Test that after a connection reset we are still able to execute commands."""
    user = getuser() or "unknown"
    exp_platform_server.platform.connect(None, reconnect=False, log_recovery_process=False)

    exp_platform_server.platform.reset()

    exp_platform_server.platform.connect(None, reconnect=False, log_recovery_process=False)

    stdin, stdout, stderr = exp_platform_server.platform.exec_command('whoami')
    assert stdin is not False
    assert stderr is not False
    # The stdout contents should be [b"user_name\n"]; thus the ugly list comprehension + extra code.
    assert user == str(''.join([x.decode('UTF-8').strip() for x in stdout.readlines()]))


@pytest.mark.parametrize(
    'x11,retries',
    [
        pytest.param(
            True, 2,
            marks=pytest.mark.xfail(reason="Apparently x11 is not called and is just broken?")
        ),
        [False, 2]
    ]
)
@pytest.mark.docker
def test_exec_command_ssh_session_not_active(x11, retries, exp_platform_server: 'ExperimentPlatformServer'):
    """This test that we retry even if the SSH session gets closed."""
    user = getuser() or "unknown"
    exp_platform_server.platform.connect(None, reconnect=False, log_recovery_process=False)

    # NOTE: We could simulate it the following way:
    #           ex = paramiko.SSHException('SSH session not active')
    #           mocker.patch.object(ps_platform.transport, 'open_session', side_effect=ex)
    #       But while that's OK, we can also avoid mocking by simply
    #       closing the connection.

    exp_platform_server.platform.transport.close()

    stdin, stdout, stderr = exp_platform_server.platform.exec_command(
        'whoami',
        x11=x11,
        retries=retries
    )

    # This will be true iff the ``ps_platform.restore_connection(None)`` ran without errors.
    assert stdin is not False
    assert stderr is not False
    # The stdout contents should be [b"user_name\n"]; thus the ugly list comprehension + extra code.
    assert user == str(''.join([x.decode('UTF-8').strip() for x in stdout.readlines()]))


@pytest.mark.docker
@pytest.mark.parametrize(
    'error',
    [
        paramiko.ssh_exception.NoValidConnectionsError({'192.168.0.1': ValueError('failed')}),  # type: ignore
        ConnectionError('Someone unplugged the networking cable.'),
        socket.error('A random socket error occurred!')
    ],
    ids=[
        'paramiko ssh exception',
        'connection error',
        'socket error'
    ]
)
def test_exec_command_socket_error(error: Exception, exp_platform_server: 'ExperimentPlatformServer', mocker):
    """Test that the command is retried and succeeds even when a socket error occurs."""
    user = getuser() or "unknown"
    exp_platform_server.platform.connect(None, reconnect=False, log_recovery_process=False)

    exp_platform_server.platform.transport.close()

    mocker.patch.object(exp_platform_server.platform.transport, 'open_session', side_effect=error)

    stdin, stdout, stderr = exp_platform_server.platform.exec_command('whoami')
    assert stdin is not False
    assert stderr is not False
    # The stdout contents should be [b"user_name\n"]; thus the ugly list comprehension + extra code.
    assert user == str(''.join([x.decode('UTF-8').strip() for x in stdout.readlines()]))


@pytest.mark.docker
def test_exec_command_ssh_session_not_active_cannot_restore(exp_platform_server: 'ExperimentPlatformServer', mocker):
    """Test that when an error occurs, and it cannot restore, then we return falsey values."""
    exp_platform_server.platform.connect(None, reconnect=False, log_recovery_process=False)

    exp_platform_server.platform.closeConnection()

    # This dummy mock prevents the platform from being able to restore its connection.
    mocker.patch.object(exp_platform_server.platform, 'restore_connection')

    stdin, stdout, stderr = exp_platform_server.platform.exec_command('whoami')
    assert stdin is False
    assert stdout is False
    assert stderr is False


@pytest.mark.docker
def test_fs_operations(exp_platform_server: 'ExperimentPlatformServer'):
    """Test that we can access files, send new files, move, delete."""
    user = getuser()

    local_file = Path(exp_platform_server.platform.tmp_path, 'test.txt')
    text = 'Lorem ipsum'

    with open(local_file, 'w+') as f:
        f.write(text)

    remote_file = Path(_PLATFORM_REMOTE_DIR, _PLATFORM_PROJECT, user, exp_platform_server.experiment.expid,
                       f'LOG_{exp_platform_server.experiment.expid}', local_file.name)

    exp_platform_server.platform.connect(None, reconnect=False, log_recovery_process=False)

    file_not_found = Path('/app', 'this-file-does-not-exist')

    assert exp_platform_server.platform.send_file(local_file.name)

    contents = exp_platform_server.platform.read_file(str(remote_file))
    assert contents.decode('UTF-8').strip() == text
    assert None is exp_platform_server.platform.read_file(str(file_not_found))

    assert exp_platform_server.platform.get_file_size(str(remote_file)) > 0
    assert None is exp_platform_server.platform.get_file_size(str(file_not_found))

    assert exp_platform_server.platform.check_absolute_file_exists(str(remote_file))
    assert not exp_platform_server.platform.check_absolute_file_exists(str(file_not_found))

    assert exp_platform_server.platform.move_file(str(remote_file), str(file_not_found), must_exist=False)

    # Here, the variable names are misleading, as we moved the existing file over the non-existing one.
    assert not exp_platform_server.platform.delete_file(str(remote_file))
    assert exp_platform_server.platform.delete_file(str(file_not_found))


def test__load_ssh_config_missing_ssh_config(
        exp_platform_server: 'ExperimentPlatformServer', tmp_path: 'LocalPath', mocker):
    """Test that the user is warned when the expected SSH file cannot be located."""
    mocked_log = mocker.patch('autosubmit.platforms.paramiko_platform.Log')

    exp_platform_server.platform.config['AS_ENV_SSH_CONFIG_PATH'] = str(tmp_path / 'you-cannot-find-me')

    as_conf = mocker.MagicMock()
    as_conf.is_current_real_user_owner = False

    # TODO: We must be able to test that we are not loading the right SSH, without a mock here.
    mocker.patch('autosubmit.platforms.paramiko_platform._create_ssh_client', side_effect=ValueError)

    with pytest.raises(AutosubmitError):
        exp_platform_server.platform.connect(as_conf, reconnect=False, log_recovery_process=False)

    assert mocked_log.warning.called


@pytest.mark.docker
def test_test_connection(exp_platform_server: 'ExperimentPlatformServer', ssh_server: 'DockerContainer'):
    """Test that we can access files, send new files, move, delete."""
    exp_platform_server.platform.connect(None, reconnect=False, log_recovery_process=False)

    # TODO: This function is odd, if it reconnects, it will return ``"OK"``, but when it's all good
    #       then it will return ``None``.
    assert None is exp_platform_server.platform.test_connection(None)
