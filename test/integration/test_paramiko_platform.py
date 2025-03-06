#
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
from typing import cast, Any, Callable, Optional, Protocol, Union, TYPE_CHECKING

import paramiko
import pytest
from paramiko import ChannelFile  # type: ignore[import]

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.log.log import AutosubmitCritical
from autosubmit.log.log import AutosubmitError
from autosubmit.platforms.headers.slurm_header import SlurmHeader
from autosubmit.platforms.paramiko_submitter import ParamikoSubmitter
from autosubmit.platforms.slurmplatform import SlurmPlatform

if TYPE_CHECKING:
    from autosubmit.platforms.psplatform import PsPlatform
    from autosubmit.platforms.slurmplatform import SlurmPlatform
    from docker.models.containers import Container
    # noinspection PyProtectedMember
    from _pytest._py.path import LocalPath
    from pytest import FixtureRequest
    from testcontainers.core.container import DockerContainer  # type: ignore
    from test.integration.conftest import AutosubmitExperiment

_PLATFORM_NAME = 'TEST_PS_PLATFORM'
_PLATFORM_REMOTE_DIR = '/app/'
_PLATFORM_PROJECT = 'test'


@pytest.fixture
def get_experiment(autosubmit_exp) -> Callable[['FixtureRequest'], 'AutosubmitExperiment']:
    def _get_experiment(request: 'FixtureRequest') -> 'AutosubmitExperiment':
        test_name = request.node.name
        platform_scratch_dir = Path(_PLATFORM_REMOTE_DIR, test_name)
        # TODO: The necessity to create a fixture here, instead of using ``autosubmit_exp``,
        #       shows that we need to rethink the design of how platforms are loaded. In our
        #       code we have the configuration object here, but it'd be simpler to maybe
        #       have a utility code or fixture that given an object or some value, it creates
        #       the platform and maybe the configuration to be inserted here... or the
        #       configuration being created from the objects... It is really cumbersome
        #       to use Autosubmit API directly, especially what came from config-parser
        #       (which is new in AS4, but already feels like legacy sometimes).
        experiment_data = {
            'PLATFORMS': {
                _PLATFORM_NAME: {
                    'TYPE': 'ps',
                    'HOST': 'localhost',
                    'PROJECT': _PLATFORM_PROJECT,
                    'USER': getuser(),
                    'SCRATCH_DIR': str(platform_scratch_dir),
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
        }

        return autosubmit_exp(experiment_data=experiment_data)

    return _get_experiment


def _get_platform(exp: 'AutosubmitExperiment') -> 'PsPlatform':
    # We load the platforms with the submitter so that the platforms have all attributes.
    # NOTE: The set up of platforms is done partially in the platform constructor and
    #       partially by a submitter (i.e., they are tightly coupled, which makes it hard
    #       to maintain and test).
    submitter = ParamikoSubmitter(as_conf=exp.as_conf)

    assert submitter.platforms
    ps_platform: 'PsPlatform' = cast('PsPlatform', submitter.platforms[_PLATFORM_NAME])

    return ps_platform


@dataclass
class JobParametersPlatform:
    job: Job
    parameters: dict
    platform: 'SlurmPlatform'


class CreateJobParametersPlatformFixture(Protocol):

    def __call__(
            self,
            experiment_data: Optional[dict] = None,
            /,
            *args: Any,
            **kwargs: Any
    ) -> JobParametersPlatform:
        ...


@pytest.fixture
def create_job_parameters_platform(
        autosubmit_exp, get_next_expid: Callable[[], str]) -> CreateJobParametersPlatformFixture:
    def job_parameters_platform(experiment_data: Optional[dict]) -> JobParametersPlatform:
        exp = autosubmit_exp(get_next_expid(), experiment_data=experiment_data, include_jobs=True)
        slurm_platform: 'SlurmPlatform' = cast('SlurmPlatform', exp.platform)

        job = Job(f"{exp.expid}_SIM", 10000, Status.SUBMITTED, 0)
        job.section = 'SIM'
        job.het = {}
        job._platform = slurm_platform

        parameters = job.update_parameters(exp.as_conf, set_attributes=True, reset_logs=False)

        return JobParametersPlatform(
            job,
            parameters,
            slurm_platform
        )

    return job_parameters_platform


@pytest.mark.parametrize(
    'filename',
    [
        'test1',
        'sub/test2'
    ],
    ids=['filename', 'filename_long_path']
)
@pytest.mark.ssh
@pytest.mark.docker
def test_send_file(
        filename: str,
        get_experiment: Callable[['FixtureRequest'], 'AutosubmitExperiment'],
        request: 'FixtureRequest',
        ssh_server: 'Container'
):
    """This test opens an SSH connection (via sftp) and sends a file to the remote location.

    It launches a Docker Image using the testcontainers library.
    """
    test_name = request.node.name
    user = getuser()

    exp = get_experiment(request)
    exp_ps_platform = _get_platform(exp)

    try:
        exp_ps_platform.connect(as_conf=exp.as_conf, reconnect=False, log_recovery_process=False)
        assert exp_ps_platform.check_remote_permissions()

        platform_tmp_path = Path(exp_ps_platform.tmp_path)

        # generate the file
        if "/" in filename:
            filename_dir = Path(filename).parent
            Path(platform_tmp_path, filename_dir).mkdir(parents=True, exist_ok=True)
            filename = Path(filename).name

        with open(str(Path(platform_tmp_path, filename)), 'w') as f:
            f.write('test')

        assert exp_ps_platform.send_file(filename)

        file = Path(
            _PLATFORM_REMOTE_DIR,
            test_name,
            _PLATFORM_PROJECT,
            user,
            exp.expid,
            f'LOG_{exp.expid}/{filename}'
        )
        result = ssh_server.exec_run(f'ls {str(file)}')
        assert result.exit_code == 0
    finally:
        exp_ps_platform.close_connection()


def test_send_file_errors(
        request: 'FixtureRequest',
        get_experiment: Callable[['FixtureRequest'], 'AutosubmitExperiment'],
        ssh_server: 'DockerContainer'
):
    """Test possible errors when sending a file."""
    exp = get_experiment(request)
    exp_ps_platform = _get_platform(exp)

    try:
        exp_ps_platform.connect(as_conf=exp.as_conf, reconnect=False, log_recovery_process=False)
        assert exp_ps_platform.check_remote_permissions()

        # Without this, the code will perform a check where it will reconnect.
        check = False

        # Fails if the connection is not active.
        exp_ps_platform.close_connection()
        with pytest.raises(AutosubmitError) as cm:
            exp_ps_platform.send_file(__file__, check=check)
        assert 'Connection does not appear to be active' in str(cm.value.message)

        # Fails if there is a Python error.
        exp_ps_platform._ftpChannel = None
        with pytest.raises(AutosubmitError) as cm:
            exp_ps_platform.send_file('this-file-does-not-exist', check=check)
        assert 'An unexpected error occurred' in str(cm.value.message)
    finally:
        exp_ps_platform.close_connection()


@pytest.mark.parametrize(
    'cmd,error,ssh_fixture,x11_enabled,mfa_enabled',
    [
        pytest.param(
            'whoami', None, 'ssh_x11_server', True, False,
            id='Server with X11, connected with X11, whoami command works'
        ),
        pytest.param(
            'parangaricutirimicuaro', AutosubmitError, 'ssh_x11_server', True, False,
            id='Server with X11, connected with X11, invalid command parangaricutirimicuaro error'
        ),
        pytest.param(
            'whoami', None, 'ssh_server', False, False,
            id='Server without X11, whoami command works'
        ),
        pytest.param(
            'whoami', None, 'ssh_x11_server', False, False,
            id='Server with X11, connected without X11, whoami command works'
        ),
        pytest.param(
            'parangaricutirimicuaro', AutosubmitError, 'ssh_x11_server', False, False,
            id='Server with X11, connected without X11, invalid command parangaricutirimicuaro error'
        ),
        pytest.param(
            'whoami', None, 'ssh_x11_mfa_server', True, True,
            id='Server with X11 and MFA, connected with X11 and MFA, whoami command works'
        ),
        pytest.param(
            'parangaricutirimicuaro', AutosubmitError, 'ssh_x11_mfa_server', True, True,
            id='Server with X11 and MFA, connected with X11 and MFA, invalid command parangaricutirimicuaro error'
        ),
        pytest.param(
            'whoami', None, 'ssh_x11_mfa_server', False, True,
            id='Server with X11 and with MFA, connected without X11 and with MFA, whoami command works'
        ),
        pytest.param(
            'parangaricutirimicuaro', AutosubmitError, 'ssh_x11_mfa_server', False, True,
            id='Server with X11 and MFA, connected without X11 and with MFA, '
               'invalid command parangaricutirimicuaro error'
        )
    ],
    indirect=['ssh_fixture'],
)
@pytest.mark.x11
@pytest.mark.mfa
@pytest.mark.ssh
@pytest.mark.docker
def test_send_command(
        cmd: str, error: Optional[Exception],
        ssh_fixture: 'DockerContainer',
        mfa_enabled: bool,
        x11_enabled: bool,
        mocker,
        request: 'FixtureRequest',
        get_experiment: Callable[['FixtureRequest'], 'AutosubmitExperiment']
):
    """This test opens an SSH connection (via sftp) and sends a command."""
    exp = get_experiment(request)
    exp_ps_platform = _get_platform(exp)

    if mfa_enabled:
        # NOTE: The container is configured for
        exp_ps_platform.two_factor_auth = mfa_enabled
        exp_ps_platform.two_factor_method = 'token'
        password = 'password'
        exp_ps_platform.pw = password  # our platform does not have a password
        mocker.patch('autosubmit.platforms.paramiko_platform.getpass.getpass', return_value=password)
        # 55192054 comes from the Docker setup for 2FA, see docker/ssh/linuxserverio-ssh-with-2fa-x11/README.md
        mocker.patch('autosubmit.platforms.paramiko_platform.input', return_value='55192054')

    try:
        exp_ps_platform.connect(exp.as_conf, reconnect=False, log_recovery_process=False)

        if error:
            assert exp_ps_platform.get_ssh_output_err() == ''
            with pytest.raises(error):  # type: ignore
                exp_ps_platform.send_command(cmd, ignore_log=False, x11=x11_enabled)

            stderr = exp_ps_platform.get_ssh_output_err()
            assert 'command not found' in stderr
        else:
            assert exp_ps_platform.get_ssh_output() == ''
            assert exp_ps_platform.send_command(cmd, ignore_log=False, x11=x11_enabled)

            stdout = exp_ps_platform.get_ssh_output()
            user = getuser()
            assert user in stdout
    finally:
        exp_ps_platform.close_connection()


@pytest.mark.parametrize(
    'cmd,timeout',
    [
        ('rsync --version', None),
        ('rm --help', 60),
        ('whoami', 120)
    ]
)
@pytest.mark.ssh
@pytest.mark.docker
def test_send_command_timeout_error_exec_command(
        cmd: str,
        timeout: Optional[int],
        mocker,
        request: 'FixtureRequest',
        get_experiment: Callable[['FixtureRequest'], 'AutosubmitExperiment'],
        ssh_server: 'DockerContainer'
):
    """Test that the correct timeout is used, and that ``exec_command`` raises ``AutosubmitError``."""
    exp = get_experiment(request)
    exp_ps_platform = _get_platform(exp)

    try:
        exp_ps_platform.connect(exp.as_conf, reconnect=False, log_recovery_process=False)

        # Capture platform log.
        mocker.patch('autosubmit.platforms.paramiko_platform.Log')
        # Simulate an error occurred, and retrying did not fix it.
        mocker.patch.object(exp_ps_platform, 'exec_command', return_value=(False, False, False))

        with pytest.raises(AutosubmitError) as cm:
            exp_ps_platform.send_command(command=cmd, ignore_log=False, x11=False)


        assert 'Failed to send' in str(cm.value.message)
        assert 6005 == cm.value.code
    finally:
        exp_ps_platform.close_connection()


@pytest.mark.ssh
@pytest.mark.docker
def test_exec_command(
        request: 'FixtureRequest',
        get_experiment: Callable[['FixtureRequest'], 'AutosubmitExperiment'],
        ssh_server: 'DockerContainer'
):
    """This test opens an SSH connection (via sftp) and executes a command."""
    exp = get_experiment(request)
    exp_ps_platform = _get_platform(exp)

    try:
        exp_ps_platform.connect(exp.as_conf, reconnect=False, log_recovery_process=False)

        stdin, stdout, stderr = exp_ps_platform.exec_command('whoami')
        assert stdin is not False
        assert stderr is not False
        # The stdout contents should be [b"user_name\n"]; thus the ugly list comprehension + extra code.
        assert isinstance(stdout, ChannelFile)
        assert getuser() == str(''.join([x.decode('UTF-8').strip() for x in stdout.readlines()]))
    finally:
        exp_ps_platform.close_connection()


@pytest.mark.parametrize(
    'ssh_fixture,command,x11,expected',
    [
        pytest.param(
            'ssh_server', 'whoami', False, getuser(), id='valid command no X11'
        ),
        pytest.param(
            'ssh_server', 'parangaricutirimicuaro', False, '', id='invalid command no X11'
        ),
        pytest.param(
            'ssh_x11_server', 'whoami', True, getuser(), id='valid command X11'
        ),
        pytest.param(
            'ssh_x11_server', 'parangaricutirimicuaro', True, '', id='invalid command X11'
        )
    ],
    indirect=['ssh_fixture']
)
@pytest.mark.x11
@pytest.mark.ssh
@pytest.mark.docker
def test_exec_command_invalid_command(
        ssh_fixture: 'DockerContainer',
        command: str,
        x11: bool,
        expected: str,
        request: 'FixtureRequest',
        get_experiment: Callable[['FixtureRequest'], 'AutosubmitExperiment'],
):
    """This test opens an SSH connection (via sftp) and executes an invalid command.

    Some tests have X11 enabled, and others do not.
    """
    exp = get_experiment(request)
    exp_ps_platform = _get_platform(exp)

    try:
        exp_ps_platform.connect(exp.as_conf, reconnect=False, log_recovery_process=False)

        stdin, stdout, stderr = exp_ps_platform.exec_command(command, x11=x11)

        # It must be able to connect, so no False's here
        assert isinstance(stdout, ChannelFile)
        assert stdin is not False
        assert stderr is not False

        if expected:
            # The stdout contents should be [b"user_name\n"]; thus the ugly list comprehension + extra code.
            assert expected == str(''.join([x.decode('UTF-8').strip() for x in stdout.readlines()]))
        else:
            err_output = str(''.join([x.decode('UTF-8').strip() for x in stderr.readlines()]))
            # cmd not found error
            assert command in err_output
    finally:
        exp_ps_platform.close_connection()


@pytest.mark.ssh
@pytest.mark.docker
def test_exec_command_after_a_reset(
        request: 'FixtureRequest',
        get_experiment: Callable[['FixtureRequest'], 'AutosubmitExperiment'],
        ssh_server: 'DockerContainer'
):
    """Test that after a connection reset we are still able to execute commands."""
    exp = get_experiment(request)
    exp_ps_platform = _get_platform(exp)

    try:
        exp_ps_platform.connect(exp.as_conf, reconnect=False, log_recovery_process=False)
        exp_ps_platform.reset()
        exp_ps_platform.connect(exp.as_conf, reconnect=False, log_recovery_process=False)

        stdin, stdout, stderr = exp_ps_platform.exec_command('whoami')
        assert isinstance(stdout, ChannelFile)
        assert stdin is not False
        assert stderr is not False
        # The stdout contents should be [b"user_name\n"]; thus the ugly list comprehension + extra code.
        assert getuser() == str(''.join([x.decode('UTF-8').strip() for x in stdout.readlines()]))
    finally:
        exp_ps_platform.close_connection()


@pytest.mark.parametrize(
    'x11,retries,command',
    [
        [True, 2, "whoami"],
        [True, 2, "timeout 10 whoami"],
        [True, 2, "timeout 0 whoami"],
        [False, 2, "whoami"]
    ]
)
@pytest.mark.x11
@pytest.mark.ssh
@pytest.mark.docker
def test_exec_command_ssh_session_not_active(
        x11: bool,
        retries: int,
        command: str,
        request: 'FixtureRequest',
        get_experiment: Callable[['FixtureRequest'], 'AutosubmitExperiment'],
        ssh_x11_server
):
    """This test that we retry even if the SSH session gets closed."""
    exp = get_experiment(request)
    exp_ps_platform = _get_platform(exp)

    user = getuser()

    try:
        exp_ps_platform.connect(exp.as_conf, reconnect=False, log_recovery_process=False)

        # NOTE: We could simulate it the following way:
        #           ex = paramiko.SSHException('SSH session not active')
        #           mocker.patch.object(ps_platform.transport, 'open_session', side_effect=ex)
        #       But while that's OK, we can also avoid mocking by simply
        #       closing the connection.

        assert exp_ps_platform.transport
        exp_ps_platform.transport.close()

        stdin, stdout, stderr = exp_ps_platform.exec_command(
            command,
            x11=x11,
            retries=retries
        )

        # This will be true iff the ``ps_platform.restore_connection(None)`` ran without errors.
        assert isinstance(stdout, ChannelFile)
        assert stdin is not False
        assert stderr is not False
        # The stdout contents should be [b"user_name\n"]; thus the ugly list comprehension + extra code.
        assert user == str(''.join([x.decode('UTF-8').strip() for x in stdout.readlines()]))
    finally:
        exp_ps_platform.close_connection()


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
@pytest.mark.ssh
@pytest.mark.docker
def test_exec_command_socket_error(
        error: Exception,
        mocker,
        request: 'FixtureRequest',
        get_experiment: Callable[['FixtureRequest'], 'AutosubmitExperiment'],
        ssh_server: 'DockerContainer'
):
    """Test that the command is retried and succeeds even when a socket error occurs."""
    exp = get_experiment(request)
    ps_platform = _get_platform(exp)

    user = getuser()
    try:
        ps_platform.connect(exp.as_conf, reconnect=False, log_recovery_process=False)

        ps_platform.transport.close()

        mocker.patch.object(ps_platform.transport, 'open_session', side_effect=error)

        stdin, stdout, stderr = ps_platform.exec_command('whoami')
        assert stdin is not False
        assert stderr is not False
        # The stdout contents should be [b"user_name\n"]; thus the ugly list comprehension + extra code.
        assert user == str(''.join([x.decode('UTF-8').strip() for x in stdout.readlines()]))
    finally:
        ps_platform.close_connection()


@pytest.mark.ssh
@pytest.mark.docker
def test_exec_command_ssh_session_not_active_cannot_restore(
        request: 'FixtureRequest',
        get_experiment: Callable[['FixtureRequest'], 'AutosubmitExperiment'],
        ssh_server,
        mocker
):
    """Test that when an error occurs, and it cannot restore, then we return falsey values."""
    exp = get_experiment(request)
    exp_ps_platform = _get_platform(exp)

    try:
        exp_ps_platform.connect(exp.as_conf, reconnect=False, log_recovery_process=False)

        exp_ps_platform.close_connection()

        # This dummy mock prevents the platform from being able to restore its connection.
        mocker.patch.object(exp_ps_platform, 'restore_connection')

        stdin, stdout, stderr = exp_ps_platform.exec_command('whoami')
        assert stdin is False
        assert stdout is False
        assert stderr is False
    finally:
        exp_ps_platform.close_connection()


@pytest.mark.ssh
@pytest.mark.docker
def test_fs_operations(
        request: 'FixtureRequest',
        get_experiment: Callable[['FixtureRequest'], 'AutosubmitExperiment'],
        ssh_server,
        tmp_path: 'LocalPath'
):
    """Test that we can access files, send new files, move, delete."""
    exp = get_experiment(request)
    exp_ps_platform = _get_platform(exp)

    test_name = request.node.name
    user = getuser()

    local_file = Path(exp_ps_platform.tmp_path, 'test.txt')
    text = 'Lorem ipsum'

    with open(local_file, 'w+') as f:
        f.write(text)

    remote_file = Path(_PLATFORM_REMOTE_DIR, test_name, _PLATFORM_PROJECT, user, exp.expid,
                       f'LOG_{exp.expid}', local_file.name)

    try:
        exp_ps_platform.connect(exp.as_conf, reconnect=False, log_recovery_process=False)

        file_not_found = Path('/app', test_name, 'this-file-does-not-exist')

        assert exp_ps_platform.send_file(local_file.name)

        contents = exp_ps_platform.read_file(str(remote_file))
        assert contents
        assert contents.decode('UTF-8').strip() == text
        assert exp_ps_platform.read_file(str(file_not_found)) is None

        file_size: Optional[int] = exp_ps_platform.get_file_size(str(remote_file))
        assert file_size
        assert file_size > 0
        assert exp_ps_platform.get_file_size(str(file_not_found)) is None

        assert exp_ps_platform.check_absolute_file_exists(str(remote_file))
        assert not exp_ps_platform.check_absolute_file_exists(str(file_not_found))

        assert exp_ps_platform.move_file(str(remote_file), str(file_not_found), must_exist=False)

        # Here, the variable names are misleading, as we moved the existing file over the non-existing one.
        assert not exp_ps_platform.delete_file(str(remote_file))
        assert exp_ps_platform.delete_file(str(file_not_found))
    finally:
        exp_ps_platform.close_connection()


@pytest.mark.parametrize(
    'ssh_fixture,x11,user_or_false',
    [
        pytest.param(
            'ssh_x11_server',
            True,
            getuser(),
            id='X11 enabled and everything works'
        ),
        pytest.param(
            'ssh_x11_server',
            False,
            getuser(),
            id='X11 disabled and everything still works'
        )
    ],
    indirect=['ssh_fixture']
)
@pytest.mark.x11
@pytest.mark.ssh
@pytest.mark.docker
def test_exec_command_with_x11(
        ssh_fixture: 'DockerContainer',
        x11: bool,
        user_or_false: Union[str, bool],
        request: 'FixtureRequest',
        get_experiment: Callable[['FixtureRequest'], 'AutosubmitExperiment']
):
    """Tests connecting and executing a command when X11 is enabled and when it is disabled (parameters)."""
    exp = get_experiment(request)
    ps_platform = _get_platform(exp)

    try:
        ps_platform.connect(as_conf=exp.as_conf, reconnect=False, log_recovery_process=False)
        assert ps_platform.local_x11_display

        stdin, stdout, stderr = ps_platform.exec_command('whoami', x11=x11)

        assert isinstance(stdout, ChannelFile), f"Invalid value for stdout: {stderr, stdout}"
        assert user_or_false == stdout.readline().decode('UTF-8').strip()
    finally:
        ps_platform.close_connection()


@pytest.mark.x11
@pytest.mark.ssh
@pytest.mark.docker
def test_xclock(
        request: 'FixtureRequest',
        get_experiment: Callable[['FixtureRequest'], 'AutosubmitExperiment'],
        ssh_x11_server
):
    """Tests connecting and executing a command when X11 is enabled and when it is disabled (parameters).

    Note, that we dynamically add the ``pytest.marker.x11`` based on a parameters.

    Also, after applying or not that marker, then we load ``exp_platform_server`` as that will load
    the other fixture ``ssh_server`` that uses the ``x11`` marker to customize the SSH image used.
    """
    exp = get_experiment(request)
    exp_ps_platform = _get_platform(exp)

    try:
        exp_ps_platform.connect(as_conf=exp.as_conf, reconnect=False, log_recovery_process=False)
        assert exp_ps_platform.local_x11_display

        _, stdout, stderr = exp_ps_platform.exec_command('timeout 1 xclock', x11=True)

        assert isinstance(stdout, ChannelFile), stdout
        assert isinstance(stderr, ChannelFile), stderr

        out_content = ''.join(stdout.readlines())
        err_content = ''.join(stderr.readlines())

        assert out_content == '', out_content
        assert err_content == '', err_content
    finally:
        exp_ps_platform.close_connection()


@pytest.mark.ssh
@pytest.mark.docker
def test_test_connection_already_connected(
        request: 'FixtureRequest',
        get_experiment: Callable[['FixtureRequest'], 'AutosubmitExperiment'],
        ssh_server
):
    """Test that calling ``test_connection`` does not interfere with an existing connection."""
    exp = get_experiment(request)
    platform = _get_platform(exp)
    as_conf = exp.as_conf

    try:
        platform.connect(as_conf, reconnect=False, log_recovery_process=False)

        assert platform.connected
        assert platform.test_connection(as_conf) is None
        assert platform.connected
    finally:
        platform.close_connection()


@pytest.mark.ssh
@pytest.mark.docker
def test_test_connection_new_connection(
        request: 'FixtureRequest',
        get_experiment: Callable[['FixtureRequest'], 'AutosubmitExperiment'],
        ssh_server
):
    """Test that calling ``test_connection`` creates a new connection."""
    exp = get_experiment(request)
    platform = _get_platform(exp)
    as_conf = exp.as_conf

    assert not platform.connected
    assert platform.test_connection(as_conf) == 'OK'
    assert platform.connected


@pytest.mark.ssh
@pytest.mark.docker
def test_test_connection_exceptions(
        mocker,
        request: 'FixtureRequest',
        get_experiment: Callable[['FixtureRequest'], 'AutosubmitExperiment'],
        ssh_server
):
    """Test that ``reset`` raising an error, this error is handled correctly.

    Note that the behavior is a bit confusing, as depending on the exception
    raised we will re-raise it or raise another type. Thus, the ``raises(exception)``.
    """
    exp = get_experiment(request)
    platform = _get_platform(exp)
    as_conf = exp.as_conf

    # NOTE: pytest.parametrize normally would be better here, but it takes a lot
    #       longer to create a new container, so in this case we are re-using it
    #       as the call to ``reset`` is mocked. In a local test the parametrized
    #       version took nearly 01m30s, while this version took <20s.
    for error_raised in [
        EOFError,
        AutosubmitError,
        AutosubmitCritical,
        IOError,
        ValueError,
        Exception
    ]:
        mocker.patch.object(platform, 'reset', side_effect=error_raised)

        assert not platform.connected
        with pytest.raises(Exception) as cm:
            platform.test_connection(as_conf)

        if error_raised in [AutosubmitError, AutosubmitCritical, IOError]:
            assert isinstance(cm.value, error_raised)
        elif error_raised is EOFError:
            assert isinstance(cm.value, AutosubmitError)
        else:
            assert isinstance(cm.value, AutosubmitCritical)


@pytest.mark.ssh
@pytest.mark.docker
def test_test_restore_fails_random(
        mocker,
        request: 'FixtureRequest',
        get_experiment: Callable[['FixtureRequest'], 'AutosubmitExperiment'],
        ssh_server
):
    """Test when ``restore`` raises a random exception we return a timeout message to the user (?)."""
    exp = get_experiment(request)
    platform = _get_platform(exp)
    as_conf = exp.as_conf

    error_message = 'I am random'

    mocker.patch.object(platform, 'restore_connection', side_effect=Exception(error_message))

    assert not platform.connected
    message = platform.test_connection(as_conf)

    # TODO: Why not raise an exception in test_connection, this way we will have
    #       the message and more context, plus we will have the option to handle
    #       the error -- if we ever need it from the user/caller side.
    #       Plus, the error here is clearly a random Exception that is hidden
    #       from the user. Instead, we treat it as a timeout, which does not make
    #       much sense...
    assert message == 'Timeout connection'


@pytest.mark.ssh
@pytest.mark.docker
def test_test_restore_fails_does_not_accept(
        mocker,
        request: 'FixtureRequest',
        get_experiment: Callable[['FixtureRequest'], 'AutosubmitExperiment'],
        ssh_server
):
    """Test when ``restore`` raises a random exception which message includes certain text it returns it."""
    exp = get_experiment(request)
    platform = _get_platform(exp)
    as_conf = exp.as_conf

    # TODO: This looks a bit fragile/buggy?
    error_message = 'The plot accept remote connections! Fear not!'

    mocker.patch.object(platform, 'restore_connection', side_effect=Exception(error_message))

    assert not platform.connected
    message = platform.test_connection(as_conf)

    assert message == error_message


@pytest.mark.parametrize(
    'processors',
    ['1', '2'],
    ids=['serial', 'parallel']
)
@pytest.mark.ssh
@pytest.mark.docker
def test_get_header_serial_parallel(
        processors: str, create_job_parameters_platform: CreateJobParametersPlatformFixture):
    """Test that when a job contains heterogeneous dictionary it calculates the het/ header."""
    # TODO: There is something wrong here, as only Slurm header contains this function,
    #       but this is not really enforced (no common interface, the code looks a bit
    #       at risk of calling that function from another header, causing a runtime error).
    job_parameters_platform = create_job_parameters_platform(experiment_data={})

    platform = job_parameters_platform.platform

    a_paramiko_platform_header = SlurmHeader()
    platform._header = a_paramiko_platform_header

    job_parameters_platform.job.processors = processors

    header = platform.get_header(job_parameters_platform.job, job_parameters_platform.parameters)
    assert header


def test_get_header_job_het(create_job_parameters_platform: CreateJobParametersPlatformFixture):
    """Test that when a job contains heterogeneous dictionary it calculates the het/ header."""
    # TODO: There is something wrong here, as only Slurm header contains this function,
    #       but this is not really enforced (no common interface, the code looks a bit
    #       at risk of calling that function from another header, causing a runtime error).
    job_parameters_platform = create_job_parameters_platform(experiment_data={})

    platform = job_parameters_platform.platform

    a_paramiko_platform_header = SlurmHeader()
    platform._header = a_paramiko_platform_header

    hetsize = 2

    job_parameters_platform.job.het = job_parameters_platform.parameters.copy()
    job_parameters_platform.job.het['HETSIZE'] = hetsize
    job_parameters_platform.job.het['NUMTHREADS'] = [i for i in range(0, hetsize)]
    job_parameters_platform.job.het['TASKS'] = [i for i in range(0, hetsize)]

    header = platform.get_header(job_parameters_platform.job, job_parameters_platform.parameters)
    assert header

    # FIXME: I thought this was supposed to be equal to the number of hetsize
    #        components (2, set above), and at some point during debugging it is;
    #        but then there is a call to other functions somewhere that reset it
    #        to just one hetjob. Looks like it might be better to create the job
    #        from the dictionary configuration, instead of trying to create the
    #        object here (i.e. an integration test that loads everything from
    #        YAML configuration).
    assert header.count('hetjob') > 0


@pytest.mark.parametrize(
    'provided_jobs,real_completed_jobs,expected_result',
    [
        (['job1', 'job2', 'job3'], ['job1', 'job2'], ['job1', 'job2']),
        ([], ['job1', 'job2'], ['job1', 'job2']),
        (['job1', 'job2'], [], []),
        ([], [], []),
    ], ids=[
        'Some jobs completed from provided list',
        'All completed jobs when no provided list',
        'No completed jobs from provided list',
        'No completed jobs when no provided list'
    ]
)
@pytest.mark.ssh
@pytest.mark.docker
def test_get_completed_job_names(
        provided_jobs: list[str],
        real_completed_jobs: list[str],
        expected_result: list[str],
        request: 'FixtureRequest',
        get_experiment: Callable[['FixtureRequest'], 'AutosubmitExperiment'],
        ssh_server
):
    exp = get_experiment(request)
    platform = _get_platform(exp)
    try:
        platform.connect(exp.as_conf, reconnect=False, log_recovery_process=False)
        platform.remote_log_dir = f"/tmp/{platform.expid}/autosubmit_test_logs/"
        platform.send_command(f"mkdir -p {platform.remote_log_dir}", ignore_log=True)
        for job_name in real_completed_jobs:
            completed_file = Path(platform.remote_log_dir) / f"{job_name}_COMPLETED"
            platform.send_command(f"touch {completed_file}", ignore_log=True)

        completed_jobs = platform.get_completed_job_names(
            job_names=provided_jobs
        )

        for job in expected_result:
            assert job in completed_jobs
    finally:
        platform.close_connection()


@pytest.mark.parametrize(
    'jobs_to_delete,real_completed_jobs,expected_result',
    [
        (['job1', 'job2', 'job3'], ['job1', 'job2'], []),
        (['job1', 'job2'], ['job1', 'job2', 'job3'], ['job3']),
        ([], ['job1', 'job2', 'job3'], ['job1', 'job2', 'job3']),
        ([], [], []),
    ], ids=[
        'Delete some completed jobs from provided list',
        'Delete all completed jobs from provided list',
        'Delete no completed jobs when no provided list',
        'Delete no completed jobs when no completed jobs'
    ]
)
@pytest.mark.ssh
@pytest.mark.docker
def test_deleted_failed_and_completed_names(
        jobs_to_delete: list[str],
        real_completed_jobs: list[str],
        expected_result: list[str],
        request: 'FixtureRequest',
        get_experiment: Callable[['FixtureRequest'], 'AutosubmitExperiment'],
        ssh_server
):
    exp = get_experiment(request)
    platform = _get_platform(exp)
    try:
        platform.connect(exp.as_conf, reconnect=False, log_recovery_process=False)
        platform.remote_log_dir = f"/tmp/{platform.expid}/autosubmit_test_logs/"
        platform.send_command(f"mkdir -p {platform.remote_log_dir}", ignore_log=True)
        for job_name in real_completed_jobs:
            completed_file = Path(platform.remote_log_dir) / f"{job_name}_COMPLETED"
            platform.send_command(f"touch {completed_file}", ignore_log=True)

        platform.delete_failed_and_completed_names(
            job_names=jobs_to_delete
        )

        # assert
        platform.send_command(
            f"ls -1 {platform.remote_log_dir}/*_COMPLETED | xargs -n1 basename", ignore_log=True
        )
        for job in expected_result:
            assert job in platform.get_ssh_output()
    finally:
        platform.close_connection()


@pytest.mark.ssh
@pytest.mark.docker
def test_test_connection(
        request: 'FixtureRequest',
        get_experiment: Callable[['FixtureRequest'], 'AutosubmitExperiment'],
        ssh_server
):
    """Test that we can access files, send new files, move, delete."""
    exp = get_experiment(request)
    exp_ps_platform = _get_platform(exp)
    try:
        exp_ps_platform.connect(exp.as_conf, reconnect=False, log_recovery_process=False)

        # TODO: This function is odd, if it reconnects, it will return ``"OK"``, but when it's all good
        #       then it will return ``None``.
        assert None is exp_ps_platform.test_connection(None)
    finally:
        exp_ps_platform.close_connection()


@pytest.mark.ssh
@pytest.mark.docker
def test_failed_connection_raises_as_error(
        autosubmit_exp,
        tmp_path,
        ssh_server: 'DockerContainer'
):
    """Test that failing to restore a connection, even with retries, results in ``AutosubmitError``."""
    exp = autosubmit_exp()
    platform_config = {
        "LOCAL_ROOT_DIR": exp.as_conf.basic_config.LOCAL_ROOT_DIR,
        "LOCAL_TMP_DIR": str(tmp_path),
        "LOCAL_ASLOG_DIR": str(tmp_path)
    }
    slurm_platform = SlurmPlatform(exp.expid, 'slurm_platform', platform_config, None)
    with pytest.raises(AutosubmitError):
        slurm_platform.restore_connection(None, log_recovery_process=False)
