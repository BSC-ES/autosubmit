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

from dataclasses import dataclass
from getpass import getuser
from pathlib import Path
from typing import cast, Optional, Protocol, Union, TYPE_CHECKING

import pytest

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.log.log import AutosubmitCritical, AutosubmitError
from autosubmit.platforms.headers.slurm_header import SlurmHeader
from autosubmit.platforms.paramiko_submitter import ParamikoSubmitter

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from testcontainers.sftp import DockerContainer
    from test.integration.conftest import AutosubmitExperiment
    from autosubmit.platforms.psplatform import PsPlatform
    from autosubmit.platforms.slurmplatform import SlurmPlatform
    from pytest import FixtureRequest

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
def exp_platform_server(autosubmit_exp, ssh_server) -> ExperimentPlatformServer:
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
    submitter.load_platforms(as_conf=exp.as_conf)

    ps_platform: 'PsPlatform' = submitter.platforms[_PLATFORM_NAME]

    return ExperimentPlatformServer(exp, ps_platform, ssh_server)


@dataclass
class JobParametersPlatform:
    job: Job
    parameters: dict
    platform: 'SlurmPlatform'


class CreateJobParametersPlatformFixture(Protocol):

    def __call__(
            self,
            experiment_data: Optional[dict] = None
    ) -> JobParametersPlatform:
        ...


@pytest.fixture
def create_job_parameters_platform(autosubmit_exp) -> CreateJobParametersPlatformFixture:
    def job_parameters_platform(experiment_data: dict) -> JobParametersPlatform:
        exp = autosubmit_exp(_EXPID, experiment_data=experiment_data)
        slurm_platform: 'SlurmPlatform' = cast('SlurmPlatform', exp.platform)

        job = Job(f"{_EXPID}_SIM", 10000, Status.SUBMITTED, 0)
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

    It launches a Docker Image using the testcontainers library.
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


def test_send_file_errors(exp_platform_server: ExperimentPlatformServer):
    """Test possible errors when sending a file."""
    exp = exp_platform_server.experiment
    ps_platform = exp_platform_server.platform

    ps_platform.connect(as_conf=exp.as_conf, reconnect=False, log_recovery_process=False)
    assert ps_platform.check_remote_permissions()

    # Without this, the code will perform a check where it will reconnect.
    check = False

    # Fails if the connection is not active.
    ps_platform.closeConnection()
    with pytest.raises(AutosubmitError) as cm:
        ps_platform.send_file(__file__, check=check)
    assert 'Connection does not appear to be active' in str(cm.value.message)

    # Fails if there is a Python error.
    ps_platform._ftpChannel = None
    with pytest.raises(AutosubmitError) as cm:
        ps_platform.send_file('this-file-does-not-exist', check=check)
    assert 'An unexpected error occurred' in str(cm.value.message)


@pytest.mark.parametrize(
    'cmd,error,x11_enabled,mfa_enabled',
    [
        ('whoami', None, True, False),
        ('parangaricutirimicuaro', AutosubmitError, True, False),
        ('whoami', None, False, False),
        ('parangaricutirimicuaro', AutosubmitError, False, False),
        ('whoami', None, True, True),
        ('parangaricutirimicuaro', AutosubmitError, True, True),
        ('whoami', None, False, True),
        ('parangaricutirimicuaro', AutosubmitError, False, True)
    ]
)
@pytest.mark.docker
def test_send_command(cmd: str, error: Optional, x11_enabled: bool, mfa_enabled: bool, request: pytest.FixtureRequest,
                      mocker):
    """This test opens an SSH connection (via sftp) and sends a command."""
    if x11_enabled:
        request.applymarker('x11')
    if mfa_enabled:
        request.applymarker('mfa')

    exp_platform_server: ExperimentPlatformServer = request.getfixturevalue('exp_platform_server')

    if mfa_enabled:
        exp_platform_server.platform.two_factor_auth = mfa_enabled
        exp_platform_server.platform.two_factor_method = 'token'
        exp_platform_server.platform.pw = 'password'
        # 55192054 comes from the Docker setup for 2FA, see docker/ssh/linuxserverio-ssh-with-2fa-x11/README.md
        mocker.patch('autosubmit.platforms.paramiko_platform.input', return_value='55192054')

    exp_platform_server.platform.connect(None, reconnect=False, log_recovery_process=False)

    if error:
        assert exp_platform_server.platform.get_ssh_output_err() == ''
        with pytest.raises(error):
            exp_platform_server.platform.send_command(cmd, ignore_log=False, x11=x11_enabled)

        stderr = exp_platform_server.platform.get_ssh_output_err()
        assert 'command not found' in stderr
    else:
        assert exp_platform_server.platform.get_ssh_output() == ''
        assert exp_platform_server.platform.send_command(cmd, ignore_log=False, x11=x11_enabled)

        stdout = exp_platform_server.platform.get_ssh_output()
        user = getuser()
        assert user in stdout


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


@pytest.mark.parametrize(
    'command,x11,expected',
    [
        ('whoami', False, getuser() or "unknown"),
        ('parangaricutirimicuaro', False, ''),
        ('whoami', True, getuser() or "unknown"),
        ('parangaricutirimicuaro', True, ''),
    ],
    ids=[
        "valid command no X11",
        "invalid command no X11",
        "valid command X11",
        "invalid command X11"
    ]
)
@pytest.mark.docker
def test_exec_command_invalid_command(command: str, expected: str, x11: bool, request: 'pytest.FixtureRequest'):
    """This test opens an SSH connection (via sftp) and executes an invalid command."""
    if x11:
        request.applymarker('x11')
    exp_platform_server: 'ExperimentPlatformServer' = request.getfixturevalue('exp_platform_server')

    exp_platform_server.platform.connect(None, reconnect=False, log_recovery_process=False)

    stdin, stdout, stderr = exp_platform_server.platform.exec_command(command, x11=x11)
    assert stdin is not False
    assert stderr is not False
    # The stdout contents should be [b"user_name\n"]; thus the ugly list comprehension + extra code.
    assert expected == str(''.join([x.decode('UTF-8').strip() for x in stdout.readlines()]))


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
    'x11,retries,command',
    [
        [True, 2, "whoami"],
        [True, 2, "timeout 10 whoami"],
        [True, 2, "timeout 0 whoami"],
        [False, 2, "whoami"]
    ]
)
@pytest.mark.docker
def test_exec_command_ssh_session_not_active(x11: bool, retries: int, command: str, request: 'FixtureRequest'):
    """This test that we retry even if the SSH session gets closed."""
    if x11:
        request.applymarker('x11')

    exp_platform_server: 'ExperimentPlatformServer' = request.getfixturevalue('exp_platform_server')
    user = getuser() or "unknown"
    exp_platform_server.platform.connect(None, reconnect=False, log_recovery_process=False)

    # NOTE: We could simulate it the following way:
    #           ex = paramiko.SSHException('SSH session not active')
    #           mocker.patch.object(ps_platform.transport, 'open_session', side_effect=ex)
    #       But while that's OK, we can also avoid mocking by simply
    #       closing the connection.

    exp_platform_server.platform.transport.close()

    stdin, stdout, stderr = exp_platform_server.platform.exec_command(
        command,
        x11=x11,
        retries=retries
    )

    # This will be true iff the ``ps_platform.restore_connection(None)`` ran without errors.
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


@pytest.mark.parametrize(
    'x11_enabled,user_or_false',
    [
        [True, getuser()],
        [False, False]
    ],
    ids=[
        'X11 enabled and everything works',
        'No X11, returns a False bool'
    ]
)
@pytest.mark.docker
def test_exec_command_with_x11(x11_enabled: bool, user_or_false: Union[str, bool], request: pytest.FixtureRequest):
    """Tests connecting and executing a command when X11 is enabled and when it is disabled (parameters).

    Note, that we dynamically add the ``pytest.marker.x11`` based on a parameters.

    Also, after applying or not that marker, then we load ``exp_platform_server`` as that will load
    the other fixture ``ssh_server`` that uses the ``x11`` marker to customize the SSH image used.
    """
    if x11_enabled:
        request.applymarker('x11')

    exp_platform_server: ExperimentPlatformServer = request.getfixturevalue('exp_platform_server')

    exp = exp_platform_server.experiment
    ps_platform = exp_platform_server.platform

    ps_platform.connect(as_conf=exp.as_conf, reconnect=False, log_recovery_process=False)
    assert ps_platform.local_x11_display

    _, stdout, _ = ps_platform.exec_command('whoami', x11=True)

    if type(user_or_false) is bool:
        assert user_or_false == stdout
    else:
        assert user_or_false == stdout.readline().decode('UTF-8').strip()


@pytest.mark.x11
def test_xclock(exp_platform_server: ExperimentPlatformServer):
    """Tests connecting and executing a command when X11 is enabled and when it is disabled (parameters).

    Note, that we dynamically add the ``pytest.marker.x11`` based on a parameters.

    Also, after applying or not that marker, then we load ``exp_platform_server`` as that will load
    the other fixture ``ssh_server`` that uses the ``x11`` marker to customize the SSH image used.
    """
    exp = exp_platform_server.experiment
    ps_platform = exp_platform_server.platform

    ps_platform.connect(as_conf=exp.as_conf, reconnect=False, log_recovery_process=False)
    assert ps_platform.local_x11_display

    _, stdout, stderr = ps_platform.exec_command('timeout 1 xclock', x11=True)

    assert ''.join(stdout.readlines()) == ''
    assert ''.join(stderr.readlines()) == ''


@pytest.mark.docker
def test_test_connection_already_connected(exp_platform_server: ExperimentPlatformServer):
    """Test that calling ``test_connection`` does not interfere with an existing connection."""
    as_conf = exp_platform_server.experiment.as_conf
    platform = exp_platform_server.platform

    platform.connect(as_conf, reconnect=False, log_recovery_process=False)

    assert platform.connected
    assert platform.test_connection(as_conf) is None
    assert platform.connected


@pytest.mark.docker
def test_test_connection_new_connection(exp_platform_server: ExperimentPlatformServer):
    """Test that calling ``test_connection`` creates a new connection."""
    as_conf = exp_platform_server.experiment.as_conf
    platform = exp_platform_server.platform

    assert not platform.connected
    assert platform.test_connection(as_conf) == 'OK'
    assert platform.connected


@pytest.mark.docker
def test_test_connection_exceptions(mocker, exp_platform_server: ExperimentPlatformServer):
    """Test that ``reset`` raising an error, this error is handled correctly.

    Note that the behavior is a bit confusing, as depending on the exception
    raised we will re-raise it or raise another type. Thus, the ``raises(exception)``.
    """
    as_conf = exp_platform_server.experiment.as_conf
    platform = exp_platform_server.platform

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


@pytest.mark.docker
def test_test_restore_fails_random(mocker, exp_platform_server: ExperimentPlatformServer):
    """Test when ``restore`` raises a random exception we return a timeout message to the user (?)."""
    as_conf = exp_platform_server.experiment.as_conf
    platform = exp_platform_server.platform

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


@pytest.mark.docker
def test_test_restore_fails_does_not_accept(mocker, exp_platform_server: ExperimentPlatformServer):
    """Test when ``restore`` raises a random exception which message includes certain text it returns it."""
    as_conf = exp_platform_server.experiment.as_conf
    platform = exp_platform_server.platform

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
@pytest.mark.docker
def test_get_header_serial_parallel(processors: str, create_job_parameters_platform):
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


def test_get_header_job_het(create_job_parameters_platform):
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
