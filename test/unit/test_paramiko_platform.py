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

from getpass import getuser
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Generator, Optional

import pytest

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.log.log import AutosubmitError, AutosubmitCritical
# noinspection PyProtectedMember
from autosubmit.platforms.paramiko_platform import ParamikoPlatform, ParamikoPlatformException
from autosubmit.platforms.psplatform import PsPlatform


@pytest.fixture
def paramiko_platform() -> Generator[ParamikoPlatform, None, None]:
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
def ps_platform(tmpdir) -> Generator[tuple[PsPlatform, Path], None, None]:
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


def test_paramiko_platform_exception():
    e = ParamikoPlatformException('test')
    assert e.message == 'test'


def test_paramiko_platform_constructor(paramiko_platform):
    platform = paramiko_platform
    assert platform.name == 'local'
    assert platform.expid == 'a000'
    assert platform.config["LOCAL_ROOT_DIR"] == platform.config["LOCAL_ROOT_DIR"]
    assert platform.header is None
    assert platform.wrapper is None
    assert platform.header is None
    assert len(platform.job_status) == 4
    # These calls are not implemented but should not raise any error
    platform.get_submit_script()
    platform.generate_submit_script()


def test_check_all_jobs_send_command1_raises_autosubmit_error(mocker, paramiko_platform):
    mocker.patch('autosubmit.platforms.paramiko_platform.Log')
    mocker.patch('autosubmit.platforms.paramiko_platform.sleep')

    platform = paramiko_platform
    platform.get_check_all_jobs_cmd = mocker.Mock()
    platform.get_check_all_jobs_cmd.side_effect = ['ls']
    platform.send_command = mocker.Mock()
    ae = AutosubmitError(message='Test', code=123, trace='ERR!')
    platform.send_command.side_effect = ae
    as_conf = mocker.Mock()
    as_conf.get_copy_remote_logs.return_value = None
    job = mocker.Mock()
    job.id = 'TEST'
    job.name = 'TEST'
    with pytest.raises(AutosubmitError) as cm:
        platform.check_all_jobs(
            job_list=[[job, None]],
            as_conf=as_conf,
            retries=-1)
    assert cm.value.message == 'Some Jobs are in Unknown status'
    assert cm.value.code == 6008
    assert cm.value.trace is None


def test_check_all_jobs_send_command2_raises_autosubmit_error(mocker, paramiko_platform):
    mocker.patch('autosubmit.platforms.paramiko_platform.sleep')

    platform = paramiko_platform
    platform.get_check_all_jobs_cmd = mocker.Mock()
    platform.get_check_all_jobs_cmd.side_effect = ['ls']
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
        platform.check_all_jobs(
            job_list=[[job, None]],
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
    for user in [getuser(), "dummy-one"]:
        ssh_content_user = ssh_content.replace("%change%", user)
        add_ssh_config_file(tmpdir, user, ssh_content_user)
    return tmpdir


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


def test_get_pscall(paramiko_platform):
    job_id = 42
    output = paramiko_platform.get_pscall(job_id)
    assert f'kill -0 {job_id}' in output


def test_remove_multiple_files_no_error_path_does_not_exist(paramiko_platform):
    """Test that calling a platform function to remove multiple files accepts non-existing directories. """
    from uuid import uuid4
    paramiko_platform.tmp_path = 'non-existing-path-' + uuid4().hex
    assert paramiko_platform.remove_multiple_files([]) == ""


@pytest.mark.parametrize(
    'exception,expected',
    [
        [Exception, None],
        [None, True]
    ]
)
def test_init_local_x11_display(exception: Optional[Exception], expected: Optional[bool], paramiko_platform, mocker):
    """Test the X11 display initialization.

    We rely heavily on mocking here.

    If an error is provided, then we expect the local X11 to be initialized to ``None``.

    If no error provided, our mock will return ``True``.
    """
    if exception:
        mocker.patch('autosubmit.platforms.paramiko_platform.xlib_connect.get_display', side_effect=exception)
    else:
        mocker.patch('autosubmit.platforms.paramiko_platform.xlib_connect.get_display', return_value=expected)

    paramiko_platform._init_local_x11_display()

    assert expected == paramiko_platform.local_x11_display


@pytest.mark.parametrize(
    'platform',
    ['linux', 'darwin']
)
def test_poller(platform: str, mocker, paramiko_platform):
    """Test the file descriptor poller, initialized to kqueue on Linux, and poll on other systems. """
    mocked_sys = mocker.patch('autosubmit.platforms.paramiko_platform.sys')
    mocked_sys.platform = platform
    mocker.patch('autosubmit.platforms.paramiko_platform.select')

    paramiko_platform._init_poller()

    assert paramiko_platform.poller


@pytest.mark.parametrize(
    'job_list,expected',
    [
        (
                [], ''
        ),
        (
                [
                    [Job(job_id='10', name=''), True]
                ],
                '10'
        ),
        (
                [
                    [Job(job_id='1', name=''), True],
                    [Job(job_id='2', name=''), True]
                ],
                '1,2'
        ),
(
                [
                    [Job(job_id=None, name=''), True],
                    [Job(job_id='2', name=''), True]
                ],
                '0,2'
        )
    ]
)
def test_parse_joblist(job_list: list, expected: str, paramiko_platform: ParamikoPlatform):
    """Test that the conversion of a list of jobs to str is working correctly. """
    cmd = paramiko_platform.parse_job_list(job_list)
    assert cmd == expected


def test_send_command_non_blocking(mocker, paramiko_platform: ParamikoPlatform):
    """Test that a ``Thread`` is created and started for ``.send_command`` (mocked).

    We mock that function as it is already tested in an integration test.

    In this function we only verify that the function is wrapped in a thread and
    receives its args.
    """
    mocked_send_command = mocker.MagicMock()
    mocker.patch.object(paramiko_platform, 'send_command', mocked_send_command)
    command = 'ls'
    ignore_log = True
    t = paramiko_platform.send_command_non_blocking(command=command, ignore_log=ignore_log)
    t.join()
    assert mocked_send_command.call_count == 1
    assert mocked_send_command.call_args_list[0][0][0] == command
    assert mocked_send_command.call_args_list[0][0][1] == ignore_log


@pytest.mark.parametrize(
    'error,expected_error_or_return_value',
    [
        (IOError, False),
        (ValueError, False),
        (Exception, False),
        (ValueError("There is a garbage truck over there"), AutosubmitCritical)
    ]
)
def test_delete_file_errors(error, expected_error_or_return_value, paramiko_platform: ParamikoPlatform, mocker,
                            tmp_path):
    """Test the error paths for ``delete_file``.

    The main execution path of that function is tested with an integration test.
    """
    mocked_ftp_channel = mocker.MagicMock()
    mocked_ftp_channel.remove.side_effect = error
    paramiko_platform._ftpChannel = mocked_ftp_channel
    mocker.patch.object(paramiko_platform, 'get_files_path', return_value=str(tmp_path))

    if expected_error_or_return_value is AutosubmitCritical:
        with pytest.raises(expected_error_or_return_value):  # type: ignore
            paramiko_platform.delete_file('a.txt')
    else:
        r = paramiko_platform.delete_file('a.txt')
        assert r == expected_error_or_return_value


@pytest.mark.parametrize(
    'error,must_exist,expected_error_or_return_value',
    [
        (IOError("Garbage"), True, AutosubmitError),
        (IOError("garbage"), True, AutosubmitError),
        (IOError("garbage"), False, False),
        (Exception("Garbage"), True, AutosubmitError),
        (Exception("garbage"), True, AutosubmitError),
        (Exception("garbage"), False, False)
    ]
)
def test_move_file_errors(error, must_exist, expected_error_or_return_value, paramiko_platform: ParamikoPlatform, mocker,
                          tmp_path):
    """Test the error paths for ``move_file``.

    The main execution path of that function is tested with an integration test.
    """
    # The function gets called first inside the try, but it may be called again in the except block.
    mocker.patch.object(paramiko_platform, 'get_files_path', side_effect=[error, tmp_path])

    if type(expected_error_or_return_value) is bool:
        r = paramiko_platform.move_file('a.txt', 'b.txt', must_exist=must_exist)
        assert r == expected_error_or_return_value
    else:
        with pytest.raises(expected_error_or_return_value):  # type: ignore
            paramiko_platform.move_file('a.txt', 'b.txt', must_exist=must_exist)


@pytest.mark.parametrize(
    'header_fn,directive,value',
    [
        ('get_queue_directive', '%QUEUE_DIRECTIVE%', '-q debug'),
        ('get_processors_directive', '%NUMPROC_DIRECTIVE%', '-np 10'),
        ('get_partition_directive', '%PARTITION_DIRECTIVE%', '-p 1'),
        ('get_tasks_per_node', '%TASKS_PER_NODE_DIRECTIVE%', '-t 1'),
        ('get_threads_per_task', '%THREADS_PER_TASK_DIRECTIVE%', '-tt 11'),
        ('get_scratch_free_space', '%SCRATCH_FREE_SPACE_DIRECTIVE%', '-s 10GB'),
        ('get_custom_directives', '%CUSTOM_DIRECTIVES%', '-t 10'),
        ('get_exclusive_directive', '%EXCLUSIVE_DIRECTIVE%', '--exclusive'),
        ('get_account_directive', '%ACCOUNT_DIRECTIVE%', '-A bsc'),
        ('get_shape_directive', '%SHAPE_DIRECTIVE%', '-s q'),
        ('get_nodes_directive', '%NODES_DIRECTIVE%', '-n 1'),
        ('get_reservation_directive', '%RESERVATION_DIRECTIVE%', '--reservation abc'),
        ('get_memory_directive', '%MEMORY_DIRECTIVE%', '--mem 1G'),
        ('get_memory_per_task_directive', '%MEMORY_PER_TASK_DIRECTIVE%', '-mt 1G'),
        ('get_hyperthreading_directive', '%HYPERTHREADING_DIRECTIVE%', '-h')
    ]
)
def test_get_header(header_fn: str, directive: str, value: str, paramiko_platform: ParamikoPlatform, mocker):
    job = Job(job_id='test', name='test')
    job.packed = True
    job.het = {}
    job.x11 = True

    job.processors = 1
    paramiko_platform._header = mocker.Mock(spec=object)
    paramiko_platform.header.SERIAL = f'{directive}\n%X11%'

    setattr(paramiko_platform._header, header_fn, lambda *args, **kwargs: value)

    header = paramiko_platform.get_header(job, {})

    assert directive not in header, "Directive was not replaced!"
    assert value in header, "Value not found!"
    assert '%X11%' not in header, "X11 was not replaced!"
    assert 'SBATCH --x11=batch' in header


def test_check_remote_log_dir_errors(paramiko_platform: ParamikoPlatform, mocker):
    """Test the error paths for ``check_remote_log_dir``.

    The main execution path of that function is tested with an integration test.
    """
    mocker.patch.object(paramiko_platform, 'send_command', side_effect=Exception)
    with pytest.raises(AutosubmitError):
        paramiko_platform.check_remote_log_dir()

    mocked_log = mocker.patch('autosubmit.platforms.paramiko_platform.Log')
    mocker.patch.object(paramiko_platform, 'send_command', lambda *args, **kwargs: False)
    mocker.patch.object(paramiko_platform, 'get_mkdir_cmd', return_value=False)
    paramiko_platform.check_remote_log_dir()
    assert mocked_log.debug.call_count == 1
    assert 'Could not create the DIR' in mocked_log.debug.call_args_list[0][0][0]


@pytest.mark.parametrize(
    'executable,timeout',
    [
        ('/bin/bash', 0),
        ('/bin/bash', 1),
        ('', 1)
    ],
    ids=[
        'bash without timeout',
        'bash with timeout',
        'no executable but still bash (type), with timeout'
    ]
)
def test_get_call(executable: str, timeout, paramiko_platform: ParamikoPlatform):
    job = Job(name='test', job_id='test')
    job.executable = executable
    job.type = 'bash'

    call = paramiko_platform.get_call(export='', job_script='job_a', job=job, timeout=timeout)

    call = call.strip()

    if timeout > 0:
        assert call.startswith('timeout')
    else:
        assert call.startswith('nohup')
    assert 'job_a' in call


def test_get_call_no_job(paramiko_platform: ParamikoPlatform):
    call = paramiko_platform.get_call(export='', job_script='job_a', job=None, timeout=-1)
    assert 'python3' in call  # Because it will be a wrapper, without the job.


@pytest.mark.parametrize(
    'exception_message,must_exist,ignore_log,messages',
    [
        ('Garbage', True, False, ['skipping', 'does not exists']),
        ('Garbage', False, False, ['skipping', 'be retrieved']),
        ('error', True, False, ['does not exists']),
        ('error', False, False, ['be retrieved']),
        ('Garbage', True, True, []),
        ('Garbage', False, True, []),
        ('error', True, True, []),
        ('error', False, True, [])
    ]
)
def test_get_file_errors(exception_message: bool, must_exist: bool, ignore_log: bool,
                         messages: list, paramiko_platform: ParamikoPlatform, tmp_path, mocker):
    # TODO: There is probably a bug in the code checking for exception messages, but not sure if we just fix that
    #       or if that logic is not necessary -- after all, it is working fine without that? Or maybe not...
    #       To reproduce the bug, just change the first message from "Garbage" to "The Garbage", and
    #       now the test should fail.

    paramiko_platform.tmp_path = tmp_path

    mocked_log = mocker.patch('autosubmit.platforms.paramiko_platform.Log')
    mocked_ftp_channel = mocker.MagicMock()
    mocked_ftp_channel.get.side_effect = Exception(exception_message)
    mocker.patch.object(paramiko_platform, '_ftpChannel', mocked_ftp_channel)

    assert not paramiko_platform.get_file('anyfile.txt', must_exist=must_exist, ignore_log=ignore_log)

    if ignore_log:
        assert mocked_log.printlog.call_count == 0
    else:
        assert mocked_log.printlog.call_count == len(messages)

