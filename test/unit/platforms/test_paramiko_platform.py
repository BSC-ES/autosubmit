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
from tempfile import TemporaryDirectory
from typing import Generator, Optional, TYPE_CHECKING

import pytest

from autosubmit.autosubmit import Autosubmit
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.log.log import AutosubmitError, AutosubmitCritical
from autosubmit.platforms.locplatform import LocalPlatform
# noinspection PyProtectedMember
from autosubmit.platforms.paramiko_platform import ParamikoPlatform, ParamikoPlatformException, _get_user_config_file
from autosubmit.platforms.psplatform import PsPlatform
from autosubmit.platforms.slurmplatform import SlurmPlatform

if TYPE_CHECKING:
    from _pytest._py.path import LocalPath


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


def test_ps_get_multi_submit_cmd(ps_platform):
    platform, _ = ps_platform
    platform.remote_log_dir = str(platform.root_dir)

    package = type("DummyPackage", (), {
        "export": "",
        "timeout": 78,
        "x11_options": "",
        "executable": "/bin/bash",
        "fail_count": 0,
        "ec_queue": None,
    })()

    command = platform.get_multi_submit_cmd({"job.cmd": package})

    assert "job.cmd" in command
    assert "timeout 78" in command


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


@pytest.mark.parametrize(
    "user,env_ssh_config_defined",
    [
        (os.environ["USER"], True),
        (os.environ["USER"], False),
        ("dummy-one", True),
        ("dummy-one", False),
        ("not-exists", True),
        ("not_exists", False)
    ],
    ids=[
        "OWNER + AS_ENV_CONFIG_SSH_PATH(defined)",
        "OWNER + AS_ENV_CONFIG_SSH_PATH(not defined)",
        "SUDO USER(exists) + AS_ENV_CONFIG_SSH_PATH(defined)",
        "SUDO USER(exists) + AS_ENV_CONFIG_SSH_PATH(not defined)",
        "SUDO USER(not exists) + AS_ENV_CONFIG_SSH_PATH(defined)",
        "SUDO USER(not exists) + AS_ENV_CONFIG_SSH_PATH(not defined)"
    ]
)
def test__get_user_config_file(user: str, env_ssh_config_defined: bool, tmpdir, autosubmit_config, generate_all_files):
    tmp_dir = str(tmpdir)
    experiment_data = {
        "ROOTDIR": tmp_dir,
        "PROJDIR": tmp_dir,
        "LOCAL_TMP_DIR": tmp_dir,
        "LOCAL_ROOT_DIR": tmp_dir,
        "AS_ENV_CURRENT_USER": user,
    }
    as_env_ssh_config_path = None
    if env_ssh_config_defined:
        experiment_data["AS_ENV_SSH_CONFIG_PATH"] = str(tmpdir.join(f".ssh/config_{user}"))
        as_env_ssh_config_path = Path(experiment_data["AS_ENV_SSH_CONFIG_PATH"])

    as_conf = autosubmit_config(expid='a000', experiment_data=experiment_data)

    user_config_file = _get_user_config_file(
        as_conf.is_current_real_user_owner,
        as_env_ssh_config_path,
        experiment_data.get('AS_ENV_CURRENT_USER', None)
    )

    if as_conf.is_current_real_user_owner:
        # The user is the owner, the code immediately loads the user's config.
        assert user_config_file == Path("~/.ssh/config").expanduser()
    elif not as_env_ssh_config_path:
        # The user is not the owner, but no env var was given, so we search in the home directory.
        assert user_config_file == Path(f"~/.ssh/config_{user}").expanduser()
    else:
        # Otherwise, we now confirm that the file was loaded using the env var value path.
        assert user_config_file == Path(tmpdir, f".ssh/config_{user}").expanduser()


def test__get_user_config_file_invalid_settings():
    """Test that an error is raised when the user is not the owner and no values
    were provided to locate its SSH configuration file."""
    with pytest.raises(ValueError):
        _get_user_config_file(False, None, None)


def test_submit_multiple_jobs(mocker, autosubmit_config, tmpdir):
    experiment_data = {
        "ROOTDIR": str(tmpdir),
        "PROJDIR": str(tmpdir),
        "LOCAL_TMP_DIR": str(tmpdir),
        "LOCAL_ROOT_DIR": str(tmpdir),
        "AS_ENV_CURRENT_USER": "dummy",
    }
    platform = ParamikoPlatform(expid='a000', name='local', config=experiment_data)
    platform._ssh_config = mocker.MagicMock()
    platform.get_submit_cmd = mocker.MagicMock(return_value="dummy")
    platform.send_command = mocker.MagicMock(return_value=True)
    platform.get_submitted_job_id = mocker.MagicMock(return_value=[10000])
    platform._ssh_output = "10000"
    jobs_id = platform.submit_multiple_jobs({"dummy.cmd": mocker.MagicMock()})
    assert jobs_id == [10000]


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
    call = paramiko_platform.get_call(
        script_name='job_a',
        timeout=timeout,
        export='',
        executable=executable,
        x11_options='',
        fail_count=0,
        sub_queue=None,
    )

    call = call.strip()

    if timeout > 0:
        assert 'timeout' in call
    else:
        assert call.startswith('nohup')
    assert call.startswith('nohup')
    assert 'job_a' in call


def test_get_call_no_job(paramiko_platform: ParamikoPlatform):
    call = paramiko_platform.get_call(
        script_name='job_a',
        timeout=-1,
        export='',
        executable='',
        x11_options='',
        fail_count=0,
        sub_queue=None,
    )
    assert 'nohup' in call
    assert 'job_a' in call


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


def test__load_ssh_config_missing_ssh_config(
        tmp_path: 'LocalPath',
        mocker
):
    """Test that the user is warned when the expected SSH file cannot be located."""
    mocked_log = mocker.patch('autosubmit.platforms.paramiko_platform.Log')
    experiment_data = {
        "ROOTDIR": str(tmp_path),
        "PROJDIR": str(tmp_path),
        "LOCAL_TMP_DIR": str(tmp_path),
        "LOCAL_ROOT_DIR": str(tmp_path),
        "AS_ENV_CURRENT_USER": "dummy",
    }
    platform = ParamikoPlatform(expid='a000', name='local', config=experiment_data)
    platform.config['AS_ENV_SSH_CONFIG_PATH'] = str(tmp_path / 'you-cannot-find-me')

    as_conf = mocker.MagicMock()
    as_conf.is_current_real_user_owner = False

    # TODO: We must be able to test that we are not loading the right SSH, without a mock here.
    mocker.patch('autosubmit.platforms.paramiko_platform._create_ssh_client', side_effect=ValueError)
    # mocker.

    with pytest.raises(AutosubmitError):
        try:
            platform.connect(as_conf, reconnect=False, log_recovery_process=False)
        finally:
            platform.close_connection()

    assert mocked_log.warning.called


@pytest.mark.parametrize("output,expected", [
    ("", {}),
    ("   \n\n  \n", {}),
    ("job_a:1001\n", {"job_a": ["1001"]}),
    ("job_a:1003,1001,1002\n", {"job_a": ["1001", "1002", "1003"]}),
    ("job_a:101\njob_b:202,203\n", {"job_a": ["101"], "job_b": ["202", "203"]}),
    ("job_a:\n", {"job_a": []}),
])
def test_parse_job_names(output: str, expected: dict) -> None:
    """Parse grouped job-name output into the expected name-to-IDs mapping.

    :param output: Raw command output in ``JobName:id,id2`` format.
    :param expected: Mapping of job name to sorted job IDs.
    """
    assert ParamikoPlatform._parse_job_names(output) == expected


def test_check_and_cancel_duplicated_job_names_no_duplicates(
        slurm_platform: SlurmPlatform,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Do not call cancel_jobs when no job name appears more than once.

    :param slurm_platform: Slurm platform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    monkeypatch.setattr(slurm_platform, "send_command", lambda cmd, **_: None)
    slurm_platform._ssh_output = "job_a:1001\n"

    cancelled: list[str] = []
    monkeypatch.setattr(slurm_platform, "cancel_jobs", lambda ids: cancelled.extend(ids))

    slurm_platform._check_and_cancel_duplicated_job_names({"job_a.cmd": None})

    assert cancelled == []


def test_check_and_cancel_duplicated_job_names_with_duplicates(
        slurm_platform: SlurmPlatform,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancel the oldest (lowest-sorted) ID when a job name has multiple entries.

    :param slurm_platform: Slurm platform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    monkeypatch.setattr(slurm_platform, "send_command", lambda cmd, **_: None)
    slurm_platform._ssh_output = "job_a:1001,1002\n"

    cancelled: list[str] = []
    monkeypatch.setattr(slurm_platform, "cancel_jobs", lambda ids: cancelled.extend(ids))

    slurm_platform._check_and_cancel_duplicated_job_names({"job_a.cmd": None})

    assert cancelled == ["1001"]


def test_check_and_cancel_duplicated_job_names_empty_output(
        slurm_platform: SlurmPlatform,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Do not cancel anything when the command returns empty output.

    :param slurm_platform: Slurm platform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    monkeypatch.setattr(slurm_platform, "send_command", lambda cmd, **_: None)
    slurm_platform._ssh_output = ""

    cancelled: list[str] = []
    monkeypatch.setattr(slurm_platform, "cancel_jobs", lambda ids: cancelled.extend(ids))

    slurm_platform._check_and_cancel_duplicated_job_names({"job_a.cmd": None})

    assert cancelled == []


def test_submit_multiple_jobs_empty_input_returns_empty(
        paramiko_platform: ParamikoPlatform,
) -> None:
    """Return an empty list immediately when no scripts are provided.

    :param paramiko_platform: ParamikoPlatform under test.
    """
    assert paramiko_platform.submit_multiple_jobs({}) == []


def test_submit_multiple_jobs_uses_fallback_when_count_mismatch(
        paramiko_platform: ParamikoPlatform,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Use get_submitted_jobs_by_name when direct parse returns the wrong count.

    :param paramiko_platform: ParamikoPlatform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    monkeypatch.setattr(paramiko_platform, "send_command", lambda cmd, **_: None)
    monkeypatch.setattr(paramiko_platform, "get_multi_submit_cmd", lambda _: "dummy_cmd")
    paramiko_platform._ssh_output = ""
    monkeypatch.setattr(paramiko_platform, "get_submitted_job_id", lambda out, **_: [])
    monkeypatch.setattr(paramiko_platform, "get_submitted_jobs_by_name", lambda names: [101])

    result = paramiko_platform.submit_multiple_jobs({"job_a.cmd": object()})

    assert result == [101]


def test_submit_multiple_jobs_raises_when_both_paths_fail(
        paramiko_platform: ParamikoPlatform,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise AutosubmitError (6005) when both direct and fallback ID parsing fail.

    :param paramiko_platform: ParamikoPlatform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    monkeypatch.setattr(paramiko_platform, "send_command", lambda cmd, **_: None)
    monkeypatch.setattr(paramiko_platform, "get_multi_submit_cmd", lambda _: "dummy_cmd")
    monkeypatch.setattr(paramiko_platform, "cancel_jobs", lambda ids: None)
    paramiko_platform._ssh_output = ""
    monkeypatch.setattr(paramiko_platform, "get_submitted_job_id", lambda out, **_: [])
    monkeypatch.setattr(paramiko_platform, "get_submitted_jobs_by_name", lambda names: [])

    with pytest.raises(AutosubmitError) as exc_info:
        paramiko_platform.submit_multiple_jobs({"job_a.cmd": object()})

    assert exc_info.value.code == 6005


def test_ps_get_job_names_cmd_contains_expected_components(
        ps_platform: tuple,
) -> None:
    """The PS command must use ps and grep to filter by job name.

    :param ps_platform: Local ps_platform fixture (platform, tmpdir).
    """
    platform, _ = ps_platform
    cmd = platform._get_job_names_cmd(["job_a", "job_b"])

    assert "job_a" in cmd
    assert "job_b" in cmd


@pytest.mark.parametrize("mode", ["all", "specific"])
def test_get_completed_job_names(tmp_path: Path, mode: str) -> None:
    """Test that completed job names are correctly retrieved from the remote platform."""
    # Actually we want to test a paramiko function, but using local platform for simplicity with the "send_command" part.
    platform = LocalPlatform(expid='t001', name='local', config={})
    platform.remote_log_dir = tmp_path / 't001/remote_logs'
    platform.remote_log_dir.mkdir(parents=True, exist_ok=True)
    platform.connected = True
    completed_jobs = ['job1_COMPLETED', 'job2_COMPLETED', 'job3_COMPLETED']
    for job_file in completed_jobs:
        (platform.remote_log_dir / job_file).touch()

    if mode == "all":
        job_names = platform.get_completed_job_names()
        expected_job_names = ['job1', 'job2', 'job3']
    else:
        job_names = platform.get_completed_job_names(job_names=['job1', 'job3'])
        expected_job_names = ['job1', 'job3']
    assert set(job_names) == set(expected_job_names)


def _make_job(name: str, job_id: str, status: int, platform: ParamikoPlatform) -> Job:
    """Return a minimal Job attached to ``platform``."""
    job = Job(name=name, job_id=job_id, status=status, priority=0)
    job.platform = platform
    return job


@pytest.fixture
def multi_platform_setup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict:
    """Return LOCAL, PS, and SLURM platforms, each with send_command patched to a tracker."""
    base_config = {"LOCAL_ROOT_DIR": str(tmp_path), "LOCAL_TMP_DIR": "tmp"}
    slurm_config = {**base_config, "LOCAL_ASLOG_DIR": "ASLOGS"}

    local = LocalPlatform(expid="a000", name="local", config=base_config)
    ps = PsPlatform(expid="a000", name="ps", config=base_config)
    slurm = SlurmPlatform(expid="a000", name="slurm", config=slurm_config)

    sent: dict[str, list[str]] = {"local": [], "ps": [], "slurm": []}
    for platform, key in [(local, "local"), (ps, "ps"), (slurm, "slurm")]:
        monkeypatch.setattr(
            platform, "send_command", lambda cmd, _k=key, **kw: sent[_k].append(cmd)
        )

    return {"platforms": {"local": local, "ps": ps, "slurm": slurm}, "sent": sent}


@pytest.mark.parametrize(
    "active_status",
    [Status.QUEUING, Status.RUNNING, Status.SUBMITTED],
    ids=["QUEUING", "RUNNING", "SUBMITTED"],
)
def test_change_status_sends_batch_cancel_per_platform(
        active_status: int,
        multi_platform_setup: dict,
) -> None:
    """Test that N active jobs per platform produce exactly one batched cancel call containing all IDs."""
    platforms = multi_platform_setup["platforms"]
    sent = multi_platform_setup["sent"]

    jobs_per_platform = 3
    all_jobs = [
        _make_job(f"job_{name}_{i}", f"{name[0]}{i}", active_status, platform)
        for name, platform in platforms.items()
        for i in range(jobs_per_platform)
    ]

    changes = Autosubmit.change_status(
        final="FAILED",
        final_status=Status.FAILED,
        final_list=all_jobs,
        save=True,
        definitive_platforms=list(platforms.keys()),
        platforms=platforms,
    )

    assert len(changes) == jobs_per_platform * len(platforms)
    for job in all_jobs:
        assert job.status == Status.FAILED
        assert job.name in changes

    for platform_name, platform in platforms.items():
        assert len(sent[platform_name]) == 1, (
            f"Expected one batched call for '{platform_name}', "
            f"got {len(sent[platform_name])}"
        )
        assert platform.cancel_cmd in sent[platform_name][0]
        for i in range(jobs_per_platform):
            assert f"{platform_name[0]}{i}" in sent[platform_name][0]


@pytest.mark.parametrize(
    "initial_status,final,final_status",
    [
        (Status.WAITING, "FAILED", Status.FAILED),
        (Status.READY, "COMPLETED", Status.COMPLETED),
        (Status.FAILED, "WAITING", Status.WAITING),
    ],
    ids=["WAITING->FAILED", "READY->COMPLETED", "FAILED->WAITING"],
)
def test_change_status_applies_status_to_all_jobs(
        initial_status: int,
        final: str,
        final_status: int,
        multi_platform_setup: dict,
) -> None:
    """Test that change_status sets the target status on all jobs and records them in performed_changes."""
    platforms = multi_platform_setup["platforms"]
    jobs = [
        _make_job(f"job_{name}", str(i), initial_status, platform)
        for i, (name, platform) in enumerate(platforms.items())
    ]

    changes = Autosubmit.change_status(
        final=final,
        final_status=final_status,
        final_list=jobs,
        save=False,
        definitive_platforms=[],
        platforms={},
    )

    for job in jobs:
        assert job.status == final_status
        assert job.name in changes
        assert Status.VALUE_TO_KEY[initial_status] in changes[job.name]
        assert final in changes[job.name]


def test_change_status_skips_jobs_already_at_final_status(
        multi_platform_setup: dict,
) -> None:
    """Test that jobs already at the target status produce no entry in performed_changes."""
    platforms = multi_platform_setup["platforms"]
    jobs = [
        _make_job(f"job_{name}", str(i), Status.FAILED, platform)
        for i, (name, platform) in enumerate(platforms.items())
    ]

    changes = Autosubmit.change_status(
        final="FAILED",
        final_status=Status.FAILED,
        final_list=jobs,
        save=False,
        definitive_platforms=[],
        platforms={},
    )

    assert changes == {}
    for job in jobs:
        assert job.status == Status.FAILED


@pytest.mark.parametrize(
    "active_status",
    [Status.QUEUING, Status.RUNNING, Status.SUBMITTED],
    ids=["QUEUING", "RUNNING", "SUBMITTED"],
)
def test_change_status_skips_active_job_with_unreachable_platform(
        active_status: int,
        multi_platform_setup: dict,
) -> None:
    """Test that active jobs whose platforms are all unreachable keep their original status."""
    platforms = multi_platform_setup["platforms"]
    jobs = [
        _make_job(f"job_{name}", str(i), active_status, platform)
        for i, (name, platform) in enumerate(platforms.items())
    ]

    changes = Autosubmit.change_status(
        final="FAILED",
        final_status=Status.FAILED,
        final_list=jobs,
        save=True,
        definitive_platforms=[],  # no platform reachable
        platforms=platforms,
    )

    assert changes == {}
    for job in jobs:
        assert job.status == active_status


def test_change_status_no_cancel_when_save_is_false(
        multi_platform_setup: dict,
) -> None:
    """Test that no cancel is dispatched to any platform when save is False."""
    platforms = multi_platform_setup["platforms"]
    sent = multi_platform_setup["sent"]

    jobs = [
        _make_job(f"job_{name}", str(i), Status.RUNNING, platform)
        for i, (name, platform) in enumerate(platforms.items())
    ]

    changes = Autosubmit.change_status(
        final="FAILED",
        final_status=Status.FAILED,
        final_list=jobs,
        save=False,
        definitive_platforms=list(platforms.keys()),
        platforms=platforms,
    )

    for job in jobs:
        assert job.status == Status.FAILED
        assert job.name in changes
    for platform_name in ("local", "ps", "slurm"):
        assert sent[platform_name] == []


def test_change_status_handles_send_command_failure_gracefully(
        multi_platform_setup: dict,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that send_command failures across all platforms do not prevent status changes."""
    platforms = multi_platform_setup["platforms"]

    def _raise(*args, **kwargs):
        raise Exception("SSH connection lost")

    for platform in platforms.values():
        monkeypatch.setattr(platform, "send_command", _raise)

    jobs = [
        _make_job(f"job_{name}", str(i), Status.RUNNING, platform)
        for i, (name, platform) in enumerate(platforms.items())
    ]

    changes = Autosubmit.change_status(
        final="FAILED",
        final_status=Status.FAILED,
        final_list=jobs,
        save=True,
        definitive_platforms=list(platforms.keys()),
        platforms=platforms,
    )

    assert len(changes) == len(platforms)
    for job in jobs:
        assert job.status == Status.FAILED
        assert job.name in changes
