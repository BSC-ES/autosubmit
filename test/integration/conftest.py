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
import configparser
import multiprocessing
import os
import socket
import time
import uuid
from dataclasses import dataclass
from fileinput import FileInput
from getpass import getuser
from importlib.metadata import version, PackageNotFoundError
from pathlib import Path
from pwd import getpwnam
from re import sub
from subprocess import check_output
from tempfile import TemporaryDirectory
from time import time_ns
from typing import cast, Any, ContextManager, Generator, Iterator, Optional, Protocol, Union, TYPE_CHECKING

import paramiko  # type: ignore
import pytest
from ruamel.yaml import YAML
from sqlalchemy import Connection, create_engine, text
from testcontainers.core.container import DockerContainer  # type: ignore
from testcontainers.core.waiting_utils import wait_for_logs  # type: ignore
from testcontainers.postgres import PostgresContainer  # type: ignore

from autosubmit.autosubmit import Autosubmit
from autosubmit.config.basicconfig import BasicConfig
from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.platforms.paramiko_platform import ParamikoPlatform
# noinspection PyProtectedMember
from autosubmit.platforms.paramiko_platform import _create_ssh_client
# noinspection PyProtectedMember
from autosubmit.platforms.psplatform import PsPlatform
from autosubmit.platforms.slurmplatform import SlurmPlatform
from test.integration.test_utils.networking import get_free_port

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from py._path.local import LocalPath  # type: ignore
    from pytest_mock import MockerFixture
    from pytest import FixtureRequest

_SSH_DOCKER_IMAGE = 'lscr.io/linuxserver/openssh-server:latest'
"""This is the vanilla image from LinuxServer.io, with OpenSSH. About 39MB."""
_SSH_DOCKER_IMAGE_X11_MFA = 'autosubmit/linuxserverio-ssh-2fa-x11:latest'
"""This is our test image, built on top of LinuxServer.io's, but with MFA and X11. About 395MB."""
_SSH_DOCKER_PASSWORD = 'password'
"""Common password used in SSH containers; we mock the SSH Client of Paramiko to avoid hassle with keys."""

_SLURM_DOCKER_IMAGE = 'autosubmit/slurm-openssh-container:25-05-0-1'
"""The Slurm Docker image. About 600 MB. It contains 2 cores, 1 node."""

_PG_USER = 'postgres'
_PG_PASSWORD = 'postgres'
_PG_DATABASE = 'autosubmit_test'


@dataclass
class AutosubmitExperiment:
    """This holds information about an experiment created by Autosubmit."""
    expid: str
    autosubmit: Autosubmit
    as_conf: AutosubmitConfig
    exp_path: Path
    tmp_dir: Path
    aslogs_dir: Path
    status_dir: Path
    platform: ParamikoPlatform


class AutosubmitExperimentFixture(Protocol):
    """Type for ``autosubmit_exp`` fixture."""

    def __call__(
            self,
            expid: Optional[str] = None,
            experiment_data: Optional[dict] = None,
            wrapper: Optional[bool] = False,
            create: Optional[bool] = True,
            reload: Optional[bool] = True,
            mock_last_name_used: Optional[bool] = True,
            *args: Any,
            **kwargs: Any
    ) -> AutosubmitExperiment:
        ...


@pytest.fixture
def autosubmit_exp(
        autosubmit: Autosubmit,
        request: "FixtureRequest",
        tmp_path: "LocalPath",
        mocker: "MockerFixture",
) -> AutosubmitExperimentFixture:
    """Create an instance of ``Autosubmit`` with an experiment.

    If an ``expid`` is provided, it will create an experiment with that ID.
    Otherwise, it will simply get the next available ID.

    It sets the ``AUTOSUBMIT_CONFIGURATION`` environment variable, pointing
    to a newly created file in a temporary directory.

    A complete experiment is created, with the default configuration files,
    unless ``experiment_data`` is provided. This is a Python dictionary that
    will be used to populate files such as `jobs_<EXPID>.yml` (the ``JOBS``
    YAML key will be written to that file).

    Returns a data class that contains the ``AutosubmitConfig``.

    TODO: Use minimal to avoid having to juggle with the configuration files.
    """

    def _create_autosubmit_exp(
            expid: Optional[str] = None,
            experiment_data: Optional[dict] = None,
            wrapper: Optional[bool] = False,
            reload: Optional[bool] = True,
            create: Optional[bool] = True,
            mock_last_name_used: Optional[bool] = True,
            *_,
            **kwargs
    ) -> AutosubmitExperiment:
        if experiment_data is None:
            experiment_data = {}

        is_postgres = hasattr(BasicConfig, 'DATABASE_BACKEND') and BasicConfig.DATABASE_BACKEND == 'postgres'
        if is_postgres or not Path(BasicConfig.DB_PATH).exists():
            autosubmit.install()
            autosubmit.configure(
                advanced=False,
                database_path=BasicConfig.DB_DIR,  # type: ignore
                database_filename=BasicConfig.DB_FILE,  # type: ignore
                local_root_path=str(tmp_path),
                platforms_conf_path=None,  # type: ignore
                jobs_conf_path=None,  # type: ignore
                smtp_hostname=None,  # type: ignore
                mail_from=None,  # type: ignore
                machine=False,
                local=False,
                database_backend="postgres" if is_postgres else "sqlite",
                database_conn_url=BasicConfig.DATABASE_CONN_URL if is_postgres else ""
            )

        operational = False
        evaluation = False
        testcase = True
        if expid:
            if mock_last_name_used:
                mocker.patch('autosubmit.experiment.experiment_common.db_common.last_name_used',
                             return_value=expid)
            operational = expid.startswith('o')
            evaluation = expid.startswith('e')
            testcase = expid.startswith('t')

        expid = autosubmit.expid(
            description="Pytest experiment (delete me)",
            hpc="local",
            copy_id="",
            dummy=True,
            minimal_configuration=False,
            git_repo="",
            git_branch="",
            git_as_conf="",
            operational=operational,
            testcase=testcase,
            evaluation=evaluation,
            use_local_minimal=False
        )
        exp_path = Path(BasicConfig.LOCAL_ROOT_DIR) / expid
        conf_dir = exp_path / "conf"
        global_logs = Path(BasicConfig.GLOBAL_LOG_DIR)
        global_logs.mkdir(parents=True, exist_ok=True)
        exp_tmp_dir = exp_path / BasicConfig.LOCAL_TMP_DIR
        aslogs_dir = exp_tmp_dir / BasicConfig.LOCAL_ASLOG_DIR
        status_dir = exp_path / 'status'
        job_data_dir = Path(BasicConfig.JOBDATA_DIR)
        job_data_dir.mkdir(parents=True, exist_ok=True)

        config = AutosubmitConfig(
            expid=expid,
            basic_config=BasicConfig
        )

        config.experiment_data = {**config.experiment_data, **experiment_data}

        key_file = {
            'JOBS': 'jobs',
            'PLATFORMS': 'platforms',
            'EXPERIMENT': 'expdef'
        }

        for key, input_lines in key_file.items():
            if key in experiment_data:
                mode = 'a' if key == 'EXPERIMENT' else 'w'
                with open(conf_dir / f'{input_lines}_{expid}.yml', mode) as f:
                    YAML().dump({key: experiment_data[key]}, f)

        other_yaml = {
            k: v for k, v in experiment_data.items()
            if k not in key_file
        }
        if other_yaml:
            with open(conf_dir / f'tests_{expid}.yml', 'w') as f:
                YAML().dump(other_yaml, f)

        if reload:
            config.reload(force_load=True)

        # Default values for experiment data
        # TODO: This probably has a way to be initialized in config-parser?
        must_exists = ['DEFAULT', 'JOBS', 'PLATFORMS', 'CONFIG']
        for must_exist in must_exists:
            if must_exist not in config.experiment_data:
                config.experiment_data[must_exist] = {}

        if not config.experiment_data.get('CONFIG').get('AUTOSUBMIT_VERSION', ''):
            try:
                config.experiment_data['CONFIG']['AUTOSUBMIT_VERSION'] = version('autosubmit')
            except PackageNotFoundError:
                config.experiment_data['CONFIG']['AUTOSUBMIT_VERSION'] = ''

        for arg, value in kwargs.items():
            setattr(config, arg, value)

        platform_config = {
            "LOCAL_ROOT_DIR": BasicConfig.LOCAL_ROOT_DIR,
            "LOCAL_TMP_DIR": str(exp_tmp_dir),
            "LOCAL_ASLOG_DIR": str(aslogs_dir)
        }
        platform = SlurmPlatform(expid=expid, name='slurm_platform', config=platform_config)
        platform.job_status = {
            'COMPLETED': [],
            'RUNNING': [],
            'QUEUING': [],
            'FAILED': []
        }
        submit_platform_script = aslogs_dir.joinpath('submit_local.sh')
        submit_platform_script.touch(exist_ok=True)

        config.experiment_data['CONFIG']['SAFETYSLEEPTIME'] = 0
        # TODO: would be nice if we had a way in Autosubmit Config Parser or
        #       Autosubmit to define variables. We are replacing it
        #       in other parts of the code, but without ``fileinput``.
        # NOTE: the context manager is instantiated here, and we use ``cast`` as mypy
        #       complains otherwise (maybe related to this mypy GH issue?
        #       https://github.com/python/mypy/issues/18320).
        file_input = cast(
            ContextManager[str],
            FileInput(conf_dir / f'autosubmit_{expid}.yml', inplace=True, backup='.bak')
        )
        with file_input as input_lines:
            for line in input_lines:
                if 'SAFETYSLEEPTIME' in line:
                    print(sub(r'\d+', '0', line), end='')
                else:
                    print(line, end='')
        # TODO: one test failed while moving things from unit to integration, but this shouldn't be
        #       needed, especially if the disk has the valid value?
        config.experiment_data['DEFAULT']['EXPID'] = expid

        if create:
            autosubmit.create(expid, noplot=True, hide=False, force=True, check_wrappers=wrapper)

        return AutosubmitExperiment(
            expid=expid,
            autosubmit=autosubmit,
            as_conf=config,
            exp_path=exp_path,
            tmp_dir=exp_tmp_dir,
            aslogs_dir=aslogs_dir,
            status_dir=status_dir,
            platform=platform
        )

    return cast(AutosubmitExperimentFixture, _create_autosubmit_exp)


class MakeSSHClientFixture(Protocol):
    def __call__(
            self,
            ssh_port: int,
            password: Optional[str],
            key: Optional[Union['Path', str]]) -> paramiko.SSHClient:
        ...


# noinspection PyUnusedLocal
def _make_ssh_client(ssh_port: int, password: Optional[str], key: Optional[Union['Path', str]],
                     mfa: Optional[bool] = False) -> paramiko.SSHClient:
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


@pytest.fixture(scope="session")
def git_server(tmp_path_factory) -> Generator[tuple[DockerContainer, Path, str], None, None]:
    # Start a container to server it -- otherwise, we would have to use
    # `git -c protocol.file.allow=always submodule ...`, and we cannot
    # change how Autosubmit uses it in `autosubmit create` (due to bad
    # code design choices).
    base_path = tmp_path_factory.mktemp('git_repos_base')

    git_repos_path = base_path / 'git_repos'
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


@pytest.fixture
def ps_platform() -> PsPlatform:
    platform = PsPlatform(expid='a000', name='ps', config={})
    return platform


def _markers_contain(request: "FixtureRequest", txt: str) -> bool:
    """Check if a marker is used in the test.

    Returns ``True`` if the caller test is decorated with a
    marker that matches the given text. Otherwise, ``False``.
    """
    markers = request.node.iter_markers()
    return any(marker.name == txt for marker in markers)


def _wait_for_ssh_port(host, port, timeout=30):
    """Tries to connect to host and port until it works or the timeout is reached."""
    start = time.time()
    while True:
        try:
            with socket.create_connection((host, port), timeout=2):
                return
        except OSError:
            if time.time() - start > timeout:
                raise TimeoutError(f"SSH not ready at {host}:{port}")
            time.sleep(1)


@pytest.fixture
def ssh_server(mocker, tmp_path, request) -> Generator[DockerContainer, None, None]:
    ssh_port = get_free_port()

    user = getuser() or "unknown"
    user_pw = getpwnam(user)
    uid = user_pw.pw_uid
    gid = user_pw.pw_gid

    mfa = _markers_contain(request, 'mfa')
    x11 = _markers_contain(request, 'x11')

    ssh_image = _SSH_DOCKER_IMAGE_X11_MFA if mfa or x11 else _SSH_DOCKER_IMAGE

    with DockerContainer(image=ssh_image, remove=True, hostname='openssh-server') \
            .with_env('TZ', 'Etc/UTC') \
            .with_env('SUDO_ACCESS', 'false') \
            .with_env('USER_NAME', user) \
            .with_env('USER_PASSWORD', 'password') \
            .with_env('PUID', str(uid)) \
            .with_env('PGID', str(gid)) \
            .with_env('UMASK', '000') \
            .with_env('PASSWORD_ACCESS', 'true') \
            .with_env('MFA', str(mfa).lower()) \
            .with_bind_ports(2222, ssh_port) as container:
        # This verifies that the server printed the line, not necessarily the port is available
        wait_for_logs(container, 'sshd is listening on port 2222')

        _wait_for_ssh_port('localhost', ssh_port, 30)

        ssh_client = _make_ssh_client(ssh_port, _SSH_DOCKER_PASSWORD, None, mfa)
        mocker.patch('autosubmit.platforms.paramiko_platform._create_ssh_client', return_value=ssh_client)

        if mfa:
            # It uses a Transport and not an SSH client directly. Ideally, we would be able
            # to use just one way
            original_paramiko_config = paramiko.SSHConfig()
            with open(Path('~/.ssh/config').expanduser()) as f:
                original_paramiko_config.parse(f)
            modified_config = original_paramiko_config.lookup('localhost')
            modified_config['port'] = f'{ssh_port}'

            paramiko_config: paramiko.SSHConfig = mocker.MagicMock(spec=paramiko.SSHConfig)
            paramiko_config.lookup = lambda *args, **kwargs: modified_config
            mocker.patch('autosubmit.platforms.paramiko_platform.paramiko.SSHConfig', return_value=paramiko_config)

        yield container


@pytest.fixture(scope='session')
def slurm_server(session_mocker, tmp_path_factory):
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
        **docker_args
    )

    # TODO: GH needs --volume /sys/fs/cgroup:/sys/fs/cgroup:rw
    if 'GITHUB_ACTION' in os.environ:
        docker_container = docker_container.with_volume_mapping('/sys/fs/cgroup', '/sys/fs/cgroup', mode='rw')

    with docker_container \
            .with_env('TZ', 'Etc/UTC') \
            .with_bind_ports(2222, ssh_port) \
            .with_name(container_name) as container:
        # TODO: or maybe wait for 'debug:  sched: Running job scheduler for full queue.'?
        wait_for_logs(container, 'No fed_mgr state file')

        container.exec('sinfo')

        # What we had in ci.yaml for the old Slurm Docker service:
        # $ docker cp slurm-container:/root/.ssh/container_root_pubkey /tmp/container_root_pubkey
        # $ chmod 600 /tmp/container_root_pubkey
        # Translated into the code below:
        ssh_key_base_dir = tmp_path_factory.getbasetemp()
        ssh_key = ssh_key_base_dir / 'container_root_pubkey'
        check_output(
            [
                'docker',
                'cp',
                f'{container_name}:/root/.ssh/container_root_pubkey',
                str(ssh_key)
            ]
        )
        Path(ssh_key).chmod(0o600)

        ssh_client = _make_ssh_client(ssh_port, password=None, key=ssh_key)
        session_mocker.patch('autosubmit.platforms.paramiko_platform._create_ssh_client', return_value=ssh_client)

        # Pytest does NOT patch when using spawn context.
        session_mocker.patch(
            'autosubmit.platforms.platform.Platform.get_mp_context',
            return_value=multiprocessing.get_context('fork')
        )

        yield container


def _setup_pg_db(conn: Connection) -> None:
    """Reset the database.

    Drops all schemas except the system ones and restoring the public schema.

    :param conn: Database connection.
    """
    # Get all schema names that are not from the system
    results = conn.execute(
        text("""SELECT schema_name FROM information_schema.schemata
               WHERE schema_name NOT LIKE 'pg_%'
               AND schema_name != 'information_schema'""")
    ).all()
    schema_names = [res[0] for res in results]

    # Drop all schemas
    for schema_name in schema_names:
        conn.execute(text(f"""DROP SCHEMA IF EXISTS "{schema_name}" CASCADE"""))

    # Restore default public schema
    conn.execute(text("CREATE SCHEMA public"))
    conn.execute(text("GRANT ALL ON SCHEMA public TO public"))
    conn.execute(text("GRANT ALL ON SCHEMA public TO postgres"))


@pytest.fixture(scope='session', autouse=True)
def postgres_server(request: 'FixtureRequest') -> Generator[Optional[PostgresContainer], None, None]:
    """Fixture to set up and tear down a Postgres database for testing.

    Enabled only if the mark 'postgres' was specified.

    The container is available throughout the whole testing session.
    """
    # ref: https://stackoverflow.com/a/58142403
    has_postgres_marker = any([item.get_closest_marker('postgres') is not None for item in request.session.items])
    if not has_postgres_marker:
        # print("Skipping Postgres setup because -m 'postgres' was not specified")
        yield None
    else:
        pg_random_port = get_free_port()
        conn_url = f'postgresql://{_PG_USER}:{_PG_PASSWORD}@localhost:{pg_random_port}/{_PG_USER}'

        image = 'postgres:17'
        with PostgresContainer(
                image=image,
                port=5432,
                username=_PG_USER,
                password=_PG_PASSWORD,
                dbname=_PG_DATABASE) \
                .with_bind_ports(5432, pg_random_port) as container:
            # Setup database
            with create_engine(conn_url).connect() as conn:
                _setup_pg_db(conn)
                conn.commit()

            yield container


@pytest.fixture(params=['postgres', 'sqlite'])
def as_db(request: 'FixtureRequest', autosubmit: Autosubmit, tmp_path: 'LocalPath', postgres_server: DockerContainer,
          autosubmit_exp):
    """A parametrized fixture that creates the autosubmitrc file for databases.

    Works with sqlite and postgres.

    The database created is exclusive for the current test. It is not shared
    with other tests.

    At the end, it calls ``autosubmit_exp`` just to get ``Autosubmit.install``
    called, which populates the database and creates other directories.

    The ``BasicConfig`` properties will have been updated too.

    :return: The current database name.
    """
    backend = request.param
    autosubmitrc_file = Path(tmp_path) / 'autosubmitrc'
    if not autosubmitrc_file.exists():
        raise ValueError(f'Missing autosubmitrc file: {autosubmitrc_file}')

    if backend == 'postgres':
        # Replace the backend by postgres (default is sqlite)
        user = postgres_server.env['POSTGRES_USER']
        password = postgres_server.env['POSTGRES_PASSWORD']
        port = postgres_server.ports[5432]
        db = request.node.name
        if '[' in db:
            db = db.split('[')[0]
        db = f'{db}_{time_ns()}'

        # Create a new DB to run the current test completely isolated from others.
        # We use the test name, minus the [params], appending the current nanoseconds
        # instead to distinguish parametrized tests too -- really isolated.
        from sqlalchemy import create_engine, text
        engine = create_engine(f'postgresql://{user}:{password}@localhost:{port}/postgres')
        with engine.connect() as conn:
            conn.execution_options(isolation_level="AUTOCOMMIT").execute(
                text(f"CREATE DATABASE {db}")
            )

        # And now replace the INI settings that have the default value set to SQLite.
        config = configparser.ConfigParser()
        config.read(autosubmitrc_file)
        to_delete = ['path', 'filename']
        for to_del in to_delete:
            if config.has_option('database', to_del):
                config.remove_option('database', to_del)
        config.set('database', 'backend', 'postgres')
        connection_url = f'postgresql://{user}:{password}@localhost:{port}/{db}'
        config.set('database', 'connection_url', connection_url)
        with open(autosubmitrc_file, 'w') as f:
            config.write(f)
    elif backend == 'sqlite':
        ...
    else:
        raise ValueError(f'Unsupported database backend: {backend}')

    BasicConfig.read()

    # DO NOT USE THIS EXPID!
    # TODO: This function calls ``Autosubmit.install``, or we could call it here.
    # Previous tests were using it and everything is working, but this doesn't
    # smell very good. There might be a better way.
    autosubmit_exp('____')

    return backend


def wait_child(timeout, retry=3):
    """A parametrized fixture that will retry function X amount of times waiting for a child process to be executed.

    In case it still fails after X retries an exception is thrown."""
    def the_real_decorator(function):
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < retry:
                try:
                    value = function(*args, **kwargs)
                    if value is None:
                        return
                except Exception:
                    time.sleep(timeout)
                    retries += 1

        return wrapper

    return the_real_decorator
