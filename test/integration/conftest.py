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
from contextlib import suppress
from dataclasses import dataclass
from importlib.metadata import version, PackageNotFoundError
from pathlib import Path
from tempfile import TemporaryDirectory
from time import time_ns
from typing import cast, Any, Callable, Generator, Iterator, Optional, Protocol, TYPE_CHECKING

import paramiko  # type: ignore
import pytest
from portalocker import Lock, LOCK_EX
from ruamel.yaml import YAML
from sqlalchemy import create_engine
from testcontainers.postgres import PostgresContainer  # type: ignore

from autosubmit.autosubmit import Autosubmit
from autosubmit.config.basicconfig import BasicConfig
from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.experiment.experiment_common import next_experiment_id
from autosubmit.log.log import AutosubmitCritical
from autosubmit.platforms.paramiko_platform import ParamikoPlatform
from autosubmit.platforms.psplatform import PsPlatform
from autosubmit.platforms.slurmplatform import SlurmPlatform
from test.integration.test_utils.docker import (
    get_git_container, get_slurm_container, get_ssh_container,
    stop_test_containers
)
from test.integration.test_utils.networking import get_free_port, wait_for_tcp_port
from test.integration.test_utils.postgres import setup_pg_db
from test.integration.test_utils.pytest import get_next_pytest_base_temp

if TYPE_CHECKING:
    from docker.models.containers import Container
    # noinspection PyProtectedMember
    from _pytest.tmpdir import TempPathFactory
    # noinspection PyProtectedMember
    from py._path.local import LocalPath  # type: ignore
    from pytest_mock import MockerFixture
    from pytest import FixtureRequest
    from testcontainers.core.container import DockerContainer  # type: ignore

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


@pytest.fixture(scope='session')
def get_next_expid(tmp_path_factory) -> Callable[[], str]:
    """Returns a factory to retrieve the next Autosubmit experiment ID.

    The returned experiment ID by the factory function is guaranteed to
    be uniquely throughout the whole test session, even with multiple
    pytest-xdist processes.

    It uses an OS-level lock for mutual exclusion, and a file to share
    the last/current expid through all processes (in case pytest-xdist is used).
    """

    def _get_next_expid() -> str:
        shared_tmp_dir = tmp_path_factory.getbasetemp().parent
        lock_path = shared_tmp_dir / "expid_global.lock"

        expid_file = shared_tmp_dir / "expid_current.txt"

        with Lock(str(lock_path), flags=LOCK_EX, timeout=120):
            current_expid = 't000'
            if expid_file.exists():
                current_expid = expid_file.read_text()

            next_expid = next_experiment_id(current_id=current_expid)

            expid_file.write_text(next_expid)

        return next_expid

    return _get_next_expid


@pytest.fixture
def autosubmit_exp(
        autosubmit: Autosubmit,
        request: "FixtureRequest",
        tmp_path: "LocalPath",
        mocker: "MockerFixture",
        get_next_expid: Callable[[], str]
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
            create: Optional[bool] = True,
            include_jobs: Optional[bool] = False,
            *_,
            **kwargs
    ) -> AutosubmitExperiment:
        if experiment_data is None:
            experiment_data = {}

        is_postgres = hasattr(BasicConfig, 'DATABASE_BACKEND') and BasicConfig.DATABASE_BACKEND == 'postgres'
        autosubmit.configure(
            advanced=False,
            database_path=BasicConfig.DB_DIR if not is_postgres else "",  # type: ignore
            database_filename=BasicConfig.DB_FILE if not is_postgres else "",  # type: ignore
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
        if not Path(BasicConfig.DB_PATH).exists() and not is_postgres:
            autosubmit.install()

        if not expid:
            expid = get_next_expid()

        mocker.patch('autosubmit.experiment.experiment_common.db_common.last_name_used', return_value=expid)
        operational = expid.startswith('o')
        evaluation = expid.startswith('e')
        testcase = expid.startswith('t')

        # Never reuse an experiment or reconfigure in tests for true test isolation.
        # - https://learn.microsoft.com/en-us/dotnet/core/testing/unit-testing-best-
        #   practices#characteristics-of-good-unit-tests
        # - https://wiki.c2.com/?UnitTestIsolation
        # - https://www.thoughtworks.com/en-es/insights/blog/testing/ephemeral-testing-environments-kill-darlings
        if Path(tmp_path / expid).exists():
            pytest.xfail(f'The test is trying to use {expid} as expid but its directory exists: {str(tmp_path)}!')

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
        # TODO: Autosubmit.install issue https://github.com/BSC-ES/autosubmit/issues/1328
        Path(BasicConfig.STRUCTURES_DIR).mkdir(parents=True, exist_ok=True)
        exp_tmp_dir = exp_path / BasicConfig.LOCAL_TMP_DIR
        aslogs_dir = exp_tmp_dir / BasicConfig.LOCAL_ASLOG_DIR
        status_dir = exp_path / 'status'
        job_data_dir = Path(BasicConfig.JOBDATA_DIR)
        job_data_dir.mkdir(parents=True, exist_ok=True)

        config = AutosubmitConfig(
            expid=expid,
            basic_config=BasicConfig
        )
        config.reload(force_load=True)

        # Remove original files. So we can save a new file with all the memory modifications.
        for f in conf_dir.iterdir():
            if f.is_file():
                f.unlink()

        must_exists = ['DEFAULT', 'JOBS', 'PLATFORMS', 'CONFIG']
        # Default values for experiment data
        # TODO: This probably has a way to be initialized in config-parser?
        for must_exist in must_exists:
            if must_exist not in config.experiment_data:
                config.experiment_data[must_exist] = {}

        if not config.experiment_data.get('CONFIG').get('AUTOSUBMIT_VERSION', ''):
            try:
                config.experiment_data['CONFIG']['AUTOSUBMIT_VERSION'] = version('autosubmit')
            except PackageNotFoundError:
                config.experiment_data['CONFIG']['AUTOSUBMIT_VERSION'] = ''

        config.experiment_data['CONFIG']['SAFETYSLEEPTIME'] = 0
        config.experiment_data['DEFAULT']['EXPID'] = expid

        if not include_jobs:
            config.experiment_data['JOBS'] = {}

        # ensure that it is always the first file loaded by Autosubmit
        with open(conf_dir / 'aaaaaabasic_structure.yml', 'w') as f:
            YAML().dump(config.experiment_data, f)

        other_yaml = {
            k: v for k, v in experiment_data.items()
        }
        if other_yaml:
            with open(conf_dir / 'additional_data.yml', 'w') as f:
                YAML().dump(other_yaml, f)

        config.reload(force_load=True)

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

        if create:
            autosubmit.create(expid, noplot=True, hide=False, force=True, check_wrappers=wrapper)
            config.set_last_as_command('create')
        else:
            config.set_last_as_command('expid')

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
def git_server(git_repos_shared_dir) -> tuple['Container', Path, str]:
    # Start a container to server it -- otherwise, we would have to use
    # `git -c protocol.file.allow=always submodule ...`, and we cannot
    # change how Autosubmit uses it in `autosubmit create` (due to bad
    # code design choices).
    shared_tmp_dir = git_repos_shared_dir
    lock_path = shared_tmp_dir / "git_global.lock"

    base_path = shared_tmp_dir / 'git_repos_base'
    git_repos_path = base_path / 'git_repos'
    git_repos_path.mkdir(exist_ok=True, parents=True)

    container, http_port = get_git_container(lock_path, git_repos_path)
    # http_port = int(container.ports['80/tcp'][0]['HostPort'])  # type: ignore

    repo_url = f'http://localhost:{http_port}/git'

    wait_for_tcp_port('localhost', http_port)

    yield container, git_repos_path, repo_url


@pytest.fixture
def ps_platform() -> PsPlatform:
    platform = PsPlatform(expid='a000', name='ps', config={})
    return platform


def _mock_ssh_config(ssh_config_path: Path, mocker: 'MockerFixture'):
    ssh_config = paramiko.SSHConfig()
    with open(ssh_config_path, 'r') as f:
        ssh_config.parse(f)
    return mocker.patch('autosubmit.platforms.paramiko_platform._load_ssh_config', return_value=ssh_config)


@pytest.fixture(scope='session')
def ssh_server(tmp_path_factory: 'TempPathFactory', session_mocker: 'MockerFixture') -> 'Container':
    """Start a single Docker container serving SSH for integration tests."""
    shared_tmp_dir = tmp_path_factory.getbasetemp()
    lock_path = shared_tmp_dir / "ssh_vanilla_global.lock"
    ssh_dir = Path(shared_tmp_dir, 'ssh/')
    ssh_dir.mkdir(exist_ok=True)
    container, ssh_port, ssh_config = get_ssh_container(lock_path, ssh_dir, mfa=False, x11=False)
    _mock_ssh_config(ssh_config, session_mocker)
    return container


@pytest.fixture(scope='session')
def ssh_x11_server(tmp_path_factory: 'TempPathFactory', session_mocker: 'MockerFixture') -> 'Container':
    """Get a running SSH server with X11 enabled (no MFA)."""
    shared_tmp_dir = tmp_path_factory.getbasetemp()
    lock_path = shared_tmp_dir / "ssh_x11_global.lock"
    ssh_dir = Path(shared_tmp_dir, 'ssh/')
    ssh_dir.mkdir(exist_ok=True)
    container, ssh_port, ssh_config = get_ssh_container(lock_path, ssh_dir, mfa=False, x11=True)
    _mock_ssh_config(ssh_config, session_mocker)
    return container


@pytest.fixture(scope='function')
def ssh_x11_mfa_server(tmp_path, mocker: 'MockerFixture') -> 'Container':
    """Get a running SSH server with X11 and MFA enabled.

    We cannot have a singleton X11+MFA because the MFA-enabled containers
    must ask the user for the MFA token. And for testing, we used a setup
    where instead of setting up proper 2FA, we use backup codes.

    And since backup codes can be used exactly once, if we used a singleton
    container then it would be able to be used in exactly one test case.
    """
    lock_path = tmp_path / "ssh_x11_mfa_global.lock"  # not really important as it's not a singleton
    ssh_dir = Path(tmp_path, 'ssh/')
    ssh_dir.mkdir()
    container, ssh_port, ssh_config = get_ssh_container(lock_path, ssh_dir, mfa=True, x11=True, singleton=False)
    _mock_ssh_config(ssh_config, mocker)
    yield container


@pytest.fixture(scope="session")
def slurm_server(tmp_path_factory, session_mocker) -> 'Container':
    """Session fixture that creates a singleton Slurm server container."""
    shared_tmp_dir = tmp_path_factory.getbasetemp().parent
    lock_path = shared_tmp_dir / "slurm_global.lock"
    ssh_dir = Path(shared_tmp_dir, 'ssh/')
    ssh_dir.mkdir(exist_ok=True)
    container, ssh_port, ssh_config = get_slurm_container(lock_path, ssh_dir)
    _mock_ssh_config(ssh_config, session_mocker)
    # TODO: Needed? If so, explain why.
    session_mocker.patch(
        'autosubmit.platforms.platform.Platform.get_mp_context',
        return_value=multiprocessing.get_context('fork')
    )
    wait_for_tcp_port('localhost', ssh_port)
    return container


@pytest.fixture
def ssh_fixture(request):
    """Used for indirect Pytest parameters resolution.

    See ``test_paramiko_platform.py`` for an example use case.
    """
    if hasattr(request, 'param'):
        return request.getfixturevalue(request.param)
    return None


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
                setup_pg_db(conn)
                conn.commit()

            yield container


@pytest.fixture(params=['postgres', 'sqlite'])
def as_db(request: 'FixtureRequest', autosubmit: Autosubmit, tmp_path: 'LocalPath', postgres_server: 'DockerContainer',
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

    os.environ['AUTOSUBMIT_CONFIGURATION'] = str(autosubmitrc_file)

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
    # TODO: check which functions call as_db twice or if this
    #       is used in combination other fixture that calls autosubmit.install
    with suppress(AutosubmitCritical):
        autosubmit.install()

    return backend


@pytest.fixture(scope='session')
def git_repos_shared_dir(request) -> Path:
    """Fixture to return the Git repository used by the Git singleton container.

    This directory is mapped in an HTTPD virtual directory, from where Git
    repositories are served. If you create a folder inside this shared directory
    it will be a new Git repository (i.e. make them unique).
    """
    config = request.config
    is_xdist_master = not hasattr(config, "workerinput")

    if is_xdist_master:
        # pytest-xdist master
        # noinspection PyProtectedMember
        return config._git_repos_shared_dir

    # pytest-xdist worker
    return Path(config.workerinput['git_repos_shared_dir'])


# --- Pytest hooks

# Pytest hooks. Review pytest docs for changes in case of problems.
#
# Before session (showing a few):
#
# - ``pytest_addoption``
# - ``pytest_load_initial_conftests``
# - ``pytest_cmdline_main``
# - ``pytest_configure``
# - ``pytest_configure_node``
#
# NOTE: ``pytest_configure_node`` is not a pytest hook, but a pytest-xdist
#       hook! It's called before the session starts, and can add configuration
#       for xdist nodes/workers like folders to be shared by all nodes.
#
# When session is created:
#
# - ``pytest_sessionstart``
#
# Then come the collection phase hooks (``pytest_collection``, ``pytest_collect_file``, ...).
#
# Followed by test execution (our fixtures are included there), reporting execution,
# and finally the session end hooks:
#
# - ``pytest_sessionfinish``
# - ``pytest_unconfigure``
#
# Try to keep these hooks together or troubleshooting it might be really challenging,
# even if other code is added below, when adding new hooks, please include them near
# these existing ones.
#
# Finally, remember that pytest-xdist will fork a new process per session, so hooks
# that have sessions (including our ``pytest.fixture(scope=session|function|etc.)``)
# are all executed as many times as processes/workers we have in pytest-xdist.
#
# Refs:
# - https://github.com/pytest-dev/pytest/issues/3261#issuecomment-3380604983
# - https://pytest-xdist.readthedocs.io/en/stable/how-it-works.html

def pytest_configure(config) -> None:
    is_xdist_master = not hasattr(config, "workerinput")

    if not is_xdist_master:
        return

    # The rest of the code only runs on pytest-xdist master.
    stop_test_containers()

    # This hook is called before the one-or-many pytest-xdist nodes
    # are created, so we create the shared directories here, to then
    # use them in ``pytest_configure_node`` (next function).
    #
    # Also, remember to use private attributes to avoid conflicts with
    # pytest attributes, or attributes set by other libraries. An
    # attribute like ``_test`` has high risk of conflicts and weird
    # bugs.

    next_pytest_folder = get_next_pytest_base_temp()

    # NOTE: Do NOT call ``.mkdir`` yet. Pytest will create a directory
    #       upon starting a new session.

    git_repos_path = Path(next_pytest_folder, 'git_repos')
    config._git_repos_shared_dir = git_repos_path


def pytest_configure_node(node) -> None:
    """Pytest hook to configure workers.

    Ref: https://github.com/pytest-dev/pytest-xdist/blob/
         2e1b1ad03f2c285639f40b1365fbbb1c447997c2/src/xdist/newhooks.py#L73-L75
    """
    # These directories are created in the last hook of pytest before
    # this hook is called, and before the pytest-xdist workers are launched
    # and the pytest sessions start.
    #
    # This allows to give all the pytest-xdist workers the same shared
    # directory to load things like common SSH keys or Git repositories
    # to be used by the singleton Docker containers used in our tests.
    # We are doing this as spawning one container per test started causing
    # slowness and resource contention in our tests.

    # NOTE: Do not call ``.mkdir`` on these folders yet.

    # noinspection PyProtectedMember
    node.workerinput["git_repos_shared_dir"] = str(
        node.config._git_repos_shared_dir
    )


def pytest_sessionstart(session) -> None:
    config = session.config
    is_xdist_master = not hasattr(config, "workerinput")

    if not is_xdist_master:
        return

    # The rest of the code only runs on pytest-xdist master.

    # noinspection PyProtectedMember
    tmp_path_factory = session.config._tmp_path_factory

    # Calling ``.getbasetemp`` is dangerous as it is called by
    # Pytest and must create a base path **per-session**. However,
    # in this case it is safe because we already have a session,
    # and the next time Pytest calls it, it knows there is already
    # a session and ``._basetemp`` created, so it re-uses that.
    _ = tmp_path_factory.getbasetemp()

    # Now call ``.mkdir`` for the shared folders.

    # noinspection PyProtectedMember
    config._git_repos_shared_dir.mkdir()


# noinspection PyUnusedLocal
def pytest_sessionfinish(session, exitstatus) -> None:
    """Finish pytest session."""
    config = session.config
    is_xdist_master = not hasattr(config, "workerinput")

    if not is_xdist_master:
        return

    # The rest of the code only runs on pytest-xdist master.

    stop_test_containers()

# --- Pytest hooks
