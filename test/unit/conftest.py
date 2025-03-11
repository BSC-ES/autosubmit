# Fixtures available to multiple test files must be created in this file.
from contextlib import suppress

import pytest
import pytest_mock
from dataclasses import dataclass
from pathlib import Path
from ruamel.yaml import YAML
from shutil import rmtree
from tempfile import TemporaryDirectory
from typing import Any, Dict, Callable, Generator, List, Protocol, Optional, Type
import os
from sqlalchemy import Connection, create_engine, text

from autosubmit.autosubmit import Autosubmit
from autosubmit.platforms.slurmplatform import SlurmPlatform, ParamikoPlatform
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory


DEFAULT_DATABASE_CONN_URL = (
    "postgresql://postgres:mysecretpassword@localhost:5432/autosubmit_test"
)


@dataclass
class AutosubmitExperiment:
    """This holds information about an experiment created by Autosubmit."""
    expid: str
    autosubmit: Autosubmit
    exp_path: Path
    tmp_dir: Path
    aslogs_dir: Path
    status_dir: Path
    platform: ParamikoPlatform

@pytest.fixture(scope='function')
def autosubmit_exp(autosubmit: Autosubmit, request: pytest.FixtureRequest) -> Callable:
    """Create an instance of ``Autosubmit`` with an experiment."""

    original_root_dir = BasicConfig.LOCAL_ROOT_DIR
    tmp_dir = TemporaryDirectory()
    tmp_path = Path(tmp_dir.name)


    def _create_autosubmit_exp(expid: str):
        root_dir = tmp_path
        BasicConfig.LOCAL_ROOT_DIR = str(root_dir)
        exp_path = BasicConfig.expid_dir(expid)
        
        # directories used when searching for logs to cat
        exp_tmp_dir = BasicConfig.expid_tmp_dir(expid) 
        aslogs_dir = BasicConfig.expid_aslog_dir(expid) 
        status_dir =exp_path / 'status'
        if not os.path.exists(aslogs_dir):
            os.makedirs(aslogs_dir)
        if not os.path.exists(status_dir):
            os.makedirs(status_dir)
        
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

        return AutosubmitExperiment(
            expid=expid,
            autosubmit=autosubmit,
            exp_path=exp_path,
            tmp_dir=exp_tmp_dir,
            aslogs_dir=aslogs_dir,
            status_dir=status_dir,
            platform=platform
        )

    def finalizer():
        BasicConfig.LOCAL_ROOT_DIR = original_root_dir
        if tmp_path and tmp_path.exists():
            rmtree(tmp_path)

    request.addfinalizer(finalizer)

    return _create_autosubmit_exp


@pytest.fixture(scope='module')
def autosubmit() -> Autosubmit:
    """Create an instance of ``Autosubmit``.

    Useful when you need ``Autosubmit`` but do not need any experiments."""
    autosubmit = Autosubmit()
    return autosubmit


@pytest.fixture(scope='function')
def create_as_conf() -> Callable:  # May need to be changed to use the autosubmit_config one
    def _create_as_conf(autosubmit_exp: AutosubmitExperiment, yaml_files: List[Path], experiment_data: Dict[str, Any]):
        conf_dir = autosubmit_exp.exp_path.joinpath('conf')
        conf_dir.mkdir(parents=False, exist_ok=False)
        basic_config = BasicConfig
        parser_factory = YAMLParserFactory()
        as_conf = AutosubmitConfig(
            expid=autosubmit_exp.expid,
            basic_config=basic_config,
            parser_factory=parser_factory
        )
        for yaml_file in yaml_files:
            with open(conf_dir / yaml_file.name, 'w+') as f:
                f.write(yaml_file.read_text())
                f.flush()
        # add user-provided experiment data
        with open(conf_dir / 'conftest_as.yml', 'w+') as f:
            yaml = YAML()
            yaml.indent(sequence=4, offset=2)
            yaml.dump(experiment_data, f)
            f.flush()
        return as_conf

    return _create_as_conf

class AutosubmitConfigFactory(Protocol):  # Copied from the autosubmit config parser, that I believe is a revised one from the create_as_conf

    def __call__(self, expid: str, experiment_data: Optional[Dict], *args: Any, **kwargs: Any) -> AutosubmitConfig: ...


@pytest.fixture(scope="function")
def autosubmit_config(
        request: pytest.FixtureRequest,
        mocker: "pytest_mock.MockerFixture") -> AutosubmitConfigFactory:
    """Return a factory for ``AutosubmitConfig`` objects.

    Abstracts the necessary mocking in ``AutosubmitConfig`` and related objects,
    so that if we need to modify these, they can all be done in a single place.

    It is able to create any configuration, based on the ``request`` parameters.

    When the function (see ``scope``) finishes, the object and paths created are
    cleaned (see ``finalizer`` below).
    """

    original_root_dir = BasicConfig.LOCAL_ROOT_DIR
    tmp_dir = TemporaryDirectory()
    tmp_path = Path(tmp_dir.name)

    # Mock this as otherwise BasicConfig.read resets our other mocked values above.
    mocker.patch.object(BasicConfig, "read", autospec=True)

    def _create_autosubmit_config(expid: str, experiment_data: Dict = None, *_, **kwargs) -> AutosubmitConfig:
        """Create an instance of ``AutosubmitConfig``."""
        root_dir = tmp_path
        BasicConfig.LOCAL_ROOT_DIR = str(root_dir)
        exp_path = root_dir / expid
        exp_tmp_dir = exp_path / BasicConfig.LOCAL_TMP_DIR
        aslogs_dir = exp_tmp_dir / BasicConfig.LOCAL_ASLOG_DIR
        conf_dir = exp_path / "conf"
        aslogs_dir.mkdir(parents=True)
        conf_dir.mkdir()

        if not expid:
            raise ValueError("No value provided for expid")
        config = AutosubmitConfig(
            expid=expid,
            basic_config=BasicConfig
        )
        if experiment_data is not None:
            config.experiment_data = experiment_data

        for arg, value in kwargs.items():
            setattr(config, arg, value)

        config.current_loaded_files = [conf_dir / 'dummy-so-it-doesnt-force-reload.yml']
        return config

    def finalizer() -> None:
        BasicConfig.LOCAL_ROOT_DIR = original_root_dir
        with suppress(FileNotFoundError):
            rmtree(tmp_path)

    request.addfinalizer(finalizer)

    return _create_autosubmit_config


@pytest.fixture
def prepare_basic_config(tmpdir):
    basic_conf = BasicConfig()
    BasicConfig.DB_DIR = (tmpdir / "exp_root")
    BasicConfig.DB_FILE = "debug.db"
    BasicConfig.LOCAL_ROOT_DIR = (tmpdir / "exp_root")
    BasicConfig.LOCAL_TMP_DIR = "tmp"
    BasicConfig.LOCAL_ASLOG_DIR = "ASLOGS"
    BasicConfig.LOCAL_PROJ_DIR = "proj"
    BasicConfig.DEFAULT_PLATFORMS_CONF = ""
    BasicConfig.CUSTOM_PLATFORMS_PATH = ""
    BasicConfig.DEFAULT_JOBS_CONF = ""
    BasicConfig.SMTP_SERVER = ""
    BasicConfig.MAIL_FROM = ""
    BasicConfig.ALLOWED_HOSTS = ""
    BasicConfig.DENIED_HOSTS = ""
    BasicConfig.CONFIG_FILE_FOUND = False
    return basic_conf


def _identity_value(value=None):
    """A type of identity function; returns a function that returns ``value``."""
    return lambda *ignore_args, **ignore_kwargs: value


@pytest.fixture
def as_db_sqlite(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Type[BasicConfig]:
    """Overwrites the BasicConfig to use SQLite database for testing.
    Args:
        monkeypatch: Monkey Patcher.
    Returns:
        BasicConfig class.
    """
    monkeypatch.setattr(BasicConfig, "read", _identity_value())
    monkeypatch.setattr(BasicConfig, "DATABASE_BACKEND", "sqlite")
    monkeypatch.setattr(BasicConfig, "DB_PATH", str(tmp_path / "autosubmit.db"))

    return BasicConfig


def _setup_pg_db(conn: Connection) -> None:
    """Reset the database.
    Drops all schemas except the system ones and restoring the public schema.
    Args:
        conn: Database connection.
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


@pytest.fixture
def as_db_postgres(monkeypatch: pytest.MonkeyPatch) -> Generator[BasicConfig, Any, None]:
    """Fixture to set up and tear down a Postgres database for testing.
    It will overwrite the ``BasicConfig`` to use Postgres.
    It uses the environment variable ``PYTEST_DATABASE_CONN_URL`` to connect to the database.
    If the variable is not set, it uses the default connection URL.
    Args:
        monkeypatch: Monkey Patcher.
    Returns:
        Autosubmit configuration for Postgres.
    """

    conn_url = os.environ.get("PYTEST_DATABASE_CONN_URL", DEFAULT_DATABASE_CONN_URL)

    # Apply patch BasicConfig
    monkeypatch.setattr(BasicConfig, "read", _identity_value())
    monkeypatch.setattr(BasicConfig, "DATABASE_BACKEND", "postgres")
    monkeypatch.setattr(
        BasicConfig,
        "DATABASE_CONN_URL",
        conn_url,
    )

    # Setup database
    with create_engine(conn_url).connect() as conn:
        _setup_pg_db(conn)
        conn.commit()

    yield BasicConfig

    # Teardown database
    with create_engine(conn_url).connect() as conn:
        _setup_pg_db(conn)
        conn.commit()
