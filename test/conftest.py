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

# Fixtures available to multiple test files must be created in this file.
import os
import pwd
from dataclasses import dataclass
from datetime import datetime
from fileinput import FileInput
from pathlib import Path
from random import seed, randint, choice
from re import sub
from textwrap import dedent
from time import time
from typing import TYPE_CHECKING, Any, Dict, Protocol, Optional, Type, List

import pytest
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from pytest_mock import MockerFixture
from ruamel.yaml import YAML

from autosubmit.autosubmit import Autosubmit
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.platforms.slurmplatform import SlurmPlatform, ParamikoPlatform

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from py._path.local import LocalPath  # type: ignore


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


def _initialize_autosubmitrc(folder: Path) -> Path:
    """Initialize the ``autosubmit.rc`` file.

    This function should populate enough information so ``BasicConfig.read()`` works
    without the need of any mocking.

    The Autosubmit database file used is called ``tests.db``.

    This function can be called multiple times.

    :param folder: Folder to sve the ``.autosubmitrc`` file in.
    :return: Path to the ``.autosubmitrc`` file.
    """
    autosubmitrc = folder / '.autosubmitrc'
    autosubmitrc.write_text(
        dedent(f'''\
                [database]
                path = {folder}
                filename = tests.db

                [local]
                path = {folder}

                [globallogs]
                path = {folder / "logs"}

                [structures]
                path = {folder / "metadata/structures"}

                [historicdb]
                path = {folder / "metadata/data"}

                [historiclog]
                path = {folder / "metadata/logs"}

                [defaultstats]
                path = {folder / "as_output/stats"}
                ''')
    )
    return autosubmitrc


class AutosubmitExperimentFixture(Protocol):
    """Type for ``autosubmit_exp`` fixture."""
    def __call__(
            self,
            expid: Optional[str] = None,
            experiment_data: Optional[Dict] = None,
            *args: Any,
            **kwargs: Any
    ) -> AutosubmitExperiment:
        ...


@pytest.fixture(scope='function')
def autosubmit_exp(
        autosubmit: Autosubmit,
        request: pytest.FixtureRequest,
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
            experiment_data: Optional[Dict] = None,
            *_,
            **kwargs
    ):
        if experiment_data is None:
            experiment_data = {}

        autosubmitrc = _initialize_autosubmitrc(tmp_path)
        os.environ['AUTOSUBMIT_CONFIGURATION'] = str(autosubmitrc)

        BasicConfig.read()

        if not Path(BasicConfig.DB_PATH).exists():
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
            )

        if expid:
            mocker.patch('autosubmit.experiment.experiment_common.db_common.last_name_used', return_value=expid)

        expid = autosubmit.expid(
            description=f"Pytest experiment (delete me)",
            hpc="local",
            copy_id="",
            dummy=True,
            minimal_configuration=False,
            git_repo="",
            git_branch="",
            git_as_conf="",
            operational=False,
            testcase=True,
            evaluation=False,
            use_local_minimal=False
        )
        exp_path = Path(BasicConfig.LOCAL_ROOT_DIR) / expid
        conf_dir = exp_path / "conf"
        global_logs = Path(BasicConfig.GLOBAL_LOG_DIR)
        global_logs.mkdir(parents=True, exist_ok=True)
        exp_tmp_dir = exp_path / BasicConfig.LOCAL_TMP_DIR
        aslogs_dir = exp_tmp_dir / BasicConfig.LOCAL_ASLOG_DIR
        status_dir = exp_path / 'status'

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

        for key, file in key_file.items():
            if key in experiment_data:
                mode = 'a' if key == 'EXPERIMENT' else 'w'
                with open(conf_dir / f'{file}_{expid}.yml', mode) as f:
                    YAML().dump({key: experiment_data[key]}, f)

        other_yaml = {
            k: v for k, v in experiment_data.items()
            if k not in key_file
        }
        if other_yaml:
            with open(conf_dir / f'tests_{expid}.yml', 'w') as f:
                YAML().dump(other_yaml, f)

        config.reload(force_load=True)

        # TBD: this is not set in ``AutosubmitConfig``, but
        # maybe it should be? Ignore linter errors for now.
        config.autosubmitrc = autosubmitrc

        # Default values for experiment data
        # TODO: This probably has a way to be initialized in config-parser?
        must_exists = ['DEFAULT', 'JOBS', 'PLATFORMS', 'CONFIG']
        for must_exist in must_exists:
            if must_exist not in config.experiment_data:
                config.experiment_data[must_exist] = {}

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
        with FileInput(conf_dir / f'autosubmit_{expid}.yml', inplace=True, backup='.bak') as file:
            for line in file:
                if 'SAFETYSLEEPTIME' in line:
                    print(sub(r'\d+', '0', line), end='')
                else:
                    print(line, end='')
        # TODO: one test failed while moving things from unit to integration, but this shouldn't be
        #       needed, especially if the disk has the valid value?
        config.experiment_data['DEFAULT']['EXPID'] = expid

        autosubmit.create(expid, noplot=True, hide=False, force=True)

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

    return _create_autosubmit_exp


@pytest.fixture(scope='module')
def autosubmit() -> Autosubmit:
    """Create an instance of ``Autosubmit``.

    Useful when you need ``Autosubmit`` but do not need any experiments."""
    return Autosubmit()


# Copied from the autosubmit config parser, that I believe is a revised one from the create_as_conf
class AutosubmitConfigFactory(Protocol):

    def __call__(self, expid: str, experiment_data: Optional[Dict] = None, *args: Any,
                 **kwargs: Any) -> AutosubmitConfig: ...


@pytest.fixture(scope="function")
def autosubmit_config(
        request: pytest.FixtureRequest,
        tmp_path: Path,
        autosubmit: Autosubmit,
        mocker: MockerFixture
) -> AutosubmitConfigFactory:
    """Return a factory for ``AutosubmitConfig`` objects.

    Abstracts the necessary mocking in ``AutosubmitConfig`` and related objects,
    so that if we need to modify these, they can all be done in a single place.

    It is able to create any configuration, based on the ``request`` parameters.

    When the function (see ``scope``) finishes, the object and paths created are
    cleaned (see ``finalizer`` below).
    """

    # Mock this as otherwise BasicConfig.read resets our other mocked values above.
    mocker.patch.object(BasicConfig, "read", autospec=True)

    def _prepare_basic_config(folder: Path) -> BasicConfig:
        """Sets up ``BasicConfig`` using a given temporary directory as root dir."""
        basic_conf = BasicConfig()
        BasicConfig.DB_DIR = folder / "exp_root"
        BasicConfig.DB_FILE = "debug.db"
        BasicConfig.DB_PATH = BasicConfig.DB_DIR / BasicConfig.DB_FILE
        BasicConfig.LOCAL_ROOT_DIR = folder / "exp_root"
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
        BasicConfig.CONFIG_FILE_FOUND = True
        BasicConfig.GLOBAL_LOG_DIR = folder / "global_logs"
        return basic_conf

    def _create_autosubmit_config(
            expid: str,
            experiment_data: Dict = None,
            *_,
            **kwargs
    ) -> AutosubmitConfig:
        """Create an Autosubmit configuration object.

        The values in ``BasicConfig`` are configured to use a temporary directory as base,
        then create the ``exp_root`` as the experiment directory (equivalent to the
        ``~/autosubmit/<EXPID>``).

        This function also sets the environment variable ``AUTOSUBMIT_CONFIGURATION``.

        :param expid: Experiment ID
        :param experiment_data: YAML experiment data dictionary
        """
        if not expid:
            raise ValueError("No value provided for expid")

        if experiment_data is None:
            experiment_data = {}

        autosubmitrc = _initialize_autosubmitrc(tmp_path)
        os.environ['AUTOSUBMIT_CONFIGURATION'] = str(autosubmitrc)

        basic_config = _prepare_basic_config(tmp_path)
        for k, v in basic_config.__dict__.items():
            setattr(BasicConfig, k, v)

        exp_path = BasicConfig.LOCAL_ROOT_DIR / expid
        # <expid>/tmp/
        exp_tmp_dir = exp_path / BasicConfig.LOCAL_TMP_DIR
        # <expid>/tmp/ASLOGS
        aslogs_dir = exp_tmp_dir / BasicConfig.LOCAL_ASLOG_DIR
        # <expid>/tmp/LOG_<expid>
        expid_logs_dir = exp_tmp_dir / f'LOG_{expid}'
        Path(expid_logs_dir).mkdir(parents=True, exist_ok=True)
        # <expid>/conf
        conf_dir = exp_path / "conf"
        Path(aslogs_dir).mkdir(exist_ok=True)
        Path(conf_dir).mkdir(exist_ok=True)
        # <expid>/pkl
        pkl_dir = exp_path / "pkl"
        Path(pkl_dir).mkdir(exist_ok=True)
        # ~/autosubmit/autosubmit.db
        db_path = Path(BasicConfig.DB_PATH)
        db_path.touch()
        # <TEMP>/global_logs
        global_logs = Path(BasicConfig.GLOBAL_LOG_DIR)
        global_logs.mkdir(parents=True, exist_ok=True)

        config = AutosubmitConfig(
            expid=expid,
            basic_config=BasicConfig
        )

        # Populate the configuration object's ``experiment_data`` dictionary with the values
        # in ``BasicConfig``. For some reason, some platforms use variables like ``LOCAL_ROOT_DIR``
        # from the configuration object, instead of using ``BasicConfig``.
        for k, v in {k: v for k, v in basic_config.__class__.__dict__.items() if not k.startswith('__')}.items():
            config.experiment_data[k] = v
        config.experiment_data.update(experiment_data)

        # Default values for experiment data
        # TODO: This probably has a way to be initialized in config-parser?
        must_exists = ['DEFAULT', 'JOBS', 'PLATFORMS', 'CONFIG']
        for must_exist in must_exists:
            if must_exist not in config.experiment_data:
                config.experiment_data[must_exist] = {}

        config.experiment_data['CONFIG']['SAFETYSLEEPTIME'] = 0
        config.experiment_data['DEFAULT']['EXPID'] = expid

        if 'HPCARCH' not in config.experiment_data['DEFAULT']:
            config.experiment_data['DEFAULT']['HPCARCH'] = 'LOCAL'

        for arg, value in kwargs.items():
            setattr(config, arg, value)
        config.current_loaded_files[str(conf_dir / 'dummy-so-it-doesnt-force-reload.yml')] = time()
        return config

    return _create_autosubmit_config


@pytest.fixture
def current_tmpdir(tmpdir_factory):
    folder = tmpdir_factory.mktemp(f'tests')
    os.mkdir(folder.join('scratch'))
    file_stat = os.stat(f"{folder.strpath}")
    file_owner_id = file_stat.st_uid
    file_owner = pwd.getpwuid(file_owner_id).pw_name
    folder.owner = file_owner
    return folder


@pytest.fixture
def prepare_test(current_tmpdir):
    # touch as_misc
    platforms_path = Path(f"{current_tmpdir.strpath}/platforms_t000.yml")
    jobs_path = Path(f"{current_tmpdir.strpath}/jobs_t000.yml")
    project = "whatever"
    scratch_dir = f"{current_tmpdir.strpath}/scratch"
    Path(f"{scratch_dir}/{project}/{current_tmpdir.owner}").mkdir(parents=True, exist_ok=True)
    Path(f"{scratch_dir}/LOG_t000").mkdir(parents=True, exist_ok=True)
    Path(f"{scratch_dir}/LOG_t000/t000.cmd.out.0").touch()
    Path(f"{scratch_dir}/LOG_t000/t000.cmd.err.0").touch()

    # Add each platform to test
    with platforms_path.open('w') as f:
        f.write(f"""
PLATFORMS:
    pytest-ps:
        type: ps
        host: 127.0.0.1
        user: {current_tmpdir.owner}
        project: {project}
        scratch_dir: {scratch_dir}
        """)
    # add a job of each platform type
    with jobs_path.open('w') as f:
        f.write("""
JOBS:
    base:
        SCRIPT: |
            echo "Hello World"
            echo sleep 5
        QUEUE: hpc
        PLATFORM: pytest-ps
        RUNNING: once
        wallclock: 00:01
EXPERIMENT:
    # List of start dates
    DATELIST: '20000101'
    # List of members.
    MEMBERS: fc0
    # Unit of the chunk size. Can be hour, day, month, or year.
    CHUNKSIZEUNIT: month
    # Size of each chunk.
    CHUNKSIZE: '4'
    # Number of chunks of the experiment.
    NUMCHUNKS: '2'
    CHUNKINI: ''
    # Calendar used for the experiment. Can be standard or noleap.
    CALENDAR: standard
  """)
    return current_tmpdir


@pytest.fixture
def local(prepare_test):
    # Init Local platform
    from autosubmit.platforms.locplatform import LocalPlatform
    config = {
        'LOCAL_ROOT_DIR': f"{prepare_test}/scratch",
        'LOCAL_TMP_DIR': f"{prepare_test}/scratch",
    }
    local = LocalPlatform(expid='t000', name='local', config=config)
    return local


def _identity_value(value=None):
    """A type of identity function; returns a function that returns ``value``."""
    return lambda *ignore_args, **ignore_kwargs: value


@pytest.fixture
def as_db_sqlite(monkeypatch: pytest.MonkeyPatch, tmp_path: "LocalPath") -> Type[BasicConfig]:
    """Overwrites the BasicConfig to use SQLite database for testing.
    Args:
        monkeypatch: Monkey Patcher.
        tmp_path: Temporary path fixture.
    Returns:
        BasicConfig class.
    """
    monkeypatch.setattr(BasicConfig, "read", _identity_value())
    monkeypatch.setattr(BasicConfig, "DATABASE_BACKEND", "sqlite")
    monkeypatch.setattr(BasicConfig, "DB_PATH", str(tmp_path / "autosubmit.db"))

    return BasicConfig


@pytest.fixture(scope="function")
def create_jobs(
        mocker,
        request
) -> list[Job]:
    """
    :return: Jobs with random attributes and retrials.
    """

    def _create_jobs(
            mock,
            num_jobs,
            max_num_retrials_per_job
    ) -> List[Job]:
        jobs = []
        seed(time())
        submit_time = datetime(2023, 1, 1, 10, 0, 0)
        start_time = datetime(2023, 1, 1, 10, 30, 0)
        end_time = datetime(2023, 1, 1, 11, 0, 0)
        completed_retrial = [submit_time, start_time, end_time, "COMPLETED"]
        partial_retrials = [
            [submit_time, start_time, end_time, ""],
            [submit_time, start_time, ""],
            [submit_time, ""],
            [""]
        ]
        job_statuses = Status.LOGICAL_ORDER
        for i in range(num_jobs):
            status = job_statuses[i % len(job_statuses)]  # random status
            job_aux = Job(
                name="example_name_" + str(i),
                job_id="example_id_" + str(i),
                status=status,
                priority=i
            )

            # Custom values for job attributes
            job_aux.processors = str(i)
            job_aux.wallclock = '00:05'
            job_aux.section = "example_section_" + str(i)
            job_aux.member = "example_member_" + str(i)
            job_aux.chunk = "example_chunk_" + str(i)
            job_aux.processors_per_node = str(i)
            job_aux.tasks = str(i)
            job_aux.nodes = str(i)
            job_aux.exclusive = "example_exclusive_" + str(i)

            num_retrials = randint(1, max_num_retrials_per_job)  # random number of retrials, grater than 0
            retrials = []

            for j in range(num_retrials):
                if j < num_retrials - 1:
                    retrial = completed_retrial
                else:
                    if job_aux.status == "COMPLETED":
                        retrial = completed_retrial
                    else:
                        retrial = choice(partial_retrials)
                        if len(retrial) == 1:
                            retrial[0] = job_aux.status
                        elif len(retrial) == 2:
                            retrial[1] = job_aux.status
                        elif len(retrial) == 3:
                            retrial[2] = job_aux.status
                        else:
                            retrial[3] = job_aux.status
                retrials.append(retrial)
            mock.patch("autosubmit.job.job.Job.get_last_retrials", return_value=retrials)
            jobs.append(job_aux)

        return jobs

    return _create_jobs(mocker, request.param[0], request.param[1])
