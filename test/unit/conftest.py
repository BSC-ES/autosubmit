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

"""Fixtures for unit tests."""

from datetime import datetime
from importlib.metadata import version, PackageNotFoundError
from pathlib import Path
from random import seed, randint, choice
from time import time
from typing import Any, Optional, Protocol

import pytest

from autosubmit.autosubmit import Autosubmit
from autosubmit.config.basicconfig import BasicConfig
from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status


# Copied from the autosubmit config parser, that I believe is a revised one from the create_as_conf
class AutosubmitConfigFactory(Protocol):

    def __call__(
            self,
            expid: str,
            experiment_data: Optional[dict] = None,
            include_basic_config: bool = True,
            *args: Any,
            **kwargs: Any
    ) -> AutosubmitConfig: ...


@pytest.fixture(scope="function")
def autosubmit_config(
        request: pytest.FixtureRequest,
        tmp_path: Path,
        autosubmit: Autosubmit
) -> AutosubmitConfigFactory:
    """Return a factory for ``AutosubmitConfig`` objects.

    Abstracts the necessary mocking in ``AutosubmitConfig`` and related objects,
    so that if we need to modify these, they can all be done in a single place.

    It is able to create any configuration, based on the ``request`` parameters.

    When the function (see ``scope``) finishes, the object and paths created are
    cleaned (see ``finalizer`` below).
    """

    def _create_autosubmit_config(
            expid: str,
            experiment_data: dict = None,
            include_basic_config: bool = True,
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
        :param include_basic_config: Whether to include ``BasicConfig`` attributes or not (for some platforms).
        Enabled by default.
        """
        if not expid:
            raise ValueError("No value provided for expid")

        if experiment_data is None:
            experiment_data = {}

        # FIXME: (BRUNO) Do we really need postgres here in the unit tests conftest?
        is_postgres = hasattr(BasicConfig, 'DATABASE_BACKEND') and BasicConfig.DATABASE_BACKEND == 'postgres'
        if is_postgres or not Path(BasicConfig.DB_PATH).exists():
            autosubmit.install()

        exp_path = Path(BasicConfig.LOCAL_ROOT_DIR, expid)
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
        is_postgres = hasattr(BasicConfig, 'DATABASE_BACKEND') and BasicConfig.DATABASE_BACKEND == 'postgres'
        db_path = Path(BasicConfig.DB_PATH)
        if not is_postgres:
            db_path.touch()
        # <TEMP>/global_logs
        global_logs = Path(BasicConfig.GLOBAL_LOG_DIR)
        global_logs.mkdir(parents=True, exist_ok=True)
        job_data_dir = Path(BasicConfig.JOBDATA_DIR)
        job_data_dir.mkdir(parents=True, exist_ok=True)

        config = AutosubmitConfig(
            expid=expid,
            basic_config=BasicConfig
        )

        config.experiment_data = {**config.experiment_data, **experiment_data}
        # Populate the configuration object's ``experiment_data`` dictionary with the values
        # in ``BasicConfig``. For some reason, some platforms use variables like ``LOCAL_ROOT_DIR``
        # from the configuration object, instead of using ``BasicConfig``.
        if include_basic_config:

            for k, v in {k: v for k, v in BasicConfig.__dict__.items() if not k.startswith('__')}.items():
                config.experiment_data[k] = v

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

        config.experiment_data['CONFIG']['SAFETYSLEEPTIME'] = 0
        # TODO: one test failed while moving things from unit to integration, but this shouldn't be
        #       needed, especially if the disk has the valid value?
        config.experiment_data['DEFAULT']['EXPID'] = expid

        if 'HPCARCH' not in config.experiment_data['DEFAULT']:
            config.experiment_data['DEFAULT']['HPCARCH'] = 'LOCAL'

        for arg, value in kwargs.items():
            setattr(config, arg, value)
        config.current_loaded_files[str(conf_dir / 'dummy-so-it-doesnt-force-reload.yml')] = time()
        return config

    return _create_autosubmit_config


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
    ) -> list[Job]:
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
