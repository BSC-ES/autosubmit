# Fixtures available to multiple test files must be created in this file.

import pytest
from dataclasses import dataclass
from pathlib import Path
from shutil import rmtree
from tempfile import TemporaryDirectory
from typing import Callable

from autosubmit.autosubmit import Autosubmit
from autosubmit.platforms.slurmplatform import SlurmPlatform, ParamikoPlatform
from autosubmitconfigparser.config.basicconfig import BasicConfig


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


@pytest.fixture(
    scope='function'
)
def autosubmit_exp(autosubmit: Autosubmit, request: pytest.FixtureRequest) -> Callable:
    """Create an instance of ``Autosubmit`` with an experiment."""

    original_root_dir = BasicConfig.LOCAL_ROOT_DIR
    tmp_dir = TemporaryDirectory()
    tmp_path = Path(tmp_dir.name)

    def _create_autosubmit_exp(expid: str):
        # directories used when searching for logs to cat
        root_dir = tmp_path
        BasicConfig.LOCAL_ROOT_DIR = str(root_dir)
        exp_path = root_dir / expid
        tmp_dir = exp_path / BasicConfig.LOCAL_TMP_DIR
        aslogs_dir = tmp_dir / BasicConfig.LOCAL_ASLOG_DIR
        status_dir = exp_path / 'status'
        aslogs_dir.mkdir(parents=True)
        status_dir.mkdir()

        platform_config = {
            "LOCAL_ROOT_DIR": BasicConfig.LOCAL_ROOT_DIR,
            "LOCAL_TMP_DIR": str(tmp_dir),
            "LOCAL_ASLOG_DIR": str(aslogs_dir)
        }
        platform = SlurmPlatform(expid=expid, name='slurm_platform', config=platform_config)
        platform.job_status = {
            'COMPLETED': [],
            'RUNNING': [],
            'QUEUING': [],
            'FAILED': []
        }
        submit_platform_script = aslogs_dir / 'submit_local.sh'
        submit_platform_script.touch(exist_ok=True)

        return AutosubmitExperiment(
            expid=expid,
            autosubmit=autosubmit,
            exp_path=exp_path,
            tmp_dir=tmp_dir,
            aslogs_dir=aslogs_dir,
            status_dir=status_dir,
            platform=platform
        )

    def finalizer():
        BasicConfig.LOCAL_ROOT_DIR = original_root_dir
        rmtree(tmp_path)

    request.addfinalizer(finalizer)

    return _create_autosubmit_exp


@pytest.fixture(scope='module')
def autosubmit():
    """Create an instance of ``Autosubmit``.

    Useful when you need ``Autosubmit`` but do not need any experiments."""
    autosubmit = Autosubmit()
    return autosubmit