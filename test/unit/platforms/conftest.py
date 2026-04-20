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

import pytest

from autosubmit.platforms.ecplatform import EcPlatform
from autosubmit.platforms.locplatform import LocalPlatform
from autosubmit.platforms.pjmplatform import PJMPlatform
from autosubmit.platforms.psplatform import PsPlatform
from autosubmit.platforms.slurmplatform import SlurmPlatform

"""Fixtures for unit tests."""


@pytest.fixture
def slurm_platform(autosubmit_config, tmp_path):
    """Minimal SlurmPlatform with the directory structure it expects."""
    exp_data = {
        "LOCAL_ROOT_DIR": str(tmp_path),
        "LOCAL_TMP_DIR": "tmp",
        "LOCAL_ASLOG_DIR": "ASLOGS",
        "PLATFORMS": {
            "local": {
                "type": "slurm",
                "host": "localhost",
                "user": "user",
                "project": "project",
                "scratch_dir": str(tmp_path),
            }
        },
    }
    as_conf = autosubmit_config("a000", experiment_data=exp_data)
    aslogs = tmp_path / "a000" / "tmp" / "ASLOGS"
    aslogs.mkdir(parents=True, exist_ok=True)
    (aslogs / "submit_local.sh").touch()
    return SlurmPlatform(expid="a000", name="local", config=as_conf.experiment_data)


@pytest.fixture
def pjm_platform(autosubmit_config, tmp_path):
    """Minimal PJMPlatform."""
    exp_data = {
        "LOCAL_ROOT_DIR": str(tmp_path),
        "LOCAL_TMP_DIR": "tmp",
        "LOCAL_ASLOG_DIR": "ASLOGS",
        "PLATFORMS": {
            "local": {
                "type": "pjm",
                "host": "localhost",
                "user": "user",
                "project": "project",
                "scratch_dir": str(tmp_path),
            }
        },
    }
    as_conf = autosubmit_config("a000", experiment_data=exp_data)
    aslogs = tmp_path / "a000" / "tmp" / "ASLOGS"
    aslogs.mkdir(parents=True, exist_ok=True)
    (aslogs / "submit_local.sh").touch()
    return PJMPlatform(expid="a000", name="local", config=as_conf.experiment_data)


@pytest.fixture
def ec_platform(tmp_path):
    """Minimal EcPlatform (slurm scheduler variant)."""
    config = {"LOCAL_ROOT_DIR": str(tmp_path), "LOCAL_TMP_DIR": "tmp"}
    return EcPlatform(expid="a000", name="local", config=config, scheduler="slurm")


@pytest.fixture
def ps_platform(tmp_path):
    """Minimal PsPlatform."""
    config = {
        "LOCAL_ROOT_DIR": str(tmp_path),
        "LOCAL_TMP_DIR": "tmp",
        "PLATFORMS": {
            "local": {
                "type": "ps",
                "host": "127.0.0.1",
                "user": "user",
                "project": "project",
                "scratch_dir": str(tmp_path),
            }
        },
    }
    return PsPlatform(expid="a000", name="local", config=config)


@pytest.fixture
def local_platform(tmp_path):
    """Minimal LocalPlatform."""
    config = {"LOCAL_ROOT_DIR": str(tmp_path), "LOCAL_TMP_DIR": "tmp"}
    return LocalPlatform(expid="a000", name="local", config=config)
