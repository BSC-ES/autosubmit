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

"""Integration tests for the Slurm platform."""

import pytest
from autosubmitconfigparser.config.configcommon import AutosubmitConfig

from autosubmit.platforms.slurmplatform import SlurmPlatform
from test.conftest import AutosubmitExperimentFixture

_EXPID = "t000"
_PLATFORM_NAME = 'TEST_SLURM'


def _create_slurm_platform(as_conf: AutosubmitConfig):
    return SlurmPlatform(_EXPID, _PLATFORM_NAME, config=as_conf.experiment_data, auth_password=None)


@pytest.mark.slurm
def test_create_platform_slurm(autosubmit_exp):
    """Test the Slurm platform object creation."""
    exp = autosubmit_exp(_EXPID, experiment_data={
        'JOBS': {
            'SIM': {
                'PLATFORM': _PLATFORM_NAME,
                'RUNNING': 'once',
                'SCRIPT': 'echo "This is job ${SLURM_JOB_ID} EOM"'
            }
        },
        'PLATFORMS': {
            _PLATFORM_NAME: {
                'ADD_PROJECT_TO_HOST': False,
                'HOST': 'localDocker',
                'MAX_WALLCLOCK': '00:03',
                'PROJECT': 'group',
                'QUEUE': 'gp_debug',
                'SCRATCH_DIR': '/tmp/scratch/',
                'TEMP_DIR': '',
                'TYPE': 'slurm',
                'USER': 'root'
            }
        }
    })
    platform = _create_slurm_platform(exp.as_conf)
    assert platform.name == _PLATFORM_NAME
    # TODO: add more assertion statements...


@pytest.mark.slurm
def test_run_simple_workflow_slurm(autosubmit_exp: AutosubmitExperimentFixture):
    """Runs a simple Bash script using Slurm."""
    exp = autosubmit_exp(_EXPID, experiment_data={
        'JOBS': {
            'SIM': {
                'PLATFORM': _PLATFORM_NAME,
                'RUNNING': 'once',
                'SCRIPT': 'echo "This is job ${SLURM_JOB_ID} EOM"'
            }
        },
        'PLATFORMS': {
            _PLATFORM_NAME: {
                'ADD_PROJECT_TO_HOST': False,
                'HOST': 'localDocker',
                'MAX_WALLCLOCK': '00:03',
                'PROJECT': 'group',
                'QUEUE': 'gp_debug',
                'SCRATCH_DIR': '/tmp/scratch/',
                'TEMP_DIR': '',
                'TYPE': 'slurm',
                'USER': 'root'
            }
        }
    })
    _create_slurm_platform(exp.as_conf)

    exp.autosubmit._check_ownership_and_set_last_command(exp.as_conf, exp.expid, 'run')
    assert 0 == exp.autosubmit.run_experiment(_EXPID)


@pytest.mark.slurm
def test_run_dependency_workflow_slurm(autosubmit_exp: AutosubmitExperimentFixture):
    """Runs a simple Bash script using Slurm."""
    exp = autosubmit_exp(_EXPID, experiment_data={
        'JOBS': {
            'SIM': {
                'PLATFORM': _PLATFORM_NAME,
                'RUNNING': 'chunk',
                'SCRIPT': 'sleep 2'
            },
            'SIM_2': {
                'PLATFORM': _PLATFORM_NAME,
                'RUNNING': 'chunk',
                'SCRIPT': 'sleep 2',
                'DEPENDENCIES': 'SIM'
            }
        },
        'PLATFORMS': {
            _PLATFORM_NAME: {
                'ADD_PROJECT_TO_HOST': False,
                'HOST': 'localDocker',
                'MAX_WALLCLOCK': '00:03',
                'PROJECT': 'group',
                'QUEUE': 'gp_debug',
                'SCRATCH_DIR': '/tmp/scratch/',
                'TEMP_DIR': '',
                'TYPE': 'slurm',
                'USER': 'root'
            }
        }
    })
    _create_slurm_platform(exp.as_conf)

    exp.autosubmit._check_ownership_and_set_last_command(exp.as_conf, exp.expid, 'run')
    assert 0 == exp.autosubmit.run_experiment(_EXPID)
