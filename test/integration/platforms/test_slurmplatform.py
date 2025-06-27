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
@pytest.mark.parametrize('experiment_data', [
    {
        'JOBS': {
            'SIM': {
                'PLATFORM': _PLATFORM_NAME,
                'RUNNING': 'once',
                'SCRIPT': 'echo "This is job ${SLURM_JOB_ID} EOM"'
            },
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
            },
        },
    },
    {
        'JOBS': {
            'SIM': {
                'PLATFORM': _PLATFORM_NAME,
                'RUNNING': 'chunk',
                'SCRIPT': 'sleep 1'
            },
            'SIM_2': {
                'PLATFORM': _PLATFORM_NAME,
                'RUNNING': 'chunk',
                'SCRIPT': 'sleep 1',
                'DEPENDENCIES': 'SIM'
            },
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
            },
        },
    },
], ids=[
    'Simple Workflow',
    'Dependency Workflow',
])
def test_run_simple_workflow_slurm(autosubmit_exp: AutosubmitExperimentFixture, experiment_data):
    """Runs a simple Bash script using Slurm."""
    exp = autosubmit_exp(_EXPID, experiment_data=experiment_data)
    _create_slurm_platform(exp.as_conf)

    exp.autosubmit._check_ownership_and_set_last_command(exp.as_conf, exp.expid, 'run')
    assert 0 == exp.autosubmit.run_experiment(_EXPID)


@pytest.mark.slurm
@pytest.mark.parametrize('experiment_data', [
    {
        'JOBS': {
            'SIM': {
                'DEPENDENCIES': {
                    'SIM - 1': {}
                },
                'SCRIPT': 'sleep 1',
                'WALLCLOCK': '00:10',
                'RUNNING': 'chunk',
                'CHECK': 'on_submission',
            },
            'POST': {
                'DEPENDENCIES': {
                    'SIM'
                },
                'SCRIPT': 'sleep 1',
                'WALLCLOCK': '00:05',
                'RUNNING': 'chunk',
                'CHECK': 'on_submission',
            },
            'TA': {
                'DEPENDENCIES': {
                    'SIM',
                    'POST',
                },
                'SCRIPT': 'sleep 1',
                'WALLCLOCK': '00:05',
                'RUNNING': 'once',
                'CHECK': 'on_submission',
            },
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
            },
        },
        'WRAPPERS': {
            'WRAPPER': {
                'TYPE': 'vertical',
                'JOBS_IN_WRAPPER': 'SIM',
                'RETRIALS': 0
            }
        },
    },
    # {
    #     'JOBS': {
    #         'SIMV': {
    #             'DEPENDENCIES': {
    #                 'SIMV - 1': {}
    #             },
    #             'SCRIPT': 'sleep 1',
    #             'WALLCLOCK': '00:10',
    #             'RUNNING': 'chunk',
    #             'CHECK': 'on_submission',
    #             'RETRIALS': 1,
    #         },
    #     },
    #     'PLATFORMS': {
    #         _PLATFORM_NAME: {
    #             'ADD_PROJECT_TO_HOST': False,
    #             'HOST': 'localDocker',
    #             'MAX_WALLCLOCK': '00:03',
    #             'PROJECT': 'group',
    #             'QUEUE': 'gp_debug',
    #             'SCRATCH_DIR': '/tmp/scratch/',
    #             'TEMP_DIR': '',
    #             'TYPE': 'slurm',
    #             'USER': 'root'
    #         },
    #     },
    #     'WRAPPERS': {
    #         'WRAPPERV': {
    #             'TYPE': 'vertical',
    #             'JOBS_IN_WRAPPER': 'SIMV',
    #             'RETRIALS': 0
    #         },
    #     },
    # },
    # {
    #     'JOBS': {
    #         'SIMHV': {
    #             'DEPENDENCIES': {
    #                 'SIMHV - 1': {}
    #             },
    #             'SCRIPT': 'sleep 1',
    #             'WALLCLOCK': '00:10',
    #             'RUNNING': 'chunk',
    #             'CHECK': 'on_submission',
    #             'RETRIALS': 1,
    #         },
    #     },
    #     'PLATFORMS': {
    #         _PLATFORM_NAME: {
    #             'ADD_PROJECT_TO_HOST': False,
    #             'HOST': 'localDocker',
    #             'MAX_WALLCLOCK': '00:03',
    #             'PROJECT': 'group',
    #             'QUEUE': 'gp_debug',
    #             'SCRATCH_DIR': '/tmp/scratch/',
    #             'TEMP_DIR': '',
    #             'TYPE': 'slurm',
    #             'USER': 'root'
    #         },
    #     },
    #     'WRAPPERS': {
    #         'WRAPPERHV': {
    #             'TYPE': 'horizontal-vertical',
    #             'JOBS_IN_WRAPPER': 'SIMHV',
    #             'RETRIALS': 0
    #         },
    #     },
    # },
    # {
    #     'JOBS': {
    #         'SIMVH': {
    #             'DEPENDENCIES': {
    #                 'SIMVH - 1': {},
    #             },
    #             'SCRIPT': 'sleep 1',
    #             'WALLCLOCK': '00:10',
    #             'RUNNING': 'chunk',
    #             'CHECK': 'on_submission',
    #             'RETRIALS': 1,
    #         },
    #     },
    #     'PLATFORMS': {
    #         _PLATFORM_NAME: {
    #             'ADD_PROJECT_TO_HOST': False,
    #             'HOST': 'localDocker',
    #             'MAX_WALLCLOCK': '00:03',
    #             'PROJECT': 'group',
    #             'QUEUE': 'gp_debug',
    #             'SCRATCH_DIR': '/tmp/scratch/',
    #             'TEMP_DIR': '',
    #             'TYPE': 'slurm',
    #             'USER': 'root'
    #         },
    #     },
    #     'WRAPPERS': {
    #         'WRAPPERVH': {
    #             'TYPE': 'vertical-horizontal',
    #             'JOBS_IN_WRAPPER': 'SIMVH',
    #             'RETRIALS': 0
    #         },
    #     },
    # },
    {
        'JOBS': {
            'SIMH': {
                'SCRIPT': 'sleep 1',
                'WALLCLOCK': "00:10",
                'RUNNING': 'chunk',
                'CHECK': 'on_submission',
                'RETRIALS': 1,
            },
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
            },
        },
        'WRAPPERS': {
            'WRAPPERH': {
                'TYPE': 'horizontal',
                'JOBS_IN_WRAPPER': 'SIMH',
                'RETRIALS': 0
            },
        },
    },
], ids=[
    'Vertical Wrapper Workflow',
    # 'Wrapper Vertical',
    # 'Wrapper Horizontal-vertical',
    # 'Wrapper Vertical-horizontal',
    'Wrapper Horizontal',
])
def test_run_all_wrappers_workflow_slurm(autosubmit_exp: AutosubmitExperimentFixture, experiment_data):
    """Runs a simple Bash script using Slurm."""
    exp = autosubmit_exp(_EXPID, experiment_data=experiment_data)
    _create_slurm_platform(exp.as_conf)

    exp.autosubmit._check_ownership_and_set_last_command(exp.as_conf, exp.expid, 'run')
    assert 0 == exp.autosubmit.run_experiment(_EXPID)
