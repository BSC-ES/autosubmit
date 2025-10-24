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

from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.platforms.paramiko_submitter import ParamikoSubmitter
from autosubmit.history.experiment_history import ExperimentHistory
from autosubmit.log.utils import is_gzip_file, is_xz_file
from autosubmit.platforms.slurmplatform import SlurmPlatform

if TYPE_CHECKING:
    from test.integration.conftest import AutosubmitExperimentFixture
    from testcontainers.core.container import DockerContainer

_EXPID = 't001'

_PLATFORM_NAME = 'TEST_SLURM'


def _create_slurm_platform(expid: str, as_conf: AutosubmitConfig):
    return SlurmPlatform(expid, _PLATFORM_NAME, config=as_conf.experiment_data)


def test_create_platform_slurm(autosubmit_exp):
    """Test the Slurm platform object creation."""
    exp = autosubmit_exp(_EXPID, experiment_data={
        'JOBS': {
            'SIM': {
                'PLATFORM': _PLATFORM_NAME,
                'RUNNING': 'once',
                'SCRIPT': 'echo "This is job ${SLURM_JOB_ID} EOM"',
            }
        },
        'PLATFORMS': {
            _PLATFORM_NAME: {
                'ADD_PROJECT_TO_HOST': False,
                'HOST': '127.0.0.1',
                'MAX_WALLCLOCK': '00:03',
                'PROJECT': 'group',
                'QUEUE': 'gp_debug',
                'SCRATCH_DIR': '/tmp/scratch/',
                'TEMP_DIR': '',
                'TYPE': 'slurm',
                'USER': 'root',
            }
        }
    })
    platform = _create_slurm_platform(exp.expid, exp.as_conf)
    assert platform.name == _PLATFORM_NAME
    # TODO: add more assertion statements...


@pytest.mark.slurm
@pytest.mark.parametrize('experiment_data', [
    {
        'JOBS': {
            'SIM': {
                'PLATFORM': _PLATFORM_NAME,
                'RUNNING': 'once',
                'SCRIPT': 'echo "This is job ${SLURM_JOB_ID} EOM"',
            },
        },
        'PLATFORMS': {
            _PLATFORM_NAME: {
                'ADD_PROJECT_TO_HOST': False,
                'HOST': '127.0.0.1',
                'MAX_WALLCLOCK': '00:03',
                'PROJECT': 'group',
                'QUEUE': 'gp_debug',
                'SCRATCH_DIR': '/tmp/scratch/',
                'TEMP_DIR': '',
                'TYPE': 'slurm',
                'USER': 'root',
            },
        },
    },
    {
        'JOBS': {
            'SIM': {
                'PLATFORM': _PLATFORM_NAME,
                'RUNNING': 'chunk',
                'SCRIPT': 'sleep 0',
            },
            'SIM_2': {
                'PLATFORM': _PLATFORM_NAME,
                'RUNNING': 'chunk',
                'SCRIPT': 'sleep 0',
                'DEPENDENCIES': 'SIM',
            },
        },
        'PLATFORMS': {
            _PLATFORM_NAME: {
                'ADD_PROJECT_TO_HOST': False,
                'HOST': '127.0.0.1',
                'MAX_WALLCLOCK': '00:03',
                'PROJECT': 'group',
                'QUEUE': 'gp_debug',
                'SCRATCH_DIR': '/tmp/scratch/',
                'TEMP_DIR': '',
                'TYPE': 'slurm',
                'USER': 'root',
            },
        },
    },
], ids=[
    'Simple Workflow',
    'Dependency Workflow',
])
def test_run_simple_workflow_slurm(autosubmit_exp: 'AutosubmitExperimentFixture', experiment_data: dict,
                                   slurm_server: 'DockerContainer'):
    """Runs a simple Bash script using Slurm."""
    exp = autosubmit_exp(_EXPID, experiment_data=experiment_data)
    _create_slurm_platform(exp.expid, exp.as_conf)

    exp.autosubmit._check_ownership_and_set_last_command(exp.as_conf, exp.expid, 'run')
    assert 0 == exp.autosubmit.run_experiment(exp.expid)


@pytest.mark.parametrize('experiment_data', [
    # Vertical Wrapper Workflow
    {
        'DEFAULT': {
            'EXPID': _EXPID,
            'HPCARCH': _PLATFORM_NAME,
        },
        'JOBS': {
            'SIM': {
                'DEPENDENCIES': {
                    'SIM-1': {}
                },
                'SCRIPT': 'sleep 0',
                'WALLCLOCK': '00:03',
                'RUNNING': 'chunk',
                'CHECK': 'on_submission',
                'PLATFORM': _PLATFORM_NAME,
            },
            'POST': {
                'DEPENDENCIES': {
                    'SIM',
                },
                'SCRIPT': 'sleep 0',
                'WALLCLOCK': '00:03',
                'RUNNING': 'chunk',
                'CHECK': 'on_submission',
                'PLATFORM': _PLATFORM_NAME,
            },
            'TA': {
                'DEPENDENCIES': {
                    'SIM',
                    'POST',
                },
                'SCRIPT': 'sleep 0',
                'WALLCLOCK': '00:03',
                'RUNNING': 'once',
                'CHECK': 'on_submission',
                'PLATFORM': _PLATFORM_NAME,
            },
        },
        'PLATFORMS': {
            _PLATFORM_NAME: {
                'ADD_PROJECT_TO_HOST': False,
                'HOST': '127.0.0.1',
                'MAX_WALLCLOCK': '02:00',
                'PROJECT': 'group',
                'QUEUE': 'gp_debug',
                'SCRATCH_DIR': '/tmp/scratch/',
                'TEMP_DIR': '',
                'TYPE': 'slurm',
                'USER': 'root',
                'MAX_PROCESSORS': 10,
                'PROCESSORS_PER_NODE': 10,
            },
        },
        'WRAPPERS': {
            'WRAPPER': {
                'TYPE': 'vertical',
                'JOBS_IN_WRAPPER': 'SIM',
                'RETRIALS': 0,
            }
        },
    },
    # Wrapper Vertical
    {
        'DEFAULT': {
            'EXPID': _EXPID,
            'HPCARCH': _PLATFORM_NAME,
        },
        'JOBS': {
            'SIM_V': {
                'DEPENDENCIES': {
                    'SIM_V-1': {}
                },
                'SCRIPT': 'sleep 0',
                'WALLCLOCK': '00:03',
                'RUNNING': 'chunk',
                'CHECK': 'on_submission',
                'RETRIALS': 1,
                'PLATFORM': _PLATFORM_NAME,
            },
        },
        'PLATFORMS': {
            _PLATFORM_NAME: {
                'ADD_PROJECT_TO_HOST': False,
                'HOST': '127.0.0.1',
                'MAX_WALLCLOCK': '00:03',
                'PROJECT': 'group',
                'QUEUE': 'gp_debug',
                'SCRATCH_DIR': '/tmp/scratch/',
                'TEMP_DIR': '',
                'TYPE': 'slurm',
                'USER': 'root',
                'MAX_PROCESSORS': 10,
                'PROCESSORS_PER_NODE': 10,
            },
        },
        'WRAPPERS': {
            'WRAPPER_V': {
                'TYPE': 'vertical',
                'JOBS_IN_WRAPPER': 'SIM_V',
                'RETRIALS': 0,
            },
        },
    },
    # Wrapper Horizontal
    {
        'DEFAULT': {
            'EXPID': _EXPID,
            'HPCARCH': _PLATFORM_NAME,
        },
        'JOBS': {
            'SIM_H': {
                'DEPENDENCIES': {
                    'SIM_H-1': {}
                },
                'SCRIPT': 'sleep 0',
                'WALLCLOCK': '00:03',
                'RUNNING': 'chunk',
                'CHECK': 'on_submission',
                'RETRIALS': 1,
                'PLATFORM': _PLATFORM_NAME,
            },
        },
        'PLATFORMS': {
            _PLATFORM_NAME: {
                'ADD_PROJECT_TO_HOST': False,
                'HOST': '127.0.0.1',
                'MAX_WALLCLOCK': '00:03',
                'PROJECT': 'group',
                'QUEUE': 'gp_debug',
                'SCRATCH_DIR': '/tmp/scratch/',
                'TEMP_DIR': '',
                'TYPE': 'slurm',
                'USER': 'root',
                'MAX_PROCESSORS': 10,
                'PROCESSORS_PER_NODE': 10,
            },
        },
        'WRAPPERS': {
            'WRAPPER_H': {
                'TYPE': 'horizontal',
                'JOBS_IN_WRAPPER': 'SIM_H',
                'RETRIALS': 0,
            },
        },
    },
    # Wrapper Horizontal-vertical
    {
        'DEFAULT': {
            'EXPID': _EXPID,
            'HPCARCH': _PLATFORM_NAME,
        },
        'JOBS': {
            'SIM_H_V': {
                'DEPENDENCIES': {
                    'SIM_H_V-1': {}
                },
                'SCRIPT': 'sleep 0',
                'WALLCLOCK': '00:03',
                'RUNNING': 'chunk',
                'CHECK': 'on_submission',
                'RETRIALS': 1,
                'PLATFORM': _PLATFORM_NAME,
            },
        },
        'PLATFORMS': {
            _PLATFORM_NAME: {
                'ADD_PROJECT_TO_HOST': False,
                'HOST': '127.0.0.1',
                'MAX_WALLCLOCK': '00:03',
                'PROJECT': 'group',
                'QUEUE': 'gp_debug',
                'SCRATCH_DIR': '/tmp/scratch/',
                'TEMP_DIR': '',
                'TYPE': 'slurm',
                'USER': 'root',
                'MAX_PROCESSORS': 10,
                'PROCESSORS_PER_NODE': 10,
            },
        },
        'WRAPPERS': {
            'WRAPPER_H_V': {
                'TYPE': 'horizontal-vertical',
                'JOBS_IN_WRAPPER': 'SIM_H_V',
                'RETRIALS': 0,
            },
        },
    },
    # Wrapper Vertical-horizontal
    {
        'DEFAULT': {
            'EXPID': _EXPID,
            'HPCARCH': _PLATFORM_NAME,
        },
        'JOBS': {
            'SIM_V_H': {
                'DEPENDENCIES': {
                    'SIM_V_H-1': {},
                },
                'SCRIPT': 'sleep 0',
                'WALLCLOCK': '00:03',
                'RUNNING': 'chunk',
                'CHECK': 'on_submission',
                'RETRIALS': 1,
                'PLATFORM': _PLATFORM_NAME,
            },
        },
        'PLATFORMS': {
            _PLATFORM_NAME: {
                'ADD_PROJECT_TO_HOST': False,
                'HOST': '127.0.0.1',
                'MAX_WALLCLOCK': '00:03',
                'PROJECT': 'group',
                'QUEUE': 'gp_debug',
                'SCRATCH_DIR': '/tmp/scratch/',
                'TEMP_DIR': '',
                'TYPE': 'slurm',
                'USER': 'root',
                'MAX_PROCESSORS': 10,
                'PROCESSORS_PER_NODE': 10,
            },
        },
        'WRAPPERS': {
            'WRAPPER_V_H': {
                'TYPE': 'vertical-horizontal',
                'JOBS_IN_WRAPPER': 'SIM_V_H',
                'RETRIALS': 0,
            },
        },
    },
], ids=[
    'Vertical Wrapper Workflow',
    'Wrapper Vertical',
    'Wrapper Horizontal',
    'Wrapper Horizontal-vertical',
    'Wrapper Vertical-horizontal',
])
@pytest.mark.docker
@pytest.mark.slurm
def test_run_all_wrappers_workflow_slurm(experiment_data: dict, autosubmit_exp: 'AutosubmitExperimentFixture',
                                         slurm_server: 'DockerContainer'):
    """Runs a simple Bash script using Slurm."""
    exp = autosubmit_exp(_EXPID, experiment_data=experiment_data, wrapper=True)
    _create_slurm_platform(exp.expid, exp.as_conf)

    exp.as_conf.experiment_data = {
        'EXPERIMENT': {
            'DATELIST': '20000101',
            'MEMBERS': 'fc0 fc1',
            'CHUNKSIZEUNIT': 'day',
            'CHUNKSIZE': 1,
            'NUMCHUNKS': '2',
            'CHUNKINI': '',
            'CALENDAR': 'standard',
        }
    }

    exp.autosubmit._check_ownership_and_set_last_command(exp.as_conf, exp.expid, 'run')
    assert 0 == exp.autosubmit.run_experiment(exp.expid)


@pytest.mark.parametrize('experiment_data', [
    {
        'JOBS': {
            'LOCAL_SETUP': {
                'SCRIPT': 'sleep 0',
                'RUNNING': 'once',
                'NOTIFY_ON': 'COMPLETED',
                'PLATFORM': _PLATFORM_NAME,
            },
            'LOCAL_SEND_SOURCE': {
                'SCRIPT': 'sleep 0',
                'PLATFORM': _PLATFORM_NAME,
                'DEPENDENCIES': 'LOCAL_SETUP',
                'RUNNING': 'once',
                'NOTIFY_ON': 'FAILED',
            },
            'LOCAL_SEND_STATIC': {
                'SCRIPT': 'sleep 0',
                'PLATFORM': _PLATFORM_NAME,
                'DEPENDENCIES': 'LOCAL_SETUP',
                'RUNNING': 'once',
                'NOTIFY_ON': 'FAILED',
            },
            'REMOTE_COMPILE': {
                'SCRIPT': 'sleep 0',
                'DEPENDENCIES': 'LOCAL_SEND_SOURCE',
                'RUNNING': 'once',
                'PROCESSORS': '1',
                'WALLCLOCK': '00:01',
                'NOTIFY_ON': 'COMPLETED',
            },
            'SIM': {
                'SCRIPT': 'sleep 0',
                'DEPENDENCIES': {
                    'LOCAL_SEND_STATIC': {},
                    'REMOTE_COMPILE': {},
                    'SIM-1': {},
                    'DA-1': {},
                },
                'RUNNING': 'once',
                'PROCESSORS': '1',
                'WALLCLOCK': '00:01',
                'NOTIFY_ON': 'FAILED',
                'PLATFORM': _PLATFORM_NAME,
            },
            'LOCAL_SEND_INITIAL_DA': {
                'SCRIPT': 'sleep 0',
                'PLATFORM': _PLATFORM_NAME,
                'DEPENDENCIES': 'LOCAL_SETUP LOCAL_SEND_INITIAL_DA-1',
                'RUNNING': 'chunk',
                'SYNCHRONIZE': 'member',
                'DELAY': '0',
            },
            'COMPILE_DA': {
                'SCRIPT': 'sleep 0',
                'DEPENDENCIES': 'LOCAL_SEND_SOURCE',
                'RUNNING': 'once',
                'WALLCLOCK': '00:01',
                'NOTIFY_ON': 'FAILED',
            },
            'DA': {
                'SCRIPT': 'sleep 0',
                'DEPENDENCIES': {
                    'SIM': {},
                    'LOCAL_SEND_INITIAL_DA': {
                        'CHUNKS_TO': 'all',
                        'DATES_TO': 'all',
                        'MEMBERS_TO': 'all',
                    },
                    'COMPILE_DA': {},
                    'DA': {
                        'DATES_FROM': {
                            '20120201': {
                                'CHUNKS_FROM': {
                                    '1': {
                                        'DATES_TO': '20120101',
                                    },
                                },
                            },
                        },
                    },
                },
                'RUNNING': 'chunk',
                'SYNCHRONIZE': 'member',
                'DELAY': '0',
                'WALLCLOCK': '00:01',
                'PROCESSORS': '1',
                'NOTIFY_ON': 'FAILED',
                'PLATFORM': _PLATFORM_NAME,
            },
        },
        'PLATFORMS': {
            _PLATFORM_NAME: {
                'ADD_PROJECT_TO_HOST': False,
                'HOST': '127.0.0.1',
                'MAX_WALLCLOCK': '00:03',
                'PROJECT': 'group',
                'QUEUE': 'gp_debug',
                'SCRATCH_DIR': '/tmp/scratch/',
                'TEMP_DIR': '',
                'TYPE': 'slurm',
                'USER': 'root',
                'MAX_PROCESSORS': 10,
                'PROCESSORS_PER_NODE': 10,
            },
        },
        'WRAPPERS': {
            'WRAPPER_SIMDA': {
                'TYPE': 'vertical-horizontal',
                'JOBS_IN_WRAPPER': 'SIM DA',
                'RETRIALS': '0',
            }
        },
    },
    {
        'JOBS': {
            'LOCAL_SETUP': {
                'SCRIPT': 'sleep 0',
                'RUNNING': 'once',
                'WALLCLOCK': '00:01',
                'NOTIFY_ON': 'COMPLETED',
                'PLATFORM': _PLATFORM_NAME,
            },
            'LOCAL_SEND_SOURCE': {
                'SCRIPT': 'sleep 0',
                'DEPENDENCIES': 'LOCAL_SETUP',
                'RUNNING': 'once',
                'WALLCLOCK': '00:01',
                'NOTIFY_ON': 'FAILED',
                'PLATFORM': _PLATFORM_NAME,
            },
            'LOCAL_SEND_STATIC': {
                'SCRIPT': 'sleep 0',
                'DEPENDENCIES': 'LOCAL_SETUP',
                'RUNNING': 'once',
                'WALLCLOCK': '00:01',
                'NOTIFY_ON': 'FAILED',
                'PLATFORM': _PLATFORM_NAME,
            },
            'REMOTE_COMPILE': {
                'SCRIPT': 'sleep 0',
                'DEPENDENCIES': 'LOCAL_SEND_SOURCE',
                'RUNNING': 'once',
                'PROCESSORS': '1',
                'WALLCLOCK': '00:01',
                'NOTIFY_ON': 'COMPLETED',
            },
            'SIM': {
                'SCRIPT': 'sleep 0',
                'DEPENDENCIES': {
                    'LOCAL_SEND_STATIC': {},
                    'REMOTE_COMPILE': {},
                    'SIM-1': {},
                    'DA-1': {},
                },
                'RUNNING': 'once',
                'PROCESSORS': '1',
                'WALLCLOCK': '00:01',
                'NOTIFY_ON': 'FAILED',
                'PLATFORM': _PLATFORM_NAME,
            },
            'LOCAL_SEND_INITIAL_DA': {
                'SCRIPT': 'sleep 0',
                'DEPENDENCIES': 'LOCAL_SETUP LOCAL_SEND_INITIAL_DA-1',
                'RUNNING': 'chunk',
                'WALLCLOCK': '00:01',
                'SYNCHRONIZE': 'member',
                'DELAY': '0',
                'PLATFORM': _PLATFORM_NAME,
            },
            'COMPILE_DA': {
                'SCRIPT': 'sleep 0',
                'DEPENDENCIES': 'LOCAL_SEND_SOURCE',
                'RUNNING': 'once',
                'WALLCLOCK': '00:01',
                'NOTIFY_ON': 'FAILED',
            },
            'DA': {
                'SCRIPT': 'sleep 0',
                'DEPENDENCIES': {
                    'SIM': {},
                    'LOCAL_SEND_INITIAL_DA': {
                        'CHUNKS_TO': 'all',
                        'DATES_TO': 'all',
                        'MEMBERS_TO': 'all',
                    },
                    'COMPILE_DA': {},
                    'DA': {
                        'DATES_FROM': {
                            '20120201': {
                                'CHUNKS_FROM': {
                                    '1': {
                                        'DATES_TO': '20120101',
                                        'CHUNKS_TO': '1',
                                    },
                                },
                            },
                        },
                    },
                },
                'RUNNING': 'chunk',
                'SYNCHRONIZE': 'member',
                'DELAY': '0',
                'WALLCLOCK': '00:01',
                'PROCESSORS': '1',
                'NOTIFY_ON': 'FAILED',
                'PLATFORM': _PLATFORM_NAME,
            },
        },
        'PLATFORMS': {
            _PLATFORM_NAME: {
                'ADD_PROJECT_TO_HOST': False,
                'HOST': '127.0.0.1',
                'MAX_WALLCLOCK': '00:03',
                'PROJECT': 'group',
                'QUEUE': 'gp_debug',
                'SCRATCH_DIR': '/tmp/scratch/',
                'TEMP_DIR': '',
                'TYPE': 'slurm',
                'USER': 'root',
                'MAX_PROCESSORS': 10,
                'PROCESSORS_PER_NODE': 10,
            },
        },
        'WRAPPERS': {
            'WRAPPER_SIMDA': {
                'TYPE': 'horizontal-vertical',
                'JOBS_IN_WRAPPER': 'SIM&DA',
                'RETRIALS': '0',
            }
        },
    }
], ids=[
    'Complex Wrapper vertical-horizontal',
    'Complex Wrapper horizontal-vertical',
])
@pytest.mark.docker
@pytest.mark.slurm
def test_run_all_wrappers_workflow_slurm_complex(experiment_data: dict, autosubmit_exp: 'AutosubmitExperimentFixture',
                                                 slurm_server: 'DockerContainer'):
    """Runs a simple Bash script using Slurm."""
    exp = autosubmit_exp(_EXPID, experiment_data=experiment_data, wrapper=True)
    _create_slurm_platform(exp.expid, exp.as_conf)

    exp.as_conf.experiment_data = {
        'EXPERIMENT': {
            'DATELIST': '20120101 20120201',
            'MEMBERS': '000 001',
            'CHUNKSIZEUNIT': 'day',
            'CHUNKSIZE': '1',
            'NUMCHUNKS': '3',
            'CHUNKINI': '',
            'CALENDAR': 'standard',
        }
    }

    exp.autosubmit._check_ownership_and_set_last_command(exp.as_conf, exp.expid, 'run')
    assert 0 == exp.autosubmit.run_experiment(exp.expid)


@pytest.mark.slurm
def test_check_remote_permissions(autosubmit_exp, slurm_server: 'DockerContainer'):
    exp = autosubmit_exp(_EXPID, experiment_data={
        'JOBS': {
            'SIM_V_H': {
                'DEPENDENCIES': {
                    'SIM_V_H-1': {},
                },
                'SCRIPT': 'sleep 0',
                'WALLCLOCK': '00:03',
                'RUNNING': 'chunk',
                'CHECK': 'on_submission',
                'RETRIALS': 1,
                'PLATFORM': _PLATFORM_NAME,
            },
        },
        'PLATFORMS': {
            _PLATFORM_NAME: {
                'ADD_PROJECT_TO_HOST': False,
                'HOST': '127.0.0.1',
                'MAX_WALLCLOCK': '00:03',
                'PROJECT': 'group',
                'QUEUE': 'gp_debug',
                'SCRATCH_DIR': '/tmp/scratch/',
                'TEMP_DIR': '',
                'TYPE': 'slurm',
                'USER': 'root',
                'MAX_PROCESSORS': 10,
                'PROCESSORS_PER_NODE': 10,
            },
        }
    }, wrapper=True)
    submitter = ParamikoSubmitter()
    submitter.load_platforms(as_conf=exp.as_conf)

    slurm_platform: SlurmPlatform = submitter.platforms[_PLATFORM_NAME]

    slurm_platform.connect(as_conf=exp.as_conf)

    assert slurm_platform.check_remote_permissions()

    slurm_platform.closeConnection()
    assert not slurm_platform.check_remote_permissions()


@pytest.mark.slurm
@pytest.mark.parametrize(
    "experiment_data",
    [
        {
            "JOBS": {
                "SIM": {
                    "PLATFORM": _PLATFORM_NAME,
                    "RUNNING": "once",
                    "SCRIPT": 'echo "This is job ${SLURM_JOB_ID} EOM"',
                },
            },
            "PLATFORMS": {
                _PLATFORM_NAME: {
                    "ADD_PROJECT_TO_HOST": False,
                    "HOST": "127.0.0.1",
                    "MAX_WALLCLOCK": "00:03",
                    "PROJECT": "group",
                    "QUEUE": "gp_debug",
                    "SCRATCH_DIR": "/tmp/scratch/",
                    "TEMP_DIR": "",
                    "TYPE": "slurm",
                    "USER": "root",
                    "COMPRESS_REMOTE_LOGS": True,
                },
            },
        },
        {
            "JOBS": {
                "SIM": {
                    "PLATFORM": _PLATFORM_NAME,
                    "RUNNING": "once",
                    "SCRIPT": 'echo "This is job ${SLURM_JOB_ID} EOM"',
                },
            },
            "PLATFORMS": {
                _PLATFORM_NAME: {
                    "ADD_PROJECT_TO_HOST": False,
                    "HOST": "127.0.0.1",
                    "MAX_WALLCLOCK": "00:03",
                    "PROJECT": "group",
                    "QUEUE": "gp_debug",
                    "SCRATCH_DIR": "/tmp/scratch/",
                    "TEMP_DIR": "",
                    "TYPE": "PS",
                    "USER": "root",
                    "COMPRESS_REMOTE_LOGS": True,
                    "REMOTE_LOGS_COMPRESS_TYPE": "xz",
                },
            },
        },
        {
            "JOBS": {
                "SIM": {
                    "PLATFORM": _PLATFORM_NAME,
                    "RUNNING": "once",
                    "SCRIPT": 'echo "This is job ${SLURM_JOB_ID} EOM"',
                },
            },
            "PLATFORMS": {
                _PLATFORM_NAME: {
                    "ADD_PROJECT_TO_HOST": False,
                    "HOST": "127.0.0.1",
                    "MAX_WALLCLOCK": "00:03",
                    "PROJECT": "group",
                    "QUEUE": "gp_debug",
                    "SCRATCH_DIR": "/tmp/scratch/",
                    "TEMP_DIR": "",
                    "TYPE": "slurm",
                    "USER": "root",
                    "COMPRESS_REMOTE_LOGS": True,
                    "PROCESSORS_PER_NODE": 1,
                    "MAX_PROCESSORS": 1,
                },
            },
            "WRAPPERS": {
                "WRAPPER": {
                    "TYPE": "vertical",
                    "JOBS_IN_WRAPPER": "SIM",
                }
            },
        },
        {
            "JOBS": {
                "SIM": {
                    "PLATFORM": _PLATFORM_NAME,
                    "RUNNING": "once",
                    "SCRIPT": 'd_echo "FAIL"',
                    "RETRIALS": 2,
                },
            },
            "PLATFORMS": {
                _PLATFORM_NAME: {
                    "ADD_PROJECT_TO_HOST": False,
                    "HOST": "127.0.0.1",
                    "MAX_WALLCLOCK": "00:03",
                    "PROJECT": "group",
                    "QUEUE": "gp_debug",
                    "SCRATCH_DIR": "/tmp/scratch/",
                    "TEMP_DIR": "",
                    "TYPE": "slurm",
                    "USER": "root",
                    "COMPRESS_REMOTE_LOGS": True,
                    "PROCESSORS_PER_NODE": 1,
                    "MAX_PROCESSORS": 1,
                },
            },
            "WRAPPERS": {
                "WRAPPER": {
                    "TYPE": "vertical",
                    "JOBS_IN_WRAPPER": "SIM",
                    "POLICY": "flexible",
                }
            },
        }
    ],
    ids=[
        "Compress logs with default gzip",
        "Compress logs with xz in PS platform",
        "Compress logs with gzip and vertical wrapper",
        "Compress logs with gzip, vertical wrappers and retrials",
    ],
)
def test_simple_workflow_compress_logs_slurm(
    autosubmit_exp: "AutosubmitExperimentFixture",
    experiment_data: dict,
    slurm_server: "DockerContainer",
):
    """Test compressing remote logs in a simple workflow using Slurm."""
    with_wrapper = "WRAPPERS" in experiment_data

    exp = autosubmit_exp(_EXPID, experiment_data=experiment_data, wrapper=with_wrapper)
    _create_slurm_platform(exp.expid, exp.as_conf)

    exp.autosubmit._check_ownership_and_set_last_command(exp.as_conf, exp.expid, "run")
    exp.autosubmit.run_experiment(exp.expid)

    # Check if the log files are compressed
    logs_dir = Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR).joinpath(
        exp.expid, exp.as_conf.basic_config.LOCAL_TMP_DIR, f"LOG_{exp.expid}"
    )
    compression_type = (
        experiment_data.get("PLATFORMS", {})
        .get(_PLATFORM_NAME, {})
        .get("REMOTE_LOGS_COMPRESS_TYPE", "gzip")
    )

    # Get all files in the logs directory
    files = [f for f in Path(logs_dir).glob("*")]

    assert len(files) > 0, f"No log files found in {logs_dir}"

    # Get job_data
    exp_history = ExperimentHistory(
        exp.expid,
        exp.as_conf.basic_config.JOBDATA_DIR,
        exp.as_conf.basic_config.HISTORICAL_LOG_DIR,
    )
    last_job_data = exp_history.manager.get_all_last_job_data_dcs()
    assert len(last_job_data) > 0, "No job data found after running the experiment."

    logs_filenames: list[str] = []
    for job in last_job_data:
        logs_filenames.extend([job.out, job.err])

    if compression_type == "xz":
        _val_fn = is_xz_file
        assert any(is_xz_file(str(f)) for f in files), (
            "No compressed xz log files found."
        )
        assert all(log_filename.endswith(".xz") for log_filename in logs_filenames)
    else:
        _val_fn = is_gzip_file
        assert any(is_gzip_file(str(f)) for f in files), (
            "No compressed gzip log files found."
        )
        assert all(log_filename.endswith(".gz") for log_filename in logs_filenames)

    for log_filename in logs_filenames:
        log_path = logs_dir.joinpath(log_filename)
        assert log_path.exists(), f"Log file {log_path} does not exist."
        assert _val_fn(str(log_path)), (
            f"Log file {log_path} is not compressed as expected."
        )


@pytest.mark.slurm
@pytest.mark.parametrize(
    "experiment_data",
    [
        {
            "JOBS": {
                "SIM": {
                    "PLATFORM": _PLATFORM_NAME,
                    "RUNNING": "once",
                    "SCRIPT": 'echo "This is job ${SLURM_JOB_ID} EOM"',
                },
            },
            "PLATFORMS": {
                _PLATFORM_NAME: {
                    "ADD_PROJECT_TO_HOST": False,
                    "HOST": "127.0.0.1",
                    "MAX_WALLCLOCK": "00:03",
                    "PROJECT": "group",
                    "QUEUE": "gp_debug",
                    "SCRATCH_DIR": "/tmp/scratch/",
                    "TEMP_DIR": "",
                    "TYPE": "slurm",
                    "USER": "root",
                    "COMPRESS_REMOTE_LOGS": True,
                },
            },
        },
    ],
    ids=[
        "Default compress logs with missing compression tool",
    ],
)
def test_compress_log_missing_tool(
    experiment_data: dict,
    autosubmit_exp: "AutosubmitExperimentFixture",
    slurm_server: "DockerContainer",
    mocker,
):
    exp = autosubmit_exp(_EXPID, experiment_data=experiment_data)
    _create_slurm_platform(exp.expid, exp.as_conf)

    exp.autosubmit._check_ownership_and_set_last_command(exp.as_conf, exp.expid, "run")

    # Mock the compress_file method to simulate missing compression tool
    mocker.patch(
        "autosubmit.platforms.paramiko_platform.ParamikoPlatform.compress_file",
        return_value=None,
    )
    exp.autosubmit.run_experiment(exp.expid)

    # Check if the log files are compressed
    logs_dir = Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR).joinpath(
        exp.expid, exp.as_conf.basic_config.LOCAL_TMP_DIR, f"LOG_{exp.expid}"
    )

    # Get all files in the logs directory
    files = [f for f in Path(logs_dir).glob("*")]
    assert len(files) > 0, f"No log files found in {logs_dir}"

    # Get job_data
    exp_history = ExperimentHistory(
        exp.expid,
        exp.as_conf.basic_config.JOBDATA_DIR,
        exp.as_conf.basic_config.HISTORICAL_LOG_DIR,
    )
    last_job_data = exp_history.manager.get_all_last_job_data_dcs()
    assert len(last_job_data) > 0, "No job data found after running the experiment."

    logs_filenames: list[str] = []
    for job in last_job_data:
        logs_filenames.extend([job.out, job.err])

    # None of the log files should be compressed
    for log_filename in logs_filenames:
        assert not log_filename.endswith(".gz") and not log_filename.endswith(".xz"), (
            f"Log file {log_filename} should not have a compressed extension."
        )

        log_path = logs_dir.joinpath(log_filename)
        assert log_path.exists(), f"Log file {log_path} does not exist."
        assert not is_gzip_file(str(log_path)) and not is_xz_file(str(log_path)), (
            f"Log file {log_path} should not be compressed."
        )


@pytest.mark.slurm
@pytest.mark.parametrize(
    "experiment_data",
    [
        {
            "JOBS": {
                "SIM": {
                    "PLATFORM": _PLATFORM_NAME,
                    "RUNNING": "once",
                    "SCRIPT": 'echo "This is job ${SLURM_JOB_ID} EOM"',
                },
            },
            "PLATFORMS": {
                _PLATFORM_NAME: {
                    "ADD_PROJECT_TO_HOST": False,
                    "HOST": "127.0.0.1",
                    "MAX_WALLCLOCK": "00:03",
                    "PROJECT": "group",
                    "QUEUE": "gp_debug",
                    "SCRATCH_DIR": "/tmp/scratch/",
                    "TEMP_DIR": "",
                    "TYPE": "slurm",
                    "USER": "root",
                    "COMPRESS_REMOTE_LOGS": True,
                },
            },
        },
    ],
    ids=[
        "Default compress logs with missing compression tool",
    ],
)
def test_compress_log_fail_command(
    experiment_data: dict,
    autosubmit_exp: "AutosubmitExperimentFixture",
    slurm_server: "DockerContainer",
    mocker,
):
    exp = autosubmit_exp(_EXPID, experiment_data=experiment_data)
    _create_slurm_platform(exp.expid, exp.as_conf)

    mocker.patch(
        "autosubmit.platforms.paramiko_platform.ParamikoPlatform.send_command",
        side_effect=Exception("cmd not found"),
    )

    result = exp.platform.compress_file("/some_log_file.log")
    assert result is None
