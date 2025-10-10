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

"""Integration tests for the Slurm platform.

As these tests use a GitHub Actions service with limited capacity for running jobs,
we limit in pytest how many tests we run in parallel to avoid the service becoming
unresponsive (which likely explains our banner timeout messages before, as probably
it was busy churning the previous messages and Slurm jobs).

This is done by assigning the tests the group "slurm". This forces pytest to send
all the grouped tests to the same worker,
"""

import pytest

from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.config.yamlparser import YAMLParserFactory
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_list import JobList
from autosubmit.job.job_list_persistence import JobListPersistencePkl
from autosubmit.job.job_packager import JobPackager

from autosubmit.platforms.slurmplatform import SlurmPlatform

from test.integration.conftest import AutosubmitExperimentFixture, DockerContainer

_EXPID = 't001'

_PLATFORM_NAME = 'TEST_SLURM'


def _create_slurm_platform(expid: str, as_conf: AutosubmitConfig):
    return SlurmPlatform(expid, _PLATFORM_NAME, config=as_conf.experiment_data, auth_password=None)


@pytest.mark.xdist_group('slurm')
@pytest.mark.slurm
def test_create_platform_slurm(
        autosubmit_exp,
        slurm_server: 'DockerContainer',
):
    """Test the Slurm platform object creation."""
    exp = autosubmit_exp('t000', experiment_data={
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
                'MAX_PROCESSORS': 1,
                'PROCESSORS_PER_NODE': 1,
            }
        }
    })
    platform = _create_slurm_platform(exp.expid, exp.as_conf)
    assert platform.name == _PLATFORM_NAME
    # TODO: add more assertion statements...


@pytest.mark.xdist_group('slurm')
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
                'MAX_PROCESSORS': 1,
                'PROCESSORS_PER_NODE': 1,
            },
        },
    },
    {
        'JOBS': {
            'SIM': {
                'PLATFORM': _PLATFORM_NAME,
                'RUNNING': 'chunk',
                'SCRIPT': 'echo "0"',
            },
            'SIM_2': {
                'PLATFORM': _PLATFORM_NAME,
                'RUNNING': 'chunk',
                'SCRIPT': 'echo "0"',
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
                'MAX_PROCESSORS': 1,
                'PROCESSORS_PER_NODE': 1,
            },
        },
    },
], ids=[
    'Simple Workflow',
    'Dependency Workflow',
])
def test_run_simple_workflow_slurm(
        autosubmit_exp: AutosubmitExperimentFixture,
        experiment_data: dict,
        slurm_server: 'DockerContainer'
):
    """Runs a simple Bash script using Slurm."""
    exp = autosubmit_exp('t001', experiment_data=experiment_data)
    _create_slurm_platform(exp.expid, exp.as_conf)

    exp.autosubmit._check_ownership_and_set_last_command(exp.as_conf, exp.expid, 'run')
    assert 0 == exp.autosubmit.run_experiment(exp.expid)


@pytest.mark.parametrize('experiment_data', [
    {
        'JOBS': {
            'SIM': {
                'DEPENDENCIES': {
                    'SIM-1': {}
                },
                'SCRIPT': 'echo "0"',
                'WALLCLOCK': '00:03',
                'RUNNING': 'chunk',
                'CHECK': 'on_submission',
                'PLATFORM': _PLATFORM_NAME,
            },
            'POST': {
                'DEPENDENCIES': {
                    'SIM',
                },
                'SCRIPT': 'echo "0"',
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
                'SCRIPT': 'echo "0"',
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
                'MAX_WALLCLOCK': '00:03',
                'PROJECT': 'group',
                'QUEUE': 'gp_debug',
                'SCRATCH_DIR': '/tmp/scratch/',
                'TEMP_DIR': '',
                'TYPE': 'slurm',
                'USER': 'root',
                'MAX_PROCESSORS': 1,
                'PROCESSORS_PER_NODE': 1,
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
    {
        'JOBS': {
            'SIMV': {
                'DEPENDENCIES': {
                    'SIMV-1': {}
                },
                'SCRIPT': 'echo "0"',
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
                'MAX_PROCESSORS': 1,
                'PROCESSORS_PER_NODE': 1,
            },
        },
        'WRAPPERS': {
            'WRAPPERV': {
                'TYPE': 'vertical',
                'JOBS_IN_WRAPPER': 'SIMV',
                'RETRIALS': 0,
            },
        },
    },
    {
        'JOBS': {
            'SIMH': {
                'DEPENDENCIES': {
                    'SIMH-1': {}
                },
                'SCRIPT': 'echo "0"',
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
                'MAX_PROCESSORS': 1,
                'PROCESSORS_PER_NODE': 1,
            },
        },
        'WRAPPERS': {
            'WRAPPERH': {
                'TYPE': 'horizontal',
                'JOBS_IN_WRAPPER': 'SIMH',
                'RETRIALS': 0,
            },
        },
    },
    {
        'JOBS': {
            'SIMHV': {
                'DEPENDENCIES': {
                    'SIMHV-1': {}
                },
                'SCRIPT': 'echo "0"',
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
                'MAX_PROCESSORS': 1,
                'PROCESSORS_PER_NODE': 1,
            },
        },
        'WRAPPERS': {
            'WRAPPERHV': {
                'TYPE': 'horizontal-vertical',
                'JOBS_IN_WRAPPER': 'SIMHV',
                'RETRIALS': 0,
            },
        },
    },
    {
        'JOBS': {
            'SIMVH': {
                'DEPENDENCIES': {
                    'SIMVH-1': {},
                },
                'SCRIPT': 'echo "0"',
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
                'MAX_PROCESSORS': 1,
                'PROCESSORS_PER_NODE': 1,
            },
        },
        'WRAPPERS': {
            'WRAPPERVH': {
                'TYPE': 'vertical-horizontal',
                'JOBS_IN_WRAPPER': 'SIMVH',
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
                'MAX_PROCESSORS': 1,
                'PROCESSORS_PER_NODE': 1,
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
                'MAX_PROCESSORS': 1,
                'PROCESSORS_PER_NODE': 1,
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

    exp = autosubmit_exp('t002', experiment_data=experiment_data, wrapper=True)
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


def test_check_if_packages_are_ready_to_build(autosubmit_exp):
    exp = autosubmit_exp('a000', experiment_data={})
    platform_config = {
        "LOCAL_ROOT_DIR": exp.as_conf.basic_config.LOCAL_ROOT_DIR,
        "LOCAL_TMP_DIR": str(exp.as_conf.basic_config.LOCAL_ROOT_DIR+'exp_tmp_dir'),
        "LOCAL_ASLOG_DIR": str(exp.as_conf.basic_config.LOCAL_ROOT_DIR+'aslogs_dir')
    }
    platform = SlurmPlatform('a000', "wrappers_test", platform_config)

    job_list = JobList('a000', exp.as_conf, YAMLParserFactory(), JobListPersistencePkl())
    for i in range(3):
        job = Job(f"job{i}", i, Status.READY, 0)
        job.section = f"SECTION{i}"
        job.platform = platform
        job_list._job_list.append(job)

    packager = JobPackager(exp.as_conf, platform, job_list)
    packager.wallclock = "01:00"
    packager.het = {'HETSIZE': {'CURRENT_QUEUE': ''}, 'CURRENT_QUEUE': '', 'NODES': [2], 'PARTITION': [''], 'CURRENT_PROJ': '', 'EXCLUSIVE': 'false',
                  'MEMORY': '', 'MEMORY_PER_TASK': 2, 'NUMTHREADS': '', 'RESERVATION': '', 'CUSTOM_DIRECTIVES': '', 'TASKS': '',
                  }

    job_result, check = packager.check_if_packages_are_ready_to_build()
    assert check and len(job_result) == 3
