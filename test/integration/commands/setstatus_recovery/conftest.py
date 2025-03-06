import pytest
from typing import Dict
from pathlib import Path


@pytest.fixture(scope="function")
def general_data(tmp_path: Path) -> Dict[str, object]:
    """
    Provides part of the `experiment_data` dictionary used by the
    integration tests in `commands/setstatus_recovery`.

    :param tmp_path: Temporary directory fixture from pytest.
    :type tmp_path: Path
    :return: A dictionary compatible with AutosubmitConfig.experiment_data
    :rtype: Dict[str, object]
    """
    return {
        'PROJECT': {
            'PROJECT_TYPE': 'none',
            'PROJECT_DESTINATION': 'dummy_project'
        },
        'AUTOSUBMIT': {
            'WORKFLOW_COMMIT': 'dummy_commit',
            'LOCAL_ROOT_DIR': str(tmp_path)  # Override root dir to tmp_path
        },
        'CONFIG': {
            "SAFETYSLEEPTIME": 0,
            "TOTALJOBS": 20,
            "MAXWAITINGJOBS": 20
        },
        'DEFAULT': {
            'HPCARCH': "local",
        },
        'PLATFORMS': {
            'TEST_SLURM': {
                'TYPE': 'slurm',
                'ADD_PROJECT_TO_HOST': 'False',
                'HOST': '127.0.0.1',
                'MAX_WALLCLOCK': '48:00',
                'PROJECT': 'group',
                'QUEUE': 'gp_debug',
                'SCRATCH_DIR': '/tmp/scratch',
                'TEMP_DIR': '',
                'USER': 'root',
                'PROCESSORS': '1',
                'MAX_PROCESSORS': '128',
                'PROCESSORS_PER_NODE': '128',
            },
            'TEST_PS': {
                'TYPE': 'PS',
                'ADD_PROJECT_TO_HOST': 'False',
                'HOST': '127.0.0.1',
                'MAX_WALLCLOCK': '48:00',
                'PROJECT': 'group',
                'QUEUE': 'gp_debug',
                'SCRATCH_DIR': '/tmp/scratch',
                'TEMP_DIR': '',
                'USER': 'root',
                'PROCESSORS': '1',
                'MAX_PROCESSORS': '128',
                'PROCESSORS_PER_NODE': '128',
            }
        }
    }


@pytest.fixture(scope="function")
def experiment_data(tmp_path: Path) -> Dict[str, object]:
    """
    Provide part of the `experiment_data` dictionary used by the
    integration tests in `commands/setstatus_recovery`.

    :param tmp_path: Temporary directory fixture from pytest.
    :type tmp_path: Path
    :return: A dictionary compatible with AutosubmitConfig.experiment_data
    :rtype: Dict[str, object]
    """
    return {
        'EXPERIMENT': {
            'DATELIST': '20200101',
            'MEMBERS': 'fc0',
            'CHUNKSIZEUNIT': 'month',
            'SPLITSIZEUNIT': 'day',
            'CHUNKSIZE': 1,
            'NUMCHUNKS': 1,
            'CALENDAR': 'standard',
        }
    }


@pytest.fixture(scope="function")
def jobs_data(tmp_path: Path) -> Dict[str, object]:
    """
    Provide a minimal but representative `jobs_data` dictionary used by the
    integration tests in `commands/setstatus_recovery`.

    :param tmp_path: Temporary directory fixture from pytest.
    :type tmp_path: Path
    :return: A dictionary compatible with AutosubmitConfig.jobs_data
    :rtype: Dict[str, object]
    """
    return {
        'JOBS': {
            'LOCALJOB': {
                'SCRIPT': "|"
                          "sleep 1",
                'DEPENDENCIES': {
                    'LOCALJOB': {
                        'SPLITS_FROM': {
                            'ALL': {'SPLITS_TO': 'previous'}
                        }
                    },
                },
                'RUNNING': 'chunk',
                'WALLCLOCK': '02:00',
                'PLATFORM': 'LOCAL',
                'SPLITS': '3',
                'CHECK': 'on_submission',
            },
            'PSJOB': {
                'SCRIPT': "|"
                          "sleep 1",
                'DEPENDENCIES': {
                    'PSJOB': {
                        'SPLITS_FROM': {
                            'ALL': {'SPLITS_TO': 'previous'}
                        }
                    }
                },
                'RUNNING': 'chunk',
                'WALLCLOCK': '02:00',
                'PLATFORM': 'TEST_PS',
                'SPLITS': '3',
                'CHECK': 'on_submission',
            },
            'SLURMJOB': {
                'SCRIPT': "|"
                          "sleep 1",
                'DEPENDENCIES': {
                    'SLURMJOB': {
                        'SPLITS_FROM': {
                            'ALL': {'SPLITS_TO': 'previous'}
                        }
                    }
                },
                'RUNNING': 'chunk',
                'WALLCLOCK': '02:00',
                'PLATFORM': 'TEST_SLURM',
                'SPLITS': "3",
                'CHECK': 'on_submission',
            }
        }
    }
