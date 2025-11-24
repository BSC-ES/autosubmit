import pytest
from typing import Dict
from pathlib import Path


@pytest.fixture(scope="function")
def general_data(tmp_path: Path) -> Dict[str, object]:
    """
    Provides part of the `experiment_data` dictionary used by the
    integration tests in `commands`.

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
    integration tests in `commands`.

    :param tmp_path: Temporary directory fixture from pytest.
    :type tmp_path: Path
    :return: A dictionary compatible with AutosubmitConfig.experiment_data
    :rtype: Dict[str, object]
    """
    return {
        'EXPERIMENT': {
            'DATELIST': '20200101 20200102',
            'MEMBERS': 'fc0 fc1',
            'CHUNKSIZEUNIT': 'month',
            'SPLITSIZEUNIT': 'day',
            'CHUNKSIZE': 1,
            'NUMCHUNKS': 2,
            'CALENDAR': 'standard',
        }
    }


@pytest.fixture(scope="function")
def jobs_data(tmp_path: Path) -> Dict[str, object]:
    """
    Provide a representative `jobs` dictionary used by the
    integration tests in `commands`.

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


def wrapped_jobs(wrapper_type: str, structure: dict, size: dict) -> Dict[str, object]:
    """Provides a `jobs_data` dictionary with wrapped jobs used by the
    integration tests in `commands`.

    :param wrapper_type: The type of wrapper to use ['vertical', 'horizontal', 'vertical-horizontal', 'horizontal-vertical']
    :type wrapper_type: str
    :param structure: The structure of the wrapper [min_trigger_status, from_step]
    :type structure: dict
    :param size: The size limits of the wrapper [MAX_V, MAX_H, MIN_V, MIN_H]
    :param size: dict
    :return: A dictionary compatible with AutosubmitConfig.jobs_data
    :rtype: Dict[str, object]
    """
    mod_experiment_data = {
        'EXPERIMENT': {
            'DATELIST': '20200101',
            'MEMBERS': 'fc0 fc1',
            'CHUNKSIZEUNIT': 'month',
            'SPLITSIZEUNIT': 'day',
            'CHUNKSIZE': 1,
            'NUMCHUNKS': 2,
            'CALENDAR': 'standard',
        }
    }
    complex = {}
    simple = {
        'JOBS': {
            'WRAPPED_JOB_SIMPLE': {
                'SCRIPT': "|"
                          "sleep 0",
                'RUNNING': 'chunk',
                'DEPENDENCIES': {
                    'WRAPPED_JOB_SIMPLE-1': {},
                },
                'WALLCLOCK': '00:01',
                'PLATFORM': 'TEST_SLURM',
                'CHECK': 'on_submission',
                'PROCESSORS': '1',

            },
        },
        'WRAPPERS': {
            'SIMPLE_WRAPPER': {
                'TYPE': wrapper_type,
                'JOBS_IN_WRAPPER': 'WRAPPED_JOB_SIMPLE',
                'MAX_WRAPPED_V': size.get('MAX_V', 2),
                'MAX_WRAPPED_H': size.get('MAX_H', 2),
                'MIN_WRAPPED_V': size.get('MIN_V', 2),
                'MIN_WRAPPED_H': size.get('MIN_H', 2),
            },
        }
    }
    if len(structure) > 0:
        complex = {
            'JOBS': {
                'JOB': {
                    'SCRIPT': "|"
                              "sleep 0"
                              "as_checkpoint"
                              "as_checkpoint",
                    'RUNNING': 'chunk',
                    'DEPENDENCIES': {
                        'JOB-1': {},
                    },
                    'WALLCLOCK': '00:01',
                    'PLATFORM': 'TEST_SLURM',
                    'CHECK': 'on_submission',
                    'PROCESSORS': '1',
                },
                'COMPLEX_WRAPPER': {
                    'SCRIPT': "|"
                              "sleep 0",
                    'DEPENDENCIES': {
                        'JOB': {
                            'STATUS': structure['min_trigger_status'],
                            'FROM_STEP': structure['from_step'],
                        },
                        'COMPLEX_WRAPPER-1': {},
                    },
                    'RUNNING': 'chunk',
                    'WALLCLOCK': '00:01',
                    'PROCESSORS': '1',
                    'PLATFORM': 'TEST_SLURM',
                },
            },
            'WRAPPERS': {
                'COMPLEX_WRAPPER': {
                    'TYPE': wrapper_type,
                    'JOBS_IN_WRAPPER': 'COMPLEX_WRAPPER',
                    'MAX_WRAPPED_V': size.get('MAX_V', 2),
                    'MAX_WRAPPED_H': size.get('MAX_H', 2),
                    'MIN_WRAPPED_V': size.get('MIN_V', 2),
                    'MIN_WRAPPED_H': size.get('MIN_H', 2),
                }
            }
        }
    full_config = mod_experiment_data | simple
    full_config['JOBS'].update(complex.get('JOBS', {}))
    full_config['WRAPPERS'].update(complex.get('WRAPPERS', {}))
    return full_config
