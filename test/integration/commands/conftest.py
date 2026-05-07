# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
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

from getpass import getuser
from pathlib import Path
from typing import Dict, Any

import pytest


@pytest.fixture
def redirect_log_info(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Redirect `Log` calls to a temporary file called `autosubmit.log` and return the file path.
    """
    from datetime import datetime

    log_file = tmp_path / "autosubmit.log"

    def _new_log_path(message: Any, *args: Any, **kwargs: Any) -> None:
        timestamp = datetime.now().astimezone().isoformat()
        with log_file.open("a", encoding="utf-8") as fh:
            fh.write(f"{timestamp} {message}\n")

    try:
        from autosubmit.log.log import Log
        monkeypatch.setattr(Log, "info", _new_log_path)
        monkeypatch.setattr(Log, "debug", _new_log_path)
        monkeypatch.setattr(Log, "warning", _new_log_path)
        monkeypatch.setattr(Log, "error", _new_log_path)
        monkeypatch.setattr(Log, "critical", _new_log_path)
        monkeypatch.setattr(Log, "status", _new_log_path)
    except Exception:
        raise

    return log_file


@pytest.fixture(scope="function")
def general_data(tmp_path: Path) -> dict[str, Any]:
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
                'USER': getuser(),
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
                'USER': getuser(),
                'PROCESSORS': '1',
                'MAX_PROCESSORS': '128',
                'PROCESSORS_PER_NODE': '128',
            },
            # TODO: Containerize ecaccess to be able to run these tests in CI/CD.
            'TEST_EC': {
                'TYPE': 'ecaccess',  # enables the usage of ecaccess commands, requires a valid .eccert in ~/.eccert
                'VERSION': 'slurm',  # HPC scheduler accessed via ecaccess commands
                'ADD_PROJECT_TO_HOST': 'False',
                'HOST': 'hpc-login',  # Can only run locally with a valid .eccert, see https://gitlab.earth.bsc.es/es/auto-ecearth3/-/wikis/Running-on-ECMWF-HPC2020
                'MAX_WALLCLOCK': '48:00',
                'PROJECT': 'spesiccf',
                'QUEUE': 'nf',
                'EC_QUEUE': 'hpc',
                'SCRATCH_DIR': '/ec/res4/scratch/c3d',
                'USER': 'c3d',  # Requires a valid user, contact ecmwf for one.
                'PROCESSORS_PER_NODE': '4',
                'MAX_PROCESSORS': '4',
                'CUSTOM_DIRECTIVES': ['#SBATCH --hint=nomultithread'],
            },
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


def wrapped_jobs(wrapper_type: str, structure: dict, size: dict) -> Dict[str, Any]:
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
