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

import pytest


@pytest.mark.parametrize(
    "data,expected_data,must_exists",
    [
        pytest.param(
            {
                "DEFAULT": {
                    "HPCARCH": "local",
                    "CUSTOM_CONFIG": {
                        "PRE": ["configpre", "configpre2"],
                        "POST": ["configpost", "configpost2"]
                    }
                },
                "WRAPPERS": {
                    "wrapper1": {
                        "JOBS_IN_WRAPPER": "job1 job2",
                        "TYPE": "VERTICAL"
                    }
                },
                "JOBS": {
                    "job1": {
                        "DEPENDENCIES": "job2 job3",
                        "CUSTOM_DIRECTIVES": ["directive1", "directive2"],
                        "FILE": "file1 file2"
                    }
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            {
                "DEFAULT": {
                    "HPCARCH": "LOCAL",
                    "CUSTOM_CONFIG": {
                        "PRE": "configpre,configpre2",
                        "POST": "configpost,configpost2"
                    },
                    'EXPID': 't000'
                },
                "WRAPPERS": {
                    "WRAPPER1": {
                        "JOBS_IN_WRAPPER": "JOB1 JOB2",
                        "TYPE": "vertical"
                    }
                },
                "JOBS": {
                    "JOB1": {
                        "DEPENDENCIES": {"JOB2": {}, "JOB3": {}},
                        "CUSTOM_DIRECTIVES": "['directive1', 'directive2']",
                        "FILE": "file1",
                        "ADDITIONAL_FILES": ["file2"]
                    }
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            True,
            id="complete_conf_and_unified"
        ),
        pytest.param(
            {
                "WRAPPERS": {
                    "wrapper1": "job1 job2"
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            {
                "WRAPPERS": {
                    "WRAPPER1": "job1 job2"
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            False,
            id="wrappers_new_data"
        ),
        pytest.param(
            {
                "JOBS": {
                    "job1": {
                        "DEPENDENCIES": "job2 job3"
                    }
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            {
                "JOBS": {
                    "JOB1": {
                        'FILE': '',
                        'ADDITIONAL_FILES': [],
                        "DEPENDENCIES": {"JOB2": {}, "JOB3": {}}
                    }
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            True,
            id="jobs_with_dependencies_conf_unified"
        ),
        pytest.param(
            {
                "JOBS": {
                    "job1": {}
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            {
                'JOBS': {
                    'JOB1': {
                        'FILE': '',
                        'ADDITIONAL_FILES': [],
                        'DEPENDENCIES': {},
                    },
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            True,
            id="jobs_unified_and_empty"
        ),
        pytest.param(
            {
                "JOBS": {
                    "JOB": {
                        "PROCESSORS": 30
                    }
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            {
                'JOBS': {
                    'JOB': {
                        'PROCESSORS': 30
                    }
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            False,
            id="jobs_new_data"
        ),
        pytest.param(
            {
                "JOBS": {
                    "job1": {
                        "CUSTOM_DIRECTIVES": "directive1 directive2"
                    }
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            {
                "JOBS": {
                    "JOB1": {
                        "CUSTOM_DIRECTIVES": "directive1 directive2",
                        "FILE": "",
                        "ADDITIONAL_FILES": [],
                        "DEPENDENCIES": {}
                    }
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            True,
            id="custom_directives_unified"
        ),
        pytest.param(
            {
                "JOBS": {
                    "job1": {
                        "CUSTOM_DIRECTIVES": "directive1 directive2"
                    }
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            {
                "JOBS": {
                    "JOB1": {
                        "CUSTOM_DIRECTIVES": "directive1 directive2",
                    }
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            False,
            id="custom_directives_new_data"
        ),
        pytest.param(
            {
                "JOBS": {
                    "job1": {
                        "CUSTOM_DIRECTIVES": ["directive1", "directive2"]
                    }
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            {
                "JOBS": {
                    "JOB1": {
                        "CUSTOM_DIRECTIVES": "['directive1', 'directive2']",
                    }
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            False,
            id="custom_directives_list_new_data"
        ),
        pytest.param(
            {
                "JOBS": {
                    "job1": {
                        "FILE": "file1, file2, file3"
                    }
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            {
                'JOBS': {
                    'JOB1': {
                        'FILE': 'file1',
                        'ADDITIONAL_FILES': ['file2', 'file3'],
                        'DEPENDENCIES': {},
                    },
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            True,
            id="additional_jobs_unified"
        ),
        pytest.param(
            {
                "JOBS": {
                    "job1": {
                        "FILE": "file1, file2, file3"
                    }
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            {
                'JOBS': {
                    'JOB1': {
                        'FILE': 'file1',
                        'ADDITIONAL_FILES': ['file2', 'file3'],
                    },
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            False,
            id="additional_jobs_new_data"
        ),
        pytest.param(
            {
                "JOBS": {
                    "job1": {
                        "FILE": ["file1", "file2", "file3"]
                    }
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            {
                'JOBS': {
                    'JOB1': {
                        'FILE': 'file1',
                        'ADDITIONAL_FILES': ['file2', 'file3'],
                        'DEPENDENCIES': {},
                    },
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            True,
            id="file_yaml_list"
        ),
        pytest.param(
            {
                "JOBS": {
                    "job1": {
                        "FILE": "FILE1",
                        "DEPENDENCIES": {
                            "job2": {"STATUS": "FAILED"},
                            "job3": {"STATUS": "FAILED?"},
                            "job4": {"STATUS": "RUNNING"},
                            "job5": {"STATUS": "COMPLETED"},
                            "job6": {"STATUS": "SKIPPED"},
                            "job7": {"STATUS": "READY"},
                            "job8": {"STATUS": "DELAYED"},
                            "job9": {"STATUS": "PREPARED"},
                            "job10": {"STATUS": "QUEUING"},
                            "job11": {"STATUS": "SUBMITTED"},
                            "job12": {"STATUS": "HELD"},
                            "job13": {"STATUS": "RUNNING?"},
                        },
                    }
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            {
                'JOBS': {
                    'JOB1': {
                        'FILE': 'FILE1',
                        'ADDITIONAL_FILES': [],
                        'DEPENDENCIES': {
                            'JOB2': {'STATUS': 'FAILED', 'OPTIONAL': False},
                            'JOB3': {'STATUS': 'FAILED', 'OPTIONAL': True},
                            'JOB4': {'STATUS': 'RUNNING', 'OPTIONAL': False},
                            'JOB5': {'STATUS': 'COMPLETED', 'OPTIONAL': False},
                            'JOB6': {'STATUS': 'SKIPPED', 'OPTIONAL': False},
                            'JOB7': {'STATUS': 'READY', 'OPTIONAL': False},
                            'JOB8': {'STATUS': 'DELAYED', 'OPTIONAL': False},
                            'JOB9': {'STATUS': 'PREPARED', 'OPTIONAL': False},
                            'JOB10': {'STATUS': 'QUEUING', 'OPTIONAL': False},
                            'JOB11': {'STATUS': 'SUBMITTED', 'OPTIONAL': False},
                            'JOB12': {'STATUS': 'HELD', 'OPTIONAL': False},
                            'JOB13': {'STATUS': 'RUNNING', 'OPTIONAL': True},
                        },
                    },
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            True,
            id="dependencies_status"
        ),
        pytest.param(
            {
                "JOBS": {
                    "job1": {
                        "FILE": "FILE1",
                        "NOTIFY_ON": ["running", "COmpLETED"]
                    }
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            {
                'JOBS': {
                    'JOB1': {
                        'FILE': 'FILE1',
                        'ADDITIONAL_FILES': [],
                        'DEPENDENCIES': {},
                        'NOTIFY_ON': ['RUNNING', 'COMPLETED'],
                    },
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            True,
            id="notify_on_list"
        ),
        pytest.param(
            {
                "JOBS": {
                    "job1": {
                        "FILE": "FILE1",
                        "NOTIFY_ON": "running, COmpLETED"
                    }
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            {
                'JOBS': {
                    'JOB1': {
                        'FILE': 'FILE1',
                        'ADDITIONAL_FILES': [],
                        'DEPENDENCIES': {},
                        'NOTIFY_ON': ['RUNNING', 'COMPLETED'],
                    },
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            True,
            id="notify_on_string_with_,"
        ),
        pytest.param(
            {
                "JOBS": {
                    "job1": {
                        "FILE": "FILE1",
                        "NOTIFY_ON": "running COmpLETED"
                    }
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            {
                'JOBS': {
                    'JOB1': {
                        'FILE': 'FILE1',
                        'ADDITIONAL_FILES': [],
                        'DEPENDENCIES': {},
                        'NOTIFY_ON': ['RUNNING', 'COMPLETED'],
                    },
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            True,
            id="notify_on_string_without_,"
        ),
        pytest.param(
            {
                "JOBS": {
                    "job1": {
                        "FILE": "FILE1",
                        "NOTIFY_ON": "running"
                    }
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            {
                'JOBS': {
                    'JOB1': {
                        'FILE': 'FILE1',
                        'ADDITIONAL_FILES': [],
                        'DEPENDENCIES': {},
                        'NOTIFY_ON': ['RUNNING'],
                    },
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            True,
            id="notify_on_string_single"
        ),
        pytest.param(
            {
                "JOBS": {
                    "job1": {
                        "FILE": "FILE1",
                        "WALLCLOCK": "00:20:00"
                    }
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            {
                'JOBS': {
                    'JOB1': {
                        'FILE': 'FILE1',
                        'ADDITIONAL_FILES': [],
                        'DEPENDENCIES': {},
                        'WALLCLOCK': "00:20",
                    },
                },
                'STORAGE': {
                    'TYPE': 'sqlite'
                }
            },
            True,
            id="wallclock"
        ),
    ]
)
def test_normalize_variables(autosubmit_config, data, expected_data, must_exists):
    as_conf = autosubmit_config(expid='t000', experiment_data=data)
    normalized_data = as_conf.normalize_variables(data, must_exists=must_exists)
    assert normalized_data == expected_data
    normalized_data = as_conf.normalize_variables(normalized_data, must_exists=must_exists)
    assert normalized_data == expected_data
