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
                }
            },
            True,
            id="complete_conf_and_unified"
        ),
        pytest.param(
            {
                "WRAPPERS": {
                    "wrapper1": "job1 job2"
                }
            },
            {
                "WRAPPERS": {
                    "WRAPPER1": "job1 job2"
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
                }
            },
            {
                "JOBS": {
                    "JOB1": {
                        'FILE': '',
                        'ADDITIONAL_FILES': [],
                        "DEPENDENCIES": {"JOB2": {}, "JOB3": {}}
                    }
                }
            },
            True,
            id="jobs_with_dependencies_conf_unified"
        ),
        pytest.param(
            {
                "JOBS": {
                    "job1": {}
                }
            },
            {
                'JOBS': {
                    'JOB1': {
                        'FILE': '',
                        'ADDITIONAL_FILES': [],
                        'DEPENDENCIES': {},
                    },
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
                }
            },
            {
                'JOBS': {
                    'JOB': {
                        'PROCESSORS': 30
                    }
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
                }
            },
            {
                "JOBS": {
                    "JOB1": {
                        "CUSTOM_DIRECTIVES": "directive1 directive2",
                    }
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
                }
            },
            {
                "JOBS": {
                    "JOB1": {
                        "CUSTOM_DIRECTIVES": "['directive1', 'directive2']",
                    }
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
                }
            },
            {
                'JOBS': {
                    'JOB1': {
                        'FILE': 'file1',
                        'ADDITIONAL_FILES': ['file2', 'file3'],
                        'DEPENDENCIES': {},
                    },
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
                }
            },
            {
                'JOBS': {
                    'JOB1': {
                        'FILE': 'file1',
                        'ADDITIONAL_FILES': ['file2', 'file3'],
                    },
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
                }
            },
            {
                'JOBS': {
                    'JOB1': {
                        'FILE': 'file1',
                        'ADDITIONAL_FILES': ['file2', 'file3'],
                        'DEPENDENCIES': {},
                    },
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
                        },
                    }
                }
            },
            {
                'JOBS': {
                    'JOB1': {
                        'FILE': 'FILE1',
                        'ADDITIONAL_FILES': [],
                        'DEPENDENCIES': {
                            'JOB2': {'STATUS': 'FAILED', 'ANY_FINAL_STATUS_IS_VALID': False},
                            'JOB3': {'STATUS': 'FAILED', 'ANY_FINAL_STATUS_IS_VALID': True},
                            'JOB4': {'STATUS': 'RUNNING', 'ANY_FINAL_STATUS_IS_VALID': True},
                            'JOB5': {'STATUS': 'COMPLETED', 'ANY_FINAL_STATUS_IS_VALID': False},
                            'JOB6': {'STATUS': 'SKIPPED', 'ANY_FINAL_STATUS_IS_VALID': False},
                            'JOB7': {'STATUS': 'READY', 'ANY_FINAL_STATUS_IS_VALID': False},
                            'JOB8': {'STATUS': 'DELAYED', 'ANY_FINAL_STATUS_IS_VALID': False},
                            'JOB9': {'STATUS': 'PREPARED', 'ANY_FINAL_STATUS_IS_VALID': False},
                            'JOB10': {'STATUS': 'QUEUING', 'ANY_FINAL_STATUS_IS_VALID': True},
                            'JOB11': {'STATUS': 'SUBMITTED', 'ANY_FINAL_STATUS_IS_VALID': True},
                            'JOB12': {'STATUS': 'HELD', 'ANY_FINAL_STATUS_IS_VALID': True},
                        },
                    },
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
