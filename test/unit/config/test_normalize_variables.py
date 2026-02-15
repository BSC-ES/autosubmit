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

from autosubmit.log.log import AutosubmitCritical


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
                        "JOBS_IN_WRAPPER": ["JOB1", "JOB2"],
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
                            "job2": {"MIN_TRIGGER_STATUS": "FAILED"},
                            "job3": {"MIN_TRIGGER_STATUS": "FAILED?"},
                            "job4": {"MIN_TRIGGER_STATUS": "RUNNING"},
                            "job5": {"MIN_TRIGGER_STATUS": "COMPLETED"},
                            "job6": {"MIN_TRIGGER_STATUS": "SKIPPED"},
                            "job7": {"MIN_TRIGGER_STATUS": "READY"},
                            "job8": {"MIN_TRIGGER_STATUS": "DELAYED"},
                            "job9": {"MIN_TRIGGER_STATUS": "PREPARED"},
                            "job10": {"MIN_TRIGGER_STATUS": "QUEUING"},
                            "job11": {"MIN_TRIGGER_STATUS": "SUBMITTED"},
                            "job12": {"MIN_TRIGGER_STATUS": "HELD"},
                            "job13": {"MIN_TRIGGER_STATUS": "RUNNING?"},
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
                            'JOB2': {'MIN_TRIGGER_STATUS': 'FAILED', 'FAIL_OK': False},
                            'JOB3': {'MIN_TRIGGER_STATUS': 'FAILED', 'FAIL_OK': True},
                            'JOB4': {'MIN_TRIGGER_STATUS': 'RUNNING', 'FAIL_OK': False},
                            'JOB5': {'MIN_TRIGGER_STATUS': 'COMPLETED', 'FAIL_OK': False},
                            'JOB6': {'MIN_TRIGGER_STATUS': 'SKIPPED', 'FAIL_OK': False},
                            'JOB7': {'MIN_TRIGGER_STATUS': 'READY', 'FAIL_OK': False},
                            'JOB8': {'MIN_TRIGGER_STATUS': 'DELAYED', 'FAIL_OK': False},
                            'JOB9': {'MIN_TRIGGER_STATUS': 'PREPARED', 'FAIL_OK': False},
                            'JOB10': {'MIN_TRIGGER_STATUS': 'QUEUING', 'FAIL_OK': False},
                            'JOB11': {'MIN_TRIGGER_STATUS': 'SUBMITTED', 'FAIL_OK': False},
                            'JOB12': {'MIN_TRIGGER_STATUS': 'HELD', 'FAIL_OK': False},
                            'JOB13': {'MIN_TRIGGER_STATUS': 'RUNNING', 'FAIL_OK': True},
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


def test_normalize_wrappers_jobs_in_wrapper(autosubmit_config):
    input_data = {
        "JOBS": {
            "JOB1": {},
            "JOB2": {},
            "JOB3": {},
            "JOB4": {},
            "JOB5": {},
            "JOB6": {},
            "JOB7": {},
            "JOB8": {},
            "JOB9": {},
            "JOB10": {},
        },
        'WRAPPERS': {
            "WRAPPER1": {
                "JOBS_IN_WRAPPER": "job1 job2",
                "TYPE": "VERTICAL"
            },
            "WRAPPER2": {
                "JOBS_IN_WRAPPER": "job3&job4",
                "TYPE": "HORIZONTAL"
            },
            "WRAPPER3": {
                "JOBS_IN_WRAPPER": "job5, job6",
                "TYPE": "HORIZONTAL"
            },
            "WRAPPER4": {
                "JOBS_IN_WRAPPER": ["job7", "job8"],
                "TYPE": "VERTICAL"
            },
            "WRAPPER5": {
                "JOBS_IN_WRAPPER": "[job9, job10]",
                "TYPE": "HORIZONTAL"
            },
        }
    }

    expected_data = {
        "WRAPPER1": {
            "JOBS_IN_WRAPPER": ["JOB1", "JOB2"],
            "TYPE": "vertical"
        },
        "WRAPPER2": {
            "JOBS_IN_WRAPPER": ["JOB3", "JOB4"],
            "TYPE": "horizontal"
        },
        "WRAPPER3": {
            "JOBS_IN_WRAPPER": ["JOB5", "JOB6"],
            "TYPE": "horizontal"
        },
        "WRAPPER4": {
            "JOBS_IN_WRAPPER": ["JOB7", "JOB8"],
            "TYPE": "vertical"
        },
        "WRAPPER5": {
            "JOBS_IN_WRAPPER": ["JOB9", "JOB10"],
            "TYPE": "horizontal"
        },
    }

    as_conf = autosubmit_config(expid='t000', experiment_data=input_data)
    as_conf._normalize_wrappers_section(input_data, raise_exception=True)
    assert as_conf.experiment_data["WRAPPERS"] == expected_data
    as_conf._normalize_wrappers_section(input_data, raise_exception=True)
    assert as_conf.experiment_data["WRAPPERS"] == expected_data


@pytest.mark.parametrize(
    "wrappers",
    [
        pytest.param({"WRAPPER1": {"JOBS_IN_WRAPPER": {"bla": "bla"}, "TYPE": "VERTICAL"}}, id="mapping_not_allowed"),
        pytest.param({"WRAPPER2": {"JOBS_IN_WRAPPER": 12345, "TYPE": "VERTICAL"}}, id="non_string_single_value"),
        pytest.param({"WRAPPER3": {"JOBS_IN_WRAPPER": None, "TYPE": "VERTICAL"}}, id="none_value"),
        pytest.param({"WRAPPER4": {"TYPE": "VERTICAL"}}, id="missing_key"),
        pytest.param({"WRAPPER5": {"JOBS_IN_WRAPPER": ["JOB1", "NON_EXISTENT_JOB"], "TYPE": "VERTICAL"}}, id="unknown_job"),
        pytest.param({"WRAPPER6": {"JOBS_IN_WRAPPER": [], "TYPE": "VERTICAL"}}, id="empty_list"),
        pytest.param({"WRAPPER7": {"JOBS_IN_WRAPPER": ["", " "], "TYPE": "VERTICAL"}}, id="blank_entries"),
        pytest.param({"WRAPPER8": {"JOBS_IN_WRAPPER": ["JOB1", 123]}, "TYPE": "VERTICAL"}, id="non_string_job_name"),
        pytest.param({"WRAPPER9": {"JOBS_IN_WRAPPER": ["JOB1"]}}, id="type_variable_not_found"),
    ],
)
def test_normalize_wrappers_raise_error(autosubmit_config, wrappers):
    input_data = {
        "JOBS": {"JOB1": {}, "JOB2": {}},
        "WRAPPERS": wrappers,
    }

    as_conf = autosubmit_config(expid='t000', experiment_data=input_data)
    with pytest.raises(AutosubmitCritical):
        as_conf._normalize_wrappers_section(input_data, raise_exception=True)
