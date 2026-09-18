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


import pytest

from autosubmit.log.log import AutosubmitCritical


@pytest.mark.parametrize(
    "experiment_data",
    [
        {
            "TEST_SLURM": {
                "FDB_COPY_BIN": '%"CURRENT_FDB_COPY_BIN%/fdb-copy',
                "FDB_LIST_BIN": "%^CURRENT_FDB_LIST_BIN%/fdb-list",
            },
        },
        {
            "TEST_SLURM": {
                "FDB_COPY_BIN": "%CURRENT_FDB_COPY_BIN%/fdb-copy",
                "FDB_LIST_BIN": "%CURRENT_FDB_LIST_BIN%/fdb-list",
            },
        },
        {
            "TEST_SLURM": {
                "FDB_COPY_BIN": "%CURRENT_FDB_CPY_BIN%/fdb-copy",
                "FDB_LIST_BIN": "%CURRENT_FDB_LIST_BIN%/fdb-list",
            },
        },
        {
            "TEST_SLURM": {
                "TESTS": '%"CURRENT_TESTS%/fdb-copy',
                "FDB_DICT_BIN": "TEST_FOLDER/%^CURRENT_FDB_DICT_BIN%/fdb-list",
            },
        },
        {
            "TEST_SLURM": {
                "FDB_COPY_BIN": "TEST/%CURRENT_FDB_COPY_BIN%/fdb-copy",
                "FDB_LIST_BIN": "TEST/%CURRENT_FDB_LIST_BIN%/fdb-list",
            },
        },
        {
            "TEST_SLURM": {
                "FDB_LIST_BIN": "TEST/%current_FDB_LIST_BIN%/fdb-list",
            }
        },
        {
            "TEST_SLURM": {
                "fdb_list_bin": "TEST/%current_FDB_LIST_BIN%/fdb-list",
            }
        },
        {"SCRIPT": "%current_script%"},
    ],
    ids=[
        "Special Dynamic Variable",
        "Dynamic Variable",
        "Multiple Dynamic Variable",
        "Special Dynamic Variable With Reference",
        "Dynamic Variable With Reference",
        "Oneliner Dynamic Variable With Reference",
        "Scripts lower/upper",
        "Scripts upper/lower",
    ],
)
def test_dynamic_variable_deep_add_missing_starter_conf(
    autosubmit_config, general_data, experiment_data
):
    as_conf = autosubmit_config(
        "t000",
        {
            "PLATFORMS": experiment_data,
        },
    )
    with pytest.raises(AutosubmitCritical) as ac:
        as_conf.experiment_data = as_conf.deep_add_missing_starter_conf(
            general_data, as_conf.experiment_data
        )
    assert "causing infinite recursion during evaluation" in ac.value.message


@pytest.mark.parametrize(
    "new_data",
    [
        {
            "JOBS": {
                "A": {
                    "SCRIPT": "OK %TEST.TE_ME%",
                },
            },
            "TEST": {
                "NAME": "AUTOSUBMIT",
                "TE_ME": "Hi %TEST.TE_ME%",
                "CHUNK": "%CHUNK%",
            },
        },
        {
            "JOBS": {
                "A": {
                    "SCRIPT": {"TEST": "OK %TEST.TEST_ME.TE_ME%"},
                },
            },
            "TEST": {
                "NAME": "AUTOSUBMIT",
                "TEST_ME": {"TE_ME": "Hi %TEST.TEST_ME.TE_ME%", "CHUNK": "%CHUNK%"},
            },
        },
        {
            "JOBS": {
                "A": {"SCRIPT": "OK %TEST.TE_ME%", "CHUNK": "%CHUNK%"},
            },
            "TEST": {
                "NAME": "AUTOSUBMIT",
                "TE_ME": "Hi %JOBS.A.SCRIPT%",
            },
        },
        {
            "JOBS": {
                "A": {"SCRIPT": "OK %TEST.CURRENT_TE_ME%", "CHUNK": "%CHUNK%"},
            },
            "TEST": {
                "NAME": "AUTOSUBMIT",
                "TE_ME": "Hi %JOBS.A.SCRIPT%",
                "CURRENT_TE_ME": "Hi %JOBS.A.SCRIPT%",
            },
        },
    ],
    ids=[
        "Special Dynamic Variable",
        "Special Deeper Dynamic Variable",
        "Full Cycle Dynamic Variable",
        "Full Cycle Current Dynamic Variable",
    ],
)
def test_infinite_loop_dynamic_variable_unify_conf(
    autosubmit_config, mocker, new_data
):
    as_conf = autosubmit_config(
        "t000",
        {},
    )

    with pytest.raises(AutosubmitCritical) as ac:
        as_conf.unify_conf(current_data={}, new_data=new_data)
    assert "Recursion was found validating the configuration files! " in ac.value.message


@pytest.mark.parametrize(
    "new_data,result",
    [
        ({
            "JOBS": {
                "A": {
                    "SCRIPT": "OK %TEST.TE_ME%",
                    "B": 'OK'
                },
            },
            "TEST": {
                "NAME": "AUTOSUBMIT",
                "TE_ME": "Hi %JOBS.A.B%",
            },
        },{
            "JOBS": {
                "A": {
                    "SCRIPT": "OK Hi OK",
                    "B": 'OK'
                },
            },
            "TEST": {
                "NAME": "AUTOSUBMIT",
                "TE_ME": "Hi OK",
            },
        },),
        ({
            "JOBS": {
                "A": {
                    "SCRIPT": {
                        "TEST": "OK %TEST.NAME%"
                    },
                },
                "B": "Ok"
            },
            "TEST": {
                "NAME": "AUTOSUBMIT",
                "TEST_ME": {
                    "TE_ME": "Hi %JOBS.B%",
                }
            },
        },{
            "JOBS": {
                "A": {
                    "SCRIPT": {
                        "TEST": "OK AUTOSUBMIT"
                    },
                },
                "B": "Ok"
            },
            "TEST": {
                "NAME": "AUTOSUBMIT",
                "TEST_ME": {
                    "TE_ME": "Hi Ok",
                }
            },
        },),
        ({
            "JOBS": {
                "A": {
                    "SCRIPT": "OK %TEST.TEST.TE_ME%",
                    "CHUNK": "%CHUNK%"
                },
            },
            "TEST": {
                "NAME": "AUTOSUBMIT",
                "TE_ME": "Hi %JOBS.A.SCRIPT%",
            },
        },{
            "JOBS": {
                "A": {
                    "SCRIPT": "OK %TEST.TEST.TE_ME%",
                    "CHUNK": "%CHUNK%"
                },
            },
            "TEST": {
                "NAME": "AUTOSUBMIT",
                "TE_ME": "Hi OK %TEST.TEST.TE_ME%",
            },
        },),
    ],
    ids=[
        "Special Dynamic Variable",
        "Special Deeper Dynamic Variable",
        "Wrong Substitution Dynamic Variable",
    ],
)
def test_finite_loop_dynamic_variable_unify_conf(
    autosubmit_config, new_data, result
):
    as_conf = autosubmit_config(
        "t000",
        {},
    )

    parameters = as_conf.unify_conf(current_data={}, new_data=new_data)
    assert isinstance(parameters, dict)
    assert parameters == result
