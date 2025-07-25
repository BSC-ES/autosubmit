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

from pathlib import Path

import pytest
from ruamel.yaml import YAML

FOR_CONF = {
    "TEST": "VARIABLEX",
    "TEST2": "VARIABLEY",
    "VAR": ["%NOTFOUND%", "%TEST%", "%TEST2%"],
    "jobs": {
        "job": {
            "FOR": {
                "NAME": "%var%"
            },
            "path": "/home/dbeltran/conf/stuff_to_read/%NAME%/test.yml"
        }
    }
}

ONE_DIM = {
    "TEST": "VARIABLEX",
    "TEST2": "VARIABLEY",
    "VAR": ["%NOTFOUND%", "%TEST%", "%TEST2%"],
    "JOBS": {
        "JOB": {
            "VARIABLEX": "%TEST%",
            "VARIABLEY": "%TEST2%"
        },
    },
}

TEST_NESTED_DICT = {
    "TEST": "VARIABLEX",
    "TEST2": "VARIABLEY",
    "FOO": {
        "BAR": {
            "VAR": ["%NOTFOUND%", "%TEST%", "%TEST2%"],
            "VAR_STRING": "%NOTFOUND% %TEST% %TEST2%"
        }
    }
}


@pytest.mark.skip('references /home/dbeltran/...')
def test_substitute_dynamic_variables_yaml_files_short_format_for(autosubmit_config):
    as_conf = autosubmit_config(
        expid='a000',
        experiment_data=FOR_CONF)
    as_conf.experiment_data = as_conf.deep_normalize(as_conf.experiment_data)
    as_conf.dynamic_variables = {'VAR': ['%NOTFOUND%', '%TEST%', '%TEST2%']}
    as_conf.experiment_data = as_conf.substitute_dynamic_variables(as_conf.experiment_data)
    assert as_conf.experiment_data['VAR'] == ['%NOTFOUND%', 'VARIABLEX', 'VARIABLEY']


@pytest.mark.skip('references /home/dbeltran/...')
def test_substitute_dynamic_variables_yaml_files_with_for_short_format(autosubmit_config):
    as_conf = autosubmit_config(
        expid='a000',
        experiment_data=FOR_CONF)
    as_conf.experiment_data = as_conf.normalize_variables(as_conf.experiment_data, must_exists=True)
    as_conf.experiment_data = as_conf.deep_read_loops(as_conf.experiment_data)
    as_conf.experiment_data = as_conf.substitute_dynamic_variables(as_conf.experiment_data)
    as_conf.experiment_data = as_conf.parse_data_loops(as_conf.experiment_data)

    assert as_conf.experiment_data['VAR'] == ['%NOTFOUND%', 'VARIABLEX', 'VARIABLEY']
    assert as_conf.experiment_data['JOBS']['JOB_VARIABLEX'] == {'ADDITIONAL_FILES': [], 'DEPENDENCIES': {}, 'FILE': '',
                                                                 'NAME': 'VARIABLEX',
                                                                 'PATH': '/home/dbeltran/conf/stuff_to_read/VARIABLEX/test.yml'}
    assert as_conf.experiment_data['JOBS']['JOB_VARIABLEY'] == {'ADDITIONAL_FILES': [], 'DEPENDENCIES': {}, 'FILE': '',
                                                                 'NAME': 'VARIABLEY',
                                                                 'PATH': '/home/dbeltran/conf/stuff_to_read/VARIABLEY/test.yml'}
    assert as_conf.experiment_data['JOBS'].get('%NOTFOUND%', None) is None


@pytest.mark.skip('references /home/dbeltran/...')
def test_substitute_dynamic_variables_yaml_files_with_for_short_format_and_custom_config(autosubmit_config):
    as_conf = autosubmit_config(
        expid='a000',
        experiment_data=FOR_CONF)
    as_conf.experiment_data = as_conf.normalize_variables(as_conf.experiment_data, must_exists=True)
    as_conf.experiment_data = as_conf.deep_read_loops(as_conf.experiment_data)
    as_conf.experiment_data = as_conf.substitute_dynamic_variables(as_conf.experiment_data)
    as_conf.experiment_data = as_conf.parse_data_loops(as_conf.experiment_data)

    assert as_conf.experiment_data['VAR'] == ['%NOTFOUND%', 'VARIABLEX', 'VARIABLEY']
    assert as_conf.experiment_data['JOBS']['JOB_VARIABLEX'] == {'ADDITIONAL_FILES': [], 'DEPENDENCIES': {}, 'FILE': '',
                                                                 'NAME': 'VARIABLEX',
                                                                 'PATH': '/home/dbeltran/conf/stuff_to_read/VARIABLEX/test.yml'}
    assert as_conf.experiment_data['JOBS']['JOB_VARIABLEY'] == {'ADDITIONAL_FILES': [], 'DEPENDENCIES': {}, 'FILE': '',
                                                                 'NAME': 'VARIABLEY',
                                                                 'PATH': '/home/dbeltran/conf/stuff_to_read/VARIABLEY/test.yml'}
    assert as_conf.experiment_data['JOBS'].get('%NOTFOUND%', None) is None


def test_substitute_dynamic_variables_long_format(autosubmit_config):
    as_conf = autosubmit_config(
        expid='a000',
        experiment_data=ONE_DIM)
    as_conf.experiment_data = as_conf.normalize_variables(as_conf.experiment_data, must_exists=True)
    as_conf.experiment_data = as_conf.deep_read_loops(as_conf.experiment_data)
    param = as_conf.substitute_dynamic_variables()
    assert param['JOBS.JOB.VARIABLEX'] == 'VARIABLEX'
    assert param['JOBS.JOB.VARIABLEY'] == 'VARIABLEY'


def test_substitute_keys_short_strings(autosubmit_config):
    as_conf = autosubmit_config(
        expid='a000',
        experiment_data=ONE_DIM)
    result = as_conf._substitute_keys(
        ["%A%/bar/%B%"],
        ("FOO", "%A%/bar/%B%"),
        {"FOO": "%A%/bar/%B%", "A": "a", "B": "b"},
        "%[a-zA-Z0-9_.-]*%",
        1,
        "short",
        {"FOO": "%A%/bar/%B%"},
    )

    assert result == ({'FOO': 'a/bar/b'}, {'A': 'a', 'B': 'b', 'FOO': 'a/bar/b'})


def test_substitute_keys_short_strings_dict(autosubmit_config):
    as_conf = autosubmit_config(
        expid='a000',
        experiment_data=ONE_DIM)
    result = as_conf._substitute_keys(
        ["%variables.Z%/bar/%VARIABLES.Y%"],
        ("FOO", "%variables.Z%/bar/%variables.Y%"),
        {"FOO": "%variables.Z%/bar/%variables.Y%", "VARIABLES": {"Z": "z", "Y": "y"}},
        "%[a-zA-Z0-9_.-]*%",
        1,
        "short",
        {"FOO": "%variables.Z%/bar/%variables.Y%"},
    )

    assert result[0] == {'FOO': 'z/bar/y'}


def test_substitute_dynamic_variables_yaml_files_short_format_nested(autosubmit_config):
    as_conf = autosubmit_config(
        expid='a000',
        experiment_data=TEST_NESTED_DICT)
    as_conf.experiment_data = as_conf.normalize_variables(as_conf.experiment_data, must_exists=True)
    as_conf.experiment_data = as_conf.deep_read_loops(as_conf.experiment_data)
    as_conf.experiment_data = as_conf.substitute_dynamic_variables(as_conf.experiment_data)
    as_conf.experiment_data = as_conf.parse_data_loops(as_conf.experiment_data)

    assert as_conf.experiment_data['FOO']['BAR']['VAR'] == ['%NOTFOUND%', 'VARIABLEX', 'VARIABLEY']
    assert as_conf.experiment_data['FOO']['BAR']['VAR_STRING'] == '%NOTFOUND% VARIABLEX VARIABLEY'


def test_substitute_placeholders_after_all_files_loaded(autosubmit_config, tmpdir):
    """Test substitution of placeholders after all files have been loaded."""
    as_conf = autosubmit_config(
        expid='a000',
        experiment_data={}
    )
    ca_yml = {
        "model": {
            "version": "first"
        }
    }
    conf_yml = {
        "other_variable": "something",
        "test_in_place": "%other_variable%/%model.version%/%another_other_variable%",
        "test_at_the_end": "%other_variable%/%^model.version%/%another_other_variable%",
        "another_other_variable": "something"
    }
    cz_yml = {
        "model": {
            "version": "last"
        }
    }
    as_conf.conf_folder_yaml = Path(as_conf.basic_config.LOCAL_ROOT_DIR, as_conf.expid, 'conf')
    as_conf.conf_folder_yaml.mkdir(parents=True, exist_ok=True)
    ca_yaml_file = as_conf.conf_folder_yaml / "ca.yml"
    conf_yaml_file = as_conf.conf_folder_yaml / "conf.yml"
    cz_yaml_file = as_conf.conf_folder_yaml / "cz.yml"
    with open(ca_yaml_file, 'w', encoding="UTF-8") as yaml_file:
        YAML().dump(ca_yml, yaml_file)
    with open(conf_yaml_file, 'w', encoding="UTF-8") as yaml_file:
        YAML().dump(conf_yml, yaml_file)
    with open(cz_yaml_file, 'w', encoding="UTF-8") as yaml_file:
        YAML().dump(cz_yml, yaml_file)
    as_conf = autosubmit_config(
        expid='a000',
        experiment_data={}
    )

    as_conf.reload()

    assert 'TEST_IN_PLACE' not in as_conf.experiment_data
    assert 'TEST_AT_THE_END' not in as_conf.experiment_data

    # Load the YAML files
    as_conf.reload(force_load=True)

    assert as_conf.experiment_data["TEST_IN_PLACE"] == "something/first/something"
    assert as_conf.experiment_data["TEST_AT_THE_END"] == "something/last/something"
