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

import cProfile
import pstats
import shutil
from pathlib import Path
from typing import Dict, Any
from unittest.mock import MagicMock

import pytest
from ruamel.yaml import YAML

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.config.configcommon import AutosubmitConfig
from test.regression.config.conftest import prepare_yaml_files

PROFILE = False  # Enable/disable profiling ( speed up the tests )

as_conf_content: Dict[str, Any] = {
    "job": {
        "FOR": {
            "NAME": "%var%"
        },
        "path": "TOFILL"
    },
    "test": "variableX",
    "test2": "variableY",
    "test3": "variableZ",
    "var": [
        "%hola%",
        "%test%",
        "%test2%",
        "%test3%",
        "variableW"
    ],
    "DEFAULT": {
        "EXPID": "a000",
        "HPCARCH": "local",
        "CUSTOM_CONFIG": {
            "PRE": [
                "%job_variableX.path%",
                "%job_variableY.path%",
                "%job_variableZ.path%",
                "%job_variableW.path%"

            ]
        }
    },
    "Jobs": {
        "test": {
            "file": "test.sh"
        }
    }
}


def prepare_custom_config_tests(default_yaml_file: Dict[str, Any], project_yaml_files: Dict[str, Dict[str, str]],
                                temp_folder: Path) -> Dict[str, Any]:
    """
    Prepare custom configuration tests by creating necessary YAML files.

    :param default_yaml_file: Default YAML file content.
    :type default_yaml_file: Dict[str, Any]
    :param project_yaml_files: Dictionary of project YAML file paths and their content.
    :type project_yaml_files: Dict[str, Dict[str, str]]
    :param temp_folder: Temporary folder .
    :type temp_folder: Path
    :return: Updated default YAML file content.
    :rtype: Dict[str, Any]
    """
    yaml_file_path = Path(f"{str(temp_folder)}/test_exp_data.yml")
    for path, content in project_yaml_files.items():
        test_file_path = Path(f"{str(temp_folder)}{path}")
        test_file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(test_file_path, "w") as f:
            f.write(str(content))
    default_yaml_file["job"]["path"] = f"{str(temp_folder)}/%NAME%/test.yml"
    with yaml_file_path.open("w") as f:
        f.write(str(default_yaml_file))
    return default_yaml_file


def deep_check_all_keys_uppercase(data: dict) -> bool:
    """
    Recursively check if all keys in the nested dictionary are uppercase.

    :param data: The dictionary to check.
    :return: True if all keys are uppercase, False otherwise.
    """
    for key, value in data.items():
        if not key.isdigit() and not key.isupper():
            return False
        if isinstance(value, dict) and not deep_check_all_keys_uppercase(value):
            return False
    return True


@pytest.mark.parametrize("default_yaml_file, project_yaml_files, expected_data",
                         [(as_conf_content,
                           {"/variableX/test.yml": {"varX": "a_test"},
                            "/variableY/test.yml": {"varY": "a_test"},
                            "/variableZ/test.yml": {"varZ": "%test3%"},
                            "/variableW/test.yml": {"varW": "%varZ%"}},
                           {"VARX": "a_test",
                            "VARY": "a_test",
                            "VARZ": "variableZ",
                            "VARW": "variableZ",
                            "JOB_VARIABLEX_PATH": "variableX/test.yml",
                            "JOB_VARIABLEY_PATH": "variableY/test.yml"})])
def test_custom_config_for(temp_folder: Path, default_yaml_file: Dict[str, Any],
                           project_yaml_files: Dict[str, Dict[str, str]], expected_data: Dict[str, str],
                           mocker) -> None:
    """
    Test custom configuration and "FOR" for the given YAML files.

    :param temp_folder: Temporary folder path.
    :type temp_folder: Path
    :param default_yaml_file: Default YAML file content.
    :type default_yaml_file: Dict[str, Any]
    :param project_yaml_files: Dictionary of project YAML file paths and their content.
    :type project_yaml_files: Dict[str, Dict[str, str]]
    :param expected_data: Expected data for validation.
    :type expected_data: Dict[str, str]
    :param mocker: Mocker fixture for patching.
    :type mocker: Any
    """
    mocker.patch('pathlib.Path.exists', return_value=True)
    default_yaml_file = prepare_custom_config_tests(default_yaml_file, project_yaml_files, temp_folder)
    prepare_yaml_files(default_yaml_file, temp_folder)
    as_conf = AutosubmitConfig("test")
    as_conf.conf_folder_yaml = Path(temp_folder)
    as_conf.load_workflow_commit = MagicMock()
    as_conf.reload(True)
    for file_name in project_yaml_files.keys():
        assert temp_folder / file_name in as_conf.current_loaded_files.keys()
    assert as_conf.experiment_data["VARX"] == expected_data["VARX"]
    assert as_conf.experiment_data["VARY"] == expected_data["VARY"]
    assert as_conf.experiment_data["JOB_VARIABLEX"]["PATH"] == str(temp_folder / expected_data["JOB_VARIABLEX_PATH"])
    assert as_conf.experiment_data["JOB_VARIABLEY"]["PATH"] == str(temp_folder / expected_data["JOB_VARIABLEY_PATH"])
    assert as_conf.experiment_data["VARZ"] == expected_data["VARZ"]
    assert as_conf.experiment_data["VARW"] == expected_data["VARW"]

    # check that all variables are in upper_case
    assert deep_check_all_keys_uppercase(as_conf.experiment_data)


@pytest.fixture()
def prepare_basic_config(temp_folder):
    basic_conf = BasicConfig()
    BasicConfig.DB_DIR = (temp_folder / "DestinE_workflows")
    BasicConfig.DB_FILE = "as_times.db"
    BasicConfig.LOCAL_ROOT_DIR = (temp_folder / "DestinE_workflows")
    BasicConfig.LOCAL_TMP_DIR = "tmp"
    BasicConfig.LOCAL_ASLOG_DIR = "ASLOGS"
    BasicConfig.LOCAL_PROJ_DIR = "proj"
    BasicConfig.DEFAULT_PLATFORMS_CONF = ""
    BasicConfig.CUSTOM_PLATFORMS_PATH = ""
    BasicConfig.DEFAULT_JOBS_CONF = ""
    BasicConfig.SMTP_SERVER = ""
    BasicConfig.MAIL_FROM = ""
    BasicConfig.ALLOWED_HOSTS = ""
    BasicConfig.DENIED_HOSTS = ""
    BasicConfig.CONFIG_FILE_FOUND = False
    return basic_conf


def check_differences(data1: dict, data2: dict) -> list:
    """
    check differences between two planned as_conf.experiment_data dictionaries.

    :param data1: First dictionary to compare (actual).
    :param data2: Second dictionary to compare (reference) .
    :return: List of differences in the format (key, value1, value2).
    """
    differences = []

    for key in data1.keys() | data2.keys():
        value1 = data1.get(key, "NOT FOUND in actual_data")
        value2 = data2.get(key, "NOT FOUND in reference")
        if "pytest" in str(value1) or "pytest" in str(value2):
            continue
        if value1 != value2:
            differences.append((key, value1, value2))

    return differences


def test_destine_workflows(temp_folder: Path, mocker, prepare_basic_config: Any) -> None:
    """
    Test the destine workflow (a1q2) hardcoded until CI/CD.
    """
    import os
    profiler = cProfile.Profile()
    os.environ["AS_ENV_PLATFORMS_PATH"] = "test"
    os.environ["AS_ENV_SSH_CONFIG_PATH"] = "test2"
    os.environ["SUDO_USER"] = "dummy"
    expid = "a000"  # TODO parametrize
    mocker.patch.object(BasicConfig, 'read', return_value=True)
    current_script_location = Path(__file__).resolve().parent
    experiments_root = Path(f"{current_script_location}/DestinE_workflows")

    temp_folder_experiments_root = Path(f"{temp_folder}/DestinE_workflows")
    temp_folder_experiments_root.parent.mkdir(parents=True, exist_ok=True)
    # copy experiment files
    shutil.copytree(experiments_root, temp_folder_experiments_root)
    as_conf = AutosubmitConfig(expid, prepare_basic_config)
    if PROFILE:
        profiler.enable()
    as_conf.reload(True)
    for l_file in as_conf.current_loaded_files.keys():
        print(l_file)
    if PROFILE:
        profiler.disable()
    # Check if the files are loaded
    assert len(as_conf.current_loaded_files) > 1
    # Load reference files
    reference_experiment_data_path = Path(
        f"{current_script_location}/DestinE_workflows/{expid}/ref/experiment_data.yml")

    with reference_experiment_data_path.open('r') as f:
        yaml_loader = YAML(typ='safe')
        reference_experiment_data = yaml_loader.load(f)

    # Skip some data that depends on the environment
    for key in ["ROOTDIR", "PROJDIR", "CUSTOM_CONFIG", "PLATFORMS", "AS_TEMP"]:
        as_conf.experiment_data.pop(key, None)
        reference_experiment_data.pop(key, None)

    parameters = as_conf.deep_parameters_export(as_conf.experiment_data, as_conf.default_parameters)
    for key in list(parameters.keys()):
        if key.endswith(".NAME") and not key.startswith(
                "MODEL"):  # Added in this branch, so it is not in the reference file, the model.NAME has to not be hardcoded #todo
            parameters.pop(key)

    parameters_ref = as_conf.deep_parameters_export(reference_experiment_data, as_conf.default_parameters)
    list_of_differences = check_differences(parameters, parameters_ref)
    basic_parameters = BasicConfig().props()
    # TODO Reference File has to be updated
    list_of_differences = [(key, value, reference) for key, value, reference in list_of_differences if
                           key not in basic_parameters and not isinstance(value, MagicMock) and not key.startswith(
                               "HPC")]

    if list_of_differences:
        print("\n")
        print("Experiment data")
        for key, value in as_conf.experiment_data.items():
            print(f"\n---Key---: {key}\n Value: {value}")
        print("=====================================================")
        print("\nKeys with different values experiment_data -> reference")
        for key, value, reference in list_of_differences:
            print(f"\n---Key---: {key}\n Value: {value}\n Reference: {reference}")

    parameters = as_conf.deep_parameters_export(as_conf.experiment_data, as_conf.default_parameters)

    # Check that all parameters are being substituted
    parameters_values = ' '.join(map(str, parameters.values()))
    import re
    placeholders = re.findall(r"%\w+%", parameters_values)
    placeholders_in_parameters = [placeholder for placeholder in placeholders if
                                  placeholder.strip("%") in parameters.keys()]
    assert not placeholders_in_parameters

    # Check that all keys are in upper_case
    assert deep_check_all_keys_uppercase(as_conf.experiment_data)

    # check that all files are well set
    for job in as_conf.experiment_data["JOBS"].values():
        if "FILE" in job:
            assert job["FILE"] != ""
        else:
            assert False  # All jobs should have a file in a real experiment

    for job in as_conf.experiment_data["JOBS"].values():
        assert "ADDITIONAL_FILES" in job

    assert list_of_differences == []

    if PROFILE:
        stats = pstats.Stats(profiler).sort_stats('cumtime')
        stats.print_stats()
