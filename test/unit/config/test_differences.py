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

from autosubmit.config.configcommon import AutosubmitConfig

"""Tests for the configuration diff functions."""


def test_detailed_deep_diff(as_conf_small: AutosubmitConfig) -> None:
    """Test the ``AutosubmitConfig`` detailed difference function."""
    new_data = {
        "CONFIG":
            {
                "AUTOSUBMIT_VERSION": "4.1.0",
                "MAXWAITINGJOBS": 2,
                "TOTALJOBS": 2,
                "SAFETYSLEEPTIME": 10,
                "TEST": 1
            }
    }
    differences = as_conf_small.detailed_deep_diff(new_data, as_conf_small.experiment_data, {})
    assert differences['CONFIG']['TEST'] == {"test_value": 1}


def test_detailed_deep_imitate_autosubmit_usage(as_conf_large: AutosubmitConfig) -> None:
    """Test a real use case of the Autosubmit diff functions.

    The config_parser does a quick_diff of all data to detected if there are changes.
    If there are changes, this function is called inside Autosubmit with the following args::

        as_conf.detailed_deep_diff:
        args:
            - a section in current_loaded data. The sections to look for are:
            EXPERIMENT (any)
            CONFIG ( only interested in VERSION )
            DEFAULT ( HPCARCH )
            JOBS ( any ) and if jobs is modified:
            JOBS.SECTION.name ( DEPENDENCIES )

    The same section in the previous run alias:
    `( conf/metadata/experiment_data.yml ) last_experiment_data`.

    The function returns a dictionary with the differences between the two sections.
    """
    # Testing Experiment changes
    # We reset the value here to 10, as the fixture sets it to 0 to execute real experiments
    # faster, when needed.
    as_conf_large.experiment_data['CONFIG']['SAFETYSLEEPTIME'] = 10
    as_conf_large.last_experiment_data = as_conf_large.experiment_data
    changes = {
        "EXPERIMENT": as_conf_large.detailed_deep_diff(
            as_conf_large.experiment_data.get("EXPERIMENT", {}),
            as_conf_large.last_experiment_data.get("EXPERIMENT", {})),
        "CONFIG": as_conf_large.detailed_deep_diff(
            as_conf_large.experiment_data.get("CONFIG", {}),
            as_conf_large.last_experiment_data.get("CONFIG", {})),
        "DEFAULT": as_conf_large.detailed_deep_diff(
            as_conf_large.experiment_data.get("DEFAULT", {}),
            as_conf_large.last_experiment_data.get("DEFAULT", {})),
        "JOBS": as_conf_large.detailed_deep_diff(
            as_conf_large.experiment_data.get("JOBS", {}),
            as_conf_large.last_experiment_data.get("JOBS", {}))
    }
    assert changes == {"EXPERIMENT": {}, "CONFIG": {}, "DEFAULT": {}, "JOBS": {}}

    # change experiment
    as_conf_large.last_experiment_data = {
        "EXPERIMENT":
            {"DATELIST": "20000101", "MEMBERS": "fc0",
             "CHUNKSIZEUNIT": "month", "CHUNKSIZE": "4", "NUMCHUNKS": "10",
             "CHUNKINI": "", "CALENDAR": "standard"},
        "DEFAULT":
            {"EXPID": "a02j", "HPCARCH": "marenostrum5"},  # was marenostrum4
        "CONFIG":
            {"AUTOSUBMIT_VERSION": "4.1.0", "MAXWAITINGJOBS": 2,
             "TOTALJOBS": 40, "SAFETYSLEEPTIME": 50, "RETRIALS": 0},  # was totaljobs 2, safetysleeptime 10
        "JOBS": {
            "DN":
                {"DEPENDENCIES": {"DN": {
                    "SPLITS_FROM": {"ALL": {"SPLITS_TO": "dummy"}}},  # was splits_to previous
                    "SIM": {"STATUS": "RUNNING"}},
                    "FILE": "templates/dn.sh",
                    "PLATFORM": "marenostrum4-login", "RUNNING": "chunk",
                    "SPLITS": 31, "WALLCLOCK": "00:15",
                    "ADDITIONAL_FILES": ["conf/mother_request.yml"]},
            "INI_RENAMED":  # was INI
                {"DEPENDENCIES": {"REMOTE_SETUP": {}},
                 "FILE": "templates/ini.sh",
                 "PLATFORM": "marenostrum4-login", "RUNNING": "member",
                 "WALLCLOCK": "00:30", "ADDITIONAL_FILES": []},
            "LOCAL_SETUP": {"FILE": "templates/local_setup.sh",
                            "PLATFORM": "LOCAL",
                            "RUNNING": "once", "DEPENDENCIES": {},
                            "ADDITIONAL_FILES": []},
            "REMOTE_SETUP": {"DEPENDENCIES": {"SYNCHRONIZE": {}},
                             "FILE": "templates/remote_setup.sh",
                             "PLATFORM": "marenostrum4-login",
                             "RUNNING": "once",
                             "WALLCLOCK": "02:00",
                             "ADDITIONAL_FILES": [
                                 "templates/fdb/confignative.yaml",
                                 "templates/fdb/configregularll.yaml",
                                 "templates/fdb/confighealpix.yaml"]},
            "SIM": {"DEPENDENCIES": {"INI": {}, "SIM-1": {}},
                    "FILE": "<to-be-replaced-by-user-conf>",
                    "PLATFORM": "marenostrum4",
                    "WALLCLOCK": "00:30", "RUNNING": "chunk",
                    "ADDITIONAL_FILES": []},
            "SYNCHRONIZE": {"DEPENDENCIES": {"LOCAL_SETUP": {}},
                            "FILE": "templates/synchronize.sh",
                            "PLATFORM": "LOCAL",
                            "RUNNING": "once",
                            "ADDITIONAL_FILES": []},
            "APP_MHM": {
                "DEPENDENCIES": {"OPA_MHM_1": {"SPLITS_FROM": {"ALL": {"SPLITS_TO": "[1:31]*\\1"}}},
                                 "OPA_MHM_2": {"SPLITS_FROM": {"ALL": {"SPLITS_TO": "[1:31]*\\1"}}}},
                "FILE": "templates/application.sh", "PLATFORM": "marenostrum4", "RUNNING": "chunk",
                "WALLCLOCK": "00:05", "SPLITS": "31", "ADDITIONAL_FILES": ["templates/only_lra.yaml"]},
            "APP_URBAN": {
                "DEPENDENCIES": {"OPA_URBAN_1": {"SPLITS_FROM": {"ALL": {"SPLITS_TO": "[1:31]*\\1"}}}},
                "FILE": "templates/application.sh", "PLATFORM": "marenostrum4", "RUNNING": "chunk",
                "WALLCLOCK": "00:05", "SPLITS": "31", "ADDITIONAL_FILES": ["templates/only_lra.yaml"]},
            "OPA_MHM_1": {
                "DEPENDENCIES": {"DN": {"SPLITS_FROM": {"ALL": {"SPLITS_TO": "[1:31]*\\1"}}},
                                 "OPA_MHM_1": {"SPLITS_FROM": {"ALL": {"SPLITS_TO": "previous"}}}},
                "FILE": "templates/opa.sh", "PLATFORM": "marenostrum4", "RUNNING": "chunk", "SPLITS": "31",
                "ADDITIONAL_FILES": []},
            "OPA_MHM_2": {
                "DEPENDENCIES": {"DN": {"SPLITS_FROM": {"ALL": {"SPLITS_TO": "[1:31]*\\1"}}},
                                 "OPA_MHM_2": {"SPLITS_FROM": {"ALL": {"SPLITS_TO": "previous"}}}},
                "FILE": "templates/opa.sh", "PLATFORM": "marenostrum4", "RUNNING": "chunk", "SPLITS": "31",
                "ADDITIONAL_FILES": []},
            "OPA_URBAN_1": {
                "DEPENDENCIES": {"DN": {"SPLITS_FROM": {"ALL": {"SPLITS_TO": "[1:31]*\\1"}}},
                                 "OPA_URBAN_1": {"SPLITS_FROM": {"ALL": {"SPLITS_TO": "previous"}}}},
                "FILE": "templates/opa.sh", "PLATFORM": "marenostrum4", "RUNNING": "chunk", "SPLITS": "31",
                "ADDITIONAL_FILES": []},
            "NEW_JOB":  # added
                {"DEPENDENCIES": {"INI": {}, "SIM-1": {}},
                 "FILE": "<to-be-replaced-by-user-conf>",
                 "PLATFORM": "marenostrum4", "WALLCLOCK": "00:30",
                 "RUNNING": "chunk", "ADDITIONAL_FILES": []}
        }
    }
    # Changes
    changes = {
        "EXPERIMENT": as_conf_large.detailed_deep_diff(
            as_conf_large.experiment_data.get("EXPERIMENT", {}),
            as_conf_large.last_experiment_data.get("EXPERIMENT", {})),
        "CONFIG": as_conf_large.detailed_deep_diff(
            as_conf_large.experiment_data.get("CONFIG", {}),
            as_conf_large.last_experiment_data.get("CONFIG", {})),
        "DEFAULT": as_conf_large.detailed_deep_diff(
            as_conf_large.experiment_data.get("DEFAULT", {}),
            as_conf_large.last_experiment_data.get("DEFAULT", {})),
        "JOBS": as_conf_large.detailed_deep_diff(
            as_conf_large.experiment_data.get("JOBS", {}),
            as_conf_large.last_experiment_data.get("JOBS", {}))
    }

    expected_changes = {
        "EXPERIMENT": {"NUMCHUNKS": "2"},
        "CONFIG": {"SAFETYSLEEPTIME": 10, "TOTALJOBS": 2},
        "DEFAULT": {"HPCARCH": "marenostrum4"},
        "JOBS": {
            # DN because it was modified.
            "DN": {"DEPENDENCIES": {"DN": {"SPLITS_FROM": {"ALL": {"SPLITS_TO": "dummy"}}}}},
            # INI because it was removed (renamed to INI_RENAMED), only in the last.
            "INI": {"ADDITIONAL_FILES": [], "DEPENDENCIES": {"REMOTE_SETUP": {}},
                    "FILE": "templates/ini.sh", "PLATFORM": "marenostrum4-login", "RUNNING": "member",
                    "WALLCLOCK": "00:30"},
            # INI_RENAMED as this is new (was INI, renamed).
            "INI_RENAMED": {"ADDITIONAL_FILES": [], "DEPENDENCIES": {"REMOTE_SETUP": {}},
                            "FILE": "templates/ini.sh", "PLATFORM": "marenostrum4-login",
                            "RUNNING": "member", "WALLCLOCK": "00:30"},
            # NEW_JOB is new as by its name.
            "NEW_JOB": {"ADDITIONAL_FILES": [], "DEPENDENCIES": {"INI": {}, "SIM-1": {}},
                        "FILE": "<to-be-replaced-by-user-conf>", "PLATFORM": "marenostrum4",
                        "RUNNING": "chunk", "WALLCLOCK": "00:30"},
        }
    }

    assert changes == expected_changes
