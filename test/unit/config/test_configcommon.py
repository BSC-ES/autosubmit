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

"""Basic tests for ``AutosubmitConfig``."""

from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING

import pytest

from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.log.log import AutosubmitCritical, AutosubmitError

if TYPE_CHECKING:
    from test.unit.conftest import AutosubmitConfigFactory


def test_get_submodules_list_default_empty_list(autosubmit_config: 'AutosubmitConfigFactory'):
    """If nothing is provided, we get a list with an empty string."""
    as_conf: AutosubmitConfig = autosubmit_config(expid='a000', experiment_data={})
    submodules_list = as_conf.get_submodules_list()
    assert submodules_list == ['']


def test_get_submodules_list_returns_false(autosubmit_config: 'AutosubmitConfigFactory'):
    """If the user provides a boolean ``False``, we return that value.

    This effectively disables submodules. See issue https://earth.bsc.es/gitlab/es/autosubmit/-/issues/1130.
    """
    as_conf: AutosubmitConfig = autosubmit_config(expid='a000', experiment_data={
        'GIT': {
            'PROJECT_SUBMODULES': False
        }
    })
    submodules_list = as_conf.get_submodules_list()
    assert submodules_list is False


def test_get_submodules_true_not_valid_value(autosubmit_config: 'AutosubmitConfigFactory'):
    """If nothing is provided, we get a list with an empty string."""
    # TODO: move this to configuration validator when we have that...
    as_conf: AutosubmitConfig = autosubmit_config(expid='a000', experiment_data={
        'GIT': {
            'PROJECT_SUBMODULES': True
        }
    })
    with pytest.raises(ValueError) as cm:
        as_conf.get_submodules_list()

    assert str(cm.value) == 'GIT.PROJECT_SUBMODULES must be false (bool) or a string'


def test_get_submodules(autosubmit_config: 'AutosubmitConfigFactory'):
    """A string separated by spaces is returned as a list."""
    as_conf: AutosubmitConfig = autosubmit_config(expid='a000', experiment_data={
        'GIT': {
            'PROJECT_SUBMODULES': "sub_a sub_b sub_c sub_d"
        }
    })
    submodules_list = as_conf.get_submodules_list()
    assert isinstance(submodules_list, list)
    assert len(submodules_list) == 4
    assert "sub_b" in submodules_list


@pytest.mark.parametrize('owner', [True, False])
def test_is_current_real_user_owner(owner: bool, autosubmit_config: 'AutosubmitConfigFactory'):
    as_conf = autosubmit_config(expid='a000', experiment_data={})
    as_conf.experiment_data = as_conf.load_common_parameters(as_conf.experiment_data)
    if owner:
        as_conf.experiment_data["AS_ENV_CURRENT_USER"] = Path(as_conf.experiment_data['ROOTDIR']).owner()
    else:
        as_conf.experiment_data["AS_ENV_CURRENT_USER"] = "dummy"
    assert as_conf.is_current_real_user_owner == owner


def test_clean_dynamic_variables(autosubmit_config: 'AutosubmitConfigFactory') -> None:
    """
    This tests that only dynamic variables are kept in the ``dynamic_variables`` dictionary.
    A dynamic variable is a variable that its value is a string that starts with ``%^`` or
    ``%`` and ends with ``%``.

    These tests that once called with the right arguments, ``clean_dynamic_variables`` will
    leave ``as_conf.dynamic_variables`` with only dynamic variables.
    """

    as_conf: AutosubmitConfig = autosubmit_config(expid='a000', experiment_data={})
    _, pattern, _ = as_conf._initialize_variables()

    as_conf.dynamic_variables = {
        'popeye_eats': 'spinach',
        'penguin_eats': 'fish',
        'thor_eats': '%^DYNAMIC_1%',
        'floki_eats': '%^DYNAMIC_2%',
        'jaspion_eats': '%PLACEHOLDER%'
    }

    as_conf.clean_dynamic_variables(pattern)

    assert len(as_conf.dynamic_variables) == 1
    assert 'jaspion_eats' in as_conf.dynamic_variables


def test_yaml_deprecation_warning(tmp_path, autosubmit_config: 'AutosubmitConfigFactory'):
    """Test that the conversion from YAML to INI works as expected, without warnings.

    Creates a dummy AS3 INI file, calls ``AutosubmitConfig.ini_to_yaml``, and
    verifies that the YAML files exist and are not empty, and a backup file was
    created. All without warnings being raised (i.e., they were suppressed).
    """
    as_conf: AutosubmitConfig = autosubmit_config(expid='a000', experiment_data={})
    ini_file = tmp_path / 'a000_jobs.ini'
    with open(ini_file, 'w+') as f:
        f.write(dedent('''\
            [LOCAL_SETUP]
            FILE = LOCAL_SETUP.sh
            PLATFORM = LOCAL
            '''))
        f.flush()
    as_conf.ini_to_yaml(root_dir=tmp_path, ini_file=ini_file)

    backup_file = Path(f'{ini_file}_AS_v3_backup')
    assert backup_file.exists()
    assert backup_file.stat().st_size > 0

    new_yaml_file = Path(ini_file.parent, ini_file.stem).with_suffix('.yml')
    assert new_yaml_file.exists()
    assert new_yaml_file.stat().st_size > 0


def test_key_error_raise(autosubmit_config: 'AutosubmitConfigFactory'):
    """Test that a KeyError is raised when a key is not found in the configuration."""
    as_conf: AutosubmitConfig = autosubmit_config(expid="a000", experiment_data=None)
    # We need to set it here again, as the fixture prevents ``experiment_data``
    # from being ``None``.
    as_conf.experiment_data = None

    with pytest.raises(AutosubmitCritical):
        _ = as_conf.jobs_data

    with pytest.raises(AutosubmitCritical):
        _ = as_conf.platforms_data

    with pytest.raises(AutosubmitCritical):
        as_conf.get_platform()

    as_conf.experiment_data = {
        "JOBS": {"SIM": {}},
        "PLATFORMS": {"LOCAL": {}},
        "DEFAULT": {"HPCARCH": "DUMMY"},
    }

    assert as_conf.jobs_data == {"SIM": {}}
    assert as_conf.platforms_data == {"LOCAL": {}}
    assert as_conf.get_platform() == "DUMMY"


@pytest.mark.parametrize(
    'error,expected',
    [
        [IOError, AutosubmitError],
        [AutosubmitCritical, AutosubmitCritical],
        [AutosubmitError, AutosubmitError],
        [ValueError, AutosubmitCritical]
    ]
)
def test_check_conf_files_errors(error: Exception, expected: Exception,
                                 autosubmit_config: 'AutosubmitConfigFactory', mocker):
    """Test errors when calling ``check_conf_files()``."""
    as_conf: AutosubmitConfig = autosubmit_config(expid="a000", experiment_data=None)

    mocker.patch.object(as_conf, 'reload', side_effect=error)
    with pytest.raises(expected):  # type: ignore[arg-type]
        as_conf.reload.side_effect = as_conf.check_conf_files()


@pytest.mark.parametrize(
    'experiment_job,expected',
    [
        [{
            'JOBS': {
                'A': {
                    'RUNNING': 'once',
                    'FILE': 'test.sh'
                }
            }
        }, False],
        [{
            'JOBS': {
                'A': {
                    'RUNNING': 'once',
                    'FILE': 'test.sh',
                    'CHECK': 'False',
                }
            }
        }, False],
        [{
            'JOBS': {
                'A': {
                    'RUNNING': 'once',
                    'FILE': 'test.sh',
                    'CHECK': 'ON_SUBMISSION',
                }
            }
        }, True],
        [{
            'JOBS': {
                'A': {
                    'SCRIPT': '',
                    'RUNNING': 'once',
                    'FILE': 'test.sh',
                    'RERUN_DEPENDENCIES': 'RUNNING A'
                }
            }
        }, True]
    ]
)
def test_set_version(autosubmit_config: 'AutosubmitConfigFactory', experiment_job, expected):
    as_conf: AutosubmitConfig = autosubmit_config(expid="a000", experiment_data=experiment_job)
    as_conf.ignore_file_path = True
    assert as_conf.check_jobs_conf() == expected


@pytest.mark.parametrize(
    'experiment_data, raise_error',
    [
        [{}, False],
        [{'CONFIG': {"SAFE_PLACEHOLDERS": "some"}}, False],
        [{'CONFIG': {"SAFE_PLACEHOLDERS": "some some"}}, False],
        [{'CONFIG': {"SAFE_PLACEHOLDERS": "some, some"}}, False],
        [{'CONFIG': {"SAFE_PLACEHOLDERS": ["some", "some"]}}, False],
        [{'CONFIG': {"SAFE_PLACEHOLDERS": 123}}, True],
    ], ids=[
        'empty_experiment_data',
        'single_safe_placeholder',
        'multiple_safe_placeholders_space',
        'multiple_safe_placeholders_comma',
        'safe_placeholders_as_list',
        'invalid_safe_placeholders_type'
    ]
)
def test_set_default_parameters(autosubmit_config: 'AutosubmitConfigFactory', experiment_data: dict, raise_error, tmp_path):
    """Test that default parameters are set correctly."""
    as_conf: AutosubmitConfig = autosubmit_config(expid="a000", experiment_data=experiment_data)
    if raise_error:
        with pytest.raises(AutosubmitCritical):
            as_conf.set_default_parameters()
    else:
        as_conf.set_default_parameters()
        if experiment_data:
            assert "some" in as_conf.default_parameters.keys()
            assert "%some%" in as_conf.default_parameters.values()


@pytest.mark.parametrize(
    'email,expected',
    [
        ('user@example.com', True),
        ('user.name@example.com', True),
        ('user', False),
        ('user@localhost', True),  # Fixed: local/intranet addresses are valid (issue #1471)
        ('admin@mailserver', True),  # Single-label domain
        ('user+tag@example.com', True),  # Plus addressing
        ('user@sub.domain.example.com', True),  # Subdomain
        ('', False),  # Empty string
        ('    ', False),  # Blank string
    ]
)
def test_is_valid_mail_address(email, expected):
    assert AutosubmitConfig.is_valid_mail_address(email) is expected


@pytest.mark.parametrize(
    'exp_data, invalid_settings',
    [
        (
                {
                    "CONFIG": {
                        "AUTOSUBMIT_VERSION": "4.1.12",
                        "TOTALJOBS": 20,
                        "MAXWAITINGJOBS": 20
                    },
                    "DEFAULT": {
                        "EXPID": "",
                        "HPCARCH": "",
                        "CUSTOM_CONFIG": ""
                    },
                    "PROJECT": {
                        "PROJECT_TYPE": "git",
                        "PROJECT_DESTINATION": "git_project"
                    },
                    "GIT": {
                        "PROJECT_ORIGIN": "",
                        "PROJECT_BRANCH": "",
                        "PROJECT_COMMIT": "",
                        "PROJECT_SUBMODULES": "",
                        "FETCH_SINGLE_BRANCH": True
                    },
                    "JOBS": {
                        "JOB1": {
                            "WALLCLOCK": "01:00",
                            "PLATFORM": "test"
                        }
                    },
                    "PLATFORMS": {
                        "test": {
                            "MAX_WALLCLOCK": "01:30"
                        }
                    },
                },
                [],
        ),
        (
                {
                    "CONFIG": {
                        "AUTOSUBMIT_VERSION": "4.1.12",
                        "TOTALJOBS": 20,
                        "MAXWAITINGJOBS": 20
                    },
                    "DEFAULT": {
                        "EXPID": "",
                        "HPCARCH": "",
                        "CUSTOM_CONFIG": ""
                    },
                    "PROJECT": {
                        "PROJECT_TYPE": "git",
                        "PROJECT_DESTINATION": "git_project"
                    },
                    "GIT": {
                        "PROJECT_ORIGIN": "",
                        "PROJECT_BRANCH": "",
                        "PROJECT_COMMIT": "",
                        "PROJECT_SUBMODULES": "",
                        "FETCH_SINGLE_BRANCH": True
                    },
                    "JOBS": {
                        "JOB1": {
                            "WALLCLOCK": "01:00",
                            "PLATFORM": "test"
                        }
                    },
                    "PLATFORMS": {
                        "test": {
                            "MAX_WALLCLOCK": "01:30"
                        }
                    },
                    "STORAGE": {
                        "TYPE": "sqlite",
                    }
                },
                [],
        ),
        (
                {
                    "CONFIG": {
                        "AUTOSUBMIT_VERSION": "4.1.12",
                        "TOTALJOBS": 20,
                        "MAXWAITINGJOBS": 20
                    },
                    "DEFAULT": {
                        "EXPID": "",
                        "HPCARCH": "",
                        "CUSTOM_CONFIG": ""
                    },
                    "PROJECT": {
                        "PROJECT_TYPE": "git",
                        "PROJECT_DESTINATION": "git_project"
                    },
                    "GIT": {
                        "PROJECT_ORIGIN": "",
                        "PROJECT_BRANCH": "",
                        "PROJECT_COMMIT": "",
                        "PROJECT_SUBMODULES": "",
                        "FETCH_SINGLE_BRANCH": True
                    },
                    "JOBS": {
                        "JOB1": {
                            "WALLCLOCK": "01:00",
                            "PLATFORM": "test"
                        }
                    },
                    "PLATFORMS": {
                        "test": {
                            "MAX_WALLCLOCK": "01:30"
                        }
                    },
                    "STORAGE": {
                        "TYPE": "invalid",
                    }
                },
                ['STORAGE.TYPE'],
        ),
        (
                {
                    "CONFIG": {
                        "AUTOSUBMIT_VERSION": "4.1.12",
                    },
                    "DEFAULT": {
                        "EXPID": "",
                        "HPCARCH": "",
                        "CUSTOM_CONFIG": ""
                    },
                    "PROJECT": {
                        "PROJECT_TYPE": "git",
                        "PROJECT_DESTINATION": "git_project"
                    },
                    "GIT": {
                        "PROJECT_ORIGIN": "",
                        "PROJECT_BRANCH": "",
                        "PROJECT_COMMIT": "",
                        "PROJECT_SUBMODULES": "",
                        "FETCH_SINGLE_BRANCH": True
                    },
                    "JOBS": {
                        "JOB1": {
                            "WALLCLOCK": "01:00",
                            "PLATFORM": "test"
                        }
                    },
                    "PLATFORMS": {
                        "test": {
                            "MAX_WALLCLOCK": "01:30"
                        }
                    },
                    "STORAGE": {
                        "TYPE": "sqlite",
                    }
                },
                ['CONFIG.TOTALJOBS', 'CONFIG.MAXWAITINGJOBS', 'CONFIG.AUTOSUBMIT_VERSION'],
        ),
        (
                {
                    "CONFIG": {
                        "AUTOSUBMIT_VERSION": "4.1.12",
                        "TOTALJOBS": 20,
                        "MAXWAITINGJOBS": 20
                    },
                    "DEFAULT": {
                        "EXPID": "",
                        "HPCARCH": "",
                        "CUSTOM_CONFIG": ""
                    },
                    "PROJECT": {
                        "PROJECT_TYPE": "git",
                        "PROJECT_DESTINATION": "git_project"
                    },
                    "GIT": {
                        "PROJECT_ORIGIN": "",
                        "PROJECT_BRANCH": "",
                        "PROJECT_COMMIT": "",
                        "PROJECT_SUBMODULES": "",
                        "FETCH_SINGLE_BRANCH": True
                    },
                    "JOBS": {
                        "JOB1": {
                            "WALLCLOCK": "01:00",
                            "PLATFORM": "test"
                        }
                    },
                    "PLATFORMS": {
                        "test": {
                            "MAX_WALLCLOCK": "01:30"
                        }
                    },
                    "STORAGE": {
                        "TYPE": "sqlite",
                    },
                    "MAIL": {
                        "NOTIFICATIONS": True,
                        "TO": ["valid_email_not_actually_exists@bsc.es", "another_valid_email_not_actually_exists@bsc.es"],
                    }
                },
                [],
        ),
        (
                {
                    "CONFIG": {
                        "AUTOSUBMIT_VERSION": "4.1.12",
                        "TOTALJOBS": 20,
                        "MAXWAITINGJOBS": 20
                    },
                    "DEFAULT": {
                        "EXPID": "",
                        "HPCARCH": "",
                        "CUSTOM_CONFIG": ""
                    },
                    "PROJECT": {
                        "PROJECT_TYPE": "git",
                        "PROJECT_DESTINATION": "git_project"
                    },
                    "GIT": {
                        "PROJECT_ORIGIN": "",
                        "PROJECT_BRANCH": "",
                        "PROJECT_COMMIT": "",
                        "PROJECT_SUBMODULES": "",
                        "FETCH_SINGLE_BRANCH": True
                    },
                    "JOBS": {
                        "JOB1": {
                            "WALLCLOCK": "01:00",
                            "PLATFORM": "test"
                        }
                    },
                    "PLATFORMS": {
                        "test": {
                            "MAX_WALLCLOCK": "01:30"
                        }
                    },
                    "STORAGE": {
                        "TYPE": "sqlite",
                    },
                    "MAIL": {
                        "NOTIFICATIONS": True,
                        "TO": "invalidbsc.es invalid2bsc.es",
                    }
                },
                ["MAIL.TO"],
        ),

        (
                {
                    "CONFIG": {
                        "AUTOSUBMIT_VERSION": "4.1.12",
                        "TOTALJOBS": 20,
                        "MAXWAITINGJOBS": 20
                    },
                    "DEFAULT": {
                        "EXPID": "",
                        "HPCARCH": "",
                        "CUSTOM_CONFIG": ""
                    },
                    "PROJECT": {
                        "PROJECT_TYPE": "git",
                        "PROJECT_DESTINATION": "git_project"
                    },
                    "GIT": {
                        "PROJECT_ORIGIN": "",
                        "PROJECT_BRANCH": "",
                        "PROJECT_COMMIT": "",
                        "PROJECT_SUBMODULES": "",
                        "FETCH_SINGLE_BRANCH": True
                    },
                    "JOBS": {
                        "JOB1": {
                            "WALLCLOCK": "01:00",
                            "PLATFORM": "test"
                        }
                    },
                    "PLATFORMS": {
                        "test": {
                            "MAX_WALLCLOCK": "01:30"
                        }
                    },
                    "STORAGE": {
                        "TYPE": "sqlite",
                    },
                    "MAIL": {
                        "NOTIFICATIONS": True,
                        "TO": "valid@bsc.es,invalid2bsc.es",
                    }
                },
                ["MAIL.TO"],
        ),

    ],
    ids=[
        'valid_config_without_storage',
        'valid_config_with_storage',
        'invalid_storage_type',
        'invalid_total_jobs_maxwaitingjobs,autosubmit_version',
        'valid_mail_configuration',
        'invalid_mail_to_no_at',
        'invalid_mail_to_no_at_comma'
    ], )
def test_check_autosubmit_conf(autosubmit_config, exp_data, invalid_settings):
    """Test that ``check_autosubmit_conf()`` works as expected."""
    as_conf: AutosubmitConfig = autosubmit_config(expid='a000', experiment_data=exp_data)
    as_conf.check_autosubmit_conf()

    if not invalid_settings:
        assert len(as_conf.wrong_config) == 0
    else:
        assert len(as_conf.wrong_config) > 0
        # normalize for comparison
        section_list = []
        keys = []
        # TODO for some reason this is a list of lists ( and the last list should be tuple?)
        for weird_list in as_conf.wrong_config["Autosubmit"]:
            section, key = weird_list
            key = key[1].split(" ")[0].upper()
            section_list.append(section.strip().upper())
            keys.append(key.strip().upper())

        for expected in invalid_settings:
            if "." in expected:
                items = expected.split(".")
                section = items[0].upper()
                keys = [item.upper() for item in items[1:]]
                assert section in section_list
                for key in keys:
                    assert key in keys
            else:
                assert expected.upper() in section_list


def test_set_safetysleeptime_updates_file(autosubmit_config, tmp_path: Path) -> None:
    """Ensure set_safetysleeptime replaces the existing SAFETYSLEEPTIME line."""
    as_conf = autosubmit_config(expid='a000', experiment_data={})
    as_conf._conf_parser_file = tmp_path / "config.txt"
    initial = (
        "SOME_SETTING: whatever\n"
        "SAFETYSLEEPTIME: 10\n"
        "OTHER: value\n"
    )
    as_conf._conf_parser_file.write_text(initial)
    as_conf.set_safetysleeptime(25)
    content = as_conf._conf_parser_file.read_text()
    expected = (
        "SOME_SETTING: whatever\n"
        "SAFETYSLEEPTIME: 25\n"
        "OTHER: value\n"
    )
    assert content == expected


@pytest.mark.parametrize(
    "storage_type,expected",
    [
        ("sqlite", True),
        ("postgres", True),
        ("sqlite3", False),
        ("pkl", False),
        ("", True),  # default sqlite
    ],
)
def test_is_valid_storage_type(storage_type: str, expected: bool,
                               autosubmit_config: 'AutosubmitConfigFactory') -> None:
    """Return whether the configured storage type is accepted.

    Uses realistic `STORAGE.TYPE` values and asserts the boolean result
    from ``is_valid_storage_type()``.
    """
    as_conf = autosubmit_config(expid='a000', experiment_data={
        "STORAGE": {"TYPE": storage_type}
    })
    assert as_conf.is_valid_storage_type() is expected


@pytest.mark.parametrize(
    "exp_data, expected",
    [
        ({}, []),
        ({"WRAPPERS": {}}, []),
        ({"WRAPPERS": {"JOBS_IN_WRAPPER": ""}}, []),
        ({"WRAPPERS": {"JOBS_IN_WRAPPER": "job1 job2  job3"}}, ["job1", "job2", "job3"]),
        ({"WRAPPERS": {"JOBS_IN_WRAPPER": "jobA&jobB & jobC"}}, ["jobA", "jobB", "jobC"]),
        ({"WRAPPERS": {"JOBS_IN_WRAPPER": [" jobX ", "", "jobY"]}}, ["jobX", "jobY"]),
        ({"WRAPPERS": {"JOBS_IN_WRAPPER": [""]}}, []),
    ],
)
def test_get_wrapped_jobs_various_formats(
        autosubmit_config: "AutosubmitConfigFactory", exp_data: dict, expected: list[str]
) -> None:
    as_conf = autosubmit_config(expid="a000", experiment_data=exp_data)
    result = as_conf.get_wrapped_jobs()
    assert result == expected


def test_check_files_loaded_reads_existing_files(
    autosubmit_config: "AutosubmitConfigFactory", tmp_path: Path, monkeypatch
) -> None:
    as_conf = autosubmit_config(expid="a000", experiment_data={})

    file1 = tmp_path / "a.yml"
    file2 = tmp_path / "b.yml"

    # mock reload, as we don't want to actually reload anything (unit test)
    monkeypatch.setattr(as_conf, "reload", lambda x: None)

    file1.write_text("content1\n")
    file2.write_text("content2\n")

    as_conf.current_loaded_files = [str(file1), str(file2)]

    result = as_conf.check_files_loaded()

    expected = f"header:{file1}\ncontent1\nheader:{file2}\ncontent2\n"
    assert result == expected


def test_platforms_missing_hpcarch_local(autosubmit_config: "AutosubmitConfigFactory"):
    """Test that if the PLATFORMS key is missing but DEFAULT.HPCARCH is LOCAL, we return an empty dictionary."""
    as_conf: AutosubmitConfig = autosubmit_config(
        expid="a000", experiment_data={"DEFAULT": {"HPCARCH": "LOCAL"}}
    )
    as_conf.experiment_data.pop("PLATFORMS", None)

    assert as_conf.platforms_data == {}


def test_platforms_missing_hpcarch_non_local(
    autosubmit_config: "AutosubmitConfigFactory",
):
    """Test that if the PLATFORMS key is missing and DEFAULT.HPCARCH is not LOCAL, we raise an AutosubmitCritical."""
    as_conf: AutosubmitConfig = autosubmit_config(
        expid="a000", experiment_data={"DEFAULT": {"HPCARCH": "MARENOSTRUM5"}}
    )
    as_conf.experiment_data.pop("PLATFORMS", None)

    with pytest.raises(AutosubmitCritical) as exc_info:
        _ = as_conf.platforms_data

    assert exc_info.value.code == 7014
    assert "PLATFORMS section not found in configuration file" in str(exc_info.value)


@pytest.mark.parametrize(
    "platform_description",
    [
        {
            "PYTEST-UNDEFINED": {
                "host": "",
                "user": "",
                "project": "",
                "scratch_dir": "",
                "MAX_WALLCLOCK": "",
                "DISABLE_RECOVERY_THREADS": True,
            }
        },
        "not_a_dict",
    ],
)
def test_platforms_not_dict(
    autosubmit_config: "AutosubmitConfigFactory", platform_description
):
    """Test that if PLATFORMS is not a dictionary, we raise an AutosubmitCritical."""
    as_conf: AutosubmitConfig = autosubmit_config(
        expid="a000",
        experiment_data={
            "PLATFORMS": platform_description,
            "DEFAULT": {"HPCARCH": "MARENOSTRUM5"},
        },
    )

    if not isinstance(platform_description, dict):
        with pytest.raises(AutosubmitCritical) as exc_info:
            _ = as_conf.platforms_data

        assert exc_info.value.code == 7014
        assert "PLATFORMS section is malformed in configuration file" in str(
            exc_info.value
        )
    else:
        assert platform_description == as_conf.platforms_data

@pytest.mark.parametrize('experiment_data, expected', 
    [
        (
            {
                'JOBS': {
                    'SIM': {
                        'CPMIP_THRESHOLDS': {
                            'SYPD':{
                                'THRESHOLD': 5.0,
                                'COMPARISON': 'greater_than',
                                '%_ACCEPTED_ERROR': 10
                            }
                        }
                    }
                }
            }, {'SYPD':{ 'THRESHOLD': 5.0, 'COMPARISON': 'greater_than', '%_ACCEPTED_ERROR': 10}}
        ),
        (
            {
                'JOBS': {
                    'SIM': {}
                }
            }, {}
        ),
        (
            {
                'JOBS': {
                    'SIM': {
                        'CPMIP_THRESHOLDS': {
                            'not_a_dict'
                        }
                    }
                }
            }, {}
        ),
    ], 
    ids=[
        'valid_thresholds',
        'no_thresholds',
        'invalid_thresholds'
    ])
def test_get_cpmip_thresholds_different_cases(autosubmit_config, experiment_data, expected):
    as_conf = autosubmit_config(expid='a000', experiment_data=experiment_data)
    thresholds = as_conf.get_cpmip_thresholds('SIM')
    assert thresholds == expected

def test_validate_wallclock(autosubmit_config: 'AutosubmitConfigFactory'):
    """Test should succeed"""
    as_conf: AutosubmitConfig = autosubmit_config(
        expid="a000",
        experiment_data={
            "PLATFORMS": {
                "MARENOSTRUM5": {
                    "MAX_WALLCLOCK": "2:00"
                },
            },
            "DEFAULT": {"HPCARCH": "MARENOSTRUM5"},
            "CONFIG": {
                "JOB_WALLCLOCK": "2:00"
            },
            "JOBS": {
                "AQUA_ANALYSIS": {
                    "PLATFORM": "MARENOSTRUM5",
                    "WALLCLOCK": "2:00"
                }
            },
        },
    )

    res = as_conf.validate_wallclock()
    assert res == ""

def test_validate_wallclock_errors(autosubmit_config: 'AutosubmitConfigFactory'):
    """Test should produce an error that the job WALLCLOCK is greater than the platform MAX_WALLCLOCK"""
    as_conf: AutosubmitConfig = autosubmit_config(
        expid="a000",
        experiment_data={
            "PLATFORMS": {
                "MARENOSTRUM5": {
                    "MAX_WALLCLOCK": "1:00"
                },
            },
            "DEFAULT": {"HPCARCH": "MARENOSTRUM5"},
            "CONFIG": {
                "JOB_WALLCLOCK": "2:00"
            },
            "JOBS": {
                "AQUA_ANALYSIS": {
                    "PLATFORM": "MARENOSTRUM5",
                    "WALLCLOCK": "2:00"
                }
            },
        },
    )

    res = as_conf.validate_wallclock()
    assert res == "Job AQUA_ANALYSIS has a wallclock value of 7200.0s, which is greater than the platform's 3600.0s wallclock time\n"


def test_load_config_file(autosubmit_config, tmp_path):
    """Test most basic functionality of ``load_config_file``."""
    as_conf = autosubmit_config(expid='a000', experiment_data={})
    config_file = tmp_path / 'a000.yml'
    with open(config_file, 'w') as f:
        f.write(dedent('''\
        JOB:
          A:
            SCRIPT: "echo OK"
            PLATFORM: local
        '''))
    current_config = {}
    new_config = as_conf.load_config_file(current_config, tmp_path / 'a000.yml', False)
    assert 'JOB' in new_config

    # This implies DEFAULT.CUSTOM_CONFIG is also not present.
    assert 'DEFAULT' not in new_config


def test_load_config_file_custom_config(autosubmit_config, tmp_path):
    """Test loading custom configuration files with ``load_config_file``."""
    as_conf = autosubmit_config(expid='a000', experiment_data={})
    config_file = tmp_path / 'a000.yml'
    with open(config_file, 'w') as f:
        f.write(dedent('''\
        DEFAULT:
          CUSTOM_CONFIG:
            - a.yml
            - b.yml
        JOB:
          A:
            SCRIPT: "echo OK"
            PLATFORM: local
        '''))
    current_config = {}
    new_config = as_conf.load_config_file(current_config, tmp_path / 'a000.yml', False)

    custom_config = new_config['DEFAULT']['CUSTOM_CONFIG']
    assert 'a.yml' in custom_config
    assert 'b.yml' in custom_config


@pytest.mark.parametrize(
    'new_config_data,load_misc,expected_misc_files_length',
    [
        (
            'AS_MISC: False',
            True,
            0
        ),
        (
            'AS_MISC: False',
            False,
            0
        ),
        (
            'AS_MISC: True',
            True,
            0
        ),
        (
            'AS_MISC: True',
            False,
            1
        )
    ],
    ids=[
        'Contains AS_MISC load_misc True',
        'Contains AS_MISC load_misc False',
        'Does not contain AS_MISC load_misc True',
        'Does not contain AS_MISC load_misc False'
    ]
)
def test_load_config_file_misc(new_config_data: str, load_misc: bool, expected_misc_files_length: int,
                               autosubmit_config, tmp_path):
    """Test loading miscellaneous configuration files with ``load_config_file``.

    If the current data contains ``AS_MISC``, then the function never loads new miscellaneous files.

    If it does not contain ``AS_MISC``, the ``load_misc`` argument of the function controls where new files are
    loaded or not.

    Loaded here simply means added to the list ``as_conf.misc_files``. The ``reload`` function is the only place
    where these files are finally parsed and its configuration merged into Autosubmit's main configuration.

    TODO: this could probably be simplified.
    """
    as_conf = autosubmit_config(expid='a000', experiment_data={})
    config_file = tmp_path / 'a000.yml'
    with open(config_file, 'w') as f:
        f.write(new_config_data)
    current_config = as_conf.experiment_data
    as_conf.load_config_file(current_config, tmp_path / 'a000.yml', load_misc=load_misc)

    assert len(as_conf.misc_files) == expected_misc_files_length
