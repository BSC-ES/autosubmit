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

"""Basic tests for ``AutosubmitConfig``."""

import copy
from pathlib import Path
from textwrap import dedent
from types import SimpleNamespace
from typing import TYPE_CHECKING

import pytest

from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.log.log import AutosubmitCritical, AutosubmitError
from autosubmit.platforms.platform_type import PlatformType

if TYPE_CHECKING:
    from test.unit.conftest import AutosubmitConfigFactory


_EXPID = "t000"
"""Experiment ID used for testing."""


@pytest.fixture
def submitter(mocker):
    """Create a fake submitter."""
    local = mocker.Mock(name="local_platform")
    remote = mocker.Mock(name="remote_platform")

    return SimpleNamespace(
        platforms={
            "HPC": remote,
            PlatformType.LOCAL.upper(): local,
        }
    )


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
    "parameter, value, error_msg",
    [
        (
            "TOTALJOBS",
            0,
            "TOTALJOBS parameter not found or not strictly positive integer",
        ),
        (
            "TOTALJOBS",
            -1,
            "TOTALJOBS parameter not found or not strictly positive integer",
        ),
        (
            "MAXWAITINGJOBS",
            0,
            "MAXWAITINGJOBS parameter not found or not strictly positive integer",
        ),
        (
            "MAXWAITINGJOBS",
            -1,
            "MAXWAITINGJOBS parameter not found or not strictly positive integer",
        ),
    ],
    ids=[
        "TOTALJOBS set to zero",
        "TOTALJOBS set to negative",
        "MAXWAITINGJOBS set to zero",
        "MAXWAITINGJOBS set to negative",
    ],
)
def test_check_autosubmit_conf_invalid_param(
    as_conf_small: AutosubmitConfig, parameter, value, error_msg
):
    """Test that check_autosubmit_conf writes the error in the wrong_config
    dictionary when a parameter is invalid."""
    as_conf_small.experiment_data["CONFIG"][parameter] = value
    assert not as_conf_small.check_autosubmit_conf()
    assert error_msg in str(as_conf_small.wrong_config["Autosubmit"][0])


@pytest.mark.parametrize(
    "experiment_data, should_pass",
    [
        (
            {
                "PROJECT": {"PROJECT_TYPE": "git"},
                "GIT": {"PROJECT_ORIGIN": "https://github.com/user/repo.git"},
            },
            True,
        ),
        (
            {
                "PROJECT": {"PROJECT_TYPE": "git"},
                "GIT": {
                    "PROJECT_ORIGIN": "https://github.com/user/repo.git",
                    "PROJECT_BRANCH": "master",
                },
            },
            True,
        ),
        ({"PROJECT": {"PROJECT_TYPE": "git"}}, False),
        ({"PROJECT": {"PROJECT_TYPE": "git"}, "GIT": {"PROJECT_ORIGIN": ""}}, False),
        (
            {
                "PROJECT": {"PROJECT_TYPE": "svn"},
                "SVN": {
                    "PROJECT_URL": "https://svn.example.com/repo",
                    "PROJECT_REVISION": "123",
                },
            },
            True,
        ),
        ({"PROJECT": {"PROJECT_TYPE": "svn"}}, False),
        ({"PROJECT": {"PROJECT_TYPE": "svn"}, "SVN": {"PROJECT_URL": ""}}, False),
        ({"PROJECT": {"PROJECT_TYPE": "svn"}, "SVN": {"PROJECT_REVISION": ""}}, False),
        (
            {
                "PROJECT": {"PROJECT_TYPE": "local"},
                "LOCAL": {"PROJECT_PATH": "/path/to/project"},
            },
            True,
        ),
        ({"PROJECT": {"PROJECT_TYPE": "local"}}, False),
        ({"PROJECT": {"PROJECT_TYPE": "local"}, "LOCAL": {"PROJECT_PATH": ""}}, False),
        ({"PROJECT": {"PROJECT_TYPE": "invalid"}}, False),
        ({}, False),
        ({"PROJECT": {"PROJECT_TYPE": "none"}}, True),
    ],
    ids=[
        "Valid git project configuration without PROJECT_BRANCH",
        "Valid git project configuration with PROJECT_BRANCH",
        "Missing GIT section for git project",
        "Empty PROJECT_ORIGIN for git project",
        "Valid SVN project configuration",
        "Missing SVN section for SVN project",
        "Empty PROJECT_URL for SVN project",
        "Empty PROJECT_REVISION for SVN project",
        "Valid local project configuration",
        "Missing LOCAL section for local project",
        "Empty PROJECT_PATH for local project",
        "Invalid project type",
        "Missing PROJECT section",
        "Valid none project type",
    ],
)
def test_check_expdef_conf_invalid_params(
    autosubmit_config: "AutosubmitConfigFactory",
    experiment_data: dict,
    should_pass: bool,
) -> None:
    """Test experiment configuration validation for different project types."""
    base = {
        "DEFAULT": {"EXPID": "a000", "HPCARCH": "LOCAL"},
        "EXPERIMENT": {
            "DATELIST": "20200101",
            "MEMBERS": "fc0",
            "CHUNKSIZEUNIT": "month",
            "CHUNKSIZE": 1,
            "NUMCHUNKS": 1,
            "CALENDAR": "standard",
        },
    }
    base.update(experiment_data)
    as_conf: AutosubmitConfig = autosubmit_config(expid="a000", experiment_data=base)

    result = as_conf.check_expdef_conf()

    if should_pass:
        assert (
            result is True
        ), f"Expected check_expdef_conf to pass, but it failed. wrong_config={as_conf.wrong_config}"
    else:
        assert result is False, "Expected check_expdef_conf to fail, but it passed"
        assert "Expdef" in as_conf.wrong_config


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


@pytest.mark.parametrize(
    'experiment_data, section, expected',
    [
        (
            {
                'WRAPPERS': {
                    'WRAPPER_A': {
                        'TYPE': 'horizontal',
                        'JOBS_IN_WRAPPER': ['SIM', 'POST'],
                    }
                }
            },
            'SIM',
            True,
        ),
        (
            {
                'WRAPPERS': {
                    'WRAPPER_A': {
                        'TYPE': 'horizontal',
                        'JOBS_IN_WRAPPER': ['SIM', 'POST'],
                    }
                }
            },
            'INI',
            False,
        ),
        (
            {
                'WRAPPERS': {
                    'WRAPPER_A': {
                        'TYPE': 'horizontal',
                        'JOBS_IN_WRAPPER': ['SIM'],
                    },
                    'WRAPPER_B': {
                        'TYPE': 'vertical',
                        'JOBS_IN_WRAPPER': ['POST'],
                    },
                }
            },
            'POST',
            True,
        ),
        (
            {},
            'SIM',
            False,
        ),
        (
            {
                'WRAPPERS': {
                    'WRAPPER_A': 'not_a_dict',
                }
            },
            'SIM',
            False,
        ),
    ],
    ids=[
        'section_in_single_wrapper',
        'section_not_in_wrapper',
        'section_in_second_wrapper',
        'no_wrappers_section',
        'wrapper_value_not_dict',
    ],
)
def test_is_section_in_any_wrapper(
    autosubmit_config: 'AutosubmitConfigFactory',
    experiment_data: dict,
    section: str,
    expected: bool,
) -> None:
    """Test that is_section_in_any_wrapper returns the correct result."""
    as_conf: AutosubmitConfig = autosubmit_config(expid='a000', experiment_data=experiment_data)
    as_conf.experiment_data = experiment_data
    assert as_conf.is_section_in_any_wrapper(section) is expected


def test_immutable_variables_overwrites_default_values(
    autosubmit_config: "AutosubmitConfigFactory",
) -> None:
    """Test that the _pin_immutable_variables method correctly pins immutable variables."""
    as_conf: AutosubmitConfig = autosubmit_config(expid="a000", experiment_data={})
    as_conf.starter_conf = {"DEFAULT": {"EXPID": "a000", "HPCARCH": "LOCAL"}}
    parameters = {
        "DEFAULT": {"EXPID": "a001", "HPCARCH": "MARENOSTRUM5", "OTHER": "value"}
    }
    pinned = as_conf._pin_immutable_variables(parameters)

    # Check immutable variables keep original values, other variables not affected
    assert pinned["DEFAULT"]["EXPID"] == "a000"
    assert pinned["DEFAULT"]["HPCARCH"] == "MARENOSTRUM5"
    assert pinned["DEFAULT"]["OTHER"] == "value"


def test_immutable_variables_adds_missing_sections(
    autosubmit_config: "AutosubmitConfigFactory",
) -> None:
    """Test that the _pin_immutable_variables method adds missing sections and keys."""
    as_conf: AutosubmitConfig = autosubmit_config(expid="a000", experiment_data={})
    as_conf.starter_conf = {"DEFAULT": {"EXPID": "a000", "HPCARCH": "LOCAL"}}

    parameters = {}
    pinned = as_conf._pin_immutable_variables(parameters)

    # Only EXPID is pinned
    assert pinned["DEFAULT"]["EXPID"] == "a000"
    assert not pinned["DEFAULT"].get("HPCARCH")


def test_load_custom_config(autosubmit_config, tmp_path) -> None:
    """Test that the load_custom_config method correctly loads and merges custom configuration files."""
    as_conf: AutosubmitConfig = autosubmit_config(expid="a000", experiment_data={})

    git_project_dir = tmp_path / "proj" / "git_project"
    conf_dir = git_project_dir / "conf"
    common_dir = git_project_dir / "as_conf" / "common"
    real_dir = git_project_dir / "as_conf" / "real_from_ideal"
    post_dir = git_project_dir / "as_conf" / "post"
    conf_dir.mkdir(parents=True, exist_ok=True)
    # PRE
    common_dir.mkdir(parents=True, exist_ok=True)
    real_dir.mkdir(parents=True, exist_ok=True)
    # POST
    post_dir.mkdir(parents=True, exist_ok=True)

    as_conf.starter_conf = {
        "DEFAULT": {"EXPID": "a000", "HPCARCH": "LOCAL"},
        "JOBS": {"DO_NOTHING": {"SCRIPT": "sleep 20", "PLATFORM": "LOCAL", "RUNNING": "once"}},
        "CONFIG": {},
        "PROJDIR": str(git_project_dir),
    }

    current_data = {
        "DEFAULT": {"EXPID": "a000", "HPCARCH": "LOCAL"},
        "JOBS": {"DO_NOTHING": {"SCRIPT": "sleep 20", "PLATFORM": "LOCAL", "RUNNING": "once"}},
        "CONFIG": {},
        "PROJDIR": str(git_project_dir),
    }

    current_data_before = copy.deepcopy(current_data)

    root_file = conf_dir / "root_config.yml"
    root_file.write_text(
        dedent(
            """\
            DEFAULT:
              CUSTOM_CONFIG:
                PRE: "%PROJDIR%/as_conf/common,%PROJDIR%/as_conf/real_from_ideal"
                POST: "%PROJDIR%/as_conf/post"
            """
        )
    )

    (common_dir / "common_config.yml").write_text(
        dedent("""\
        DEFAULT:
          EXPID: "a001"
          HPCARCH: "MARENOSTRUM5"
          COMMON_CONFIG_VALUE: "common_value"
        JOBS:
          DO_NOTHING:
            SCRIPT: "pre a.yml!"

    """)
    )

    (real_dir / "real_from_ideal_config.yml").write_text(
        dedent("""\
        DEFAULT:
          EXPID: "a002"
          HPCARCH: "MARENOSTRUM5"
          REAL_CONFIG_VALUE: "real_from_ideal_value_1"
        REAL_FROM_IDEAL_VALUE: "real_from_ideal_value_2"
    """)
    )

    (post_dir / "post_config.yml").write_text(
        dedent("""\
        DEFAULT:
          EXPID: "a003"
          HPCARCH: "MARENOSTRUM6"
          POST_CONFIG_VALUE: "post_value"
        JOBS:
          DO_NOTHING:
            SCRIPT: "post b.yml!"
    """)
    )

    data_pre, data_post = as_conf.load_custom_config(current_data, [str(root_file)])

    # check nested configurations are merged in PRE
    assert data_pre["DEFAULT"]["COMMON_CONFIG_VALUE"] == "common_value"
    assert data_pre["DEFAULT"]["REAL_CONFIG_VALUE"] == "real_from_ideal_value_1"
    assert data_pre["REAL_FROM_IDEAL_VALUE"] == "real_from_ideal_value_2"

    # check that custom_config does not appear in data_pre
    assert "CUSTOM_CONFIG" not in data_pre.get("DEFAULT", {})
    assert "CUSTOM_CONFIG" not in data_post.get("DEFAULT", {})
    assert "CUSTOM_CONFIG" not in data_post.get("DEFAULT", {})

    # check that pinned variables are not overwritten in data_pre
    assert data_pre["DEFAULT"]["EXPID"] == "a000"
    assert data_pre["DEFAULT"]["HPCARCH"] == "LOCAL"

    # check that POST config is merged in data_post
    assert "POST_CONFIG_VALUE" not in data_pre.get("DEFAULT", {})
    assert data_post["DEFAULT"]["POST_CONFIG_VALUE"] == "post_value"
    assert data_post["DEFAULT"]["COMMON_CONFIG_VALUE"] == "common_value"
    assert data_post["DEFAULT"]["REAL_CONFIG_VALUE"] == "real_from_ideal_value_1"
    assert data_post["REAL_FROM_IDEAL_VALUE"] == "real_from_ideal_value_2"

    # check there is no aliasing between data_pre and data_post
    assert data_pre is not data_post
    data_post["DEFAULT"]["POST_ONLY_TMP"] = "tmp"
    assert "POST_ONLY_TMP" not in data_pre["DEFAULT"]

    # check input current_data is not mutated by load_custom_config
    assert current_data == current_data_before

    # check that JOBS section has DO_NOTHING script with post b.yml!
    assert data_post["JOBS"]["DO_NOTHING"]["SCRIPT"] == "post b.yml!"
    assert data_pre["JOBS"]["DO_NOTHING"]["SCRIPT"] == "sleep 20"

    assert data_pre is not data_post


@pytest.mark.parametrize(
    "section, d_value, must_exists, expected",
    [
        (["CONFIG", "TOTALJOBS"], None, False, None),
        (["CONFIG", "TOTALJOBS"], 10, False, 10),
        (["CONFIG", "TOTALJOBS"], None, True, AutosubmitCritical),
        (["CONFIG", "TOTALJOBS"], 10, True, AutosubmitCritical),
        (["LOCAL", "PROJECT_PATH"], "", False, ""),
    ],
    ids=[
        "Non-mandatory section missing with None d_value returns None",
        "Non-mandatory section missing with d_value returns d_value",
        "Mandatory section missing with None d_value raises AutosubmitCritical",
        "Mandatory section missing with d_value raises AutosubmitCritical",
        "Non-mandatory section missing with default d_value returns default d_value",
    ],
)
def test_get_section_missing_returns_d_value(
    autosubmit_config,
    section,
    d_value,
    must_exists,
    expected,
):
    """Test that get_section returns the correct value when the section is missing."""
    as_conf: AutosubmitConfig = autosubmit_config(expid="a000", experiment_data={})
    as_conf.experiment_data.pop(section[0], None)

    if expected is AutosubmitCritical:
        with pytest.raises(AutosubmitCritical):
            as_conf.get_section(section, d_value=d_value, must_exists=must_exists)
    else:
        result = as_conf.get_section(section, d_value=d_value, must_exists=must_exists)
        assert result == expected


def test_no_conf_folder(mocker, tmp_path):
    mocked_basic_config = mocker.patch('autosubmit.config.configcommon.BasicConfig')
    mocked_basic_config.LOCAL_ROOT_DIR.return_value = str(tmp_path / 'does_not_exist')
    with pytest.raises(IOError):
        AutosubmitConfig(_EXPID)


def test_jobs_data_no_jobs(autosubmit_config):
    as_conf = autosubmit_config(_EXPID, experiment_data={})
    del as_conf.experiment_data['JOBS']
    with pytest.raises(AutosubmitCritical) as cm:
        len(as_conf.jobs_data)
    assert 'JOBS section not found' in str(cm.value)


def test_jobs_data_unexpected_error(autosubmit_config):
    as_conf = autosubmit_config(_EXPID, experiment_data={})
    as_conf.experiment_data = None
    with pytest.raises(AutosubmitCritical) as cm:
        len(as_conf.jobs_data)
    assert 'Error while reading JOBS' in str(cm.value)


def test_get_wrapper_export_when_none(autosubmit_config):
    """Test that when the ``wrapper`` given is ``None``, it uses an empty dictionary."""
    as_conf = autosubmit_config(_EXPID, experiment_data={})
    wrapper_export = as_conf.get_wrapper_export(None)
    assert wrapper_export == ''


def test_check_platforms_conf_valid_main_platform(autosubmit_config):
    """Test that the main platform is found in the configuration."""
    as_conf = autosubmit_config(
        _EXPID,
        experiment_data={
            "PLATFORMS": {
                "MARENOSTRUM": {
                    "TYPE": "slurm",
                    "HOST": "host",
                    "PROJECT": "proj",
                    "USER": "user",
                    "SCRATCH_DIR": "/scratch",
                }
            }
        },
    )
    as_conf.hpcarch = "MARENOSTRUM"

    assert as_conf.check_platforms_conf() is True
    assert "Platform" not in as_conf.wrong_config


def test_check_platforms_conf_main_platform_not_defined(autosubmit_config):
    """Test the experiment default HPCARCH missing from the platforms."""
    as_conf = autosubmit_config(
        _EXPID,
        experiment_data={
            "PLATFORMS": {
                "OTHER": {
                    "TYPE": "slurm",
                    "HOST": "host",
                    "PROJECT": "proj",
                    "USER": "user",
                    "SCRATCH_DIR": "/scratch",
                }
            }
        },
    )
    as_conf.hpcarch = "MARENOSTRUM"

    assert as_conf.check_platforms_conf() is True

    assert any(
        "Main platform is not defined" in err
        for _, err in as_conf.wrong_config["Expdef"]
    )


@pytest.mark.parametrize(
    ("missing_key", "expected"),
    [
        ("TYPE", "Mandatory TYPE parameter"),
        ("HOST", "Mandatory HOST parameter"),
        ("PROJECT", "Mandatory PROJECT parameter"),
        ("USER", "Mandatory USER parameter"),
        ("SCRATCH_DIR", "Mandatory SCRATCH_DIR parameter"),
    ],
)
def test_check_platforms_conf_missing_required_parameter(
    autosubmit_config,
    missing_key,
    expected,
):
    """Test when the platform has missing required parameters."""
    platform = {
        "TYPE": "slurm",
        "HOST": "host",
        "PROJECT": "proj",
        "USER": "user",
        "SCRATCH_DIR": "/scratch",
    }
    del platform[missing_key]

    as_conf = autosubmit_config(
        _EXPID,
        experiment_data={"PLATFORMS": {"MARENOSTRUM": platform}},
    )
    as_conf.hpcarch = "MARENOSTRUM"

    assert as_conf.check_platforms_conf() is False

    assert any(
        expected in message
        for _, message in as_conf.wrong_config["Platform"]
    )


def test_check_platforms_conf_ps_platform_does_not_require_project_or_user(autosubmit_config):
    """Test when a ``PS`` platform is missing parameters (project or user)."""
    as_conf = autosubmit_config(
        _EXPID,
        experiment_data={
            "PLATFORMS": {
                "PS": {
                    "TYPE": PlatformType.PS,
                    "HOST": "host",
                    "SCRATCH_DIR": "/scratch",
                }
            }
        },
    )
    as_conf.hpcarch = "PS"

    assert as_conf.check_platforms_conf() is True
    assert "Platform" not in as_conf.wrong_config


def test_check_platforms_conf_invalid_secondary_platform_is_ignored(autosubmit_config):
    """Test when invalid configuration is found in a unused platform, the other remains OK."""
    as_conf = autosubmit_config(
        _EXPID,
        experiment_data={
            "PLATFORMS": {
                "MAIN": {
                    "TYPE": "slurm",
                    "HOST": "host",
                    "PROJECT": "proj",
                    "USER": "user",
                    "SCRATCH_DIR": "/scratch",
                },
                "BROKEN": {
                    "USER": "someone",
                },
            }
        },
    )
    as_conf.hpcarch = "MAIN"

    assert as_conf.check_platforms_conf() is True
    assert "Platform" not in as_conf.wrong_config


def test_check_platforms_conf_local_platform_is_implicit(autosubmit_config):
    """Test when the no platforms are defined, the ``LOCAL`` platform is still auto-initialised."""
    as_conf = autosubmit_config(_EXPID, experiment_data={"PLATFORMS": {}})
    as_conf.hpcarch = PlatformType.LOCAL

    assert as_conf.check_platforms_conf() is True


def test_check_platforms_conf_ignore_undefined_platforms(autosubmit_config):
    """Test when no platforms are defined and ``.ignore_undefined_platforms`` is set to ``True``."""
    as_conf = autosubmit_config(_EXPID, experiment_data={"PLATFORMS": {}})
    as_conf.hpcarch = "UNKNOWN"
    as_conf.ignore_undefined_platforms = True

    assert as_conf.check_platforms_conf() is True


def test_check_wrapper_conf_local_platform_not_supported(autosubmit_config):
    """Test that using wrappers with the ``LOCAL`` platform raises an error."""
    as_conf = autosubmit_config(
        _EXPID,
        experiment_data={
            "JOBS": {
                "JOB1": {
                    "PLATFORM": "LOCAL",
                },
            },
        },
    )

    wrappers = {
        "wrapper": {
            "JOBS_IN_WRAPPER": ["JOB1"],
        },
    }

    with pytest.raises(AutosubmitCritical) as exc:
        as_conf.check_wrapper_conf(wrappers)

    assert "LOCAL platform does not support wrappers" in str(exc.value)


def test_load_section_parameters_uses_default_platform(autosubmit_config, submitter, mocker):
    """Test that jobs without a platform use the default platform."""
    as_conf = autosubmit_config("a000")

    as_conf.hpcarch = "HPC"
    as_conf.check_conf_files = mocker.Mock()

    job = mocker.Mock()
    job.platform_name = None
    job.section = "SIM"
    job.parameters = {}

    job_list = mocker.Mock()
    job_list.get_job_list.return_value = [job]
    job_list.parameters = {}

    job.update_parameters.side_effect = (lambda *_: job.parameters.update({"FOO": "BAR"}))

    result = as_conf.load_section_parameters(job_list, as_conf, submitter)

    as_conf.check_conf_files.assert_called_once_with(False)  # type: ignore
    assert job.platform is submitter.platforms["HPC"]
    assert result == {"SIM_FOO": "BAR"}


def test_load_section_parameters_falls_back_to_local_platform(autosubmit_config, submitter, mocker):
    """Test that an unknown platform falls back to LOCAL."""
    as_conf = autosubmit_config("a000")
    as_conf.check_conf_files = mocker.Mock()

    job = mocker.Mock()
    job.platform_name = "DOES_NOT_EXIST"
    job.section = "SIM"
    job.parameters = {}

    job_list = mocker.Mock()
    job_list.get_job_list.return_value = [job]
    job_list.parameters = {}

    job.update_parameters.side_effect = lambda *_: None

    as_conf.load_section_parameters(job_list, as_conf, submitter)

    assert job.platform is submitter.platforms[PlatformType.LOCAL.upper()]


def test_load_section_parameters_updates_each_section_once(autosubmit_config, submitter, mocker):
    """Test that parameters are updated once per section."""
    as_conf = autosubmit_config("a000")
    as_conf.check_conf_files = mocker.Mock()

    job1 = mocker.Mock(section="SIM", platform_name="HPC", parameters={})
    job2 = mocker.Mock(section="SIM", platform_name="HPC", parameters={})
    job3 = mocker.Mock(section="POST", platform_name="HPC", parameters={})

    job_list = mocker.Mock()
    job_list.get_job_list.return_value = [job1, job2, job3]
    job_list.parameters = {}

    job1.update_parameters.side_effect = lambda *_: None
    job2.update_parameters.side_effect = lambda *_: None
    job3.update_parameters.side_effect = lambda *_: None

    as_conf.load_section_parameters(job_list, as_conf, submitter)

    job1.update_parameters.assert_called_once()
    job2.update_parameters.assert_not_called()
    job3.update_parameters.assert_called_once()


@pytest.mark.parametrize(
    "existing,expected",
    [
        ({}, {"SIM_A": 1, "SIM_B": 2}),
        ({"A": 0}, {"SIM_B": 2}),
        ({"A": 0, "B": 0}, {}),
    ],
)
def test_load_section_parameters_filters_existing_parameters(
    autosubmit_config,
    submitter,
    mocker,
    existing: dict[str, int],
    expected: dict[str, int],
):
    """Test that existing parameters are not returned."""
    as_conf = autosubmit_config("a000")
    as_conf.check_conf_files = mocker.Mock()

    job = mocker.Mock()
    job.platform_name = "HPC"
    job.section = "SIM"
    job.parameters = {}

    def update_parameters(*_):
        job.parameters = { "A": 1,"B": 2 }

    job.update_parameters.side_effect = update_parameters

    job_list = mocker.Mock()
    job_list.get_job_list.return_value = [job]
    job_list.parameters = existing

    result = as_conf.load_section_parameters(job_list, as_conf, submitter)

    assert result == expected
