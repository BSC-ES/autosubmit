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
    assert pinned["DEFAULT"]["HPCARCH"] == "LOCAL"
    assert pinned["DEFAULT"]["OTHER"] == "value"


def test_immutable_variables_adds_missing_sections(
    autosubmit_config: "AutosubmitConfigFactory",
) -> None:
    """Test that the _pin_immutable_variables method adds missing sections and keys."""
    as_conf: AutosubmitConfig = autosubmit_config(expid="a000", experiment_data={})
    as_conf.starter_conf = {"DEFAULT": {"EXPID": "a000", "HPCARCH": "LOCAL"}}

    parameters = {}
    pinned = as_conf._pin_immutable_variables(parameters)

    # Check that missing DEFAULT section is added with original values
    assert pinned["DEFAULT"]["EXPID"] == "a000"
    assert pinned["DEFAULT"]["HPCARCH"] == "LOCAL"


def test_load_custom_config(autosubmit_config, tmp_path) -> None:
    as_conf: AutosubmitConfig = autosubmit_config(expid="a000", experiment_data={})

    git_project_dir = tmp_path / "proj" / "git_project"
    conf_dir = git_project_dir / "conf"
    conf_dir.mkdir(parents=True, exist_ok=True)

    # create common and real_from_ideal dirs with dummy files to be loaded as custom config
    (git_project_dir / "as_conf" / "common").mkdir(parents=True, exist_ok=True)
    (git_project_dir / "as_conf" / "real_from_ideal").mkdir(parents=True, exist_ok=True)

    as_conf.starter_conf = {
        "DEFAULT": {"EXPID": "a000", "HPCARCH": "LOCAL"},
        "CONFIG": {},
        "PROJDIR": str(git_project_dir),
    }

    current_data = {
        "DEFAULT": {"EXPID": "a000", "HPCARCH": "LOCAL"},
        "CONFIG": {},
        "PROJDIR": str(git_project_dir),
    }

    real_from_ideal_file = conf_dir / "real_from_ideal.yml"
    real_from_ideal_file.write_text(
        dedent("""\
        DEFAULT:
          CUSTOM_CONFIG:
            PRE:
              - "%PROJDIR%/as_conf/common"
              - "%PROJDIR%/as_conf/real_from_ideal"
    """)
    )

    common_file = conf_dir / "common_config.yml"
    common_file.write_text(
        dedent("""\
        DEFAULT:
          EXPID: "a001"
          HPCARCH: "MARENOSTRUM5"
          COMMON_CONFIG_VALUE: "common_value"
    """)
    )

    real_from_ideal_file = conf_dir / "real_from_ideal.yml"
    real_from_ideal_file.write_text(
        dedent("""\
        DEFAULT:
          EXPID: "a002"
          HPCARCH: "MARENOSTRUM5"
          REAL_CONFIG_VALUE: "real_from_ideal_value"
        REAL_FROM_IDEAL_VALUE: "real_from_ideal_value"
    """)
    )

    pre_config_dynamic_path = ["%PROJDIR%/conf"]

    data_pre, _ = as_conf.load_custom_config(current_data, pre_config_dynamic_path)

    # check pinned variables are not overwritten
    assert data_pre["DEFAULT"]["EXPID"] == "a000"
    assert data_pre["DEFAULT"]["HPCARCH"] == "LOCAL"
    # check default config from common and real_from_ideal are loaded
    assert data_pre["DEFAULT"]["COMMON_CONFIG_VALUE"] == "common_value"
    assert data_pre["DEFAULT"]["REAL_CONFIG_VALUE"] == "real_from_ideal_value"
    # check real_from_ideal values are loaded
    assert data_pre["REAL_FROM_IDEAL_VALUE"] == "real_from_ideal_value"
