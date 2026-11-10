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

"""Unit tests for ``ParamikoSubmitter``."""

from getpass import getuser
from typing import TYPE_CHECKING, Union

import pytest

from autosubmit.log.log import AutosubmitCritical, AutosubmitError
from autosubmit.platforms import paramiko_submitter
from autosubmit.platforms.ecplatform import EcPlatform
from autosubmit.platforms.locplatform import LocalPlatform
from autosubmit.platforms.paramiko_submitter import (
    ParamikoSubmitter,
    get_platform_by_type,
)
from autosubmit.platforms.pjmplatform import PJMPlatform
from autosubmit.platforms.platform_type import PlatformType
from autosubmit.platforms.psplatform import PsPlatform
from autosubmit.platforms.slurmplatform import SlurmPlatform

if TYPE_CHECKING:
    from autosubmit.platforms.paramiko_platform import ParamikoPlatform

_EXPID = 't000'


@pytest.fixture
def experiment_data():
    return {"foo": "bar"}


def test_load_local_platform(autosubmit_config):
    """Test that the function to load the local platform (only) works."""
    as_conf = autosubmit_config(_EXPID, {})
    submitter = ParamikoSubmitter(as_conf=as_conf)

    assert len(submitter.platforms) == 2  # local and LOCAL, right?

    local_platform = submitter.platforms['local']
    assert isinstance(local_platform, LocalPlatform)

    assert local_platform.expid == as_conf.expid
    assert local_platform.name == 'local'


def test_load_platforms_only_local(autosubmit_config):
    """Test that loads the platforms without any experiment data, ensuring local is loaded anyway."""
    as_conf = autosubmit_config(_EXPID, {})
    submitter = ParamikoSubmitter(as_conf, None, None)

    assert len(submitter.platforms) == 2  # local and LOCAL, right?

    local_platform = submitter.platforms['local']
    assert isinstance(local_platform, LocalPlatform)

    assert local_platform.expid == as_conf.expid
    assert local_platform.name == 'local'


def test_platform_with_no_jobs(autosubmit_config):
    """Test that adding a platform but not referencing it results in the platform being discarded."""
    user = getuser()
    as_conf = autosubmit_config(_EXPID, {
        'PLATFORMS': {
            'MN5': {
                'TYPE': 'slurm',
                'USER': user,
                'HOST': 'marenostrum.bsc.es',
                'MAX_WALLCLOCK': '48:00',
            }
        },
        'JOBS': {
            'A': {
                'RUNNING': 'once',
                'SCRIPT': 'sleep 0',
                'PLATFORM': 'local'
            }
        }
    })
    submitter = ParamikoSubmitter(as_conf=as_conf, auth_password=None, local_auth_password=None)
    submitter.load_platforms(as_conf, None, None)

    assert len(submitter.platforms) == 2  # local and LOCAL, right?

    local_platform = submitter.platforms['local']
    assert isinstance(local_platform, LocalPlatform)

    assert local_platform.expid == as_conf.expid
    assert local_platform.name == 'local'

    assert 'MN5' not in submitter.platforms


def test_load_slurm_platform(autosubmit_config):
    """Test that we are able to load a Slurm platform."""
    user = getuser()
    as_conf = autosubmit_config(_EXPID, {
        'PLATFORMS': {
            'MN5': {
                'TYPE': 'slurm',
                'USER': user,
                'HOST': 'marenostrum.bsc.es',
                'MAX_WALLCLOCK': '48:00',
                'CUSTOM_DIRECTIVES': '[ "#SBATCH -n 2" ]'
            }
        },
        'JOBS': {
            'A': {
                'RUNNING': 'once',
                'SCRIPT': 'sleep 0',
                'PLATFORM': 'mn5'
            }
        }
    })
    submitter = ParamikoSubmitter(as_conf, None, None)

    assert len(submitter.platforms) == 3

    local_platform = submitter.platforms['local']
    assert isinstance(local_platform, LocalPlatform)

    assert local_platform.expid == as_conf.expid
    assert local_platform.name == 'local'

    assert 'MN5' in submitter.platforms
    assert 'SBATCH' in submitter.platforms['MN5'].custom_directives


@pytest.mark.parametrize(
    'experiment_data',
    [
        {
            'PLATFORMS': {
                'MN5-LOGIN': {
                    'TYPE': 'slurm',
                    'USER': '',
                    'HOST': 'marenostrum.bsc.es',
                    'MAX_WALLCLOCK': '02:00'
                },
                'MN5': {
                    'TYPE': 'slurm',
                    'USER': '',
                    'HOST': 'marenostrum.bsc.es',
                    'MAX_WALLCLOCK': '48:00',
                    'SERIAL_PLATFORM': 'MN5-LOGIN'
                }
            },
            'JOBS': {
                'A': {
                    'RUNNING': 'once',
                    'SCRIPT': 'sleep 0',
                    'PLATFORM': 'mn5',
                    'PROCESSORS': '1'
                }
            }
        },
        {
            'PLATFORMS': {
                'MN5-LOGIN': {
                    'TYPE': 'slurm',
                    'USER': '',
                    'HOST': 'marenostrum.bsc.es',
                    'MAX_WALLCLOCK': '02:00'
                },
                'MN5': {
                    'TYPE': 'slurm',
                    'USER': '',
                    'HOST': 'marenostrum.bsc.es',
                    'MAX_WALLCLOCK': '48:00',
                    'SERIAL_PLATFORM': 'MN5-LOGIN'
                }
            },
            'JOBS': {
                'A': {
                    'RUNNING': 'once',
                    'SCRIPT': 'sleep 0',
                    'PLATFORM': 'mn5',
                    'PROCESSORS': '1'
                },
                'B': {
                    'RUNNING': 'once',
                    'SCRIPT': 'sleep 0',
                    'PLATFORM': 'MN5-login',
                    'PROCESSORS': '1'
                }
            }
        }
    ],
    ids=[
        'Serial platform did not exist',
        'Serial platform already existed'
    ]
)
def test_serial_platform(experiment_data: dict, autosubmit_config):
    """Test that we are able to load a Slurm platform."""
    user = getuser()
    for platform, data in experiment_data['PLATFORMS'].items():
        data['USER'] = user
    as_conf = autosubmit_config(_EXPID, experiment_data=experiment_data)
    submitter = ParamikoSubmitter(as_conf, None, None)

    assert len(submitter.platforms) == 4

    slurm_platform = submitter.platforms['MN5']
    assert isinstance(slurm_platform, SlurmPlatform)

    assert slurm_platform.expid == as_conf.expid
    assert slurm_platform.name == 'MN5'

    assert 'MN5' in submitter.platforms
    assert 'MN5-LOGIN' in submitter.platforms


@pytest.mark.parametrize(
    'platform_type,expected_type_or_error',
    [
        ['ps', PsPlatform],
        ['ecaccess', EcPlatform],
        ['slurm', SlurmPlatform],
        ['pjm', PJMPlatform],
        ['abcd', AutosubmitCritical]
    ]
)
def test_platform_types(platform_type: str, expected_type_or_error: Union['ParamikoPlatform', Exception],
                        autosubmit_config):
    """Test that we are able to load a Slurm platform."""
    user = getuser()
    as_conf = autosubmit_config(_EXPID, {
        'PLATFORMS': {
            'sample': {
                'TYPE': platform_type,
                'USER': user,
                'HOST': 'sample.local',
                'MAX_WALLCLOCK': '48:00',
                'VERSION': 'slurm'  # For ecaccess, it requires another type
            }
        },
        'JOBS': {
            'A': {
                'RUNNING': 'once',
                'SCRIPT': 'sleep 0',
                'PLATFORM': 'sample'
            }
        }
    })
    if expected_type_or_error is AutosubmitCritical:
        with pytest.raises(expected_type_or_error):  # type: ignore
            ParamikoSubmitter(as_conf, None, None)
    else:
        submitter = ParamikoSubmitter(as_conf, None, None)
        assert len(submitter.platforms) == 3

        platform = submitter.platforms['sample']
        assert isinstance(platform, expected_type_or_error)  # type: ignore

        assert platform.expid == as_conf.expid
        assert platform.name == 'sample'


def test_ecplatform_fails_without_crashing(autosubmit_config):
    """Test that ecaccess platform is ignored when it does not have a version.

    Not sure if it should fail without crashing the execution, but... the
    current code silently ignores the platform.

    Note that the configuration contains the ecaccess platform. And a job is
    using it.

    However, there is no version in the ecaccess platform. It needs a version
    like PBS or Slurm platform, which will be used with/in conjunction (my
    understanding of the platform).

    In the end, the test verifies that there are two platforms loaded, which
    are always present, even though it's a single platform, the local, aliased
    as LOCAL (i.e. a dictionary with two entries to the same object, the local
    platform object).

    This code is quite confusing and error-prone, but having this test should
    be a good starting point.
    """
    user = getuser()
    as_conf = autosubmit_config(_EXPID, {
        'PLATFORMS': {
            'ecaccess': {
                'TYPE': 'ecaccess',
                'USER': user,
                'HOST': 'sample.local',
                'MAX_WALLCLOCK': '48:00',
                # MISSING VERSION!
            }
        },
        'JOBS': {
            'A': {
                'RUNNING': 'once',
                'SCRIPT': 'sleep 0',
                'PLATFORM': 'ecaccess'
            }
        }
    })
    submitter = ParamikoSubmitter(as_conf=as_conf, auth_password=None, local_auth_password=None)

    assert len(submitter.platforms) == 2

    assert 'ecaccess' not in submitter.platforms


@pytest.mark.parametrize(
    'hostname,add_project_to_host,expected_hostname',
    [
        ['marenostrum.bsc.es', False, 'marenostrum.bsc.es'],
        ['marenostrum.bsc.es', True, 'marenostrum.bsc.es-OCEAN'],
        ['a.bsc.es,b.bsc.es,c.bsc.es', True, 'a.bsc.es-OCEAN,b.bsc.es-OCEAN,c.bsc.es-OCEAN'],
    ]
)
def test_adding_project_to_host(hostname: str, add_project_to_host: bool, expected_hostname: str, autosubmit_config):
    """Test that adding platforms with hosts separated by comma, and with a project being added to host works."""
    user = getuser()
    as_conf = autosubmit_config(_EXPID, {
        'PLATFORMS': {
            'sample': {
                'TYPE': 'slurm',
                'USER': user,
                'HOST': hostname,
                'MAX_WALLCLOCK': '48:00',
                'PROJECT': 'OCEAN',
                'ADD_PROJECT_TO_HOST': str(add_project_to_host)
            }
        },
        'JOBS': {
            'A': {
                'RUNNING': 'once',
                'SCRIPT': 'sleep 0',
                'PLATFORM': 'sample'
            }
        }
    })
    submitter = ParamikoSubmitter(as_conf, None, None)

    assert len(submitter.platforms) == 3

    assert submitter.platforms['sample'].host == expected_hostname


def test_add_invalid_platform(autosubmit_config):
    """Test that an invalid platform raises ``AutosubmitError`` (i.e. no crash)."""
    user = getuser()
    as_conf = autosubmit_config(_EXPID, {
        'PLATFORMS': {
            'sample_invalid_scratch': {
                'TYPE': 'slurm',
                'USER': user,
                'MAX_WALLCLOCK': '48:00',
                'SCRATCH_DIR': 1
            }
        },
        'JOBS': {
            'A': {
                'RUNNING': 'once',
                'SCRIPT': 'sleep 0',
                'PLATFORM': 'sample_invalid_scratch'
            }
        }
    })

    with pytest.raises(AutosubmitError) as cm:
        ParamikoSubmitter(as_conf=as_conf)

    assert 'must be defined' in str(cm.value.message)


@pytest.mark.parametrize("parameter_name", ["TOTALJOBS", "MAX_WAITING_JOBS"])
def test_platform_parameter_zero_raises_error(autosubmit_config, parameter_name):
    """Test that setting a platform parameter to 0 raises ``AutosubmitCritical``."""
    user = getuser()
    as_conf = autosubmit_config(
        _EXPID,
        {
            "PLATFORMS": {
                "sample_zero_jobs": {
                    "TYPE": "slurm",
                    "USER": user,
                    "HOST": "sample_zero_jobs.bsc.es",
                    "MAX_WALLCLOCK": "48:00",
                    parameter_name: 0,
                }
            },
            "JOBS": {
                "A": {
                    "RUNNING": "once",
                    "SCRIPT": "sleep 0",
                    "PLATFORM": "sample_zero_jobs",
                }
            },
        },
    )

    with pytest.raises(AutosubmitCritical) as cm:
        ParamikoSubmitter(as_conf=as_conf)

    assert parameter_name in str(cm.value.message)
    assert "greater than 0" in str(cm.value.message)


@pytest.mark.parametrize("config", [
    {
        "DEFAULT": {
            "HPCARCH": "PYTEST-UNDEFINED",
        },
        "LOCAL_ROOT_DIR": "blabla",
        "LOCAL_TMP_DIR": 'tmp',
        "PLATFORMS": {
            "PYTEST-UNDEFINED": {
                "host": "",
                "user": "",
                "project": "",
                "scratch_dir": "",
                "MAX_WALLCLOCK": "",
                "DISABLE_RECOVERY_THREADS": True
            }
        },
        "JOBS": {
            "job1": {
                "PLATFORM": "PYTEST-UNDEFINED",
                "SCRIPT": "echo 'hello world'",
            },
        }
    },
    {
        "DEFAULT": {
            "HPCARCH": "PYTEST-UNSUPPORTED",
        },
        "LOCAL_ROOT_DIR": "blabla",
        "LOCAL_TMP_DIR": 'tmp',
        "PLATFORMS": {
            "PYTEST-UNSUPPORTED": {
                "TYPE": "unknown",
                "host": "",
                "user": "",
                "project": "",
                "scratch_dir": "",
                "MAX_WALLCLOCK": "",
                "DISABLE_RECOVERY_THREADS": True
            }
        },
        "JOBS": {
            "job1": {
                "PLATFORM": "PYTEST-UNSUPPORTED",
                "SCRIPT": "echo 'hello world'",
            },
        }
    }
], ids=["Undefined", "Unsupported"])
def test_load_platforms(autosubmit_config, config):
    experiment_id = 'random-id'
    as_conf = autosubmit_config(experiment_id, config)
    with pytest.raises(AutosubmitCritical):
        ParamikoSubmitter(as_conf=as_conf)


def test_get_platform_ecaccess(mocker, experiment_data):
    """Test creating an ECaccess platform."""
    instance = object()

    mocker.patch.dict(
        paramiko_submitter._PLATFORM_MAPPING,
        {
            PlatformType.ECACCESS: mocker.Mock(return_value=instance),
        },
    )

    result = get_platform_by_type(
        PlatformType.ECACCESS,
        "expid",
        "platform",
        experiment_data,
        "1.2.3",
        None,
    )

    assert result is instance
    paramiko_submitter._PLATFORM_MAPPING[PlatformType.ECACCESS].assert_called_once_with(  # type: ignore
        "expid",
        "platform",
        experiment_data,
        "1.2.3",
    )


def test_get_platform_slurm(mocker, experiment_data):
    """Test creating a Slurm platform."""
    instance = object()

    mocker.patch.dict(
        paramiko_submitter._PLATFORM_MAPPING,
        {
            PlatformType.SLURM: mocker.Mock(return_value=instance),
        },
    )

    result = get_platform_by_type(
        PlatformType.SLURM,
        "expid",
        "platform",
        experiment_data,
        "ignored",
        "secret",
    )

    assert result is instance
    paramiko_submitter._PLATFORM_MAPPING[PlatformType.SLURM].assert_called_once_with(  # type: ignore
        "expid",
        "platform",
        experiment_data,
        auth_password="secret",
    )


def test_get_platform_unsupported(mocker):
    """Test the request to create a platform that is not supported."""
    mocker.patch.object(
        paramiko_submitter,
        "PlatformType",
        side_effect=ValueError,
    )

    with pytest.raises(paramiko_submitter.AutosubmitCritical) as exc:
        get_platform_by_type(
            "unknown",
            "expid",
            "platform",
            {},
            "v1",
            None,
        )

    assert exc.value.args == (
        "Platform PLATFORM type unknown is not supported",
        6003,
    )


@pytest.mark.parametrize(
    "platform_type",
    [
        PlatformType.PS,
        PlatformType.PJM,
        PlatformType.PBS,
    ],
)
def test_get_platform_simple(mocker, experiment_data, platform_type):
    """Test creating platforms with the same constructor."""
    instance = object()
    factory = mocker.Mock(return_value=instance)

    mocker.patch.dict(
        paramiko_submitter._PLATFORM_MAPPING,
        {platform_type: factory},
    )

    result = get_platform_by_type(
        f" {platform_type.value.upper()} ",
        "expid",
        "platform",
        experiment_data,
        "v1",
        None,
    )

    assert result is instance
    factory.assert_called_once_with(
        "expid",
        "platform",
        experiment_data,
    )
