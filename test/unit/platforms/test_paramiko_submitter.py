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
from typing import Union, TYPE_CHECKING

import pytest

from autosubmit.log.log import AutosubmitCritical, AutosubmitError
from autosubmit.platforms.ecplatform import EcPlatform
from autosubmit.platforms.locplatform import LocalPlatform
from autosubmit.platforms.paramiko_submitter import ParamikoSubmitter
from autosubmit.platforms.pjmplatform import PJMPlatform
from autosubmit.platforms.psplatform import PsPlatform
from autosubmit.platforms.slurmplatform import SlurmPlatform

if TYPE_CHECKING:
    from autosubmit.platforms.paramiko_platform import ParamikoPlatform

_EXPID = 't000'


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
