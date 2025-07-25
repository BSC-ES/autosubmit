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
from textwrap import dedent
from typing import Callable

import pytest

from autosubmit.config.configcommon import AutosubmitConfig
from log.log import AutosubmitCritical

"""Basic tests for ``AutosubmitConfig``."""


def test_get_submodules_list_default_empty_list(autosubmit_config: Callable):
    """If nothing is provided, we get a list with an empty string."""
    as_conf: AutosubmitConfig = autosubmit_config(expid='a000', experiment_data={})
    submodules_list = as_conf.get_submodules_list()
    assert submodules_list == ['']


def test_get_submodules_list_returns_false(autosubmit_config: Callable):
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


def test_get_submodules_true_not_valid_value(autosubmit_config: Callable):
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


def test_get_submodules(autosubmit_config: Callable):
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
def test_is_current_real_user_owner(autosubmit_config: Callable, owner):
    as_conf = autosubmit_config(expid='a000', experiment_data={})
    as_conf.experiment_data = as_conf.load_common_parameters(as_conf.experiment_data)
    if owner:
        as_conf.experiment_data["AS_ENV_CURRENT_USER"] = Path(as_conf.experiment_data['ROOTDIR']).owner()
    else:
        as_conf.experiment_data["AS_ENV_CURRENT_USER"] = "dummy"
    assert as_conf.is_current_real_user_owner == owner


def test_clean_dynamic_variables(autosubmit_config: Callable) -> None:
    """
    This tests that only dynamic variables are kept in the ``dynamic_variables`` dictionary.
    a dynamic variable is a variable that it's value is a string that starts with ``%^`` or ``%`` and ends with ``%``.

    This tests that once called with the right arguments, ``clean_dynamic_variables`` will
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


def test_yaml_deprecation_warning(tmp_path, autosubmit_config: Callable):
    """Test that the conversion from YAML to INI works as expected, without warnings.

    Creates a dummy AS3 INI file, calls ``AutosubmitConfig.ini_to_yaml``, and
    verifies that the YAML files exists and is not empty, and a backup file was
    created. All without warnings being raised (i.e. they were suppressed).
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
    as_conf.ini_to_yaml(root_dir=tmp_path, ini_file=str(ini_file))

    backup_file = Path(f'{ini_file}_AS_v3_backup')
    assert backup_file.exists()
    assert backup_file.stat().st_size > 0

    new_yaml_file = Path(ini_file.parent, ini_file.stem).with_suffix('.yml')
    assert new_yaml_file.exists()
    assert new_yaml_file.stat().st_size > 0


def test_key_error_raise(autosubmit_config: Callable):
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
