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

import os

import pytest


@pytest.mark.parametrize("is_owner", [True, False])
def test_is_current_real_user_owner(autosubmit_config, mocker, is_owner):
    """Test if the SUDO_USER from the environment matches the owner of the current experiment."""
    as_conf = autosubmit_config(
        expid='a000',
        experiment_data={"ROOTDIR": "/dummy/rootdir", "AS_ENV_CURRENT_USER": "dummy"}
    )
    mocker.patch("pathlib.Path.owner", return_value="dummy" if is_owner else "otheruser")
    assert as_conf.is_current_real_user_owner == is_owner


@pytest.mark.parametrize("is_owner", [True, False])
def test_is_current_logged_user_owner(autosubmit_config, mocker, is_owner):
    """Test if the USER from the environment matches the owner of the current experiment."""
    as_conf = autosubmit_config(
        expid='a000',
        experiment_data={"ROOTDIR": "/dummy/rootdir"}
    )
    os.environ["USER"] = "dummy" if is_owner else "otheruser"

    mocker.patch("pathlib.Path.owner", return_value="dummy")
    assert as_conf.is_current_logged_user_owner == is_owner
