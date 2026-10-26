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

import pytest


@pytest.mark.parametrize("is_owner", [True, False])
def test_is_current_real_user_owner(autosubmit_config, mocker, is_owner, tmp_path):
    """Test if the real user (``AS_ENV_CURRENT_USER``) owns the current experiment."""
    as_conf = autosubmit_config(
        expid='a000',
        experiment_data={"ROOTDIR": str(tmp_path), "AS_ENV_CURRENT_USER": "dummy"}
    )
    owner_uid = tmp_path.stat().st_uid
    mocker.patch(
        "pwd.getpwnam", return_value=mocker.Mock(pw_uid=owner_uid if is_owner else owner_uid + 1)
    )
    assert as_conf.is_current_real_user_owner == is_owner


@pytest.mark.parametrize("is_owner", [True, False])
def test_is_current_logged_user_owner(autosubmit_config, mocker, is_owner, tmp_path, monkeypatch):
    """Test that the ownership of the experiment is decided comparing UIDs."""
    as_conf = autosubmit_config(
        expid='a000',
        experiment_data={"ROOTDIR": f"{str(tmp_path)}"}
    )
    owner_uid = tmp_path.stat().st_uid
    mocker.patch("os.getuid", return_value=owner_uid if is_owner else owner_uid + 1)
    assert as_conf.is_current_logged_user_owner == is_owner
