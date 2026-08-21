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

"""Tests for ownership checks of experiments."""

import os
from getpass import getuser

import pytest

from autosubmit.experiment.utils import check_ownership, get_experiment_owner
from autosubmit.log.log import AutosubmitCritical


def test_get_experiment_owner(autosubmit_config):
    as_conf = autosubmit_config(expid="t000", experiment_data={})
    owner_username, owner_uid, is_owner, is_eadmin = get_experiment_owner(as_conf.expid)

    assert owner_username
    assert owner_uid
    assert is_eadmin is False
    assert is_owner is True


def test_check_ownership_different_owner(autosubmit_config, mocker):
    as_conf = autosubmit_config(expid="t000", experiment_data={})

    user = getuser()
    current_user_id = os.getuid()
    not_owner = f"not_{user}"

    mocker.patch("os.getuid", return_value=current_user_id + 42)

    r = mocker.MagicMock()
    r.pw_name = not_owner
    mocker.patch("pwd.getpwuid", return_value=r)

    owner_username, owner_uid, is_owner, is_eadmin = get_experiment_owner(as_conf.expid)

    assert owner_username
    assert owner_uid
    assert is_eadmin is False
    assert is_owner is False


def test_check_ownership_different_owner_exception(autosubmit_config, mocker):
    as_conf = autosubmit_config(expid="t000", experiment_data={})

    user = getuser()
    current_user_id = os.getuid()
    not_owner = f"not_{user}"

    mocker.patch("os.getuid", return_value=current_user_id + 42)

    r = mocker.MagicMock()
    r.pw_name = not_owner
    mocker.patch("pwd.getpwuid", return_value=r)

    with pytest.raises(AutosubmitCritical):
        check_ownership(as_conf.expid)


def test_check_ownership_with_eadmin(autosubmit_config, mocker):
    as_conf = autosubmit_config(expid="t000", experiment_data={})

    current_user_id = os.getuid()

    fake_uid = current_user_id + 42
    mocker.patch("os.getuid", return_value=fake_uid)

    # eadmin
    r = mocker.MagicMock
    r.pw_uid = fake_uid
    mocker.patch("pwd.getpwnam", return_value=r)

    _, _, is_owner, is_eadmin = get_experiment_owner(as_conf.expid)

    assert not is_owner
    assert is_eadmin is True


def test_check_ownership_missing_user(autosubmit_config, mocker):
    as_conf = autosubmit_config(expid="t000", experiment_data={})

    mocker.patch("pwd.getpwuid", side_effect=KeyError)

    _, _, is_owner, is_eadmin = get_experiment_owner(as_conf.expid)

    assert is_owner is True
    assert is_eadmin is False
