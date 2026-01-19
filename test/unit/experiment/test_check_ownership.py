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

"""Tests for ownership checks of experiments."""

import os
from getpass import getuser

import pytest

from autosubmit.experiment.experiment_common import check_ownership
from autosubmit.log.log import AutosubmitCritical


def test_check_ownership(autosubmit_config):
    as_conf = autosubmit_config(expid='t000', experiment_data={})
    owner, eadmin, current_owner = check_ownership(as_conf.expid, raise_error=False)

    assert owner
    assert eadmin is False
    assert current_owner


def test_check_ownership_different_owner(autosubmit_config, mocker):
    as_conf = autosubmit_config(expid='t000', experiment_data={})

    user = getuser()
    current_user_id = os.getuid()
    not_owner = f'not_{user}'

    mocker.patch('os.getuid', return_value=current_user_id + 42)

    r = mocker.MagicMock()
    r.pw_name = not_owner
    mocker.patch('pwd.getpwuid', return_value=r)

    owner, eadmin, current_owner = check_ownership(as_conf.expid, raise_error=False)

    assert not owner
    assert eadmin is False
    assert current_owner != owner


def test_check_ownership_different_owner_exception(autosubmit_config, mocker):
    as_conf = autosubmit_config(expid='t000', experiment_data={})

    user = getuser()
    current_user_id = os.getuid()
    not_owner = f'not_{user}'

    mocker.patch('os.getuid', return_value=current_user_id + 42)

    r = mocker.MagicMock()
    r.pw_name = not_owner
    mocker.patch('pwd.getpwuid', return_value=r)

    with pytest.raises(AutosubmitCritical):
        check_ownership(as_conf.expid, raise_error=True)


def test_check_ownership_with_eadmin(autosubmit_config, mocker):
    as_conf = autosubmit_config(expid='t000', experiment_data={})

    current_user_id = os.getuid()

    fake_uid = current_user_id + 42
    mocker.patch('os.getuid', return_value=fake_uid)

    # eadmin
    r = mocker.MagicMock
    r.pw_uid = fake_uid
    mocker.patch('pwd.getpwnam', return_value=r)

    owner, eadmin, current_owner = check_ownership(as_conf.expid, raise_error=False)

    assert not owner
    assert eadmin is True
    assert current_owner != owner


def test_check_ownership_missing_user(autosubmit_config, mocker):
    as_conf = autosubmit_config(expid='t000', experiment_data={})

    mocker.patch('pwd.getpwuid', side_effect=KeyError)

    owner, eadmin, current_owner = check_ownership(as_conf.expid, raise_error=False)

    assert owner is True
    assert eadmin is False
    assert current_owner is None
