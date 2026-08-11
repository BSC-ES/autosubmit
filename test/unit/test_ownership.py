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

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.experiment.utils import get_experiment_owner
from autosubmit.helpers.utils import check_experiment_ownership
from autosubmit.log.log import AutosubmitCritical


@pytest.fixture
def mock_owner_user_same(mocker):
    mock = mocker.patch("os.getuid")
    mock.return_value = 1  # user
    mock2 = mocker.patch("pathlib.Path.stat")
    mock2.return_value.st_uid = 1  # owner
    mock3 = mocker.patch("pwd.getpwuid")
    mock3.return_value.pw_name = "test1"
    return mock, mock2, mock3


@pytest.fixture
def mock_owner_user_diff(mocker):
    mock = mocker.patch("os.getuid")
    mock.return_value = 1  # user
    mock2 = mocker.patch("pathlib.Path.stat")
    mock2.return_value.st_uid = 2  # owner
    mock3 = mocker.patch("pwd.getpwuid")
    mock3.return_value.pw_name = "test1"
    return mock, mock2, mock3


def test_get_experiment_owner_same_owner(mock_owner_user_same):
    expid = "test_expid"
    owner_username, _, is_owner, is_eadmin = get_experiment_owner(expid)
    assert is_owner is True
    assert is_eadmin is False
    assert owner_username == "test1"


def test_get_experiment_owner_diff_owner(mock_owner_user_diff):
    expid = "test_expid"
    owner_username, _, is_owner, is_eadmin = get_experiment_owner(expid)
    assert is_owner is False
    assert is_eadmin is False
    assert owner_username == "test1"
    with pytest.raises(AutosubmitCritical):
        check_experiment_ownership(expid, BasicConfig, True)
