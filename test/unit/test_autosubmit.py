#!/usr/bin/env python3

# Copyright 2015-2020 Earth Sciences Department, BSC-CNS

# This file is part of Autosubmit.

# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

""" Test file for autosubmit/autosubmit.py """

from mock.mock import MagicMock, patch
from autosubmit.autosubmit import Autosubmit

def build_db_mock(current_experiment_id, mock_db_common):
    """
    function to help to connect with the database

    :param current_experiment_id:
    :param mock_db_common:
    :return:
    """
    mock_db_common.last_name_used = MagicMock(return_value=current_experiment_id)
    mock_db_common.check_experiment_exists = MagicMock(return_value=False)


@patch('autosubmit.experiment.experiment_common.db_common')
def test_expid(db_common_mock, tmp_path) -> None:
    """
    Function to test if the autosubmit().expid generates the paths and expid properly

    :param db_common_mock: Mock of the db_common
    :param tmp_path: Path
    :return: None
    """
    current_experiment_id = "empty"
    build_db_mock(current_experiment_id, db_common_mock)
    with patch('autosubmit.autosubmit.BasicConfig', MagicMock()) as fake_basic_config:
        # act
        fake_basic_config.STRUCTURES_DIR = fake_basic_config.LOCAL_ROOT_DIR = str(tmp_path)
        fake_basic_config.JOBDATA_DIR = str(tmp_path)
        fake_basic_config.read()

        expid = Autosubmit.expid("Test")
        experiment = Autosubmit.describe(expid)
        path = tmp_path / expid

        assert path.exists()
        assert experiment is not None
        assert isinstance(expid, str) and len(expid) == 4
