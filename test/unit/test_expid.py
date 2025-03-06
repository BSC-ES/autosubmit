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


""" Test file for autosubmit/autosubmit.py """

from contextlib import contextmanager

import pytest
from pathlib import Path

from autosubmit.autosubmit import Autosubmit
from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database import db_common
from autosubmit.log.log import AutosubmitCritical

@contextmanager
def does_not_raise():
    yield


def build_db_mock(current_experiment_id, mock_db_common, mocker):
    """
    function to help to connect with the database

    :param current_experiment_id:
    :param mock_db_common:
    :param mocker:
    :return:
    """
    mock_db_common.last_name_used = mocker.Mock(return_value=current_experiment_id)
    mock_db_common.check_experiment_exists = mocker.Mock(return_value=False)


@pytest.mark.parametrize('copy_id, expected', [
    ('', does_not_raise()),
    ('test', pytest.raises(AutosubmitCritical))
], ids=['success', 'fail'])
def test_expid(copy_id, expected, tmp_path, autosubmit_config, monkeypatch, autosubmit) -> None:
    """
    Function to test if the autosubmit().expid generates the paths and expid properly

    :param copy_id: to validate if statement is working properly and giving error as expected
    :param tmp_path: Path
    :return: None
    """
    autosubmit.install()
    monkeypatch.setattr(db_common, 'TIMEOUT', 1)
    with expected:
        expid = Autosubmit.expid("Test", copy_id=copy_id)
        experiment = Autosubmit.describe(expid)
        assert Path(BasicConfig.LOCAL_ROOT_DIR) / expid
        assert experiment is not None
        assert isinstance(expid, str) and len(expid) == 4
