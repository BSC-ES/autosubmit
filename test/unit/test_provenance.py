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
from pathlib import Path

import pytest

from autosubmit.autosubmit import Autosubmit
from log.log import AutosubmitCritical


@pytest.fixture
def mock_paths(tmp_path, mocker):
    """
    Fixture to set temporary paths for BasicConfig values.
    """
    mocker.patch('autosubmitconfigparser.config.basicconfig.BasicConfig.LOCAL_ROOT_DIR', str(tmp_path))
    mocker.patch('autosubmitconfigparser.config.basicconfig.BasicConfig.LOCAL_TMP_DIR', 'tmp')
    mocker.patch('autosubmitconfigparser.config.basicconfig.BasicConfig.LOCAL_ASLOG_DIR', 'ASLOGS')
    yield tmp_path


def test_provenance_rocrate_success(mock_paths, mocker):
    """
    Test the provenance function when rocrate=True and the process is successful.
    """
    mock_rocrate = mocker.patch('autosubmit.autosubmit.Autosubmit.rocrate')
    mock_log_info = mocker.patch('log.log.Log.info')

    expid = "expid123"
    exp_folder = os.path.join(str(mock_paths), expid)
    tmp_folder = os.path.join(exp_folder, 'tmp')
    aslogs_folder = os.path.join(tmp_folder, 'ASLOGS')
    expected_aslogs_path = aslogs_folder

    Autosubmit.provenance(expid, rocrate=True)

    mock_rocrate.assert_called_once_with(expid, Path(expected_aslogs_path))
    mock_log_info.assert_called_once_with('RO-Crate ZIP file created!')


def test_provenance_rocrate_failure(mocker):
    """
    Test the provenance function when Autosubmit.rocrate fails
    """
    mock_rocrate = mocker.patch('autosubmit.autosubmit.Autosubmit.rocrate', side_effect=Exception("Mocked exception"))

    with pytest.raises(AutosubmitCritical) as excinfo:
        Autosubmit.provenance("expid123", rocrate=True)

    assert "Error creating RO-Crate ZIP file: Mocked exception" in str(excinfo)

    mock_rocrate.assert_called_once()


def test_provenance_no_rocrate(mocker):
    """
    Test the provenance function when rocrate=False 
    """
    mock_rocrate = mocker.patch('autosubmit.autosubmit.Autosubmit.rocrate')
    with pytest.raises(AutosubmitCritical) as excinfo:
        Autosubmit.provenance("expid123", rocrate=False)

    assert "Can not create RO-Crate ZIP file. Argument '--rocrate' required" in str(excinfo)
    mock_rocrate.assert_not_called()
