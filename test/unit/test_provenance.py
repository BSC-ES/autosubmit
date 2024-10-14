import pytest
from pathlib import Path
from autosubmit.autosubmit import Autosubmit
from log.log import AutosubmitCritical, Log
import os
from unittest.mock import patch

@pytest.fixture
def mock_paths(tmp_path):
    """
    Fixture to set temporary paths for BasicConfig values.
    """
    with patch('autosubmitconfigparser.config.basicconfig.BasicConfig.LOCAL_ROOT_DIR', str(tmp_path)), \
         patch('autosubmitconfigparser.config.basicconfig.BasicConfig.LOCAL_TMP_DIR', 'tmp'), \
         patch('autosubmitconfigparser.config.basicconfig.BasicConfig.LOCAL_ASLOG_DIR', 'ASLOGS'):
        yield tmp_path  # Provide the temporary path to the test

def test_provenance_rocrate_success(mock_paths):
    """
    Test the provenance function when rocrate=True and the process is successful.
    """
    with patch('autosubmit.autosubmit.Autosubmit.rocrate') as mock_rocrate, \
         patch('log.log.Log.info') as mock_log_info:
        
        expid = "expid123"

        exp_folder = os.path.join(str(mock_paths), expid)
        tmp_folder = os.path.join(exp_folder, 'tmp')  # Using the mocked value directly
        aslogs_folder = os.path.join(tmp_folder, 'ASLOGS')  # Using the mocked value directly
        expected_aslogs_path = aslogs_folder  # Adjust the path based on your expectations

        # Call the provenance function
        Autosubmit.provenance(expid, rocrate=True)

        # Assertions
        mock_rocrate.assert_called_once_with(expid, Path(expected_aslogs_path))
        mock_log_info.assert_called_once_with('RO-Crate ZIP file created!')
        
        assert Path(expected_aslogs_path) == mock_paths / "expid123/tmp/ASLOGS"

def test_provenance_rocrate_failure():
    with patch('autosubmit.autosubmit.Autosubmit.rocrate', side_effect=Exception("Mocked exception")) as mock_rocrate:
        
        # Testing when Autosubmit.rocrate fails
        with pytest.raises(AutosubmitCritical) as excinfo:
            Autosubmit.provenance("expid123", rocrate=True)

        # Assert that the correct error message is raised
        assert "Error creating RO-Crate ZIP file: Mocked exception" in str(excinfo)
        
        # Make sure that the rocrate method was called once
        mock_rocrate.assert_called_once()


def test_provenance_no_rocrate():
    with patch('autosubmit.autosubmit.Autosubmit.rocrate') as mock_rocrate:
        with pytest.raises(AutosubmitCritical) as excinfo:
            Autosubmit.provenance("expid123", rocrate=False)

        # Assert that the correct error message is raised
        assert "Can not create RO-Crate ZIP file" in str(excinfo)
        mock_rocrate.assert_not_called()  # Ensure rocrate was never called
