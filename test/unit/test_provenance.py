import os
from pathlib import Path
from unittest.mock import patch

import pytest

from autosubmit.autosubmit import Autosubmit
from log.log import AutosubmitCritical


@pytest.fixture
def mock_paths(tmp_path):
    """
    Fixture to set temporary paths for BasicConfig values.
    """
    with patch('autosubmitconfigparser.config.basicconfig.BasicConfig.LOCAL_ROOT_DIR', str(tmp_path)), \
         patch('autosubmitconfigparser.config.basicconfig.BasicConfig.LOCAL_TMP_DIR', 'tmp'), \
         patch('autosubmitconfigparser.config.basicconfig.BasicConfig.LOCAL_ASLOG_DIR', 'ASLOGS'):
        yield tmp_path  


def test_provenance_rocrate_success(mock_paths):
    """
    Test the provenance function when rocrate=True and the process is successful.
    """
    with patch('autosubmit.autosubmit.Autosubmit.rocrate') as mock_rocrate, \
         patch('log.log.Log.info') as mock_log_info:
        
        expid = "expid123"
        exp_folder = os.path.join(str(mock_paths), expid)
        tmp_folder = os.path.join(exp_folder, 'tmp')  
        aslogs_folder = os.path.join(tmp_folder, 'ASLOGS')  
        expected_aslogs_path = aslogs_folder  

        Autosubmit.provenance(expid, rocrate=True)

        mock_rocrate.assert_called_once_with(expid, Path(expected_aslogs_path))
        mock_log_info.assert_called_once_with('RO-Crate ZIP file created!')


def test_provenance_rocrate_failure():
    """
    Test the provenance function when Autosubmit.rocrate fails
    """
    with patch('autosubmit.autosubmit.Autosubmit.rocrate', side_effect=Exception("Mocked exception")) as mock_rocrate:
        
        with pytest.raises(AutosubmitCritical) as excinfo:
            Autosubmit.provenance("expid123", rocrate=True)

        assert "Error creating RO-Crate ZIP file: Mocked exception" in str(excinfo)

        mock_rocrate.assert_called_once()


def test_provenance_no_rocrate():
    """
    Test the provenance function when rocrate=False 
    """
    with patch('autosubmit.autosubmit.Autosubmit.rocrate') as mock_rocrate:
        with pytest.raises(AutosubmitCritical) as excinfo:
            Autosubmit.provenance("expid123", rocrate=False)

        assert "Can not create RO-Crate ZIP file. Argument '--rocrate' required" in str(excinfo)
        mock_rocrate.assert_not_called() 
