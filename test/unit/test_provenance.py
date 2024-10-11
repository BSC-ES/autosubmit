import pytest
from pathlib import Path
from autosubmit.autosubmit import Autosubmit
from log.log import AutosubmitCritical
import os
from autosubmitconfigparser.config.basicconfig import BasicConfig

@pytest.fixture
def test_provenance_rocrate_success(mocker, tmp_path):
    """
    Test the provenance function when rocrate=True and the process is successful.
    """
    mock_rocrate = mocker.patch('autosubmit.autosubmit.Autosubmit.rocrate')
    mock_log_info = mocker.patch('Log.info')
    mock_basic_config = mocker.patch('BasicConfig')
    
    expid = "expid123"

    path = tmp_path / "expid123/tmp/ASLOGS"

    exp_folder = os.path.join(tmp_path, expid)

    tmp_folder = os.path.join(exp_folder, mock_basic_config.LOCAL_TMP_DIR) 
        
    aslogs_folder = os.path.join(tmp_folder, mock_basic_config.LOCAL_ASLOG_DIR) 

    expected_aslogs_path = os.path.join(tmp_folder, aslogs_folder) 

    Autosubmit.provenance("expid123", rocrate=True)

    mock_rocrate.assert_called_once_with("expid123", Path(expected_aslogs_path))
    mock_log_info.assert_called_once_with('RO-Crate ZIP file created!')

    assert expected_aslogs_path == path

def test_provenance_rocrate_failure(mocker):

    mock_rocrate = mocker.patch('autosubmit.autosubmit.Autosubmit.rocrate', side_effect=Exception("Mocked exception"))

    with pytest.raises(Exception) as excinfo:
        Autosubmit.provenance("expid123", rocrate=True)

    assert "Error creating RO-Crate ZIP file" in str(excinfo.value)

    mock_rocrate.assert_called_once()


def test_provenance_no_rocrate(mocker):

    mock_rocrate = mocker.patch('autosubmit.autosubmit.Autosubmit.rocrate')

    with pytest.raises(AutosubmitCritical) as excinfo:
        Autosubmit.provenance("expid123", rocrate=False)

    assert "Can not create RO-Crate ZIP file" in str(excinfo.value)

    mock_rocrate.assert_not_called()
