import os
import socket
import psutil
import pytest
from pathlib import Path
from autosubmit.helpers.utils import ASLock
from autosubmitconfigparser.config.basicconfig import BasicConfig
from log.log import AutosubmitCritical
class MockBasicConfig(BasicConfig):
    def read(self):
        self.LOCAL_ROOT_DIR = "/tmp"

@pytest.fixture
def mock_basic_config(monkeypatch):
    monkeypatch.setattr("autosubmit.helpers.utils.BasicConfig", MockBasicConfig)

def test_aslock(mock_basic_config):
    expid = "test_expid"
    current_hostname = socket.gethostname()
    current_pid = os.getpid()
    lock_file_path = Path("/tmp") / expid / "tmp" / "autosubmit.lock"

    # Ensure the directory exists
    lock_file_path.parent.mkdir(parents=True, exist_ok=True)

    # Test acquiring the lock
    with ASLock(expid):
        assert lock_file_path.exists(), "Lock file should be created"
        with open(lock_file_path, "r") as f:
            content = f.read()
            assert content == f"{current_hostname},{current_pid}", "Lock file content should match"

    # Test releasing the lock
    assert not lock_file_path.exists(), "Lock file should be removed after releasing"

def test_aslock_error(mock_basic_config):
    expid = "test_expid"
    lock_file_path = Path("/tmp") / expid / "tmp" / "autosubmit.lock"

    # Ensure the directory exists
    lock_file_path.parent.mkdir(parents=True, exist_ok=True)

    # Test acquiring the lock
    with ASLock(expid):
        assert lock_file_path.exists(), "Lock file should be created"
        # Test acquiring the lock again
        with pytest.raises(AutosubmitCritical):
            with ASLock(expid):
                pass
        assert lock_file_path.exists(), "Lock file should still exist after failing to acquire the lock"
        # Test another time
        with pytest.raises(AutosubmitCritical):
            with ASLock(expid):
                pass
        assert lock_file_path.exists(), "Lock file should still exist after the first failure"