import subprocess
import sys
from pathlib import Path

from autosubmit.autosubmit import Autosubmit


def test_autosubmit_version():
    bin_path = Path(__file__, '../../../autosubmit/scripts/bin_autosubmit.py').resolve()
    exit_code, out = subprocess.getstatusoutput(' '.join([sys.executable, str(bin_path), '-v']))
    assert exit_code == 0
    assert out.strip().endswith(Autosubmit.autosubmit_version)

def test_autosubmit_version_broken():
    bin_path = Path(__file__, '../../../autosubmit/scripts/bin_autosubmit.py').resolve()
    exit_code, _ = subprocess.getstatusoutput(' '.join([sys.executable, str(bin_path), '-abcdefg']))
    assert exit_code == 1
