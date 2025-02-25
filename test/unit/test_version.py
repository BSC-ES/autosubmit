import subprocess
import sys
from pathlib import Path

from autosubmit.autosubmit import Autosubmit


def test_autosubmit_version():
    exit_code, out = subprocess.getstatusoutput('autosubmit -v')
    assert exit_code == 0
    assert out.strip().endswith(Autosubmit.autosubmit_version)

def test_autosubmit_version_broken():
    exit_code, _ = subprocess.getstatusoutput('autosubmit -abcdefg')
    assert exit_code == 1
