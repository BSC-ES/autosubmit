# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
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

"""Integration tests for Autosubmit script templates written in Bash Shell."""

from multiprocessing import Process
from os import kill
from pathlib import Path
from signal import SIGKILL, SIGTERM
from subprocess import run
from time import time, sleep
from typing import cast, TYPE_CHECKING

from autosubmit.job.template import bash

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from py._path.local import LocalPath  # type: ignore


def _replace_placeholders(content: str, tmp_path: 'LocalPath'):
    return content \
        .replace('%CURRENT_LOGDIR%', str(tmp_path)) \
        .replace('%JOBNAME%', 'test_job') \
        .replace('%FAIL_COUNT%', '1') \
        .replace('%EXTENDED_HEADER%', '') \
        .replace('%EXTENDED_TAILER%', '')


def test_successful_script(tmp_path: 'LocalPath'):
    """Test a Bash script running without errors."""
    header = _replace_placeholders(bash.as_header('', '/bin/bash'), tmp_path)
    body = _replace_placeholders(bash.as_body('touch OK && echo "OK!"'), tmp_path)
    footer = _replace_placeholders(bash.as_tailer(), tmp_path)

    bash_script_content = '\n'.join([header, body, footer])
    bash_script = tmp_path / 'test.sh'
    with open(bash_script, 'w+') as f:
        f.write(bash_script_content)

    bash_script.chmod(0o755)

    r = run([str(bash_script)], shell=True, capture_output=True, cwd=str(tmp_path))
    assert r.stdout == b'OK!\n', r.stderr

    assert Path(tmp_path, 'OK').exists()
    assert Path(tmp_path, 'test_job_COMPLETED').exists()
    assert Path(tmp_path, 'test_job_STAT_1').exists()


def test_command_err_script(tmp_path: 'LocalPath'):
    """Test a Bash script that executes a faulty command."""
    header = _replace_placeholders(bash.as_header('', '/bin/bash'), tmp_path)
    body = _replace_placeholders(bash.as_body('touch ERR && false'), tmp_path)
    footer = _replace_placeholders(bash.as_tailer(), tmp_path)

    bash_script_content = '\n'.join([header, body, footer])
    bash_script = tmp_path / 'test.sh'
    with open(bash_script, 'w+') as f:
        f.write(bash_script_content)

    bash_script.chmod(0o755)

    r = run([str(bash_script)], shell=True, capture_output=True, cwd=str(tmp_path))
    assert r.returncode == 1

    assert Path(tmp_path, 'ERR').exists()
    assert not Path(tmp_path, 'test_job_COMPLETED').exists()
    assert Path(tmp_path, 'test_job_STAT_1').exists()


def test_killed_script(tmp_path: 'LocalPath'):
    """Test a Bash script killed (e.g., SIGKILL by Slurm or user)."""
    header = _replace_placeholders(bash.as_header('', '/bin/bash'), tmp_path)
    body = _replace_placeholders(bash.as_body('sleep 15 && touch KILLED && false'), tmp_path)
    footer = _replace_placeholders(bash.as_tailer(), tmp_path)

    bash_script_content = '\n'.join([header, body, footer])
    bash_script = tmp_path / 'test.sh'
    with open(bash_script, 'w+') as f:
        f.write(bash_script_content)

    bash_script.chmod(0o755)

    def _run():
        r = run([str(bash_script)], shell=True, capture_output=True, cwd=str(tmp_path))
        assert r.returncode == 1

    p = Process(target=_run)
    p.start()

    kill(cast(int, p.pid), SIGKILL)

    p.join(timeout=5)

    assert not Path(tmp_path, 'KILLED').exists()
    assert not Path(tmp_path, 'test_job_COMPLETED').exists()
    assert not Path(tmp_path, 'test_job_STAT_1').exists()


def test_signalled_script(tmp_path: 'LocalPath'):
    """Test a Bash script that receives a linux signal (e.g., SIGTERM by Slurm or user)."""
    header = _replace_placeholders(bash.as_header('', '/bin/bash'), tmp_path)
    body = _replace_placeholders(bash.as_body("touch SIGNALLED\nsleep 10 &\n echo 'OK!'\n"), tmp_path)
    footer = _replace_placeholders(bash.as_tailer(), tmp_path)

    bash_script_content = '\n'.join([header, body, footer])
    bash_script = tmp_path / 'test.sh'
    with open(bash_script, 'w+') as f:
        f.write(bash_script_content)

    bash_script.chmod(0o755)

    def _run():
        r = run([str(bash_script)], shell=True, capture_output=True, cwd=str(tmp_path))
        assert r.returncode == 1

    p = Process(target=_run)
    p.start()

    # First, we ensure the SIGNALLED file was created, and the script is now
    # sleeping for 10 seconds...
    signalled_file = Path(tmp_path, 'SIGNALLED')
    must_end = time() + 30
    while time() < must_end and not signalled_file.exists():
        sleep(0.1)

    # ... and now we kill it in while it's sleeping! ARGH!
    kill(cast(int, p.pid), SIGTERM)
    p.join()

    assert signalled_file.exists()
    assert not Path(tmp_path, 'test_job_COMPLETED').exists()
    assert Path(tmp_path, 'test_job_STAT_1').exists(), "STAT file not created upon SIGTERM!"
