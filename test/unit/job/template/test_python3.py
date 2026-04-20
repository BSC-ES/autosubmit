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

"""This package contains unit tests for Python 3 templates."""

import sys
from subprocess import run
from textwrap import dedent
from typing import TYPE_CHECKING

import pytest

from autosubmit.job.template.python3 import _DEFAULT_EXECUTABLE, as_body, as_header, as_tailer

if TYPE_CHECKING:
    from pathlib import Path

_JOBNAME = 't000_test'
_FAIL_COUNT = '0'


def _build_script(tmp_path: 'Path', body: str, executable: str = None) -> 'Path':
    """Assemble and write a runnable Python 3 script, returning its path."""
    executable = executable or sys.executable
    h = as_header(platform_header='', executable=executable)
    b = as_body(dedent(body))
    t = as_tailer()

    script = '\n'.join([h, b, t])
    script = script.replace('%EXTENDED_HEADER%', '')
    script = script.replace('%EXTENDED_TAILER%', '')
    script = script.replace('%CURRENT_LOGDIR%', str(tmp_path))
    script = script.replace('%JOBNAME%', _JOBNAME)
    script = script.replace('%FAIL_COUNT%', _FAIL_COUNT)

    script_path = tmp_path / 'the_script.py'
    script_path.write_text(script)
    script_path.chmod(0o755)
    return script_path


def test_header_default_executable_used_when_empty():
    """Use the default executable when an empty string is given."""
    header = as_header(platform_header='', executable='')
    assert _DEFAULT_EXECUTABLE.strip() in header.splitlines()[0]


def test_header_default_executable_used_when_none():
    """Use the default executable when ``None`` is given."""
    header = as_header(platform_header='', executable=None)
    assert _DEFAULT_EXECUTABLE.strip() in header.splitlines()[0]


def test_header_absolute_executable_produces_direct_shebang():
    """An absolute path executable is used directly in the shebang."""
    header = as_header(platform_header='', executable='/usr/bin/python3')
    assert header.startswith('#!/usr/bin/python3')


def test_header_bare_executable_uses_env():
    """A bare executable name is wrapped with ``/usr/bin/env``."""
    header = as_header(platform_header='', executable='python3')
    assert header.startswith('#!/usr/bin/env python3')


def test_header_platform_header_included():
    """The platform header is present in the output."""
    platform_header = '#SBATCH --job-name=test'
    header = as_header(platform_header=platform_header, executable='')
    assert platform_header in header


def test_header_autosubmit_section_present():
    """The Autosubmit header section marker is present."""
    header = as_header(platform_header='', executable='')
    assert 'Autosubmit header' in header


def test_header_checkpoint_function_present():
    """The checkpoint function is present in the header."""
    header = as_header(platform_header='', executable='')
    assert 'as_checkpoint' in header


@pytest.mark.parametrize('executable,expected_shebang', [
    ('/usr/bin/python3', '#!/usr/bin/python3'),
    ('/usr/local/bin/python3', '#!/usr/local/bin/python3'),
    ('python3', '#!/usr/bin/env python3'),
])
def test_header_shebang_variants(executable: str, expected_shebang: str):
    """Various executable inputs produce the correct shebang line."""
    header = as_header(platform_header='', executable=executable)
    first_line = header.splitlines()[0]
    assert first_line == expected_shebang


def test_body_contains_user_code():
    """The user's code is present in the body."""
    body = as_body("print('hello')")
    assert "print('hello')" in body


def test_body_has_autosubmit_job_marker():
    """The Autosubmit job section marker is present."""
    body = as_body("print('hello')")
    assert 'Autosubmit job' in body


def test_body_wraps_in_try_finally():
    """The body is wrapped in a try/finally block for stat file writing."""
    body = as_body("print('hello')")
    assert 'try:' in body
    assert 'finally:' in body


def test_tailer_has_autosubmit_marker():
    """The Autosubmit tailer section marker is present."""
    tailer = as_tailer()
    assert 'Autosubmit tailer' in tailer


def test_tailer_creates_completed_file():
    """The tailer contains the logic to create the ``_COMPLETED`` file."""
    tailer = as_tailer()
    assert '_COMPLETED' in tailer


def test_completed_file_created_on_success(tmp_path: 'Path'):
    """A successful script produces a ``_COMPLETED`` marker file."""
    script_path = _build_script(tmp_path, "print('ok')")
    result = run([sys.executable, str(script_path)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert (tmp_path / f'{_JOBNAME}_COMPLETED').exists()


def test_stat_file_written_on_success(tmp_path: 'Path'):
    """A successful script writes a ``_STAT_`` file with two timestamps."""
    script_path = _build_script(tmp_path, "print('ok')")
    run([sys.executable, str(script_path)], capture_output=True, text=True)
    stat_file = tmp_path / f'{_JOBNAME}_STAT_{_FAIL_COUNT}'
    assert stat_file.exists()
    lines = [line for line in stat_file.read_text().strip().splitlines() if line.strip()]
    assert len(lines) == 2  # start + end timestamps


def test_completed_file_not_created_on_failure(tmp_path: 'Path'):
    """A failing script does NOT produce a ``_COMPLETED`` marker file."""
    script_path = _build_script(tmp_path, "raise RuntimeError('fail')")
    result = run([sys.executable, str(script_path)], capture_output=True, text=True)
    assert result.returncode != 0
    assert not (tmp_path / f'{_JOBNAME}_COMPLETED').exists()


def test_stat_file_written_on_failure(tmp_path: 'Path'):
    """A failing script still writes the ``_STAT_`` file (via finally block)."""
    script_path = _build_script(tmp_path, "raise RuntimeError('fail')")
    run([sys.executable, str(script_path)], capture_output=True, text=True)
    assert (tmp_path / f'{_JOBNAME}_STAT_{_FAIL_COUNT}').exists()


def test_checkpoint_file_created(tmp_path: 'Path'):
    """Calling ``as_checkpoint()`` creates a ``_CHECKPOINT_N`` file."""
    script_path = _build_script(tmp_path, "as_checkpoint()")
    run([sys.executable, str(script_path)], capture_output=True, text=True)
    assert (tmp_path / f'{_JOBNAME}_CHECKPOINT_1').exists()


def test_multiple_checkpoints_create_sequential_files(tmp_path: 'Path'):
    """Each ``as_checkpoint()`` call creates a sequentially numbered file."""
    script_path = _build_script(
        tmp_path,
        dedent("""\
            as_checkpoint()
            as_checkpoint()
            as_checkpoint()
        """),
    )
    run([sys.executable, str(script_path)], capture_output=True, text=True)
    assert (tmp_path / f'{_JOBNAME}_CHECKPOINT_1').exists()
    assert (tmp_path / f'{_JOBNAME}_CHECKPOINT_2').exists()
    assert (tmp_path / f'{_JOBNAME}_CHECKPOINT_3').exists()
