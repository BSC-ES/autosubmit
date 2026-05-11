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

"""This package contains unit tests for Bash templates."""

from subprocess import run
from textwrap import dedent
from typing import TYPE_CHECKING

import pytest

from autosubmit.job.template.bash import _DEFAULT_EXECUTABLE, as_body, as_header, as_tailer

if TYPE_CHECKING:
    from pathlib import Path
    # noinspection PyProtectedMember
    from _pytest._py.path import LocalPath


def _build_script(tmp_path: 'Path', body: str, executable: str = '/bin/bash') -> 'Path':
    """Assemble and write a runnable Bash script, returning its path."""
    h = as_header(platform_header='', executable=executable)
    b = as_body(dedent(body))
    t = as_tailer()

    script = '\n'.join([h, b, t])
    script = script.replace('%EXTENDED_HEADER%', '')
    script = script.replace('%EXTENDED_TAILER%', '')
    script = script.replace('%CURRENT_LOGDIR%', str(tmp_path))
    script = script.replace('%JOBNAME%', 't000_test')
    script = script.replace('%FAIL_COUNT%', '0')

    script_path = tmp_path / 'the_script.sh'
    script_path.write_text(script)
    script_path.chmod(0o755)
    return script_path


def test_header_default_executable_used_when_empty():
    """Use the default executable when an empty string is given."""
    header = as_header(platform_header='', executable='')
    assert header.startswith(f'#!{_DEFAULT_EXECUTABLE.strip()}')


def test_header_default_executable_used_when_none():
    """Use the default executable when ``None`` is given."""
    header = as_header(platform_header='', executable=None)
    assert header.startswith(f'#!{_DEFAULT_EXECUTABLE.strip()}')


def test_header_absolute_executable_produces_direct_shebang():
    """An absolute path executable is used directly in the shebang."""
    header = as_header(platform_header='', executable='/bin/bash')
    assert header.startswith('#!/bin/bash')


def test_header_bare_executable_uses_env():
    """A bare executable name is wrapped with ``/usr/bin/env``."""
    header = as_header(platform_header='', executable='bash')
    assert header.startswith('#!/usr/bin/env bash')


def test_header_platform_header_included():
    """The platform header is present in the output."""
    platform_header = '#SBATCH --job-name=test'
    header = as_header(platform_header=platform_header, executable='')
    assert platform_header in header


def test_header_autosubmit_section_present():
    """The Autosubmit header section marker is present."""
    header = as_header(platform_header='', executable='')
    assert 'Autosubmit header' in header


def test_header_trap_functions_present():
    """Signal trap functions are present in the header."""
    header = as_header(platform_header='', executable='')
    assert 'as_signals_handler' in header
    assert 'as_exit_handler' in header


def test_header_checkpoint_function_present():
    """The checkpoint function is present in the header."""
    header = as_header(platform_header='', executable='')
    assert 'as_checkpoint' in header


@pytest.mark.parametrize('executable,expected_shebang', [
    ('/bin/bash', '#!/bin/bash'),
    ('/usr/local/bin/bash', '#!/usr/local/bin/bash'),
    ('bash', '#!/usr/bin/env bash'),
])
def test_header_shebang_variants(executable: str, expected_shebang: str):
    """Various executable inputs produce the correct shebang line."""
    header = as_header(platform_header='', executable=executable)
    first_line = header.splitlines()[0]
    assert first_line == expected_shebang


def test_body_contains_user_code():
    """The user's code is present in the body."""
    body = as_body("echo hello")
    assert "echo hello" in body


def test_body_has_autosubmit_job_marker():
    """The Autosubmit job section marker is present."""
    body = as_body("echo hello")
    assert 'Autosubmit job' in body


def test_tailer_has_autosubmit_marker():
    """The Autosubmit tailer section marker is present."""
    tailer = as_tailer()
    assert 'Autosubmit tailer' in tailer


def test_tailer_has_wait():
    """The tailer contains a ``wait`` statement for background jobs."""
    tailer = as_tailer()
    assert 'wait' in tailer


def test_stat_file_has_completed_status_on_success(tmp_path: 'Path'):
    """A successful script writes ``COMPLETED`` as the last line of the ``_STAT_`` file."""
    script_path = _build_script(tmp_path, "echo 'ok'")
    result = run([str(script_path)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    stat_file = tmp_path / 't000_test_STAT_0'
    assert stat_file.exists()
    assert stat_file.read_text().strip().splitlines()[-1] == 'COMPLETED'
    assert (tmp_path / 't000_test_COMPLETED').exists()


def test_stat_file_written_on_success(tmp_path: 'Path'):
    """A successful script writes a ``_STAT_`` file with two timestamps and a status."""
    script_path = _build_script(tmp_path, "echo 'ok'")
    run([str(script_path)], capture_output=True, text=True)
    stat_file = tmp_path / 't000_test_STAT_0'
    assert stat_file.exists()
    lines = [line for line in stat_file.read_text().strip().splitlines() if line.strip()]
    assert len(lines) == 3  # start timestamp, end timestamp, COMPLETED


def test_stat_file_has_failed_status_on_failure(tmp_path: 'Path'):
    """A failing script writes ``FAILED`` as the last line of the ``_STAT_`` file."""
    script_path = _build_script(tmp_path, "exit 1")
    result = run([str(script_path)], capture_output=True, text=True)
    assert result.returncode != 0
    stat_file = tmp_path / 't000_test_STAT_0'
    assert stat_file.exists()
    assert stat_file.read_text().strip().splitlines()[-1] == 'FAILED'
    assert not (tmp_path / 't000_test_COMPLETED').exists()


def test_stat_file_written_on_failure(tmp_path: 'Path'):
    """A failing script still writes the ``_STAT_`` file (via EXIT trap)."""
    script_path = _build_script(tmp_path, "exit 1")
    run([str(script_path)], capture_output=True, text=True)
    assert (tmp_path / 't000_test_STAT_0').exists()


def test_checkpoint_file_created(tmp_path: 'Path'):
    """Calling ``as_checkpoint`` creates a ``_CHECKPOINT_N`` file."""
    script_path = _build_script(tmp_path, "as_checkpoint")
    run([str(script_path)], capture_output=True, text=True)
    assert (tmp_path / 't000_test_CHECKPOINT_1').exists()


def test_error_line_numbering(tmp_path: 'LocalPath'):
    h = as_header(platform_header='', executable='/bin/bash')
    b = as_body(dedent('''
    a="42"
    q="The answer?"
    echo $q
    d_echo "And the answer: "
    echo $a
    '''))
    t = as_tailer()

    the_script = '\n'.join([h, b, t])
    the_script_path = tmp_path / 'the_script.sh'

    # TODO: maybe it'd be better to have this as an integration test?
    the_script = the_script.replace('%EXTENDED_HEADER%', '')
    the_script = the_script.replace('%CURRENT_LOGDIR%', str(tmp_path))
    the_script = the_script.replace('%JOBNAME%', 't000_test')
    the_script = the_script.replace('%FAIL_COUNT%', '0')
    the_script_path.write_text(the_script)

    the_script_path.chmod(0o755)

    result = run([the_script_path], capture_output=True, text=True)

    assert result.returncode == 127  # 127 is nix exit code for cmd not found (d_echo)

    stderr = result.stderr

    assert "The answer?" in stderr, stderr
    assert "d_echo" in stderr, stderr

    assert the_script_path.exists()
    error_line = -1
    with open(the_script_path) as f:
        for i, line in enumerate(f, start=1):
            if 'd_echo' in line:
                error_line = i
                break

    # Here we verify that the first line where d_echo (typo) appears
    # is where Bash shell reports the error, not a line with a heredoc anymore.
    assert f'line {error_line}: d_echo: command not found' in stderr, stderr
