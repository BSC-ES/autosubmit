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

from autosubmit.job.template.bash import as_body, as_header, as_tailer

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from _pytest._py.path import LocalPath


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

    assert result.returncode == 127

    # stdout = result.stdout
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
    # is where Bash shell reports the error, not a line with a heredoc
    # anymore.
    assert f'line {error_line}: d_echo: command not found' in stderr, stderr
