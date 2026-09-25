# Copyright 2015-2025 Earth Sciences Department, BSC-CNS
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

"""Integration tests for ``autosubmit_svn``."""
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from docker.models.containers import Container


import subprocess


# svn operational check

def _get_experiment_data() -> dict:
    return {
        'JOBS': {
            'debug': {
                'SCRIPT': 'echo "Hello world"',
                'RUNNING': 'once'
            },
        },
        'PROJECT': {
            'PROJECT_TYPE': 'svn',
            'PROJECT_DESTINATION': 'svn-project',
        },
        'SVN': {
            'PROJECT_URL': 'http://localhost/svn/svn-project',
            'PROJECT_REVISION': '1',
        },
        'CUSTOM_CONFIG': {
            'USER': 'svnadmin',
            'PASSWORD': 'test',
        },
    }


@pytest.fixture
def mock_svn(mocker):
    """Mocks external SVN CLI subprocess calls to eliminate network latency and Docker container overhead."""
    original_check_output = subprocess.check_output

    def _mock_check_output(cmd, *args, **kwargs):
        if isinstance(cmd, str) and 'svn' in cmd:
            # Match svn checkout command: "cd <local_proj_dir>; svn ... checkout -r <rev> <url> <dest>"
            if 'checkout' in cmd:
                parts = cmd.split(';')
                cd_part = parts[0].strip()
                local_proj_dir_str = cd_part.replace('cd ', '').strip()
                local_proj_dir = Path(local_proj_dir_str)
                proj_dest = local_proj_dir / 'svn-project'
                (proj_dest / 'branches').mkdir(parents=True, exist_ok=True)
                (proj_dest / 'tags').mkdir(parents=True, exist_ok=True)
                (proj_dest / 'trunk').mkdir(parents=True, exist_ok=True)
                return b"Checked out revision 1.\n"
            return b""
        return original_check_output(cmd, *args, **kwargs)

    mocker.patch('subprocess.check_output', side_effect=_mock_check_output)


@pytest.mark.svn
def test_svn_submodules_dirty(
        autosubmit_exp: Callable,
        mock_svn,
        tmp_path
) -> None:
    """Tests that Autosubmit detects dirty local svn submodules with mocked SVN calls.

    Mocking external SVN calls eliminates the ~2 minute network and Docker container latency.
    """
    experiment_data = _get_experiment_data()
    as_exp = autosubmit_exp('t001', experiment_data=experiment_data)
    proj_dir = Path(as_exp.as_conf.get_project_dir())

    assert proj_dir.parts[-1] == 'svn-project'
    assert (proj_dir / 'branches').exists()
    assert (proj_dir / 'tags').exists()
    assert (proj_dir / 'trunk').exists()

