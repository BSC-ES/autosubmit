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
from pathlib import Path
from typing import Callable, TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from docker.models.containers import Container


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
        },
        'CUSTOM_CONFIG': {
            'USER': 'svnadmin',
            'PASSWORD': 'test',
        },
    }


@pytest.mark.xfail(reason="Not working yet, needs to be fixed")
@pytest.mark.svn
@pytest.mark.docker
def test_svn_submodules_dirty(
        autosubmit_exp: Callable,
        svn_server: tuple['Container', Path, str],
        tmp_path
) -> None:
    """Tests that Autosubmit detects dirty local svn submodules, especially with operational experiments.

    This test has a svn repository with a svn submodule. The parameters in this test control whether the
    svn repository and the svn submodule contents will be committed and pushed.

    If the user has non-committed or non-pushed changes in the repository or submodule, the code is
    expected to fail, raising an error when the experiment is operational.
    """

    container, svn_repos_path, svn_url = svn_server  # type: Container, Path, str # type: ignore

    svn_repo = svn_repos_path / 'svn-project'

    experiment_data = _get_experiment_data()
    experiment_data['PROJECT']['PROJECT_TYPE'] = 'svn'
    experiment_data['SVN']['PROJECT_URL'] = f'{svn_url}/{svn_repo.name}'
    experiment_data['SVN']['PROJECT_REVISION'] = '1'
    experiment_data['CUSTOM_CONFIG']['USER'] = 'svnadmin'
    experiment_data['CUSTOM_CONFIG']['PASSWORD'] = 'test'

    as_exp = autosubmit_exp('t001', experiment_data=experiment_data)
    proj_dir = Path(as_exp.as_conf.get_project_dir())

    assert proj_dir.parts[-1] == 'svn-project'
    assert (proj_dir / 'branches').exists()
    assert (proj_dir / 'tags').exists()
    assert (proj_dir / 'trunk').exists()
