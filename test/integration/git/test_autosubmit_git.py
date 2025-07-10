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

"""Integration tests for ``autosubmit_git``."""

from contextlib import nullcontext as does_not_raise
from getpass import getuser
from pathlib import Path
from typing import Callable, ContextManager

import pytest

from autosubmit.git.autosubmit_git import check_unpushed_changes
from log.log import AutosubmitCritical
from test.integration.test_utils.git import create_git_repository, git_commit_all_in_dir

_EXPID = 'a000'


def _get_experiment_data(tmp_path) -> dict:
    _user = getuser()

    return {
        'PLATFORMS': {
            'pytest-ps': {
                'type': 'ps',
                'host': '127.0.0.1',
                'user': _user,
                'project': 'whatever',
                'scratch': str(tmp_path / 'scratch'),
                'DISABLE_RECOVERY_THREADS': 'True'
            }
        },
        'JOBS': {
            'debug': {
                'SCRIPT': 'echo "Hello world"',
                'RUNNING': 'once'
            },
        },
        'PROJECT': {
            'PROJECT_DESTINATION': 'the_project',
        },
        'GIT': {
            'PROJECT_BRANCH': 'master',
            'PROJECT_COMMIT': '',
            'PROJECT_SUBMODULES': False,
            'FETCH_SINGLE_BRANCH': True
        }
    }


@pytest.mark.parametrize(
    "project_type,dirty,expid,expected",
    [
        ('git', True, 'o001', pytest.raises(AutosubmitCritical)),
        ('git', False, 'o001', does_not_raise()),
        ('local', True, 'o001', does_not_raise()),
        ('git', True, 'a001', does_not_raise()),
        ('local', True, 'a001', does_not_raise()),
        ('git', True, 't001', does_not_raise()),
        ('git', True, 'e001', does_not_raise()),
        ('git', False, 'e001', does_not_raise()),
    ]
)
def test_git_local_dirty(
        project_type: str,
        dirty: bool,
        expid: str,
        expected: ContextManager,
        tmp_path: Path,
        autosubmit_exp: Callable
) -> None:
    """Tests that Autosubmit detects dirty local Git repositories, especially with operational experiments."""
    git_repo = tmp_path / 'git_repository'

    create_git_repository(git_repo, bare=True)

    experiment_data = _get_experiment_data(tmp_path)
    experiment_data['PROJECT']['PROJECT_TYPE'] = project_type
    experiment_data['GIT']['PROJECT_ORIGIN'] = f'file://{str(git_repo)}'
    experiment_data['LOCAL'] = {
        'PROJECT_PATH': str(git_repo)
    }

    as_exp = autosubmit_exp(expid, experiment_data)
    as_conf = as_exp.as_conf
    proj_dir = Path(as_conf.get_project_dir())

    with open(proj_dir / 'a_file.yaml', 'w') as f:
        f.write('initial content')

    if project_type == 'git':
        git_commit_all_in_dir(proj_dir, push=True)

    if dirty:
        # Make the Git repository have changes/dirty
        with open(proj_dir / 'a_file.yaml', 'w') as f:
            f.write('modified content')

    with expected:
        check_unpushed_changes(expid, as_conf)
