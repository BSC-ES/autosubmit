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
from typing import Callable, ContextManager, Generator, Tuple, Union, cast

import pytest
from testcontainers.sftp import DockerContainer

from autosubmit.git.autosubmit_git import check_unpushed_changes, clean_git
from log.log import AutosubmitCritical
from test.integration.test_utils.git import (
    create_git_repository, git_commit_all_in_dir, git_clone_repository, git_add_submodule
)

_EXPID = 'a000'


# git operational check

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
    "project_type,commit,push,expid,expected",
    [
        ('git', False, False, 'o001', pytest.raises(AutosubmitCritical)),
        ('git', True, False, 'o001', pytest.raises(AutosubmitCritical)),
        ('git', True, True, 'o001', does_not_raise()),
        ('git', False, False, 'a001', does_not_raise()),
        ('git', True, False, 'a001', does_not_raise()),
        ('git', True, True, 'a001', does_not_raise()),
    ],
    ids=[
        'NOK: Did not commit nor push an operational experiment',
        'NOK: Committed but did not push an operational experiment',
        'OK: Commited and pushed an operational experiment',
        'OK: Did not commit nor push a common experiment',
        'OK: Committed but did not commit nor push a common experiment',
        'OK: Committed and pushed a common experiment'
    ]
)
@pytest.mark.docker
def test_git_local_dirty(
        project_type: str,
        commit: bool,
        push: bool,
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

    if project_type == 'git' and commit:
        git_commit_all_in_dir(proj_dir, push=push)

    with expected:
        check_unpushed_changes(expid, as_conf)


@pytest.mark.parametrize(
    "commit_git,push_git,commit_submodule,push_submodule,expid,expected",
    [
        (False, False, False, False, 'o001', pytest.raises(AutosubmitCritical)),
        (False, False, True, False, 'o001', pytest.raises(AutosubmitCritical)),
        (False, False, True, True, 'o001', pytest.raises(AutosubmitCritical)),
        (True, False, True, True, 'o001', pytest.raises(AutosubmitCritical)),
        (True, True, True, True, 'o001', does_not_raise()),
        (False, False, False, False, 'a001', does_not_raise()),
        (False, False, True, False, 'a001', does_not_raise()),
        (False, False, True, True, 'a001', does_not_raise()),
        (True, False, True, True, 'a001', does_not_raise()),
        (True, True, True, True, 'a001', does_not_raise()),
    ],
    ids=[
        'NOK: Did not commit nor push repository and submodule of an operational experiment',
        'NOK: Committed the submodule, but did not push it, nor committed or pushed the repository of an operational '
        'experiment',
        'NOK: Committed and pushed the submodule, but without committing and pushing the repository of an operational '
        'experiment',
        'NOK: Committed and pushed the submodule, committed but did not push the repository of an operational '
        'experiment',
        'OK: Committed and pushed the submodule, and committed and pushed the repository of an operational '
        'experiment',
        'OK: Did not commit nor push repository and submodule of a common experiment',
        'OK: Committed the submodule, but did not push it, nor committed or pushed the repository of a common '
        'experiment',
        'OK: Committed and pushed the submodule, but without committing and pushing the repository of a common '
        'experiment',
        'OK: Committed and pushed the submodule, committed but did not push the repository of a common experiment',
        'OK: Committed and pushed the submodule, and committed and pushed the repository of a common experiment',
    ]
)
@pytest.mark.docker
def test_git_submodules_dirty(
        commit_git: bool,
        push_git: bool,
        commit_submodule: bool,
        push_submodule: bool,
        expid: str,
        expected: ContextManager,
        autosubmit_exp: Callable,
        git_server: Generator[Tuple[DockerContainer, Path, str], None, None],
        tmp_path
) -> None:
    """Tests that Autosubmit detects dirty local Git submodules, especially with operational experiments.

    This test has a Git repository with a Git submodule. The parameters in this test control whether the
    Git repository and the Git submodule contents will be committed and pushed.

    If the user has non-committed or non-pushed changes in the repository or submodule, the code is
    expected to fail, raising an error when the experiment is operational.
    """

    _, git_repos_path, git_url = git_server  # type: DockerContainer, Path, str

    git_repo = git_repos_path / 'git_repository'
    git_submodule = git_repos_path / 'git_submodule'

    # Create main repository and submodule repository.
    create_git_repository(git_repo, bare=True)
    create_git_repository(git_submodule, bare=True)

    git_repo_url = f'{git_url}/{git_repo.name}'
    git_submodule_url = f'{git_url}/{git_submodule.name}'

    # Add submodule repository as a submodule in the main repository.
    temp_clone = tmp_path / 'git_clone'
    git_clone_repository(git_repo_url, temp_clone)
    submodule_name = git_submodule.name
    git_add_submodule(git_submodule_url, temp_clone, name=submodule_name, push=True)

    experiment_data = _get_experiment_data(tmp_path)
    experiment_data['PROJECT']['PROJECT_TYPE'] = 'git'
    experiment_data['GIT']['PROJECT_ORIGIN'] = git_repo_url
    experiment_data['GIT']['PROJECT_SUBMODULES'] = submodule_name
    experiment_data['LOCAL'] = {
        'PROJECT_PATH': str(git_repo)
    }

    as_exp = autosubmit_exp(expid, experiment_data)
    as_conf = as_exp.as_conf
    proj_dir = Path(as_conf.get_project_dir())

    with open(proj_dir / submodule_name / 'a_file.yaml', 'w') as f:
        f.write('modified content')

    if commit_submodule:
        git_commit_all_in_dir(proj_dir / submodule_name, push=push_submodule)

    if commit_git:
        git_commit_all_in_dir(proj_dir, push=push_git)

    with expected:
        check_unpushed_changes(expid, as_conf)


@pytest.mark.parametrize(
    'git_operational_check_enabled,expected',
    [
        (True, AutosubmitCritical),
        (False, 0)
    ]
)
@pytest.mark.docker
def test_git_operational_experiment_toggle_flag(
        git_operational_check_enabled: bool,
        expected: Union[int, Exception],
        autosubmit_exp: Callable,
        git_server: Generator[Tuple[DockerContainer, Path, str], None, None],
        tmp_path,
        mocker,
        autosubmit
) -> None:
    """Tests running an operational works as expected when the toggle flag is used.

    Users can configure ``CONFIG.GIT_OPERATIONAL_CHECK_ENABLED`` to toggle the Git
    operational check on and off.

    We launch the same experiment ``o001``, with a dirty Git repository, with and
    without that feature enabled and confirm that when the Git operational check is
    enabled, Autosubmit fails to launch the experiment. And when that's off, then
    Autosubmit ignores that, warning the user about it though.
    """
    mocked_log = mocker.patch('autosubmit.autosubmit.Log')
    expid = 'o001'

    _, git_repos_path, git_url = git_server  # type: DockerContainer, Path, str

    git_repo = git_repos_path / 'git_repository'

    create_git_repository(git_repo, bare=True)
    git_repo_url = f'{git_url}/{git_repo.name}'

    # Add submodule repository as a submodule in the main repository.
    temp_clone = tmp_path / 'git_clone'
    git_clone_repository(git_repo_url, temp_clone)

    experiment_data = _get_experiment_data(tmp_path)
    experiment_data['PROJECT']['PROJECT_TYPE'] = 'git'
    experiment_data['GIT']['PROJECT_ORIGIN'] = git_repo_url
    experiment_data['GIT']['PROJECT_SUBMODULES'] = ''

    if 'CONFIG' not in experiment_data:
        experiment_data['CONFIG'] = {}
    experiment_data['CONFIG']['GIT_OPERATIONAL_CHECK_ENABLED'] = git_operational_check_enabled

    as_exp = autosubmit_exp(expid, experiment_data)
    as_conf = as_exp.as_conf
    proj_dir = Path(as_conf.get_project_dir())

    # Dirty Git repository.
    with open(proj_dir / 'a_file.yaml', 'w') as f:
        f.write('modified content')

    mocker.patch('sys.argv', ['autosubmit', '-lc', 'DEBUG', '-lf', 'DEBUG', 'run', expid])
    _, args = autosubmit.parse_args()

    if type(expected) is int:
        exit_code = autosubmit.run_command(args=args)
        assert exit_code == expected

        assert mocked_log.warning.called
        assert mocked_log.warning.call_args[0][0] == 'Git operational check disabled by user'
    else:
        with pytest.raises(cast(expected, Exception)):
            autosubmit.run_command(args=args)


# -- clean_git

def test_clean_git_not_a_dir(autosubmit_config, mocker):
    """Test that cleaning Git fails when the project directory is actually a file."""
    mocked_log = mocker.patch('autosubmit.git.autosubmit_git.Log')
    as_conf = autosubmit_config(_EXPID, {})

    # Remove proj dir, replacing by a file.
    proj_dir = Path(as_conf.get_project_dir())
    proj_dir.parent.mkdir(parents=True, exist_ok=True)
    proj_dir.touch()

    assert not clean_git(as_conf)

    assert mocked_log.debug.call_args_list[1][0][0] == 'Not a directory... SKIPPING!'


def test_clean_git_not_a_git_repo(autosubmit_config, mocker):
    """Test that cleaning Git fails when the project directory is not a Git repository."""
    mocked_log = mocker.patch('autosubmit.git.autosubmit_git.Log')
    as_conf = autosubmit_config(_EXPID, {})

    # Missing .git folder.
    proj_dir = Path(as_conf.get_project_dir())
    proj_dir.mkdir(parents=True, exist_ok=True)

    assert not clean_git(as_conf)

    assert mocked_log.debug.call_args_list[1][0][0] == 'Not a git repository... SKIPPING!'


def test_clean_git_not_committed(
        tmp_path,
        autosubmit_exp,
        git_server: Generator[Tuple[DockerContainer, Path, str], None, None]
):
    """Test that cleaning Git fails when the project directory has new files not committed yet."""
    _, git_repos_path, git_url = git_server  # type: DockerContainer, Path, str

    git_repo = git_repos_path / test_clean_git_not_committed.__name__
    create_git_repository(git_repo, bare=True)

    git_repo_url = f'{git_url}/{git_repo.name}'

    experiment_data = _get_experiment_data(tmp_path)
    experiment_data['PROJECT']['PROJECT_TYPE'] = 'git'
    experiment_data['GIT']['PROJECT_ORIGIN'] = git_repo_url

    as_exp = autosubmit_exp(_EXPID, experiment_data)
    as_conf = as_exp.as_conf
    proj_dir = Path(as_conf.get_project_dir())

    Path(proj_dir / 'not_committed.txt').touch()

    with pytest.raises(AutosubmitCritical) as cm:
        clean_git(as_conf)

    assert str(cm.value.code) == '7013'


def test_clean_git_not_pushed(
        tmp_path,
        autosubmit_exp,
        git_server: Generator[Tuple[DockerContainer, Path, str], None, None]
):
    """Test that cleaning Git fails when the project directory has staged changed not pushed."""
    _, git_repos_path, git_url = git_server  # type: DockerContainer, Path, str

    git_repo = git_repos_path / test_clean_git_not_pushed.__name__
    create_git_repository(git_repo, bare=True)

    git_repo_url = f'{git_url}/{git_repo.name}'

    experiment_data = _get_experiment_data(tmp_path)
    experiment_data['PROJECT']['PROJECT_TYPE'] = 'git'
    experiment_data['GIT']['PROJECT_ORIGIN'] = git_repo_url

    as_exp = autosubmit_exp(_EXPID, experiment_data)
    as_conf = as_exp.as_conf
    proj_dir = Path(as_conf.get_project_dir())

    Path(proj_dir / 'not_committed.txt').touch()
    git_commit_all_in_dir(proj_dir, push=False)

    with pytest.raises(AutosubmitCritical) as cm:
        clean_git(as_conf)

    assert str(cm.value.code) == '7064'


def test_clean_git_set_git_project_commit_fails(
        tmp_path,
        autosubmit_exp,
        mocker,
        git_server: Generator[Tuple[DockerContainer, Path, str], None, None]
):
    """Test that cleaning Git fails when the project commit cannot be recorded."""
    _, git_repos_path, git_url = git_server  # type: DockerContainer, Path, str

    mocked_log = mocker.patch('autosubmit.git.autosubmit_git.Log')

    git_repo = git_repos_path / test_clean_git_not_pushed.__name__
    create_git_repository(git_repo, bare=True)

    git_repo_url = f'{git_url}/{git_repo.name}'

    experiment_data = _get_experiment_data(tmp_path)
    experiment_data['PROJECT']['PROJECT_TYPE'] = 'git'
    experiment_data['GIT']['PROJECT_ORIGIN'] = git_repo_url

    as_exp = autosubmit_exp(_EXPID, experiment_data)
    as_conf = as_exp.as_conf

    mocker.patch.object(as_conf, 'set_git_project_commit', return_value=False)

    assert not clean_git(as_conf)

    assert mocked_log.info.call_args_list[1][0][0] == 'Failed to set Git project commit... SKIPPING!'


def test_clean_git(
        tmp_path,
        autosubmit_exp,
        mocker,
        git_server: Generator[Tuple[DockerContainer, Path, str], None, None]
):
    """Test that cleaning Git fails when the project commit cannot be recorded."""
    _, git_repos_path, git_url = git_server  # type: DockerContainer, Path, str

    git_repo = git_repos_path / test_clean_git_not_pushed.__name__
    create_git_repository(git_repo, bare=True)

    git_repo_url = f'{git_url}/{git_repo.name}'

    experiment_data = _get_experiment_data(tmp_path)
    experiment_data['PROJECT']['PROJECT_TYPE'] = 'git'
    experiment_data['GIT']['PROJECT_ORIGIN'] = git_repo_url

    as_exp = autosubmit_exp(_EXPID, experiment_data)
    as_conf = as_exp.as_conf

    # TODO: remove when bug in config parser is fixed!
    mocker.patch.object(as_conf, 'set_git_project_commit', return_value=True)

    proj_dir = Path(as_conf.get_project_dir())
    Path(proj_dir / 'not_committed.txt').touch()
    git_commit_all_in_dir(proj_dir, push=True)

    assert proj_dir.exists()
    assert clean_git(as_conf)
    assert not proj_dir.exists()
