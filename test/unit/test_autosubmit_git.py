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

"""Tests for ``AutosubmitGit``."""
from getpass import getuser
from pathlib import Path
from typing import Callable

import pytest
from pytest_mock import MockerFixture
from ruamel.yaml import YAML

from autosubmit.git.autosubmit_git import AutosubmitGit
from log.log import AutosubmitCritical

_EXPID = 'a000'


def test_submodules_fails_with_invalid_as_conf(mocker):
    as_conf = mocker.MagicMock()
    as_conf.is_valid_git_repository.return_value = False
    with pytest.raises(AutosubmitCritical):
        hpcarch = mocker.MagicMock()
        AutosubmitGit.clone_repository(as_conf=as_conf, force=True, hpcarch=hpcarch)


def test_submodules_empty_string(mocker, autosubmit_config):
    """Verifies that submodules configuration is processed correctly with empty strings."""
    as_conf = autosubmit_config(_EXPID, experiment_data={
        'GIT': {
            'PROJECT_ORIGIN': 'https://earth.bsc.es/gitlab/es/autosubmit.git',
            'PROJECT_BRANCH': 'master',
            'PROJECT_COMMIT': '123',
            'REMOTE_CLONE_ROOT': 'workflow',
            'PROJECT_SUBMODULES': ''
        },
        'PROJECT': {
            'PROJECT_DESTINATION': 'git_project'
        }
    })

    force = False
    hpcarch = mocker.Mock()
    AutosubmitGit.clone_repository(as_conf=as_conf, force=force, hpcarch=hpcarch)

    # Should be the last command, but to make sure we iterate all the commands.
    # A good improvement would have to break the function called into smaller
    # parts, like ``get_git_version``, ``clone_submodules(recursive=True)``, etc.
    # as that would be a lot easier to test.
    recursive_in_any_call = any([call for call in hpcarch.method_calls if
                                 'git submodule update --init --recursive' in str(call)])

    assert recursive_in_any_call


def test_submodules_list_not_empty(mocker, autosubmit_config):
    """Verifies that submodules configuration is processed correctly with a list of strings."""
    as_conf = autosubmit_config(_EXPID, experiment_data={
        'GIT': {
            'PROJECT_ORIGIN': 'https://earth.bsc.es/gitlab/es/autosubmit.git',
            'PROJECT_BRANCH': '',
            'PROJECT_COMMIT': '123',
            'REMOTE_CLONE_ROOT': 'workflow',
            'PROJECT_SUBMODULES': 'clone_me_a clone_me_b'
        },
        'PROJECT': {
            'PROJECT_DESTINATION': 'git_project'
        }
    })

    force = False
    hpcarch = mocker.Mock()
    AutosubmitGit.clone_repository(as_conf=as_conf, force=force, hpcarch=hpcarch)

    # Here the call happens in the hpcarch, not in subprocess
    clone_me_a_in_any_call = any([call for call in hpcarch.method_calls if
                                  'clone_me_a' in str(call)])

    assert clone_me_a_in_any_call


def test_submodules_false_disables_submodules(mocker, autosubmit_config):
    """Verifies that submodules are not used when users pass a False bool value."""
    as_conf = autosubmit_config(_EXPID, {
        'GIT': {
            'PROJECT_ORIGIN': 'https://earth.bsc.es/gitlab/es/autosubmit.git',
            'PROJECT_BRANCH': '',
            'PROJECT_COMMIT': '123',
            'REMOTE_CLONE_ROOT': 'workflow',
            'PROJECT_SUBMODULES': False
        },
        'PROJECT': {
            'PROJECT_DESTINATION': 'git_project'
        }
    })

    force = False
    hpcarch = mocker.Mock()
    AutosubmitGit.clone_repository(as_conf=as_conf, force=force, hpcarch=hpcarch)

    # Because we have ``PROJECT_SUBMODULES: False``, there must be no calls
    # to git submodules.
    any_one_used_submodules = any([call for call in hpcarch.method_calls if
                                   'submodules' in str(call)])

    assert not any_one_used_submodules


@pytest.mark.parametrize("config", [
    {
        "DEFAULT": {
            "HPCARCH": "PYTEST-UNDEFINED",
        },
        "LOCAL_ROOT_DIR": "blabla",
        "LOCAL_TMP_DIR": 'tmp',
        "PROJECT": {
            "PROJECT_DESTINATION": "git_project"
        },
        "PLATFORMS": {
            "PYTEST-UNDEFINED": {
                "host": "",
                "user": "",
                "project": "",
                "scratch_dir": "",
                "MAX_WALLCLOCK": "",
                "DISABLE_RECOVERY_THREADS": True
            }
        },
        "JOBS": {
            "job1": {
                "PLATFORM": "PYTEST-UNDEFINED",
                "SCRIPT": "echo 'hello world'",
            },
        }
    },
    {
        "DEFAULT": {
            "HPCARCH": "PYTEST-PS",
        },
        "LOCAL_ROOT_DIR": "blabla",
        "LOCAL_TMP_DIR": 'tmp',
        "PROJECT": {
            "PROJECT_DESTINATION": "git_project"
        },
        "PLATFORMS": {
            "PYTEST-PS": {
                "TYPE": "ps",
                "host": "",
                "user": "",
                "project": "",
                "scratch_dir": "",
                "MAX_WALLCLOCK": "",
                "DISABLE_RECOVERY_THREADS": True
            }
        },
        "JOBS": {
            "job1": {
                "PLATFORM": "PYTEST-PS",
                "SCRIPT": "echo 'hello world'",
            },
        }
    }], ids=["Git clone without type defined", "Git clone with the correct type defined"])
def test_copy_code(autosubmit_config, config, mocker, autosubmit):
    expid = 'random-id'
    as_conf = autosubmit_config(expid, config)
    mocker.patch('autosubmit.git.autosubmit_git.AutosubmitGit.clone_repository', return_value=True)
    assert autosubmit._copy_code(as_conf, expid, "git", True)


@pytest.mark.parametrize(
    "git_repo, expected",
    [
        ("https://github.com/user/repo.git", True),         # valid GH link
        ("file:///home/user/project", True),                # valid file link
        ("not-a-repo-link", False),                         # invalid
        ("git@github.com:user/repo.git", True),             # SSH format
        ("user@gitserver.com:user/repo.git", True),         # SSH format
        ("http://bitbucket.org/user/repo.git", True),       # valid git host
        ("ftp://invalid/protocol/repo.git", False),         # invalid protocol
        ("", False),                                        # empty string
        ("file://", False),                                 # incomplete file URL
        ("https://github.com/user/repo", False),            # missing .git
    ]
)
def test_valid_git_repo_check(git_repo: str, expected: str) -> None:
    assert AutosubmitGit.is_git_repo(git_repo) == expected


def _get_experiment_data(tmp_path):
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
        }
    }


@pytest.mark.parametrize(
    "project_type,expid",
    [
        ('git', 'o001'),
        ('git', 'a001'),
        ('git', 't001'),
        ('git', 'e001'),
    ]
)
def test_remote_repo_operational(project_type: str, expid: str, tmp_path: Path, autosubmit_exp: Callable,
                                 autosubmit, mocker: MockerFixture) -> None:
    """
    Tests the check_unpushed_changed function from AutosubmitGit.

    Ensures no operational test with unpushed changes in their Git repository is run.
    """
    as_exp = autosubmit_exp(expid, _get_experiment_data(tmp_path))
    run_dir = Path(as_exp.as_conf.basic_config.LOCAL_ROOT_DIR)
    temp_path = run_dir / expid / "conf" / f"expdef_{expid}.yml"

    with open(temp_path, 'r') as f:
        yaml = YAML(typ='rt')
        data = yaml.load(f)
    data["PROJECT"]["PROJECT_TYPE"] = project_type
    with open(temp_path, 'w') as f:
        yaml.dump(data, f)

    mocker.patch('subprocess.check_output', return_value=b'M\n')
    if expid[0] != 'o':
        AutosubmitGit.check_unpushed_changes(expid)
    else:
        with pytest.raises(AutosubmitCritical):
            AutosubmitGit.check_unpushed_changes(expid)
