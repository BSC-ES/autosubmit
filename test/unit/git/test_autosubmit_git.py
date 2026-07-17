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

from pathlib import Path
from subprocess import CalledProcessError

import pytest

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.git.autosubmit_git import (
    check_unpushed_changes,
    clone_repository,
    is_git_repo,
)
from autosubmit.log.log import AutosubmitCritical

_EXPID = 'a000'


def test_submodules_empty_string(mocker, autosubmit_config):
    """Verifies that submodule configuration is processed correctly with empty strings."""
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

    platform = mocker.Mock()

    submitter_cls = mocker.patch("autosubmit.git.autosubmit_git.ParamikoSubmitter")
    submitter = submitter_cls.return_value

    hpcarch = as_conf.get_platform()
    submitter.platforms = {
        hpcarch: platform,
    }
    
    clone_repository(as_conf=as_conf, force=force)

    # Should be the last command, but to make sure we iterate all the commands.
    # A good improvement would have to break the function called into smaller
    # parts, like ``get_git_version``, ``clone_submodules(recursive=True)``, etc.
    # as that would be a lot easier to test.
    recursive_in_any_call = any([call for call in platform.method_calls if
                                 'git submodule update --init --recursive' in str(call)])

    assert recursive_in_any_call


def test_submodules_list_not_empty(mocker, autosubmit_config):
    """Verifies that submodule configuration is processed correctly with a list of strings."""
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

    platform = mocker.Mock()

    submitter_cls = mocker.patch("autosubmit.git.autosubmit_git.ParamikoSubmitter")
    submitter = submitter_cls.return_value

    hpcarch = as_conf.get_platform()
    submitter.platforms = {
        hpcarch: platform,
    }
    
    clone_repository(as_conf=as_conf, force=force)

    # Here the call happens in the hpcarch, not in subprocess
    clone_me_a_in_any_call = any([call for call in platform.method_calls if
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

    platform = mocker.Mock()

    submitter_cls = mocker.patch("autosubmit.git.autosubmit_git.ParamikoSubmitter")
    submitter = submitter_cls.return_value

    hpcarch = as_conf.get_platform()
    submitter.platforms = {
        hpcarch: platform,
    }
    
    clone_repository(as_conf=as_conf, force=force)

    # Because we have ``PROJECT_SUBMODULES: False``, there must be no calls
    # to git submodules.
    any_one_used_submodules = any([call for call in platform.method_calls if
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
                "DISABLE_RECOVERY_THREADS": True,
                "TYPE": "ps"
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
    mocker.patch('autosubmit.autosubmit.clone_repository', return_value=True)
    assert autosubmit._copy_code(as_conf, expid, "git", True)


def test_clone_repository_falls_back_to_local_platform(autosubmit_config, mocker) -> None:
    """Verify that an unknown platform falls back to the local platform."""
    as_conf = autosubmit_config(_EXPID, experiment_data={
        'PROJECT': {
            'PROJECT_TYPE': 'GIT'
        },
        'GIT': {
            'PROJECT_ORIGIN': 'bla'
        }
    })

    platform = mocker.Mock()

    submitter_cls = mocker.patch("autosubmit.git.autosubmit_git.ParamikoSubmitter")
    submitter = submitter_cls.return_value

    submitter.platforms = {"local": platform}

    as_conf.get_platform = lambda: "UNKNOWN"

    mocker.patch(
        "autosubmit.git.autosubmit_git.subprocess.check_output",
        side_effect=[
            b"git version 2.39.0\n",
            b"",
            b"",
        ],
    )

    clone_repository(as_conf, False)


def test_clone_repository_empty_submodule_depth_list(autosubmit_config, mocker) -> None:
    """Verify that a recursive submodule update is used when no depth is configured."""
    as_conf = autosubmit_config(
        _EXPID,
        experiment_data={
            "GIT": {
                "PROJECT_ORIGIN": "https://github.com/user/repo.git",
                "PROJECT_BRANCH": "main",
                "PROJECT_COMMIT": "",
                "REMOTE_CLONE_ROOT": "workflow",
                "PROJECT_SUBMODULES": "",
            },
            "PROJECT": {
                "PROJECT_TYPE": "GIT",
                "PROJECT_DESTINATION": "git_project",
            },
        },
    )

    # Force the branch under test.
    as_conf.get_project_submodules_depth = lambda: []
    platform = mocker.Mock()
    submitter_cls = mocker.patch("autosubmit.git.autosubmit_git.ParamikoSubmitter"
    )
    submitter_cls.return_value.platforms = {
        as_conf.get_platform(): platform,
    }

    clone_repository(as_conf, False)

    assert any(
        "--recursive" in str(call)
        and "--depth" not in str(call)
        for call in platform.method_calls
    )


def test_clone_repository_single_branch_clone(autosubmit_config, mocker) -> None:
    """Verify that --single-branch is used when configured."""
    as_conf = autosubmit_config(
        _EXPID,
        experiment_data={
            "GIT": {
                "FETCH_SINGLE_BRANCH": "true",
                "PROJECT_ORIGIN": "https://github.com/user/repo.git",
                "PROJECT_BRANCH": "main",
                "PROJECT_COMMIT": "",
                "REMOTE_CLONE_ROOT": "workflow",
            },
            "PROJECT": {
                "PROJECT_DESTINATION": "git_project",
            },
        },
    )

    platform = mocker.Mock()
    submitter_cls = mocker.patch("autosubmit.git.autosubmit_git.ParamikoSubmitter")
    submitter_cls.return_value.platforms = {
        as_conf.get_platform(): platform,
    }

    clone_repository(as_conf, False)

    assert any(
        "--single-branch" in str(call)
        for call in platform.method_calls
    )


def test_clone_repository_existing_project_skips_clone(autosubmit_config, mocker) -> None:
    """Verify that cloning is skipped when the project already exists."""
    as_conf = autosubmit_config(_EXPID, experiment_data={
        "PROJECT": {
            "PROJECT_TYPE": "GIT"
        },
        "GIT": {
            "PROJECT_ORIGIN": "https://github.com/user/repo.git"
        }
    })

    mocker.patch("autosubmit.git.autosubmit_git.os.path.exists", return_value=True)

    assert clone_repository(as_conf, False)


def test_clone_repository_force_removes_existing_project(autosubmit_config, mocker, tmp_path: Path) -> None:
    """Verify that ``force=True`` removes the existing project directory."""
    as_conf = autosubmit_config(
        _EXPID,
        experiment_data={
            "GIT": {
                "PROJECT_ORIGIN": "https://github.com/user/repo.git",
                "PROJECT_BRANCH": "main",
                "PROJECT_COMMIT": "",
                "REMOTE_CLONE_ROOT": "workflow",
            },
            "PROJECT": {
                "PROJECT_TYPE": "GIT",
                "PROJECT_DESTINATION": "git_project",
            },
        },
    )

    platform = mocker.Mock()

    submitter_cls = mocker.patch("autosubmit.git.autosubmit_git.ParamikoSubmitter")
    submitter_cls.return_value.platforms = {
        as_conf.get_platform(): platform,
    }

    project_path = tmp_path / as_conf.expid / "proj"
    destination = project_path / as_conf.get_project_destination()

    def fake_exists(path: str) -> bool:
        """Return True only for the paths used by this test."""
        path = Path(path)
        return path == project_path or path == destination

    mocker.patch("autosubmit.git.autosubmit_git.BasicConfig.LOCAL_ROOT_DIR", str(tmp_path))
    mocker.patch("autosubmit.git.autosubmit_git.os.path.exists", side_effect=fake_exists)

    move = mocker.patch("autosubmit.git.autosubmit_git.shutil.move")
    rmtree = mocker.patch("autosubmit.git.autosubmit_git.shutil.rmtree")

    clone_repository(as_conf, True)

    move.assert_called_once()
    rmtree.assert_called_once()


def test_clone_repository_force_creates_backup(
    autosubmit_config,
    mocker,
) -> None:
    """Verify that ``force=True`` creates a backup of an existing project."""
    as_conf = autosubmit_config(
        _EXPID,
        experiment_data={
            "GIT": {
                "PROJECT_ORIGIN": "https://github.com/user/repo.git",
                "PROJECT_BRANCH": "main",
                "PROJECT_COMMIT": "",
                "REMOTE_CLONE_ROOT": "workflow",
            },
            "PROJECT": {
                "PROJECT_TYPE": "GIT",
                "PROJECT_DESTINATION": "git_project",
            },
        },
    )

    platform = mocker.Mock()

    submitter_cls = mocker.patch("autosubmit.git.autosubmit_git.ParamikoSubmitter")
    submitter_cls.return_value.platforms = {
        as_conf.get_platform(): platform,
    }

    project_path = (
        Path(BasicConfig.LOCAL_ROOT_DIR)
        / as_conf.expid
        / BasicConfig.LOCAL_PROJ_DIR
    )

    def fake_exists(path: str) -> bool:
        return path == str(project_path)

    mocker.patch("autosubmit.git.autosubmit_git.os.path.exists", side_effect=fake_exists)
    move = mocker.patch("autosubmit.git.autosubmit_git.shutil.move")
    mocker.patch("autosubmit.git.autosubmit_git.shutil.rmtree")

    clone_repository(as_conf, True)

    move.assert_called_once()


def test_clone_repository_without_single_branch(autosubmit_config, mocker) -> None:
    """Verify that a normal Git clone is used when ``FETCH_SINGLE_BRANCH`` is disabled."""
    as_conf = autosubmit_config(
        _EXPID,
        experiment_data={
            "GIT": {
                "FETCH_SINGLE_BRANCH": "false",
                "PROJECT_ORIGIN": "https://github.com/user/repo.git",
                "PROJECT_BRANCH": "main",
                "PROJECT_COMMIT": "",
                "REMOTE_CLONE_ROOT": "workflow",
            },
            "PROJECT": {
                "PROJECT_TYPE": "GIT",
                "PROJECT_DESTINATION": "git_project",
            },
        },
    )

    platform = mocker.Mock()

    submitter_cls = mocker.patch("autosubmit.git.autosubmit_git.ParamikoSubmitter")
    submitter_cls.return_value.platforms = {
        as_conf.get_platform(): platform,
    }

    clone_repository(as_conf, False)

    assert any(
        "git clone -b main" in str(call)
        and "--single-branch" not in str(call)
        for call in platform.method_calls
    )


def test_clone_repository_without_branch_omits_flag(autosubmit_config, mocker) -> None:
    """Verify that clone omits ``-b`` when ``PROJECT_BRANCH`` is empty (auto-detect default branch)."""
    as_conf = autosubmit_config(
        _EXPID,
        experiment_data={
            "GIT": {
                "PROJECT_ORIGIN": "https://github.com/user/repo.git",
                "PROJECT_BRANCH": "",
                "PROJECT_COMMIT": "",
                "REMOTE_CLONE_ROOT": "workflow",
            },
            "PROJECT": {
                "PROJECT_TYPE": "GIT",
                "PROJECT_DESTINATION": "git_project",
            },
        },
    )

    platform = mocker.Mock()
    submitter_cls = mocker.patch("autosubmit.git.autosubmit_git.ParamikoSubmitter")
    submitter_cls.return_value.platforms = {
        as_conf.get_platform(): platform,
    }

    clone_repository(as_conf, False)

    clone_calls = [
        str(call) for call in platform.method_calls
        if "git clone" in str(call)
    ]
    assert len(clone_calls) > 0
    assert all("-b " not in call for call in clone_calls)
    assert all("--single-branch" not in call for call in clone_calls)


def test_clone_repository_force_without_existing_project_skips_backup(autosubmit_config, mocker) -> None:
    """Verify that force=True does not create a backup if the project does not exist."""
    as_conf = autosubmit_config(
        _EXPID,
        experiment_data={
            "GIT": {
                "PROJECT_ORIGIN": "https://github.com/user/repo.git",
                "PROJECT_BRANCH": "main",
                "PROJECT_COMMIT": "",
                "REMOTE_CLONE_ROOT": "workflow",
            },
            "PROJECT": {
                "PROJECT_TYPE": "GIT",
                "PROJECT_DESTINATION": "git_project",
            },
        },
    )

    platform = mocker.Mock()

    submitter_cls = mocker.patch("autosubmit.git.autosubmit_git.ParamikoSubmitter")
    submitter_cls.return_value.platforms = {
        as_conf.get_platform(): platform,
    }

    mocker.patch("autosubmit.git.autosubmit_git.os.path.exists", return_value=False)

    move = mocker.patch("autosubmit.git.autosubmit_git.shutil.move")
    rmtree = mocker.patch("autosubmit.git.autosubmit_git.shutil.rmtree")

    clone_repository(as_conf, True)

    move.assert_not_called()
    rmtree.assert_not_called()


def test_clone_repository_invalid_git_configuration(autosubmit_config):
    as_conf = autosubmit_config(
        _EXPID,
        experiment_data={
            "GIT": {
                "PROJECT_ORIGIN": "",
                "PROJECT_BRANCH": "",
                "PROJECT_COMMIT": "",
            },
            "PROJECT": {
                "PROJECT_TYPE": "GIT",
            },
        },
    )

    with pytest.raises(AutosubmitCritical):
        clone_repository(as_conf, True)


def test_clone_repository_fails_loading_platforms(autosubmit_config, mocker) -> None:
    """Verify that failing to load platforms raises AutosubmitCritical."""
    as_conf = autosubmit_config(
        _EXPID,
        experiment_data={
            "GIT": {
                "PROJECT_ORIGIN": "https://github.com/user/repo.git",
                "PROJECT_BRANCH": "main",
                "PROJECT_COMMIT": "",
            },
            "PROJECT": {
                "PROJECT_TYPE": "GIT",
                "PROJECT_DESTINATION": "git_project",
            },
        },
    )

    mocker.patch(
        "autosubmit.git.autosubmit_git.ParamikoSubmitter",
        side_effect=RuntimeError("platform loading failed"),
    )

    with pytest.raises(
        AutosubmitCritical,
        match="Failed to load the Autosubmit platforms: platform loading failed",
    ):
        clone_repository(as_conf, False)


@pytest.mark.parametrize("walk_result", [
    [("/proj/.githooks", ["nested"], ["pre-commit"])],
    [("/proj/.githooks", [], [])],
])
def test_clone_repository_githooks_with_git_version_fallback(autosubmit_config, mocker, walk_result) -> None:
    """Verify githooks are configured when git version detection fails."""
    as_conf = autosubmit_config(_EXPID, experiment_data={
        "GIT": {
            "PROJECT_ORIGIN": "https://earth.bsc.es/gitlab/es/autosubmit.git",
            "PROJECT_BRANCH": "main",
            "PROJECT_COMMIT": "",
            "REMOTE_CLONE_ROOT": "workflow",
        },
        "PROJECT": {
            "PROJECT_TYPE": "GIT",
            "PROJECT_DESTINATION": "git_project",
        },
    })

    platform = mocker.Mock()
    submitter_cls = mocker.patch("autosubmit.git.autosubmit_git.ParamikoSubmitter")
    submitter_cls.return_value.platforms = {as_conf.get_platform(): platform}

    mocker.patch("autosubmit.git.autosubmit_git.subprocess.check_output",
                 side_effect=[Exception("git unavailable"), b"", b""])
    mocker.patch.object(as_conf, "parse_githooks")

    git_path = as_conf.get_project_dir()

    mocker.patch("autosubmit.git.autosubmit_git.os.path.exists",
                 side_effect=lambda path: path == str(Path(git_path) / ".githooks"))
    mocker.patch("autosubmit.git.autosubmit_git.os.walk", return_value=walk_result)

    chmod = mocker.patch("autosubmit.git.autosubmit_git.os.chmod")

    clone_repository(as_conf, False)

    if walk_result[0][1] or walk_result[0][2]:
        assert chmod.called
    else:
        chmod.assert_not_called()

    assert any("core.hooksPath" in str(call) for call in platform.method_calls)


def test_clone_repository_remote_clone_root_with_trailing_slash(autosubmit_config, mocker) -> None:
    """Verify remote clone roots ending with slash are handled correctly."""
    as_conf = autosubmit_config(_EXPID, experiment_data={
        "GIT": {
            "PROJECT_ORIGIN": "https://github.com/user/repo.git",
            "PROJECT_BRANCH": "main",
            "PROJECT_COMMIT": "",
            "REMOTE_CLONE_ROOT": "workflow/",
        },
        "PROJECT": {
            "PROJECT_TYPE": "GIT",
            "PROJECT_DESTINATION": "git_project",
        },
    })

    platform = mocker.Mock()

    submitter_cls = mocker.patch("autosubmit.git.autosubmit_git.ParamikoSubmitter")
    submitter_cls.return_value.platforms = {
        as_conf.get_platform(): platform,
    }

    clone_repository(as_conf, False)

    assert any(
        "workflow/a000/proj" in str(call)
        for call in platform.method_calls
    )


def test_clone_repository_clone_failure_restores_backup(autosubmit_config, mocker) -> None:
    """Verify failed clones restore the backup project."""
    as_conf = autosubmit_config(
        _EXPID,
        experiment_data={
            "GIT": {
                "PROJECT_ORIGIN": "https://github.com/user/repo.git",
                "PROJECT_BRANCH": "main",
                "PROJECT_COMMIT": "",
            },
            "PROJECT": {
                "PROJECT_TYPE": "GIT",
                "PROJECT_DESTINATION": "git_project",
            },
        },
    )

    mocker.patch("autosubmit.git.autosubmit_git.time", return_value=123)

    project_path = Path(BasicConfig.LOCAL_ROOT_DIR) / _EXPID / BasicConfig.LOCAL_PROJ_DIR
    backup_path = Path(BasicConfig.LOCAL_ROOT_DIR) / _EXPID / "proj_123"

    # noinspection PyUnusedLocal
    def check_output(command, shell):
        if command == "git --version":
            return b"git version 2.39.0\n"
        raise CalledProcessError(1, command)

    mocker.patch("autosubmit.git.autosubmit_git.subprocess.check_output", side_effect=check_output)

    mocker.patch(
        "autosubmit.git.autosubmit_git.os.path.exists",
        side_effect=lambda path: path == str(backup_path),
    )

    rmtree = mocker.patch("autosubmit.git.autosubmit_git.shutil.rmtree")
    move = mocker.patch("autosubmit.git.autosubmit_git.shutil.move")

    with pytest.raises(AutosubmitCritical):
        clone_repository(as_conf, False)

    rmtree.assert_called_once_with(str(project_path))
    move.assert_called_once_with(str(backup_path), str(project_path))


def test_clone_repository_clone_failure_without_backup(autosubmit_config, mocker) -> None:
    """Verify failed clones do not restore when no backup exists."""
    as_conf = autosubmit_config(_EXPID, experiment_data={
        "GIT": {
            "PROJECT_ORIGIN": "https://github.com/user/repo.git",
            "PROJECT_BRANCH": "main",
            "PROJECT_COMMIT": "",
        },
        "PROJECT": {
            "PROJECT_TYPE": "GIT",
            "PROJECT_DESTINATION": "git_project",
        },
    })

    mocker.patch(
        "autosubmit.git.autosubmit_git.subprocess.check_output",
        side_effect=CalledProcessError(1, "git clone"),
    )

    move = mocker.patch("autosubmit.git.autosubmit_git.shutil.move")

    mocker.patch("autosubmit.git.autosubmit_git.os.path.exists", return_value=False)

    with pytest.raises(AutosubmitCritical):
        clone_repository(as_conf, False)

    move.assert_not_called()


def test_clone_repository_submodule_failure_returns_false(autosubmit_config, mocker) -> None:
    """Verify submodule failures return False."""
    as_conf = autosubmit_config(
        _EXPID,
        experiment_data={
            "GIT": {
                "PROJECT_ORIGIN": "https://github.com/user/repo.git",
                "PROJECT_BRANCH": "main",
                "PROJECT_COMMIT": "",
                "PROJECT_SUBMODULES": "",
            },
            "PROJECT": {
                "PROJECT_TYPE": "GIT",
                "PROJECT_DESTINATION": "git_project",
            },
        },
    )

    # noinspection PyUnusedLocal
    def check_output(command, shell):
        if command == "git --version":
            return b"git version 2.39.0\n"
        if "git clone" in command:
            return b""
        raise CalledProcessError(1, command)

    mocker.patch(
        "autosubmit.git.autosubmit_git.subprocess.check_output",
        side_effect=check_output,
    )

    assert clone_repository(as_conf, False) is False


def test_clone_repository_removes_backup_after_success(autosubmit_config, mocker) -> None:
    """Verify successful clones remove old backups."""
    as_conf = autosubmit_config(_EXPID, experiment_data={
        "GIT": {
            "PROJECT_ORIGIN": "https://github.com/user/repo.git",
            "PROJECT_BRANCH": "main",
            "PROJECT_COMMIT": "",
            "REMOTE_CLONE_ROOT": "workflow",
        },
        "PROJECT": {
            "PROJECT_TYPE": "GIT",
            "PROJECT_DESTINATION": "git_project",
        },
    })

    platform = mocker.Mock()

    submitter_cls = mocker.patch("autosubmit.git.autosubmit_git.ParamikoSubmitter")
    submitter_cls.return_value.platforms = {
        as_conf.get_platform(): platform,
    }

    def fake_exists(path):
        return "proj_" in str(path)

    mocker.patch(
        "autosubmit.git.autosubmit_git.os.path.exists",
        side_effect=fake_exists,
    )

    rmtree = mocker.patch("autosubmit.git.autosubmit_git.shutil.rmtree")

    clone_repository(as_conf, False)

    rmtree.assert_called_once()


def test_clone_repository_submodules_empty_list_with_depth(autosubmit_config, mocker) -> None:
    """Verify empty submodules use depth when configured."""
    as_conf = autosubmit_config(
        _EXPID,
        experiment_data={
            "GIT": {
                "PROJECT_ORIGIN": "https://github.com/user/repo.git",
                "PROJECT_BRANCH": "main",
                "PROJECT_COMMIT": "",
                "REMOTE_CLONE_ROOT": "workflow",
            },
            "PROJECT": {
                "PROJECT_TYPE": "GIT",
                "PROJECT_DESTINATION": "git_project",
            },
        },
    )

    as_conf.get_submodules_list = lambda: []
    as_conf.get_project_submodules_depth = lambda: [3]

    platform = mocker.Mock()
    submitter_cls = mocker.patch("autosubmit.git.autosubmit_git.ParamikoSubmitter")
    submitter_cls.return_value.platforms = {
        as_conf.get_platform(): platform,
    }

    clone_repository(as_conf, False)

    assert any(
        "--depth 3" in str(call)
        for call in platform.method_calls
    )


def test_clone_repository_submodules_empty_list_recursive(autosubmit_config, mocker) -> None:
    """Verify empty submodules use recursive update without depth."""
    as_conf = autosubmit_config(
        _EXPID,
        experiment_data={
            "GIT": {
                "PROJECT_ORIGIN": "https://github.com/user/repo.git",
                "PROJECT_BRANCH": "main",
                "PROJECT_COMMIT": "",
                "REMOTE_CLONE_ROOT": "workflow",
            },
            "PROJECT": {
                "PROJECT_TYPE": "GIT",
                "PROJECT_DESTINATION": "git_project",
            },
        },
    )

    as_conf.get_submodules_list = lambda: []
    as_conf.get_project_submodules_depth = lambda: []

    platform = mocker.Mock()
    submitter_cls = mocker.patch("autosubmit.git.autosubmit_git.ParamikoSubmitter")
    submitter_cls.return_value.platforms = {
        as_conf.get_platform(): platform,
    }

    clone_repository(as_conf, False)

    assert any(
        "git submodule update --init --recursive" in str(call)
        for call in platform.method_calls
    )


def test_clone_repository_submodule_list_with_depth(autosubmit_config, mocker) -> None:
    """Verify listed submodules use individual depth updates."""
    as_conf = autosubmit_config(
        _EXPID,
        experiment_data={
            "GIT": {
                "PROJECT_ORIGIN": "https://github.com/user/repo.git",
                "PROJECT_BRANCH": "main",
                "PROJECT_COMMIT": "",
                "REMOTE_CLONE_ROOT": "workflow",
            },
            "PROJECT": {
                "PROJECT_TYPE": "GIT",
                "PROJECT_DESTINATION": "git_project",
            },
        },
    )

    as_conf.get_submodules_list = lambda: ["submodule_a"]
    as_conf.get_project_submodules_depth = lambda: [2]

    platform = mocker.Mock()
    submitter_cls = mocker.patch("autosubmit.git.autosubmit_git.ParamikoSubmitter")
    submitter_cls.return_value.platforms = {
        as_conf.get_platform(): platform,
    }

    clone_repository(as_conf, False)

    assert any(
        "--depth 2 submodule_a" in str(call)
        for call in platform.method_calls
    )


def test_clone_repository_runs_githook_command(autosubmit_config, mocker) -> None:
    """Verify githook command is executed when githooks are configured."""
    as_conf = autosubmit_config(
        _EXPID,
        experiment_data={
            "GIT": {
                "PROJECT_ORIGIN": "https://github.com/user/repo.git",
                "PROJECT_BRANCH": "main",
                "PROJECT_COMMIT": "",
                "REMOTE_CLONE_ROOT": "",
            },
            "PROJECT": {
                "PROJECT_TYPE": "GIT",
                "PROJECT_DESTINATION": "git_project",
            },
        },
    )

    platform = mocker.Mock()

    submitter_cls = mocker.patch("autosubmit.git.autosubmit_git.ParamikoSubmitter")
    submitter_cls.return_value.platforms = {
        as_conf.get_platform(): platform,
    }

    mocker.patch(
        "autosubmit.git.autosubmit_git.os.path.exists",
        side_effect=lambda path: ".githooks" in str(path),
    )
    mocker.patch("autosubmit.git.autosubmit_git.os.walk", return_value=[])
    mocker.patch.object(as_conf, "parse_githooks")

    check_output = mocker.patch(
        "autosubmit.git.autosubmit_git.subprocess.check_output",
        side_effect=[
            b"git version 2.39.0\n",
            b"",
            b"",
        ],
    )

    clone_repository(as_conf, False)

    assert any(
        "git config core.hooksPath" in str(call)
        for call in check_output.call_args_list
    )


def test_clone_repository_submodules_use_max_depth_when_depth_list_exhausted(autosubmit_config, mocker) -> None:
    """Verify submodules use max_depth when no individual depth is available."""
    as_conf = autosubmit_config(
        _EXPID,
        experiment_data={
            "GIT": {
                "PROJECT_ORIGIN": "https://github.com/user/repo.git",
                "PROJECT_BRANCH": "main",
                "PROJECT_COMMIT": "",
            },
            "PROJECT": {
                "PROJECT_TYPE": "GIT",
                "PROJECT_DESTINATION": "git_project",
            },
        },
    )

    platform = mocker.Mock()

    submitter_cls = mocker.patch("autosubmit.git.autosubmit_git.ParamikoSubmitter")
    submitter_cls.return_value.platforms = {
        as_conf.get_platform(): platform,
    }

    as_conf.get_submodules_list = lambda: ["submodule_a", "submodule_b"]
    as_conf.get_project_submodules_depth = lambda: [1]

    check_output = mocker.patch(
        "autosubmit.git.autosubmit_git.subprocess.check_output",
        side_effect=[
            b"git version 2.39.0\n",
            b"",
            b"",
        ],
    )

    clone_repository(as_conf, False)

    assert any(
        "git submodule update --init --depth 1 submodule_b" in str(call)
        for call in check_output.call_args_list
    )


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
    assert is_git_repo(git_repo) == expected


def test_check_unpushed_changes_clean_repository(autosubmit_config, mocker, tmp_path: Path) -> None:
    """Verify that a clean operational Git repository is accepted."""
    as_conf = autosubmit_config(_EXPID, experiment_data={})

    mocker.patch(
        "autosubmit.git.autosubmit_git.subprocess.check_output",
        side_effect=[
            "\n",  # git status --porcelain
            "",    # git log --branches --not --remotes
        ],
    )

    check_unpushed_changes(as_conf.expid, as_conf)


def test_check_unpushed_changes_detects_uncommitted_code(autosubmit_config, mocker, tmp_path: Path) -> None:
    """Verify that uncommitted changes raise an exception."""
    as_conf = autosubmit_config("o000", experiment_data={
        "PROJECT": {
            "PROJECT_TYPE": "GIT"
        }
    })

    as_conf.get_project_dir = lambda: str(tmp_path)

    mocker.patch("autosubmit.git.autosubmit_git.subprocess.check_output", return_value=" M file.py\n")

    with pytest.raises(AutosubmitCritical):
        check_unpushed_changes(as_conf.expid, as_conf)


def test_check_unpushed_changes_detects_unpushed_commits(autosubmit_config, mocker, tmp_path: Path) -> None:
    """Verify that unpushed commits raise an exception."""
    as_conf = autosubmit_config(
        "o000", experiment_data={"PROJECT": {"PROJECT_TYPE": "GIT"}}
    )

    as_conf.get_project_dir = lambda: str(tmp_path)

    mocker.patch(
        "autosubmit.git.autosubmit_git.subprocess.check_output",
        side_effect=[
            "",
            "abc123 Commit message\n",
        ],
    )

    with pytest.raises(AutosubmitCritical):
        check_unpushed_changes(as_conf.expid, as_conf)


def test_check_unpushed_changes_non_operational_experiment(autosubmit_config, mocker, tmp_path: Path) -> None:
    """Verify that non-operational experiments skip Git checks."""
    as_conf = autosubmit_config(_EXPID, experiment_data={})
    as_conf.get_project_dir = lambda: str(tmp_path)

    check_output = mocker.patch("autosubmit.git.autosubmit_git.subprocess.check_output")

    check_unpushed_changes(_EXPID, as_conf)

    check_output.assert_not_called()


def test_check_unpushed_changes_blank_status_line(autosubmit_config, mocker, tmp_path: Path) -> None:
    """Blank status lines should be ignored."""
    as_conf = autosubmit_config(
        "o000",
        experiment_data={"PROJECT": {"PROJECT_TYPE": "GIT"}},
    )

    as_conf.get_project_dir = lambda: str(tmp_path)

    mocker.patch(
        "autosubmit.git.autosubmit_git.subprocess.check_output",
        side_effect=[
            "\n",      # produces one blank line
            "",
        ],
    )

    check_unpushed_changes(as_conf.expid, as_conf)


def test_check_unpushed_changes_blank_unpushed_line(autosubmit_config, mocker, tmp_path: Path) -> None:
    """Blank git log output should not be considered unpushed commits."""
    as_conf = autosubmit_config(
        "o000",
        experiment_data={"PROJECT": {"PROJECT_TYPE": "GIT"}},
    )

    as_conf.get_project_dir = lambda: str(tmp_path)

    mocker.patch(
        "autosubmit.git.autosubmit_git.subprocess.check_output",
        side_effect=[
            "",
            "\n",
        ],
    )

    check_unpushed_changes(as_conf.expid, as_conf)
