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

"""Helper functions for working with Git in integration tests."""

from os import system
from pathlib import Path
from subprocess import check_output
from tempfile import TemporaryDirectory

# TODO: Rename branch to main to match the other repositories; note that this
#       needs to wait until our laptops & CICD env are using Git 2.28+, in order
#       to use the --initial-branch=main (the -c init.defaultBranch option is
#       harmless right now, but useless as that's 2.28+ too, thus master for now).


def create_git_repository(path: Path, bare=False, branch='master') -> None:
    """Creates a Git repository.

    Creates the directory if it does not exist.

    :param path: Path to the Git repository.
    :param bare: Flag to make it a Git bare repository.
    :param branch: Branch to use.
    """
    if not path or path.is_file():
        raise ValueError(f'You must provide a valid path for your Git repository: {path}')

    if not path.exists():
        path.mkdir(parents=True)

    git_init_args = []
    if bare:
        git_init_args.append('--bare --shared=true')

    # Newer versions of Git have --initial-branch=???, but some of us at the BSC
    # still have older versions, so we will use a different approach, with
    # symbolic-ref.
    commands = [
        f'git -c init.defaultBranch=master init . {" ".join(git_init_args)}',
        'git symbolic-ref HEAD refs/heads/master'
    ]

    command = ';'.join(commands)
    check_output(command, cwd=str(path), shell=True)

    if bare:
        # NOTE: You cannot ``git clone file:///location/to/bare/repo -b master``, even
        #       if the repository exists and has the master branch configured. First,
        #       you need to push something so Git will create the refs/master for the
        #       branch. We do it here, since Autosubmit uses ``-b`` by default.
        with TemporaryDirectory() as td:
            clone_repo = Path(td, 'clone')
            git_clone_repository(f'file:///{str(path)}', clone_repo)

            with open(clone_repo / 'README.md', 'w') as f:
                f.write('This is a test repository of Autosubmit.')

            git_commit_all_in_dir(clone_repo, branch=branch, push=True)

    # git-http-backend (which comes with Git) may have issues with directory
    # permissions depending on the settings and file system permissions.
    # Thus, we chmod everything to 0x777 here.
    system(f'chmod -R 0777 {str(path)}')


def git_clone_repository(url: str, path: Path) -> None:
    """Clone a Git repository into the given path."""
    if not path or not path.is_absolute() or path.exists():
        raise ValueError(f'You must provide a valid, absolute, non-existent path for your Git repository: {path}')

    if not url:
        raise ValueError(f'You must provide a valid URL to be cloned: {url}')

    commands = [
        f'git clone {url} {str(path)}'
    ]

    command = ' '.join(commands)
    # Change cwd even if the path is absolute, just in case...
    check_output(command, cwd=str(path.parent), shell=True)


def git_commit_all_in_dir(path: Path, push=False, remote='origin', branch='master') -> None:
    """Adds all files in the given path, and creates a single commit for it."""
    if not path or not path.is_dir():
        raise ValueError(f'You must provide a valid path for your Git repository: {path}')

    if not list(path.iterdir()):
        raise ValueError(f'Tried to commit all files, but directory is empty: {path}')

    commands = [
        'git add .',
        'git commit -am "Initial commit"'
    ]

    if push:
        commands.append(f'git push {remote} {branch}')

    command = ';'.join(commands)
    check_output(command, cwd=str(path), shell=True)


def git_add_submodule(url: str, path: Path, name: str, push=False, remote='origin', branch='master') -> None:
    """Adds a submodule to a Git repository."""
    if not path or not path.is_dir():
        raise ValueError(f'You must provide a valid path for your Git repository: {path}')

    if not url:
        raise ValueError(f'You must provide a valid URL to be cloned: {url}')

    commands = [
        'git submodule init',
        f'git -c protocol.file.allow=always submodule add {url} {name}',
        'git add .',
        'git commit -am "Add submodule"'
    ]

    if push:
        commands.append(f'git push {remote} {branch}')

    command = ';'.join(commands)
    check_output(command, cwd=str(path), shell=True)
