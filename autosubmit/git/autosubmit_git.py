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

import locale
import os
import re
import shutil
import subprocess
from pathlib import Path
from shutil import rmtree
from time import time
from typing import List, Optional, Union

from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmitconfigparser.config.configcommon import AutosubmitConfig

from log.log import Log, AutosubmitCritical

Log.get_logger("Autosubmit")

_GIT_URL_PATTERN = re.compile(
    r'''^(
    (https?://[a-zA-Z0-9.-]+/[a-zA-Z0-9.-]+/[a-zA-Z0-9.-]+\.git) |  # e.g. https://github.com/user/repo
    (\w+@[a-zA-Z0-9.-]+:[a-zA-Z0-9./-]+\.git) |                     # e.g. git@github.com:user/repo.git
    (file://.+$)                                                    # e.g. file:///path/to/repo.git/
    )''', re.VERBOSE)
"""Regular expression to match Git URL."""

_GIT_UNCOMMITTED_CMD = ('git', 'status', '--porcelain')
"""Command to check if there are changes not committed go local Git repository."""

_GIT_UNPUSHED_CMD = ('git', 'log', '--branches', '--not', '--remotes')
"""Command to check if there are changes not pushed to Git remotes."""


def _get_uncommitted_code(git_repo: Path) -> Optional[str]:
    """Return any uncommitted changes in the given Git repository or submodules.

    If there are no uncommitted changes, return None.

    :param git_repo: Path to the Git repository.
    :return: A string containing the list of code not committed (directly git command output).
    Returns``None`` if there are no uncommitted changes.
    """
    encoding = locale.getlocale()[1]

    git_output = subprocess.check_output(_GIT_UNCOMMITTED_CMD, cwd=git_repo, encoding=encoding)
    for line in git_output.splitlines():
        tokens = line.strip().split(' ')
        if tokens[0] != '':
            return git_output

    return None


def _get_code_not_pushed(git_repo: Path) -> Optional[str]:
    """Return any code not pushed to remotes in the given Git repository or submodules.

    If there are no code not pushed changes, return None.

    :param git_repo: Path to the Git repository.
    :return: A string containing the list of code not pushed (directly git command output).
    Returns``None`` if there are no pushed not changes.
    """
    encoding = locale.getlocale()[1]

    git_output = subprocess.check_output(_GIT_UNPUSHED_CMD, cwd=git_repo, encoding=encoding)
    for line in git_output.splitlines():
        if line.strip():
            return git_output

    return None


def check_unpushed_changes(expid: str, as_conf: AutosubmitConfig) -> None:
    """Check if the Git repository is dirty for an operational experiment.

    Raises an AutosubmitCritical error if the experiment is operational,
    the platform is Git, and there are unpushed changes in the local Git
    repository, or in any of its Git submodules.

    :param expid: The experiment ID.
    :param as_conf: Autosubmit configuration object.
    """
    project_type = as_conf.get_project_type()
    if expid[0] == 'o' and project_type == 'git':
        proj_dir = Path(as_conf.get_project_dir())

        if uncommitted_code := _get_uncommitted_code(proj_dir):
            message = ("You must commit and push your code to the remote Git repository in an "
                       f"operational experiment before running it.\n\n{uncommitted_code}")
            raise AutosubmitCritical(message, 7075)

        if not_pushed := _get_code_not_pushed(proj_dir):
            message = ("You must push your code to the remote Git repository in an "
                       f"operational experiment before running it.\n\n{not_pushed}")
            raise AutosubmitCritical(message, 7075)


def clean_git(as_conf: AutosubmitConfig) -> bool:
    """Clean the cloned Git repository inside the project directory of the experiment.

    Skipped if the project directory location is not a valid directory.

    Skipped if the project directory is not a valid Git repository.

    Skipped if there are changes in the Git repository that were not committed or
    not pushed.

    :param as_conf: experiment configuration
    :return: ``True`` if the Git project directory was successfully deleted, ``False`` otherwise.
    """
    dirname_path = Path(as_conf.get_project_dir())
    Log.debug("Checking git directory status...")

    if not dirname_path.is_dir():
        Log.debug("Not a directory... SKIPPING!")
        return False

    if not Path(dirname_path, '.git').is_dir():
        Log.debug("Not a git repository... SKIPPING!")
        return False

    if _get_uncommitted_code(dirname_path):
        Log.info("Changes not committed detected... SKIPPING!")
        raise AutosubmitCritical("Commit needed!", 7013)

    if _get_code_not_pushed(dirname_path):
        Log.info("Changes not pushed detected... SKIPPING!")
        raise AutosubmitCritical("Synchronization needed!", 7064)

    if not as_conf.set_git_project_commit(as_conf):
        Log.info("Failed to set Git project commit... SKIPPING!")
        return False

    proj_dir = Path(BasicConfig.LOCAL_ROOT_DIR, as_conf.expid, BasicConfig.LOCAL_PROJ_DIR)
    Log.debug(f"Removing project directory {str(proj_dir)}")
    rmtree(proj_dir)

    return True


class AutosubmitGit:
    """Class to handle experiment git repository."""

    @staticmethod
    def clone_repository(as_conf, force, hpcarch):
        """
        Clones a specified git repository on the project folder

        :param as_conf: experiment configuration
        :type as_conf: autosubmit.config.AutosubmitConfig
        :param force: if True, it will overwrite any existing clone
        :type force: bool
        :param hpcarch: current main platform
        :type force: bool
        :return: True if clone was successful, False otherwise
        """
        submodule_failure = False

        if not as_conf.is_valid_git_repository():
            raise AutosubmitCritical(
                "Incorrect git Configuration, check origin,commit and branch settings of expdef file", 7064)
        git_project_origin = as_conf.get_git_project_origin()
        git_project_branch = as_conf.get_git_project_branch()
        git_remote_project_path = as_conf.get_git_remote_project_root()

        git_project_commit = as_conf.get_git_project_commit()
        git_project_submodules: Union[False, List[str]] = as_conf.get_submodules_list()
        git_project_submodules_depth = as_conf.get_project_submodules_depth()
        max_depth = -1
        if len(git_project_submodules_depth) > 0:
            max_depth = max(git_project_submodules_depth)
        if as_conf.get_fetch_single_branch() != "true":
            git_single_branch = False
        else:
            git_single_branch = True
        project_destination = as_conf.get_project_destination()
        project_path = os.path.join(
            BasicConfig.LOCAL_ROOT_DIR, as_conf.expid, BasicConfig.LOCAL_PROJ_DIR)
        project_backup_path = os.path.join(
            BasicConfig.LOCAL_ROOT_DIR, as_conf.expid, 'proj_{0}'.format(int(time())))
        git_path = as_conf.get_project_dir()

        # Making proj backup
        if force:
            if os.path.exists(project_path):
                Log.info("Making a backup of your current proj folder at {0}".format(
                    project_backup_path))
                shutil.move(project_path, project_backup_path)
            # shutil.make_archive(project_backup_path, 'zip', project_path)
            # project_backup_path = project_backup_path + ".zip"

        if os.path.exists(os.path.join(project_path, project_destination)):
            Log.info("Using project folder: {0}", project_path)
            # print("Force {0}".format(force))
            if not force:
                Log.debug("The project folder exists. SKIPPING...")
                return True
            else:
                shutil.rmtree(project_path)
        if not os.path.exists(project_path):
            os.mkdir(project_path)
            Log.debug("The project folder {0} has been created.", project_path)
        command_0 = ""
        command_githook = ""
        command_1 = ""

        if git_remote_project_path != '':
            if git_remote_project_path[-1] == '/':
                git_remote_path = os.path.join(
                    git_remote_project_path[:-1], as_conf.expid, BasicConfig.LOCAL_PROJ_DIR)
            else:
                git_remote_path = os.path.join(
                    git_remote_project_path, as_conf.expid, BasicConfig.LOCAL_PROJ_DIR)
            project_path = git_remote_path

        Log.info("Cloning {0} into {1}", git_project_branch + " " + git_project_origin, project_path)
        if not git_single_branch:
            command_0 += " git clone -b {0} {1} {2};".format(git_project_branch, git_project_origin,
                                                             project_destination)
        else:
            command_0 += " git clone --single-branch -b {0} {1} {2};".format(git_project_branch,
                                                                             git_project_origin,
                                                                             project_destination)
        try:
            # command 0
            Log.debug('Clone command: {0}', command_0)
            try:
                git_version = subprocess.check_output("git --version", shell=True)
                git_version = git_version.decode(locale.getlocale()[1]).split(" ")[-1].strip("\n")

                version_int = ""
                for number in git_version.split("."):
                    version_int += number
                git_version = int(version_int)
            except Exception:
                git_version = 2251
            if git_remote_project_path == '':
                command_0 = "cd {0} ; {1}".format(project_path, command_0)
                subprocess.check_output(command_0, shell=True)
            else:
                command_0 = "cd {0} ; {1}".format(project_path, command_0)
                hpcarch.send_command(command_0)
            # command 1

            if os.path.exists(os.path.join(git_path, ".githooks")) and git_version > 2136:
                for root_dir, dirs, files in os.walk(os.path.join(git_path, ".githooks")):
                    for f_dir in dirs:
                        os.chmod(os.path.join(root_dir, f_dir), 0o750)
                    for f_file in files:
                        os.chmod(os.path.join(root_dir, f_file), 0o750)
                command_githook += " git config core.hooksPath ./.githooks ; ".format(
                    git_path)
            if git_project_commit:
                command_1 += "git checkout {0}; ".format(git_project_commit)
            else:
                command_1 += "git checkout; "

            if git_project_submodules is not False:
                if len(git_project_submodules) == 0:
                    if max_depth > 0:
                        Log.info("Depth is incompatible with --recursive, ignoring recursive option")
                        command_1 += " git submodule update --init --depth {0}; ".format(max_depth)
                    else:
                        command_1 += " git submodule update --init --recursive; "
                else:
                    command_1 += " git submodule init; ".format(project_destination)
                    index_submodule = 0
                    for submodule in git_project_submodules:
                        if max_depth > 0:
                            Log.info("Depth is incompatible with --recursive, ignoring recursive option")
                            if index_submodule < len(git_project_submodules_depth):
                                command_1 += " git submodule update --init --depth {0} {1}; ".format(
                                    git_project_submodules_depth[index_submodule], submodule)
                            else:
                                command_1 += " git submodule update --init --depth {0} {1}; ".format(
                                    max_depth, submodule)
                        else:
                            command_1 += " git submodule update --init --recursive {0}; ".format(submodule)
                        index_submodule += 1
            if git_remote_project_path == '':
                try:
                    if len(command_githook) > 0:
                        command_githook = "cd {0} ; {1}".format(git_path, command_githook)
                        as_conf.parse_githooks()
                        subprocess.check_output(command_githook, shell=True)
                    command_1 = "cd {0}; {1} ".format(git_path, command_1)
                    Log.debug(f'Githook + Checkout and Submodules: {command_githook} {command_1}')
                    subprocess.check_output(command_1, shell=True)
                except BaseException as e:
                    submodule_failure = True
                    Log.printlog("Trace: {0}".format(str(e)), 6014)
                    Log.printlog(
                        "Submodule has a wrong configuration.\n{0}".format(command_1), 6014)
            else:
                if len(command_githook) > 0:
                    command_githook = "cd {0} ; {1}".format(project_path, command_githook)
                    as_conf.parse_githooks()
                    hpcarch.send_command(command_githook)
                command_1 = "cd {0}; {1} ".format(project_path, command_1)
                hpcarch.send_command(command_1)
        except subprocess.CalledProcessError:
            shutil.rmtree(project_path)
            if os.path.exists(project_backup_path):
                Log.info("Restoring proj folder...")
                shutil.move(project_backup_path, project_path)
            raise AutosubmitCritical(
                f'Can not clone {git_project_branch + " " + git_project_origin} into {project_path}', 7065)
        if submodule_failure:
            Log.info("Some Submodule failures have been detected. Backup {0} will not be removed.".format(
                project_backup_path))
            return False

        if os.path.exists(project_backup_path):
            Log.info("Removing backup...")
            shutil.rmtree(project_backup_path)

        return True

    @staticmethod
    def is_git_repo(git_repo: str) -> bool:
        git_repo = git_repo.lower().strip()

        return _GIT_URL_PATTERN.match(git_repo) is not None
