#!/usr/bin/env python3

# Copyright 2015-2020 Earth Sciences Department, BSC-CNS

# This file is part of Autosubmit.

# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

from os import path

# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.
import locale
import os
import shutil
import subprocess
from shutil import rmtree
from time import time
from typing import List, Union

# from autosubmit import Autosubmit
from autosubmitconfigparser.config.basicconfig import BasicConfig
from log.log import Log, AutosubmitCritical

Log.get_logger("Autosubmit")


class AutosubmitGit:
    """
    Class to handle experiment git repository

    :param expid: experiment identifier
    :type expid: str
    """

    def __init__(self, expid):
        self._expid = expid

    @staticmethod
    def clean_git(as_conf):
        """
        Function to clean space on BasicConfig.LOCAL_ROOT_DIR/git directory.

        :param as_conf: experiment configuration
        :type as_conf: autosubmitconfigparser.config.AutosubmitConfig
        """
        proj_dir = os.path.join(
            BasicConfig.LOCAL_ROOT_DIR, as_conf.expid, BasicConfig.LOCAL_PROJ_DIR)
        dirname_path = as_conf.get_project_dir()
        Log.debug("Checking git directory status...")
        if path.isdir(dirname_path):
            if path.isdir(os.path.join(dirname_path, '.git')):
                try:
                    output = subprocess.check_output("cd {0}; git diff-index HEAD --".format(dirname_path),
                                                     shell=True)
                except subprocess.CalledProcessError as e:
                    raise AutosubmitCritical(
                        "Failed to retrieve git info ...", 7064, str(e))
                if output:
                    Log.info("Changes not committed detected... SKIPPING!")
                    raise AutosubmitCritical("Commit needed!", 7013)
                else:
                    output = subprocess.check_output("cd {0}; git log --branches --not --remotes".format(dirname_path),
                                                     shell=True)
                    if output:
                        Log.info("Changes not pushed detected... SKIPPING!")
                        raise AutosubmitCritical(
                            "Synchronization needed!", 7064)
                    else:
                        if not as_conf.set_git_project_commit(as_conf):
                            return False
                        Log.debug("Removing directory")
                        rmtree(proj_dir)
            else:
                Log.debug("Not a git repository... SKIPPING!")
        else:
            Log.debug("Not a directory... SKIPPING!")
        return True

    @staticmethod
    def check_commit(as_conf):
        """
        Function to check uncommitted changes

        :param as_conf: experiment configuration
        :type as_conf: autosubmitconfigparser.config.AutosubmitConfig
        """
        dirname_path = as_conf.get_project_dir()
        if path.isdir(dirname_path):
            Log.debug("Checking git directory status...")
            if path.isdir(os.path.join(dirname_path, '.git')):
                try:
                    output = subprocess.check_output("cd {0}; git diff-index HEAD --".format(dirname_path),
                                                     shell=True)
                except subprocess.CalledProcessError:
                    Log.info("This is not a git experiment")
                    return True

                if output:
                    Log.printlog(
                        "There are local changes not committed to git", 3000)
                    return True
                else:
                    output = subprocess.check_output("cd {0}; git log --branches --not --remotes".format(dirname_path),
                                                     shell=True)
                    if output:
                        Log.printlog(
                            "There are local changes not pushed to git", 3000)
                        return True
                    else:
                        Log.info("Model Git repository is updated")
                        Log.result("Model Git repository is updated")

        return True

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
            raise AutosubmitCritical(f'Can not clone {git_project_branch+" "+git_project_origin} into {project_path}', 7065)
        if submodule_failure:
            Log.info("Some Submodule failures have been detected. Backup {0} will not be removed.".format(project_backup_path))
            return False

        if os.path.exists(project_backup_path):
            Log.info("Removing backup...")
            shutil.rmtree(project_backup_path)

        return True
