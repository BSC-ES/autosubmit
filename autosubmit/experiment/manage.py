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

"""Code to manage Autosubmit experiments."""

import copy
import json
import os
import pwd
import signal
import subprocess
import tarfile
from contextlib import suppress
from datetime import datetime
from importlib.resources import files as read_files
from pathlib import Path
from re import sub
from shutil import copyfile, rmtree
from time import localtime
from typing import TYPE_CHECKING, Any

from portalocker import Lock
from ruamel.yaml import YAML

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.config.utils import copy_as_config
from autosubmit.config.yamlparser import YAMLParserFactory
from autosubmit.database import db_common
from autosubmit.database.db_common import (
    database_backup,
    get_experiment_description,
    get_experiment_expids,
    update_experiment_description_version,
)
from autosubmit.experiment.detail_updater import ExperimentDetails
from autosubmit.experiment.utils import (
    create_required_folders,
    get_experiment_owner,
    next_experiment_id,
    print_job_details,
)
from autosubmit.git.autosubmit_git import (
    clone_repository,
    is_git_repo,
)
from autosubmit.helpers.enums import ChunkUnit
from autosubmit.helpers.processes import process_id
from autosubmit.helpers.utils import user_yes_no_query
from autosubmit.helpers.version import get_version
from autosubmit.history.experiment_history import (
    ExperimentHistory,
)
from autosubmit.job.job_common import get_job_status
from autosubmit.job.job_grouping import JobGrouping
from autosubmit.job.job_list import JobList, load_job_list
from autosubmit.log.log import AutosubmitCritical, AutosubmitError, Log
from autosubmit.platforms.paramiko_submitter import ParamikoSubmitter
from autosubmit.scheduler import (
    generate_scripts_andor_wrappers,
)
from autosubmit.utils import (
    as_conf_default_values,
)

if TYPE_CHECKING:
    from collections.abc import Generator

    from rocrate.rocrate import ROCrate

__all__ = [
    "archive",
    "clean",
    "copy_code",
    "copy_experiment",
    "create",
    "delete_experiment",
    "describe",
    "expid_fn",
    "new_experiment",
    "provenance",
    "refresh",
    "report",
    "rocrate",
    "unarchive",
    "update_version",
]


def _generate_as_config(
    exp_id: str,
    dummy: bool = False,
    minimal_configuration: bool = False,
    local: bool = False,
    parameters: dict[str, dict | list | str] | None = None,
) -> None:
    """Retrieve the configuration from autosubmit.config package.

    :param exp_id: Experiment ID
    :param dummy: Whether the experiment is a dummy one or not.
    :param minimal_configuration: Whether the experiment is configured with minimal configuration or not.
    :param local: Whether the experiment project type is local or not.
    :param parameters: Optional list of parameters to be used when processing the configuration files.
    """

    def _add_comments_to_yaml(yaml_data, parameters, keys=None):
        """A recursive generator that visits every leaf node and yields the flatten parameter."""
        if keys is None:
            keys = []
        if isinstance(yaml_data, dict):
            for key, value in yaml_data.items():
                if isinstance(value, dict):
                    _add_comments_to_yaml(value, parameters, [*keys, key])
                else:
                    parameter_key = ".".join([*keys, key]).upper()
                    if parameter_key in parameters:
                        comment = parameters[parameter_key]
                        yaml_data.yaml_set_comment_before_after_key(
                            key, before=comment, indent=yaml_data.lc.col
                        )

    def _recurse_into_parameters(
        parameters: dict[str, dict | list | str], keys=None
    ) -> "Generator":
        """Recurse into the ``PARAMETERS`` dictionary, and emits a dictionary.

        The key in the dictionary is the flattened parameter key/ID, and the value
        is the parameter documentation.

        :param parameters: Global parameters dictionary.
        :param keys: For recursion, the accumulated keys.
        :return: A dictionary with the
        """
        if keys is None:
            keys = []
        if isinstance(parameters, dict):
            for key, value in parameters.items():
                if isinstance(value, dict):
                    yield from _recurse_into_parameters(value, [*keys, key])
                else:
                    key = key.upper()
                    # The parameters have some keys that contain ``${PARENT}.key`` as that is
                    # how they are displayed in the Sphinx docs. So we need to detect it and
                    # handle it. p.s. We also know the max-length of the parameters dict is 2!
                    # See the ``autosubmit.helpers.parameters`` module for more.
                    if not key.startswith(f"{keys[0]}."):
                        yield ".".join([*keys, key]).upper(), value
                    else:
                        yield key, value

    template_files = [
        file.name
        for file in (read_files("autosubmit.config") / "files").iterdir()
        if file.is_file()
    ]
    if parameters is None:
        parameters = {}
    parameter_comments: dict = dict(_recurse_into_parameters(parameters))

    for as_conf_file in template_files:
        origin = str(read_files("autosubmit.config") / f"files/{as_conf_file}")
        target = None
        if dummy:
            # Create a ``dummy.yml`` file.
            if as_conf_file.endswith("dummy.yml"):
                file_name = f"{as_conf_file.split('-')[0]}_{exp_id}.yml"
                target = Path(BasicConfig.LOCAL_ROOT_DIR, exp_id, "conf", file_name)
        elif minimal_configuration:
            # Create a ``minimal.yml`` file.
            #
            # Here we have two minimal configuration files that we can copy, the local or the git files.
            # The function knows whether it is a local through the ``local`` argument, and that defines
            # which files we will copy (``local-minimal.yml`` if ``local``, ``git-minimal.yml`` otherwise.)
            if (local and as_conf_file.endswith("local-minimal.yml")) or (
                not local and as_conf_file.endswith("git-minimal.yml")
            ):
                target = Path(BasicConfig.LOCAL_ROOT_DIR, exp_id, "conf/minimal.yml")
        elif not as_conf_file.endswith("dummy.yml") and not as_conf_file.endswith(
            "minimal.yml"
        ):
            # Create any other file that is not ``dummy.yml`` nor ``minimal.yml``.
            file_name = f"{Path(as_conf_file).stem}_{exp_id}.yml"
            target = Path(BasicConfig.LOCAL_ROOT_DIR, exp_id, "conf", file_name)

        # Here we annotate the copied configuration with comments from the Python source code.
        # This means the YAML configuration files contain the exact same comments from our
        # Python code, which is also displayed in our Sphinx documentation (be careful with what
        # you write!)
        #
        # The previous code was simply doing a shutil(origin, target). This does not modify
        # much that logic, except we add comments before writing the copy...
        if origin and target:
            with open(origin, "r") as input_data, open(str(target), "w+") as output:
                yaml = YAML(typ="rt")
                yaml_data = yaml.load(input_data)
                _add_comments_to_yaml(yaml_data, parameter_comments)
                yaml.dump(yaml_data, output)


def expid_fn(
    description,
    hpc="",
    copy_id="",
    dummy=False,
    minimal_configuration=False,
    git_repo="",
    git_branch="",
    git_as_conf="",
    operational=False,
    testcase=False,
    evaluation=False,
    use_local_minimal=False,
) -> str:
    """Create a new experiment.

    :param description: Description of the experiment.
    :param hpc: Name of the target platform where the experiment will run.
    :param copy_id: Identifier of an existing experiment to copy.
    :param dummy: If ``True``, create a dummy experiment.
    :param minimal_configuration: If ``True``, create the experiment using a minimal configuration.
    :param git_repo: URL of the Git repository to clone.
    :param git_branch: Git branch to clone.
    :param git_as_conf: Path to the Autosubmit configuration file in the Git repository.
    :param operational: If ``True``, create an operational experiment.
    :param testcase: If ``True``, create a test case experiment.
    :param evaluation: If ``True``, create an evaluation experiment.
    :param use_local_minimal: If ``True``, use the local minimal configuration instead of the Git one.
    :return: Identifier of the newly created experiment.
    """
    root_folder = Path(BasicConfig.LOCAL_ROOT_DIR)
    if not description:
        raise AutosubmitCritical("You must provide an experiment description.", 7011)
    if hpc is None and not minimal_configuration:
        raise AutosubmitCritical("You must provide an HPC (-H)", 7011)
    autosubmit_version = get_version()
    # Register the experiment in the database
    # Copy another experiment from the database
    if copy_id:
        copy_id_folder = root_folder / copy_id
        if not copy_id_folder.exists():
            raise AutosubmitCritical(f"Experiment does not exist: {copy_id}", 7011)
        if minimal_configuration:
            conf_dir = Path(copy_id_folder) / "conf"
            if (
                not Path(conf_dir / "minimal.yml").is_file()
                and not Path(conf_dir / "minimal.yaml").is_file()
            ):
                raise AutosubmitCritical(
                    "Cannot copy an experiment that does not have a minimal.yml file",
                    7011,
                )
        exp_id = copy_experiment(
            copy_id,
            description,
            autosubmit_version,
            testcase,
            operational,
            evaluation,
        )
    else:
        # Create a new experiment from scratch
        exp_id = new_experiment(
            description, autosubmit_version, testcase, operational, evaluation
        )

    if exp_id == "":
        raise AutosubmitCritical("Autosubmit failed to create an expid", 7011)

    # Create the experiment structure
    Log.info("Generating folder structure...")

    exp_folder = root_folder / Path(exp_id)
    try:
        Log.info(f"Experiment folder: {exp_folder}")
        create_required_folders(exp_id, exp_folder)
    except OSError as e:
        with suppress(Exception):
            delete_experiment(exp_id, True)
        raise AutosubmitCritical(
            f"Error creating the experiment structure: {str(e)}", 7011
        )
    # Create the experiment configuration
    Log.info("Generating configuration files...")
    try:
        if copy_id != "" and copy_id is not None:
            # Copy the configuration from selected experiment
            copy_as_config(exp_id, copy_id)
        else:
            # Create a new configuration
            _generate_as_config(exp_id, dummy, minimal_configuration, use_local_minimal)
    except Exception as e:
        with suppress(Exception):
            delete_experiment(exp_id, True)
        raise AutosubmitCritical(
            f"Error creating the experiment configuration: {str(e)}", 7011
        )
    # Change template values by default values specified from the commandline
    try:
        if use_local_minimal and is_git_repo(git_repo):
            git_repo = ""
            git_branch = ""
        as_conf_default_values(
            autosubmit_version,
            exp_id,
            hpc,
            minimal_configuration,
            git_repo,
            git_branch,
            git_as_conf,
        )
    except Exception as e:
        with suppress(Exception):
            delete_experiment(exp_id, True)
        raise AutosubmitCritical(f"Error setting the default values: {str(e)}", 7011)

    # Try to update the experiment details
    try:
        ExperimentDetails(exp_id).save_update_details()
    except Exception as e:
        Log.warning(
            f"Could not update experiment details for {exp_id}. Omitting this step."
        )
        Log.debug(f"Error calling save_update_details: {str(e)}")

    Log.result(f"Experiment {exp_id} created")
    return exp_id


def new_experiment(
    description, version, test=False, operational=False, evaluation=False
) -> str:
    """Stores a new experiment on the database and generates its identifier

    :param description: description of the experiment
    :param version: version of the experiment
    :param test: if True, the experiment is a test experiment
    :param operational: if True, the experiment is an operational experiment
    :param evaluation: if True, the experiment is an evaluation experiment
    :return: the experiment id for the new experiment
    """
    try:
        if test:
            last_exp_name = db_common.last_name_used(True)
        elif operational:
            last_exp_name = db_common.last_name_used(False, True)
        elif evaluation:
            last_exp_name = db_common.last_name_used(False, False, True)
        else:
            last_exp_name = db_common.last_name_used()
        if last_exp_name == "":
            return ""
        if last_exp_name == "empty":
            if test:
                # test identifier restricted also to 4 characters.
                new_name = "t000"
            elif operational:
                # operational identifier restricted also to 4 characters.
                new_name = "o000"
            elif evaluation:
                # evaluation identifier restricted also to 4 characters.
                new_name = "e000"
            else:
                new_name = "a000"
        else:
            new_name = last_exp_name
            if new_name == "":
                return ""
        while db_common.check_experiment_exists(new_name, False):
            new_name = next_experiment_id(new_name)
            if new_name == "":
                return ""
        if not db_common.save_experiment(new_name, description, version):
            return ""
        Log.info('The new experiment "{0}" has been registered.', new_name)
        return new_name
    except Exception as e:
        raise AutosubmitCritical(
            f"Error while generating a new experiment in the db: {e}", 7011
        ) from e


def delete_experiment(expids: str, force: bool) -> bool:
    """Delete an experiment from the database.

    Deletes the experiment's folder database entry and all the related metadata files.

    :param expids: List of experiment IDs to delete.
    :param force: Ask for confirmation if ``False``.
    :returns: ``True`` if successful, ``False`` otherwise.
    """
    # expid will come from argparse, which provides nix-style comma-separated values,
    # so here we parse the comma-separated values. ``.fromkeys`` keeps order and removes
    # duplicates.
    expid_list = expids.replace(",", " ").split(" ")
    expid_list = [expid.lower() for expid in filter(lambda x: x, expid_list)]

    failed: list[str] = []

    for expid in expid_list:
        try:
            _delete_experiment(expid, force)
        except Exception as e:
            Log.error(f"Failed to delete experiment {expid}: {str(e)}")
            failed.append(expid)

    if failed:
        Log.error(f"Deletion failed for experiments: {', '.join(failed)}")
        return False

    return True


def _delete_experiment(expid: str, force: bool) -> None:
    if process_id(expid) is not None:
        raise AutosubmitCritical(
            "Ensure no processes are running in the experiment directory", 7076
        )

    experiment_path = Path(f"{BasicConfig.LOCAL_ROOT_DIR}/{expid}")
    if not experiment_path.exists():
        raise AutosubmitCritical("Experiment does not exist", 7012)

    confirm_removal = force or user_yes_no_query(f"Do you want to delete {expid} ?")

    if not confirm_removal:
        Log.info(f"Experiment {expid} deletion cancelled by user")
        return

    Log.info(f"Deleting experiment {expid}")

    # Try to delete the experiment details
    try:
        ExperimentDetails(expid).delete_details()
    except Exception as e:
        Log.warning(f"Failed to delete DB details for experiment {expid}: {str(e)}")
        raise

    try:
        _delete_expid(expid, force)
        Log.info(f"Experiment {expid} has been deleted")
    except Exception as e:
        raise AutosubmitCritical(
            "Seems that something went wrong, please check the trace", 7012, str(e)
        )


def _delete_expid(expid_delete: str, force: bool = False) -> None:
    """Removes an experiment from the path and database.

    If the current user is eadmin and the -f flag has been sent, it deletes regardless of experiment owner.

    :param expid_delete: Identifier of the experiment to delete.
    :param force: If True, does not ask for confirmation.
    :returns: True if successfully deleted, False otherwise.
    :raises AutosubmitCritical: If the experiment does not exist or if there are insufficient permissions.
    """
    expid_delete = expid_delete.lower().strip()
    if not expid_delete:
        raise AutosubmitCritical(
            "Experiment identifier is required for deletion.", 7011
        )
    experiment_path = Path(f"{BasicConfig.LOCAL_ROOT_DIR}/{expid_delete}")
    structure_db_path = Path(
        f"{BasicConfig.STRUCTURES_DIR}/structure_{expid_delete}.db"
    )
    job_data_db_path = Path(f"{BasicConfig.JOBDATA_DIR}/job_data_{expid_delete}")
    experiment_path = experiment_path.resolve()
    structure_db_path = structure_db_path.resolve()
    job_data_db_path = job_data_db_path.resolve()
    if (
        Path(BasicConfig.LOCAL_ROOT_DIR) == experiment_path
        or Path(BasicConfig.STRUCTURES_DIR) == structure_db_path
        or Path(BasicConfig.JOBDATA_DIR) == job_data_db_path
    ):
        raise AutosubmitCritical(
            f"Invalid paths for experiment deletion: {expid_delete}. "
            "Paths must not be the root directories.",
            7011,
        )

    if not experiment_path.is_relative_to(BasicConfig.LOCAL_ROOT_DIR):
        raise AutosubmitCritical(
            f"Invalid paths for experiment deletion: {expid_delete}. "
            "Paths must be within the configured directories.",
            7011,
        )

    if not experiment_path.exists():
        Log.printlog("Experiment directory does not exist.", Log.WARNING)
        return

    # TODO: Sort out the eadmin user: https://github.com/BSC-ES/autosubmit/issues/944
    _, _, is_owner, is_eadmin = get_experiment_owner(expid_delete)
    if not is_owner and not (force and is_eadmin):
        if is_eadmin:
            raise AutosubmitCritical(
                f"Detected Eadmin user however, -f flag is not found. {expid_delete} cannot be deleted!",
                7012,
            )
        else:
            raise AutosubmitCritical(
                f"Current user is not the owner of the experiment. {expid_delete} cannot be deleted!",
                7012,
            )

    message_parts = [
        f"The {expid_delete} experiment was removed from the local disk and from the database.",
        "Note that this action does not delete any data written by the experiment.",
        "Complete list of files/directories deleted:",
        "",
    ]
    message_parts.extend(f"{path}" for path in experiment_path.rglob("*"))
    message_parts.append(f"{structure_db_path}")
    message_parts.append(f"{job_data_db_path}.db")
    message_parts.append(f"{job_data_db_path}.sql")
    message = "\n".join(message_parts)

    error_message = _perform_deletion(
        experiment_path, structure_db_path, job_data_db_path, expid_delete
    )

    if not error_message:
        Log.printlog(message, Log.RESULT)
    else:
        Log.printlog(error_message, Log.ERROR)
        raise AutosubmitError(
            "Some experiment files weren't correctly deleted\n"
            "Please if the trace shows DATABASE IS LOCKED, report it to git\n"
            "If there are I/O issues, wait until they're solved and then use this command again.\n",
            6004,
            error_message,
        )


def _perform_deletion(
    experiment_path: Path,
    structure_db_path: Path,
    job_data_db_path: Path,
    expid_delete: str,
) -> str:
    """Perform the deletion of an experiment.

    Deletion includes its directory, structure database, and job data database.

    :param experiment_path: Path to the experiment directory.
    :param structure_db_path: Path to the structure database file.
    :param job_data_db_path: Path to the job data database file.
    :param expid_delete: Identifier of the experiment to delete.
    :return: An error message if any errors occurred during deletion, otherwise an empty string.
    """
    error_message = []

    is_sqlite = BasicConfig.DATABASE_BACKEND == "sqlite"

    Log.info(f"Deleting experiment from {BasicConfig.DATABASE_BACKEND} database...")
    try:
        db_common.delete_experiment(expid_delete)
        Log.result(f"Experiment {expid_delete} deleted from database")
    except Exception as e:
        error_message.append(f"Cannot delete experiment entry: {e}")

    Log.info("Removing experiment directory...")
    try:
        rmtree(experiment_path)
        Log.result(f"Experiment directory {experiment_path} deleted from disk")
    except Exception as e:
        error_message.append(f"Cannot delete directory: {e}")

    if is_sqlite:
        Log.info("Removing structure db...")

        structure_db_path.unlink(missing_ok=True)
        Log.info(f"Experiment {expid_delete} structure db deleted")

        Log.info("Removing job_data db...")
        db_path = job_data_db_path.with_suffix(".db")
        sql_path = job_data_db_path.with_suffix(".sql")
        db_path.unlink(missing_ok=True)
        sql_path.unlink(missing_ok=True)
        Log.info(f"Experiment {expid_delete} job_data db deleted")

    return "\n".join(error_message)


def copy_experiment(
    experiment_id, description, version, test=False, operational=False, evaluation=False
) -> str:
    """Creates a new experiment by copying an existing experiment

    :param version: experiment's associated autosubmit version
    :param experiment_id: identifier of experiment to copy
    :param description: experiment's description
    :param test: specifies if it is a test experiment
    :param operational: specifies if it is an operational experiment
    :param evaluation: specifies if it is an evaluation experiment
    :return: new experiment identifier
    """
    try:
        if not db_common.check_experiment_exists(experiment_id):
            return ""
        return new_experiment(description, version, test, operational, evaluation)
    except Exception as e:
        raise AutosubmitCritical(
            f"Error while copying the experiment {experiment_id} "
            f"as a new experiment in the db: {e}",
            7011,
        ) from e


def clean(expid: str, project: bool, plot: bool, stats: bool) -> bool:
    """Clean the experiment directory to save storage space.

    :param expid: The experiment ID.
    :param project: Whether to clean the experiment project folder or not.
    :param plot: Whether to delete plot files (keeping two newest) or not.
    :param stats: Whether to delete statistics files (keeping two newest) or not.
    :return: ``True`` is the command ran successfully and ``False`` otherwise.
    :raises: AutosubmitCritical if anything goes wrong cleaning the experiment folders.
    """
    from autosubmit.config.configcommon import AutosubmitConfig
    from autosubmit.config.yamlparser import YAMLParserFactory
    from autosubmit.git.autosubmit_git import clean_git
    from autosubmit.monitor.monitor import clean_plot, clean_stats

    Log.info(f"Cleaning experiment {expid}...")

    try:
        if project:
            autosubmit_config = AutosubmitConfig(
                expid, BasicConfig, YAMLParserFactory()
            )
            autosubmit_config.check_conf_files(False)

            project_type = autosubmit_config.get_project_type()
            if project_type == "git":
                Log.info("Cleaning GIT directory...")
                if not clean_git(autosubmit_config):
                    # TODO: We need to seriously improve our exception handling and logging - #1199
                    Log.error("Error cleaning GIT directory")
                    return False
                Log.result("Git project cleaned!\n")
            else:
                Log.info("No project to clean...\n")
        if plot:
            Log.info("Cleaning plots...")
            clean_plot(expid)
            Log.result("Plots cleaned!\n")
        if stats:
            Log.info("Cleaning stats directory...")
            clean_stats(expid)
            Log.result("Stats cleaned!\n")
    except Exception as e:
        raise AutosubmitCritical(
            "Couldn't clean this experiment, check if you have the correct permissions",
            7012,
            str(e),
        )

    Log.result("Experiment cleaned successfully!")
    return True


def copy_code(
    as_conf: AutosubmitConfig, expid: str, project_type: str, force: bool
) -> bool:
    """Method to copy code from experiment repository to project directory.

    :param as_conf: experiment configuration class
    :param expid: experiment identifier
    :param project_type: project type (git, svn, local)
    :param force: if True, overwrites current data
    :return: True if successful, False if not
    """
    project_destination = as_conf.get_project_destination()
    if not project_destination and project_type.lower() != "none":
        raise AutosubmitCritical(
            "Autosubmit couldn't identify the project destination.", 7014
        )

    if project_type == "git":
        return clone_repository(as_conf, force)
    elif project_type == "svn":
        svn_project_url = as_conf.get_svn_project_url()
        svn_project_revision = as_conf.get_svn_project_revision()
        local_proj_dir = os.path.join(
            BasicConfig.LOCAL_ROOT_DIR, expid, BasicConfig.LOCAL_PROJ_DIR
        )
        if os.path.exists(local_proj_dir):
            Log.info(f"Using project folder: {local_proj_dir}")
            if not force:
                Log.debug("The project folder exists. SKIPPING...")
                return True
            else:
                rmtree(local_proj_dir, ignore_errors=True)
        try:
            os.mkdir(local_proj_dir)
        except Exception as e:
            raise AutosubmitCritical(
                f"Project path:{local_proj_dir} can't be created. Revise that the path"
                f" is the correct one.",
                7014,
                str(e),
            )

        Log.debug(f"The project folder {local_proj_dir} has been created.")
        Log.info(
            f"Checking out revision {svn_project_revision + ' ' + svn_project_url} into {local_proj_dir}"
        )
        try:
            cmd = (
                f"cd {local_proj_dir}; svn --force-interactive checkout -r {svn_project_revision} "
                f"{svn_project_url} {project_destination}"
            )
            svn_user = as_conf.experiment_data.get("CUSTOM_CONFIG", "").get("USER")
            if svn_user is not None:
                cmd += f" --username {svn_user}"
            svn_password = as_conf.experiment_data.get("CUSTOM_CONFIG", "").get(
                "PASSWORD"
            )
            if svn_password is not None:
                cmd += f" --password {svn_password}"
            output = subprocess.check_output(cmd, shell=True)
        except subprocess.CalledProcessError:
            try:
                rmtree(local_proj_dir, ignore_errors=True)
            except Exception:
                pass
            raise AutosubmitCritical(
                f"Can not check out revision {svn_project_revision} {svn_project_url} "
                f"into {local_proj_dir}",
                7062,
            )
        Log.debug(f"{output}")
        Log.debug(f"The project folder {local_proj_dir} has been created.")
        Log.info(
            f"Checking out revision {svn_project_revision + ' ' + svn_project_url} into {local_proj_dir}"
        )
        try:
            output = subprocess.check_output(
                "cd "
                + local_proj_dir
                + "; svn --force-interactive checkout -r "
                + svn_project_revision
                + " "
                + svn_project_url
                + " "
                + project_destination,
                shell=True,
            )
        except subprocess.CalledProcessError:
            try:
                rmtree(local_proj_dir, ignore_errors=True)
            except Exception:
                pass
            raise AutosubmitCritical(
                f"Can not check out revision {svn_project_revision} {svn_project_url} "
                f"into {local_proj_dir}",
                7062,
            )
        Log.debug(f"{output}")
    elif project_type == "local":
        local_project: Path = as_conf.get_local_project_path()
        if not local_project:
            raise AutosubmitCritical(
                "Empty project path! Please change this parameter to a valid one.", 7014
            )
        # check if local_project_path is a valid path
        local_project_path: Path = Path(local_project)
        if not local_project_path.is_dir():
            msg = f"Local project path is not a valid path and/or it does not exist: {str(local_project_path)}"
            raise AutosubmitCritical(msg, 7014)

        local_proj_dir_path: Path = Path(
            BasicConfig.LOCAL_ROOT_DIR, expid, BasicConfig.LOCAL_PROJ_DIR
        )
        project_destination = local_proj_dir_path / project_destination

        # TODO: Move to a new package/file once we simplify ``autosubmit.py``.
        def copy_contents(from_: Path, to: Path):
            try:
                # TODO: Do it in pure-python?
                Log.info(f"Copying {str(from_)} into {str(to)}")
                cmd_output = subprocess.check_output(
                    f"cp -R {str(from_)}/* {str(to)}/", shell=True
                )
                Log.debug(str(cmd_output))
            except subprocess.CalledProcessError:
                with suppress(Exception):
                    Log.debug(f"Deleting {str(to.parent)}")
                    rmtree(to.parent)
                raise AutosubmitCritical(
                    f"Cannot copy {str(from_)} into {str(to.parent)}. Exiting...", 7063
                )

        if not local_proj_dir_path.exists():
            Path(local_proj_dir_path).mkdir(parents=True)
            Path(project_destination).mkdir(parents=True)
            Log.debug(f"The project folder {local_proj_dir_path} has been created.")
            copy_contents(Path(local_project_path), project_destination)
        else:
            Log.info(f"Using project folder: {str(local_proj_dir_path)}")

            # We use ``rsync`` if the directory already exists, syncing existing files.
            # If the file does not exist, we create the directory and issue an ``cp``
            # command to copy the files. Otherwise, we inform the user of no action.
            if not project_destination.exists():
                Path(project_destination).mkdir(parents=True)
                copy_contents(Path(local_project_path), project_destination)
            elif force:
                try:
                    cmd = f"rsync -ach --info=progress2 {str(local_project_path)}/* {str(project_destination)}"
                    subprocess.call([cmd], shell=True)
                except (OSError, subprocess.CalledProcessError):
                    raise AutosubmitCritical(
                        f"Cannot rsync {str(local_project_path)} into "
                        f"{str(project_destination.parent)}. Exiting...",
                        7063,
                    )
            else:
                # Previously we did not inform users when nothing was copied.
                Log.info(
                    "Local project destination already exists, will not sync project files."
                )

    return True


def _signal_handler_create(signal_received, frame) -> None:
    """Used to handle KeyboardInterrupt signals while the create method is being executed

    :param signal_received: The signal received by the process.
    :param frame: Current program frame.
    """
    raise AutosubmitCritical(
        "Autosubmit has been closed in an unexpected way. Killed or control + c.", 7010
    )


def create(
    expid: str,
    noplot: bool,
    hide: bool,
    output="pdf",
    group_by: str | None = None,
    expand: list | None = [],
    expand_status: list = [],
    check_wrappers=False,
    detail=False,
    force=False,
) -> int:
    """Creates job list for given experiment. Configuration files must be valid before executing this process.

    :param detail: Show Job List view in terminal
    :param check_wrappers: Generate possible wrapper in the current workflow
    :param expand_status: Select the statuses to be expanded
    :param expand: Supply the list of dates/members/chunks to filter the list of jobs.
    :param group_by: Groups the jobs automatically by date, member, chunk or split
    :param expid: Experiment identifier
    :param noplot: if True, method omits final plotting of the jobs list. Only needed on large experiments when
        plotting time can be much larger than creation time.
    :return: True if successful, False if not
    :param hide: hides plot window
    :param hide: hides plot window
    :param output: plot's file format. It can be pdf, png, ps or svg
    :param force: Whether to force the creation of a new job object or not.
    """
    exp_path = os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid)
    tmp_path = os.path.join(exp_path, BasicConfig.LOCAL_TMP_DIR)
    with Lock(os.path.join(tmp_path, "autosubmit.lock"), timeout=1) as fh:
        try:
            Log.info(
                "Preparing .lock file to avoid multiple instances with same expid."
            )

            as_conf = AutosubmitConfig(expid, BasicConfig, YAMLParserFactory())
            # Get original configuration
            as_conf.reload(force_load=True, only_experiment_data=True)
            # Getting output type provided by the user in config, 'pdf' as default
            try:
                if not copy_code(as_conf, expid, as_conf.get_project_type(), False):
                    return False
            except AutosubmitCritical:
                raise
            except Exception as e:
                raise AutosubmitCritical(
                    "Error obtaining the project data, check the parameters related to PROJECT and GIT/SVN or LOCAL sections",
                    code=7014,
                    trace=str(e),
                )
            # Update configuration with the new config in the dist ( if any )
            as_conf.check_conf_files(running_time=False, force_load=True, no_log=False)
            if len(
                as_conf.experiment_data.get("JOBS", {})
            ) == 0 and "CUSTOM_CONFIG" in as_conf.experiment_data.get("DEFAULT", {}):
                raise AutosubmitCritical(
                    f"Job list is empty\nCheck if there are YML files in {as_conf.experiment_data.get('DEFAULT', '').get('CUSTOM_CONFIG', '')}",
                    code=7015,
                )
            output_type = as_conf.get_output_type()

            if not os.path.exists(os.path.join(exp_path, "db")):
                raise AutosubmitCritical(
                    f"The db folder doesn't exists. Make sure that the 'db'"
                    f" folder exists in the following path: {exp_path}",
                    code=6013,
                )
            if not os.path.exists(os.path.join(exp_path, "plot")):
                raise AutosubmitCritical(
                    f"The plot folder doesn't exists. Make sure that the 'plot'"
                    f" folder exists in the following path: {exp_path}",
                    code=6013,
                )

            # Load parameters
            Log.info("Loading parameters...")
            parameters = as_conf.load_parameters()

            date_list = as_conf.get_date_list()
            if len(date_list) != len(set(date_list)):
                raise AutosubmitCritical("There are repeated start dates!", 7014)
            num_chunks = as_conf.get_num_chunks()
            chunk_ini = as_conf.get_chunk_ini()
            member_list = as_conf.get_member_list()
            # print("Run only members {0}".format(run_only_members))
            if len(member_list) != len(set(member_list)):
                raise AutosubmitCritical("There are repeated member names!")
            rerun = as_conf.get_rerun()

            Log.info("\nCreating the jobs list...")
            job_list = JobList(expid, as_conf, YAMLParserFactory())
            date_format = ""
            if as_conf.get_chunk_size_unit() == ChunkUnit.HOUR:
                date_format = "H"
            for date in date_list:
                if date.hour > 1:
                    date_format = "H"
                if date.minute > 1:
                    date_format = "M"

            job_list.generate(
                as_conf,
                date_list,
                member_list,
                num_chunks,
                chunk_ini,
                parameters,
                date_format,
                as_conf.get_retrials(),
                as_conf.get_default_job_type(),
                force=force,
                full_load=True,
            )
            if str(rerun).lower() == "true":
                job_list.rerun(as_conf.get_rerun_jobs(), as_conf)
            else:
                job_list.remove_rerun_only_jobs()
            job_list.clear_generate()
            for job in job_list.get_job_list():
                if not job.is_wrapper:
                    job.wrapper_type = None
                    job.packed = False
            as_conf.save()

            groups_dict = {}
            # Setting up job historical database header. Must create a new run.
            # Historical Database: Setup new run
            try:
                exp_history = ExperimentHistory(expid)
                exp_history.initialize_database()

                # exp_history.create_new_experiment_run(as_conf.get_chunk_size_unit(), as_conf.get_chunk_size(), as_conf.get_full_config_as_json(), job_list.get_job_list())
                run_dc = exp_history.process_status_changes(
                    job_list.get_job_list(),
                    chunk_unit=as_conf.get_chunk_size_unit(),
                    chunk_size=as_conf.get_chunk_size(),
                    current_config=as_conf.get_full_config_as_json(),
                    create=True,
                )
                job_list.run_id = run_dc.run_id if run_dc else None
                database_backup(expid)
            except Exception:
                Log.warning(
                    "Historic database seems corrupted, AS will repair it and resume the run"
                )
                try:
                    # FIXME: https://github.com/BSC-ES/autosubmit/issues/3179
                    raise NotImplementedError(
                        "Removed in 4.2.0 (joblist pull request)!"
                    )
                except Exception:
                    Log.warning(
                        "Couldn't recover the Historical database, AS will continue without it, GUI may be affected"
                    )
            if detail:
                output = "txt"
            if output == "txt":
                noplot = False
            try:
                Log.info("\nPlotting the jobs list...")
                if (
                    len(as_conf.experiment_data.get("WRAPPERS", {})) > 0
                    and check_wrappers
                ):
                    as_conf.check_conf_files(
                        running_time=True, force_load=True, no_log=False
                    )
                    generate_scripts_andor_wrappers(as_conf, job_list, True)
                    job_list.load_wrappers(preview=check_wrappers)
            except AutosubmitCritical as e:
                Log.warning(f"Couldn't generate a preview of the wrappers due: {e}")

            if not noplot:
                from autosubmit.monitor.monitor import Monitor

                if group_by:
                    status = []
                    if expand_status:
                        for s in expand_status.split():
                            status.append(get_job_status(s.upper()))

                    job_grouping = JobGrouping(
                        group_by,
                        copy.deepcopy(job_list.get_job_list()),
                        job_list,
                        expand_list=expand,
                        expanded_status=status,
                    )
                    groups_dict = job_grouping.group_jobs()
                monitor_exp = Monitor(edge_info=job_list.graph_dict_by_job_name)
                # if output is set, use output
                monitor_exp.generate_output(
                    expid,
                    job_list.get_job_list(),
                    os.path.join(
                        BasicConfig.LOCAL_ROOT_DIR, expid, "tmp", f"LOG_{expid}"
                    ),
                    output if output is not None else output_type,
                    list(job_list.job_package_map.values()),
                    not hide,
                    groups=groups_dict,
                    job_list_object=job_list,
                )
            Log.result("\nJob list created successfully")
            Log.warning("Remember to MODIFY the MODEL config files!")
            fh.flush()
            os.fsync(fh.fileno())
            if detail:
                print_job_details(job_list)
            return 0
        except KeyboardInterrupt:
            # Setting signal handler to handle subsequent CTRL-C
            signal.signal(signal.SIGINT, _signal_handler_create)
            fh.flush()
            os.fsync(fh.fileno())
            raise AutosubmitCritical("Stopped by user input", 7010)


def archive(expid: str, noclean=True, uncompress=True, create_rocrate=False) -> bool:
    """Archives an experiment: call clean (if experiment is of version 3 or later), compress folder
    to tar.gz and moves to year's folder

    :param expid: experiment identifier
    :param noclean: flag telling it whether to clean the experiment or not.
    :param uncompress: flag telling it whether to decompress or not.
    :param create_rocrate: flag to enable RO-Crate
    :return: ``True`` if the experiment has been successfully archived. ``False`` otherwise.
    """
    exp_folder = Path(BasicConfig.LOCAL_ROOT_DIR).joinpath(expid)

    if not noclean:
        # Cleaning to reduce file size.
        version = db_common.get_autosubmit_version(expid)
        if (
            version is not None
            and version.startswith("3")
            and not clean(expid, True, True, True)
        ):
            raise AutosubmitCritical(
                "Can not archive project. Clean not successful", 7012
            )

    # Getting year of last completed. If not, year of expid folder
    year = None
    tmp_folder = exp_folder.joinpath(BasicConfig.LOCAL_TMP_DIR)
    if tmp_folder.is_dir():
        for filename in tmp_folder.iterdir():
            if filename.name.endswith("COMPLETED"):
                file_year = localtime(
                    tmp_folder.joinpath(filename).stat().st_mtime
                ).tm_year
                if year is None or year < file_year:
                    year = file_year

    if year is None:
        year = localtime(os.path.getmtime(exp_folder)).tm_year
    try:
        year_path = Path(BasicConfig.LOCAL_ROOT_DIR).joinpath(str(year))
        if not year_path.exists():
            year_path.mkdir(mode=0o775, parents=True)
    except Exception as e:
        raise AutosubmitCritical(
            f"Failed to create year-directory {str(year)} for experiment {expid}",
            7012,
            str(e),
        )
    Log.info(f"Archiving in year {str(year)}")

    if create_rocrate:
        rocrate(expid, year_path)
        Log.info("RO-Crate ZIP file created!")
    else:
        # Creating tar file
        Log.info("Creating tar file ... ")
        try:
            compress_type: str
            if not uncompress:
                compress_type = "w:gz"
                output_filepath = f"{expid}.tar.gz"
            else:
                compress_type = "w"
                output_filepath = f"{expid}.tar"
            year_path_file = year_path.joinpath(output_filepath)
            with tarfile.open(year_path_file, compress_type) as tar:
                tar.add(exp_folder, arcname="")
                tar.close()
                year_path_file.chmod(mode=0o775)
        except Exception as e:
            raise AutosubmitCritical("Can not write tar file", 7012, str(e))

        Log.info("Tar file created!")

    try:
        rmtree(exp_folder)
    except Exception as e:
        Log.warning(f"Can not fully remove experiments folder: {str(e)}")
        if os.stat(exp_folder):
            try:
                tmp_folder = os.path.join(BasicConfig.LOCAL_ROOT_DIR, "tmp")
                tmp_expid = os.path.join(tmp_folder, expid + "_to_delete")
                os.rename(exp_folder, tmp_expid)
                Log.warning(f"Experiment folder renamed to: {exp_folder}_to_delete")
            except Exception as e:
                unarchive(expid, uncompressed=False, create_rocrate=create_rocrate)
                raise AutosubmitCritical(
                    "Can not remove or rename experiments folder", 7012, str(e)
                )

    Log.result("Experiment archived successfully")
    return True


def unarchive(experiment_id: str, uncompressed=True, create_rocrate=False) -> bool:
    """Unarchives an experiment.

    Decompress folder from tar.gz and moves to experiment root folder.

    :param experiment_id: experiment identifier
    :param uncompressed: if True, the tar file is uncompressed
    :param create_rocrate: flag to enable RO-Crate
    :return: True if successful, False otherwise
    """
    exp_folder = Path(BasicConfig.LOCAL_ROOT_DIR, experiment_id)

    # Searching by year. We will store it on database
    year = datetime.today().year
    archive_path = Path()
    if create_rocrate:
        compress_type = None
        output_pathfile = f"{experiment_id}.zip"
    elif not uncompressed:
        compress_type = "r:gz"
        output_pathfile = f"{experiment_id}.tar.gz"
    else:
        compress_type = "r:"
        output_pathfile = f"{experiment_id}.tar"
    while year > 2000:
        archive_path = Path(BasicConfig.LOCAL_ROOT_DIR).joinpath(
            str(year), output_pathfile
        )
        if archive_path.exists():
            break
        year -= 1

    if year == 2000:
        Log.error(f"Experiment {experiment_id} is not archived")
        return False
    Log.info(f"Experiment located in {year} archive")

    # Creating tar file
    Log.info("Unpacking tar file ... ")
    exp_folder.mkdir(exist_ok=True, parents=True)
    try:
        if create_rocrate:
            import zipfile

            with zipfile.ZipFile(str(archive_path), "r") as zip_file:
                zip_file.extractall(exp_folder)
        else:
            with tarfile.open(archive_path, compress_type) as tar:
                tar.extractall(exp_folder)
                tar.close()
    except Exception as e:
        rmtree(exp_folder, ignore_errors=True)
        Log.printlog(f"Can not extract file: {str(e)}", 6012)
        return False

    Log.info("Unpacking finished")

    try:
        archive_path.unlink()
    except Exception as e:
        Log.printlog(f"Can not remove archived file folder: {str(e)}", 7012)
        Log.result(f"Experiment {experiment_id} unarchived successfully")
        return True

    Log.result(f"Experiment {experiment_id} unarchived successfully")
    return True


def rocrate(expid: str, path: Path) -> "ROCrate | None":
    """Produces an RO-Crate archive for an Autosubmit experiment.

    Skips other crate ZIP archive files in ``tmp/ASLOGS``. It ignores
    files that start with ``<EXPID>-crate`` and end with ``.zip``.

    :param expid: experiment ID
    :param path: path to save the RO-Crate in
    :return: ``True`` if successful, ``False`` otherwise
    """
    from textwrap import dedent

    from autosubmit.statistics.statistics import Statistics

    as_conf = AutosubmitConfig(expid)
    # ``.reload`` will call the function to unify the YAML configuration.
    as_conf.reload(True)

    workflow_configuration = as_conf.experiment_data

    # Load the rocrate prepopulated file, or raise an error and write the template.
    # Similar to what COMPSs does.
    # See: https://github.com/bsc-wdc/compss/blob/9e79542eef60afa9e288e7246e697bd7ac42db08/compss/runtime/scripts/
    #      system/provenance/generate_COMPSs_RO-Crate.py
    rocrate_json = workflow_configuration.get("ROCRATE", None)
    if not rocrate_json:
        Log.error(
            dedent("""\
            No ROCRATE configuration value provided! Use it to create your
            JSON-LD schema, using @id, @type, and other schema.org attributes,
            and it will be merged with the values retrieved from the workflow
            configuration. Some values are not present in Autosubmit, such as
            license, so you must provide it if you want to include in your
            RO-Crate data, e.g. create a file $expid/conf/rocrate.yml (or use
            an existing one) with a top level ROCRATE key, containing your
            JSON-LD data:

            ROCRATE:
              INPUTS:
                # Add the extra keys to be exported.
                - "MHM"
              OUTPUTS:
                # Relative to the Autosubmit project folder.
                - "*/*.gif"
              PATCH: |
                {
                  "@graph": [
                    {
                      "@id": "./",
                      "license": "Apache-2.0",
                      "creator": {
                        "@id": "https://orcid.org/0000-0001-8250-4074"
                      }
                    },
                    {
                      "@id": "https://orcid.org/0000-0001-8250-4074",
                      "@type": "Person",
                      "affiliation": {
                          "@id": "https://ror.org/05sd8tv96"
                      }
                    },
                    ...
                  ]
                }
            """)
        )
        raise AutosubmitCritical(
            "You must provide an ROCRATE configuration key when using RO-Crate...",
            7014,
        )

    # Read job list (from pickles) to retrieve start and end time.
    # Code adapted from ``autosubmit stats``.
    job_list = load_job_list(expid, as_conf)
    jobs = job_list.get_job_list()
    exp_stats = Statistics(jobs=jobs, start=None, end=None, queue_time_fix={})
    exp_stats.calculate_statistics()
    start_time = None
    end_time = None
    # N.B.: ``exp_stats.jobs_stat`` is sorted in reverse order.
    number_of_jobs = len(exp_stats.jobs_stat)
    if number_of_jobs > 0:
        start_time = (
            exp_stats.jobs_stat[-1].start_time.replace(microsecond=0).isoformat()
        )
    if number_of_jobs > 1:
        end_time = exp_stats.jobs_stat[0].finish_time.replace(microsecond=0).isoformat()

    from autosubmit.provenance.rocrate import create_rocrate_archive

    return create_rocrate_archive(
        as_conf, rocrate_json, jobs, start_time, end_time, path
    )


def provenance(expid: str, create_rocrate: bool = False) -> bool:
    """Create the experiment provenance archive.

    :param expid: experiment identifier
    :param create_rocrate: flag to enable RO-Crate
    """

    if not create_rocrate:
        msg = "Can not create RO-Crate ZIP file. Argument '--rocrate' required"
        raise AutosubmitCritical(msg, 7012)

    aslogs_folder = Path(
        BasicConfig.LOCAL_ROOT_DIR,
        expid,
        BasicConfig.LOCAL_TMP_DIR,
        BasicConfig.LOCAL_ASLOG_DIR,
    )

    try:
        r = rocrate(expid, Path(aslogs_folder))
        Log.info("RO-Crate ZIP file created!")
        return r is not None
    except Exception as e:
        raise AutosubmitCritical(f"Error creating RO-Crate ZIP file: {str(e)}", 7012)


def report(
    expid: str,
    template_file_path="",
    show_all_parameters=False,
    folder_path="",
    placeholders=False,
) -> bool:
    """Show the report for a specified experiment.

    :param expid: Experiment identifier.
    :param template_file_path: Path to a template file.
    :param show_all_parameters: Show all parameters.
    :param folder_path: Path to folder.
    :param placeholders: Show placeholders.
    :return: True if successful, False otherwise.
    """
    try:
        ignore_performance_keys = ["error_message", "warnings_job_data", "considered"]
        exp_path = os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid)
        tmp_path = os.path.join(exp_path, BasicConfig.LOCAL_TMP_DIR)
        if folder_path is not None and len(str(folder_path)) > 0:
            tmp_path = folder_path
        # Gather experiment info
        as_conf = AutosubmitConfig(expid)
        try:
            as_conf.reload(True)
            parameters = as_conf.load_parameters()
        except Exception:
            raise AutosubmitCritical(
                "Unable to gather the parameters from config files, check permissions.",
                7012,
            )
        # Performance Metrics call
        try:
            import requests

            BasicConfig.read()
            request = requests.get(
                f"{BasicConfig.AUTOSUBMIT_API_URL}/performance/{expid}"
            )
            performance_metrics = json.loads(request.text)
            # If error, then None
            performance_metrics = (
                None
                if performance_metrics and performance_metrics["error"] is True
                else performance_metrics
            )
            if performance_metrics:
                for key in ignore_performance_keys:
                    performance_metrics.pop(key, None)
        except Exception:
            Log.printlog("Autosubmit couldn't retrieve performance metrics.")
            performance_metrics = None
        # Preparation for section parameters
        try:
            submitter = ParamikoSubmitter(as_conf=as_conf)
            hpcarch = submitter.platforms[as_conf.get_platform()]
        except Exception as e:
            Log.warning(
                f"Failed creating Paramiko submitter, will try loading only the local platform: {str(e)}"
            )
            submitter = ParamikoSubmitter(as_conf=as_conf)
            hpcarch = submitter.platforms[as_conf.get_platform()]

        job_list = load_job_list(expid, as_conf)
        for job in job_list.get_job_list():
            if job.platform_name is None or job.platform_name == "":
                job.platform_name = hpcarch.name
            job.platform = submitter.platforms[job.platform_name]

        if show_all_parameters:
            Log.info("Gathering all parameters (all keys are on upper_case)")
            parameter_output = f"{expid}_parameter_list_{datetime.today().strftime('%Y%m%d-%H%M%S')}.txt"
            parameter_path = os.path.join(tmp_path, parameter_output)
            # parameters
            parameters = as_conf.load_parameters()
            global_keys = set(parameters.keys())
            jobs_parameters = {}
            try:
                for job in job_list.get_job_list():
                    job_parameters = job.update_parameters(as_conf, set_attributes=True)
                    for key, value in job_parameters.items():
                        if key in global_keys or key.startswith("JOBS."):
                            continue
                        jobs_parameters["JOBS" + "." + job.section + "." + key] = value
            except Exception:
                pass
            parameters.update(jobs_parameters)
            with open(parameter_path, "w") as parameter_file:
                for key, value in parameters.items():
                    if value is not None and len(str(value)) > 0:
                        full_value = key + "=" + str(value) + "\n"
                        parameter_file.write(full_value)
                    else:
                        if placeholders:
                            parameter_file.write(key + "=" + "%" + key + "%" + "\n")
                        else:
                            parameter_file.write(key + "=" + "-" + "\n")

                if performance_metrics:
                    for key in performance_metrics:
                        parameter_file.write(
                            f"{key} = {performance_metrics.get(key, '-')}\n"
                        )

            os.chmod(os.path.join(tmp_path, parameter_output), 0o755)
            Log.result(
                f"A list of all parameters has been written on {os.path.join(tmp_path, parameter_output)}"
            )

        if template_file_path is not None:
            if not os.path.exists(template_file_path):
                raise AutosubmitCritical(
                    f"Template {template_file_path} doesn't exist ", 7014
                )
            Log.info("Rendering report template (keys are case-insensitive)")
            with open(template_file_path, "r") as f:
                template_content = f.read()
            lookup = {k.upper(): str(v) for k, v in parameters.items()}
            if performance_metrics:
                lookup.update(
                    {k.upper(): str(v) for k, v in performance_metrics.items()}
                )
            unknown: list[str] = []

            def _sub(match):
                escaped, key = match.group(1), match.group(2)
                if escaped is not None:
                    return f"%{escaped}%"
                value = lookup.get(key.upper())
                if value is None or value == "":
                    unknown.append(key)
                    return f"%{key}%" if placeholders else "-"
                return value

            rendered = sub(r"%%([^%\s]+)%%|%([^%\s]+)%", _sub, template_content)
            if unknown:
                unique = sorted(set(unknown))
                sample = ", ".join(unique[:10])
                suffix = "..." if len(unique) > 10 else ""
                rendering = "kept verbatim" if placeholders else 'replaced with "-"'
                Log.warning(
                    f"{len(unique)} placeholder(s) in the template did not "
                    f"match any parameter and were {rendering}: {sample}{suffix}"
                )
            template_suffix = os.path.splitext(template_file_path)[1] or ".txt"
            report = (
                f"{expid}_report_"
                f"{datetime.today().strftime('%Y%m%d-%H%M%S')}"
                f"{template_suffix}"
            )
            report_path = os.path.join(tmp_path, report)
            with open(report_path, "w") as f:
                f.write(rendered)
            os.chmod(report_path, 0o755)
            Log.result(f"Report {report} has been created on {report_path}")
        return True
    except AutosubmitError:
        raise
    except AutosubmitCritical:
        raise
    except Exception as e:
        raise AutosubmitCritical(
            "Unknown error while reporting the parameters list, likely it is due IO issues",
            7040,
            str(e),
        )


def update_version(expid: str) -> bool:
    """Refresh the experiment version with the current Autosubmit version.

    :param expid: experiment identifier
    :return: True if successful, False otherwise
    """
    as_conf = AutosubmitConfig(expid)
    as_conf.reload(force_load=True)
    as_conf.check_expdef_conf()

    autosubmit_version = get_version()
    Log.info(
        f"Changing {expid} experiment version from {as_conf.get_version()} to {autosubmit_version}"
    )
    as_conf.set_version(autosubmit_version)
    update_experiment_description_version(expid, version=autosubmit_version)

    return True


def describe(
    input_experiment_list="*", get_from_user=""
) -> (
    tuple[str | Any, str | datetime | Any, str | Any, str | Any, str | Any]
    | None
    | bool
):
    """Show details for the specified experiment.

    :param input_experiment_list: experiments identifier:
    :param get_from_user: user to get the experiments from
    :return: tuple with user, created time, model, branch, and HPC
    """
    if get_from_user == "*" or get_from_user == "":
        get_from_user = pwd.getpwuid(os.getuid())[0]

    user = created = model = branch = hpc = ""
    not_described_experiments = []
    experiments_ids_not_in_db = []

    if "," in input_experiment_list:
        requested = [
            e.strip().lower() for e in input_experiment_list.split(",") if e.strip()
        ]
    elif "*" in input_experiment_list:
        requested = None  # all experiments
    else:
        requested = [
            e.strip().lower() for e in input_experiment_list.split(" ") if e.strip()
        ]

    if requested is None:
        experiments_ids = sorted(get_experiment_expids())
    else:
        found = get_experiment_expids(expids=requested)
        experiments_ids = []
        for e in requested:
            (experiments_ids if e in found else experiments_ids_not_in_db).append(e)

    if experiments_ids_not_in_db:
        Log.warning(
            f"Experiments not found in the database, skipping: {experiments_ids_not_in_db}"
        )

    for experiment_id in experiments_ids:
        exp_path = Path(BasicConfig.LOCAL_ROOT_DIR).joinpath(experiment_id)
        if exp_path.is_dir():
            with suppress(OSError, KeyError, TypeError):
                folder_owner = pwd.getpwuid(exp_path.stat().st_uid).pw_name
                if folder_owner != get_from_user:
                    continue
        try:
            try:
                # Preferred source of truth: the on-disk config files.
                as_conf = AutosubmitConfig(experiment_id)
                as_conf.check_conf_files(False, no_log=True)

                uid = int(Path(as_conf.conf_folder_yaml).stat().st_uid)
                try:
                    user = pwd.getpwuid(uid).pw_name
                except (KeyError, TypeError, OverflowError):
                    Log.warning(
                        "The user does not exist anymore in the system, using id instead"
                    )
                    user = str(uid)

                created = datetime.fromtimestamp(
                    Path(as_conf.conf_folder_yaml).stat().st_mtime
                )

                if as_conf.get_svn_project_url():
                    model = branch = as_conf.get_svn_project_url()
                else:
                    model = as_conf.get_git_project_origin()
                    branch = as_conf.get_git_project_branch()
                if model == "":
                    model = "Not Found"
                if branch == "":
                    branch = "Not Found"

                submitter = ParamikoSubmitter(as_conf=as_conf)
                if not submitter.platforms:
                    return False
                hpc = as_conf.get_platform()

                description = get_experiment_description(experiment_id)
                description = description[0][0] if description else ""
            except Exception as e:
                Log.warning(f"Experiment files are not available: {str(e)}")
                # Files are not available (e.g. archived): fall back to the
                # last snapshot stored in the database.
                snapshot = ExperimentDetails(
                    experiment_id, init_reload=False
                ).get_details()
                if not snapshot:
                    raise
                user = snapshot["user"]
                created = snapshot["created"]
                model = snapshot["model"]
                branch = snapshot["branch"]
                hpc = snapshot["hpc"]
                description = get_experiment_description(experiment_id)
                description = description[0][0] if description else ""
                Log.info(
                    f"Experiment '{experiment_id}' files not found; "
                    f"it may have been archived. Showing the last "
                    f"stored snapshot."
                )
            Log.result(f"Describing {experiment_id}")
            Log.result(f"Owner: {user}")
            Log.result(f"Location: {exp_path}")
            Log.result(f"Created: {created}")
            Log.result(f"Model: {model}")
            Log.result(f"Branch: {branch}")
            Log.result(f"HPC: {hpc}")
            Log.result(f"Description: {description}")
        except Exception as e:
            Log.warning(f"Failed to describe experiment {experiment_id}: {str(e)}")
            not_described_experiments.append(experiment_id)
    if len(not_described_experiments) > 0:
        Log.printlog(
            f"Could not describe the following experiments:\n{not_described_experiments}",
            Log.WARNING,
        )
    if len(experiments_ids) == 1:
        # for backward compatibility or GUI
        return user, created, model, branch, hpc
    elif len(experiments_ids) == 0:
        Log.result(
            f"No experiments found for expid={input_experiment_list} and user {get_from_user}"
        )
    return None


def _create_project_associated_conf(
    as_conf: AutosubmitConfig, force_model_conf: bool, force_jobs_conf: bool
) -> None:
    project_destiny = as_conf.get_file_project_conf()
    jobs_destiny = as_conf.get_file_jobs_conf()

    if as_conf.get_project_type() != "none":
        if as_conf.get_file_project_conf():
            copy = True
            if os.path.exists(
                os.path.join(as_conf.get_project_dir(), as_conf.get_file_project_conf())
            ):
                if os.path.exists(project_destiny):
                    if force_model_conf:
                        os.rename(project_destiny, str(project_destiny) + "_backup")
                    else:
                        copy = False
                if copy:
                    copyfile(
                        os.path.join(
                            as_conf.get_project_dir(), as_conf.get_file_project_conf()
                        ),
                        project_destiny,
                    )

        if as_conf.get_file_jobs_conf():
            copy = True
            if os.path.exists(
                os.path.join(as_conf.get_project_dir(), as_conf.get_file_jobs_conf())
            ):
                if os.path.exists(jobs_destiny):
                    if force_jobs_conf:
                        os.rename(jobs_destiny, str(jobs_destiny) + "_backup")
                    else:
                        copy = False
                if copy:
                    copyfile(
                        os.path.join(
                            as_conf.get_project_dir(), as_conf.get_file_jobs_conf()
                        ),
                        jobs_destiny,
                    )


def refresh(expid: str, model_conf: bool, jobs_conf: bool):
    """Refresh the project folder for a given experiment.

    :param expid: Experiment identifier.
    :param model_conf:
    :param jobs_conf:
    """
    try:
        as_conf = AutosubmitConfig(expid)
        as_conf.reload(force_load=True)
    except (AutosubmitError, AutosubmitCritical):
        raise
    except Exception as e:
        raise AutosubmitCritical(
            "Error while reading the configuration files", 7064, str(e)
        )
    try:
        # FIXME: as_conf is not validated, so it never enters the if statement below.
        #        Should be deleted.
        if "Expdef" in as_conf.wrong_config:
            as_conf.show_messages()
        project_type = as_conf.get_project_type()
        if copy_code(as_conf, expid, project_type, True):
            Log.result("Project folder updated")
        _create_project_associated_conf(as_conf, model_conf, jobs_conf)
    except (AutosubmitError, AutosubmitCritical):
        raise
    except Exception as e:
        raise AutosubmitCritical("  Download failed", 7064, str(e))
    return True
