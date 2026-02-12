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

"""Module containing functions to manage autosubmit's experiments."""

import os
import pwd
import string
from pathlib import Path
from shutil import rmtree
from typing import Optional

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database import db_common
from autosubmit.experiment.detail_updater import ExperimentDetails
from autosubmit.helpers.processes import process_id
from autosubmit.helpers.utils import user_yes_no_query
from autosubmit.log.log import Log, AutosubmitCritical, AutosubmitError

__all__ = [
    'base36encode',
    'base36decode',
    'check_ownership',
    'copy_experiment',
    'create_required_folders',
    'delete_experiment',
    'is_valid_experiment_id',
    'new_experiment',
    'next_experiment_id'
]

Log.get_logger("Autosubmit")


def new_experiment(description, version, test=False, operational=False, evaluation=False):
    """
    Stores a new experiment on the database and generates its identifier

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
        if last_exp_name == '':
            return ''
        if last_exp_name == 'empty':
            if test:
                # test identifier restricted also to 4 characters.
                new_name = 't000'
            elif operational:
                # operational identifier restricted also to 4 characters.
                new_name = 'o000'
            elif evaluation:
                # evaluation identifier restricted also to 4 characters.
                new_name = 'e000'
            else:
                new_name = 'a000'
        else:
            new_name = last_exp_name
            if new_name == '':
                return ''
        while db_common.check_experiment_exists(new_name, False):
            new_name = next_experiment_id(new_name)
            if new_name == '':
                return ''
        if not db_common.save_experiment(new_name, description, version):
            return ''
        Log.info('The new experiment "{0}" has been registered.', new_name)
        return new_name
    except Exception as e:
        raise AutosubmitCritical(f'Error while generating a new experiment in the db: {e}',
                                 7011) from e


def delete_experiment(expids: str, force: bool) -> bool:
    """Deletes an experiment from the database,
    the experiment's folder database entry and all the related metadata files.

    :param expids: List of experiment IDs to delete.
    :param force: Ask for confirmation if ``False``.
    :returns: ``True`` if successful, ``False`` otherwise.
    :raises AutosubmitCritical: If the experiment does not exist or if there are insufficient permissions.
    """
    # expid will come from argparse, which provides nix-style comma-separated values,
    # so here we parse the comma-separated values. ``.fromkeys`` keeps order and removes
    # duplicates.
    expid_list = expids.replace(',', ' ').split(' ')
    expid_list = [expid.lower() for expid in filter(lambda x: x, expid_list)]

    failed: list[str] = []

    for expid in expid_list:
        try:
            _delete_experiment(expid, force)
        except Exception as e:
            Log.error(f'Failed to delete experiment {expid}: {str(e)}')
            failed.append(expid)

    if failed:
        Log.error(f"Deletion failed for experiments: {', '.join(failed)}")
        return False

    return True


def _delete_experiment(expid: str, force: bool) -> None:
    if process_id(expid) is not None:
        raise AutosubmitCritical("Ensure no processes are running in the experiment directory", 7076)

    experiment_path = Path(f"{BasicConfig.LOCAL_ROOT_DIR}/{expid}")
    if not experiment_path.exists():
        raise AutosubmitCritical("Experiment does not exist", 7012)

    confirm_removal = force or user_yes_no_query(f"Do you want to delete {expid} ?")

    if not confirm_removal:
        Log.info(f'Experiment {expid} deletion cancelled by user')
        return

    Log.info(f'Deleting experiment {expid}')

    # Try to delete the experiment details
    try:
        ExperimentDetails(expid).delete_details()
    except Exception as e:
        Log.warning(f'Failed to delete DB details for experiment {expid}: {str(e)}')
        raise

    try:
        _delete_expid(expid, force)
        Log.info(f'Experiment {expid} has been deleted')
    except Exception as e:
        raise AutosubmitCritical("Seems that something went wrong, please check the trace", 7012, str(e))


def _delete_expid(expid_delete: str, force: bool = False) -> None:
    """Removes an experiment from the path and database.
    If the current user is eadmin and the -f flag has been sent, it deletes regardless of experiment owner.

    :param expid_delete: Identifier of the experiment to delete.
    :type expid_delete: str
    :param force: If True, does not ask for confirmation.
    :type force: bool

    :returns: True if successfully deleted, False otherwise.
    :rtype: bool

    :raises AutosubmitCritical: If the experiment does not exist or if there are insufficient permissions.
    """

    if not expid_delete:
        raise AutosubmitCritical("Experiment identifier is required for deletion.", 7011)
    experiment_path = Path(f"{BasicConfig.LOCAL_ROOT_DIR}/{expid_delete}")
    structure_db_path = Path(f"{BasicConfig.STRUCTURES_DIR}/structure_{expid_delete}.db")
    job_data_db_path = Path(f"{BasicConfig.JOBDATA_DIR}/job_data_{expid_delete}")
    experiment_path = experiment_path.resolve()
    structure_db_path = structure_db_path.resolve()
    job_data_db_path = job_data_db_path.resolve()
    if Path(BasicConfig.LOCAL_ROOT_DIR) == experiment_path or \
            Path(BasicConfig.STRUCTURES_DIR) == structure_db_path or \
            Path(BasicConfig.JOBDATA_DIR) == job_data_db_path:
        raise AutosubmitCritical(f"Invalid paths for experiment deletion: {expid_delete}. "
                                 "Paths must not be the root directories.", 7011)

    if not experiment_path.is_relative_to(BasicConfig.LOCAL_ROOT_DIR):
        raise AutosubmitCritical(f"Invalid paths for experiment deletion: {expid_delete}. "
                                 "Paths must be within the configured directories.", 7011)

    if not experiment_path.exists():
        Log.printlog("Experiment directory does not exist.", Log.WARNING)
        return

    owner, eadmin, _ = check_ownership(expid_delete)
    if not (owner or (force and eadmin)):
        if not eadmin:
            raise AutosubmitCritical(
                f"Detected Eadmin user however, -f flag is not found. {expid_delete} cannot be deleted!", 7012)
        else:
            raise AutosubmitCritical(
                f"Current user is not the owner of the experiment. {expid_delete} cannot be deleted!", 7012)

    message_parts = [
        f"The {expid_delete} experiment was removed from the local disk and from the database.",
        "Note that this action does not delete any data written by the experiment.",
        "Complete list of files/directories deleted:",
        ""
    ]
    message_parts.extend(f"{path}" for path in experiment_path.rglob('*'))
    message_parts.append(f"{structure_db_path}")
    message_parts.append(f"{job_data_db_path}.db")
    message_parts.append(f"{job_data_db_path}.sql")
    message = '\n'.join(message_parts)

    error_message = _perform_deletion(experiment_path, structure_db_path, job_data_db_path, expid_delete)

    if not error_message:
        Log.printlog(message, Log.RESULT)
    else:
        Log.printlog(error_message, Log.ERROR)
        raise AutosubmitError(
            "Some experiment files weren't correctly deleted\n"
            "Please if the trace shows DATABASE IS LOCKED, report it to git\n"
            "If there are I/O issues, wait until they're solved and then use this command again.\n",
            6004, error_message
        )


def _perform_deletion(experiment_path: Path, structure_db_path: Path, job_data_db_path: Path,
                      expid_delete: str) -> str:
    """Perform the deletion of an experiment, including its directory, structure database, and job data database.

    :param experiment_path: Path to the experiment directory.
    :param structure_db_path: Path to the structure database file.
    :param job_data_db_path: Path to the job data database file.
    :param expid_delete: Identifier of the experiment to delete.
    :return: An error message if any errors occurred during deletion, otherwise an empty string.
    """
    error_message = []

    is_sqlite = BasicConfig.DATABASE_BACKEND == 'sqlite'

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


def copy_experiment(experiment_id, description, version, test=False, operational=False, evaluation=False):
    """
    Creates a new experiment by copying an existing experiment

    :param version: experiment's associated autosubmit version
    :type version: str
    :param experiment_id: identifier of experiment to copy
    :type experiment_id: str
    :param description: experiment's description
    :type description: str
    :param test: specifies if it is a test experiment
    :type test: bool
    :param operational: specifies if it is an operational experiment
    :type operational: bool
    :param evaluation: specifies if it is an evaluation experiment
    :type evaluation: bool
    :return: experiment id for the new experiment
    :rtype: str
    """
    try:
        if not db_common.check_experiment_exists(experiment_id):
            return ''
        new_name = new_experiment(description, version, test, operational, evaluation)
        return new_name
    except Exception as e:
        raise AutosubmitCritical(f"Error while copying the experiment {experiment_id} "
                                 f"as a new experiment in the db: {e}", 7011) from e


def next_experiment_id(current_id):
    """
    Get next experiment identifier

    :param current_id: previous experiment identifier
    :type current_id: str
    :return: new experiment identifier
    :rtype: str
    """
    if not is_valid_experiment_id(current_id):
        return ''
    # Convert the name to base 36 in number add 1 and then encode it
    next_id = base36encode(base36decode(current_id) + 1)
    return next_id if is_valid_experiment_id(next_id) else ''


def is_valid_experiment_id(name):
    """
    Checks if it is a valid experiment identifier

    :param name: experiment identifier to check
    :type name: str
    :return: name if is valid, terminates program otherwise
    :rtype: str
    """
    name = name.lower()
    if len(name) < 4 or not name.isalnum():
        raise AutosubmitCritical("Incorrect experiment, it must have exactly 4 alphanumeric chars")
    return True


def base36encode(number, alphabet=string.digits + string.ascii_lowercase):
    """
    Convert positive integer to a base36 string.

    :param number: number to convert
    :type number: int
    :param alphabet: set of characters to use
    :type alphabet: str
    :return: number's base36 string value
    :rtype: str
    """
    if not isinstance(number, int):
        raise TypeError('number must be an integer')

    # Special case for zero
    if number == 0:
        return '0'

    base36 = ''

    sign = ''
    if number < 0:
        sign = '-'
        number = - number

    while number > 0:
        number, i = divmod(number, len(alphabet))
        # noinspection PyAugmentAssignment
        base36 = alphabet[i] + base36

    return sign + base36.rjust(4, '0')


def base36decode(number):
    """
    Converts a base36 string to a positive integer

    :param number: base36 string to convert
    :type number: str
    :return: number's integer value
    :rtype: int
    """
    return int(number, 36)


def create_required_folders(exp_id: str, exp_folder: Path) -> None:
    """Create the minimum set of required folders for an Autosubmit experiment.

    The newly created folders will be relative to the given experiment folder.

    Each new folder with have the sme permission, ``755`` (important if you are
    expecting something else, e.g. umask).

    :param exp_id: experiment identifier
    :param exp_folder: experiment folder
    :raises IOError: if there are errors creating the new experiment folders (permission, not found, etc.)
    """
    dir_mode = 0o755

    exp_folder.mkdir(mode=dir_mode)

    required_dirs = ["conf", "pkl", "tmp", "tmp/ASLOGS", f"tmp/LOG_{exp_id}", "plot", "status"]
    for required_dir in required_dirs:
        Path(exp_folder / required_dir).mkdir(mode=dir_mode)


def check_ownership(expid: str, raise_error=False) -> tuple[bool, bool, Optional[str]]:
    """Check if the user owns and if it is eadmin.

    :return: the owner, eadmin and current_owner
    """
    current_owner = None
    eadmin = False
    owner = False
    current_user_id = os.getuid()
    # TODO: to be improved in #944
    admin_user = "eadmin"
    try:
        eadmin = current_user_id == pwd.getpwnam(admin_user).pw_uid
    except Exception as e:
        Log.info(f"Autosubmit admin user: {admin_user} is not set: {str(e)}")
    current_owner_id = Path(BasicConfig.LOCAL_ROOT_DIR, expid).stat().st_uid
    try:
        current_owner = pwd.getpwuid(current_owner_id).pw_name
    except (TypeError, KeyError) as e:
        Log.warning(f"Current owner of experiment {expid} could not be retrieved. "
                    f"The owner is no longer in the system database: {str(e)}")
    if current_owner_id == current_user_id:
        owner = True
    elif raise_error:
        raise AutosubmitCritical(f"You do not own the experiment {expid}.", 7012)
    return owner, eadmin, current_owner
