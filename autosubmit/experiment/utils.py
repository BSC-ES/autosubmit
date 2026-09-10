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

"""Module containing functions to manage autosubmit's experiments."""

import os
import pwd
import string
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.log.log import AutosubmitCritical, Log

if TYPE_CHECKING:
    from autosubmit.job.job_list import JobList

__all__ = [
    "base36decode",
    "base36encode",
    "check_ownership",
    "create_required_folders",
    "experiment_exists",
    "get_experiment_owner",
    "is_valid_experiment_id",
    "next_experiment_id",
    "print_job_details",
]


def next_experiment_id(current_id: str) -> str:
    """Get next experiment identifier.

    :param current_id: previous experiment identifier
    :return: new experiment identifier
    """
    if not is_valid_experiment_id(current_id):
        return ""
    # Convert the name to base 36 in number add 1 and then encode it
    next_id = base36encode(base36decode(current_id) + 1)
    return next_id if is_valid_experiment_id(next_id) else ""


def is_valid_experiment_id(expid: str) -> bool:
    """Checks if it is a valid experiment identifier.

    :param expid: The experiment identifier.
    :return: Whether the experiment identifier is valid.
    """
    expid = expid.lower()
    return len(expid) >= 4 and expid.isalnum()


def experiment_exists(expid: str) -> bool:
    """Checks if an experiment exists.

    :param expid: The experiment identifier.
    """
    expected_expid_path = Path(BasicConfig.LOCAL_ROOT_DIR, expid)
    return expected_expid_path.exists() and expected_expid_path.is_dir()


def base36encode(
    number: int, alphabet: str = string.digits + string.ascii_lowercase
) -> str:
    """Convert a positive integer to a base36 string.

    :param number:Number to convert
    :param alphabet: Set of characters to use
    :return: Number's base36 string value
    """
    if not isinstance(number, int):
        raise TypeError("number must be an integer")

    # Special case for zero
    if number == 0:
        return "0"

    base36 = ""

    sign = ""
    if number < 0:
        sign = "-"
        number = -number

    while number > 0:
        number, i = divmod(number, len(alphabet))
        # noinspection PyAugmentAssignment
        base36 = alphabet[i] + base36

    return sign + base36.rjust(4, "0")


def base36decode(number: str) -> int:
    """Convert a base36 string to a positive integer.

    :param number: base36 string to convert
    :return: number's integer value
    """
    return int(number, 36)


def create_required_folders(exp_id: str, exp_folder: Path) -> None:
    """Create the required folders for an Autosubmit experiment.

    The plot and status directories are created with group-writable permissions (775),
    while all other required directories use the default Autosubmit permissions (755).

    This allows shared users to write to experiment output directories while keeping
    the remaining directory structure more restrictive.

    :param exp_id: Experiment identifier.
    :param exp_folder: Experiment folder.
    :raises OSError: If a folder cannot be created or its permissions cannot
        be set.
    """
    required_dirs = {
        "conf": 0o755,
        "db": 0o755,
        "tmp": 0o755,
        "tmp/ASLOGS": 0o755,
        f"tmp/LOG_{exp_id}": 0o755,
        "plot": 0o775,
        "status": 0o775,
    }

    exp_folder.mkdir(
        mode=0o755,
        parents=False,
        exist_ok=True,
    )

    for directory, mode in required_dirs.items():
        path = exp_folder / directory
        path.mkdir(mode=mode, parents=True, exist_ok=True)
        path.chmod(mode)


def get_experiment_owner(expid: str) -> tuple[str | None, int, bool, bool]:
    """Get the experiment owner and ownership information.

    The owner username may be ``None`` if the owner UID is not present
    in the system user database (e.g. the user was deleted).

    NOTE: The eadmin flag is deprecated and will be removed in #944.

    :param expid: The experiment identifier.
    :return: A tuple with the owner username, owner UID, whether the
        current user is the owner, and a deprecated eadmin flag.
    """
    current_user_id = os.getuid()
    owner_uid = Path(BasicConfig.LOCAL_ROOT_DIR, expid).stat().st_uid

    is_owner = owner_uid == current_user_id

    # TODO: to be removed in #944
    is_eadmin = False
    with suppress(Exception):
        is_eadmin = current_user_id == pwd.getpwnam("eadmin").pw_uid

    try:
        owner_username = pwd.getpwuid(owner_uid).pw_name
    except (TypeError, KeyError):
        Log.warning(
            f"Current owner of experiment {expid} could not be retrieved. "
            f"The owner is no longer in the system database."
        )
        owner_username = None

    return owner_username, owner_uid, is_owner, is_eadmin


def check_ownership(expid: str) -> None:
    """Check if the user owns and if it is eadmin.

    :raise AutosubmitCritical: If the current user does not own the experiment.
    """
    owner_username, owner_uid, is_owner, _ = get_experiment_owner(expid)

    if not is_owner:
        owner = (
            f"{owner_username} (UID {owner_uid})"
            if owner_username
            else f"deleted user (UID {owner_uid})"
        )
        raise AutosubmitCritical(
            f"The current user does not own experiment {expid}; owner: {owner}"
        )


def print_job_details(job_list: "JobList") -> None:
    """Log job details.

    :param job_list: The job list object.
    """
    current_length = len(job_list.graph.nodes())
    if current_length > 1000:
        Log.warning(
            "-d option: Experiment has too many jobs to be printed in the terminal. "
            f"Maximum job quantity is 1000, your experiment has {str(current_length)} jobs."
        )
    else:
        Log.info(job_list.print_with_status())
        Log.status(job_list.print_with_status(nocolor=True))
