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
# along with Autosubmit. If not, see <http://www.gnu.org/licenses/>.

"""Code to list and describe Autosubmit experiments.

The code is intended to be used mainly by the ``autosubmit describe`` command.

If you use code from this module elsewhere, please, consider moving the
common code to another location -- if appropriately.
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from pwd import getpwnam

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.database.db_common import (
    get_experiment_description,
    get_experiment_expids,
)
from autosubmit.experiment.detail_updater import ExperimentDetails
from autosubmit.experiment.utils import get_experiment_owner
from autosubmit.log.log import AutosubmitCritical, Log
from autosubmit.platforms.paramiko_submitter import ParamikoSubmitter

__all__ = [
    "ExperimentDescription",
    "describe_experiment",
    "get_experiment_ids",
    "log_experiment_description",
]


@dataclass(frozen=True)
class ExperimentDescription:
    """Description information for an Autosubmit experiment."""

    user: str
    created: datetime
    model: str
    branch: str
    hpc: str
    description: str


def get_experiment_ids(
    experiment_ids: str,
    user: str,
) -> tuple[list[str], list[str]]:
    """Resolve experiment IDs against the database and filter by owner.

    Comma- and whitespace-separated experiment IDs are supported.
    An empty value selects all experiments. ``*`` selects all experiments
    regardless of owner.

    :param experiment_ids: Experiment IDs to resolve.
    :param user: Owner username used to filter the experiments, or ``*``
        to include experiments owned by all users.
    :return: A tuple containing matching experiment IDs and requested IDs
        that were not found in the database.
    """
    if "*" in experiment_ids or not experiment_ids.strip():
        requested = sorted(get_experiment_expids())
        not_found: list[str] = []
    else:
        requested_ids = [
            expid.lower() for expid in experiment_ids.replace(",", " ").split()
        ]

        found = get_experiment_expids(expids=requested_ids)
        not_found = [expid for expid in requested_ids if expid not in found]
        requested = [expid for expid in requested_ids if expid in found]

    if user == "*":
        return requested, not_found

    try:
        requested_user_uid = getpwnam(user).pw_uid
    except KeyError:
        return [], not_found

    matching = []

    for expid in requested:
        _, owner_uid, _, _ = get_experiment_owner(expid)

        if owner_uid == requested_user_uid:
            matching.append(expid)

    return matching, not_found


def describe_experiment(experiment_id: str) -> ExperimentDescription:
    """Retrieve the details of a single experiment.

    Configuration files are used as the preferred source of information.
    If they cannot be read, the last experiment snapshot stored in the
    database is used instead.

    :param experiment_id: The experiment identifier.
    :return: The experiment description.
    :raises AutosubmitCritical: If the experiment configuration cannot be
        read and no database snapshot is available.
    """
    try:
        as_conf = AutosubmitConfig(experiment_id)
        as_conf.check_conf_files(False, no_log=True)

        conf_path = Path(as_conf.conf_folder_yaml)
        stat = conf_path.stat()

        owner, owner_uid, _, _ = get_experiment_owner(experiment_id)
        user = owner if owner is not None else str(owner_uid)

        created = datetime.fromtimestamp(stat.st_mtime)

        svn_url = as_conf.get_svn_project_url()
        if svn_url:
            model = branch = svn_url
        else:
            model = as_conf.get_git_project_origin() or "Not Found"
            branch = as_conf.get_git_project_branch() or "Not Found"

        submitter = ParamikoSubmitter(as_conf=as_conf)
        if not submitter.platforms:
            raise AutosubmitCritical(
                f"No platforms available for experiment {experiment_id}",
                7012,
            )

        hpc = as_conf.get_platform()

    except AutosubmitCritical:
        raise
    except Exception as e:
        Log.info(f"Could not read configuration files for {experiment_id}: {e}")
        Log.info("Trying the last stored experiment snapshot...")

        snapshot = ExperimentDetails(
            experiment_id,
            init_reload=False,
        ).get_details()

        if not snapshot:
            raise AutosubmitCritical(
                f"Could not retrieve details for experiment {experiment_id}",
                7012,
                str(e),
            ) from e

        user = snapshot["user"]
        created = snapshot["created"]
        model = snapshot["model"]
        branch = snapshot["branch"]
        hpc = snapshot["hpc"]

    experiment_description = get_experiment_description(experiment_id)
    description = experiment_description[0][0] if experiment_description else ""

    return ExperimentDescription(
        user=user,
        created=created,
        model=model,
        branch=branch,
        hpc=hpc,
        description=description,
    )


def log_experiment_description(
    experiment_id: str,
    experiment: ExperimentDescription,
) -> None:
    """Print an experiment description in a readable format.

    :param experiment_id: The experiment identifier.
    :param experiment: The experiment description to print.
    """
    Log.info("")
    Log.info(f"Experiment {experiment_id}")
    Log.info(f"  Owner:       {experiment.user}")
    Log.info(f"  Location:    {Path(BasicConfig.LOCAL_ROOT_DIR) / experiment_id}")
    Log.info(f"  Created:     {experiment.created}")
    Log.info(f"  Model:       {experiment.model}")
    Log.info(f"  Branch:      {experiment.branch}")
    Log.info(f"  HPC:         {experiment.hpc}")
    Log.info(f"  Description: {experiment.description}")
