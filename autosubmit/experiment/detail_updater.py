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

"""Code to manage experiment details in databases."""

import datetime
import pwd
from pathlib import Path
from typing import Any

from sqlalchemy import Table, delete, insert, select

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.config.yamlparser import YAMLParserFactory
from autosubmit.database.db_common import get_experiment_id
from autosubmit.database.session import get_engine
from autosubmit.database.tables import TableRegistry

__all__ = [
    "LOCAL_TZ",
    "ExperimentDetails",
    "ExperimentDetailsRepository",
]

LOCAL_TZ = datetime.datetime.now(datetime.timezone.utc).astimezone().tzinfo


class ExperimentDetailsRepository:
    """Manage the experiment details in the general database."""

    def __init__(self) -> None:
        table_registry = TableRegistry(None)
        self.table: Table = table_registry.get("details")
        self.engine = get_engine(db_path=BasicConfig.DB_PATH)

    def get_details(self, exp_id: int) -> dict[str, Any] | None:
        """Get the details of an experiment by its id.

        :param exp_id: The id of the experiment.
        :return: A dictionary with the details, or ``None`` if not found.
        """
        with self.engine.connect() as conn:
            result = conn.execute(
                select(self.table).where(self.table.c.exp_id == exp_id)
            ).one_or_none()

        if result is None:
            return None
        return {
            "exp_id": result.exp_id,
            "user": result.user,
            "created": result.created,
            "model": result.model,
            "branch": result.branch,
            "hpc": result.hpc,
        }

    def upsert_details(
        self, exp_id: int, user: str, created: str, model: str, branch: str, hpc: str
    ) -> None:
        """Upsert the details of an experiment.

        :param exp_id: The id of the experiment.
        :param user: The user that created the experiment.
        :param created: The creation date of the experiment.
        :param model: The model of the experiment.
        :param branch: The branch of the experiment.
        :param hpc: The HPC of the experiment.
        """
        with self.engine.connect() as conn, conn.begin():
            conn.execute(delete(self.table).where(self.table.c.exp_id == exp_id))
            conn.execute(
                insert(self.table).values(
                    exp_id=exp_id,
                    user=user,
                    created=created,
                    model=model,
                    branch=branch,
                    hpc=hpc,
                )
            )

    def delete_details(self, exp_id: int) -> None:
        """Delete the details of an experiment by its id.

        :param exp_id: The id of the experiment.
        """
        with self.engine.connect() as conn, conn.begin():
            conn.execute(delete(self.table).where(self.table.c.exp_id == exp_id))


class ExperimentDetails:
    """Manage the experiment details."""

    def __init__(self, expid: str, init_reload: bool = True) -> None:
        self.expid = expid
        self._details_repo = ExperimentDetailsRepository()
        if init_reload:
            self.reload()

    def reload(self) -> None:
        """Reload the necessary components to get the experiment details."""
        # Build path stat
        self.exp_path = Path(BasicConfig.LOCAL_ROOT_DIR).joinpath(self.expid)
        self.exp_dir_stat = self.exp_path.stat()

        # Get experiment id
        self.exp_id: int = get_experiment_id(self.expid)

        # Get experiment config
        self.as_conf = AutosubmitConfig(self.expid, BasicConfig, YAMLParserFactory())
        self.as_conf.reload()

    def save_update_details(self) -> None:
        """Save the details of the experiment to the database. Upserts the details."""
        self._details_repo.upsert_details(
            self.exp_id, self.user, self.created, self.model, self.branch, self.hpc
        )

    def get_details(self) -> dict[str, Any] | None:
        """Retrieve the last stored snapshot of the experiment's details from the database."""
        exp_id = getattr(self, "exp_id", None)
        if exp_id is None:
            exp_id = get_experiment_id(self.expid)
        return self._details_repo.get_details(exp_id)

    def delete_details(self) -> None:
        """Delete the details of the experiment from the database."""
        self._details_repo.delete_details(self.exp_id)

    @property
    def user(self) -> str:
        """Get the user that created the experiment, from the experiment directory stat."""
        return pwd.getpwuid(self.exp_dir_stat.st_uid).pw_name

    @property
    def created(self) -> str:
        """Get the creation date of the experiment, from the experiment directory stat."""
        return datetime.datetime.fromtimestamp(
            int(self.exp_dir_stat.st_ctime), tz=LOCAL_TZ
        ).isoformat()

    @property
    def model(self) -> str:
        """Get the model of the experiment, from the Autosubmit configuration."""
        project_type = self.as_conf.get_project_type()
        if project_type == "git":
            return self.as_conf.get_git_project_origin()
        else:
            return "NA"

    @property
    def branch(self) -> str:
        """Get the branch of the experiment, from the Autosubmit configuration."""
        project_type = self.as_conf.get_project_type()
        if project_type == "git":
            return self.as_conf.get_git_project_branch()
        else:
            return "NA"

    @property
    def hpc(self) -> str:
        """Get the HPC of the experiment, from the Autosubmit configuration."""
        try:
            return self.as_conf.get_platform()
        except Exception:
            return "NA"
