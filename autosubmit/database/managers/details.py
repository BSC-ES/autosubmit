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

"""Repository for the experiment ``details`` table."""

from typing import Any

from sqlalchemy import Table, delete, insert, select

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database.models.tables import TableRegistry
from autosubmit.database.session import get_engine


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
