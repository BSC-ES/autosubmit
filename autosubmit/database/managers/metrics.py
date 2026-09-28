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

"""Repository for the per-experiment ``user_metrics`` database."""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import delete, insert
from sqlalchemy.schema import CreateSchema, CreateTable

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database import session
from autosubmit.database.models.tables import TableRegistry


class UserMetricRepository:
    """Store user metrics in the experiment's metrics database."""

    def __init__(self, expid: str):
        self.expid = expid

        exp_path = Path(BasicConfig.LOCAL_ROOT_DIR).joinpath(expid)
        tmp_path = Path(exp_path).joinpath(BasicConfig.LOCAL_TMP_DIR)
        db_path = tmp_path.joinpath(f"metrics_{expid}.db")

        if BasicConfig.DATABASE_BACKEND == "postgres":
            # Postgres backend
            self.schema = self.expid
        else:
            # SQLite backend
            self.schema = None

        self.table_registry = TableRegistry(schema=self.schema)
        self.table = self.table_registry.get("user_metrics")
        self.engine = session.get_engine(db_path=db_path)

        with self.engine.connect() as conn, conn.begin():
            if self.schema:
                conn.execute(CreateSchema(self.schema, if_not_exists=True))
            conn.execute(CreateTable(self.table, if_not_exists=True))

    def store_metric(self, run_id: int, job_name: str, metric_name: str, metric_value: Any):
        """Store the metric value in the database, overwriting it if it already exists.

        :param run_id: The experiment run id.
        :param job_name: The job name.
        :param metric_name: The metric name.
        :param metric_value: The metric value.
        """
        with self.engine.connect() as conn, conn.begin():
            # Delete the existing metric
            conn.execute(
                delete(self.table).where(
                    self.table.c.run_id == run_id,
                    self.table.c.job_name == job_name,
                    self.table.c.metric_name == metric_name,
                )
            )

            # Insert the new metric
            conn.execute(
                insert(self.table).values(
                    run_id=run_id,
                    job_name=job_name,
                    metric_name=metric_name,
                    metric_value=str(metric_value),
                    modified=datetime.now(tz=timezone.utc).isoformat(
                        timespec="seconds"
                    ),
                )
            )
