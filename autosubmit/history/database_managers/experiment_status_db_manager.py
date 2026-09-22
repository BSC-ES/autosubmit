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

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.schema import CreateTable

import autosubmit.history.utils as HUtils
from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database import session
from autosubmit.database.tables import ExperimentStatusTable, ExperimentTable
from autosubmit.history.database_managers import database_models as Models

# TODO(#3114): the as_times database (experiment_status) is not versioned yet.
#             When it is, it should use the per-target schema_migrations helper
#             from autosubmit.database.migrations.


class SqlAlchemyExperimentStatusDbManager:
    """An experiment status database manager using SQLAlchemy.

    It can be used with any engine supported by SQLAlchemy, such as PostgreSQL
    and SQLite.
    """

    def __init__(self) -> None:
        # ``experiment_status`` lives in the as_times database, while ``experiment``
        # lives in the general database. On PostgreSQL both paths resolve to the
        # same engine.
        self.status_engine = session.get_engine(db_path=BasicConfig.AS_TIMES_DB_PATH)
        self.general_engine = session.get_engine(db_path=BasicConfig.DB_PATH)
        with self.status_engine.begin() as conn:
            conn.execute(CreateTable(ExperimentStatusTable, if_not_exists=True))

    def set_existing_experiment_status_as_running(self, expid):
        self.update_exp_status(expid, Models.RunningStatus.RUNNING)

    def create_experiment_status_as_running(self, experiment):
        self.create_exp_status(experiment.id, experiment.name, Models.RunningStatus.RUNNING)

    def get_experiment_status_row_by_expid(self, expid: str) -> Models.ExperimentStatusRow | None:
        experiment_row = self.get_experiment_row_by_expid(expid)
        return self.get_experiment_status_row_by_exp_id(experiment_row.id)

    def get_experiment_row_by_expid(self, expid: str) -> Models.ExperimentRow:
        query = (
            select(ExperimentTable).
            where(ExperimentTable.c.name == expid)  # type: ignore
        )
        with self.general_engine.connect() as conn:
            row = conn.execute(query).first()
            if not row:
                raise ValueError(f"Experiment {expid} not found in the database")
        return Models.ExperimentRow(*row)

    def get_experiment_status_row_by_exp_id(self, exp_id: int) -> Models.ExperimentStatusRow | None:
        query = (
            select(ExperimentStatusTable).
            where(ExperimentStatusTable.c.exp_id == exp_id)  # type: ignore
        )
        with self.status_engine.connect() as conn:
            row = conn.execute(query).first()
            if not row:
                return None
        return Models.ExperimentStatusRow(*row)

    def create_exp_status(self, exp_id: int, expid: str, status: str) -> int:
        """Upsert a new experiment status row in the database. If the row already exists, it will be updated."""
        if BasicConfig.DATABASE_BACKEND == "postgres":
            _insert_fn = pg_insert
        else:
            _insert_fn = sqlite_insert
        query = (
            _insert_fn(ExperimentStatusTable)
            .values(
                exp_id=exp_id,
                name=expid,
                status=status,
                seconds_diff=0,
                modified=HUtils.get_current_datetime(),
            )
            .on_conflict_do_update(
                index_elements=[ExperimentStatusTable.c.exp_id],  # type: ignore
                set_={
                    "name": expid,
                    "status": status,
                    "seconds_diff": 0,
                    "modified": HUtils.get_current_datetime(),
                },
            )
        )
        with self.status_engine.connect() as conn:
            with conn.begin():
                result = conn.execute(query)
                # NOTE: SQLite == rowcount(), PG == rowcount. Intriguing.
                row_count = result.rowcount() if callable(result.rowcount) else result.rowcount
        return row_count

    def update_exp_status(self, expid: str, status="RUNNING") -> None:
        query = (
            update(ExperimentStatusTable).
            where(ExperimentStatusTable.c.name == expid).  # type: ignore
            values(
                status=status,
                seconds_diff=0,
                modified=HUtils.get_current_datetime()
            )
        )
        with self.status_engine.connect() as conn, conn.begin():
            conn.execute(query)

