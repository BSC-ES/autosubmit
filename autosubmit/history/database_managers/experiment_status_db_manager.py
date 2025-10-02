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

import os
import textwrap
from pathlib import Path
from typing import Optional, Protocol, cast

from sqlalchemy import insert, select, update
from sqlalchemy.schema import CreateTable

import autosubmit.history.utils as HUtils
from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database import session
from autosubmit.database.db_common import get_connection_url
from autosubmit.database.tables import ExperimentStatusTable, ExperimentTable
from autosubmit.history.database_managers import database_models as Models
from autosubmit.history.database_managers.database_manager import DatabaseManager, DEFAULT_LOCAL_ROOT_DIR


class ExperimentStatusDbManager(DatabaseManager):
    """ Manages the actions on the status database """

    def __init__(
            self,
            expid: str,
            db_dir_path: str,
            main_db_name: str,
            local_root_dir_path: str = DEFAULT_LOCAL_ROOT_DIR
    ):
        super(ExperimentStatusDbManager, self).__init__(expid, local_root_dir_path=local_root_dir_path)
        self._as_times_file_path = os.path.join(db_dir_path, BasicConfig.AS_TIMES_DB)
        self._ecearth_file_path = os.path.join(db_dir_path, main_db_name)
        self._pkl_file_path = os.path.join(local_root_dir_path, self.expid, "pkl", f"job_list_{self.expid}.pkl")
        self._validate_status_database()

    def _validate_status_database(self):
        """ Creates experiment_status table if it does not exist """
        create_table_query = textwrap.dedent(
            '''CREATE TABLE
                IF NOT EXISTS experiment_status (
                exp_id integer PRIMARY KEY,
                name text NOT NULL,
                status text NOT NULL,
                seconds_diff integer NOT NULL,
                modified text NOT NULL
            );'''
        )
        self.execute_statement_on_dbfile(self._as_times_file_path, create_table_query)

    def set_existing_experiment_status_as_running(self, expid: str) -> None:
        """ Set the experiment_status row as running. """
        self.update_exp_status(expid, Models.RunningStatus.RUNNING)

    def create_experiment_status_as_running(self, experiment: Models.ExperimentRow) -> None:
        """ Create a new experiment_status row for the Models.Experiment item."""
        self.create_exp_status(experiment.id, experiment.name, Models.RunningStatus.RUNNING)

    def get_experiment_status_row_by_expid(self, expid: str) -> Optional[Models.ExperimentStatusRow]:
        """Get Models.ExperimentRow by expid."""
        experiment_row = self.get_experiment_row_by_expid(expid)
        return self.get_experiment_status_row_by_exp_id(experiment_row.id)

    def get_experiment_row_by_expid(self, expid: str) -> Models.ExperimentRow:
        """Get the experiment from ecearth.db by expid as Models.ExperimentRow."""
        statement = self.get_built_select_statement("experiment", "name=?")
        current_rows = self.get_from_statement_with_arguments(self._ecearth_file_path, statement, (expid,))

        if not current_rows:
            raise ValueError(f"Experiment {expid} not found in {self._ecearth_file_path}")

        return Models.ExperimentRow(*current_rows[0])

    def get_experiment_status_row_by_exp_id(self, exp_id: int) -> Optional[Models.ExperimentStatusRow]:
        """ Get Models.ExperimentStatusRow from as_times.db by exp_id (int)."""
        statement = self.get_built_select_statement("experiment_status", "exp_id=?")
        arguments = (exp_id,)
        current_rows = self.get_from_statement_with_arguments(self._as_times_file_path, statement, arguments)
        if len(current_rows) <= 0:
            return None
        return Models.ExperimentStatusRow(*current_rows[0])

    def create_exp_status(self, exp_id: int, expid: str, status: str) -> int:
        """Create experiment status."""
        statement = ''' INSERT INTO experiment_status(exp_id, name,
        status, seconds_diff, modified) VALUES(?,?,?,?,?) '''
        arguments = (exp_id, expid, status, 0, HUtils.get_current_datetime())
        return self.insert_statement_with_arguments(self._as_times_file_path, statement, arguments)

    def update_exp_status(self, expid: str, status="RUNNING") -> None:
        """
        Update status, seconds_diff, modified in experiment_status.
        """
        statement = ''' UPDATE experiment_status SET status = ?, 
        seconds_diff = ?, modified = ? WHERE name = ? '''
        arguments = (status, 0, HUtils.get_current_datetime(), expid)
        self.execute_statement_with_arguments_on_dbfile(
            self._as_times_file_path, statement, arguments)


class ExperimentStatusDatabaseManager(Protocol):

    def set_existing_experiment_status_as_running(self, expid: str) -> None: ...

    def create_experiment_status_as_running(self, experiment: Models.ExperimentRow) -> None: ...

    def get_experiment_status_row_by_expid(self, expid: str) -> Optional[Models.ExperimentStatusRow]: ...

    def get_experiment_row_by_expid(self, expid: str) -> Models.ExperimentRow: ...

    def get_experiment_status_row_by_exp_id(self, exp_id: int) -> Optional[Models.ExperimentStatusRow]: ...

    def create_exp_status(self, exp_id: int, expid: str, status: str) -> int: ...

    def update_exp_status(self, expid: str, status="RUNNING") -> None: ...


class SqlAlchemyExperimentStatusDbManager:
    """An experiment status database manager using SQLAlchemy.
    It contains the same public functions as ``ExperimentStatusDbManager``
    (SQLite only), but uses SQLAlchemy instead of calling database
    driver functions directly.
    It can be used with any engine supported by SQLAlchemy, such
    as Postgres, Mongo, MySQL, etc.
    Some operations here may raise ``NotImplemented``, as they existed in
    the ``ExperimentStatusDbManager`` but were never used in Autosubmit (i.e. we can
    delete that code -- for later).
    """

    def __init__(self) -> None:
        connection_url = get_connection_url(Path(BasicConfig.DATABASE_CONN_URL))
        self.engine = session.create_engine(connection_url=connection_url)
        with self.engine.connect() as conn:
            conn.execute(CreateTable(ExperimentStatusTable, if_not_exists=True))
            conn.commit()

    def set_existing_experiment_status_as_running(self, expid):
        self.update_exp_status(expid, Models.RunningStatus.RUNNING)

    def create_experiment_status_as_running(self, experiment):
        self.create_exp_status(experiment.id, experiment.name, Models.RunningStatus.RUNNING)

    def get_experiment_status_row_by_expid(self, expid: str) -> Optional[Models.ExperimentRow]:
        experiment_row = self.get_experiment_row_by_expid(expid)
        return self.get_experiment_status_row_by_exp_id(experiment_row.id)

    def get_experiment_row_by_expid(self, expid: str) -> Models.ExperimentRow:
        query = (
            select(ExperimentTable).
            where(ExperimentTable.c.name == expid)  # type: ignore
        )
        with self.engine.connect() as conn:
            row = conn.execute(query).first()
            if not row:
                raise ValueError("Experiment {0} not found in Postgres {1}".format(expid, expid))
        return Models.ExperimentRow(*row)

    def get_experiment_status_row_by_exp_id(self, exp_id: int) -> Optional[Models.ExperimentStatusRow]:
        query = (
            select(ExperimentStatusTable).
            where(ExperimentStatusTable.c.exp_id == exp_id)  # type: ignore
        )
        with self.engine.connect() as conn:
            row = conn.execute(query).first()
            if not row:
                return None
        return Models.ExperimentStatusRow(*row)

    def create_exp_status(self, exp_id: int, expid: str, status: str) -> int:
        query = (
            insert(ExperimentStatusTable).
            values(
                exp_id=exp_id,
                name=expid,
                status=status,
                seconds_diff=0,
                modified=HUtils.get_current_datetime()
            )
        )
        with self.engine.connect() as conn:
            result = conn.execute(query)
            # NOTE: SQLite == rowcount(), PG == rowcount. Intriguing.
            row_count = result.rowcount() if callable(result.rowcount) else result.rowcount
            conn.commit()
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
        with self.engine.connect() as conn:
            conn.execute(query)
            conn.commit()


def create_experiment_status_db_manager(db_engine: str, **options) -> ExperimentStatusDatabaseManager:
    """Creates a Postgres or SQLite database manager based on the Autosubmit configuration.

    Note that you must provide the options even if they are optional, in which case
    you must provide ``options=None``, or you will get a ``KeyError``.

    TODO: better example and/or link to DbManager.

    :param db_engine: The database engine type.
    :return: An ``ExperimentStatusDatabaseManager``.
    :raises ValueError: If the database engine type is not valid.
    :raises KeyError: If the ``options`` dictionary is missing a required parameter for an engine.
    """
    if db_engine == "postgres":
        return cast(ExperimentStatusDatabaseManager, SqlAlchemyExperimentStatusDbManager())
    elif db_engine == "sqlite":
        return cast(ExperimentStatusDatabaseManager,
                    ExperimentStatusDbManager(
                        expid=options['expid'],
                        db_dir_path=options['db_dir_path'],
                        main_db_name=options['main_db_name'],
                        local_root_dir_path=options['local_root_dir_path']))
    else:
        raise ValueError(f"Invalid database engine: {db_engine}")
