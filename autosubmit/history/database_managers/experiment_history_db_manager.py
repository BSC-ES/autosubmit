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
from typing import Any, Optional, Protocol, cast

from sqlalchemy import and_, func, inspect, desc, insert, select, update
from sqlalchemy.schema import CreateTable, CreateSchema

import autosubmit.history.utils as HUtils
from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database import session
from autosubmit.database.db_common import get_connection_url
from autosubmit.database.tables import (
    ExperimentRunTable,
    JobDataTable,
    get_table_with_schema,
)
from autosubmit.history.data_classes.experiment_run import ExperimentRun
from autosubmit.history.data_classes.job_data import JobData
from autosubmit.history.database_managers import database_models as Models
from autosubmit.history.database_managers.database_manager import (
    DatabaseManager,
    DEFAULT_JOBDATA_DIR,
)

CURRENT_DB_VERSION = 19  # Update this if you change the database schema
DB_EXPERIMENT_HEADER_SCHEMA_CHANGES = 14
DB_VERSION_SCHEMA_CHANGES = 12
DEFAULT_DB_VERSION = 10
DEFAULT_MAX_COUNTER = 0


class ExperimentHistoryDbManager(DatabaseManager):
    """ Manages actions directly on the database.
    """

    def __init__(self, expid, jobdata_dir_path=DEFAULT_JOBDATA_DIR):
        """ Requires expid and jobdata_dir_path. """
        super(ExperimentHistoryDbManager, self).__init__(expid, jobdata_dir_path=jobdata_dir_path)
        self._set_schema_changes()
        self._set_table_queries()
        self.historicaldb_file_path = os.path.join(self.JOBDATA_DIR, "job_data_{0}.db".format(self.expid))  # type : str

    def initialize(self):
        if self.my_database_exists():
            if not self.is_current_version():
                self.update_historical_database()
        else:
            self.create_historical_database()

    def my_database_exists(self):
        if os.path.exists(self.historicaldb_file_path):
            # Check if the 2 tables exists
            statement = "SELECT name FROM sqlite_master WHERE type='table' AND (name='job_data' OR name='experiment_run');"
            table_result = self.get_from_statement(self.historicaldb_file_path, statement)
            return len(table_result) == 2
        else:
            return False

    def is_header_ready_db_version(self):
        if self.my_database_exists():
            return self._get_pragma_version() >= DB_EXPERIMENT_HEADER_SCHEMA_CHANGES
        return False

    def is_current_version(self):
        if self.my_database_exists():
            return self._get_pragma_version() == CURRENT_DB_VERSION
        return False

    def _set_table_queries(self):
        """ Sets basic table queries. """
        self.create_table_header_query = textwrap.dedent(
            '''CREATE TABLE 
            IF NOT EXISTS experiment_run (
            run_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            created TEXT NOT NULL,
            modified TEXT NOT NULL,
            start INTEGER NOT NULL,
            finish INTEGER,
            chunk_unit TEXT NOT NULL,
            chunk_size INTEGER NOT NULL,
            completed INTEGER NOT NULL,
            total INTEGER NOT NULL,
            failed INTEGER NOT NULL,
            queuing INTEGER NOT NULL,
            running INTEGER NOT NULL,
            submitted INTEGER NOT NULL,
            suspended INTEGER NOT NULL DEFAULT 0,
            metadata TEXT
            );
            ''')
        self.create_table_query = textwrap.dedent(
            '''CREATE TABLE
            IF NOT EXISTS job_data (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            counter INTEGER NOT NULL,
            job_name TEXT NOT NULL,
            created TEXT NOT NULL,
            modified TEXT NOT NULL,
            submit INTEGER NOT NULL,
            start INTEGER NOT NULL,
            finish INTEGER NOT NULL,
            status TEXT NOT NULL,
            rowtype INTEGER NOT NULL,
            ncpus INTEGER NOT NULL,
            wallclock TEXT NOT NULL,
            qos TEXT NOT NULL,
            energy INTEGER NOT NULL,
            date TEXT NOT NULL,
            section TEXT NOT NULL,
            member TEXT NOT NULL,
            chunk INTEGER NOT NULL,
            last INTEGER NOT NULL,
            platform TEXT NOT NULL,
            job_id INTEGER NOT NULL,
            extra_data TEXT NOT NULL,
            nnodes INTEGER NOT NULL DEFAULT 0,
            run_id INTEGER,
            MaxRSS REAL NOT NULL DEFAULT 0.0,
            AveRSS REAL NOT NULL DEFAULT 0.0,
            out TEXT NOT NULL,
            err TEXT NOT NULL,
            rowstatus INTEGER NOT NULL DEFAULT 0,
            children TEXT,
            platform_output TEXT,
            workflow_commit TEXT,
            UNIQUE(counter,job_name)
            );
            ''')
        self.create_index_query = textwrap.dedent(''' 
      CREATE INDEX IF NOT EXISTS ID_JOB_NAME ON job_data(job_name);
      ''')

    def _set_schema_changes(self):
        # type : () -> None
        """ Creates the list of schema changes"""
        self.version_schema_changes = [
            "ALTER TABLE job_data ADD COLUMN nnodes INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE job_data ADD COLUMN run_id INTEGER"
        ]
        # Version 15
        self.version_schema_changes.extend([
            "ALTER TABLE job_data ADD COLUMN MaxRSS REAL NOT NULL DEFAULT 0.0",
            "ALTER TABLE job_data ADD COLUMN AveRSS REAL NOT NULL DEFAULT 0.0",
            "ALTER TABLE job_data ADD COLUMN out TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE job_data ADD COLUMN err TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE job_data ADD COLUMN rowstatus INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE experiment_run ADD COLUMN suspended INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE experiment_run ADD COLUMN metadata TEXT"
        ])
        # Version 16
        self.version_schema_changes.extend([
            "ALTER TABLE experiment_run ADD COLUMN modified TEXT"
        ])
        # Version 17
        self.version_schema_changes.extend([
            "ALTER TABLE job_data ADD COLUMN children TEXT",
            "ALTER TABLE job_data ADD COLUMN platform_output TEXT"
        ])
        # Version 18
        self.version_schema_changes.extend([
            "ALTER TABLE job_data ADD COLUMN workflow_commit TEXT"
        ])

    def create_historical_database(self):
        """ Creates the historical database with the latest changes. """
        self.execute_statement_on_dbfile(self.historicaldb_file_path, self.create_table_header_query)
        self.execute_statement_on_dbfile(self.historicaldb_file_path, self.create_table_query)
        self.execute_statement_on_dbfile(self.historicaldb_file_path, self.create_index_query)
        self._set_historical_pragma_version(CURRENT_DB_VERSION)

    def update_historical_database(self):
        """ Updates the historical database with the latest changes IF necessary. """
        self.execute_many_statements_on_dbfile(self.historicaldb_file_path, self.version_schema_changes)
        self.execute_statement_on_dbfile(self.historicaldb_file_path, self.create_index_query)
        self.execute_statement_on_dbfile(self.historicaldb_file_path, self.create_table_header_query)
        self._set_historical_pragma_version(CURRENT_DB_VERSION)

    def get_experiment_run_dc_with_max_id(self):
        """ Get Current (latest) ExperimentRun data class. """
        return ExperimentRun.from_model(self._get_experiment_run_with_max_id())

    def register_experiment_run_dc(self, experiment_run_dc):
        self._insert_experiment_run(experiment_run_dc)
        return ExperimentRun.from_model(self._get_experiment_run_with_max_id())

    def update_experiment_run_dc_by_id(self, experiment_run_dc):
        """ Requires ExperimentRun data class. """
        self._update_experiment_run(experiment_run_dc)
        return ExperimentRun.from_model(self._get_experiment_run_with_max_id())

    def _get_experiment_run_with_max_id(self):
        """ Get Models.ExperimentRunRow for the maximum id run. """
        statement = self.get_built_select_statement("experiment_run", "run_id > 0 ORDER BY run_id DESC LIMIT 0, 1")
        max_experiment_run = self.get_from_statement(self.historicaldb_file_path, statement)
        if len(max_experiment_run) == 0:
            raise Exception("No Experiment Runs registered.")
        return Models.ExperimentRunRow(*max_experiment_run[0])

    def is_there_a_last_experiment_run(self):
        statement = self.get_built_select_statement("experiment_run", "run_id > 0 ORDER BY run_id DESC LIMIT 0, 1")
        max_experiment_run = self.get_from_statement(self.historicaldb_file_path, statement)
        if len(max_experiment_run) > 0:
            return True
        return False

    def get_job_data_all(self):
        """Used for tests only.\

        Gets all content from job_data as list of Models.JobDataRow from database.
        """
        statement = self.get_built_select_statement("job_data")
        job_data_rows = self.get_from_statement(self.historicaldb_file_path, statement)
        return [Models.JobDataRow(*row) for row in job_data_rows]

    def register_submitted_job_data_dc(self, job_data_dc):
        """ Sets previous register to last=0 and inserts the new job_data_dc data class."""
        self._set_current_job_data_rows_last_to_zero_by_job_name(job_data_dc.job_name)
        self._insert_job_data(job_data_dc)
        return self.get_job_data_dc_unique_latest_by_job_name(job_data_dc.job_name)

    def _set_current_job_data_rows_last_to_zero_by_job_name(self, job_name):
        """ Sets the column last = 0 for all job_rows by job_name and last = 1. """
        job_data_row_last = self._get_job_data_last_by_name(job_name)
        job_data_dc_list = [JobData.from_model(row) for row in job_data_row_last]
        for job_data_dc in job_data_dc_list:
            job_data_dc.last = 0
            self._update_job_data_by_id(job_data_dc)

    def update_job_data_dc_by_job_id_name(self, job_data_dc: Any) -> Any:
        """
        Update JobData data class. Returns the latest row from job_data by job_name.

        :param job_data_dc: The JobData data class instance containing job_id and job_name.
        :type job_data_dc: JobData
        :return: The latest row from job_data corresponding to the given job_id and job_name.
        :rtype: Any
        """
        self._update_job_data_by_id(job_data_dc)
        # Return the latest row from job_data by job_id and job_name
        return self.get_job_data_by_job_id_name(job_data_dc.job_id, job_data_dc.job_name)

    def update_list_job_data_dc_by_each_id(self, job_data_dcs):
        """ Return length of updated list. """
        for job_data_dc in job_data_dcs:
            self._update_job_data_by_id(job_data_dc)
        return len(job_data_dcs)

    def get_job_data_dc_unique_latest_by_job_name(self, job_name):
        """ Returns JobData data class for the latest job_data_row with last=1 by job_name. """
        job_data_row_last = self._get_job_data_last_by_name(job_name)
        if len(job_data_row_last) > 0:
            return JobData.from_model(job_data_row_last[0])
        return None

    def _get_job_data_last_by_name(self, job_name):
        """ Get List of Models.JobDataRow for job_name and last=1 """
        statement = self.get_built_select_statement("job_data", "last=1 and job_name=? ORDER BY counter DESC")
        arguments = (job_name,)
        job_data_rows_last = self.get_from_statement_with_arguments(self.historicaldb_file_path, statement, arguments)
        if not job_data_rows_last:  # if previous job didn't finished but a new create has been made
            statement = self.get_built_select_statement("job_data", "last=0 and job_name=? ORDER BY counter DESC")
            job_data_rows_last = self.get_from_statement_with_arguments(self.historicaldb_file_path, statement,
                                                                        arguments)
        return [Models.JobDataRow(*row) for row in job_data_rows_last]

    def get_job_data_dcs_last_by_wrapper_code(self, wrapper_code):
        if wrapper_code and wrapper_code > 2:
            return [JobData.from_model(row) for row in self._get_job_data_last_by_wrapper_code(wrapper_code)]
        else:
            return []

    def _get_job_data_last_by_wrapper_code(self, wrapper_code):
        """ Get List of Models.JobDataRow for last=1 and rowtype=wrapper_code """
        statement = self.get_built_select_statement("job_data", "rowtype = ? and last=1 ORDER BY id")
        arguments = (wrapper_code,)
        job_data_rows = self.get_from_statement_with_arguments(self.historicaldb_file_path, statement, arguments)
        return [Models.JobDataRow(*row) for row in job_data_rows]

    def get_all_last_job_data_dcs(self):
        """ Gets JobData data classes in job_data for last=1. """
        job_data_rows = self._get_all_last_job_data_rows()
        return [JobData.from_model(row) for row in job_data_rows]

    def _get_all_last_job_data_rows(self):
        """ Get List of Models.JobDataRow for last=1. """
        statement = self.get_built_select_statement("job_data", "last=1")
        job_data_rows = self.get_from_statement(self.historicaldb_file_path, statement)
        return [Models.JobDataRow(*row) for row in job_data_rows]

    def _insert_job_data(self, job_data):
        # type : (JobData) -> int
        """ Insert data class JobData into job_data table. """
        statement = ''' INSERT INTO job_data(counter, job_name, created, modified, 
                submit, start, finish, status, rowtype, ncpus, 
                wallclock, qos, energy, date, section, member, chunk, last, 
                platform, job_id, extra_data, nnodes, run_id, MaxRSS, AveRSS, 
                out, err, rowstatus, children, platform_output, workflow_commit) 
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
        arguments = (job_data.counter, job_data.job_name, HUtils.get_current_datetime(), HUtils.get_current_datetime(),
                     job_data.submit, job_data.start, job_data.finish, job_data.status, job_data.rowtype,
                     job_data.ncpus,
                     job_data.wallclock, job_data.qos, job_data.energy, job_data.date, job_data.section,
                     job_data.member, job_data.chunk, job_data.last,
                     job_data.platform, job_data.job_id, job_data.extra_data, job_data.nnodes, job_data.run_id,
                     job_data.MaxRSS, job_data.AveRSS,
                     job_data.out, job_data.err, job_data.rowstatus, job_data.children, job_data.platform_output,
                     job_data.workflow_commit)
        return self.insert_statement_with_arguments(self.historicaldb_file_path, statement, arguments)

    def _insert_experiment_run(self, experiment_run):
        """ Insert data class ExperimentRun into database """
        statement = ''' INSERT INTO experiment_run(created, modified, start, finish, 
                chunk_unit, chunk_size, completed, total, 
                failed, queuing, running, 
                submitted, suspended, metadata) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
        arguments = (
        HUtils.get_current_datetime(), HUtils.get_current_datetime(), experiment_run.start, experiment_run.finish,
        experiment_run.chunk_unit, experiment_run.chunk_size, experiment_run.completed, experiment_run.total,
        experiment_run.failed, experiment_run.queuing, experiment_run.running,
        experiment_run.submitted, experiment_run.suspended, experiment_run.metadata)
        return self.insert_statement_with_arguments(self.historicaldb_file_path, statement, arguments)

    def update_many_job_data_change_status(self, changes):
        # type : (List[Tuple]) -> None
        """
        Update many job_data rows in bulk. Requires a changes list of argument tuples.
        Only updates finish, modified, status, and rowstatus by id.
        """
        statement = ''' UPDATE job_data SET modified=?, status=?, rowstatus=?  WHERE id=? '''
        self.execute_many_statement_with_arguments_on_dbfile(self.historicaldb_file_path, statement, changes)

    def _update_job_data_by_id(self, job_data_dc: Any) -> None:
        """
        Update job_data table with data class JobData.
        Update last, submit, start, finish, modified, job_id, status, energy, extra_data, nnodes, ncpus, rowstatus, out, err by id.

        :param job_data_dc: The JobData data class instance containing job data to be updated.
        :type job_data_dc: JobData
        """
        statement = ''' UPDATE job_data SET last=?, submit=?, start=?, finish=?, modified=?, 
                    job_id=?, status=?, energy=?, extra_data=?, 
                    nnodes=?, ncpus=?, rowstatus=?, out=?, err=?, 
                    children=?, platform_output=?, id=?, workflow_commit=? WHERE id=?'''
        # noinspection PyProtectedMember
        arguments = (
            job_data_dc.last, job_data_dc.submit, job_data_dc.start, job_data_dc.finish, HUtils.get_current_datetime(),
            job_data_dc.job_id, job_data_dc.status, job_data_dc.energy, job_data_dc.extra_data,
            job_data_dc.nnodes, job_data_dc.ncpus, job_data_dc.rowstatus, job_data_dc.out, job_data_dc.err,
            job_data_dc.children, job_data_dc.platform_output, job_data_dc._id, job_data_dc.workflow_commit, job_data_dc._id
            )
        self.execute_statement_with_arguments_on_dbfile(self.historicaldb_file_path, statement, arguments)

    def _update_experiment_run(self, experiment_run_dc):
        """
        Update experiment_run table with data class ExperimentRun.
        Updates by run_id (finish, chunk_unit, chunk_size, completed, total, failed, queuing, running, submitted, suspended)
        """
        statement = ''' UPDATE experiment_run SET finish=?, chunk_unit=?, chunk_size=?, completed=?, total=?, 
                failed=?, queuing=?, running=?, submitted=?, 
                suspended=?, modified=? WHERE run_id=? '''
        arguments = (experiment_run_dc.finish, experiment_run_dc.chunk_unit, experiment_run_dc.chunk_size,
                     experiment_run_dc.completed, experiment_run_dc.total,
                     experiment_run_dc.failed, experiment_run_dc.queuing, experiment_run_dc.running,
                     experiment_run_dc.submitted,
                     experiment_run_dc.suspended, HUtils.get_current_datetime(), experiment_run_dc.run_id)
        self.execute_statement_with_arguments_on_dbfile(self.historicaldb_file_path, statement, arguments)

    def get_job_data_by_job_id_name(self, job_id: int, job_name: str) -> JobData:
        """
        Get the latest JobData for a given job_id and job_name.

        :param job_id: The job ID.
        :type job_id: int
        :param job_name: The job name.
        :type job_name: str
        :return: The latest JobData instance.
        :rtype: JobData
        """
        statement = self.get_built_select_statement("job_data", "job_id=? AND job_name=? ORDER BY counter")
        arguments = (int(job_id), str(job_name),)
        job_data_rows = self.get_from_statement_with_arguments(self.historicaldb_file_path, statement, arguments)
        models = [Models.JobDataRow(*row) for row in job_data_rows][-1]
        return JobData.from_model(models)

    def get_job_data_max_counter(self, job_name: str = None) -> int:
        """
        Get the maximum counter value from the `job_data` table. If a `job_name` is provided,
        the query will filter by that specific job name.

        :param job_name: The name of the job to filter by (optional).
        :type job_name: str, optional
        :return: The maximum counter value, or the default value if no rows are found.
        :rtype: int
        """
        if job_name:
            statement = "SELECT MAX(counter) as maxcounter FROM job_data WHERE job_name = ?"
            arguments = (job_name,)
            counter_result: list[tuple[Optional[int]]] = self.get_from_statement_with_arguments(self.historicaldb_file_path, statement, arguments)
        else:
            statement = "SELECT MAX(counter) as maxcounter FROM job_data"
            counter_result: list[tuple[Optional[int]]] = self.get_from_statement(self.historicaldb_file_path, statement)

        if not counter_result[0][0]:
            return DEFAULT_MAX_COUNTER
        else:
            max_counter = Models.MaxCounterRow(*counter_result[0]).maxcounter
            return max_counter if max_counter else DEFAULT_MAX_COUNTER

    def _set_historical_pragma_version(self, version=10):
        """ Sets the pragma version. """
        statement = "pragma user_version={v:d};".format(v=version)
        self.execute_statement_on_dbfile(self.historicaldb_file_path, statement)

    def _get_pragma_version(self):
        """ Gets current pragma version as int. """
        statement = "pragma user_version;"
        pragma_result = self.get_from_statement(self.historicaldb_file_path, statement)
        # First row, first column -- single value.
        pragma_value = pragma_result[0][0]
        if pragma_value <= 0:
            raise Exception(
                "Error while getting the pragma version. This might be a signal of a deeper problem. Review previous errors.")
        return Models.PragmaVersion(pragma_value).version


class ExperimentHistoryDatabaseManager(Protocol):
    def initialize(self): ...

    def my_database_exists(self): ...

    def is_header_ready_db_version(self): ...

    def is_current_version(self): ...

    def create_historical_database(self): ...

    def update_historical_database(self): ...

    def get_experiment_run_dc_with_max_id(self) -> ExperimentRun: ...

    def register_experiment_run_dc(self, experiment_run_dc): ...

    def update_experiment_run_dc_by_id(self, experiment_run_dc): ...

    def is_there_a_last_experiment_run(self): ...

    def get_job_data_all(self): ...

    def register_submitted_job_data_dc(self, job_data_dc): ...

    def update_job_data_dc_by_job_id_name(self, job_data_dc: Any) -> Any: ...

    def update_list_job_data_dc_by_each_id(self, job_data_dcs): ...

    def get_job_data_dc_unique_latest_by_job_name(self, job_name): ...

    def get_job_data_dcs_last_by_wrapper_code(self, wrapper_code): ...

    def get_all_last_job_data_dcs(self): ...

    def update_many_job_data_change_status(self, changes): ...

    def get_job_data_by_job_id_name(self, job_id: int, job_name: str): ...

    def get_job_data_max_counter(self, job_name: str = None) -> int: ...


class SqlAlchemyExperimentHistoryDbManager:
    """A SQLAlchemy experiment history database manager.
    Its interface was designed based on the SQLite database manager,
    with the following differences:
    - We do not have the DB migration system that they used, as that
      used SQLite pragmas, which are not portable across DB engines
      (i.e. no ``_set_schema_changes()`` nor ``_set_table_queries()``).
    """

    def __init__(self, schema: Optional[str]):
        connection_url = get_connection_url(Path(BasicConfig.DATABASE_CONN_URL))
        self.engine = session.create_engine(connection_url=connection_url)
        self.schema = schema

    def initialize(self):
        # There is no update database in SQLAlchemy (yet), so we just create it.
        self.create_historical_database()

    def my_database_exists(self):
        """Return ``True`` if the schema and tables exist in the database. ``False`` otherwise."""
        connection_url = get_connection_url(Path(BasicConfig.DATABASE_CONN_URL))
        engine = session.create_engine(connection_url=connection_url)
        inspector = inspect(engine)
        return (
                (self.schema in inspector.get_schema_names())
                and inspector.has_table(ExperimentRunTable.name, schema=self.schema)
                and inspector.has_table(JobDataTable.name, schema=self.schema)
        )

    def is_header_ready_db_version(self):
        raise NotImplementedError("This feature has not been implemented yet with SQLAlchemy / Alembic.")

    def is_current_version(self):
        raise NotImplementedError("This feature has not been implemented yet with SQLAlchemy / Alembic.")

    def create_historical_database(self):
        with self.engine.connect() as conn:
            conn.execute(CreateSchema(self.schema, if_not_exists=True))
            conn.execute(CreateTable(get_table_with_schema(self.schema, ExperimentRunTable), if_not_exists=True))
            conn.execute(CreateTable(get_table_with_schema(self.schema, JobDataTable), if_not_exists=True))
            conn.commit()
            # TODO: implement db migrations?
            # self._set_historical_pragma_version(CURRENT_DB_VERSION)

    def update_historical_database(self):
        raise NotImplementedError("This feature has not been implemented yet with SQLAlchemy / Alembic.")

    def get_experiment_run_dc_with_max_id(self):
        run = self._get_experiment_run_with_max_id()
        return ExperimentRun.from_model(run)

    def register_experiment_run_dc(self, experiment_run_dc):
        query = (
            insert(get_table_with_schema(self.schema, ExperimentRunTable)).
            values(
                created=HUtils.get_current_datetime(),
                modified=HUtils.get_current_datetime(),
                start=experiment_run_dc.start,
                finish=experiment_run_dc.finish,
                chunk_unit=experiment_run_dc.chunk_unit,
                chunk_size=experiment_run_dc.chunk_size,
                completed=experiment_run_dc.completed,
                total=experiment_run_dc.total,
                failed=experiment_run_dc.failed,
                queuing=experiment_run_dc.queuing,
                running=experiment_run_dc.running,
                submitted=experiment_run_dc.submitted,
                suspended=experiment_run_dc.suspended,
                metadata=experiment_run_dc.metadata
            )
        )
        with self.engine.connect() as conn:
            conn.execute(query)
            conn.commit()
        return ExperimentRun.from_model(self._get_experiment_run_with_max_id())

    def update_experiment_run_dc_by_id(self, experiment_run_dc):
        experiment_run_table = get_table_with_schema(self.schema, ExperimentRunTable)
        query = (
            update(experiment_run_table).
            where(experiment_run_table.c.run_id == experiment_run_dc.run_id).  # type: ignore
            values(
                finish=experiment_run_dc.finish,
                chunk_unit=experiment_run_dc.chunk_unit,
                chunk_size=experiment_run_dc.chunk_size,
                completed=experiment_run_dc.completed,
                total=experiment_run_dc.total,
                failed=experiment_run_dc.failed,
                queuing=experiment_run_dc.queuing,
                running=experiment_run_dc.running,
                submitted=experiment_run_dc.submitted,
                suspended=experiment_run_dc.suspended,
                modified=HUtils.get_current_datetime()
            )
        )
        with self.engine.connect() as conn:
            conn.execute(query)
            conn.commit()
        return ExperimentRun.from_model(self._get_experiment_run_with_max_id())

    def _get_experiment_run_with_max_id(self):
        experiment_run_table = get_table_with_schema(self.schema, ExperimentRunTable)
        query = (
            select(experiment_run_table).
            where(experiment_run_table.c.run_id > 0).
            order_by(desc(experiment_run_table.c.run_id))
        )
        with self.engine.connect() as conn:
            row = conn.execute(query).first()
            if not row:
                raise Exception("No Experiment Runs registered.")
        return Models.ExperimentRunRow(*row)

    def is_there_a_last_experiment_run(self):
        experiment_run_table = get_table_with_schema(self.schema, ExperimentRunTable)
        query = (
            select(experiment_run_table).
            where(experiment_run_table.c.run_id > 0).
            order_by(desc(experiment_run_table.c.run_id))
        )
        with self.engine.connect() as conn:
            result = conn.execute(query).first()
        return result is not None

    def get_job_data_all(self):
        job_data_table = get_table_with_schema(self.schema, JobDataTable)
        with self.engine.connect() as conn:
            job_data_rows = conn.execute(select(job_data_table)).all()
        return [Models.JobDataRow(*row) for row in job_data_rows]

    def register_submitted_job_data_dc(self, job_data_dc):
        self._set_current_job_data_rows_last_to_zero_by_job_name(job_data_dc.job_name)
        self._insert_job_data(job_data_dc)
        return self.get_job_data_dc_unique_latest_by_job_name(job_data_dc.job_name)

    def _set_current_job_data_rows_last_to_zero_by_job_name(self, job_name):
        """ Sets the column last = 0 for all job_rows by job_name and last = 1. """
        job_data_row_last = self._get_job_data_last_by_name(job_name)
        job_data_dc_list = [JobData.from_model(row) for row in job_data_row_last]
        for job_data_dc in job_data_dc_list:
            job_data_dc.last = 0
            self._update_job_data_by_id(job_data_dc)

    def update_job_data_dc_by_job_id_name(self, job_data_dc: Any) -> Any:
        """
        Update JobData data class. Returns the latest row from job_data by job_name.

        :param job_data_dc: The JobData data class instance containing job_id and job_name.
        :type job_data_dc: JobData
        :return: The latest row from job_data corresponding to the given job_id and job_name.
        :rtype: Any
        """
        self._update_job_data_by_id(job_data_dc)
        # Return the latest row from job_data by job_id and job_name
        return self.get_job_data_by_job_id_name(job_data_dc.job_id, job_data_dc.job_name)

    def update_list_job_data_dc_by_each_id(self, job_data_dcs):
        """ Return length of updated list. """
        for job_data_dc in job_data_dcs:
            self._update_job_data_by_id(job_data_dc)
        return len(job_data_dcs)

    def get_job_data_dc_unique_latest_by_job_name(self, job_name):
        job_data_row_last = self._get_job_data_last_by_name(job_name)
        if len(job_data_row_last) > 0:
            return JobData.from_model(job_data_row_last[0])
        return None

    def _get_job_data_last_by_name(self, job_name):
        job_data_table = get_table_with_schema(self.schema, JobDataTable)
        query = (
            select(job_data_table).
            where(
                and_(job_data_table.c.last == 1, job_data_table.c.job_name == job_name)
            ).
            order_by(desc(job_data_table.c.counter))
        )
        with self.engine.connect() as conn:
            job_data_rows_last = conn.execute(query).all()
        # if previous job didn't finished but a new create has been made
        if not job_data_rows_last:
            new_query = (
                select(job_data_table).
                where(
                    and_(job_data_table.c.last == 0, job_data_table.c.job_name == job_name)
                ).
                order_by(desc(job_data_table.c.counter))
            )
            with self.engine.connect() as conn:
                job_data_rows_last = conn.execute(new_query).all()
        return [Models.JobDataRow(*row) for row in job_data_rows_last]

    def get_job_data_dcs_last_by_wrapper_code(self, wrapper_code):
        if wrapper_code and wrapper_code > 2:
            return [JobData.from_model(row) for row in self._get_job_data_last_by_wrapper_code(wrapper_code)]
        else:
            return []

    def _get_job_data_last_by_wrapper_code(self, wrapper_code):
        """ Get List of Models.JobDataRow for last=1 and rowtype=wrapper_code """
        job_data_table = get_table_with_schema(self.schema, JobDataTable)
        query = (
            select(job_data_table).
            where(
                and_(
                    job_data_table.c.rowtype == wrapper_code,
                    job_data_table.c.last == 1
                )
            ).
            order_by(job_data_table.c.id)
        )
        with self.engine.connect() as conn:
            job_data_rows = conn.execute(query).all()
        return [Models.JobDataRow(*row) for row in job_data_rows]

    def get_all_last_job_data_dcs(self):
        """ Gets JobData data classes in job_data for last=1. """
        job_data_rows = self._get_all_last_job_data_rows()
        return [JobData.from_model(row) for row in job_data_rows]

    def _get_all_last_job_data_rows(self):
        """ Get List of Models.JobDataRow for last=1. """
        job_data_table = get_table_with_schema(self.schema, JobDataTable)
        query = (
            select(job_data_table).
            where(job_data_table.c.last == 1)  # type: ignore
        )
        with self.engine.connect() as conn:
            job_data_rows = conn.execute(query).all()
        return [Models.JobDataRow(*row) for row in job_data_rows]

    def _insert_job_data(self, job_data):
        job_data_table = get_table_with_schema(self.schema, JobDataTable)
        insert_query = (
            insert(job_data_table).
            values(
                counter=job_data.counter,
                job_name=job_data.job_name,
                created=HUtils.get_current_datetime(),
                modified=HUtils.get_current_datetime(),
                submit=job_data.submit,
                start=job_data.start,
                finish=job_data.finish,
                status=job_data.status,
                rowtype=job_data.rowtype,
                ncpus=job_data.ncpus,
                wallclock=job_data.wallclock,
                qos=job_data.qos,
                energy=job_data.energy,
                date=job_data.date,
                section=job_data.section,
                member=job_data.member,
                chunk=job_data.chunk,
                last=job_data.last,
                platform=job_data.platform,
                job_id=job_data.job_id,
                extra_data=job_data.extra_data,
                nnodes=job_data.nnodes,
                run_id=job_data.run_id,
                MaxRSS=job_data.MaxRSS,
                AveRSS=job_data.AveRSS,
                out=job_data.out,
                err=job_data.err,
                rowstatus=job_data.rowstatus,
                children=job_data.children,
                platform_output=job_data.platform_output
            )
        )
        with self.engine.connect() as conn:
            result = conn.execute(insert_query)
            conn.commit()
        return result.lastrowid

    def update_many_job_data_change_status(self, changes):
        # type : (List[Tuple]) -> None
        """
        Update many job_data rows in bulk. Requires a changes list of argument tuples.
        Only updates finish, modified, status, and rowstatus by id.
        """
        job_data_table = get_table_with_schema(self.schema, JobDataTable)
        with self.engine.connect() as conn:
            for change in changes:
                query = (
                    update(job_data_table).
                    where(job_data_table.c.id == change[3]).  # type: ignore
                    values(
                        modified=change[0],
                        status=change[1],
                        rowstatus=change[2]
                    )
                )
                conn.execute(query)
            conn.commit()

    def _update_job_data_by_id(self, job_data_dc):
        job_data_table = get_table_with_schema(self.schema, JobDataTable)
        # noinspection PyProtectedMember
        query = (
            update(job_data_table).
            where(job_data_table.c.id == job_data_dc._id).  # type: ignore
            values(
                last=job_data_dc.last,
                submit=job_data_dc.submit,
                start=job_data_dc.start,
                finish=job_data_dc.finish,
                modified=HUtils.get_current_datetime(),
                job_id=job_data_dc.job_id,
                status=job_data_dc.status,
                energy=job_data_dc.energy,
                extra_data=job_data_dc.extra_data,
                nnodes=job_data_dc.nnodes,
                ncpus=job_data_dc.ncpus,
                rowstatus=job_data_dc.rowstatus,
                out=job_data_dc.out,
                err=job_data_dc.err,
                children=job_data_dc.children,
                platform_output=job_data_dc.platform_output,
            )
        )
        with self.engine.connect() as conn:
            conn.execute(query)
            conn.commit()

    def get_job_data_by_job_id_name(self, job_id: int, job_name: str) -> JobData:
        """Get the job data by job ID and name."""
        job_data_table = get_table_with_schema(self.schema, JobDataTable)
        query = (
            select(job_data_table)
            .where(job_data_table.c.job_id == job_id)  # type: ignore
            .where(job_data_table.c.job_name == job_name)
            .order_by(job_data_table.c.counter.desc())
        )
        with self.engine.connect() as conn:
            result = conn.execute(query).first()
            return JobData.from_model(result)

    def get_job_data_max_counter(self, job_name: str = None):
        """ The max counter is the maximum count value for the count column in job_data. """
        job_data_table = get_table_with_schema(self.schema, JobDataTable)
        query = select(func.max(job_data_table.c.counter).label("maxcounter"))
        if job_name:
            query = query.where(job_data_table.c.job_name == job_name)  # type: ignore
        with self.engine.connect() as conn:
            result = conn.execute(query).first()
        max_counter = result.maxcounter
        return max_counter if max_counter else DEFAULT_MAX_COUNTER


def create_experiment_history_db_manager(db_engine: str, **options: Any) -> ExperimentHistoryDatabaseManager:
    if db_engine == 'postgres':
        return cast(ExperimentHistoryDatabaseManager, SqlAlchemyExperimentHistoryDbManager(options['schema']))
    elif db_engine == 'sqlite':
        return cast(ExperimentHistoryDatabaseManager, ExperimentHistoryDbManager(
            options['schema'],
            options.get('jobdata_dir_path', DEFAULT_JOBDATA_DIR)
        ))
    else:
        raise ValueError(f"Invalid database engine: {db_engine}")
