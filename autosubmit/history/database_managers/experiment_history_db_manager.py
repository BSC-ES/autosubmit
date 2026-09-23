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

import traceback
from pathlib import Path
from typing import Any

from sqlalchemy import (
    Connection,
    and_,
    bindparam,
    desc,
    func,
    insert,
    inspect,
    select,
    text,
    update,
)
from sqlalchemy.schema import CreateIndex, CreateSchema, CreateTable

import autosubmit.history.utils as HUtils
from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database import session
from autosubmit.database.db_utils import batch_size_for, chunked, max_params
from autosubmit.database.migrations import (
    ensure_schema_migrations_table,
    get_schema_version,
    record_migration,
    schema_migrations_table,
)
from autosubmit.database.tables import (
    ExperimentRunTable,
    JobDataTable,
    TableRegistry,
)
from autosubmit.history.data_classes.experiment_run import ExperimentRun
from autosubmit.history.data_classes.job_data import JobData
from autosubmit.history.database_managers import database_models as Models
from autosubmit.log.log import Log

CURRENT_DB_VERSION = 21  # Update this if you change the database schema
DB_EXPERIMENT_HEADER_SCHEMA_CHANGES = 14
DB_VERSION_SCHEMA_CHANGES = 12
DEFAULT_DB_VERSION = 10
DEFAULT_MAX_COUNTER = 0

class SqlAlchemyExperimentHistoryDbManager:
    """A SQLAlchemy experiment history database manager.
    Its interface was designed based on the SQLite database manager,
    with the following differences:
    - We do not have the DB migration system that they used, as that
      used SQLite pragmas, which are not portable across DB engines
      (i.e. no ``_set_schema_changes()`` nor ``_set_table_queries()``).
    """

    def __init__(
            self,
            schema: str,
            jobdata_path: str,
            jobdata_file: str | None = None,
    ) -> None:
        """Initialize the SQLAlchemy experiment-history manager.

        :param schema: DB schema name.
        :param jobdata_path: Directory (sqlite) or URL Path (postgres).
        :param jobdata_file: Optional DB filename (used for sqlite; ignored for Postgres).
        """
        base = jobdata_path if jobdata_path else BasicConfig.JOBDATA_DIR
        file_name = jobdata_file if jobdata_file else f"job_data_{schema}.db"
        db_path = Path(base) / file_name

        # Decide whether to use a schema based on the database backend
        # Postgres supports schemas, while SQLite does not.
        if BasicConfig.DATABASE_BACKEND == "postgres":
            self.schema = schema
        else:
            self.schema = None

        self.table_registry = TableRegistry(schema=self.schema)
        self.engine = session.get_engine(db_path=db_path)
        self._version_table = schema_migrations_table(
            self.table_registry.metadata, name="history_schema_migrations"
        )

    def initialize(self) -> None:
        """Create the historical database tables if they do not exist, then migrate any missing columns."""
        self.create_historical_database()
        self._migrate_schema()

    def _create_indexes(self, conn: Connection) -> None:
        """Create the indexes declared on the ``job_data`` table, if they do not exist.

        ``CreateTable`` only emits the ``CREATE TABLE`` DDL, it does not emit the
        indexes declared with ``Column(..., index=True)``. This method ensures the
        ``job_data(job_name)`` index exists.

        :param conn: An open SQLAlchemy connection.
        """
        table = self.table_registry.get(JobDataTable.name)
        for index in table.indexes:
            conn.execute(CreateIndex(index, if_not_exists=True))

    def _set_db_version(self, conn: Connection, version: int) -> None:
        """Record the schema version as applied in the historical database.

        :param conn: An open SQLAlchemy connection.
        :param version: The schema version to record.
        """
        record_migration(conn, self._version_table, version)

    def _get_db_version(self) -> int:
        """Read the schema version recorded in the historical database.

        :return: The recorded version, or 0 if it is unknown.
        """
        return get_schema_version(self.engine, self._version_table, self.schema)

    def _migrate_schema(self) -> None:
        """Add missing columns, ensure indexes and update the stored schema version."""
        inspector = inspect(self.engine)
        quote = self.engine.dialect.identifier_preparer.quote
        dialect = self.engine.dialect

        for model_table in (JobDataTable, ExperimentRunTable):
            if not inspector.has_table(model_table.name, schema=self.schema):
                continue
            existing = {c['name'] for c in inspector.get_columns(model_table.name, schema=self.schema)}
            pending = [c for c in model_table.columns if c.name not in existing]
            if not pending:
                continue
            qualified_name = f"{quote(self.schema)}.{quote(model_table.name)}" if self.schema else quote(model_table.name)
            with self.engine.connect() as conn:
                for col in pending:
                    parts = [col.type.compile(dialect=dialect)]
                    if not col.nullable:
                        parts.append("NOT NULL")
                    if col.default is not None:
                        raw = col.default.arg
                        if isinstance(raw, str):
                            parts.append(f"DEFAULT '{raw}'")
                        elif isinstance(raw, bool):
                            parts.append(f"DEFAULT {'TRUE' if raw else 'FALSE'}")
                        else:
                            parts.append(f"DEFAULT {raw}")
                    conn.execute(
                        text(f"ALTER TABLE {qualified_name} ADD COLUMN {quote(col.name)} {' '.join(parts)}")
                    )
                conn.commit()

        if inspector.has_table(JobDataTable.name, schema=self.schema):
            with self.engine.begin() as conn:
                self._create_indexes(conn)
                self._set_db_version(conn, CURRENT_DB_VERSION)


    def my_database_exists(self) -> bool:
        """Return ``True`` if the schema and tables exist in the database. ``False`` otherwise."""
        inspector = inspect(self.engine)
        if self.schema is not None and self.schema not in inspector.get_schema_names():
            return False
        return (
                inspector.has_table(ExperimentRunTable.name, schema=self.schema)
                and inspector.has_table(JobDataTable.name, schema=self.schema)
        )

    def is_header_ready_db_version(self) -> bool:
        """Return ``True`` if the stored schema version is new enough for the completion trigger."""
        return self._get_db_version() >= DB_EXPERIMENT_HEADER_SCHEMA_CHANGES

    def is_current_version(self) -> bool:
        """Return ``True`` if the stored schema version matches the current one."""
        return self._get_db_version() == CURRENT_DB_VERSION

    def create_historical_database(self) -> None:
        """Create the historical tables, the version table and the indexes if they do not exist."""
        with self.engine.begin() as conn:
            if BasicConfig.DATABASE_BACKEND != "sqlite":
                conn.execute(CreateSchema(self.schema, if_not_exists=True))
            conn.execute(CreateTable(self.table_registry.get(ExperimentRunTable.name), if_not_exists=True))
            conn.execute(CreateTable(self.table_registry.get(JobDataTable.name), if_not_exists=True))
            ensure_schema_migrations_table(conn, self._version_table)
            self._create_indexes(conn)
            self._set_db_version(conn, CURRENT_DB_VERSION)
            # TODO(#1286): implement the SQLite -> PostgreSQL data migration.

    def update_historical_database(self) -> None:
        """Bring an existing historical database up to date (missing columns, indexes and version)."""
        self.create_historical_database()
        self._migrate_schema()

    def get_experiment_run_dc_with_max_id(self):
        run = self._get_experiment_run_with_max_id()
        return ExperimentRun.from_model(run)

    def get_experiment_run_dc_with_max_id_or_none(self) -> ExperimentRun | None:
        """Get Current (latest) ExperimentRun data class, or None if not available."""
        try:
            return self.get_experiment_run_dc_with_max_id()
        except Exception:
            return None

    def register_experiment_run_dc(self, experiment_run_dc):
        query = (
            insert(self.table_registry.get(ExperimentRunTable.name)).
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
        with self.engine.connect() as conn, conn.begin():
            conn.execute(query)
        return ExperimentRun.from_model(self._get_experiment_run_with_max_id())

    def update_experiment_run_dc_by_id(self, experiment_run_dc):
        experiment_run_table = self.table_registry.get(ExperimentRunTable.name)
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
        with self.engine.connect() as conn, conn.begin():
            conn.execute(query)
        return ExperimentRun.from_model(self._get_experiment_run_with_max_id())

    def _get_experiment_run_with_max_id(self):
        """ Get Models.ExperimentRunRow for the maximum id run. """
        experiment_run_table = self.table_registry.get(ExperimentRunTable.name)
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

    def _get_max_experiment_run_id(self) -> int:
        """Get the maximum experiment run ID from the experiment_run table."""
        experiment_run_table = self.table_registry.get(ExperimentRunTable.name)
        query = select(func.max(experiment_run_table.c.run_id))
        with self.engine.connect() as conn:
            result = conn.execute(query).first()
        return result[0] if result and result[0] is not None else 0

    def is_there_a_last_experiment_run(self):
        """Return ``True`` if there is at least one experiment run in the database. ``False`` otherwise."""
        experiment_run_table = self.table_registry.get(ExperimentRunTable.name)
        query = (
            select(experiment_run_table).
            where(experiment_run_table.c.run_id > 0).
            order_by(desc(experiment_run_table.c.run_id))
        )
        with self.engine.connect() as conn:
            result = conn.execute(query).first()
        return result is not None

    def get_job_data_all(self):
        job_data_table = self.table_registry.get(JobDataTable.name)
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

    def get_job_data_dc_unique_latest_by_job_name(self, job_name: str | None):
        """ Returns JobData data class for the latest job_data_row with last=1 by job_name. """
        job_data_row_last = self._get_job_data_last_by_name(job_name)
        if len(job_data_row_last) > 0:
            return JobData.from_model(job_data_row_last[0])
        return None

    def _get_job_data_last_by_name(self, job_name):
        job_data_table = self.table_registry.get(JobDataTable.name)
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
        job_data_table = self.table_registry.get(JobDataTable.name)
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
        job_data_table = self.table_registry.get(JobDataTable.name)
        # TODO(#3114): select only the needed columns once callers no longer
        #             require a full Models.JobDataRow.
        query = (
            select(job_data_table).
            where(job_data_table.c.last == 1)  # type: ignore
        )
        with self.engine.connect() as conn:
            job_data_rows = conn.execute(query).all()
        return [Models.JobDataRow(*row) for row in job_data_rows]

    def _insert_job_data(self, job_data):
        job_data_table = self.table_registry.get(JobDataTable.name)
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
                platform_output=job_data.platform_output,
                workflow_commit=job_data.workflow_commit,
                split=job_data.split,
                splits=job_data.splits,
                fail_count=job_data.fail_count,
            )
        )
        with self.engine.connect() as conn, conn.begin():
            result = conn.execute(insert_query)
        return result.lastrowid

    def update_many_job_data_change_status(self, changes) -> None:
        """Update many job_data rows in bulk.

        Requires a changes list of tuples ``(modified, status, rowstatus, id)``.
        Only updates modified, status, and rowstatus by id.

        :param changes: The list of change tuples to apply.
        """
        job_data_table = self.table_registry.get(JobDataTable.name)
        query = (
            update(job_data_table)
            .where(job_data_table.c.id == bindparam("_id"))
            .values(
                modified=bindparam("_modified"),
                status=bindparam("_status"),
                rowstatus=bindparam("_rowstatus"),
            )
        )
        params = [
            {
                "_id": change[3],
                "_modified": change[0],
                "_status": change[1],
                "_rowstatus": change[2],
            }
            for change in changes
        ]
        batch_size = batch_size_for(4, self.engine.dialect.name)
        with self.engine.begin() as conn:
            for batch in chunked(params, batch_size):
                conn.execute(query, batch)

    def _update_job_data_by_id(self, job_data_dc):
        job_data_table = self.table_registry.get(JobDataTable.name)
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
                workflow_commit=job_data_dc.workflow_commit,
                split=job_data_dc.split,
                splits=job_data_dc.splits,
                fail_count=job_data_dc.fail_count,
            )
        )
        with self.engine.connect() as conn, conn.begin():
            conn.execute(query)

    def get_job_data_by_job_id_name(self, job_id: int, job_name: str) -> JobData:
        """Get the latest job data for a job ID and name.

        :param job_id: The job ID.
        :param job_name: The job name.
        :return: The most recent JobData for the given job ID and name.
        :raises Exception: If no job_data is found.
        """
        job_data_table = self.table_registry.get(JobDataTable.name)
        query = (
            select(job_data_table)
            .where(job_data_table.c.job_id == job_id)  # type: ignore
            .where(job_data_table.c.job_name == job_name)
            .order_by(job_data_table.c.counter.desc())
        )
        with self.engine.connect() as conn:
            result = conn.execute(query).first()
        if result is None:
            raise Exception(f"No job_data found for job_id='{job_id}' and job_name='{job_name}'.")
        return JobData.from_model(result)

    def get_last_job_data_dc_by_job_name_and_fail_counter(self, job_name: str, fail_count: int) -> JobData:
        """Get the last job data by job name and fail_count.

        :param job_name: The job name.
        :type job_name: str
        :param fail_count: The counter value.
        :type fail_count: int
        :return: The most recent JobData instance for the given job_name and counter.
        :rtype: JobData
        :raises Exception: If no job_data is found for the given job_name and counter.
        """
        job_data_table = self.table_registry.get(JobDataTable.name)
        query = (
            select(job_data_table)
            .where(job_data_table.c.job_name == job_name)  # type: ignore
            .where(job_data_table.c.fail_count == fail_count)  # type: ignore
            .order_by(desc(job_data_table.c.id))
        )
        with self.engine.connect() as conn:
            result = conn.execute(query).first()
        if result is None:
            raise Exception(f"No job_data found for job_name='{job_name}' and fail_count={fail_count}.")
        return JobData.from_model(result)

    def get_last_job_data_dc_by_job_name_and_counter(self, job_name: str, counter: int) -> JobData:
        """Get the last JobData for a given job_name and counter.

        :param job_name: The job name.
        :type job_name: str
        :param counter: The counter value.
        :type counter: int
        :return: The most recent JobData instance for the given job_name and counter.
        :rtype: JobData
        :raises Exception: If no job_data is found for the given job_name and counter.
        """
        job_data_table = self.table_registry.get(JobDataTable.name)
        query = (
            select(job_data_table)
            .where(job_data_table.c.job_name == job_name)  # type: ignore
            .where(job_data_table.c.counter == counter)  # type: ignore
            .order_by(desc(job_data_table.c.id))
        )
        with self.engine.connect() as conn:
            result = conn.execute(query).first()
        if result is None:
            raise Exception(f"No job_data found for job_name='{job_name}' and counter={counter}.")
        return JobData.from_model(result)

    def get_last_job_data_dc_by_job_name(self, job_name: str) -> JobData:
        """Get the most recent JobData for a given job_name regardless of counter.

        :param job_name: The job name.
        :type job_name: str
        :return: The JobData instance with the highest id for the given job_name.
        :rtype: JobData
        :raises Exception: If no job_data is found for the given job_name.
        """
        job_data_table = self.table_registry.get(JobDataTable.name)
        query = (
            select(job_data_table)
            .where(job_data_table.c.job_name == job_name)  # type: ignore
            .order_by(desc(job_data_table.c.id))
        )
        with self.engine.connect() as conn:
            result = conn.execute(query).first()
        if result is None:
            raise Exception(f"No job_data found for job_name='{job_name}'.")
        return JobData.from_model(result)

    def get_job_data_max_counter(self, job_name: str | None = None) -> int:
        """Get the maximum counter value in job_data.

        :param job_name: Optional job name to filter by.
        :return: The maximum counter, or the default when there is none.
        """
        job_data_table = self.table_registry.get(JobDataTable.name)
        query = select(func.max(job_data_table.c.counter))
        if job_name:
            query = query.where(job_data_table.c.job_name == job_name)  # type: ignore
        with self.engine.connect() as conn:
            max_counter = conn.execute(query).scalar()
        return max_counter if max_counter else DEFAULT_MAX_COUNTER

    # Columns needed to recover a job's log data. Selecting only these keeps the
    # result small on large experiments.
    _JOB_DATA_LAST_ROW_COLUMNS = ("job_name", "counter", "job_id", "out", "err", "submit", "status")

    def get_jobs_data_last_row(self, job_names: list[str]) -> dict[str, Any]:
        """Return the last job_data row for each requested job name.

        :param job_names: The job names to look up.
        :return: A mapping of job name to its last row as a dictionary.
        """
        rows = self.select_jobs_data(job_names, columns=list(self._JOB_DATA_LAST_ROW_COLUMNS))
        jobs_data_by_name: dict[str, Any] = {}
        counters: dict[str, int] = {}
        for job in rows:
            if job["job_name"] not in counters or job["counter"] > counters[job["job_name"]]:
                counters[job["job_name"]] = job["counter"]
                jobs_data_by_name[job["job_name"]] = job
        return jobs_data_by_name

    def select_jobs_data(
        self, job_names: list[str], columns: list[str] | None = None
    ) -> list[dict[str, Any]]:
        """Return last=1 job_data rows for the requested job names.

        Job names are queried in chunks to stay below the maximum number of bound
        parameters of the backend, without relying on temporary tables.

        :param job_names: Job names to look up.
        :param columns: Optional subset of columns to select. Defaults to all.
        :return: One dictionary per matching row, keyed by column name.
        """
        table = self.table_registry.get(JobDataTable.name)
        selected = columns if columns is not None else list(table.c.keys())
        selected_columns = [table.c[name] for name in selected]
        # Leave one slot for the ``last == 1`` predicate.
        batch_size = max(1, max_params(self.engine.dialect.name) - 1)
        rows: list[dict[str, Any]] = []
        with self.engine.connect() as conn:
            for batch in chunked(job_names, batch_size):
                query = (
                    select(*selected_columns)
                    .where(table.c.job_name.in_(batch))
                    .where(table.c.last == 1)
                )
                rows.extend(dict(row) for row in conn.execute(query).mappings())
        return rows

    def get_stale_rows(self) -> list:
        """Return all job_data rows with submit>0 and (start=0 or finish=0).

        :return: List of Row objects with job_name, fail_count, platform.
        :rtype: list
        """
        job_data_table = self.table_registry.get(JobDataTable.name)
        query = select(
            job_data_table.c.job_name,
            job_data_table.c.fail_count,
            job_data_table.c.platform
        ).where(
            job_data_table.c.submit > 0,
            (job_data_table.c.start == 0) | (job_data_table.c.finish == 0)
        ).distinct()
        with self.engine.connect() as conn:
            return conn.execute(query).fetchall()

    def update_job_data_values(self, job_name: str, fail_count: int, start: int, finish: int) -> int:
        """Update start and finish for a specific job_data row.

        :param job_name: Job identifier.
        :param fail_count: Retry attempt number.
        :param start: Start epoch timestamp.
        :param finish: Finish epoch timestamp.
        :return: Number of rows updated.
        :rtype: int
        """
        job_data_table = self.table_registry.get(JobDataTable.name)
        stmt = update(job_data_table).where(
            job_data_table.c.job_name == job_name,
            job_data_table.c.fail_count == fail_count
        ).values(start=start, finish=finish)
        with self.engine.connect() as conn:
            result = conn.execute(stmt)
            conn.commit()
            return result.rowcount


def get_last_run_id(expid: str) -> int | None:
    """Get the last experiment run ID, or None if not available.
    Bypasses ExperimentHistory.__init__ to avoid silent failure on manager creation.
    """
    try:
        BasicConfig.read()
        manager = SqlAlchemyExperimentHistoryDbManager(expid, BasicConfig.JOBDATA_DIR)
        manager.initialize()
        run = manager.get_experiment_run_dc_with_max_id_or_none()
        return run.run_id if run else None
    except Exception as exp:
        Log.warning(f"Could not get last experiment run ID for {expid}: {exp}")
        Log.debug(traceback.format_exc())
        return None


