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


import datetime
from typing import Optional

from sqlalchemy import (
    MetaData,
    Integer,
    String,
    Table,
    Text,
    Float,
    UniqueConstraint,
    Column,
    Boolean, ForeignKey)

metadata_obj = MetaData()

ExperimentTable = Table(
    "experiment",
    metadata_obj,
    Column("id", Integer, nullable=False, primary_key=True),
    Column("name", String, nullable=False),
    Column("description", String, nullable=False),
    Column("autosubmit_version", String),
)
"""The main table, populated by Autosubmit. Should be read-only by the API."""

# NOTE: In the original SQLite DB, db_version.version was the only field,
#       and not a PK.
DBVersionTable = Table(
    "db_version",
    metadata_obj,
    Column("version", Integer, nullable=False, primary_key=True),
)

ExperimentStatusTable = Table(
    "experiment_status",
    metadata_obj,
    Column("exp_id", Integer, primary_key=True),
    Column("name", Text, nullable=False),
    Column("status", Text, nullable=False),
    Column("seconds_diff", Integer, nullable=False),
    Column("modified", Text, nullable=False),
)
"""Stores the status of the experiments."""

# NOTE: The column ``metadata`` has a name that is reserved in
#       SQLAlchemy ORM. It works for SQLAlchemy Core, here, but
#       if you plan to use ORM, be warned that you will have to
#       search how to workaround it (or will probably have to
#       use SQLAlchemy core here).
ExperimentRunTable = Table(
    "experiment_run",
    metadata_obj,
    Column("run_id", Integer, primary_key=True),
    Column("created", Text, nullable=False),
    Column("modified", Text, nullable=True),
    Column("start", Integer, nullable=False),
    Column("finish", Integer),
    Column("chunk_unit", Text, nullable=False),
    Column("chunk_size", Integer, nullable=False),
    Column("completed", Integer, nullable=False),
    Column("total", Integer, nullable=False),
    Column("failed", Integer, nullable=False),
    Column("queuing", Integer, nullable=False),
    Column("running", Integer, nullable=False),
    Column("submitted", Integer, nullable=False),
    Column("suspended", Integer, nullable=False, default=0),
    Column("metadata", Text),
)

DetailsTable = Table(
    "details",
    metadata_obj,
    Column("exp_id", Integer, primary_key=True),
    Column("user", Text, nullable=False),
    Column("created", Text, nullable=False),
    Column("model", Text, nullable=False),
    Column("branch", Text, nullable=False),
    Column("hpc", Text, nullable=False),
)

"""Table that holds the structure of the experiment jobs."""
JobDataTable = Table(
    "job_data",
    metadata_obj,
    Column("id", Integer, nullable=False, primary_key=True),
    Column("counter", Integer, nullable=False),
    Column("job_name", Text, nullable=False, index=True),
    Column("created", Text, nullable=False),
    Column("modified", Text, nullable=False),
    Column("submit", Integer, nullable=False),
    Column("start", Integer, nullable=False),
    Column("finish", Integer, nullable=False),
    Column("status", Text, nullable=False),
    Column("rowtype", Integer, nullable=False),
    Column("ncpus", Integer, nullable=False),
    Column("wallclock", Text, nullable=False),
    Column("qos", Text, nullable=False),
    Column("energy", Integer, nullable=False),
    Column("date", Text, nullable=False),
    Column("section", Text, nullable=False),
    Column("member", Text, nullable=False),
    Column("chunk", Integer, nullable=False),
    Column("last", Integer, nullable=False),
    Column("platform", Text, nullable=False),
    Column("job_id", Integer, nullable=False),
    Column("extra_data", Text, nullable=False),
    Column("nnodes", Integer, nullable=False, default=0),
    Column("run_id", Integer),
    Column("MaxRSS", Float, nullable=False, default=0.0),
    Column("AveRSS", Float, nullable=False, default=0.0),
    Column("out", Text, nullable=False),
    Column("err", Text, nullable=False),
    Column("rowstatus", Integer, nullable=False, default=0),
    Column("children", Text, nullable=True),
    Column("platform_output", Text, nullable=True),
    Column("workflow_commit", Text, nullable=True),
    Column("split", Text, nullable=True),
    Column("splits", Text, nullable=True),
    Column("fail_count", Integer, nullable=False, default=0),
    UniqueConstraint("counter", "job_name", name="unique_counter_and_job_name"),
)

"""Table that holds the Historical structure of the experiment jobs."""

# TODO this doesn't work in POSTGRESQL
# JobStatusEnum = Enum(
#     "WAITING", "DELAYED", "PREPARED", "READY", "SUBMITTED", "HELD", "QUEUING", "RUNNING",
#     "SKIPPED", "FAILED", "UNKNOWN", "COMPLETED", "SUSPENDED",
#     name="job_status_enum"
# )

"""All these tables will go inside the $expid/db/job_list.db."""
# Jobs table
"""Table that holds the minium neccesary info about the experiment jobs."""
JobsTable = Table(
    "jobs",
    metadata_obj,
    Column("name", String, nullable=False, primary_key=True),
    Column("id", Integer),
    Column("script_name", String),
    Column("priority", Integer),
    Column("status", Text, nullable=False, index=True),  # Should be job_status_enum
    Column("frequency", String),  # TODO move to Section table ?
    Column("synchronize", Boolean),  # TODO move to Section table ?
    Column("section", String),
    Column("chunk", Integer),
    Column("member", Text),
    Column("splits", Integer),
    Column("split", Integer),
    Column("date", String),
    Column("date_split", String),
    Column("max_checkpoint_step", Integer, nullable=False, default=0),
    Column("start_time", String),
    Column("start_time_timestamp", Integer),
    Column("submit_time_timestamp", Integer),
    Column("finish_time_timestamp", Integer),
    Column("ready_date", String),
    Column("local_logs_out", String),  # tuple, to modify double value in two
    Column("local_logs_err", String),  # tuple, to modify double value in two
    Column("remote_logs_out", String),
    Column("remote_logs_err", String),
    Column("updated_log", Integer),
    Column("packed", Boolean),
    Column("current_checkpoint_step", Integer, nullable=False, default=0),
    Column("platform_name", String),
    Column("created", Text, nullable=False, default=lambda: datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")),
    Column("modified", Text, nullable=False, default=lambda: datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))
)

"""Table that holds the structure of the experiment jobs."""
ExperimentStructureTable = Table(
    "experiment_structure",
    metadata_obj,
    Column("e_from", String, nullable=False, primary_key=True, index=True),
    Column("e_to", String, nullable=False, primary_key=True, index=True),
    Column("min_trigger_status", String),
    Column("completion_status", String),
    Column("from_step", Integer),
    Column("fail_ok", Boolean),
    UniqueConstraint("e_from", "e_to", name="unique_e_from_and_e_to"),
)

# TODO: This should have
# Column("e_from", String, ForeignKey("experiment_structure.e_to"), nullable=False, primary_key=True, index=True),
# Column("e_to", String, ForeignKey("experiment_structure.e_from"), nullable=False, primary_key=True, index=True),
# But in sqlite the table is in another file
StructureDataTable = Table(
    "structure_data",
    metadata_obj,
    Column("run_id", Integer, ForeignKey("experiment_run.run_id"), nullable=False, primary_key=True, index=True),
    Column("e_from", String, nullable=False, primary_key=True, index=True),
    Column("e_to", String, nullable=False, primary_key=True, index=True),
    Column("min_trigger_status", String),
    Column("completion_status", String),
    Column("from_step", Integer),
    Column("fail_ok", Boolean),
    UniqueConstraint("run_id", "e_from", "e_to", name="unique_structure_data_run_id_e_from_and_e_to"),
)

SectionsStructureTable = Table(
    "sections",
    metadata_obj,
    Column("name", String, nullable=False, primary_key=True),
    Column("splits", Integer, nullable=True),
    Column("dependencies", String, nullable=True),
    Column("datelist", String, nullable=True),
    Column("members", String, nullable=True),
    Column("numchunks", Integer, nullable=True),
    Column("expid", String, nullable=True),
)


def create_wrapper_tables(name, metadata_obj_):
    """Create a wrapper table for the given name."""
    table_package_info = Table(
        f"{name}_info",
        metadata_obj_,
        Column("name", String, nullable=False, primary_key=True),
        Column("id", Integer),
        Column("script_name", String),
        Column("status", Text, nullable=False),  # Should be job_status_enum
        Column("local_logs_out", String),  # TODO: We should recover the log from the remote at some point
        Column("local_logs_err", String),  # TODO: We should recover the log from the remote at some point
        Column("remote_logs_out", String),  # TODO: We should recover the log from the remote at some point
        Column("remote_logs_err", String),  # TODO: We should recover the log from the remote at some point
        Column("updated_log", Integer),  # TODO: We should recover the log from the remote at some point
        Column("platform_name", String),
        Column("wallclock", String),
        Column("num_processors", Integer),
        Column("type", Text),
        Column("sections", Text),
        Column("method", Text),
    )

    table_jobs_inside_wrapper = Table(
        f"{name}_jobs",
        metadata_obj_,
        Column("package_id", Integer, nullable=False, primary_key=True),
        Column("package_name", String, nullable=False, primary_key=True),
        Column("job_name", String, ForeignKey("jobs.name"), nullable=False, primary_key=True),
        Column("timestamp", String, nullable=True),
        UniqueConstraint("package_id", "package_name", "job_name", name=f"unique_{name}_jobs_package_id_package_name_job_name"),

    )
    return table_package_info, table_jobs_inside_wrapper


WrapperInfoTable, WrapperJobsTable = create_wrapper_tables("wrappers", metadata_obj)
PreviewWrapperInfoTable, PreviewWrapperJobsTable = create_wrapper_tables("preview_wrappers", metadata_obj)

UserMetricsTable = Table(
    "user_metrics",
    metadata_obj,
    Column("user_metric_id", Integer, primary_key=True),
    Column("run_id", Integer),
    Column("job_name", Text),
    Column("metric_name", Text),
    Column("metric_value", Text),
    Column("modified", Text),
)

GENERALTABLES = {
    ExperimentTable.name: ExperimentTable,
    ExperimentStatusTable.name: ExperimentStatusTable,
    ExperimentRunTable.name: ExperimentRunTable,
    DBVersionTable.name: DBVersionTable,
    JobDataTable.name: JobDataTable,
    StructureDataTable.name: StructureDataTable,
    DetailsTable.name: DetailsTable,
    UserMetricsTable.name: UserMetricsTable,
}

JOBLISTTABLES = {
    JobsTable.name: JobsTable,
    ExperimentStructureTable.name: ExperimentStructureTable,
    WrapperInfoTable.name: WrapperInfoTable,
    WrapperJobsTable.name: WrapperJobsTable,
    PreviewWrapperInfoTable.name: PreviewWrapperInfoTable,
    PreviewWrapperJobsTable.name: PreviewWrapperJobsTable,
    SectionsStructureTable.name: SectionsStructureTable,
}


def get_all_tables_by_name() -> dict[str, Table]:
    """Return a dictionary of all tables, combining general and job-list tables."""
    return {**GENERALTABLES, **JOBLISTTABLES}


class TableRegistry:
    """Manage SQLAlchemy Table instances keyed by schema and table name.

    Tables are created once per (schema, table_name) pair and reused on
    subsequent lookups, avoiding redundant MetaData and Table construction.
    """

    def __init__(self, schema) -> None:
        """Initialize the registry with an empty cache."""
        self._cache: dict[tuple[Optional[str], str], Table] = {}
        self._metadata: dict[Optional[str], MetaData] = {}
        self._schema = schema

    def get_metadata(self) -> MetaData:
        """Return the MetaData instance for the given schema, creating it if needed.
        :return: The MetaData instance for the schema.
        """
        if self._schema not in self._metadata:
            self._metadata[self._schema] = MetaData(schema=self._schema)
        return self._metadata[self._schema]

    def get(self, table_name: str) -> Table:
        """Return the Table for the given name and schema, creating it if needed.

        :param table_name: The name of the table.
        :return: The SQLAlchemy Table instance.
        :raises KeyError: If no table definition exists for ``table_name``.
        """
        key = (self._schema, table_name)
        if key not in self._cache:
            self._cache[key] = self._build(table_name)
        return self._cache[key]

    def _build(self, table_name: str) -> Table:
        """Build and return a new Table for the given name attached to this schema.

        :param table_name: The name of the table to build.
        :return: A new SQLAlchemy ``Table`` instance.
        :raises KeyError: If ``table_name`` is not found in the global table registry.
        """
        all_tables_def = get_all_tables_by_name()
        if table_name not in all_tables_def:
            raise KeyError(f"No table definition found for '{table_name}'.")
        definition_table = all_tables_def[table_name]
        metadata = self.get_metadata()
        return definition_table.to_metadata(metadata)
