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
from typing import cast, List, Optional

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
    UniqueConstraint("counter", "job_name", name="unique_counter_and_job_name"),
)

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
    Column("section", String, ForeignKey("sections.name")),
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
    Column("e_from", String, ForeignKey("jobs.job_name"), nullable=False, primary_key=True, index=True),
    Column("e_to", String, ForeignKey("jobs.job_name"), nullable=False, primary_key=True, index=True),
    Column("min_trigger_status", String),
    Column("completion_status", String),
    Column("from_step", Integer),
    Column("fail_ok", Boolean),
    UniqueConstraint("e_from", "e_to", name="unique_e_from_and_e_to"),
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
        Column("package_id", Integer, ForeignKey("{name}_info.id"), nullable=False, primary_key=True),
        Column("package_name", String, ForeignKey(f"{name}_info.name"), nullable=False, primary_key=True),
        Column("job_name", String, ForeignKey("jobs.name"), nullable=False, primary_key=True),
        Column("timestamp", String, nullable=True),
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

GENERALTABLES = (
    ExperimentTable,
    ExperimentStatusTable,
    ExperimentRunTable,
    DBVersionTable,
    JobDataTable,
    DetailsTable,
    UserMetricsTable,
)

JOBLISTTABLES = (
    JobsTable,
    ExperimentStructureTable,
    WrapperInfoTable,
    WrapperJobsTable,
    PreviewWrapperInfoTable,
    PreviewWrapperJobsTable,
    SectionsStructureTable,
)

TABLES = GENERALTABLES + JOBLISTTABLES

"""The tables available in the Autosubmit databases."""


def get_table_with_schema(schema: Optional[str], table: Table) -> Table:
    """Get the ``Table`` instance with the metadata modified.
    The metadata will use the given container. This means you can
    have table ``A`` with no schema, then call this function with
    ``schema=a000``, and then a new table ``A`` with ``schema=a000``
    will be returned.
    :param schema: The target schema for the table metadata.
    :param table: The SQLAlchemy Table.
    :return: The same table, but with the given schema set as metadata.
    """
    if not isinstance(table, Table):
        raise ValueError("Invalid source type on table schema change")

    metadata = MetaData(schema=schema)
    dest_table = Table(table.name, metadata)

    # TODO: .copy is deprecated, https://github.com/sqlalchemy/sqlalchemy/discussions/8213
    for col in cast(List, table.columns):
        dest_table.append_column(col._copy())

    return dest_table


def get_table_from_name(*, schema: Optional[str], table_name: str) -> Table:
    """Get the table from a given table name.
    :param schema: The schema name.
    :param table_name: The table name.
    :return: The table if found, ``None`` otherwise.
    :raises ValueError: If the table name is not provided.
    """
    if not table_name:
        raise ValueError(f"Missing table name: {table_name}")

    def predicate(t: Table) -> bool:
        return t.name.lower() == table_name.lower()

    table = next(filter(predicate, TABLES), None)
    return get_table_with_schema(schema, table)
