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


from functools import cache, cached_property
from typing import cast, List, Optional

from sqlalchemy import (
    MetaData,
    Integer,
    String,
    Table,
    Text,
    Float,
    LargeBinary,
    UniqueConstraint,
    Column, ForeignKey,
)

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

ExperimentStructureTable = Table(
    "experiment_structure",
    metadata_obj,
    Column("e_from", Text, nullable=False, primary_key=True),
    Column("e_to", Text, nullable=False, primary_key=True),
    UniqueConstraint("e_from", "e_to", name="unique_e_from_and_e_to"),
)
"""Table that holds the structure of the experiment jobs."""

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
    Column("split", Integer, nullable=True),
    Column("splits", Integer, nullable=True),
    Column("fail_count", Integer, nullable=False, default=0),
    UniqueConstraint("counter", "job_name", name="unique_counter_and_job_name"),
)

JobListTable = Table(
    "job_list",
    metadata_obj,
    Column("name", String, primary_key=True),
    Column("id", Integer),
    Column("status", Integer),
    Column("priority", Integer),
    Column("section", String),
    Column("date", String),
    Column("member", String),
    Column("chunk", Integer),
    Column("split", Integer),
    Column("local_out", String),
    Column("local_err", String),
    Column("remote_out", String),
    Column("remote_err", String),
)

JobPklTable = Table(
    "job_pkl",
    metadata_obj,
    Column("expid", String, primary_key=True),
    Column("pkl", LargeBinary),
    Column("modified", String),
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
        UniqueConstraint("package_id", "package_name", "job_name",
                         name=f"unique_{name}_jobs_package_id_package_name_job_name"),

    )
    return table_package_info, table_jobs_inside_wrapper


WrapperInfoTable, WrapperJobsTable = create_wrapper_tables("wrappers", metadata_obj)
PreviewWrapperInfoTable, PreviewWrapperJobsTable = create_wrapper_tables("preview_wrappers", metadata_obj)

TABLES = (
    ExperimentTable,
    ExperimentStatusTable,
    ExperimentStructureTable,
    ExperimentRunTable,
    DBVersionTable,
    WrapperInfoTable,
    WrapperJobsTable,
    PreviewWrapperInfoTable,
    PreviewWrapperJobsTable,
    JobDataTable,
    JobListTable,
    JobPklTable,
    DetailsTable,
    UserMetricsTable,
)
"""The tables available in the Autosubmit databases."""


def get_table_with_schema(schema: Optional[str], table: Optional[Table]) -> Table:
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
        dest_table.append_column(col.copy())

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


def get_all_tables_by_name() -> dict[str, Table]:
    """Return a dictionary of all tables, combining general and job-list tables."""
    return {table.name: table for table in TABLES}


# From 4.2.0 , used for wrappers only to keep changes minimal
class TableRegistry:
    """Manage SQLAlchemy Table instances keyed by schema and table name.

    Tables are created once per (schema, table_name) pair and reused on
    subsequent lookups, avoiding redundant MetaData and Table construction.
    """

    def __init__(self, schema: Optional[str]) -> None:
        """Initialize the registry with the target schema."""
        self._schema = schema

    @cached_property
    def metadata(self) -> MetaData:
        """Return the MetaData instance for the given schema, creating it once."""
        return MetaData(schema=self._schema)

    @cache
    def get(self, table_name: str) -> Table:
        """Return the Table for the given name and schema, creating it if needed.

        :param table_name: The name of the table.
        :return: The SQLAlchemy Table instance.
        :raises KeyError: If no table definition exists for ``table_name``.
        """
        all_tables_def = get_all_tables_by_name()
        if table_name not in all_tables_def:
            raise KeyError(f"No table definition found for '{table_name}'.")
        return all_tables_def[table_name].to_metadata(self.metadata)
