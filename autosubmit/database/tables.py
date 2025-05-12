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
    Column,
    DateTime,
    Boolean, ForeignKey,
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
    UniqueConstraint("counter", "job_name", name="unique_counter_and_job_name"),
)

# Jobs table
JobsTable = Table(
    "jobs",
    metadata_obj,
    Column("name", String, nullable=False, primary_key=True),
    Column("id", Integer),
    Column("script_name", String),
    Column("priority", Integer),
    Column("status", String),
    Column("frequency", String),
    Column("synchronize", Boolean),
    Column("section", String),
    Column("chunk", Integer),
    Column("splits", Integer),
    Column("split", Integer),
    Column("date", String),
    Column("date_split", String),
    Column("edge_info", Text),
    Column("max_checkpoint_step", Integer),
    Column("start_time", DateTime),
    Column("end_time_timestamp", Float),
    Column("submit_time_timestamp", Float),
    Column("finish_time_timestamp", Float),
    Column("ready_date", DateTime),
    Column("local_logs", String),
    Column("remote_logs", String),
    Column("log_available", Boolean),
    Column("updated_log", Boolean),
    Column("packed", Boolean),
)

# ExperimentStructureTable with dependencies
ExperimentStructureTable = Table(
    "experiment_structure",
    metadata_obj,
    Column("e_from", String, ForeignKey("jobs.job_name"), nullable=False, primary_key=True),
    Column("e_to", String, ForeignKey("jobs.job_name"), nullable=False, primary_key=True),
    Column("edge_info", Text, nullable=True),
    Column("current_checkpoint_step", Integer),
    UniqueConstraint("e_from", "e_to", name="unique_e_from_and_e_to"),


)
WrapperTable = Table(
    "wrappers",
    metadata_obj,
    Column("package_name", Text, primary_key=True),
    Column("job_name", Text, primary_key=True),
)
"""Stores a mapping between the wrapper name and the actual job in SLURM."""

PreviewWrapperTable = Table(
    "preview_wrappers",
    metadata_obj,
    Column("package_name", Text, primary_key=True),
    Column("job_name", Text, primary_key=True),
)
"""It is a replication.
It is only created/used when using inspect and create or monitor
with flag -cw in Autosubmit.
This replication is used to not interfere with the current
autosubmit run of that experiment since wrapper_job_package
will contain a preview, not the real wrapper packages."""


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

TABLES = (
    ExperimentTable,
    ExperimentStatusTable,
    ExperimentStructureTable,
    ExperimentRunTable,
    DBVersionTable,
    JobDataTable,
    DetailsTable,
    JobsTable,
    ExperimentStructureTable,
    WrapperTable,
    PreviewWrapperTable,
)
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
