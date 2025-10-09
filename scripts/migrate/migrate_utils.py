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

from typing import List, Optional, Type, Union

from sqlalchemy import (
    Column,
    Connection,
    Engine,
    Float,
    Integer,
    MetaData,
    Table,
    Text,
    UniqueConstraint,
    delete,
    insert,
    inspect,
    select,
)
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.schema import CreateSchema, CreateTable


def check_table_schema(engine: Engine, valid_tables: List[Table]) -> Union[Table, None]:
    """
    Check if one of the valid table schemas matches the current table schema.
    Returns the first matching table schema or None if no match is found.
    ORDER MATTERS!!! Table with more columns (more restrictive) should be first
    """
    for valid_table in valid_tables:
        try:
            # Get the current columns of the table
            current_columns = inspect(engine).get_columns(
                valid_table.name, valid_table.schema
            )
            column_names = [column["name"] for column in current_columns]

            # Get the columns of the valid table
            valid_columns = valid_table.columns.keys()
            # Check if all the valid table columns are present in the current table
            if all(column in column_names for column in valid_columns):
                return valid_table
        except Exception as exc:
            print(f"Error inspecting table {valid_table.name}: {exc}")
            continue
    return None


def table_copy(table: Table, metadata: Optional[MetaData] = None) -> Table:
    """
    Copy a table schema
    """
    if not isinstance(metadata, MetaData):
        metadata = MetaData()
    return Table(
        table.name,
        metadata,
        *[col.copy() for col in table.columns],
    )


def table_change_schema(
    schema: str, source: Union[Type[DeclarativeBase], Table]
) -> Table:
    """
    Copy the source table and change the schema of that SQLAlchemy table into a new table instance
    """
    if isinstance(source, type) and issubclass(source, DeclarativeBase):
        _source_table: Table = source.__table__
    elif isinstance(source, Table):
        _source_table = source
    else:
        raise RuntimeError("Invalid source type on table schema change")

    metadata = MetaData(schema=schema)
    return table_copy(_source_table, metadata)


OldJobDataTable = Table(
    "job_data",
    MetaData(),
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
    UniqueConstraint("counter", "job_name", name="unique_counter_and_job_name"),
)


def _copy_table_data(
    source_conn: Connection,
    target_conn: Connection,
    expid: str,
    source_table: Table,
    dest_table: Optional[Table] = None,
    schema_required=True,
):
    """
    Helper function to copy table data from SQLite to Postgres.
    """
    if dest_table is None:
        dest_table = source_table

    # Change schema if needed
    target_table = (
        table_change_schema(expid, dest_table)
        if expid and schema_required
        else dest_table
    )
    if schema_required and expid:
        target_conn.execute(CreateSchema(expid, if_not_exists=True))
    target_conn.execute(CreateTable(target_table, if_not_exists=True))
    rows = source_conn.execute(select(source_table)).all()
    if rows:
        target_conn.execute(delete(target_table))  # Clear existing data
        target_conn.execute(insert(target_table), [row._asdict() for row in rows])
