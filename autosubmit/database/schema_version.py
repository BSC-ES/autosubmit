# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
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

"""Per-target schema version tracking.

This module keeps track of a schema version per target. Targets are the general
database (which also holds ``experiment`` and ``details``), ``as_times``
(``experiment_status``), and the per-experiment job_list and history databases
(each experiment has its own SQLite file or PostgreSQL schema).

The version table lives in the target database and is created and recorded
lazily: on a fresh install for the general database, or the first time a target
database is opened. A pre-existing database that is never opened again therefore
has no version row, which is fine: the version is schema metadata, not a registry
of experiments, and nothing depends on it until the database is used. The legacy
versioning (SQLite pragmas / the ``db_version`` table) is not read here, so a
pre-existing database is stamped with the current version the first time it is
opened.

TODO(#1286): a dedicated migration tool should eventually read the legacy version
and apply the ordered migration steps, taking over this layer. The tool is TBD.
Alternatives to evaluate:
- Alembic: SQLAlchemy-native; autogenerates migration scripts by diffing the
  models against the DB, orders them, and handles SQLite ALTER via batch mode.
  Would need customization to fit the per-experiment files/schemas.
- Minimal custom runner (Cylc-style): ordered migration steps plus a version
  table. Zero dependencies and full control of the per-experiment targets, but
  migrations are written by hand (no autogeneration).
"""

import datetime

from sqlalchemy import Column, DateTime, Integer, MetaData, Table, func, inspect, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.schema import CreateTable

__all__ = [
    "ensure_schema_migrations_table",
    "get_schema_version",
    "record_schema_migration",
    "schema_migrations_table",
]


def schema_migrations_table(metadata: MetaData, name: str) -> Table:
    """Build the table that records which migrations were applied.

    :param metadata: The metadata the table belongs to.
    :param name: The table name.
    :return: The ``schema_migrations`` table.
    """
    return Table(
        name,
        metadata,
        Column("version", Integer, primary_key=True, nullable=False),
        Column("applied_at", DateTime(timezone=True), nullable=False),
    )


def _now() -> datetime.datetime:
    """Return the current UTC timestamp used for ``applied_at``."""
    return datetime.datetime.now(datetime.timezone.utc)


def ensure_schema_migrations_table(target: Engine | Connection, table: Table) -> None:
    """Create the ``schema_migrations`` table if it does not exist.

    :param target: The engine that owns the target database, or an open connection.
    :param table: The ``schema_migrations`` table.
    """
    if isinstance(target, Engine):
        with target.begin() as conn:
            conn.execute(CreateTable(table, if_not_exists=True))
    else:
        target.execute(CreateTable(table, if_not_exists=True))


def get_schema_version(engine: Engine, table: Table, schema: str | None = None) -> int:
    """Return the highest migration version recorded for the target.

    :param engine: The engine that owns the target database.
    :param table: The ``schema_migrations`` table.
    :param schema: Optional schema name (PostgreSQL).
    :return: The current schema version, or 0 if none was recorded.
    """
    if not inspect(engine).has_table(table.name, schema=schema):
        return 0
    with engine.connect() as conn:
        version = conn.execute(select(func.max(table.c.version))).scalar()
    return int(version) if version is not None else 0


def record_schema_migration(conn: Connection, table: Table, version: int) -> None:
    """Record a migration as applied. Safe to call more than once.

    Uses a dialect upsert (``ON CONFLICT DO NOTHING``) so concurrent writers do
    not race on the check-then-insert.

    :param conn: An open connection.
    :param table: The ``schema_migrations`` table.
    :param version: The migration version to record.
    """
    values = {"version": version, "applied_at": _now()}
    if conn.dialect.name == "postgresql":
        stmt = pg_insert(table).values(**values).on_conflict_do_nothing(
            index_elements=[table.c.version]
        )
    elif conn.dialect.name == "sqlite":
        stmt = sqlite_insert(table).values(**values).on_conflict_do_nothing(
            index_elements=[table.c.version]
        )
    else:
        # Unknown dialect: fall back to a non-atomic check-then-insert.
        exists = conn.execute(select(table.c.version).where(table.c.version == version)).first()
        if exists:
            return
        stmt = table.insert().values(**values)
    conn.execute(stmt)
