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

"""Per-tenant schema migration tracking.

This module is intentionally small and tool-agnostic. It keeps track of a schema
version per tenant (each experiment has its own SQLite file or PostgreSQL schema)
and exposes a hook for ordered migrations, so that a dedicated migration tool can
later replace what happens inside without callers noticing.

TODO(#1286): a migration tool should eventually take over this layer. The tool
is TBD. Alternatives to evaluate:
- Alembic: SQLAlchemy-native; autogenerates migration scripts by diffing the
  models against the DB, orders them, and handles SQLite ALTER via batch mode.
  Would need customization to fit the per-experiment files/schemas.
- Minimal custom runner (Cylc-style): ordered migration steps plus a version
  table. Zero dependencies and full control of the per-experiment targets, but
  migrations are written by hand (no autogeneration).
"""

import datetime
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING

from sqlalchemy import Column, Integer, MetaData, Table, Text, func, inspect, select
from sqlalchemy.schema import CreateTable

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection, Engine

__all__ = [
    "Migration",
    "apply_ordered_migrations",
    "ensure_schema_migrations_table",
    "get_schema_version",
    "record_migration",
    "schema_migrations_table",
]

SCHEMA_MIGRATIONS_TABLE_NAME = "schema_migrations"

# A migration step is a version and the callable that applies it.
Migration = tuple[int, Callable[["Connection"], None]]


def schema_migrations_table(
    metadata: MetaData, name: str = SCHEMA_MIGRATIONS_TABLE_NAME
) -> Table:
    """Build the table that records which migrations were applied.

    :param metadata: The metadata the table belongs to.
    :param name: The table name.
    :return: The ``schema_migrations`` table.
    """
    return Table(
        name,
        metadata,
        Column("version", Integer, primary_key=True, nullable=False),
        Column("applied_at", Text, nullable=False),
    )


def _now() -> str:
    """Return the current UTC timestamp used for ``applied_at``."""
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def ensure_schema_migrations_table(engine: "Engine", table: Table) -> None:
    """Create the ``schema_migrations`` table if it does not exist.

    :param engine: The engine that owns the target database.
    :param table: The ``schema_migrations`` table.
    """
    with engine.begin() as conn:
        conn.execute(CreateTable(table, if_not_exists=True))


def get_schema_version(engine: "Engine", table: Table, schema: str | None = None) -> int:
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


def record_migration(conn: "Connection", table: Table, version: int) -> None:
    """Record a migration as applied. Safe to call more than once.

    :param conn: An open connection.
    :param table: The ``schema_migrations`` table.
    :param version: The migration version to record.
    """
    exists = conn.execute(select(table.c.version).where(table.c.version == version)).first()
    if exists:
        return
    conn.execute(table.insert().values(version=version, applied_at=_now()))


def apply_ordered_migrations(
    engine: "Engine",
    table: Table,
    schema: str | None,
    migrations: Iterable[Migration],
    to_version: int,
) -> None:
    """Apply the pending migration steps in order, up to ``to_version``.

    :param engine: The engine that owns the target database.
    :param table: The ``schema_migrations`` table.
    :param schema: Optional schema name (PostgreSQL).
    :param migrations: The ordered migration steps.
    :param to_version: The version to migrate to.
    """
    current = get_schema_version(engine, table, schema)
    for version, migrate in sorted(migrations):
        if version <= current or version > to_version:
            continue
        with engine.begin() as conn:
            migrate(conn)
            record_migration(conn, table, version)
