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

"""Unit tests for ``autosubmit.database.migrations``."""

from pathlib import Path

import pytest
from sqlalchemy import MetaData, create_engine, select

from autosubmit.database.migrations import (
    apply_ordered_migrations,
    ensure_schema_migrations_table,
    get_schema_version,
    record_migration,
    schema_migrations_table,
)

TABLE_NAME = "schema_migrations"


def _make_engine(tmp_path: Path, name: str):
    return create_engine(f"sqlite:///{tmp_path / name}")


def test_schema_migrations_table_shape() -> None:
    """The helper builds the expected schema_migrations table."""
    table = schema_migrations_table(MetaData(), TABLE_NAME)

    assert table.name == TABLE_NAME
    assert {column.name for column in table.columns} == {"version", "applied_at"}
    assert table.c.version.primary_key is True


def test_get_schema_version_returns_zero_without_table(tmp_path: Path) -> None:
    """A target without the migrations table reports version 0."""
    engine = _make_engine(tmp_path, "a.db")
    table = schema_migrations_table(MetaData(), TABLE_NAME)

    assert get_schema_version(engine, table) == 0


def test_record_migration_is_idempotent(tmp_path: Path) -> None:
    """Recording the same version twice keeps a single row."""
    engine = _make_engine(tmp_path, "a.db")
    table = schema_migrations_table(MetaData(), TABLE_NAME)
    ensure_schema_migrations_table(engine, table)

    with engine.begin() as conn:
        record_migration(conn, table, 1)
        record_migration(conn, table, 1)

    assert get_schema_version(engine, table) == 1
    with engine.connect() as conn:
        rows = conn.execute(select(table.c.version)).fetchall()
    assert rows == [(1,)]


def test_apply_ordered_migrations_applies_pending_in_order(tmp_path: Path) -> None:
    """Pending migrations run in ascending order and are recorded."""
    engine = _make_engine(tmp_path, "a.db")
    table = schema_migrations_table(MetaData(), TABLE_NAME)
    ensure_schema_migrations_table(engine, table)
    executed: list[int] = []

    def step(version: int):
        def _run(conn) -> None:
            executed.append(version)
        return _run

    migrations = [(3, step(3)), (1, step(1)), (2, step(2))]

    apply_ordered_migrations(engine, table, None, migrations, to_version=2)
    assert executed == [1, 2]
    assert get_schema_version(engine, table) == 2

    apply_ordered_migrations(engine, table, None, migrations, to_version=3)
    assert executed == [1, 2, 3]
    assert get_schema_version(engine, table) == 3


@pytest.mark.parametrize("recorded, expected", [(5, 5), (21, 21)])
def test_schema_version_is_isolated_per_tenant(tmp_path: Path, recorded: int, expected: int) -> None:
    """Each tenant keeps its own schema version."""
    table = schema_migrations_table(MetaData(), TABLE_NAME)
    first = _make_engine(tmp_path, "first.db")
    second = _make_engine(tmp_path, "second.db")
    ensure_schema_migrations_table(first, table)
    ensure_schema_migrations_table(second, table)

    with first.begin() as conn:
        record_migration(conn, table, recorded)

    assert get_schema_version(first, table) == expected
    assert get_schema_version(second, table) == 0
