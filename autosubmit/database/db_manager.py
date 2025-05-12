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

"""Contains code to manage a database via SQLAlchemy."""

from typing import Any, Optional, cast, TYPE_CHECKING, List, Dict

from sqlalchemy import Engine, delete, func, insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.schema import CreateTable, CreateSchema, DropTable

from autosubmit.database import session
from autosubmit.database.tables import get_table_from_name

if TYPE_CHECKING:
    from autosubmit.database.tables import Table


class DbManager:
    """A database manager using SQLAlchemy.

    It can be used with any engine supported by SQLAlchemy, such
    as Postgres, Mongo, MySQL, etc.
    """

    def __init__(self, connection_url: str, schema: Optional[str] = None) -> None:
        self.engine: Engine = session.create_engine(connection_url)
        self.schema = schema

    def create_table(self, table_name: str) -> None:
        table = get_table_from_name(schema=self.schema, table_name=table_name)
        with self.engine.connect() as conn:
            if self.schema:
                conn.execute(CreateSchema(self.schema, if_not_exists=True))
            conn.execute(CreateTable(table, if_not_exists=True))
            conn.commit()

    def drop_table(self, table_name: str) -> None:
        table = get_table_from_name(schema=self.schema, table_name=table_name)
        with self.engine.connect() as conn:
            conn.execute(DropTable(table, if_exists=True))
            conn.commit()

    def insert(self, table_name: str, data: dict[str, Any]) -> None:
        if not data:
            return
        table = get_table_from_name(schema=self.schema, table_name=table_name)
        with self.engine.connect() as conn:
            conn.execute(insert(table), data)
            conn.commit()

    def insert_many(self, table_name: str, data: list[dict[str, Any]]) -> int:
        if not data:
            return 0
        table = get_table_from_name(schema=self.schema, table_name=table_name)
        with self.engine.connect() as conn:
            result = conn.execute(insert(table), data)
            conn.commit()
        return cast(int, result.rowcount)

    def select_first_where(self, table_name: str, where: Optional[dict[str, str]]) -> Optional[Any]:
        table = get_table_from_name(schema=self.schema, table_name=table_name)
        query = select(table)
        if where:
            for key, value in where.items():
                query = query.where(getattr(table.c, key) == value)
        with self.engine.connect() as conn:
            row = conn.execute(query).first()
        return row.tuple() if row else None

    def select_all(self, table_name: str) -> list[Any]:
        table = get_table_from_name(schema=self.schema, table_name=table_name)
        with self.engine.connect() as conn:
            rows = conn.execute(select(table)).all()
        return [row.tuple() for row in rows]

    def select_all_with_columns(self, table_name: str) -> list[tuple[str, Any]]:
        """Select rows from a table. Return a list of hasheable tuples."""
        table = get_table_from_name(schema=self.schema, table_name=table_name)
        with self.engine.connect() as conn:
            rows = conn.execute(select(table)).fetchall()
        columns = table.c.keys()
        return [tuple(zip(columns, row)) for row in rows]

    def select_where(self, table_name: str, where: dict[str, Any]) -> list[Any]:
        table = get_table_from_name(schema=self.schema, table_name=table_name)
        query = select(table)
        for key, value in where.items():
            query = query.where(getattr(table.c, key) == value)
        with self.engine.connect() as conn:
            rows = conn.execute(query).all()
        return [row.tuple() for row in rows]

    def select_where_with_columns(
            self,
            table,
            where
    ) -> List[tuple[str, Any]]:
        """
        Select rows from a table with specific columns. Return a list of hashable tuples.

        :param table: Table object or table name to select from.
        :type table: Table
        :param where: Dictionary of column:value pairs to filter by, or a SQLAlchemy clause.
        :type where: Dict[str, Any] | ClauseElement
        :return: List of tuples containing column-value pairs.
        :rtype: List[tuple[str, Any]]
        """
        self.create_table(table.name)  # Ensure the table exists

        query = select(table)
        columns = table.c.keys()

        if isinstance(where, dict):
            for key, value in where.items():
                if key in columns:
                    column = getattr(table.c, key)
                    if isinstance(value, list):
                        query = query.where(column.in_(value))
                    else:
                        query = query.where(column == value)
        else:
            query = query.where(where)

        with self.engine.connect() as conn:
            rows = conn.execute(query).fetchall()

        return [tuple(zip(columns, row)) for row in rows]

    def count(self, table_name: str) -> int:
        table = get_table_from_name(schema=self.schema, table_name=table_name)
        with self.engine.connect() as conn:
            row = conn.execute(select(func.count()).select_from(table))
            return row.scalar()

    def delete_all(self, table_name: str) -> int:
        table = get_table_from_name(schema=self.schema, table_name=table_name)
        with self.engine.connect() as conn:
            result = conn.execute(delete(table))
            conn.commit()
        return cast(int, result.rowcount)

    def delete_where(self, table_name: str, where: Dict[str, Any]) -> int:
        """
        Delete rows from a table where the specified conditions are met.
        Supports both equality and 'IN' queries for list values.

        :param table_name: Name of the table to delete from.
        :type table_name: str
        :param where: Dictionary of column names and values (single value or list for IN).
        :type where: Dict[str, Any]
        :return: Number of rows deleted.
        :rtype: int
        :raises ValueError: If 'where' is empty.
        """
        if not where:
            raise ValueError(f'You must specify a where when deleting from table "{table_name}"')

        table = get_table_from_name(schema=self.schema, table_name=table_name)
        query = delete(table)
        for key, value in where.items():
            column = getattr(table.c, key)
            if isinstance(value, list):
                query = query.where(column.in_(value))
            else:
                query = query.where(column == value)

        with self.engine.connect() as conn:
            result = conn.execute(query)
            conn.commit()
        return cast(int, result.rowcount)

    def upsert_many(self, table_name: str, data: List[Dict[str, Any]], conflict_cols: List[str]) -> int:
        """
        Perform an upsert (update or insert) operation.
        First delete the affected rows
        then insert the new data.

        :param table_name: Name of the table.
        :param data: List of dictionaries containing the data to upsert.
        :param conflict_cols: List of columns to check for conflicts. ( unique keys and primary keys )
        :return: Number of rows affected.
        """
        if not data:
            return 0

        table: Table = get_table_from_name(schema=self.schema, table_name=table_name)

        # NOTE general insert doesn't have on_conflict
        if self.engine.dialect.name == "postgresql":
            insert_stmt = pg_insert(table).values(data)
        elif self.engine.dialect.name == "sqlite":
            insert_stmt = sqlite_insert(table).values(data)
        else:
            raise ValueError(f"Unsupported dialect: {self.engine.dialect.name}")

        # add on_conflict clause
        update_stmt = insert_stmt.on_conflict_do_update(
            index_elements=conflict_cols,
            set_={c.name: c for c in insert_stmt.excluded if c.name not in conflict_cols},
        )

        with self.engine.connect() as conn:
            result = conn.execute(update_stmt)
            conn.commit()

        # Return the number of rows affected
        return cast(int, result.rowcount)

    def count_where(self, table_name: str, where: dict[str, Any]) -> int:
        """Count the number of rows in a table that match a given condition."""
        table = get_table_from_name(schema=self.schema, table_name=table_name)
        query = select(func.count()).select_from(table)
        for key, value in where.items():
            query = query.where(getattr(table.c, key) == value)
        with self.engine.connect() as conn:
            row = conn.execute(query).scalar()
        return cast(int, row) if row is not None else 0

    def reset_table(self, table_name: str) -> None:
        """
        Drop and recreate a table in the database.

        :param table_name: Name of the table to reset.
        :type table_name: str
        """
        self.drop_table(table_name)
        self.create_table(table_name)
