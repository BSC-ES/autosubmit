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

from typing import Any, Optional, cast

from sqlalchemy import Engine, delete, func, insert, select
from sqlalchemy.schema import CreateTable, CreateSchema, DropTable

from autosubmit.database import session
from autosubmit.database.tables import get_table_from_name


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

    def delete_where(self, table_name: str, where: dict[str, Any]) -> int:
        if not where:
            raise ValueError(f'You must specify a where when deleting from table " {table_name}"')

        table = get_table_from_name(schema=self.schema, table_name=table_name)
        query = delete(table)
        for key, value in where.items():
            query = query.where(getattr(table.c, key) == value)  # type: ignore

        with self.engine.connect() as conn:
            result = conn.execute(query)
            conn.commit()
        return cast(int, result.rowcount)
