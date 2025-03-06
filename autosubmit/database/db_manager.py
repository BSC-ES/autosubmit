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
import os
from pathlib import Path
from typing import Any, Optional, cast, List, Dict, Union

from sqlalchemy import Engine, delete, func, insert, select, ClauseElement, desc, asc
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.schema import CreateTable, CreateSchema, DropTable

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database import session
from autosubmit.database.tables import TableRegistry, GENERALTABLES, Table


class DbManager:
    """A database manager using SQLAlchemy.

    It can be used with any engine supported by SQLAlchemy, such
    as Postgres, Mongo, MySQL, etc.
    """

    def __init__(self, connection_url: str, schema: Optional[str] = None, historical: Optional[bool] = False) -> None:
        self.engine = None
        self.engine_historical = None
        if BasicConfig.DATABASE_BACKEND == "sqlite":
            if historical:
                self.engine_historical = session.create_engine(connection_url)
                if self.engine_historical.url.database and not Path(self.engine_historical.url.database).exists():
                    Path(self.engine_historical.url.database).touch()
                    Path(self.engine_historical.url.database).chmod(0o775)
            else:
                self.engine = session.create_engine(connection_url)
                # make file
                if self.engine.url.database and not Path(self.engine.url.database).exists():
                    Path(self.engine.url.database).touch()
                    Path(self.engine.url.database).chmod(0o775)
        else:
            # Postgres is unified
            self.engine: Engine = session.create_engine(connection_url)
            self.engine_historical = self.engine

        self.schema = schema if BasicConfig.DATABASE_BACKEND != "sqlite" else None
        self.restore_path = Path(BasicConfig.DB_PATH) / "autosubmit_db.sql"
        self.table_registry = TableRegistry(self.schema)

    def _get_engine(self, table_name: Optional[str] = None) -> Engine:
        """Return the appropriate engine based on context.

        :param table_name: If True, return the historical engine.
        :return: The selected SQLAlchemy engine.
        """
        if table_name and table_name in GENERALTABLES and self.engine_historical:
            return self.engine_historical
        return self.engine

    def create_table(self, table_name: str) -> None:
        table = self.table_registry.get(table_name)
        with self._get_engine(table_name).connect() as conn:
            with conn.begin():
                if self.schema:
                    conn.execute(CreateSchema(self.schema, if_not_exists=True))
                conn.execute(CreateTable(table, if_not_exists=True))

    def drop_table(self, table_name: str) -> None:
        table = self.table_registry.get(table_name)
        with self._get_engine(table_name).connect() as conn:
            with conn.begin():
                conn.execute(DropTable(table, if_exists=True))

    def insert(self, table_name: str, data: dict[str, Any]) -> None:
        if not data:
            return
        table = self.table_registry.get(table_name)
        with self._get_engine(table_name).connect() as conn:
            with conn.begin():
                conn.execute(insert(table), data)

    def insert_many(self, table_name: str, data: list[dict[str, Any]]) -> int:
        if not data:
            return 0
        table = self.table_registry.get(table_name)
        with self._get_engine(table_name).connect() as conn:
            with conn.begin():
                result = conn.execute(insert(table), data)
                return cast(int, result.rowcount)

    def select_first_where(self, table_name: str, where: Optional[dict[str, str]]) -> Optional[Any]:
        table = self.table_registry.get(table_name)
        query = select(table)
        if where:
            for key, value in where.items():
                query = query.where(getattr(table.c, key) == value)
        with self._get_engine(table_name).connect() as conn:
            row = conn.execute(query).first()
            return row.tuple() if row else None

    def select_all_with_columns(self, table_name: str) -> List[tuple[tuple[str, Any]]]:
        """Select rows from a table. Return a list of hasheable tuples."""
        table = self.table_registry.get(table_name)
        with self._get_engine(table_name).connect() as conn:
            rows = conn.execute(select(table)).fetchall()
            columns = table.c.keys()
            return [tuple(zip(columns, row)) for row in rows]

    def select_where_with_columns(
            self,
            table: "Table",
            where: Optional[Union[dict[str, Any], ClauseElement]] = None
    ) -> List[tuple[tuple[str, Any]]]:
        """Select rows from a table with specific columns. Return a list of hashable tuples.

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

        with self._get_engine(table.name).connect() as conn:
            rows = conn.execute(query).fetchall()

        return [tuple(zip(columns, row)) for row in rows]

    def count(self, table_name: str) -> int:
        table = self.table_registry.get(table_name)
        with self._get_engine(table_name).connect() as conn:
            row = conn.execute(select(func.count()).select_from(table))
            return cast(int, row.scalar())

    def delete_all(self, table_name: str) -> int:
        table = self.table_registry.get(table_name)
        with self._get_engine(table_name).connect() as conn:
            with conn.begin():
                result = conn.execute(delete(table))
                return result.rowcount

    def delete_where(self, table_name: str, where: Optional[Union[dict[str, Any], ClauseElement]]) -> int:
        """Delete rows from a table where the specified conditions are met.
        Supports both equality and 'IN' queries for list values.

        :param table_name: Name of the table to delete from.
        :type table_name: str
        :param where: Dictionary of column names and values (single value or list for IN).
        :type where: Dict[str, Any]
        :return: Number of rows deleted.
        :rtype: int
        :raises ValueError: If 'where' is empty.
        """
        table = self.table_registry.get(table_name)
        query = delete(table)

        if where:
            for key, value in where.items():
                column = getattr(table.c, key)
                if isinstance(value, list):
                    query = query.where(column.in_(value))
                else:
                    query = query.where(column == value)
        else:
            raise ValueError(
                "The 'where' parameter must be a non-empty dictionary. Multiple-table criteria within Delete are not supported.")

        with self._get_engine(table_name).connect() as conn:
            with conn.begin():
                result = conn.execute(query)
        return result.rowcount

    def upsert_many(self, table_name: str, data: List[Dict[str, Any]], conflict_cols: List[str], batch_size: int = 1000) -> int:
        """Perform an upsert (update or insert) operation.
        First delete the affected rows
        then insert the new data.

        :param table_name: Name of the table.
        :param data: List of dictionaries containing the data to upsert.
        :param conflict_cols: List of columns to check for conflicts. ( unique keys and primary keys )
        :return: Number of rows affected.
        :raises ValueError: If data is empty or unsupported dialect.
        """
        if not data:
            return 0

        table: Table = self.table_registry.get(table_name)
        update_cols = [col for col in data[0].keys() if col not in conflict_cols]

        # NOTE general insert doesn't have on_conflict
        if self._get_engine(table_name).dialect.name == "postgresql":
            insert_stmt = pg_insert(table)
        elif self._get_engine(table_name).dialect.name == "sqlite":
            insert_stmt = sqlite_insert(table)
        else:
            raise ValueError(f"Unsupported dialect: {self._get_engine(table_name).dialect.name}")

        # add on_conflict clause
        update_stmt = insert_stmt.on_conflict_do_update(
            index_elements=conflict_cols,
            set_={col: getattr(insert_stmt.excluded, col) for col in update_cols}
        )

        total_rows = 0
        with self._get_engine(table_name).connect() as conn:
            with conn.begin():
                for i in range(0, len(data), batch_size):
                    batch = data[i:i + batch_size]
                    result = conn.execute(update_stmt, batch)
                    total_rows += result.rowcount

        return total_rows

    def count_where(self, table_name: str, where: dict[str, Any]) -> int:
        """Count the number of rows in a table that match a given condition."""
        table = self.table_registry.get(table_name)
        query = select(func.count()).select_from(table)
        for key, value in where.items():
            query = query.where(getattr(table.c, key) == value)
        with self._get_engine(table_name).connect() as conn:
            row = conn.execute(query).scalar()
        return cast(int, row) if row is not None else 0

    # TODO: Don't mind this function, is half-cook will be done in another PR or maybe not needed at all
    def backup(self):
        """Create a backup of the database."""
        if BasicConfig.DATABASE_BACKEND == "sqlite":
            import sqlite3  # Bulk operation , SQLACHEMY is too slow for these operations
            if not self.restore_path.parent.exists():
                self.restore_path.parent.mkdir(parents=True, exist_ok=True)
                self.restore_path.parent.chmod(0o770)

            if self.restore_path.exists():
                os.remove(self.restore_path)

            con = self._get_engine().raw_connection()
            bck = sqlite3.connect(self.restore_path)
            with bck:
                con.connection.backup(bck)
            self.restore_path.chmod(0o660)
            bck.close()
            con.close()
        elif BasicConfig.DATABASE_BACKEND == "postgres":
            raise NotImplementedError("Postgres backup not implemented yet.")
            # import subprocess
            # if not self.restore_path.parent.exists():
            #     self.restore_path.parent.mkdir(parents=True, exist_ok=True)
            #     self.restore_path.parent.chmod(0o770)
            # if self.restore_path.exists():
            #     os.remove(self.restore_path)
            # try:
            #     # use pg_dump
            #     db_url = self._get_engine(table_name).url
            #     cmd = f"cd {str(self.restore_path.parent)}; pg_dump --host {db_url.host} --port {db_url.port} --username {db_url.username} --dbname {db_url.database} --format=c --file={self.restore_path}"
            #     env = os.environ.copy()
            #     env["PGPASSWORD"] = db_url.password
            #     subprocess.check_output(cmd, shell=True, env=env, stderr=subprocess.STDOUT)
            #     self.restore_path.chmod(0o660)
            # except subprocess.CalledProcessError as e:
            #     Log.warning(f"Error backing up database: {e.output.decode()}")

    def restore(self) -> int:
        if BasicConfig.DATABASE_BACKEND == "sqlite":
            import sqlite3  # Bulk operation , SQLACHEMY is too slow for these operations
            if not self.restore_path.exists():
                raise FileNotFoundError(f"Backup file {self.restore_path} does not exist.")
            con = self._get_engine().raw_connection()
            bck = sqlite3.connect(self.restore_path)
            bck.backup(con.connection)
            bck.close()
            con.close()
        elif BasicConfig.DATABASE_BACKEND == "postgres":
            raise NotImplementedError("Postgres restore not implemented yet.")
            # import subprocess
            # if not self.restore_path.exists():
            #     raise FileNotFoundError(f"Backup file {self.restore_path} does not exist.")
            # try:
            #     db_url = self._get_engine(table_name).url
            #     cmd = (
            #         f"cd {str(self.restore_path.parent)}; "
            #         f"pg_restore --host {db_url.host} --port {db_url.port} "
            #         f"--username {db_url.username} --dbname {db_url.database} "
            #         f"--clean --if-exists --jobs=4 --format=c {self.restore_path}"
            #     )
            #     env = os.environ.copy()
            #     env["PGPASSWORD"] = db_url.password
            #     subprocess.run(cmd, shell=True, check=True, env=env)
            #     self.restore_path.chmod(0o660)
            # except subprocess.CalledProcessError as e:
            #     Log.warning(f"Error restoring database: {e}")
            #     return 1
        return 0

    def update_where(self, table_name: str, values: dict[str, Any], where: dict[str, Any]) -> int:
        """Update rows in a table where conditions are met.

        Supports both equality and IN queries for list values.

        :param table_name: Name of the table to update.
        :param values: Dictionary of column names and new values to set.
        :param where: Dictionary of column names and values (single value or list for IN).
        :return: Number of rows updated.
        :raises ValueError: If 'where' is empty.
        """
        table = self.table_registry.get(table_name)
        query = table.update().values(**values)

        for key, value in where.items():
            column = getattr(table.c, key)
            if isinstance(value, list):
                query = query.where(column.in_(value))
            else:
                query = query.where(column == value)

        with self._get_engine(table_name).connect() as conn:
            with conn.begin():
                result = conn.execute(query)

        return result.rowcount

    def select_latest_inner_jobs(
            self,
            innerjobs_table: Table,
            job_names: Optional[List[str]] = None
    ) -> List[Dict[str, object]]:
        """
        Select the row with the latest timestamp for each job_name from the inner jobs table.
        If job_names is provided, filter only those job_names.

        :param innerjobs_table: SQLAlchemy Table object for the inner jobs.
        :type innerjobs_table: Table
        :param job_names: Optional list of job_name values to filter by.
        :type job_names: Optional[List[str]]
        :return: List of dictionaries with the latest row per job_name.
        :rtype: List[Dict[str, object]]
        """
        row_number = func.row_number().over(
            partition_by=innerjobs_table.c.job_name,
            order_by=desc(innerjobs_table.c.timestamp)
        ).label('row_number')

        stmt = select(*innerjobs_table.c, row_number)
        if job_names:
            stmt = stmt.where(innerjobs_table.c.job_name.in_(job_names))
        subquery = stmt.alias('subq')
        query = select(*(col for col in subquery.c if col.name != 'row_number')).where(subquery.c.row_number == 1)
        with self._get_engine(innerjobs_table.name).connect() as conn:
            result = conn.execute(query)
            return [dict(row) for row in result.mappings().all()]

    def select_last_with_columns(self, table_name: str, columns: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        """Return the latest row from a table ordered by descending update time.

        :param table_name: Name of the table to select from.
        :param columns: Optional list of column names to include. If None, all columns are included.
        :return: Dictionary representing the latest row, or None if the table is empty.
        """
        table: Table = self.table_registry.get(table_name)
        self.create_table(table.name)

        col_keys = columns if columns is not None else list(table.c.keys())
        selected_cols = [table.c[col] for col in col_keys]

        stmt = select(*selected_cols).order_by(desc(table.c.modified)).limit(1)
        with self._get_engine(table_name).connect() as conn:
            row = conn.execute(stmt).fetchone()

        return dict(zip(col_keys, row)) if row else None
