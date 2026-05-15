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

"""Database layer for the Job packages."""

import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator, List, Optional

from sqlalchemy import Table, delete, func, insert, select, text
from sqlalchemy.exc import IntegrityError

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database.db_common import get_connection_url
from autosubmit.database.db_manager import DbManager
from autosubmit.database.tables import (
    WrapperInfoTable,
    PreviewWrapperInfoTable,
    WrapperJobsTable,
    PreviewWrapperJobsTable,
    TableRegistry,
)
from autosubmit.job.job_common import Status
from autosubmit.log.log import Log


class JobPackagePersistence:
    """Class that handles packages workflow.

    Create Packages Table, Wrappers Table.
    """

    VERSION = 2

    def __init__(self, expid: str):
        database_file = Path(BasicConfig.LOCAL_ROOT_DIR, expid, 'pkl', f'job_packages_{expid}.db')
        connection_url = get_connection_url(db_path=database_file)

        if BasicConfig.DATABASE_BACKEND == "postgres":
            _schema = expid
        else:
            _schema = None

        self.db_manager = DbManager(connection_url=connection_url, schema=_schema)
        self.table_registry = TableRegistry(schema=_schema)
        self.db_manager.create_table(WrapperInfoTable.name)
        self.db_manager.create_table(PreviewWrapperInfoTable.name)
        self.db_manager.create_table(WrapperJobsTable.name)
        self.db_manager.create_table(PreviewWrapperJobsTable.name)

    def load(
            self, preview: bool = False, job_list: Optional[Any] = None
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Load the wrapper jobs and their associated information from the database.

        :param preview: If True, load all wrappers from preview tables; otherwise load
            only active wrappers from production tables filtered by ``job_list``.
        :param job_list: Optional list of job objects used to filter inner jobs when
            ``preview`` is False. Each object must expose a ``name`` attribute.
        :return: Tuple of (wrapper_info_rows, inner_job_rows), each as a list of dicts.
        """
        if preview:
            innerjobs_table = self.table_registry.get(PreviewWrapperJobsTable.name)
            wrapper_info_table = self.table_registry.get(PreviewWrapperInfoTable.name)
        else:
            innerjobs_table = self.table_registry.get(WrapperJobsTable.name)
            wrapper_info_table = self.table_registry.get(WrapperInfoTable.name)

        self.db_manager.create_table(innerjobs_table.name)
        self.db_manager.create_table(wrapper_info_table.name)

        if preview:
            wrappers_inner_jobs = self._select_latest_inner_jobs(innerjobs_table)
            wrappers_info = self._select_all_as_dicts(wrapper_info_table)
        else:
            job_names = [job.name for job in job_list] if job_list else []
            wrappers_inner_jobs = self._select_latest_inner_jobs(innerjobs_table, job_names)
            packages_names = list({job['package_name'] for job in wrappers_inner_jobs})
            wrappers_info = self._select_where_in_as_dicts(wrapper_info_table, 'name', packages_names)

        for i, wrapper in enumerate(wrappers_info):
            wrapper['status'] = Status.KEY_TO_VALUE[wrapper['status']]
            wrappers_info[i] = tuple(wrapper.items())
        return wrappers_info, wrappers_inner_jobs

    def save(
            self,
            wrappers: tuple[List[dict[str, Any]], List[dict[str, Any]]],
            preview: bool = False,
    ) -> None:
        """Save wrapper jobs and their associated information to the database.

        :param wrappers: Iterable of (wrapper_info, inner_jobs) pairs where
            ``wrapper_info`` is a dict or list of dicts, and ``inner_jobs`` is a
            list of dicts describing the jobs inside the wrapper.
        :param preview: If True, persist to preview tables; otherwise to production tables.
        """
        if preview:
            innerjobs_table = self.table_registry.get(PreviewWrapperJobsTable.name)
            wrapper_info_table = self.table_registry.get(PreviewWrapperInfoTable.name)
        else:
            innerjobs_table = self.table_registry.get(WrapperJobsTable.name)
            wrapper_info_table = self.table_registry.get(WrapperInfoTable.name)

        self.db_manager.create_table(innerjobs_table.name)
        self.db_manager.create_table(wrapper_info_table.name)

        for wrapper_info, inner_jobs in wrappers:
            if isinstance(wrapper_info, list):
                updated_wrappers = [
                    {**w, 'status': Status.VALUE_TO_KEY[int(w['status'])]}
                    for w in wrapper_info
                ]
            else:
                updated_wrappers = [
                    {**wrapper_info, 'status': Status.VALUE_TO_KEY[int(wrapper_info['status'])]}
                ]
            self._upsert_wrapper_info(wrapper_info_table, updated_wrappers)
            try:
                self.db_manager.insert_many(innerjobs_table.name, inner_jobs)
            except IntegrityError as e:
                Log.warning(f"Unique constraint failed when inserting inner jobs: {e}")

    @contextmanager
    def _names_tmp_table(
            self, names: list[str], column: str = "name"
    ) -> Generator[tuple[Any, Any], None, None]:
        """Context manager that populates a temporary table with string values.


        :param names: String values to load into the temporary table.
        :param column: Column name to use in the temporary table.
        :yields: ``(conn, tmp)`` — the connection and the temp table expression.
        """
        # Local import avoids shadowing any `table` / `column` parameter names.
        from sqlalchemy import column as _col, table as _tbl

        table_name = f"_tmp_names_{time.time_ns()}"
        tmp = _tbl(table_name, _col(column))
        with self.db_manager.engine.connect() as conn:
            conn.execute(text(
                f"CREATE TEMPORARY TABLE IF NOT EXISTS {table_name} ({column} TEXT)"
            ))
            conn.execute(text(f"DELETE FROM {table_name}"))
            conn.execute(
                text(f"INSERT INTO {table_name} ({column}) VALUES (:value)"),
                [{"value": n} for n in names],
            )
            try:
                yield conn, tmp
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                conn.commit()

    def _select_all_as_dicts(self, table: Table) -> list[dict[str, Any]]:
        """Return all rows of ``table`` as a list of dicts."""
        col_names = [col.name for col in table.columns]
        with self.db_manager.engine.connect() as conn:
            rows = conn.execute(select(table)).all()
        return [dict(zip(col_names, row)) for row in rows]

    def _select_where_in_as_dicts(
            self, table: Table, column: str, values: list
    ) -> list[dict[str, Any]]:
        """Return rows of ``table`` where ``column`` matches any value in ``values``.

        A temporary table JOIN is used instead of ``IN (…)`` to avoid SQLite's
        bind-variable limit.

        :param table: The SQLAlchemy Table to query.
        :param column: The column name to filter on.
        :param values: The list of acceptable values for ``column``.
        :return: Matching rows as a list of dicts.
        """
        if not values:
            return []
        col_names = [col.name for col in table.columns]
        with self._names_tmp_table(values, column=column) as (conn, tmp):
            rows = conn.execute(
                select(table).join(tmp, getattr(table.c, column) == getattr(tmp.c, column))
            ).all()
        return [dict(zip(col_names, row)) for row in rows]

    def _select_latest_inner_jobs(
            self, table: Table, job_names: Optional[list[str]] = None
    ) -> list[dict[str, Any]]:
        """Return the latest inner-job rows keyed by the highest ``package_id`` per job.

        :param table: The inner-jobs SQLAlchemy Table to query.
        :param job_names: Optional list of job names to restrict the query.
        :return: Latest inner-job rows as a list of dicts.
        """
        col_names = [col.name for col in table.columns]
        subquery = (
            select(table.c.job_name, func.max(table.c.package_id).label('max_id'))
            .group_by(table.c.job_name)
            .subquery()
        )
        base_query = select(table).join(
            subquery,
            (table.c.job_name == subquery.c.job_name)
            & (table.c.package_id == subquery.c.max_id),
        )
        if job_names:
            with self._names_tmp_table(job_names, column="job_name") as (conn, tmp):
                rows = conn.execute(
                    base_query.join(tmp, table.c.job_name == tmp.c.job_name)
                ).all()
        else:
            with self.db_manager.engine.connect() as conn:
                rows = conn.execute(base_query).all()
        return [dict(zip(col_names, row)) for row in rows]

    def _upsert_wrapper_info(
            self, table: Table, records: list[dict[str, Any]]
    ) -> None:
        """Upsert wrapper-info records using a delete-then-insert pattern.

        :param table: The wrapper-info SQLAlchemy Table.
        :param records: List of dicts to upsert; each must contain a ``name`` key.
        """
        if not records:
            return
        with self.db_manager.engine.connect() as conn:
            for record in records:
                conn.execute(delete(table).where(table.c.name == record['name']))
                conn.execute(insert(table).values(**record))
            conn.commit()

    def reset_table(self, preview=False):
        """Drops and recreates the database."""

        self.db_manager.drop_table(PreviewWrapperJobsTable.name)
        self.db_manager.create_table(PreviewWrapperJobsTable.name)
        if not preview:
            self.db_manager.drop_table(WrapperInfoTable.name)
            self.db_manager.drop_table(WrapperJobsTable.name)
