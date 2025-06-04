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

from typing import Any, Optional, List, Dict, TYPE_CHECKING, Union

from sqlalchemy import Engine
from sqlalchemy.orm import relationship

from autosubmit.database import session
from autosubmit.database.db_common import check_db_path, get_connection_url
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmit.database.db_manager import DbManager
from autosubmit.database.tables import ExperimentStructureTable
from autosubmit.database.tables import JobsTable
from autosubmit.job.job import Job
from log.log import Log


class JobsDbManager(DbManager):
    """A database manager for the job_list that extends DbManager using SQLAlchemy.

    It can be used with any engine supported by SQLAlchemy, such
    as Postgres, Mongo, MySQL, etc.
    """

    def __init__(self, connection_url: str, schema: Optional[str] = None) -> None:
        super().__init__(connection_url, schema)
        self.engine: Engine = session.create_engine(connection_url)
        self._ACTIVE_STATUSES = ['READY', 'SUBMITTED', 'QUEUING', 'HELD', 'RUNNING']
        self._FINAL_STATUSES = ['COMPLETED', 'FAILED']

    def save_jobs(self, job_list: List[Job]) -> None:
        """Save the job list to the database. Normally this will save the current active jobs."""
        self.create_table(JobsTable.name)
        persistent_data = [job.__getstate__(log_process=False) for job in job_list]
        # from pprint import pprint
        # pprint(persistent_data)  # TODO remove Debug

        pkeys = ['name']
        self.upsert_many(JobsTable.name, persistent_data, pkeys)

    def load_jobs(self, full_load) -> List[dict[str, Any]]:
        self.create_table(JobsTable.name)
        if full_load:
            job_list = self.select_all_jobs()
        else:
            job_list = self.select_active_jobs()
            job_list.extend(self.select_children_jobs(job_list))
            job_list = set(job_list)  # remove duplicates

        # return modificable list of dicts so it is easier to save later
        return [dict(job) for job in job_list]

    def select_all_jobs(self) -> List[dict[str, Any]]:
        """
        Return the whole job list from the database (without edges).
        """
        self.create_table(JobsTable.name)
        job_list = self.select_all_with_columns(JobsTable.name)
        return [dict(job) for job in job_list]

    def select_active_jobs(self) -> List[Union[str, Any]]:
        """
        Return the active jobs from the database (without edges).
        """
        self.create_table(JobsTable.name)

        job_list = []
        for status in self._ACTIVE_STATUSES:
            job_list.extend(self.select_where_with_columns(JobsTable.name, {"status": status}))

        return job_list

    def select_children_jobs(self, job_list: List[Union[str, Any]]) -> List[Union[str, Any]]:
        self.create_table(JobsTable.name)
        children_names = set()
        # make the select need the same structure as similar functions TODO discuss it or revise later
        job_list_tmp = [dict(job) for job in job_list]
        for job in job_list_tmp:
            child_rows = [dict(child) for child in self.select_where_with_columns(ExperimentStructureTable.name, {'e_from': job['name']})]
            for row in child_rows:
                children_names.add(row['e_to'])  # e_to

        for child_name in children_names:
            if not any(child_name == job.get("child_name") for job in job_list_tmp):
                matches = self.select_where_with_columns(JobsTable.name, {'name': child_name})
                if matches:
                    child = matches[0]
                    job_list.append(child)

        # return a hashable tuple to avoid duplicates
        return job_list

    def save_edges(self, graph: List[Dict[str, Any]]) -> None:
        """Save the experiment structure into the database."""
        self.create_table(ExperimentStructureTable.name)
        self.delete_all(ExperimentStructureTable.name)
        self.insert_many(ExperimentStructureTable.name, graph)

    def load_edges(self, job_list: List[dict[str, Any]], full_load: bool) -> List[dict[str, Any]]:
        self.create_table(ExperimentStructureTable.name)
        if full_load:
            graph = self.select_edges(job_list)
            self.delete_unused_edges(graph)
            self.save_edges(graph)
        else:
            graph = self.select_edges(job_list)
        return graph

    def select_edges(self, job_list: List[dict[str, Any]]) -> List[dict[str, Any]]:
        """
        Return the edges from the database.
        """
        self.create_table(ExperimentStructureTable.name)
        graph = set()
        for job in job_list:
            graph.update(self.select_where_with_columns(ExperimentStructureTable.name, {'e_from': job['name']}))
            graph.update(self.select_where_with_columns(ExperimentStructureTable.name, {'e_to': job['name']}))

        return [dict(edge) for edge in graph]

    def delete_unused_edges(self, graph: List[dict[str, Any]]) -> None:
        """
        Delete unused edges from the database.
        """
        self.create_table(ExperimentStructureTable.name)
        self.delete_all(ExperimentStructureTable.name)
        self.save_edges(graph)
