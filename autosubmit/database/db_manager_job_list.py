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
from pathlib import Path
from typing import Any, Optional, List, Dict, TYPE_CHECKING, Union, Tuple

from sqlalchemy import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import relationship

from autosubmit.database import session
from autosubmit.database.db_common import check_db_path, get_connection_url
from autosubmit.job.job_list import JobList
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmit.database.db_manager import DbManager
from autosubmit.database.tables import ExperimentStructureTable, PreviewWrapperJobsTable, WrapperJobsTable, \
    PreviewWrapperInfoTable, WrapperInfoTable
from autosubmit.database.tables import JobsTable
from autosubmit.job.job import Job, WrapperJob
from log.log import Log


class JobsDbManager(DbManager):
    """A database manager for the job_list that extends DbManager using SQLAlchemy.

    It can be used with any engine supported by SQLAlchemy, such
    as Postgres, Mongo, MySQL, etc.
    """

    def __init__(self, connection_url: str, schema: Optional[str] = None) -> None:
        super().__init__(connection_url, schema)
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

    def get_job_list_size(self) -> Tuple[int, int, int]:
        """
        Return the number of jobs in the database.
        """
        self.create_table(JobsTable.name)
        job_list_size = self.count(JobsTable.name)
        complete_job_list_size = self.count_where(JobsTable.name, {'status': "COMPLETED"})
        failed_job_list_size = self.count_where(JobsTable.name, {'status': "FAILED"})
        return job_list_size, complete_job_list_size, failed_job_list_size

    def select_all_jobs(self) -> List[dict[str, Any]]:
        """
        Return the whole job list from the database (without edges).
        """
        self.create_table(JobsTable.name)
        job_list = self.select_all_with_columns(JobsTable.name)
        return [dict(job) for job in job_list]

    def select_jobs_by_section(self, section: str) -> List[dict[str, Any]]:
        """
        Return the jobs from the database that belong to a specific section.
        """
        self.create_table(JobsTable.name)
        job_list = self.select_where_with_columns(JobsTable.name, {'section': section})
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
            child_rows = [dict(child) for child in
                          self.select_where_with_columns(ExperimentStructureTable.name, {'e_from': job['name']})]
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
        pkeys = ['e_from', 'e_to']
        self.upsert_many(ExperimentStructureTable.name, graph, pkeys)

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

    # Once wrappers are built, they are saved in the database.
    def select_inner_jobs(self, job_list: List[Union[str, Any]], preview: bool = False) -> List[Union[str, Any]]:
        """
        Select inner jobs from the database based on the provided loaded job list.
        This function retrieves jobs that form part of the same package.
        param job_list: List of jobs to find inner jobs for.
        param preview: If True, use the preview tables; otherwise, use the main tables.
        return: List of inner jobs that are children of the provided job list.
        :rtype: List[dict[str, Any]]
        """

        jobs_table = JobsTable
        if preview:
            innerjobs_table = PreviewWrapperJobsTable
        else:
            innerjobs_table = WrapperJobsTable
        self.create_table(innerjobs_table.name)
        self.create_table(jobs_table.name)

        packages_names = set()
        job_list_tmp = [dict(job) for job in job_list]

        # find package_name
        for job in job_list_tmp:
            where_query = {'job_name': job['name']}
            row = self.select_where_with_columns(innerjobs_table.name, where_query)
            # from row, obtain the package_name
            if row:
                packages_names.add(row['package_name'])

        # load all inner jobs that match the package_name
        for package_name in packages_names:
            matches = self.select_where_with_columns(jobs_table.name, {'package_name': package_name})
            for match in matches:
                # check if the job is already in the job_list
                if not any(match['name'] == job.get("name", None) for job in job_list_tmp):
                    job_list.append(matches[0])

        # return a hashable tuple to avoid duplicates
        return job_list

    def select_job_by_name(self, job_name: str) -> dict[str, Any]:
        """
        Select a job by its name from the database.
        :param job_name: Name of the job to select.
        :type job_name: str
        :return: List of dictionaries containing the job information.
        """

        self.create_table(JobsTable.name)
        job = self.select_where_with_columns(JobsTable.name, {'name': job_name})
        if job:
            return job[0]

    # WRAPPERS
    # At this point, we already built the wrappers, so we can save them in the database.
    def save_wrappers(
            self,
            wrappers: Tuple[List[Dict[str, Any]], List[Dict[str, Any]]],
            preview: bool = False
    ) -> None:
        """
        Save the wrapper jobs and their associated information to the database.

        :param wrappers: List of dictionaries containing wrapper job data and package info.
        :type wrappers: Tuple[Dict[str, Any], List[Dict[str, Any]]]
        :param preview: If True, use preview tables; otherwise, use production tables.
        :type preview: bool
        """
        if preview:
            innerjobs_table = PreviewWrapperJobsTable
            wrapper_info_table = PreviewWrapperInfoTable
        else:
            innerjobs_table = WrapperJobsTable
            wrapper_info_table = WrapperInfoTable
        self.create_table(innerjobs_table.name)
        self.create_table(wrapper_info_table.name)
        for wrapper_info, inner_jobs in wrappers:
            self.upsert_many(wrapper_info_table.name, wrapper_info, ['name'])
            try:
                self.insert_many(innerjobs_table.name, inner_jobs)
            except IntegrityError as e:
                Log.warning(f"Unique constraint failed when inserting inner jobs: {e}")

    def load_wrappers(self, preview: bool = False, job_list: JobList = None) -> Tuple[
        List[dict[str, Any]], List[dict[str, Any]]]:
        """
        Load the wrapper jobs and their associated information from the database.

        :param preview: If True, use preview tables; otherwise, use production tables.
        :type preview: bool
        job_list: Optional list of jobs to filter the loaded wrappers.
        :param job_list: Optional list of jobs to filter the loaded wrappers.
        :return: Tuple containing a list of dictionaries with wrapper job info and inner jobs.
        :rtype: Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]

        """
        if preview or job_list:
            full_load = True
        else:
            full_load = False

        if preview:
            innerjobs_table = PreviewWrapperJobsTable
            wrapper_info_table = PreviewWrapperInfoTable
        else:
            innerjobs_table = WrapperJobsTable
            wrapper_info_table = WrapperInfoTable

        self.create_table(innerjobs_table.name)
        self.create_table(wrapper_info_table.name)
        if full_load:
            # Load wrapper jobs
            wrappers_inner_jobs = self.select_all_with_columns(innerjobs_table.name)
            wrappers_info = self.select_all_with_columns(wrapper_info_table.name)
        else:
            # Load only active wrapper jobs
            job_names = [job.name for job in job_list] if job_list else []
            wrappers_inner_jobs = self.select_where_with_columns(innerjobs_table.name, {'job_name': job_names})
            packages_names = list(set([job['package_name'] for job in wrappers_inner_jobs]))
            wrappers_info = self.select_where_with_columns(wrapper_info_table.name, {'name': packages_names})

        # map package_name with job_name and add job_list [ ]
        for wrapper_info in wrappers_info:
            wrapper_info['job_list'] = []
            for inner_job in wrappers_inner_jobs:
                if inner_job['package_name'] == wrapper_info['name']:
                    wrapper_info['job_list'].append(inner_job)

        return wrappers_info, wrappers_inner_jobs
