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

from docutils.nodes import section
from sqlalchemy import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import relationship

from autosubmit.database import session
from autosubmit.database.db_common import check_db_path, get_connection_url
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmit.database.db_manager import DbManager
from autosubmit.database.tables import ExperimentStructureTable, PreviewWrapperJobsTable, WrapperJobsTable, \
    PreviewWrapperInfoTable, WrapperInfoTable, SectionsStructureTable
from autosubmit.database.tables import JobsTable
from autosubmit.job.job import Job, WrapperJob
from log.log import Log
from sqlalchemy import and_, or_

if TYPE_CHECKING:
    from autosubmit.job.job_list import JobList


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

    def load_jobs(self, full_load, load_failed_jobs: bool = False, members: List = None) -> List[dict[str, Any]]:
        self.create_table(JobsTable.name)
        if full_load:
            job_list = self.select_all_jobs()
        else:
            job_list = self.select_active_jobs(include_failed=load_failed_jobs, members=members)
            job_list.extend(self.select_children_jobs(job_list, members=members))
            job_list = set(job_list)  # remove duplicates

        # return modificable list of dicts so it is easier to save later
        return [dict(job) for job in job_list]

    def load_job_by_name(self, job_name: str) -> dict[str, Any]:
        """
        Load a job by its name from the database.
        :param job_name: Name of the job to load.
        :type job_name: str
        :return: Dictionary containing the job information.
        """
        self.create_table(JobsTable.name)
        job = self.select_job_by_name(job_name)
        return dict(job)

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
        job_list = self.select_where_with_columns(JobsTable, {'section': section})
        return [dict(job) for job in job_list]

    def select_active_jobs(
            self,
            include_failed: bool = False,
            members: Optional[List[Any]] = None
    ) -> List[Union[str, Any]]:
        """
        Return the active jobs from the database (without edges), optionally filtered by members.

        :param include_failed: Whether to include failed jobs.
        :type include_failed: bool
        :param members: List of member identifiers to filter jobs.
        :type members: Optional[List[Any]]
        :return: List of jobs matching the criteria.
        :rtype: List[Union[str, Any]]
        """
        self.create_table(JobsTable.name)
        statuses = self._ACTIVE_STATUSES + (['FAILED'] if include_failed else [])
        if members is not None:
            condition = and_(
                JobsTable.c.status.in_(statuses),
                or_(JobsTable.c.member.in_(members), JobsTable.c.member.is_(None))
            )
            job_list = self.select_where_with_columns(JobsTable, condition)
        else:
            condition = JobsTable.c.status.in_(statuses)
            job_list = self.select_where_with_columns(JobsTable, condition)
        return job_list

    def select_children_jobs(
            self,
            job_list: List[Union[str, Any]],
            members: Optional[List[Any]] = None
    ) -> List[Union[str, Any]]:
        """
        Select child jobs from the database, optionally filtered by members.

        :param job_list: List of jobs to find children for.
        :type job_list: List[Union[str, Any]]
        :param members: Optional list of member identifiers to filter child jobs.
        :type members: Optional[List[Any]]
        :return: List of child jobs.
        :rtype: List[Union[str, Any]]
        """
        self.create_table(JobsTable.name)
        children_names = set()
        job_list_tmp = [dict(job) for job in job_list]
        for job in job_list_tmp:
            child_rows = [dict(child) for child in
                          self.select_where_with_columns(ExperimentStructureTable, {'e_from': job['name']})]
            for row in child_rows:
                children_names.add(row['e_to'])

        for child_name in children_names:
            if not any(child_name == job.get("child_name") for job in job_list_tmp):
                where = {'name': child_name}
                if members is not None:
                    from sqlalchemy import and_
                    condition = and_(
                        JobsTable.c.name == child_name,
                        or_(JobsTable.c.member.in_(members), JobsTable.c.member.is_(None))
                    )
                    matches = self.select_where_with_columns(JobsTable, condition)
                else:
                    matches = self.select_where_with_columns(JobsTable, where)
                if matches:
                    child = matches[0]
                    job_list.append(child)

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
            graph.update(self.select_where_with_columns(ExperimentStructureTable, {'e_from': job['name']}))
            graph.update(self.select_where_with_columns(ExperimentStructureTable, {'e_to': job['name']}))

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
            row = self.select_where_with_columns(innerjobs_table, where_query)
            # from row, obtain the package_name
            if row:
                packages_names.add(row['package_name'])

        # load all inner jobs that match the package_name
        for package_name in packages_names:
            matches = self.select_where_with_columns(jobs_table, {'package_name': package_name})
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
        job = self.select_where_with_columns(JobsTable, {'name': job_name})
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

    def load_wrappers(self, preview: bool = False, job_list: Any = None) -> Tuple[
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
            wrappers_inner_jobs = self.select_where_with_columns(innerjobs_table, {'job_name': job_names})
            packages_names = list(set([job['package_name'] for job in wrappers_inner_jobs]))
            wrappers_info = self.select_where_with_columns(wrapper_info_table, {'name': packages_names})

        return wrappers_info, wrappers_inner_jobs

    def reset_workflow(self) -> None:
        """Reset the workflow by dropping all tables related to jobs and wrappers."""
        self.drop_table(JobsTable.name)
        self.drop_table(ExperimentStructureTable.name)
        self.drop_table(PreviewWrapperJobsTable.name)
        self.drop_table(WrapperJobsTable.name)
        self.drop_table(PreviewWrapperInfoTable.name)
        self.drop_table(WrapperInfoTable.name)

    def save_sections_data(self, sections_data: List[Dict[str, Any]]) -> None:
        """
        Save the section data to the database.

        :param sections_data: List of dictionaries containing section information.
        :type sections_data: List[Dict[str, Any]]
        :return: None
        :rtype: None
        """
        self.drop_table(SectionsStructureTable.name)
        self.create_table(SectionsStructureTable.name)
        self.upsert_many(SectionsStructureTable.name, sections_data, ['name'])

    def load_sections_data(self) -> list[tuple[str, Any]]:
        """Load the section data to the database."""
        self.create_table(SectionsStructureTable.name)
        section_data = self.select_all_with_columns(SectionsStructureTable.name)
        return section_data

    def clear_unused_nodes(self, differences: Dict[str, Any]) -> None:
        """
        Delete all jobs from the jobs table whose section matches any name in the provided list.

        :param differences: List of section names to match for deletion.
        :type differences: Dict[str, Any]

        """
        self.create_table(JobsTable.name)
        # Delete jobs which section has been removed in the recent yaml file.
        deleted_sections = []
        for section_name, section_data in differences.items():
            if section_data.get('status', None) == 'removed':
                deleted_sections.append(section_name)
        if deleted_sections:
            self.delete_where(JobsTable.name, {'section': deleted_sections})

        for section_name, section_data in differences.items():
            if section_data.get('status', None) == 'modified':
                # delete if chunk_number is > than the section specified

                condition = and_(
                    JobsTable.c.section == section_name,
                    JobsTable.c.numchunks > section_data.get('chunk_number', 0),
                )
                self.delete_where(JobsTable.name, condition)

                # delete if split_number is > than the section specified
                condition = and_(
                    JobsTable.c.section == section_name,
                    JobsTable.c.split > section_data.get('splits', -1),
                )
                self.delete_where(JobsTable.name, condition)

                # delete if date is not in the section specified
                if 'date' in section_data:
                    condition = and_(
                        JobsTable.c.section == section_name,
                        ~JobsTable.c.datelist.in_(section_data['datelist'].split(" ")),
                    )
                    self.delete_where(JobsTable.name, condition)
                # delete if member is not in the section specified
                if 'member' in section_data:
                    condition = and_(
                        JobsTable.c.section == section_name,
                        ~JobsTable.c.members.in_(section_data['members'].split(" ")),
                    )
                    self.delete_where(JobsTable.name, condition)

                # Update node status to 'Waiting' if it was not in the active statuses
                condition = and_(
                    JobsTable.c.section == section_name,
                    ~JobsTable.c.status.in_(self._ACTIVE_STATUSES)
                )

                #self.update_where(JobsTable.name, {'status': 'WAITING'}, condition)



    def delete_rows_with_number_greater_than(
            self,
            column: "Column",
            x: int
    ) -> List[Dict[str, Any]]:
        """
        Delete rows from the JobsTable where the specified column's value is greater than x.
        :param column: SQLAlchemy Column object to compare.
        :type column: Column
        :param x: Value to compare against.
        :type x: int
        :return: List of rows as dictionaries.
        :rtype: List[Dict[str, Any]]
        """

        self.delete_where(JobsTable.name, {column.name: column > x})

    def remove_section(self, section_name: str) -> None:
        """
        Remove a section from the database by name.

        :param section_name: Name of the section to remove.
        :type section_name: str
        """
        self.create_table(SectionsStructureTable.name)
        self.delete_where(SectionsStructureTable.name, {'name': section_name})

    def update_section(self, section_data: Dict[str, Any]) -> None:
        """
        Update a section in the database.

        :param section_data: Dictionary containing section data to update.
        :type section_data: Dict[str, Any]
        """
        self.create_table(SectionsStructureTable.name)
        self.upsert_many(SectionsStructureTable.name, [section_data], ['name'])

    def add_section(self, section_data: Dict[str, Any]) -> None:
        """
        Add a new section to the database.

        :param section_data: Dictionary containing section data to add.
        :type section_data: Dict[str, Any]
        """
        self.create_table(SectionsStructureTable.name)
        self.insert_many(SectionsStructureTable.name, [section_data])

    def clear_edges(self) -> None:
        """Clear all edges from the database."""
        self.create_table(ExperimentStructureTable.name)
        self.delete_all(ExperimentStructureTable.name)
        self.create_table(ExperimentStructureTable.name)
