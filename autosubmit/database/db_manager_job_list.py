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
import datetime
from pathlib import Path
from typing import Any, Optional, List, Dict, TYPE_CHECKING, Union, Tuple, Set

from sqlalchemy import and_, or_, select, desc, func
from sqlalchemy.exc import IntegrityError

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database.db_manager import DbManager
from autosubmit.database.tables import ExperimentStructureTable, PreviewWrapperJobsTable, WrapperJobsTable, \
    PreviewWrapperInfoTable, WrapperInfoTable, SectionsStructureTable, get_table_from_name
from autosubmit.database.tables import JobsTable, Table
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.log.log import Log

if TYPE_CHECKING:
    pass


class JobsDbManager(DbManager):
    """A database manager for the job_list that extends DbManager using SQLAlchemy.

    It can be used with any engine supported by SQLAlchemy, such
    as Postgres, Mongo, MySQL, etc.
    """

    def __init__(self, connection_url: str, schema: Optional[str] = None) -> None:
        super().__init__(connection_url, schema)
        self._ACTIVE_STATUSES = ['READY', 'SUBMITTED', 'QUEUING', 'HELD', 'RUNNING']
        self._FINAL_STATUSES = ['COMPLETED', 'FAILED']
        self.restore_path = Path(BasicConfig.LOCAL_ROOT_DIR) / 'db' / 'job_list.sql'

    def save_jobs(self, job_list: List[Job]) -> None:
        """Save the job list to the database. Normally this will save the current active jobs."""
        table: Table = get_table_from_name(schema=self.schema, table_name=JobsTable.name)
        self.create_table(table.name)
        persistent_data = [job.__getstate__(log_process=False) for job in job_list]

        pkeys = ['name']
        self.upsert_many(table.name, persistent_data, pkeys)

    def load_jobs(self, full_load, load_failed_jobs: bool = False, members: List = None) -> List[dict[str, Any]]:
        table: Table = get_table_from_name(schema=self.schema, table_name=JobsTable.name)
        self.create_table(table.name)
        if full_load:
            job_list = self.select_all_jobs()
        else:
            job_list = self.select_active_jobs(include_failed=load_failed_jobs, members=members)
            job_list.extend(self.select_children_jobs(job_list, members=members))
            job_list = set(job_list)  # remove duplicates
        job_list = [dict(job) for job in job_list]
        # return modificable list of dicts so it is easier to save later
        return job_list

    def load_job_by_name(self, job_name: str) -> dict[str, Any]:
        """
        Load a job by its name from the database.
        :param job_name: Name of the job to load.
        :type job_name: str
        :return: Dictionary containing the job information.
        """
        table: Table = get_table_from_name(schema=self.schema, table_name=JobsTable.name)
        self.create_table(table.name)
        job = self.select_job_by_name(job_name)
        return dict(job) if job else None

    def get_job_list_size(self) -> Tuple[int, int, int]:
        """
        Return the number of jobs in the database.
        """
        table: Table = get_table_from_name(schema=self.schema, table_name=JobsTable.name)

        self.create_table(table.name)
        job_list_size = self.count(table.name)
        complete_job_list_size = self.count_where(table.name, {'status': "COMPLETED"})
        failed_job_list_size = self.count_where(table.name, {'status': "FAILED"})
        return job_list_size, complete_job_list_size, failed_job_list_size

    def select_all_jobs(self) -> List[dict[str, Any]]:
        """
        Return the whole job list from the database (without edges).
        """
        table: Table = get_table_from_name(schema=self.schema, table_name=JobsTable.name)
        self.create_table(table.name)
        job_list = self.select_all_with_columns(table.name)
        return [dict(job) for job in job_list]

    def select_jobs_by_section(self, section: str) -> List[dict[str, Any]]:
        """
        Return the jobs from the database that belong to a specific section.
        """
        table: Table = get_table_from_name(schema=self.schema, table_name=JobsTable.name)

        self.create_table(table.name)
        job_list = self.select_where_with_columns(table, {'section': section})
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
        table: Table = get_table_from_name(schema=self.schema, table_name=JobsTable.name)
        self.create_table(table.name)
        statuses = self._ACTIVE_STATUSES + (['FAILED'] if include_failed else [])
        if members is not None:
            condition = and_(
                table.c.status.in_(statuses),
                or_(table.c.member.in_(members), table.c.member.is_(None))
            )
            job_list = self.select_where_with_columns(table, condition)
        else:
            condition = table.c.status.in_(statuses)
            job_list = self.select_where_with_columns(table, condition)
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
        jobs_table: Table = get_table_from_name(schema=self.schema, table_name=JobsTable.name)
        experiment_structure_table: Table = get_table_from_name(schema=self.schema,
                                                                table_name=ExperimentStructureTable.name)

        self.create_table(jobs_table.name)
        self.create_table(experiment_structure_table.name)
        children_names = set()
        job_list_tmp = [dict(job) for job in job_list]
        for job in job_list_tmp:
            child_rows = [dict(child) for child in
                          self.select_where_with_columns(experiment_structure_table, {'e_from': job['name']})]
            for row in child_rows:
                children_names.add(row['e_to'])

        for child_name in children_names:
            if not any(child_name == job.get("child_name") for job in job_list_tmp):
                where = {'name': child_name}
                if members is not None:
                    from sqlalchemy import and_
                    condition = and_(
                        jobs_table.c.name == child_name,
                        or_(jobs_table.c.member.in_(members), jobs_table.c.member.is_(None))
                    )
                    matches = self.select_where_with_columns(jobs_table, condition)
                else:
                    matches = self.select_where_with_columns(jobs_table, where)
                if matches:
                    child = matches[0]
                    job_list.append(child)

        return job_list

    def save_edges(self, graph: List[Dict[str, Any]]) -> None:
        """Save the experiment structure into the database."""
        table: Table = get_table_from_name(schema=self.schema, table_name=ExperimentStructureTable.name)

        self.create_table(table.name)
        pkeys = ['e_from', 'e_to']
        self.upsert_many(table.name, graph, pkeys)

    def load_edges(self, job_list: List[dict[str, Any]], full_load: bool) -> List[dict[str, Any]]:
        table: Table = get_table_from_name(schema=self.schema, table_name=ExperimentStructureTable.name)

        self.create_table(table.name)
        if full_load:
            graph = self.select_edges(job_list)
            self.delete_unused_edges(graph)
            self.save_edges(graph)
        else:
            graph = self.select_edges(job_list)
        return graph

    def select_edges(self, job_list: List[dict[str, Any]], only_parents: bool = False) -> List[dict[str, Any]]:
        """
        Return the edges from the database.
        """
        table: Table = get_table_from_name(schema=self.schema, table_name=ExperimentStructureTable.name)

        self.create_table(table.name)
        graph = set()
        for job in job_list:
            graph.update(self.select_where_with_columns(table, {'e_from': job['name']}))
            if not only_parents:
                graph.update(self.select_where_with_columns(table, {'e_to': job['name']}))

        return [dict(edge) for edge in graph]

    def delete_unused_edges(self, graph: List[dict[str, Any]]) -> None:
        """
        Delete unused edges from the database.
        """
        table: Table = get_table_from_name(schema=self.schema, table_name=ExperimentStructureTable.name)

        self.create_table(table.name)
        self.delete_all(table.name)
        self.save_edges(graph)

    def select_job_by_name(self, job_name: str) -> dict[str, Any]:
        """
        Select a job by its name from the database.
        :param job_name: Name of the job to select.
        :type job_name: str
        :return: List of dictionaries containing the job information.
        """
        table: Table = get_table_from_name(schema=self.schema, table_name=JobsTable.name)

        self.create_table(table.name)
        job = self.select_where_with_columns(table, {'name': job_name})
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
            innerjobs_table: Table = get_table_from_name(schema=self.schema, table_name=PreviewWrapperJobsTable.name)
            wrapper_info_table: Table = get_table_from_name(schema=self.schema, table_name=PreviewWrapperInfoTable.name)
        else:
            innerjobs_table: Table = get_table_from_name(schema=self.schema, table_name=WrapperJobsTable.name)
            wrapper_info_table: Table = get_table_from_name(schema=self.schema, table_name=WrapperInfoTable.name)
        self.create_table(innerjobs_table.name)
        self.create_table(wrapper_info_table.name)

        for wrapper_info, inner_jobs in wrappers:
            if isinstance(wrapper_info, list):
                updated_wrappers = [
                    {**wrapper, 'status': Status.VALUE_TO_KEY[int(wrapper['status'])]}
                    for wrapper in wrapper_info
                ]
                self.upsert_many(wrapper_info_table.name, updated_wrappers, ['name'])
            else:
                updated_wrapper = [{**wrapper_info, 'status': Status.VALUE_TO_KEY[int(wrapper_info['status'])]}]
                self.upsert_many(wrapper_info_table.name, updated_wrapper, ['name'])
            try:
                self.insert_many(innerjobs_table.name, inner_jobs)
            except IntegrityError as e:
                Log.warning(f"Unique constraint failed when inserting inner jobs: {e}")

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
        with self.engine.connect() as conn:
            result = conn.execute(query)
            return [dict(row) for row in result.mappings().all()]

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
        if preview:
            full_load = True
        else:
            full_load = False

        if preview:
            innerjobs_table: Table = get_table_from_name(schema=self.schema, table_name=PreviewWrapperJobsTable.name)
            wrapper_info_table: Table = get_table_from_name(schema=self.schema, table_name=PreviewWrapperInfoTable.name)
        else:
            innerjobs_table: Table = get_table_from_name(schema=self.schema, table_name=WrapperJobsTable.name)
            wrapper_info_table: Table = get_table_from_name(schema=self.schema, table_name=WrapperInfoTable.name)

        self.create_table(innerjobs_table.name)
        self.create_table(wrapper_info_table.name)
        if full_load:
            # Load wrapper jobs
            wrappers_inner_jobs = self.select_latest_inner_jobs(innerjobs_table)
            wrappers_info = self.select_all_with_columns(wrapper_info_table.name)
        else:
            # Load only active wrapper jobs
            job_names = [job.name for job in job_list] if job_list else []
            wrappers_inner_jobs = self.select_latest_inner_jobs(innerjobs_table, job_names)
            packages_names = list(set([job['package_name'] for job in wrappers_inner_jobs]))
            wrappers_info = self.select_where_with_columns(wrapper_info_table, {'name': packages_names})
        # change status to the proper value
        for i, wrapper in enumerate(wrappers_info):
            wrapper = dict(wrapper)
            wrapper['status'] = Status.KEY_TO_VALUE[wrapper['status']]
            wrappers_info[i] = tuple(wrapper.items())
        return wrappers_info, wrappers_inner_jobs

    def reset_workflow(self) -> None:
        """Reset the workflow by dropping all tables related to jobs and wrappers."""
        jobs_table: Table = get_table_from_name(schema=self.schema, table_name=JobsTable.name)
        experiment_structure_table: Table = get_table_from_name(schema=self.schema,
                                                                table_name=ExperimentStructureTable.name)
        preview_wrapper_jobs_table: Table = get_table_from_name(schema=self.schema,
                                                                table_name=PreviewWrapperJobsTable.name)
        wrapper_jobs_table: Table = get_table_from_name(schema=self.schema, table_name=WrapperJobsTable.name)
        preview_wrapper_info_table: Table = get_table_from_name(schema=self.schema,
                                                                table_name=PreviewWrapperInfoTable.name)
        wrapper_info_table: Table = get_table_from_name(schema=self.schema, table_name=WrapperInfoTable.name)
        self.drop_table(jobs_table.name)
        self.drop_table(experiment_structure_table.name)
        self.drop_table(preview_wrapper_jobs_table.name)
        self.drop_table(wrapper_jobs_table.name)
        self.drop_table(preview_wrapper_info_table.name)
        self.drop_table(wrapper_info_table.name)

    def save_sections_data(self, sections_data: List[Dict[str, Any]]) -> None:
        """
        Save the section data to the database.

        :param sections_data: List of dictionaries containing section information.
        :type sections_data: List[Dict[str, Any]]
        :return: None
        :rtype: None
        """
        section_structure_table: Table = get_table_from_name(schema=self.schema, table_name=SectionsStructureTable.name)
        self.drop_table(section_structure_table.name)
        self.create_table(section_structure_table.name)
        self.upsert_many(section_structure_table.name, sections_data, ['name'])

    def load_sections_data(self) -> list[tuple[str, Any]]:
        """Load the section data to the database."""
        section_structure_table: Table = get_table_from_name(schema=self.schema, table_name=SectionsStructureTable.name)

        self.create_table(section_structure_table.name)
        section_data = self.select_all_with_columns(section_structure_table.name)
        return section_data

    def clear_unused_nodes(self, differences: Dict[str, Dict[str, Any]]) -> None:
        """
        Remove jobs from the database that are no longer needed based on section differences.

        :param differences: Dictionary describing changes in sections.
        :type differences: Dict[str, Dict[str, Any]]
        """
        jobs_table = get_table_from_name(schema=self.schema, table_name=JobsTable.name)
        jobs_to_delete: Set[str] = set()

        for section_name, section_diff in differences.items():
            raw_list = self.select_where_with_columns(jobs_table, {'section': section_name})
            jobs_dict = [dict(row) for row in raw_list]

            if section_diff.get('status') == 'removed':
                jobs_to_delete.update(job['name'] for job in jobs_dict)
            elif section_diff.get('status') == 'modified':
                for job in jobs_dict:
                    if self._should_delete_job(job, section_diff):
                        jobs_to_delete.add(job['name'])

        if jobs_to_delete:
            self.delete_where(JobsTable.name, {'name': list(jobs_to_delete)})

    def _should_delete_job(self, job: Dict[str, Any], section_diff: Dict[str, Any]) -> bool:
        """
        Determine if a job should be deleted based on section differences.

        :param job: Job dictionary.
        :type job: Dict[str, Any]
        :param section_diff: Section difference dictionary.
        :type section_diff: Dict[str, Any]
        :return: True if the job should be deleted, False otherwise.
        :rtype: bool
        """
        if 'numchunks' in section_diff and job.get('chunk') is not None:
            if (job.get('chunk') is None and section_diff['numchunks'] is not None) or \
                    (section_diff['numchunks'] is None and job.get('chunk') is not None):
                return True
            if job['chunk'] > section_diff['numchunks']:
                return True

        if 'splits' in section_diff and job.get('split') is not None:
            if (job.get('split') is None and section_diff['splits'] is not None) or \
                    (section_diff['splits'] is None and job.get('split') is not None):
                return True
            if job['split'] > int(section_diff['splits']) or (job['split'] == -1 and int(section_diff['splits']) > 0):
                return True

        if 'datelist' in section_diff and job.get('date') is not None:
            if (job.get('date') is None and section_diff['datelist'] is not None) or \
                    (section_diff['datelist'] is None and job.get('date') is not None):
                return True
            datelist = section_diff['datelist'].split()
            date_str = datetime.datetime.fromisoformat(job['date']).strftime('%Y%m%d')
            if date_str not in datelist:
                return True

        if 'members' in section_diff and job.get('member') is not None:
            if (job.get('member') is None and section_diff['members'] is not None) or \
                    (section_diff['members'] is None and job.get('member') is not None):
                return True
            members = section_diff['members'].split()
            if job['member'] not in members:
                return True

        return False

    def clear_edges(self) -> None:
        """Clear all edges from the database."""
        experiment_structure_table: Table = get_table_from_name(schema=self.schema,
                                                                table_name=ExperimentStructureTable.name)

        self.create_table(experiment_structure_table.name)
        self.delete_all(experiment_structure_table.name)
        self.create_table(experiment_structure_table.name)

    def clear_wrappers(self, preview: bool = True) -> None:
        """
        Clear all wrapper jobs and their associated information from the database.

        :param preview: If True, use preview tables; otherwise, use production tables.
        :type preview: bool
        """
        if preview:
            innerjobs_table: Table = get_table_from_name(schema=self.schema, table_name=PreviewWrapperJobsTable.name)
            wrapper_info_table: Table = get_table_from_name(schema=self.schema, table_name=PreviewWrapperInfoTable.name)
        else:
            innerjobs_table: Table = get_table_from_name(schema=self.schema, table_name=WrapperJobsTable.name)
            wrapper_info_table: Table = get_table_from_name(schema=self.schema, table_name=WrapperInfoTable.name)

        self.create_table(innerjobs_table.name)
        self.create_table(wrapper_info_table.name)
        self.delete_all(innerjobs_table.name)
        self.delete_all(wrapper_info_table.name)

    def update_wrapper_status(self, packages) -> None:
        """
        Update the status of wrapper jobs in the database.

        :param packages: WrapperJob object containing package information.
        :type packages: WrapperJob
        """
        wrapper_info_table: Table = get_table_from_name(schema=self.schema, table_name=WrapperInfoTable.name)
        self.create_table(wrapper_info_table.name)

        for package in packages:
            where = {'id': package['id']}
            values = {'status': Status.VALUE_TO_KEY[int(package['status'])]}
            self.update_where(wrapper_info_table.name, where, values)

    def get_wrappers_id(self) -> List[int]:
        """
        Get the IDs of all wrapper jobs in the database.

        :return: List of wrapper job IDs.
        :rtype: List[int]
        """
        wrapper_info_table: Table = get_table_from_name(schema=self.schema, table_name=WrapperInfoTable.name)
        self.create_table(wrapper_info_table.name)
        wrappers = self.select_all_with_columns(wrapper_info_table.name)
        return [wrapper[1] for wrapper in wrappers]
