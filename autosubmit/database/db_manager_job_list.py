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

from sqlalchemy import and_, or_, not_, func, select, exists, update
from sqlalchemy.exc import IntegrityError

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database.db_common import get_connection_url
from autosubmit.database.db_manager import DbManager
from autosubmit.database.tables import ExperimentStructureTable, PreviewWrapperJobsTable, WrapperJobsTable, \
    PreviewWrapperInfoTable, WrapperInfoTable, SectionsStructureTable
from autosubmit.database.tables import JobsTable, Table
from autosubmit.job.job_common import Status
from autosubmit.log.log import Log

def _edge_satisfied(
    parent_status: str,
    min_trigger_status: Optional[str],
    fail_ok: bool,
    from_step: Optional[int],
    child_checkpoint_step: int,
) -> bool:
    """Check if a parent edge status satisfies the trigger requirements.

    :param parent_status: Current status of the parent job.
    :param min_trigger_status: Minimum status required to trigger.
    :param fail_ok: Whether a FAILED parent is acceptable.
    :param from_step: Step threshold for checkpoint-based satisfaction.
    :param child_checkpoint_step: Checkpoint step of the child job.
    :return: True if the edge is satisfied, False otherwise.
    """
    from_step = int(from_step) if from_step is not None else 0
    child_checkpoint_step = int(child_checkpoint_step) if child_checkpoint_step is not None else 0
    min_trigger_status = min_trigger_status or "COMPLETED"

    if parent_status == 'SUSPENDED':
        return False
    elif parent_status in ('COMPLETED', 'SKIPPED'):
         return True
    elif parent_status == 'FAILED':
        if min_trigger_status == 'FAILED' or (min_trigger_status in ('COMPLETED', 'SKIPPED') and (fail_ok or child_checkpoint_step >= from_step > 0)):
            return True
        return False
    elif parent_status == 'RUNNING':
        if min_trigger_status == 'RUNNING' and child_checkpoint_step >= from_step > 0:
            return True
        elif min_trigger_status in ('COMPLETED', 'SKIPPED', 'FAILED'):
            return True
    elif parent_status == min_trigger_status:
        return True
    elif parent_status in Status.LOGICAL_ORDER_SUCCESS_WORKFLOW and min_trigger_status in Status.LOGICAL_ORDER:
        idx_parent = Status.LOGICAL_ORDER.index(parent_status)
        idx_edge = Status.LOGICAL_ORDER.index(min_trigger_status)
        if idx_parent >= idx_edge:
            return True
    return False


_LOG_EXCLUDE_KEYS = {
    'updated_log', 'updated_stats'
}

if TYPE_CHECKING:
    from autosubmit.job.job import Job


class JobsDbManager(DbManager):
    """A database manager for the job_list that extends DbManager using SQLAlchemy.

    It can be used with any engine supported by SQLAlchemy, such
    as Postgres, Mongo, MySQL, etc.
    """

    def __init__(self, schema: Optional[str] = None) -> None:
        if BasicConfig.DATABASE_BACKEND == 'sqlite':
            persistence_full_path = Path(Path(BasicConfig.LOCAL_ROOT_DIR, schema, "db"), Path("job_list.db"))
        else:
            persistence_full_path = None
        super().__init__(get_connection_url(persistence_full_path), schema)
        self._ACTIVE_STATUSES = ['READY', 'SUBMITTED', 'QUEUING', 'HELD', 'RUNNING']
        self._FINAL_STATUSES = ['COMPLETED', 'FAILED']
        self.restore_path = Path(BasicConfig.LOCAL_ROOT_DIR) / 'db' / 'job_list.sql'

    def save_jobs(self, job_list: List["Job"], reset_log_counters: bool = False) -> None:
        """Save the job list to the database.

        Log columns are excluded from the upsert to preserve data written by
        :meth:`save_job_log`.

        :param job_list: List of Job objects to save to the database.
        :type job_list: List[Job]
        :param reset_log_counters: Whether to reset log counters.
        :type reset_log_counters: bool

        :return: None
        :raises: May raise database-related exceptions during upsert operations.
        """
        table: Table = self.table_registry.get(JobsTable.name)
        self.create_table(table.name)
        persistent_data = [job.__getstate__() for job in job_list]

        preserve_data: list = []
        for d in persistent_data:
            if reset_log_counters:
                for k in _LOG_EXCLUDE_KEYS:
                    d[k] = 0
                d['log_recovery_call_count'] = 0
            preserve_data.append(d)
        if preserve_data:
            self.upsert_many(table.name, preserve_data, ['name'], exclude_cols=list(_LOG_EXCLUDE_KEYS) if not reset_log_counters else None)

    def save_job_log(self, job: "Job") -> None:
        """Save only the log information of a single job to the database.

        only update log-related fields (name, log, updated_log, local_logs_out, local_logs_err, remote_logs_out, remote_logs_err).

        :param job: Job object whose log information is to be saved.
        :type job: Job
        :return: None
        """
        table: Table = self.table_registry.get(JobsTable.name)
        self.create_table(table.name)
        job_data: dict = job.__getstate__()
        where: dict = {'name': job.name}
        log_keys = {'name', 'log', 'updated_log', 'updated_stats', 'local_logs_out', 'local_logs_err', 'remote_logs_out', 'remote_logs_err'}
        job_data = {k: v for k, v in job_data.items() if k in log_keys}

        self.update_where(table.name, job_data, where)

    def load_jobs(
            self,
            full_load: bool = False,
            load_failed_jobs: bool = False,
            members: Optional[List[Any]] = None
    ) -> List[Dict[str, Any]]:
        """Return a  list of jobs loaded from the database.

        Load jobs according to the requested mode.

        :param full_load: If True, load all jobs.
        :param load_failed_jobs: If True, include failed jobs when loading active jobs.
        :param members: Optional list of member identifiers to filter jobs.
        :return: A list of job dictionaries.
        """
        table: Table = self.table_registry.get(JobsTable.name)
        self.create_table(table.name)
        if full_load:
            job_list = self.select_all_jobs()
        else:
            job_list = self.select_active_jobs(include_failed=load_failed_jobs, members=members)
            job_list.extend(self.select_children_jobs(job_list, members=members))
            job_list = set(job_list)  # remove duplicates

        return [dict(job) for job in job_list]

    def load_job_by_name(self, job_name: str) -> dict[str, Any]:
        """
        Load a job by its name from the database.
        :param job_name: Name of the job to load.
        :type job_name: str
        :return: Dictionary containing the job information.
        """
        table: Table = self.table_registry.get(JobsTable.name)
        self.create_table(table.name)
        job = self.select_job_by_name(job_name)
        return dict(job) if job else None

    def get_job_list_size(self) -> Tuple[int, int, int]:
        """
        Return the number of jobs in the database.
        """
        table: Table = self.table_registry.get(JobsTable.name)

        self.create_table(table.name)
        job_list_size = self.count(table.name)
        complete_job_list_size = self.count_where(table.name, {'status': "COMPLETED"})
        failed_job_list_size = self.count_where(table.name, {'status': "FAILED"})
        return job_list_size, complete_job_list_size, failed_job_list_size

    def select_job_names_by_sections(
            self,
            sections: List[str],
            exclude_names: Optional[Set[str]] = None,
            exclude_completed: bool = False,
            status_filter: Optional[str] = None,
    ) -> Set[str]:
        """Return job names from DB filtered by section, status and exclusion.

        :param sections: List of section names to filter by.
        :param exclude_names: Optional set of job names to exclude.
        :param exclude_completed: Whether to exclude COMPLETED jobs.
        :param status_filter: Optional status to filter by.
        :return: Set of job names matching the criteria.
        """
        table: Table = self.table_registry.get(JobsTable.name)
        self.create_table(table.name)
        conditions = [table.c.section.in_(sections)]
        if exclude_names:
            conditions.append(not_(table.c.name.in_(exclude_names)))
        if exclude_completed:
            conditions.append(table.c.status != 'COMPLETED')
        if status_filter is not None:
            conditions.append(table.c.status == status_filter)
        condition = and_(*conditions)
        with self._get_engine(table.name).connect() as conn:
            rows = conn.execute(select(table.c.name).select_from(table).where(condition))
            return {row[0] for row in rows.fetchall()}

    def count_remaining_jobs_in_sections(self, sections: List[str], exclude_names: List[str]) -> int:
        """Count non-completed jobs in sections, excluding given names.

        :param sections: List of section names.
        :param exclude_names: List of job names to exclude.
        :return: Count of remaining non-completed jobs.
        """
        table: Table = self.table_registry.get(JobsTable.name)
        self.create_table(table.name)
        condition = and_(
            table.c.section.in_(sections),
            table.c.status != 'COMPLETED',
            not_(table.c.name.in_(exclude_names))
        )
        with self._get_engine(table.name).connect() as conn:
            row = conn.execute(select(func.count()).select_from(table).where(condition))
            return row.scalar()

    def count_non_completed_parents_not_in_memory(
            self, remaining_names: Set[str], loaded_names: Set[str]) -> int:
        """Count non-completed parents of remaining jobs that are not in memory.

        :param remaining_names: Set of remaining job names.
        :param loaded_names: Set of job names already in memory.
        :return: Count of non-completed parents not in memory.
        """
        jobs_table: Table = self.table_registry.get(JobsTable.name)
        structure_table: Table = self.table_registry.get(ExperimentStructureTable.name)
        self.create_table(jobs_table.name)
        self.create_table(structure_table.name)
        with self._get_engine(jobs_table.name).connect() as conn:
            row = conn.execute(
                select(func.count())
                .select_from(
                    structure_table.join(
                        jobs_table,
                        structure_table.c.e_from == jobs_table.c.name
                    )
                )
                .where(
                    and_(
                        structure_table.c.e_to.in_(remaining_names),
                        jobs_table.c.status != 'COMPLETED',
                        not_(structure_table.c.e_from.in_(loaded_names))
                    )
                )
            )
            return row.scalar()

    def remaining_blocked_by_package(
            self, remaining_names: Set[str], package_names: Set[str]) -> bool:
        """Check if remaining jobs are blocked by having unsatisfied parents.

        Returns True only when all non-COMPLETED parents of remaining jobs
        are within the package or the remaining chain itself.

        :param remaining_names: Set of remaining job names.
        :param package_names: Set of job names in the current package.
        :return: True if remaining jobs are blocked by package dependencies.
        """
        jobs_table: Table = self.table_registry.get(JobsTable.name)
        structure_table: Table = self.table_registry.get(ExperimentStructureTable.name)
        self.create_table(jobs_table.name)
        self.create_table(structure_table.name)
        allowed_names = package_names | remaining_names
        with self._get_engine(jobs_table.name).connect() as conn:
            row = conn.execute(
                select(func.count())
                .select_from(
                    structure_table.join(
                        jobs_table,
                        structure_table.c.e_from == jobs_table.c.name
                    )
                )
                .where(
                    and_(
                        structure_table.c.e_to.in_(remaining_names),
                        jobs_table.c.status != 'COMPLETED',
                        not_(structure_table.c.e_from.in_(allowed_names))
                    )
                )
            )
            return row.scalar() == 0

    def select_all_jobs(self) -> List[dict[str, Any]]:
        """
        Return the whole job list from the database (without edges).
        """
        table: Table = self.table_registry.get(JobsTable.name)
        self.create_table(table.name)
        job_list = self.select_all_with_columns(table.name)
        return [dict(job) for job in job_list]

    def select_jobs_by_section(self, section: str) -> List[dict[str, Any]]:
        """
        Return the jobs from the database that belong to a specific section.
        """
        table: Table = self.table_registry.get(JobsTable.name)

        self.create_table(table.name)
        job_list = self.select_where_with_columns(table, {'section': section})
        return [dict(job) for job in job_list]

    def select_loadable_inner_jobs(
            self,
            sections: List[str],
            already_loaded_names: Set[str],
    ) -> List[tuple[tuple[str, Any]]]:
        """Return non-completed jobs in sections whose cross-section parents are all COMPLETED.

        Uses a single SQL query with NOT EXISTS to find jobs whose
        parents from other sections all have completion_status = COMPLETED.

        :param sections: List of section names to search.
        :param already_loaded_names: Set of job names already in memory.
        :return: List of hashable tuples for loadable jobs.
        """
        jobs_table: Table = self.table_registry.get(JobsTable.name)
        structure_table: Table = self.table_registry.get(ExperimentStructureTable.name)
        parent_alias = jobs_table.alias('p')
        self.create_table(jobs_table.name)
        self.create_table(structure_table.name)

        condition = and_(
            jobs_table.c.section.in_(sections),
            jobs_table.c.status != 'COMPLETED',
            ~jobs_table.c.name.in_(already_loaded_names),
            ~exists(
                select(structure_table.c.e_to)
                .select_from(
                    structure_table.join(
                        parent_alias,
                        parent_alias.c.name == structure_table.c.e_from
                    )
                )
                .where(and_(
                    structure_table.c.e_to == jobs_table.c.name,
                    parent_alias.c.section.notin_(sections),
                    structure_table.c.completion_status != 'COMPLETED'
                ))
            )
        )

        with self._get_engine(jobs_table.name).begin() as conn:
            rows = conn.execute(select(jobs_table).where(condition)).fetchall()
            columns = jobs_table.c.keys()
            return [tuple(zip(columns, row)) for row in rows]

    def select_active_jobs(
            self,
            include_failed: bool = False,
            members: Optional[List[Any]] = None
    ) -> List[Union[str, Any]]:
        table: Table = self.table_registry.get(JobsTable.name)
        structure_table: Table = self.table_registry.get(ExperimentStructureTable.name)
        self.create_table(table.name)
        self.create_table(structure_table.name)

        statuses = self._ACTIVE_STATUSES + (['FAILED'] if include_failed else [])

        condition = or_(
            table.c.status.in_(statuses),
            and_(
                table.c.status == 'WAITING',
                ~exists(
                    select(structure_table.c.e_to)
                    .where(and_(
                        structure_table.c.e_to == table.c.name,
                        structure_table.c.completion_status != 'COMPLETED'
                    ))
                )
            )
        )
        if members is not None:
            condition = and_(
                condition,
                or_(table.c.member.in_(members), table.c.member.is_(None))
            )

        with self._get_engine(table.name).begin() as conn:
            rows = conn.execute(select(table).where(condition)).fetchall()
            columns = table.c.keys()
            return [tuple(zip(columns, row)) for row in rows]

    def select_finished_jobs_needing_log_recovery(self) -> List[Dict[str, Any]]:
        """Return COMPLETED/FAILED jobs whose updated_log <= fail_count."""
        table: Table = self.table_registry.get(JobsTable.name)
        self.create_table(table.name)
        condition = and_(
            table.c.status.in_(self._FINAL_STATUSES),
            table.c.updated_log <= table.c.fail_count
        )
        return [dict(job) for job in self.select_where_with_columns(table, condition)]
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
        jobs_table: Table = self.table_registry.get(JobsTable.name)
        experiment_structure_table: Table = self.table_registry.get(ExperimentStructureTable.name)


        self.create_table(jobs_table.name)
        self.create_table(experiment_structure_table.name)
        children_names = set()
        job_list_tmp = [dict(job) for job in job_list]
        names_in_memory = {job['name'] for job in job_list_tmp}
        for job in job_list_tmp:
            child_rows = [dict(child) for child in
                          self.select_where_with_columns(experiment_structure_table, {'e_from': job['name']})]
            for row in child_rows:
                children_names.add(row['e_to'])

        if children_names:
            with self._get_engine(experiment_structure_table.name).begin() as conn:
                rows = conn.execute(
                    select(
                        experiment_structure_table.c.e_from,
                        experiment_structure_table.c.e_to,
                        experiment_structure_table.c.min_trigger_status,
                        experiment_structure_table.c.fail_ok,
                        experiment_structure_table.c.from_step,
                        jobs_table.c.status.label("parent_status"),
                    ).select_from(
                        experiment_structure_table.join(
                            jobs_table,
                            experiment_structure_table.c.e_from == jobs_table.c.name
                        )
                    ).where(
                        experiment_structure_table.c.e_to.in_(children_names)
                    )
                )

                cp_rows = conn.execute(
                    select(
                        jobs_table.c.name,
                        jobs_table.c.current_checkpoint_step,
                    ).where(
                        jobs_table.c.name.in_(children_names)
                    )
                )
                child_checkpoint = {r.name: (r.current_checkpoint_step or 0) for r in cp_rows}

                all_edges: Dict[str, list] = {}
                for row in rows:
                    all_edges.setdefault(row.e_to, []).append(row)

                keep = set()
                for child_name, edges in all_edges.items():
                    cp = child_checkpoint.get(child_name, 0)
                    blocked = False
                    for e in edges:
                        parent_status = e.parent_status
                        if e.e_from in names_in_memory:
                            p_job = next((j for j in job_list_tmp if j['name'] == e.e_from), None)
                            if p_job:
                                parent_status = p_job.get('status', parent_status)
                        if not _edge_satisfied(
                            parent_status=parent_status,
                            min_trigger_status=e.min_trigger_status or "COMPLETED",
                            fail_ok=bool(e.fail_ok) if e.fail_ok is not None else False,
                            from_step=e.from_step,
                            child_checkpoint_step=cp,
                        ):
                            blocked = True
                            break
                    if not blocked:
                        keep.add(child_name)

                children_names = keep

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
        table: Table = self.table_registry.get(ExperimentStructureTable.name)

        self.create_table(table.name)
        pkeys = ['e_from', 'e_to']
        self.upsert_many(table.name, graph, pkeys)

    def update_outgoing_edges_completion(self, job_name: str, job_status: str) -> bool:
        """Update completion_status to COMPLETED for all satisfied outgoing edges of a job.

        Queries experiment_structure directly and updates matching edges in DB.

        :param job_name: Name of the parent job.
        :param job_status: String representation of the parent job's status.
        :return: True if at least one edge was updated.
        """
        structure_table: Table = self.table_registry.get(ExperimentStructureTable.name)
        jobs_table: Table = self.table_registry.get(JobsTable.name)
        self.create_table(structure_table.name)
        self.create_table(jobs_table.name)

        with self._get_engine(structure_table.name).begin() as conn:
            rows = conn.execute(
                select(
                    structure_table.c.e_to,
                    structure_table.c.min_trigger_status,
                    structure_table.c.fail_ok,
                    structure_table.c.from_step,
                    jobs_table.c.current_checkpoint_step,
                ).select_from(
                    structure_table.join(
                        jobs_table,
                        structure_table.c.e_to == jobs_table.c.name,
                        isouter=True
                    )
                ).where(structure_table.c.e_from == job_name)
            ).fetchall()

            updated = False
            for row in rows:
                if _edge_satisfied(
                    parent_status=job_status,
                    min_trigger_status=row.min_trigger_status or "COMPLETED",
                    fail_ok=bool(row.fail_ok) if row.fail_ok is not None else False,
                    from_step=row.from_step or 0,
                    child_checkpoint_step=row.current_checkpoint_step or 0,
                ):
                    conn.execute(
                        update(structure_table)
                        .where(and_(
                            structure_table.c.e_from == job_name,
                            structure_table.c.e_to == row.e_to
                        ))
                        .values(completion_status="COMPLETED")
                    )
                    updated = True
        return updated

    def load_edges(self, job_list: List[dict[str, Any]] = None, full_load: bool = True, remove_unused_edges: bool = True) -> List[dict[str, Any]]:
        table: Table = self.table_registry.get(ExperimentStructureTable.name)

        self.create_table(table.name)
        if full_load:
            graph = self.select_edges()
            if remove_unused_edges:
                self.delete_unused_edges(graph)
                self.save_edges(graph)
        else:
            graph = self.select_edges(job_list)
        return graph

    def select_edges(self, job_list: Optional[List[dict[str, Any]]] = None, only_parents: bool = False) -> List[dict[str, Any]]:
        """Return edges from the database, optionally filtered by job list.

        :param job_list: Optional list of jobs to filter edges by. If None, all edges are returned.
        :param only_parents: If True, return only parent edges (e_from).
        :return: List of edge dictionaries.
        """
        table: Table = self.table_registry.get(ExperimentStructureTable.name)
        self.create_table(table.name)

        if not job_list:
            return [dict(edge) for edge in self.select_all_with_columns(table.name)]

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
        table: Table = self.table_registry.get(ExperimentStructureTable.name)

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
        table: Table = self.table_registry.get(JobsTable.name)

        self.create_table(table.name)
        job = self.select_where_with_columns(table, {'name': job_name})
        if job:
            return job[0]

    # WRAPPERS
    # At this point, we already built the wrappers, so we can save them in the database.
    def save_wrappers(
            self,
            wrappers: Tuple[List[Dict[str, Any]], List[Dict[str, Any]]],
            preview: bool = False,
            run_id: Optional[int] = None
    ) -> None:
        """
        Save the wrapper jobs and their associated information to the database.

        :param wrappers: List of dictionaries containing wrapper job data and package info.
        :type wrappers: Tuple[Dict[str, Any], List[Dict[str, Any]]]
        :param preview: If True, use preview tables; otherwise, use production tables.
        :type preview: bool
        :param run_id: Current experiment run ID to associate with these wrappers.
        :type run_id: Optional[int]
        """
        if preview:
            innerjobs_table: Table = self.table_registry.get(PreviewWrapperJobsTable.name)
            wrapper_info_table: Table = self.table_registry.get(PreviewWrapperInfoTable.name)
        else:
            innerjobs_table: Table = self.table_registry.get(WrapperJobsTable.name)
            wrapper_info_table: Table = self.table_registry.get(WrapperInfoTable.name)
        self.create_table(innerjobs_table.name)
        self.create_table(wrapper_info_table.name)

        for wrapper_info, inner_jobs in wrappers:
            if isinstance(wrapper_info, list):
                updated_wrappers = [
                    {**wrapper, 'status': Status.VALUE_TO_KEY[int(wrapper['status'])], 'run_id': run_id}
                    for wrapper in wrapper_info
                ]
                self.upsert_many(wrapper_info_table.name, updated_wrappers, ['name'])
            else:
                updated_wrapper = [{**wrapper_info, 'status': Status.VALUE_TO_KEY[int(wrapper_info['status'])], 'run_id': run_id}]
                self.upsert_many(wrapper_info_table.name, updated_wrapper, ['name'])
            inner_jobs_with_run_id = [{**j, 'run_id': run_id} for j in inner_jobs]
            try:
                self.insert_many(innerjobs_table.name, inner_jobs_with_run_id)
            except IntegrityError as e:
                Log.warning(f"Unique constraint failed when inserting inner jobs: {e}")

    def load_wrappers(self, preview: bool = False, job_list: Any = None, run_id: Optional[int] = None) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Load the wrapper jobs and their associated information from the database.

        :param preview: If True, use preview tables; otherwise, use production tables.
        :type preview: bool
        :param job_list: Optional list of jobs to filter the loaded wrappers.
        :type job_list: Optional[list]
        :param run_id: Optional run ID to filter wrappers by experiment run.
        :type run_id: Optional[int]
        :return: Tuple containing a list of dictionaries with wrapper job info and inner jobs.
        :rtype: Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]

        """
        full_load = preview

        if preview:
            innerjobs_table: Table = self.table_registry.get(PreviewWrapperJobsTable.name)
            wrapper_info_table: Table = self.table_registry.get(PreviewWrapperInfoTable.name)
        else:
            innerjobs_table: Table = self.table_registry.get(WrapperJobsTable.name)
            wrapper_info_table: Table = self.table_registry.get(WrapperInfoTable.name)

        self.create_table(innerjobs_table.name)
        self.create_table(wrapper_info_table.name)
        wrappers_info_filter = {'run_id': run_id} if run_id is not None else {}
        if full_load:
            # Load wrapper jobs
            wrappers_inner_jobs = self.select_latest_inner_jobs(innerjobs_table)
            wrappers_info = self.select_where_with_columns(wrapper_info_table, wrappers_info_filter) if wrappers_info_filter else self.select_all_with_columns(wrapper_info_table.name)
        else:
            # Load only active wrapper jobs
            job_names = [job.name for job in job_list] if job_list else []
            wrappers_inner_jobs = self.select_latest_inner_jobs(innerjobs_table, job_names)
            packages_names = list(set([job['package_name'] for job in wrappers_inner_jobs]))
            wrappers_info_filter_with_names = {**wrappers_info_filter, 'name': packages_names}
            wrappers_info = self.select_where_with_columns(wrapper_info_table, wrappers_info_filter_with_names)
        # change status to the proper value
        for i, wrapper in enumerate(wrappers_info):
            wrapper = dict(wrapper)
            wrapper['status'] = Status.KEY_TO_VALUE[wrapper['status']]
            wrappers_info[i] = tuple(wrapper.items())
        return wrappers_info, wrappers_inner_jobs

    def reset_workflow(self) -> None:
        """Reset the workflow by dropping all tables related to jobs and wrappers."""
        jobs_table: Table = self.table_registry.get(JobsTable.name)
        experiment_structure_table: Table = self.table_registry.get(ExperimentStructureTable.name)
        preview_wrapper_jobs_table: Table = self.table_registry.get(PreviewWrapperJobsTable.name)
        wrapper_jobs_table: Table = self.table_registry.get(WrapperJobsTable.name)
        preview_wrapper_info_table: Table = self.table_registry.get(PreviewWrapperInfoTable.name)
        wrapper_info_table: Table = self.table_registry.get(WrapperInfoTable.name)

        self.drop_table(preview_wrapper_jobs_table.name)
        self.drop_table(wrapper_jobs_table.name)
        self.drop_table(preview_wrapper_info_table.name)
        self.drop_table(wrapper_info_table.name)
        self.drop_table(jobs_table.name)
        self.drop_table(experiment_structure_table.name)

    def save_sections_data(self, sections_data: List[Dict[str, Any]]) -> None:
        """
        Save the section data to the database.

        :param sections_data: List of dictionaries containing section information.
        :type sections_data: List[Dict[str, Any]]
        :return: None
        :rtype: None
        """
        section_structure_table: Table = self.table_registry.get(SectionsStructureTable.name)
        self.drop_table(section_structure_table.name)
        self.create_table(section_structure_table.name)
        self.upsert_many(section_structure_table.name, sections_data, ['name'])

    def load_sections_data(self) -> list[tuple[str, Any]]:
        """Load the section data to the database."""
        section_structure_table: Table = self.table_registry.get(SectionsStructureTable.name)

        self.create_table(section_structure_table.name)
        section_data = self.select_all_with_columns(section_structure_table.name)
        return section_data

    def clear_unused_nodes(self, differences: Dict[str, Dict[str, Any]]) -> None:
        """
        Remove jobs from the database that are no longer needed based on section differences.

        :param differences: Dictionary describing changes in sections.
        :type differences: Dict[str, Dict[str, Any]]
        """
        jobs_table: Table = self.table_registry.get(JobsTable.name)
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

            # splits=auto makes a dictionary now
            try:
                section_splits = int(section_diff['splits'])
            except (ValueError, TypeError):
                try:
                    import ast
                    splits_dict = ast.literal_eval(section_diff['splits'])
                except Exception:
                    return True
                if not isinstance(splits_dict, dict) or job.get('date') is None:
                    return True
                date_str = datetime.datetime.fromisoformat(job['date']).strftime('%Y%m%d')
                chunk_idx = job.get('chunk', 1) or 1
                if date_str not in splits_dict:
                    return True
                chunk_splits = splits_dict[date_str]
                if isinstance(chunk_splits, list) and len(chunk_splits) >= chunk_idx:
                    section_splits = chunk_splits[chunk_idx - 1]
                else:
                    return True

            if job['split'] > section_splits or (job['split'] == -1 and section_splits > 0):
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
        experiment_structure_table: Table = self.table_registry.get(ExperimentStructureTable.name)
        self.create_table(experiment_structure_table.name)
        self.delete_all(experiment_structure_table.name)

    def clear_wrappers(self, preview: bool = True) -> None:
        """
        Clear all wrapper jobs and their associated information from the database.

        :param preview: If True, use preview tables; otherwise, use production tables.
        :type preview: bool
        """
        if preview:
            innerjobs_table: Table = self.table_registry.get(PreviewWrapperJobsTable.name)
            wrapper_info_table: Table = self.table_registry.get(PreviewWrapperInfoTable.name)
        else:
            innerjobs_table: Table = self.table_registry.get(WrapperJobsTable.name)
            wrapper_info_table: Table = self.table_registry.get(WrapperInfoTable.name)

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
        wrapper_info_table: Table = self.table_registry.get(WrapperInfoTable.name)
        self.create_table(wrapper_info_table.name)

        for package in packages:
            where = {'id': package['id']}
            values = {'status': Status.VALUE_TO_KEY[int(package['status'])]}
            self.update_where(wrapper_info_table.name, values, where)

    def get_wrappers_id_from_db(self) -> List[int]:
        """
        Get the IDs of all wrapper jobs in the database.

        :return: List of wrapper job IDs.
        :rtype: List[int]
        """
        wrapper_info_table: Table = self.table_registry.get(WrapperInfoTable.name)
        self.create_table(wrapper_info_table.name)
        wrappers = self.select_all_with_columns(wrapper_info_table.name)
        return [wrapper[1] for wrapper in wrappers]


    def reset_updated_logs(self) -> None:
        """Reset updated_log and updated_stats to 0 for all jobs with fail_count == 0."""
        table = self.table_registry.get(JobsTable.name)
        self.create_table(table.name)
        self.update_where(
            table.name,
            {'updated_log': 0, 'updated_stats': 0},
            {'fail_count': 0}
        )

    def get_failed_job_data(self) -> list[dict[str, Any]]:
        """Get the names of jobs that have failed.

        :return: List of job names that have failed.
        :rtype: List[str]
        """
        table: Table = self.table_registry.get(JobsTable.name)
        self.create_table(table.name)
        job_list_data: list[dict[str, Any]] = [
            dict(job) for job in self.select_where_with_columns(table, {'status': "FAILED"})
        ]

        return job_list_data
