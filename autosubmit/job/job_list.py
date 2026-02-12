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

import copy
import datetime
import math
import os
import re
import traceback
from contextlib import suppress
from pathlib import Path
from time import strftime, localtime, mktime
from typing import List, Dict, Tuple, Any, Optional, Union, Set

from bscearth.utils.date import date2str, parse_date
from networkx import DiGraph

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.database.db_manager_job_list import JobsDbManager
from autosubmit.helpers.data_transfer import JobRow
from autosubmit.history.experiment_history import ExperimentHistory
from autosubmit.job.job import Job
from autosubmit.job.job import WrapperJob
from autosubmit.job.job_common import Status, bcolors
from autosubmit.job.job_dict import DicJobs
from autosubmit.job.job_packages import JobPackageThread
from autosubmit.job.job_utils import Dependency
from autosubmit.log.log import AutosubmitCritical, Log
from autosubmit.platforms.platform import Platform


class JobList(object):
    """Class to manage the list of jobs to be run by autosubmit"""

    def __init__(self, expid, config, parser_factory, run_mode=False, disable_save=False, submitter=None):
        self._update_file = Path("updated_list_" + expid + ".txt")
        self._failed_file = "failed_job_list_" + expid + ".txt"
        self._expid = expid
        self._as_conf = config
        self._parser_factory = parser_factory
        self._stat_val = Status()
        self._parameters = []
        self._date_list = []
        self._member_list = []
        self._chunk_list = []
        self._dic_jobs = dict()
        self.packages_dict = dict()
        self._ordered_jobs_by_date_member = dict()
        self.dependency_map = None
        self.packages_id = dict()
        self.job_package_map = dict()
        self.sections_checked = set()
        self._run_members = None
        self.jobs_to_run_first = list()
        self.rerun_job_list = list()
        self.graph = DiGraph()
        self.depends_on_previous_chunk = dict()
        self.depends_on_previous_split = dict()
        self.path_to_logs = Path(BasicConfig.LOCAL_ROOT_DIR,
                                 self.expid, BasicConfig.LOCAL_TMP_DIR, f'LOG_{self.expid}')
        self.dbmanager = JobsDbManager(schema=self.expid)
        self.run_mode = run_mode
        self._INACTIVE_STATUSES = [Status.DELAYED, Status.SUSPENDED, Status.WAITING]
        self._ACTIVE_STATUSES = [Status.READY, Status.SUBMITTED, Status.QUEUING,
                                 Status.HELD, Status.RUNNING]
        self._IN_SCHEDULER = [Status.SUBMITTED, Status.QUEUING, Status.HELD, Status.RUNNING]
        self._FINAL_STATUSES = [Status.COMPLETED, Status.FAILED, Status.SKIPPED]
        self.total_size = 0
        self.completed_size = 0
        self.failed_size = 0
        # -cw flag, inspect
        self.disable_save = disable_save
        self.submitter = submitter
        self.check_wrapper_fake_ids = set()
        self._set_status_path = Path(BasicConfig.LOCAL_ROOT_DIR / Path(self.expid) / "status")
        self._update_file_path = self._set_status_path / self._update_file

    @property
    def graph_dict(self) -> list[dict[str, Any]]:
        """Converts the graph edges into a dictionary structure matching the ExperimentStructureTable.

        :return: A list of dictionaries representing the edges.
        """
        edges_dict = []
        for edge in self.graph.edges(data=True):  # Assuming graph.edges(data=True) provides (e_from, e_to, attributes)
            e_from, e_to, attributes = edge
            edges_dict.append({
                "e_from": e_from,
                "e_to": e_to,
                "min_trigger_status": attributes.get("min_trigger_status", "COMPLETED"),
                "from_step": attributes.get("from_step", 0),
                "fail_ok": attributes.get("fail_ok", False),
                "completion_status": attributes.get("completion_status", "WAITING"),
                # check if the edge completion status is fullfilled or not
            })
        return edges_dict

    @graph_dict.setter
    def graph_dict(self, value):
        """
        Prevent direct modification of the graph_dict.
        """
        raise AttributeError("graph_dict is a dynamic view and cannot be directly modified.")

    @property
    def graph_dict_by_job_name(self):
        """
        Converts the graph edges into a dictionary structure matching the ExperimentStructureTable.
        :return: A list of dictionaries representing the edges.
        """
        edges_by_job_name = {}
        for edge in self.graph.edges(data=True):
            e_from, e_to, attributes = edge
            if e_from not in edges_by_job_name:
                edges_by_job_name[e_from] = []
            edges_by_job_name[e_from].append({
                "e_to": e_to,
                "min_trigger_status": attributes.get("min_trigger_status", "COMPLETED"),
                "from_step": attributes.get("from_step", 0),
                "fail_ok": attributes.get("fail_ok", False),
                "completion_status": attributes.get("completion_status", "WAITING"),
                # check if the edge completion status is fullfilled or not
            })
        return edges_by_job_name

    @graph_dict_by_job_name.setter
    def graph_dict_by_job_name(self, value):
        """
        Prevent direct modification of the graph_dict.
        """
        raise AttributeError("graph_dict is a dynamic view and cannot be directly modified.")

    @property
    def job_list(self) -> List[Job]:
        """Dynamically return a list of all 'job' attributes from the graph nodes."""
        try:
            return [data['job'] for _, data in self.graph.nodes(data=True)]
        except BaseException as e:
            err_msg = ""
            for node in self.graph.nodes:
                if not isinstance(self.graph.nodes[node], dict) or 'job' not in self.graph.nodes[node]:
                    err_msg += f"Node {node} does not have a 'job' attribute.\n"
            raise AutosubmitCritical(f"Error retrieving job list: {err_msg}", 7013, str(e))

    @job_list.setter
    def job_list(self, value):
        """Prevent direct modification of the job list."""
        # TODO: Actually you can still append, but it won't do nothing.
        # Consider using collections.UserList and override append, extend, etc.

        raise AttributeError("job_list is a dynamic view and cannot be directly modified.")

    @property
    def expid(self):
        """
        Returns the experiment identifier

        :return: experiment's identifier
        :rtype: str
        """
        return self._expid

    @property
    def run_members(self):
        return self._run_members

    @run_members.setter
    def run_members(self, members: Optional[Union[str, list[str]]]) -> None:
        """Normalize and store members supplied as a string or list.

        :param members: Members to run, provided as a list or a comma/space separated string.
        :raises AutosubmitCritical: If the provided value is neither ``str`` nor ``list``.
        """
        if members is not None and len(str(members)) > 0:
            if isinstance(members, str):
                if "," in members:
                    members = [v.strip() for v in members.split(",")]
                else:
                    members = [v.strip() for v in members.split(" ")]
            elif not isinstance(members, list):
                raise AutosubmitCritical(
                    f"Invalid type for run_members: {type(members)}. Expected a list or a comma-separated string.",
                    7014
                )
            self._run_members = members
        else:
            self._run_members = None

    def _delete_edgeless_jobs(self):
        """Deletes jobs that have no dependencies and are marked for deletion when edgeless."""
        # indices to delete
        for job in self.job_list[:]:
            if job.dependencies is not None and job.dependencies not in ["{}", "[]"]:
                if ((len(job.dependencies) > 0 and not job.has_parents() and not
                job.has_children()) and str(job.delete_when_edgeless).casefold() ==
                        "true".casefold()):
                    self.graph.remove_node(job.name)

    def generate(
            self,
            as_conf: AutosubmitConfig,
            date_list: List[str],
            member_list: List[str],
            num_chunks: int,
            chunk_ini: int,
            parameters: Dict[str, Any],
            date_format: str,
            default_retrials: int,
            default_job_type: str,
            new: bool = True,
            show_log: bool = True,
            force: bool = False,
            full_load: bool = False,
            check_failed_jobs: bool = False,
            monitor: bool = False,
    ) -> None:
        """Generates the workflow graph based on the provided configuration and parameters.
        :param as_conf: Autosubmit configuration object.
        :param date_list: List of dates for job generation.
        :param member_list: List of members for job generation.
        :param num_chunks: Number of chunks for job generation.
        :param chunk_ini: Initial chunk index for job generation.
        :param parameters: Dictionary of parameters for job generation.
        :param date_format: Date format string.
        :param default_retrials: Default number of retrials for jobs.
        :param default_job_type: Default job type for jobs.
        :param new: If True, initializes new jobs.
        :param show_log: If True, shows log messages during generation.
        :param force: If True, forces regeneration of the workflow graph.
        :param full_load: If True, loads the full graph from the database.
        :param check_failed_jobs: If True, checks for failed jobs during loading.
        :param monitor: If True, the edges won't be removed even if there are differences in sections.
        """
        changes = False
        if force:
            self._reset_workflow_graph()
            changes = True
        Log.info("Generating the workflow...")
        self._initialize_workflow_parameters(
            as_conf, date_list, member_list, num_chunks, chunk_ini, parameters, date_format, default_retrials
        )

        if not force:
            changes = self._load_graph(full_load, load_failed_jobs=check_failed_jobs, monitor=monitor)

        if changes or not self.run_mode:
            Log.info("Checking for new jobs...")
            self._create_and_add_jobs(show_log, default_job_type, date_list, member_list)

        if not monitor and (changes or new):
            Log.info("Initializing new jobs...")
            self._initialize_new_jobs(changes, new)

        if changes or not self.run_mode:
            Log.info("Saving the workflow state...")
            self._save_workflow_state(full_load, new)

        if self.run_mode and changes:
            Log.info("Loading only active jobs...")
            changes = self._load_graph(full_load, load_failed_jobs=check_failed_jobs)
            if changes:
                raise AutosubmitCritical("Changes detected after loading active jobs. "
                                         "This shouldn't happen, please report it in GitHub. "
                                         "To solve this issue, autosubmit create $expid -f -np, autosubmit recovery $expid --all -s and autosubmit run $expid",
                                         7015
                                         )
        Log.result("Workflow generation completed.")

    def clear(self) -> None:
        """Clears the entire workflow graph."""
        self.graph.clear()
        self.graph.clear_edges()

    def clear_wrappers_db(self, preview=True) -> None:
        """Clears all wrapper jobs from the database.
        :param preview: If True, the action will be executed in the wrapper simulation table.
        """
        self.dbmanager.clear_wrappers(preview=preview)

    def _reset_workflow_graph(self) -> None:
        """Resets the workflow graph to a zero state by clearing the graph and resetting the database."""
        Log.debug("Resetting the workflow graph to a zero state")
        self.dbmanager.reset_workflow()

    def _initialize_workflow_parameters(
            self,
            as_conf: AutosubmitConfig,
            date_list: List[str],
            member_list: List[str],
            num_chunks: int,
            chunk_ini: int,
            parameters: Dict[str, Any],
            date_format: str,
            default_retrials: int,
    ) -> None:
        """Initializes workflow parameters for job generation.
        :param as_conf: Autosubmit configuration object.
        :param date_list: List of dates for job generation.
        :param member_list: List of members for job generation.
        :param num_chunks: Number of chunks for job generation.
        :param chunk_ini: Initial chunk index for job generation.
        :param parameters: Dictionary of parameters for job generation.
        :param date_format: Date format string.
        :param default_retrials: Default number of retrials for jobs.
        """

        self._parameters = parameters
        self._date_list = date_list
        self._member_list = member_list
        self._chunk_list = list(range(chunk_ini, num_chunks + 1))
        self._dic_jobs = DicJobs(date_list, member_list, self._chunk_list, date_format, default_retrials, as_conf)

    def _recreate_graph(
            self,
            nodes: list[dict[str, Any]],
            edges: list[dict[str, Any]],
            full_load: bool
    ) -> None:
        """Recreates the internal dependency graph from lists of nodes and edges.

        :param nodes: List of node dictionaries, each representing a job node.
        :type nodes: list[dict[str, any]]
        :param edges: List of edge dictionaries, each representing a dependency edge.
        :type edges: list[dict[str, any]]
        :param full_load: Whether to load all jobs and edges.
        :type full_load: bool
        :return: None
        """

        if full_load:
            self.graph.clear()
            self.graph.clear_edges()
        for node in [node for node in nodes if node.get("name", "") not in self.graph.nodes]:
            self._add_job_node_with_platform(node)

        for edge in (edge for edge in edges if
                     edge.get("e_from", "") in self.graph.nodes and edge.get("e_to", "") in self.graph.nodes):
            self._add_edge_and_parent(edge)

        self.fill_parents_children()

    def _add_edge_and_parent(
            self,
            edge: dict[str, Any]
    ) -> None:
        """Add an edge to the graph and update the parent relationship for the job nodes.

        :param edge: Dictionary containing edge data with keys 'e_from', 'e_to', 'min_trigger_status', 'completion_status',
                        'from_step', and 'fail_ok'.
        :type edge: dict[str, Any]
        """
        edge = {
            'e_from': edge['e_from'],
            'e_to': edge['e_to'],
            'from_step': edge.get('from_step', "0"),
            'min_trigger_status': edge.get('min_trigger_status', "COMPLETED"),
            'completion_status': edge.get('completion_status', "WAITING"),
            'fail_ok': edge.get('fail_ok', False)
        }
        if not self.graph.has_edge(edge["e_from"], edge["e_to"]):
            if edge['e_from'] not in self.graph.nodes or edge['e_to'] not in self.graph.nodes:
                raise ValueError(f"Cannot add edge from {edge['e_from']} to {edge['e_to']}: "
                                 f"one of the nodes does not exist in the graph.")
            self.graph.add_edge(
                edge["e_from"],
                edge["e_to"],
                min_trigger_status=edge["min_trigger_status"],
                completion_status=edge["completion_status"],
                from_step=edge["from_step"],
                fail_ok=edge["fail_ok"]
            )
            # Update the parent job
            self.graph.nodes[edge["e_to"]]["job"].add_parent(self.graph.nodes[edge["e_from"]]["job"])

    def _add_job_node_with_platform(
            self,
            node: Dict[str, Any],
            connect_to_platform: bool = True,
    ) -> None:
        """Add a job node to the graph and ensure the platform name is set.

        :param node: Dictionary containing job node data.
        :type node: Dict[str, Any]

        """
        self.graph.add_node(node["name"], job=Job(loaded_data=node))
        job = self.graph.nodes[node["name"]]["job"]
        if not node.get("platform_name", None):
            node["platform_name"] = self._as_conf.jobs_data.get(
                job.section, {}
            ).get(
                "PLATFORM",
                self._as_conf.experiment_data.get("DEFAULT", {}).get("HPCARCH", "LOCAL")
            )
        if not job.platform_name:
            job.platform_name = node.get(
                "platform_name",
                self._as_conf.experiment_data.get("DEFAULT", {}).get("HPCARCH", "LOCAL")
            )
        if connect_to_platform:
            job.assign_platform(self.submitter, create=False, new=False)

    def _load_graph(self, full_load: bool, load_failed_jobs: bool = False, monitor: bool = False) -> bool:
        """Loads the job graph from the database, creating nodes and edges.

        :param full_load: If True, loads all jobs and edges, otherwise loads only the necessary ones.
        :param load_failed_jobs: If True, loads failed jobs from the database.
        :param monitor: If True, the edges won't be removed even if there are differences in sections.
        :return: True if there are differences in sections, False otherwise.
        """
        Log.info("Looking for new jobs...")
        differences = {} if monitor else self.compute_section_differences()
        if differences:
            Log.warning("Differences found in sections, updating graph...")
            self.remove_outdated_information_from_database(differences)
        if differences:
            full_load = True
        Log.info("Loading jobs and edges from database...")
        nodes = self.load_jobs(full_load, load_failed_jobs)
        edges = self.load_edges(nodes, full_load)
        self._recreate_graph(nodes, edges, full_load)
        if differences:
            Log.info(
                "Differences found in sections, the whole graph will be updated accordingly. This may take a while.")
        return False if not differences else True

    def remove_outdated_information_from_database(self, differences: Dict[str, Any]) -> None:
        """Removes outdated information from the database based on the differences found in sections.

        :param differences: Dictionary containing the differences in sections.
        :type differences: Dict[str, Any]
        """
        sections = self.build_sections_data_to_store()
        Log.info("Removing outdated information from database based on section differences...")
        Log.info("All edges will be recreated")  # Not sure how to do this in a more efficient way
        self.dbmanager.clear_edges()
        self.dbmanager.clear_unused_nodes(differences)
        self.dbmanager.save_sections_data(sections)

    def compute_section_differences(self) -> dict[str, dict[str, Any]]:
        """Compute the differences between the current sections and the persistent sections in the database.

        :return: A dictionary mapping section names to their change type and details: 'removed', 'modified', or 'added'.
        :rtype: Dict[str, Dict[str, Any]]
        """
        persistent_sections_data = self.load_sections()
        if not persistent_sections_data:
            return {}

        current_sections_data = self.build_sections_data_to_store()

        persistent_sections = {section["name"]: section for section in persistent_sections_data}
        current_sections = {section["name"]: section for section in current_sections_data}

        persistent_names = set(persistent_sections)
        current_names = set(current_sections)

        differences: Dict[str, Dict[str, Any]] = {}

        for section_name in persistent_names - current_names:
            differences[section_name] = {"status": "removed"}

        for section_name in current_names - persistent_names:
            section = current_sections[section_name]
            differences[section_name] = {
                "status": "added",
                "datelist": section["datelist"],
                "members": section["members"],
                "numchunks": section["numchunks"],
                "splits": section["splits"],
                "dependencies": section["dependencies"],
                "expid": section["expid"],
            }

        for section_name in persistent_names & current_names:
            persistent = persistent_sections[section_name]
            current = current_sections[section_name]
            section_diff = {}
            for key in ["datelist", "members", "numchunks", "splits", "dependencies", "expid"]:
                if str(persistent.get(key, "")) != str(current.get(key, "")):
                    section_diff[key] = current[key]
            if section_diff:
                section_diff["status"] = "modified"
                differences[section_name] = section_diff

        return differences

    def load_sections(self) -> list[dict[str, Any]]:
        """Loads the sections from the database.

        :return: List of sections.
        """
        Log.debug("Loading sections from database...")
        return [dict(raw_row) for raw_row in self.dbmanager.load_sections_data()]

    def _create_and_add_jobs(
            self, show_log: bool, default_job_type: str, date_list: List[str], member_list: List[str]) -> None:
        """Creates and adds jobs to the workflow graph.
        :param show_log: If True, shows log messages during job creation.
        :param default_job_type: Default job type for jobs.
        :param date_list: List of dates for job generation.
        :param member_list: List of members for job generation.
        """

        if show_log:
            Log.info("Creating jobs...")
        self._create_jobs(self._dic_jobs, 0, default_job_type)

        if show_log:
            Log.info("Adding dependencies to the graph..")
        self._add_dependencies(date_list, member_list, self._chunk_list, self._dic_jobs)

        if show_log:
            Log.info("Adding dependencies to the job..")
        self.update_genealogy()

        if show_log:
            Log.info("Deleting edgeless jobs...")
        if len(self.graph.edges) > 0:
            self._delete_edgeless_jobs()

    def _initialize_new_jobs(self, changes: bool, new: bool) -> None:
        """Initializes new jobs in the workflow graph.
        :param changes: If True, resets the fail count for all jobs.
        :param new: If True, initializes new jobs.
        """
        for job in self.job_list:
            if changes:
                job._fail_count = 0
            if new:
                job.status = Status.READY if not self.has_parents(job.name) else Status.WAITING
            else:
                job.status = Status.READY if not self.has_parents(job.name) and Status.WAITING else job.status
            self.graph.nodes[job.name]["job"] = job

    def has_parents(self, job_name: str) -> bool:
        """Check if a job has parents in the graph

        :param job_name: name of the job to check
        :return: True if the job has parents, False otherwise
        """
        return len(self.graph.pred[job_name]) > 0

    def has_children(self, job_name: str) -> bool:
        """Check if a job has children in the graph

        :param job_name: name of the job to check
        :return: True if the job has children, False otherwise
        """
        return len(self.graph.succ[job_name]) > 0

    def get_parents_edges(self, job_name: str) -> dict:
        """Get the parents of a job in the graph

        :param job_name: name of the job to check
        :return: list of parents
        """
        names = list(self.graph.predecessors(job_name))
        return {child_name: self.graph.edges[child_name, job_name] for child_name in names}

    def get_children_edges(self, job_name: str) -> dict:
        """Get the children of a job in the graph

        :param job_name: name of the job to check
        :return: list of children
        """
        names = list(self.graph.successors(job_name))
        return {child_name: self.graph.edges[job_name, child_name] for child_name in names}

    def _save_workflow_state(
            self, create: bool, new: bool
    ) -> None:
        """Saves the current state of the workflow to the database.
        :param create: If True, creates new entries in the database.
        :param new: If True, saves the state of new jobs.
        """
        self.save_jobs()
        self.save_edges()
        self.save_sections()
        for job in self.job_list:
            job.assign_platform(self.submitter, create, new)
        Log.info("Save completed.")

    def build_sections_data_to_store(self) -> list[dict[str, Any]]:
        """Build a list of dictionaries representing section data for database storage.

        :return: List of dictionaries, each representing a section's data.
        :rtype: List[Dict[str, Any]]
        """
        experiment_section = self._as_conf.experiment_data.get("EXPERIMENT", {})
        sections = self._as_conf.jobs_data
        datelist_ref = str(experiment_section.get("DATELIST", ""))
        members_ref = str(experiment_section.get("MEMBERS", ""))
        numchunks_ref = int(experiment_section.get("NUMCHUNKS", 1))
        expid_ref = self._as_conf.experiment_data.get("DEFAULT", {}).get("EXPID", "unknown_expid")
        data_to_store: list[dict[str, Any]] = []
        for section_name, section_data in sections.items():
            splits = None if not section_data.get("SPLITS", None) else section_data.get("SPLITS", 0)
            dependencies = None if not section_data.get("DEPENDENCIES", None) else str(
                section_data.get("DEPENDENCIES", {}))
            datelist = datelist_ref if section_data.get("RUNNING", "once") != "once" else None
            members = members_ref if section_data.get("RUNNING", "once") not in ["once", "date"] else None
            numchunks = numchunks_ref if section_data.get("RUNNING", "once") not in ["once", "date", "member"] else None
            data_to_store.append({
                "name": section_name,
                "splits": str(splits) if splits is not None else None,
                "dependencies": dependencies,
                "datelist": datelist,
                "members": members,
                "numchunks": numchunks,
                "expid": expid_ref,
            })

        return data_to_store

    def save_sections(self):
        """
        Saves the sections of the job list to the database.
        """
        Log.info("Saving sections...")
        self.dbmanager.save_sections_data(self.build_sections_data_to_store())

    def load_inner_jobs_by_section(self, inner_sections: list[str]) -> List[Job]:
        """
        Loads jobs from the database for the specified inner sections. ( if applicable )
        :param inner_sections: List of section names to load jobs from.
        :return: List of Job objects loaded from the database.
        :rtype: List[Job]
        """
        for section in inner_sections:
            # Temporally shallow load all jobs of this section
            # This is the basic working version, but it can be optimized
            # TODO (another PR): This can (temporally) potentially load a lot of jobs, consider optimizing
            # TODO (another PR): use MIN/MAX, WALLCLOCK SUM, etc to load only the necessary jobs
            # TODO (another PR): Another alternative is to store the "vertical level" of the database and do calculations based on that
            current_section_jobs = self.dbmanager.select_jobs_by_section(section)
            # Temporally load their parents
            parents = self.dbmanager.select_edges(current_section_jobs)

            # Group the parents by their names
            group_by_parent_name = {}
            for parent in parents:
                parent_name = parent.get("e_to", "")
                if parent_name not in group_by_parent_name:
                    group_by_parent_name[parent_name] = []
                group_by_parent_name[parent_name].append(parent)

            # See if a inner_job is loadable or not ( dependencies fulfilled )
            for wrappable_job, parents in group_by_parent_name.items():
                # if not loaded already...
                if not self.get_job_by_name(wrappable_job):
                    can_be_loaded = True
                    for edge in parents:
                        p_job = self.get_job_by_name(edge["e_from"])
                        if not p_job:
                            p_job = self.load_job_by_name(edge["e_from"])
                        if p_job and p_job.section != section and edge["COMPLETION_STATUS"] != "COMPLETED":
                            # If the job is not in the current section and is not completed, it cannot be loaded
                            can_be_loaded = False
                            break

                    if can_be_loaded:
                        self.add_job(self.load_job_by_name(wrappable_job))

    def process_wrapper_jobs(self, wrapper_section: str, inner_sections: list[str]) -> None:
        """
        Processes wrapper jobs by loading inner jobs and creating a sorted dictionary of jobs.
        :param wrapper_section: The section name of the wrapper job.
        :param inner_sections: List of inner section names to load jobs from.
        """
        self.load_inner_jobs_by_section(inner_sections)
        try:
            if inner_sections:
                self._ordered_jobs_by_date_member[wrapper_section] = self._create_sorted_dict_jobs(
                    inner_sections
                )
            else:
                self._ordered_jobs_by_date_member[wrapper_section] = {}
        except BaseException as e:
            raise AutosubmitCritical(
                f"Some section jobs of the wrapper:{wrapper_section} are missing from your JOBS definition in YAML",
                7014,
                str(e),
            )

    def clear_generate(self):
        self.dependency_map = {}
        self.parameters = {}
        self._parameters = {}

    def split_by_platform(self):
        """
        Splits the job list by platform name
        :return: job list per platform
        :rtype: dict
        """
        job_list_per_platform = dict()
        for job in self.job_list:
            if job.platform_name not in job_list_per_platform:
                job_list_per_platform[job.platform_name] = []
            job_list_per_platform[job.platform_name].append(job)
        return job_list_per_platform

    def _add_all_jobs_edge_info(self, dic_jobs, option="DEPENDENCIES"):
        jobs_data = dic_jobs.experiment_data.get("JOBS", {})
        sections_gen = (section for section in jobs_data.keys())
        for job_section in sections_gen:
            jobs_gen = (job for job in dic_jobs.get_jobs(job_section))
            # This was affecting the main self.as_conf.experiment_data
            dependencies_keys = copy.deepcopy(jobs_data.get(job_section, {}).get(option, None))
            dependencies = JobList._manage_dependencies(dependencies_keys, dic_jobs) \
                if dependencies_keys else {}
            for job in jobs_gen:
                self._apply_jobs_edge_info(job, dependencies)

    def _deep_map_dependencies(self, section, jobs_data, option, dependency_list=set(),
                               strip_keys=True):
        """
        Recursive function to map dependencies of dependencies
        """
        if section in dependency_list:
            return dependency_list
        dependency_list.add(section)
        if not strip_keys:
            if "+" in section:
                section = section.split("+")[0]
            elif "-" in section:
                section = section.split("-")[0]
        dependencies_keys = jobs_data.get(section, {}).get(option, {})
        for dependency in dependencies_keys:
            if strip_keys:
                if "+" in dependency:
                    dependency = dependency.split("+")[0]
                elif "-" in dependency:
                    dependency = dependency.split("-")[0]
            dependency_list = self._deep_map_dependencies(dependency, jobs_data,
                                                          option, dependency_list, strip_keys)
            dependency_list.add(dependency)
        return dependency_list

    @staticmethod
    def _strip_key(dep: str) -> str:
        """Return the dependency string up to the first '+' or '-'."""
        for sep in ("+", "-"):
            if sep in dep:
                return dep.split(sep, 1)[0]
        return dep

    def _add_dependencies(
            self,
            date_list: list[Any],
            member_list: list[Any],
            chunk_list: list[int],
            dic_jobs: DicJobs,
            option: str = "DEPENDENCIES",
    ) -> None:
        """Build dependency maps and populate the dependency graph for all jobs.

        Iterate experiment `JOBS` sections to:
        - build deep dependency maps (with and without distance metadata),
        - compute and attach edges for each job,
        - add dependencies that couldn't (or not solved yet) be safely added for later pruning,
        - add per-edge metadata to job objects.

        :param date_list: List of dates used by the experiment.
        :param member_list: List of members used by the experiment.
        :param chunk_list: List of chunk identifiers used by the experiment.
        :param dic_jobs: DicJobs instance containing job templates and experiment data.
        :param option: Dependency option key.
        """
        jobs_data = dic_jobs.experiment_data.get("JOBS", {})
        problematic_jobs = {}
        # map dependencies
        self.dependency_map = dict()
        self.dependency_map_with_distances = dict()

        for section in jobs_data.keys():
            self.dependency_map[section] = self._deep_map_dependencies(section,
                                                                       jobs_data, option, set(), strip_keys=True)
            self.dependency_map_with_distances[section] = self._deep_map_dependencies(section,
                                                                                      jobs_data, option, set(),
                                                                                      strip_keys=False)

            if not any(self._strip_key(dependency) == section for dependency in jobs_data.get(section, {}).get(option, {})):
                self.dependency_map[section].remove(section)
                self.dependency_map_with_distances[section].remove(section)

        # Generate all graph before adding dependencies.
        for job_section in (section for section in jobs_data.keys()):
            for job in (job for job in dic_jobs.get_jobs(job_section, sort_string=True)):
                if job.name not in self.graph.nodes:
                    self.graph.add_node(job.name, job=job)

        for job_section in (section for section in jobs_data.keys()):
            # Changes when all jobs of a section are added
            self.depends_on_previous_chunk = dict()
            self.depends_on_previous_split = dict()
            self.depends_on_previous_special_section = dict()
            self.actual_job_depends_on_previous_chunk = False
            self.actual_job_depends_on_previous_member = False
            # No changes, no need to recalculate dependencies
            Log.debug(f"Adding dependencies for {job_section} jobs")
            # If it does not have dependencies, just append it to job_list and continue
            # This was affecting the main self.as_conf.experiment_data
            dependencies_keys = copy.deepcopy(jobs_data.get(job_section, {}).get(option, None))
            # call function if dependencies_key is not None
            dependencies = JobList._manage_dependencies(dependencies_keys, dic_jobs) \
                if dependencies_keys else {}
            self.job_names = set()
            for job in (job for job in dic_jobs.get_jobs(job_section, sort_string=True)):
                self.actual_job_depends_on_special_chunk = False
                if dependencies:
                    # Adds the dependencies to the job, and if not possible,
                    # adds the job to the problematic_dependencies
                    problematic_dependencies = self._manage_job_dependencies(dic_jobs, job,
                                                                             date_list, member_list, chunk_list,
                                                                             dependencies_keys, dependencies,
                                                                             self.graph)
                    if len(problematic_dependencies) > 1:
                        if job_section not in problematic_jobs.keys():
                            problematic_jobs[job_section] = {}
                        problematic_jobs[job_section].update({job.name: problematic_dependencies})

        self.find_and_delete_redundant_relations(problematic_jobs)
        self._add_all_jobs_edge_info(dic_jobs, option)

    def find_and_delete_redundant_relations(self, problematic_jobs: dict) -> None:
        """
        Jobs with intrinsic rules than can't be safely not added without messing other workflows.
        The graph will have the least amount of edges added as much as safely possible
        before this function.
        Structure:
        problematic_jobs structure is {section: {child_name: [parent_names]}}

        :return:
        """
        from itertools import combinations
        from networkx import NetworkXError

        delete_relations = set()
        for section, jobs in problematic_jobs.items():
            for child_name, parents in jobs.items():
                parents_list = list(parents)
                for parent_name, another_parent_name in combinations(parents_list, 2):
                    if self.graph.has_successor(parent_name, another_parent_name):
                        delete_relations.add((parent_name, child_name))
                    elif self.graph.has_successor(another_parent_name, parent_name):
                        delete_relations.add((another_parent_name, child_name))

        for relation_to_delete in delete_relations:
            with suppress(NetworkXError):
                self.graph.remove_edge(relation_to_delete[0], relation_to_delete[1])

    @staticmethod
    def _manage_dependencies(dependencies_keys: dict, dic_jobs: DicJobs) -> dict[Any, Dependency]:
        parameters = dic_jobs.experiment_data["JOBS"]
        dependencies = dict()
        for key in list(dependencies_keys):
            distance = None
            splits = None
            sign = None
            if '-' not in key and '+' not in key and '*' not in key and '?' not in key:
                section = key
            else:
                if '?' in key:
                    sign = '?'
                    section = key[:-1]
                else:
                    if '-' in key:
                        sign = '-'
                    elif '+' in key:
                        sign = '+'
                    elif '*' in key:
                        sign = '*'
                    key_split = key.split(sign)
                    section = key_split[0]
                    distance = int(key_split[1])
            if parameters.get(section, None):
                dependency_running_type = str(parameters[section].get('RUNNING', 'once')).lower()
                delay = int(parameters[section].get('DELAY', -1))
                dependency = Dependency(section, distance, dependency_running_type,
                                        sign, delay, splits, relationships=dependencies_keys[key])
                dependencies[key] = dependency
            else:
                dependencies_keys.pop(key)
        return dependencies

    @staticmethod
    def _parse_filters_to_check(list_of_values_to_check, value_list=[],
                                level_to_check="DATES_FROM"):
        final_values = []
        list_of_values_to_check = str(list_of_values_to_check).upper()
        if list_of_values_to_check is None:
            return None
        if list_of_values_to_check.casefold() == "ALL".casefold():
            return ["ALL"]
        if list_of_values_to_check.casefold() == "NONE".casefold():
            return ["NONE"]
        if list_of_values_to_check.casefold() == "NATURAL".casefold():
            return ["NATURAL"]
        if "," in list_of_values_to_check:
            for value_to_check in list_of_values_to_check.split(","):
                final_values.extend(JobList._parse_filter_to_check(value_to_check, value_list,
                                                                   level_to_check))
        else:
            final_values = JobList._parse_filter_to_check(list_of_values_to_check, value_list,
                                                          level_to_check)
        return final_values

    @staticmethod
    def _parse_filter_to_check(value_to_check, value_list=[], level_to_check="DATES_FROM",
                               splits=None) -> list:
        """
        Parse the filter to check and return the value to check.
        Selection process:
        value_to_check can be:
        a range: [0:], [:N], [0:N], [:-1], [0:N:M] ...
        a value: N.
        a range with step: [0::M], [::2], [0::3], [::3] ...
        :param value_to_check: value to check.
        :param value_list: list of values to check. Dates, members, chunks or splits.
        :return: parsed value to check.
        """
        step = 1
        if "SPLITS" in level_to_check:
            if "auto" in value_to_check:
                value_to_check = value_to_check.replace("auto", str(splits))
            elif "-1" in value_to_check:
                value_to_check = value_to_check.replace("-1", str(splits))
            elif "last" in value_to_check:
                value_to_check = value_to_check.replace("last", str(splits))
        if value_to_check.count(":") == 1:
            # range
            if value_to_check[1] == ":":
                # [:N]
                # Find N index in the list
                start = None
                end = value_to_check.split(":")[1].strip("[]")
                if level_to_check in ["CHUNKS_FROM", "SPLITS_FROM"]:
                    end = int(end)
            elif value_to_check[-2] == ":":
                # [N:]
                # Find N index in the list
                start = value_to_check.split(":")[0].strip("[]")
                if level_to_check in ["CHUNKS_FROM", "SPLITS_FROM"]:
                    start = int(start)
                end = None
            else:
                # [N:M]
                # Find N index in the list
                start = value_to_check.split(":")[0].strip("[]")
                end = value_to_check.split(":")[1].strip("[]")
                step = 1
                if level_to_check in ["CHUNKS_FROM", "SPLITS_FROM"]:
                    start = int(start)
                    end = int(end)
        elif value_to_check.count(":") == 2:
            # range with step
            if value_to_check[-2] == ":" and value_to_check[-3] == ":":  # [N::]
                # Find N index in the list
                start = value_to_check.split(":")[0].strip("[]")
                end = None
                step = 1
                if level_to_check in ["CHUNKS_FROM", "SPLITS_FROM"]:
                    start = int(start)
            elif value_to_check[1] == ":" and value_to_check[2] == ":":  # [::S]
                # Find N index in the list
                start = None
                end = None
                step = value_to_check.split(":")[-1].strip("[]")
                # get index in the value_list
                step = int(step)
            elif value_to_check[1] == ":" and value_to_check[-2] == ":":  # [:M:]
                # Find N index in the list
                start = None
                end = value_to_check.split(":")[1].strip("[]")
                if level_to_check in ["CHUNKS_FROM", "SPLITS_FROM"]:
                    end = int(end)
                step = 1
            else:  # [N:M:S]
                # Find N index in the list
                start = value_to_check.split(":")[0].strip("[]")
                end = value_to_check.split(":")[1].strip("[]")
                step = value_to_check.split(":")[2].strip("[]")
                step = int(step)
                if level_to_check in ["CHUNKS_FROM", "SPLITS_FROM"]:
                    start = int(start)
                    end = int(end)
        else:
            # value
            return [value_to_check]
        # values to return
        if len(value_list) > 0:
            if start is None:
                start = value_list[0]
            if end is None:
                end = value_list[-1]
            try:
                if level_to_check == "CHUNKS_TO":
                    start = int(start)
                    end = int(end)
                return value_list[slice(value_list.index(start),
                                        value_list.index(end) + 1, int(step))]
            except ValueError:
                return value_list[slice(0, len(value_list) - 1, int(step))]
        else:
            if not start:
                start = 0
            if not end:
                Log.warning(
                    "SPLITS Issue: Invalid SPLIT_TO: START:END value. Returning empty list. "
                    "Check the configuration file.")
                return []
            return [number_gen for number_gen in range(int(start), int(end) + 1, int(step))]

    def _check_relationship(self, relationships, level_to_check, value_to_check):
        """
        Check if the current_job_value is included in the filter_value
        :param relationships: current filter level to check.
        :param level_to_check: Can be dates_from, members_from, chunks_from, splits_from.
        :param value_to_check: Can be None, a date, a member, a chunk or a split.
        :return:
        """
        filters = []
        if level_to_check == "DATES_FROM":
            if type(value_to_check) is not str:
                # need to convert in some cases
                value_to_check = date2str(value_to_check, "%Y%m%d")
            try:
                # need to convert in some cases
                values_list = [date2str(date_, "%Y%m%d") for date_ in self._date_list]
            except Exception:
                values_list = self._date_list
        elif level_to_check == "MEMBERS_FROM":
            values_list = self._member_list  # Str list
        elif level_to_check == "CHUNKS_FROM":
            values_list = self._chunk_list  # int list
        else:
            values_list = []  # splits, int list ( artificially generated later )

        relationship = relationships.get(level_to_check, {})
        status = relationship.pop("MIN_TRIGGER_STATUS", relationships.get("MIN_TRIGGER_STATUS", "COMPLETED"))
        from_step = relationship.pop("FROM_STEP", relationships.get("FROM_STEP", 0))
        for filter_range, filter_data in relationship.items():
            selected_filter = JobList._parse_filters_to_check(filter_range, values_list,
                                                              level_to_check)
            if filter_range.casefold() in ["ALL".casefold(), "NATURAL".casefold(),
                                           "NONE".casefold()] or not value_to_check:
                included = True
            else:
                included = False
                for value in selected_filter:
                    if (str(value).strip(" ").casefold() ==
                            str(value_to_check).strip(" ").casefold()):
                        included = True
                        break
            if included:
                if not filter_data.get("MIN_TRIGGER_STATUS", None):
                    filter_data["MIN_TRIGGER_STATUS"] = status
                if not filter_data.get("FROM_STEP", None):
                    filter_data["FROM_STEP"] = from_step
                filters.append(filter_data)
        # Normalize the filter return
        if len(filters) == 0:
            filters = [{}]
        return filters

    def _check_dates(self, relationships: Dict, current_job: Job) -> {}:
        """
        Check if the current_job_value is included in the filter_from and retrieve filter_to value
        :param relationships: Remaining filters to apply.
        :param current_job: Current job to check.
        :return:  filters_to_apply
        """
        # Check the test_dependencies.py to see how to use this function
        filters_to_apply = self._check_relationship(relationships, "DATES_FROM",
                                                    date2str(current_job.date))
        for i, filter in enumerate(filters_to_apply):
            if "MEMBERS_FROM" in filter:
                filters_to_apply_m = self._check_members({"MEMBERS_FROM": (
                    filter.pop("MEMBERS_FROM"))}, current_job)
                if len(filters_to_apply_m) > 0:
                    filters_to_apply[i].update(filters_to_apply_m)
            # Will enter chunks_from, and obtain [{DATES_TO: "20020201", MEMBERS_TO: "fc2",
            # CHUNKS_TO: "ALL", SPLITS_TO: "2"]
            if "CHUNKS_FROM" in filter:
                filters_to_apply_c = self._check_chunks({"CHUNKS_FROM": (
                    filter.pop("CHUNKS_FROM"))}, current_job)
                if len(filters_to_apply_c) > 0 and (type(filters_to_apply_c) is not list or (
                        type(filters_to_apply_c) is list and len(filters_to_apply_c[0]) > 0)):
                    filters_to_apply[i].update(filters_to_apply_c)
            # IGNORED
            if "SPLITS_FROM" in filter:
                filters_to_apply_s = self._check_splits({"SPLITS_FROM": (
                    filter.pop("SPLITS_FROM"))}, current_job)
                if len(filters_to_apply_s) > 0:
                    filters_to_apply[i].update(filters_to_apply_s)
        # Unify filters from all filters_from where the current job is included to
        # have a single SET of filters_to
        filters_to_apply = self._unify_to_filters(filters_to_apply)
        # {DATES_TO: "20020201", MEMBERS_TO: "fc2", CHUNKS_TO: "ALL", SPLITS_TO: "2"}
        return filters_to_apply

    def _check_members(self, relationships: Dict, current_job: Job) -> Dict:
        """
        Check if the current_job_value is included in the filter_from and retrieve filter_to value
        :param relationships: Remaining filters to apply.
        :param current_job: Current job to check.
        :return: filters_to_apply
        """
        filters_to_apply = self._check_relationship(relationships,
                                                    "MEMBERS_FROM", current_job.member)
        for i, filter_ in enumerate(filters_to_apply):
            if "CHUNKS_FROM" in filter_:
                filters_to_apply_c = self._check_chunks({"CHUNKS_FROM": (
                    filter_.pop("CHUNKS_FROM"))}, current_job)
                if len(filters_to_apply_c) > 0:
                    filters_to_apply[i].update(filters_to_apply_c)
            if "SPLITS_FROM" in filter_:
                filters_to_apply_s = self._check_splits({"SPLITS_FROM": (
                    filter_.pop("SPLITS_FROM"))}, current_job)
                if len(filters_to_apply_s) > 0:
                    filters_to_apply[i].update(filters_to_apply_s)
        filters_to_apply = self._unify_to_filters(filters_to_apply)
        return filters_to_apply

    def _check_chunks(self, relationships: Dict, current_job: Job) -> {}:
        """
        Check if the current_job_value is included in the filter_from and retrieve filter_to value
        :param relationships: Remaining filters to apply.
        :param current_job: Current job to check.
        :return: filters_to_apply
        """

        filters_to_apply = self._check_relationship(relationships,
                                                    "CHUNKS_FROM", current_job.chunk)
        for i, filter in enumerate(filters_to_apply):
            if "SPLITS_FROM" in filter:
                filters_to_apply_s = self._check_splits({"SPLITS_FROM": (
                    filter.pop("SPLITS_FROM"))}, current_job)
                if len(filters_to_apply_s) > 0:
                    filters_to_apply[i].update(filters_to_apply_s)
        filters_to_apply = self._unify_to_filters(filters_to_apply)
        return filters_to_apply

    def _check_splits(self, relationships, current_job):
        """
        Check if the current_job_value is included in the filter_from and retrieve filter_to value
        :param relationships: Remaining filters to apply.
        :param current_job: Current job to check.
        :return: filters_to_apply
        """

        filters_to_apply = self._check_relationship(relationships, "SPLITS_FROM",
                                                    current_job.split)
        # No more FROM sections to check, unify _to FILTERS and return
        filters_to_apply = self._unify_to_filters(filters_to_apply, current_job.splits)
        return filters_to_apply

    def _unify_to_filter(self, unified_filter, filter_to, filter_type, splits=None) -> {}:
        """
        Unify filter_to filters into a single dictionary
        :param unified_filter: Single dictionary with all filters_to
        :param filter_to: Current dictionary that contains the filters_to
        :param filter_type: "DATES_TO", "MEMBERS_TO", "CHUNKS_TO", "SPLITS_TO"
        :return: unified_filter
        """
        if len(unified_filter[filter_type]) > 0 and unified_filter[filter_type][-1] != ",":
            unified_filter[filter_type] += ","
        value_list = []
        if filter_type == "DATES_TO":
            value_list = self._date_list
        elif filter_type == "MEMBERS_TO":
            value_list = self._member_list
        elif filter_type == "CHUNKS_TO":
            value_list = self._chunk_list
        if "all".casefold() not in unified_filter[filter_type].casefold():
            aux = str(filter_to.pop(filter_type, None))
            if aux:
                if "," in aux:
                    aux = aux.split(",")
                else:
                    aux = [aux]
                for element in aux:
                    if element == "":
                        continue
                    # Get only the first alphanumeric part and [:] chars
                    parsed_element = re.findall(r"([\[:\]a-zA-Z0-9._-]+)", element)[0].lower()
                    extra_data = element[len(parsed_element):]
                    parsed_element = JobList._parse_filter_to_check(parsed_element,
                                                                    value_list=value_list, level_to_check=filter_type,
                                                                    splits=splits)
                    # convert list to str
                    skip = False
                    # check if any element is natural or none
                    for ele in parsed_element:
                        if type(ele) is str and ele.lower() in ["natural", "none"]:
                            skip = True
                    if skip and len(unified_filter[filter_type]) > 0:
                        continue
                    else:
                        for ele in parsed_element:
                            if extra_data:
                                check_whole_string = str(ele) + extra_data + ","
                            else:
                                check_whole_string = str(ele) + ","
                            if str(check_whole_string) not in unified_filter[filter_type]:
                                unified_filter[filter_type] += check_whole_string
        return unified_filter

    @staticmethod
    def _normalize_to_filters(filter_to: dict, filter_type: str) -> None:
        """
        Normalize filter_to filters to a single string or "all"
        :param filter_to: Unified filter_to dictionary
        :param filter_type: "DATES_TO", "MEMBERS_TO", "CHUNKS_TO", "SPLITS_TO"
        :return:
        """
        if len(filter_to[filter_type]) == 0 or ("," in filter_to[filter_type] and
                                                len(filter_to[filter_type]) == 1):
            filter_to.pop(filter_type, None)
        elif "all".casefold() in filter_to[filter_type]:
            filter_to[filter_type] = "all"
        else:
            # delete last comma
            if "," in filter_to[filter_type][-1]:
                filter_to[filter_type] = filter_to[filter_type][:-1]
            # delete first comma
            if "," in filter_to[filter_type][0]:
                filter_to[filter_type] = filter_to[filter_type][1:]

    def _unify_to_filters(self, filter_to_apply, splits=None):
        """Unify all filter_to filters into a single dictionary ( of current selection ).

        :param filter_to_apply: Filters to apply
        :return: Single dictionary with all filters_to
        """
        unified_filter = {"DATES_TO": "", "MEMBERS_TO": "", "CHUNKS_TO": "", "SPLITS_TO": ""}
        for filter_to in filter_to_apply:
            if "MIN_TRIGGER_STATUS" not in unified_filter and filter_to.get("MIN_TRIGGER_STATUS", None):
                unified_filter["MIN_TRIGGER_STATUS"] = filter_to["MIN_TRIGGER_STATUS"]
            if "FROM_STEP" not in unified_filter and filter_to.get("FROM_STEP", None):
                unified_filter["FROM_STEP"] = filter_to["FROM_STEP"]
            if len(filter_to) > 0:
                self._unify_to_filter(unified_filter, filter_to, "DATES_TO")
                self._unify_to_filter(unified_filter, filter_to, "MEMBERS_TO")
                self._unify_to_filter(unified_filter, filter_to, "CHUNKS_TO")
                self._unify_to_filter(unified_filter, filter_to, "SPLITS_TO", splits=splits)

        JobList._normalize_to_filters(unified_filter, "DATES_TO")
        JobList._normalize_to_filters(unified_filter, "MEMBERS_TO")
        JobList._normalize_to_filters(unified_filter, "CHUNKS_TO")
        JobList._normalize_to_filters(unified_filter, "SPLITS_TO")
        only_none_values = [filters for filters in unified_filter.values()
                            if "none" == filters.lower()]
        if len(only_none_values) != 4:
            # remove all none filters if not all is none
            unified_filter = {key: value for key, value in unified_filter.items()
                              if "none" != value.lower()}

        return unified_filter

    def _filter_current_job(self, current_job: Job, relationships: dict) -> dict:
        """This function will filter the current job based on the relationships given.

        :param current_job: Current job to filter
        :param relationships: Relationships to apply
        :return: dict() with the filters to apply, or empty dict() if no filters to apply
        """

        # This function will look if the given relationship is set for the given job DATEs,MEMBER,
        # CHUNK,SPLIT ( _from filters )
        # And if it is, it will return the dependencies that need to be activated (_TO filters)
        # _FROM behavior:
        # DATES_FROM can contain MEMBERS_FROM,CHUNKS_FROM,SPLITS_FROM
        # MEMBERS_FROM can contain CHUNKS_FROM,SPLITS_FROM
        # CHUNKS_FROM can contain SPLITS_FROM
        # SPLITS_FROM can contain nothing
        # _TO behavior:
        # TO keywords, can be in any of the _FROM filters and they will only affect the _FROM
        # filter they are in.
        # There are 4 keywords:
        # 1. ALL: all the dependencies will be activated of the given filter type (dates, members,
        # chunks or/and splits)
        # 2. NONE: no dependencies will be activated of the given filter type (dates, members,
        # chunks or/and splits)
        # 3. NATURAL: this is the normal behavior, represents a way of letting the job to be
        # activated if they would normally be activated.
        # 4. ? : this is a weak dependency activation flag, The dependency will be activated
        # but the job can fail without affecting the workflow.

        filters_to_apply = {}
        # Check if filter_from-filter_to relationship is set
        if relationships is not None and len(relationships) > 0:
            # Look for a starting point, this can be if else because they're exclusive as a
            # DATE_FROM can't be in a MEMBER_FROM and so on
            if "DATES_FROM" in relationships:
                filters_to_apply = self._check_dates(relationships, current_job)
            elif "MEMBERS_FROM" in relationships:
                filters_to_apply = self._check_members(relationships, current_job)
            elif "CHUNKS_FROM" in relationships:
                filters_to_apply = self._check_chunks(relationships, current_job)
            elif "SPLITS_FROM" in relationships:
                filters_to_apply = self._check_splits(relationships, current_job)
            else:

                relationships.pop("CHUNKS_FROM", None)
                relationships.pop("MEMBERS_FROM", None)
                relationships.pop("DATES_FROM", None)
                relationships.pop("SPLITS_FROM", None)
                filters_to_apply = relationships
        return filters_to_apply

    def add_special_conditions(
            self,
            job: Job,
            special_conditions: Dict[str, Any],
            parent: Job
    ) -> None:
        """
        Add special conditions to the edge between a parent job and a child job in the workflow graph.

        :param job: The child job to which special conditions are applied.
        :type job: Job
        :param special_conditions: Dictionary containing special condition parameters (e.g., STATUS, FROM_STEP, FAIL_OK).
        :type special_conditions: Dict[str, Any]
        :param parent: The parent job from which the edge originates.
        :type parent: Job
        """
        min_trigger_status = special_conditions.get("MIN_TRIGGER_STATUS", "COMPLETED")
        from_step = int(special_conditions.get("FROM_STEP", 0))
        fail_ok = special_conditions.get("FAIL_OK", False)
        job.max_checkpoint_step = from_step if from_step > int(job.max_checkpoint_step) else int(
            job.max_checkpoint_step)
        self.graph.edges[parent.name, job.name].update(min_trigger_status=min_trigger_status, from_step=from_step,
                                                       fail_ok=fail_ok)

    def _apply_jobs_edge_info(self, job: Job, dependencies: dict[str, Dependency]) -> None:
        """Apply edge information to the job based on its dependencies.
        :param job: The job to which edge information is applied.
        :param dependencies: A dictionary of dependencies associated with the job.
        """
        filters_to_apply_by_section = dict()
        for key, dependency in dependencies.items():
            filters_to_apply = self._filter_current_job(job, copy.deepcopy(dependency.relationships))
            if "MIN_TRIGGER_STATUS" in filters_to_apply:
                if "-" in key:
                    key = key.split("-")[0]
                elif "+" in key:
                    key = key.split("+")[0]
                filters_to_apply_by_section[key] = filters_to_apply
        if not filters_to_apply_by_section:
            return
        # divide edge per section name
        parents_by_section = dict()
        for parent, _ in self.graph.in_edges(job.name):
            if self.graph.nodes[parent]['job'].section in filters_to_apply_by_section.keys():
                if self.graph.nodes[parent]['job'].section not in parents_by_section:
                    parents_by_section[self.graph.nodes[parent]['job'].section] = set()
                (parents_by_section[self.graph.nodes[parent]['job'].section].add(self.graph.nodes[parent]['job']))
        for key, list_of_parents in parents_by_section.items():
            special_conditions = dict()
            min_trigger_status = filters_to_apply_by_section[key].get("MIN_TRIGGER_STATUS", "COMPLETED")
            # "?" marks a weak dependency, here we're removing it from the name. ex: "STATUS: COMPLETED?" ( maybe this is already done in the as_conf)
            min_trigger_status = min_trigger_status if "?" != min_trigger_status[-1] else min_trigger_status[:-1]
            special_conditions["MIN_TRIGGER_STATUS"] = min_trigger_status
            special_conditions["FROM_STEP"] = (filters_to_apply_by_section[key].pop("FROM_STEP", 0))
            special_conditions["FAIL_OK"] = (filters_to_apply_by_section[key].pop("FAIL_OK", False))

            for parent in list_of_parents:
                self.add_special_conditions(job, special_conditions, parent)

    def find_current_section(self, job_section, section, dic_jobs, distance, visited_section):
        sections = dic_jobs.as_conf.jobs_data[section].get("DEPENDENCIES", {}).keys()
        if len(sections) == 0:
            return distance
        sections_str = str("," + ",".join(sections) + ",").upper()
        matches = re.findall(rf",{job_section}[+-]*[0-9]*,", sections_str)
        if not matches:
            for key in [dependency_keys for dependency_keys in sections
                        if job_section not in dependency_keys]:
                if "-" in key:
                    stripped_key = key.split("-")[0]
                elif "+" in key:
                    stripped_key = key.split("+")[0]
                else:
                    stripped_key = key
                if stripped_key not in visited_section:
                    distance = max(self.find_current_section(job_section, stripped_key, dic_jobs,
                                                             distance, visited_section + [stripped_key]), distance)
        else:
            for key in [dependency_keys for dependency_keys in sections
                        if job_section in dependency_keys]:
                if "-" in key:
                    distance = int(key.split("-")[1])
                elif "+" in key:
                    distance = int(key.split("+")[1])
                if distance > 0:
                    return distance
        return distance

    def _calculate_natural_dependencies(self, dic_jobs, job, dependency, date, member, chunk, graph,
                                        distances_of_current_section, key, dependencies_of_that_section,
                                        chunk_list, date_list, member_list, special_dependencies,
                                        max_distance, problematic_dependencies):
        """Calculate natural dependencies and add them to the graph if they're necessary.

        :param dic_jobs: JobList
        :param job: Current job
        :param dependency: Dependency
        :param date: Date
        :param member: Member
        :param chunk: Chunk
        :param graph: Graph
        :param distances_of_current_section: Distances of current section
        :param key: Key
        :param dependencies_of_that_section: Dependencies of that section ( Dependencies of target parent )
        :param chunk_list: Chunk list
        :param date_list: Date list
        :param member_list: Member list
        :param special_dependencies: Special dependencies ( dependencies that comes from dependency: special_filters )
        :param max_distance: Max distance ( if a dependency has CLEAN-5 SIM-10, this value would be 10 )
        :param problematic_dependencies: Problematic dependencies
        :return:
        """
        if key != job.section and not date and not member and not chunk:
            if key in dependencies_of_that_section and str(
                    dic_jobs.as_conf.jobs_data[key].get("RUNNING", "once")) == "chunk":
                natural_parents = [natural_parent for natural_parent in dic_jobs.get_jobs(
                    dependency.section, date, member, chunk_list[-1])
                                   if natural_parent.name != job.name]

            elif key in dependencies_of_that_section and str(
                    dic_jobs.as_conf.jobs_data[key].get("RUNNING", "once")) == "member":
                natural_parents = [natural_parent for natural_parent in dic_jobs.get_jobs(
                    dependency.section, date, member_list[-1], chunk)
                                   if natural_parent.name != job.name]
            else:
                natural_parents = [natural_parent for natural_parent in dic_jobs.get_jobs(
                    dependency.section, date, member, chunk) if natural_parent.name != job.name]

        else:
            natural_parents = [natural_parent for natural_parent in
                               dic_jobs.get_jobs(dependency.section, date, member, chunk) if
                               natural_parent.name != job.name]
        # Natural jobs, no filters to apply we can safely add the edge
        for parent in natural_parents:
            if parent.name in special_dependencies:
                continue
            if dependency.relationships:  # If this section has filter, selects..
                found = [aux for aux in dic_jobs.as_conf.jobs_data[parent.section].get("DEPENDENCIES", {}).keys() if
                         job.section == aux]
                if found:
                    continue
            if distances_of_current_section.get(dependency.section, 0) == 0:
                if job.section == parent.section:
                    if not self.actual_job_depends_on_previous_chunk:
                        if parent.section not in self.dependency_map[job.section]:
                            graph.add_edge(parent.name, job.name, min_trigger_status="COMPLETED",
                                           completion_status="WAITING")
                else:
                    if self.actual_job_depends_on_special_chunk and not self.actual_job_depends_on_previous_chunk:
                        if parent.section not in self.dependency_map[job.section]:
                            if parent.running == job.running:
                                graph.add_edge(parent.name, job.name, min_trigger_status="COMPLETED",
                                               completion_status="WAITING")
                    elif not self.actual_job_depends_on_previous_chunk:
                        graph.add_edge(parent.name, job.name, min_trigger_status="COMPLETED",
                                       completion_status="WAITING")
                    elif not self.actual_job_depends_on_special_chunk and self.actual_job_depends_on_previous_chunk:
                        if job.running == "chunk" and job.chunk == 1 or job.running == "member" and parent.running == "member" or job.running == "chunk" and parent.running == "chunk":
                            graph.add_edge(parent.name, job.name, min_trigger_status="COMPLETED",
                                           completion_status="WAITING")
            else:
                if job.section == parent.section:
                    if self.actual_job_depends_on_previous_chunk:
                        skip = False
                        for aux in [aux for aux in self.dependency_map[job.section] if aux != job.section]:
                            distance = 0
                            for aux_ in self.dependency_map_with_distances.get(aux, []):
                                if "-" in aux_:
                                    if job.section == aux_.split("-")[0]:
                                        distance = int(aux_.split("-")[1])
                                elif "+" in aux_:
                                    if job.section == aux_.split("+")[0]:
                                        distance = int(aux_.split("+")[1])
                                if distance >= max_distance:
                                    skip = True
                        if not skip:
                            # get max value in distances_of_current_section.values
                            if job.running == "chunk":
                                if parent.chunk <= (len(chunk_list) - max_distance):
                                    skip = False
                        if not skip:
                            problematic_dependencies.add(parent.name)
                            graph.add_edge(parent.name, job.name, min_trigger_status="COMPLETED",
                                           completion_status="WAITING")
                else:
                    if job.running == parent.running:
                        skip = False
                        problematic_dependencies.add(parent.name)
                        graph.add_edge(parent.name, job.name, min_trigger_status="COMPLETED",
                                       completion_status="WAITING")
                    if parent.running == "chunk":
                        if parent.chunk > (len(chunk_list) - max_distance):
                            graph.add_edge(parent.name, job.name, min_trigger_status="COMPLETED",
                                           completion_status="WAITING")
        JobList.handle_frequency_interval_dependencies(chunk, chunk_list, date, date_list, dic_jobs, job,
                                                       member,
                                                       member_list, dependency.section, natural_parents)
        # check if job has edges
        if len(self.graph.pred[job.name]) == 0:
            for parent in natural_parents:
                if dependency.relationships:  # If this section has filter, selects..
                    found = [aux for aux in dic_jobs.as_conf.jobs_data[parent.section].get("DEPENDENCIES", {}).keys() if
                             job.section == aux]
                    if found:
                        continue
                problematic_dependencies.add(parent.name)
                graph.add_edge(parent.name, job.name, min_trigger_status="COMPLETED", completion_status="WAITING")

        return problematic_dependencies

    def _calculate_filter_dependencies(self, filters_to_apply, dic_jobs, job, dependency, date,
                                       member, chunk, graph, dependencies_keys_without_special_chars,
                                       dependencies_of_that_section, chunk_list, date_list, member_list,
                                       special_dependencies, problematic_dependencies):
        """Calculate dependencies that has any kind of filter set and add them to the graph if they're necessary.

        :param filters_to_apply: Filters to apply
        :param dic_jobs: JobList
        :param job: Current job
        :param dependency: Dependency
        :param date: Date
        :param member: Member
        :param chunk: Chunk
        :param graph: Graph
        :param dependencies_keys_without_special_chars: Dependencies keys without special chars
        :param dependencies_of_that_section: Dependencies of that section
        :param chunk_list: Chunk list
        :param date_list: Date list
        :param member_list: Member list
        :param special_dependencies: Special dependencies
        :param problematic_dependencies: Problematic dependencies
        :return:
        """

        all_none = True
        for filter_value in filters_to_apply.values():
            if str(filter_value).lower() != "none":
                all_none = False
                break
        if all_none:
            return special_dependencies, problematic_dependencies
        any_all_filter = False
        for filter_value in filters_to_apply.values():
            if str(filter_value).lower() == "all":
                any_all_filter = True
                break
        if job.section != dependency.section:
            filters_to_apply_of_parent = self._filter_current_job(job, copy.deepcopy(
                dependencies_of_that_section.get(dependency.section)))
        else:
            filters_to_apply_of_parent = {}
        # Possible_parents are those that match the filters_to_apply without taking into account redundancy. (filters: FROM_MEMBER, FROM_DATE, FROM_CHUNK, FROM_SPLIT, TO_MEMBER, TO_DATE, TO_CHUNK, TO_SPLIT)
        possible_parents = [possible_parent for possible_parent in dic_jobs.get_jobs_filtered(
            dependency.section, job, filters_to_apply, date, member, chunk,
            filters_to_apply_of_parent) if possible_parent.name != job.name]
        for parent in possible_parents:
            # Ideally we want to avoid adding edges when the current job already has a path to the parent
            # But, that is not solved in all cases. So, we use the problematic_dependencies set to track and prune those redundancy.
            edge_added = False
            if any_all_filter:
                if (parent.chunk and parent.chunk != self.depends_on_previous_chunk.get(parent.section, parent.chunk) or
                        (parent.running == "chunk" and parent.chunk != chunk_list[-1] and parent.section in self.dependency_map[parent.section]) or
                        self.actual_job_depends_on_previous_chunk or
                        self.actual_job_depends_on_special_chunk or
                        parent.name in special_dependencies
                ):
                    continue
            if parent.section == job.section:
                if not job.splits or int(job.splits) > 0:
                    self.depends_on_previous_split[job.section] = int(parent.split)
            if self.actual_job_depends_on_previous_chunk and parent.section == job.section:
                graph.add_edge(parent.name, job.name, min_trigger_status="COMPLETED", completion_status="WAITING")
                edge_added = True
            else:
                # In case we need to improve the perfomance while generating the workflow graph, this could be a point to check. (Workflows with splits and many dependencies).
                if parent.name not in self.depends_on_previous_special_section.get(
                        job.section, set()) or job.split > 0 or (job.section == parent.section and job.running != "chunk"):
                    graph.add_edge(parent.name, job.name, min_trigger_status="COMPLETED", completion_status="WAITING")
                    edge_added = True

            if parent.section == job.section:
                self.actual_job_depends_on_special_chunk = True

            # Only fill this if, per example, jobs.A.Dependencies.A or jobs.A.Dependencies.
            # A-N is set.
            if edge_added and job.section in dependencies_keys_without_special_chars:
                if job.name not in self.depends_on_previous_special_section:
                    self.depends_on_previous_special_section[job.name] = set()
                if job.section not in self.depends_on_previous_special_section:
                    self.depends_on_previous_special_section[job.section] = set()
                if parent.name in self.depends_on_previous_special_section.keys():
                    special_dependencies.update(
                        self.depends_on_previous_special_section[parent.name])
                self.depends_on_previous_special_section[job.name].add(parent.name)
                self.depends_on_previous_special_section[job.section].add(parent.name)
                problematic_dependencies.add(parent.name)

        JobList.handle_frequency_interval_dependencies(chunk, chunk_list, date, date_list,
                                                       dic_jobs, job, member, member_list, dependency.section,
                                                       possible_parents)

        return special_dependencies, problematic_dependencies

    def get_filters_to_apply(self, job: Job, dependency: Dependency) -> dict:
        """Get the filters to apply for a given job and dependency.
        :param job: The job object containing job details.
        :param dependency: The dependency object containing dependency details.
        :return: A dictionary of filters to apply for the job based on the dependency relationships.
        """
        filters_to_apply = self._filter_current_job(job, copy.deepcopy(dependency.relationships))
        filters_to_apply.pop("MIN_TRIGGER_STATUS", "COMPLETED")
        # Don't do perform special filter if only "FROM_STEP" is applied
        if "FROM_STEP" in filters_to_apply:
            if (filters_to_apply.get("CHUNKS_TO", "none") == "none" and filters_to_apply.
                    get("MEMBERS_TO", "none") == "none" and filters_to_apply.get("DATES_TO", "none")
                    == "none" and filters_to_apply.get("SPLITS_TO", "none") == "none"):
                filters_to_apply = {}
        filters_to_apply.pop("FROM_STEP", 0)
        filters_to_apply.pop("FAIL_OK", False)

        # BACKWARDS COMPATIBILITY, at the end the filters_to_apply should only contain filters and discard the rest
        filters_to_apply.pop("OPTIONAL", False)

        # If the selected filter is "natural" for all filters_to, trigger the natural dependency
        # calculation
        all_natural = True
        for f_value in filters_to_apply.values():
            if f_value.lower() != "natural":
                all_natural = False
                break
        if all_natural:
            filters_to_apply = {}
        return filters_to_apply

    def _normalize_auto_keyword(self, job: Job, dependency: Dependency) -> Dependency:
        """Normalize the 'auto' keyword in the dependency relationships for a job.

        This function adjusts the 'SPLITS_TO' value in the dependency relationships
        if it contains the 'auto' keyword. The 'auto' keyword is replaced with the
        actual number of splits for the job.

        :param job: The job object containing job details.
        :param dependency: The dependency object containing dependency details.
        :return: The dependency object with the attribute relationships updated with the correct
        number of splits.
        """
        if (job.splits and dependency.distance and dependency.relationships and
                job.running == "chunk"):
            job_name_separated = job.name.split("_")
            if dependency.sign == "-":
                auto_chunk = int(job_name_separated[3]) - int(dependency.distance)
            else:
                auto_chunk = int(job_name_separated[3]) + int(dependency.distance)
            if auto_chunk < 1:
                auto_chunk = int(job_name_separated[3])
            auto_chunk = str(auto_chunk)
            # Get first split of the given chunk
            auto_job_name = ("_".join(job_name_separated[:3]) +
                             f"_{auto_chunk}_1_{dependency.section}")
            if auto_job_name in self.graph.nodes:
                auto_splits = str(self.graph.nodes[auto_job_name]['job'].splits)
                for filters_to_keys, filters_to in (
                        dependency.relationships.get("SPLITS_FROM", {}).items()):
                    if "auto" in filters_to.get("SPLITS_TO", "").lower():
                        filters_to["SPLITS_TO"] = filters_to["SPLITS_TO"].lower()
                        filters_to["SPLITS_TO"] = filters_to["SPLITS_TO"].replace("auto", auto_splits)
                job.splits = auto_splits
        return dependency

    def _manage_job_dependencies(
            self,
            dic_jobs: DicJobs,
            job: Job,
            date_list: List[str],
            member_list: List[str],
            chunk_list: List[int],
            dependencies_keys: Dict[str, Any],
            dependencies: Dict[str, Dependency],
            graph: DiGraph,
    ) -> set[str]:
        """Manage job dependencies for a given job and update the dependency graph.

        :param dic_jobs: Helper containing generated jobs and configuration.
        :type dic_jobs: `DicJobs`
        :param job: Current job object being processed.
        :type job: `Job`
        :param date_list: Ordered list of dates used by the workflow (YYYYMMDD strings).
        :type date_list: List[str]
        :param member_list: Ordered list of members.
        :type member_list: List[str]
        :param chunk_list: Ordered list of chunk indices.
        :type chunk_list: List[int]
        :param dependencies_keys: Raw dependency keys as defined in the configuration. Keys may include special modifiers
            such as `+`, `-`, `*` or `?` and optional numeric distances (e.g., `SIM-1`, `CLEAN+2`).
        :type dependencies_keys: Dict[str, Any]
        :param dependencies: Parsed mapping from original dependency key to `Dependency` objects.
        :type dependencies: Dict[str, `Dependency`]
        :param graph: The NetworkX directed graph being populated with edges.
        :type graph: `DiGraph`

        :return: A set with names of parent jobs considered problematic (e.g., edges added but parent missing/ambiguous).
        :rtype: set[str]

        :raises ValueError: If dependency key parsing encounters an invalid numeric distance.
        :raises KeyError: If required sections are missing from `dic_jobs.as_conf.jobs_data`.
        """
        # Initialize variables
        distances_of_current_section = {}
        distances_of_current_section_member = {}
        problematic_dependencies = set()
        special_dependencies = set()
        dependencies_to_del = set()
        dependencies_non_natural_to_del = set()
        max_distance = 0
        dependencies_keys_without_special_chars = []

        # Strip special characters from dependency keys
        for key_aux_stripped in dependencies_keys.keys():
            if "-" in key_aux_stripped:
                key_aux_stripped = key_aux_stripped.split("-")[0]
            elif "+" in key_aux_stripped:
                key_aux_stripped = key_aux_stripped.split("+")[0]
            dependencies_keys_without_special_chars.append(key_aux_stripped)

        # Update dependency map for the current job section
        self.dependency_map[job.section] = self.dependency_map[job.section].difference(set(dependencies_keys.keys()))

        # Calculate distances for dependencies (e.g., SIM-1, CLEAN-2)
        for dependency_key in dependencies_keys.keys():
            if "-" in dependency_key:
                aux_key = dependency_key.split("-")[0]
                distance = int(dependency_key.split("-")[1])
            elif "+" in dependency_key:
                aux_key = dependency_key.split("+")[0]
                distance = int(dependency_key.split("+")[1])
            else:
                aux_key = dependency_key
                distance = 0

            # Update distances based on the dependency type ( Once, chunk, member, etc. )
            if dic_jobs.as_conf.jobs_data.get(aux_key, {}).get("RUNNING", "once") == "chunk":
                distances_of_current_section[aux_key] = distance
            elif dic_jobs.as_conf.jobs_data.get(aux_key, {}).get("RUNNING", "once") == "member":
                distances_of_current_section_member[aux_key] = distance

            # Check if the job depends on previous chunks or members
            if distance != 0:
                if job.running == "chunk" and int(job.chunk) > 1:
                    if job.section == aux_key or dic_jobs.as_conf.jobs_data.get(aux_key, {}).get("RUNNING",
                                                                                                 "once") == "chunk":
                        self.actual_job_depends_on_previous_chunk = True
                if job.running in ["member", "chunk"] and job.member:
                    if member_list.index(job.member) > 0:
                        if job.section == aux_key or dic_jobs.as_conf.jobs_data.get(aux_key, {}).get("RUNNING",
                                                                                                     "once") == "member":
                            self.actual_job_depends_on_previous_member = True

            # Handle dependencies to other sections
            if aux_key != job.section:
                dependencies_of_that_section = dic_jobs.as_conf.jobs_data[aux_key].get("DEPENDENCIES", {})
                for key, relationships in dependencies_of_that_section.items():
                    if "-" in key or "+" in key:
                        continue

                    # Skip dependencies that are already defined or delayed
                    elif key != job.section:
                        if job.running == "chunk" and dic_jobs.as_conf.jobs_data[aux_key].get("DELAY", None):
                            if job.chunk <= int(dic_jobs.as_conf.jobs_data[aux_key].get("DELAY", 0)):
                                continue
                        # Only natural dependencies
                        if dependencies.get(key, None) and not relationships:
                            dependencies_to_del.add(key)

        # Calculate maximum distance for dependencies
        for key in self.dependency_map_with_distances[job.section]:
            if "-" in key:
                aux_key = key.split("-")[0]
                distance = int(key.split("-")[1])
            elif "+" in key:
                aux_key = key.split("+")[0]
                distance = int(key.split("+")[1])
            else:
                aux_key = key
                distance = 0

            max_distance = max(max_distance, distance)

            # Update distances for chunk and member dependencies
            if dic_jobs.as_conf.jobs_data.get(aux_key, {}).get("RUNNING", "once") == "chunk":
                if aux_key in distances_of_current_section and distance > distances_of_current_section[aux_key]:
                    distances_of_current_section[aux_key] = distance

            elif dic_jobs.as_conf.jobs_data.get(aux_key, {}).get("RUNNING", "once") == "member":
                if (aux_key in distances_of_current_section_member and
                        distance > distances_of_current_section_member[aux_key]):
                    distances_of_current_section_member[aux_key] = distance

        # Process sections with special filters
        sections_to_calculate = [key for key in dependencies_keys.keys() if key not in dependencies_to_del]
        natural_sections = []

        # Parse first sections with special filters if any
        for key in sections_to_calculate:
            dependency = dependencies[key]
            skip, (chunk, member, date) = JobList._calculate_dependency_metadata(job.chunk, chunk_list,
                                                                                 job.member, member_list,
                                                                                 job.date, date_list,
                                                                                 dependency)
            if skip:
                continue

            self._normalize_auto_keyword(job, dependency)
            filters_to_apply = self.get_filters_to_apply(job, dependency)

            if len(filters_to_apply) > 0:
                dependencies_of_that_section = dic_jobs.as_conf.jobs_data[dependency.section].get("DEPENDENCIES", {})
                # Adds the dependencies to the job, and if not possible, adds the job to the problematic_dependencies
                special_dependencies, problematic_dependencies = (
                    self._calculate_filter_dependencies(filters_to_apply, dic_jobs, job, dependency,
                                                        date, member, chunk, graph,
                                                        dependencies_keys_without_special_chars,
                                                        dependencies_of_that_section, chunk_list, date_list,
                                                        member_list,
                                                        special_dependencies, problematic_dependencies))
            else:
                if key in dependencies_non_natural_to_del:
                    continue
                natural_sections.append(key)

        for key in natural_sections:
            dependency = dependencies[key]
            self._normalize_auto_keyword(job, dependency)
            skip, (chunk, member, date) = JobList._calculate_dependency_metadata(job.chunk, chunk_list,
                                                                                 job.member, member_list,
                                                                                 job.date, date_list,
                                                                                 dependency)
            if skip:
                continue

            aux = dic_jobs.as_conf.jobs_data[dependency.section].get("DEPENDENCIES", {})
            dependencies_of_that_section = [key_aux_stripped.split("-")[0] if "-" in key_aux_stripped else
                                            key_aux_stripped.split("+")[0] if "+" in key_aux_stripped else
                                            key_aux_stripped for key_aux_stripped in aux.keys()]

            problematic_dependencies = self._calculate_natural_dependencies(
                dic_jobs, job, dependency, date, member, chunk, graph,
                distances_of_current_section, key,
                dependencies_of_that_section, chunk_list,
                date_list, member_list,
                special_dependencies, max_distance,
                problematic_dependencies
            )

        return problematic_dependencies

    @staticmethod
    def _calculate_dependency_metadata(
            chunk: Optional[int],
            chunk_list: List[int],
            member: Optional[str],
            member_list: List[str],
            date: Optional[str],
            date_list: List[str],
            dependency: Any
    ) -> Tuple[bool, Tuple[Optional[int], Optional[str], Optional[str]]]:
        """Compute the target chunk, member, and date for a dependency with a +/- distance.

        Compute the target chunk, member, and date for a dependency that specifies a
        positive or negative distance.

        :param chunk: Current chunk index or ``None``.
        :type chunk: Optional[int]
        :param chunk_list: Ordered list of available chunk indices.
        :type chunk_list: List[int]
        :param member: Current member identifier or ``None``.
        :type member: Optional[str]
        :param member_list: Ordered list of available members.
        :type member_list: List[str]
        :param date: Current date string or ``None``.
        :type date: Optional[str]
        :param date_list: Ordered list of available dates.
        :type date_list: List[str]
        :param dependency: Dependency object.
        :type dependency: Any
        :returns: Tuple where the first element is a boolean ``skip`` flag and the second is
            a tuple ``(chunk, member, date)`` with the computed targets.
        :rtype: Tuple[bool, Tuple[Optional[int], Optional[str], Optional[str]]].
        """
        skip = False
        if dependency.sign == '-':
            if chunk is not None and len(str(chunk)) > 0 and dependency.running == 'chunk':
                chunk_index = chunk - 1
                if chunk_index >= dependency.distance:
                    chunk = chunk_list[chunk_index - dependency.distance]
                else:
                    skip = True
            elif (member is not None and len(str(member)) > 0 and
                  dependency.running in ['chunk', 'member']):
                # improve this TODO
                member_index = member_list.index(member)
                if member_index >= dependency.distance:
                    member = member_list[member_index - dependency.distance]
                else:
                    skip = True
            elif (date is not None and len(str(date)) > 0 and
                  dependency.running in ['chunk', 'member', 'startdate']):
                # improve this TODO
                date_index = date_list.index(date)
                if date_index >= dependency.distance:
                    date = date_list[date_index - dependency.distance]
                else:
                    skip = True
        elif dependency.sign == '+':
            if chunk is not None and len(str(chunk)) > 0 and dependency.running == 'chunk':
                chunk_index = chunk_list.index(chunk)
                if (chunk_index + dependency.distance) < len(chunk_list):
                    chunk = chunk_list[chunk_index + dependency.distance]
                else:  # calculating the next one possible
                    temp_distance = dependency.distance
                    while temp_distance > 0:
                        temp_distance -= 1
                        if (chunk_index + temp_distance) < len(chunk_list):
                            chunk = chunk_list[chunk_index + temp_distance]
                            break
            elif (member is not None and len(str(member)) > 0 and
                  dependency.running in ['chunk', 'member']):
                member_index = member_list.index(member)
                if (member_index + dependency.distance) < len(member_list):
                    member = member_list[member_index + dependency.distance]
                else:
                    skip = True
            elif (date is not None and len(str(date)) > 0 and
                  dependency.running in ['chunk', 'member', 'startdate']):
                date_index = date_list.index(date)
                if (date_index + dependency.distance) < len(date_list):
                    date = date_list[date_index - dependency.distance]
                else:
                    skip = True
        return skip, (chunk, member, date)

    @staticmethod
    def handle_frequency_interval_dependencies(chunk, chunk_list, date, date_list, dic_jobs, job,
                                               member, member_list, section_name, visited_parents):
        if job.frequency and job.frequency > 1:
            if job.chunk is not None and len(str(job.chunk)) > 0:
                max_distance = (chunk_list.index(chunk) + 1) % job.frequency
                if max_distance == 0:
                    max_distance = job.frequency
                for distance in range(1, max_distance):
                    for parent in dic_jobs.get_jobs(section_name, date, member, chunk - distance):
                        if parent not in visited_parents:
                            job.add_parent(parent)
            elif job.member is not None and len(str(job.member)) > 0:
                member_index = member_list.index(job.member)
                max_distance = (member_index + 1) % job.frequency
                if max_distance == 0:
                    max_distance = job.frequency
                for distance in range(1, max_distance, 1):
                    for parent in dic_jobs.get_jobs(section_name, date,
                                                    member_list[member_index - distance], chunk):
                        if parent not in visited_parents:
                            job.add_parent(parent)
            elif job.date is not None and len(str(job.date)) > 0:
                date_index = date_list.index(job.date)
                max_distance = (date_index + 1) % job.frequency
                if max_distance == 0:
                    max_distance = job.frequency
                for distance in range(1, max_distance, 1):
                    for parent in dic_jobs.get_jobs(section_name, date_list[date_index - distance],
                                                    member, chunk):
                        if parent not in visited_parents:
                            job.add_parent(parent)

    @staticmethod
    def _create_jobs(dic_jobs, priority, default_job_type):
        for section in (job for job in dic_jobs.experiment_data.get("JOBS", {}).keys()):
            Log.debug(f"Creating {section} jobs")
            dic_jobs.read_section(section, priority, default_job_type)
            priority += 1

    def _create_sorted_dict_jobs(self, wrapper_jobs):
        """Creates a sorting of the jobs whose job.section is in wrapper_jobs, according to the
        following filters in order of importance:
        date, member, RUNNING, and chunk number; where RUNNING is defined in jobs_.yml
        for each section.

        If the job does not have a chunk number, the total number of chunks configured for
        the experiment is used.

        :param wrapper_jobs: User defined job types in autosubmit_,conf [wrapper] section to
        be wrapped.
        :type wrapper_jobs: String \n
        :return: Sorted Dictionary of List that represents the jobs included in the wrapping
        process.
        :rtype: Dictionary Key: date, Value: (Dictionary Key: Member, Value: List of jobs that
        belong to the date, member, and are ordered by chunk number if it is a chunk job otherwise
        num_chunks from JOB TYPE (section)
        """

        # Dictionary Key: date, Value: (Dictionary Key: Member, Value: List)
        job = None

        dict_jobs = dict()
        for date in self._date_list:
            dict_jobs[date] = dict()
            for member in self._member_list:
                dict_jobs[date][member] = list()
        num_chunks = len(self._chunk_list)

        sections_running_type_map = dict()
        if wrapper_jobs is not None and len(str(wrapper_jobs)) > 0:
            # TODO: Removing this causes unit tests to fail, need to investigate why in another PR
            if type(wrapper_jobs) is not list:
                if "&" in wrapper_jobs:
                    char = "&"
                else:
                    char = " "
                wrapper_jobs = wrapper_jobs.split(char)
            for section in wrapper_jobs:
                # RUNNING = once, as default. This value comes from jobs_.yml
                sections_running_type_map[section] = str(self._as_conf.experiment_data["JOBS"].
                                                         get(section, {}).get("RUNNING", 'once'))

            # Select only relevant jobs, those belonging to the sections defined in the wrapper

        sections_to_filter = ""
        for section in sections_running_type_map:
            sections_to_filter += section

        filtered_jobs_list = [job for job in self.job_list if
                              job.section in sections_running_type_map]

        filtered_jobs_fake_date_member, fake_original_job_map = self._create_fake_dates_members(
            filtered_jobs_list)

        for date in self._date_list:
            str_date = self._get_date(date)
            for member in self._member_list:
                # Filter list of fake jobs according to date and member,
                # result not sorted at this point
                sorted_jobs_list = [job for job in filtered_jobs_fake_date_member if
                                    job.name.split("_")[1] == str_date and
                                    job.name.split("_")[2] == member]

                # There can be no jobs for this member when select chunk/member is enabled
                if not sorted_jobs_list or len(sorted_jobs_list) == 0:
                    continue

                previous_job = sorted_jobs_list[0]

                # get RUNNING for this section
                section_running_type = sections_running_type_map[previous_job.section]
                jobs_to_sort = [previous_job]
                previous_section_running_type = None
                # Index starts at 1 because 0 has been taken in a previous step
                for index in range(1, len(sorted_jobs_list) + 1):
                    # If not last item
                    if index < len(sorted_jobs_list):
                        job = sorted_jobs_list[index]
                        # Test if section has changed. e.g. from INI to SIM
                        if previous_job.section != job.section:
                            previous_section_running_type = section_running_type
                            section_running_type = sections_running_type_map.get(job.section, None)
                    # Test if RUNNING is different between sections, or if we have reached
                    # the last item in sorted_jobs_list
                    if ((previous_section_running_type is not None and
                         previous_section_running_type != section_running_type) or
                            index == len(sorted_jobs_list)):

                        # Sorting by date, member, chunk number if it is a chunk job otherwise
                        # num_chunks from JOB TYPE (section)
                        # Important to note that the only differentiating factor would be chunk
                        # OR num_chunks

                        # Bringing back original job if identified
                        for idx in range(0, len(jobs_to_sort)):
                            # Test if it is a fake job
                            if jobs_to_sort[idx] in fake_original_job_map:
                                fake_job = jobs_to_sort[idx]
                                # Get original
                                jobs_to_sort[idx] = fake_original_job_map[fake_job]
                        # Add to result, and reset jobs_to_sort
                        # By adding to the result at this step, only those with the same
                        # RUNNING have been added.
                        dict_jobs[date][member] += jobs_to_sort
                        jobs_to_sort = []
                    if len(sorted_jobs_list) > 1:
                        jobs_to_sort.append(job)
                        previous_job = job

        # sort jobs
        for date in dict_jobs.keys():
            for member in dict_jobs[date].keys():
                dict_jobs[date][member].sort(
                    key=lambda job: (int(job.chunk) if sections_running_type_map.get(
                        job.section, 'once') == 'chunk' and job.chunk is not None else
                                     num_chunks))

        return dict_jobs

    def _create_fake_dates_members(self, filtered_jobs_list):
        """Using the list of jobs provided, creates clones of these jobs and modifies names conditioned
        on job.date, job.member values (testing None).
        The purpose is that all jobs share the same name structure.

        :param filtered_jobs_list: A list of jobs of only those that comply with certain criteria,
        e.g. those belonging to a user defined job type for wrapping. \n
        :type filtered_jobs_list: List() of Job Objects \n
        :return filtered_jobs_fake_date_member: List of fake jobs. \n
        :rtype filtered_jobs_fake_date_member: List of Job Objects \n
        :return fake_original_job_map: Dictionary that maps fake job to original one. \n
        :rtype fake_original_job_map: Dictionary Key: Job Object, Value: Job Object
        """
        filtered_jobs_fake_date_member = []
        fake_original_job_map = dict()

        import copy
        for job in filtered_jobs_list:
            fake_job = None
            # running once and synchronize date
            if job.date is None and job.member is None:
                # Declare None values as if they were the last items in corresponding list
                date = self._date_list[-1]
                member = self._member_list[-1]
                fake_job = copy.deepcopy(job)
                # Use previous values to modify name of fake job
                fake_job.name = fake_job.name.split('_', 1)[0] + "_" + self._get_date(date) + "_" \
                                + member + "_" + fake_job.name.split("_", 1)[1]
                # Filling list of fake jobs, only difference is the name
                filtered_jobs_fake_date_member.append(fake_job)
                # Mapping fake jobs to original ones
                fake_original_job_map[fake_job] = job
            # running date or synchronize member
            elif job.member is None:
                # Declare None values as if it were the last items in corresponding list
                member = self._member_list[-1]
                fake_name = job.name.split('_', 2)[0] + "_" + job.name.split('_', 2)[
                    1] + "_" + member + "_" + job.name.split("_", 2)[2]
                fake_job = Job(fake_name, 0, Status.WAITING, 0)
                # Filling list of fake jobs, only difference is the name
                filtered_jobs_fake_date_member.append(fake_job)
                # Mapping fake jobs to original ones
                fake_original_job_map[fake_job] = job
            # There was no result
            if fake_job is None:
                filtered_jobs_fake_date_member.append(job)

        return filtered_jobs_fake_date_member, fake_original_job_map

    def _get_date(self, date):
        """Parses a user defined Date (from [experiment] DATELIST)
        to return a special String representation of that Date.

        :param date: String representation of a date in format YYYYYMMdd. \n
        :type date: String \n
        :return: String representation of date according to format. \n
        :rtype: String \n
        """
        date_format = ''
        if date.hour > 1:
            date_format = 'H'
        if date.minute > 1:
            date_format = 'M'
        str_date = date2str(date, date_format)
        return str_date

    def __len__(self):
        return self.job_list.__len__()

    def get_date_list(self):
        """Get inner date list.

        :return: date list
        :rtype: list
        """
        return self._date_list

    def get_member_list(self):
        """Get inner member list.

        :return: member list
        :rtype: list
        """
        return self._member_list

    def get_chunk_list(self):
        """Get inner chunk list.

        :return: chunk list
        :rtype: list
        """
        return self._chunk_list

    def get_job_list(self):
        """Get inner job list.

        :return: job list
        :rtype: list
        """
        return self.job_list

    def get_date_format(self):
        date_format = ''
        for date in self.get_date_list():
            if date.hour > 1:
                date_format = 'H'
            if date.minute > 1:
                date_format = 'M'
        return date_format

    def copy_ordered_jobs_by_date_member(self):
        pass  # pragma: no cover

    def get_ordered_jobs_by_date_member(self, wrapper_name: str):
        """Get the dictionary of jobs ordered according to wrapper's
        expression divided by date and member.

        :param wrapper_name: name of the wrapper
        :type wrapper_name: str
        :return: dictionary of jobs ordered by date and member
        :rtype: dict
        """

        if len(self._ordered_jobs_by_date_member) > 0:
            return self._ordered_jobs_by_date_member[wrapper_name]

    def get_completed(self, platform=None, wrapper=False):
        """Returns a list of completed jobs

        :param wrapper:
        :param platform: job platform
        :type platform: HPCPlatform
        :return: completed jobs
        :rtype: list
        """

        completed_jobs = [job for job in self.job_list if (platform is None or
                                                           job.platform.name == platform.name) and job.status == Status.COMPLETED]
        if wrapper:
            return [job for job in completed_jobs if job.packed is False]
        return completed_jobs

    def get_uncompleted(self, platform=None, wrapper=False):
        """Returns a list of completed jobs.

        :param wrapper:
        :param platform: job platform
        :type platform: HPCPlatform
        :return: completed jobs
        :rtype: list
        """
        uncompleted_jobs = [job for job in self.job_list if
                            (platform is None or job.platform.name == platform.name) and
                            job.status != Status.COMPLETED]

        if wrapper:
            return [job for job in uncompleted_jobs if job.packed is False]
        return uncompleted_jobs

    def get_submitted(self, platform=None, hold=False, wrapper=False):
        """Returns a list of submitted jobs.

        :param wrapper:
        :param hold:
        :param platform: job platform
        :type platform: HPCPlatform
        :return: submitted jobs
        :rtype: list
        """
        submitted = list()
        if hold:
            submitted = [job for job in self.job_list if (platform is None or
                                                          job.platform.name == platform.name) and job.status == Status.SUBMITTED
                         and job.hold == hold]
        else:
            submitted = [job for job in self.job_list if (platform is None or
                                                          job.platform.name == platform.name) and job.status == Status.SUBMITTED]
        if wrapper:
            return [job for job in submitted if job.packed is False]
        return submitted

    def get_running(self, platform=None, wrapper=False):
        """Returns a list of jobs running.

        :param wrapper:
        :param platform: job platform
        :type platform: HPCPlatform
        :return: running jobs
        :rtype: list
        """
        running = [job for job in self.job_list if (platform is None or
                                                    job.platform.name == platform.name) and job.status == Status.RUNNING]
        if wrapper:
            return [job for job in running if job.packed is False]
        return running

    def get_queuing(self, platform=None, wrapper=False):
        """Returns a list of jobs queuing.

        :param wrapper:
        :param platform: job platform
        :type platform: HPCPlatform
        :return: queuedjobs
        :rtype: list
        """
        queuing = [job for job in self.job_list if (platform is None or
                                                    job.platform.name == platform.name) and job.status == Status.QUEUING]
        if wrapper:
            return [job for job in queuing if job.packed is False]
        return queuing

    def get_failed(self, platform=None, wrapper=False):
        """Returns a list of failed jobs.

        :param wrapper:
        :param platform: job platform
        :type platform: HPCPlatform
        :return: failed jobs
        :rtype: list
        """
        failed = [job for job in self.job_list if (platform is None or
                                                   job.platform.name == platform.name) and job.status == Status.FAILED]
        if wrapper:
            return [job for job in failed if job.packed is False]
        return failed

    def get_unsubmitted(self, platform=None, wrapper=False):
        """Returns a list of unsubmitted jobs.

        :param wrapper:
        :param platform: job platform
        :type platform: HPCPlatform
        :return: all jobs
        :rtype: list
        """
        unsubmitted = [job for job in self.job_list if (platform is None or
                                                        job.platform.name == platform.name) and (
                               job.status != Status.SUBMITTED and
                               job.status != Status.QUEUING and job.status != Status.RUNNING)]

        if wrapper:
            return [job for job in unsubmitted if job.packed is False]
        else:
            return unsubmitted

    def get_all(self, platform=None, wrapper=False):
        """Returns a list of all jobs.

        :param wrapper:
        :param platform: job platform
        :type platform: HPCPlatform
        :return: all jobs
        :rtype: list
        """
        all_jobs = [job for job in self.job_list]

        if wrapper:
            return [job for job in all_jobs if job.packed is False]
        else:
            return all_jobs

    def update_two_step_jobs(self):
        if len(self.jobs_to_run_first) > 0:
            self.jobs_to_run_first = [job for job in self.jobs_to_run_first
                                      if job.status != Status.COMPLETED]
            keep_running = False
            for job in self.jobs_to_run_first:
                # job is parent of itself
                running_parents = [parent for parent in job.parents if parent.status !=
                                   Status.WAITING and parent.status != Status.FAILED]
                if len(running_parents) == len(job.parents):
                    keep_running = True
            if len(self.jobs_to_run_first) > 0 and keep_running is False:
                raise AutosubmitCritical("No more jobs to run first, there were still pending jobs "
                                         "but they're unable to run without their parents or there are failed jobs.",
                                         7014)

    def parse_jobs_by_filter(self, unparsed_jobs, two_step_start=True):
        select_jobs_by_name = ""  # job_name
        if "&" in unparsed_jobs:  # If there are explicit jobs add them
            jobs_to_check = unparsed_jobs.split("&")
            select_jobs_by_name = jobs_to_check[0]
            unparsed_jobs = jobs_to_check[1]
        if ";" not in unparsed_jobs:
            if '[' in unparsed_jobs:
                select_all_jobs_by_section = unparsed_jobs
                filter_jobs_by_section = ""
            else:
                select_all_jobs_by_section = ""
                filter_jobs_by_section = unparsed_jobs
        else:
            aux = unparsed_jobs.split(';')
            select_all_jobs_by_section = aux[0]
            filter_jobs_by_section = aux[1]
        if two_step_start:
            try:
                self.jobs_to_run_first = self.get_job_related(
                    select_jobs_by_name=select_jobs_by_name,
                    select_all_jobs_by_section=select_all_jobs_by_section,
                    filter_jobs_by_section=filter_jobs_by_section)
            except Exception:
                raise AutosubmitCritical(f"Check the {unparsed_jobs} format."
                                         "\nFirst filter is optional ends with '&'."
                                         "\nSecond filter ends with ';'."
                                         "\nThird filter must contain '['. ")
        else:
            try:
                self.rerun_job_list = self.get_job_related(select_jobs_by_name=select_jobs_by_name,
                                                           select_all_jobs_by_section=select_all_jobs_by_section,
                                                           filter_jobs_by_section=filter_jobs_by_section,
                                                           two_step_start=two_step_start)
            except Exception:
                raise AutosubmitCritical(f"Check the {unparsed_jobs} format."
                                         "\nFirst filter is optional ends with '&'."
                                         "\nSecond filter ends with ';'."
                                         "\nThird filter must contain '['. ")

    def get_job_related(self, select_jobs_by_name="", select_all_jobs_by_section="",
                        filter_jobs_by_section="", two_step_start=True):
        """
        :param two_step_start:
        :param select_jobs_by_name: job name
        :param select_all_jobs_by_section: section name
        :param filter_jobs_by_section: section, date , member? , chunk?
        :return: jobs_list names
        :rtype: list
        """
        ultimate_jobs_list = []
        jobs_filtered = []
        jobs_date = []
        # First Filter {select job by name}
        if select_jobs_by_name != "":
            jobs_by_name = [job for job in self.job_list if
                            re.search("(^|[^0-9a-z_])" + job.name.lower() + "([^a-z0-9_]|$)",
                                      select_jobs_by_name.lower()) is not None]
            jobs_by_name_no_expid = [job for job in self.job_list if
                                     re.search("(^|[^0-9a-z_])" + job.name.lower()[5:] +
                                               "([^a-z0-9_]|$)", select_jobs_by_name.lower()) is not None]
            ultimate_jobs_list.extend(jobs_by_name)
            ultimate_jobs_list.extend(jobs_by_name_no_expid)

        # Second Filter { select all }
        if select_all_jobs_by_section != "":
            all_jobs_by_section = [job for job in self.job_list if
                                   re.search("(^|[^0-9a-z_])" + job.section.upper() +
                                             "([^a-z0-9_]|$)", select_all_jobs_by_section.upper()) is not None]
            ultimate_jobs_list.extend(all_jobs_by_section)
        # Third Filter N section { date , member? , chunk?}
        # Section[date[member][chunk]]
        # filter_jobs_by_section="SIM[20[C:000][M:1]],DA[20 21[M:000 001][C:1]]"
        if filter_jobs_by_section != "":
            section_name = ""
            section_dates = ""
            section_chunks = ""
            section_members = ""
            jobs_final = list()
            for complete_filter_by_section in filter_jobs_by_section.split(','):
                section_list = complete_filter_by_section.split('[')
                section_name = section_list[0].strip('[]')
                section_dates = section_list[1].strip('[]')
                if 'c' in section_list[2].lower():
                    section_chunks = section_list[2].strip('cC:[]')
                elif 'm' in section_list[2].lower():
                    section_members = section_list[2].strip('Mm:[]')
                if len(section_list) > 3:
                    if 'c' in section_list[3].lower():
                        section_chunks = section_list[3].strip('Cc:[]')
                    elif 'm' in section_list[3].lower():
                        section_members = section_list[3].strip('mM:[]')

                if section_name != "":
                    jobs_filtered = [job for job in self.job_list if
                                     re.search("(^|[^0-9a-z_])" + job.section.upper() +
                                               "([^a-z0-9_]|$)", section_name.upper()) is not None]
                if section_dates != "":
                    jobs_date = [job for job in jobs_filtered if
                                 re.search("(^|[^0-9a-z_])" + date2str(job.date, job.date_format) +
                                           "([^a-z0-9_]|$)", section_dates.lower()) is not None or
                                 job.date is None]

                if section_chunks != "" or section_members != "":
                    jobs_final = [job for job in jobs_date if (section_chunks == "" or
                                                               re.search(
                                                                   "(^|[^0-9a-z_])" + str(job.chunk) + "([^a-z0-9_]|$)",
                                                                   section_chunks) is not None) and (
                                          section_members == "" or
                                          re.search("(^|[^0-9a-z_])" + str(job.member) + "([^a-z0-9_]|$)",
                                                    section_members.lower()) is not None)]
                ultimate_jobs_list.extend(jobs_final)
        # Duplicates out
        ultimate_jobs_list = list(set(ultimate_jobs_list))
        Log.debug(f"List of jobs filtered by TWO_STEP_START parameter:\n{[job.name for job in ultimate_jobs_list]}")
        return ultimate_jobs_list

    def get_ready(self, platform=None, hold=False, wrapper=False):
        """Returns a list of ready jobs.

        :param wrapper:
        :param hold:
        :param platform: job platform
        :type platform: HPCPlatform
        :return: ready jobs
        :rtype: list
        """
        ready = [job for job in self.job_list if
                 (platform is None or platform == "" or job.platform.name == platform.name) and
                 job.status == Status.READY and job.hold is hold]

        if wrapper:
            return [job for job in ready if job.packed is False]
        return ready

    def get_prepared(self, platform=None):
        """Returns a list of prepared jobs.

        :param platform: job platform
        :type platform: HPCPlatform
        :return: prepared jobs
        :rtype: list
        """
        prepared = [job for job in self.job_list if (platform is None or
                                                     job.platform.name == platform.name) and job.status == Status.PREPARED]
        return prepared

    def get_delayed(self, platform=None):
        """Returns a list of delayed jobs.

        :param platform: job platform
        :type platform: HPCPlatform
        :return: delayed jobs
        :rtype: list
        """
        delayed = [job for job in self.job_list if (platform is None or
                                                    job.platform.name == platform.name) and job.status == Status.DELAYED]
        return delayed

    def get_waiting(self, platform=None, wrapper=False):
        """Returns a list of jobs waiting.

        :param wrapper:
        :param platform: job platform
        :type platform: HPCPlatform
        :return: waiting jobs
        :rtype: list
        """
        waiting_jobs = [job for job in self.job_list if (platform is None or
                                                         job.platform.name == platform.name) and job.status == Status.WAITING]
        if wrapper:
            return [job for job in waiting_jobs if job.packed is False]
        return waiting_jobs

    def get_waiting_remote_dependencies(self, platform_type='slurm'.lower()):
        """Returns a list of jobs waiting on slurm scheduler.

        :param platform_type: platform type
        :type platform_type: str
        :return: waiting jobs
        :rtype: list

        """
        waiting_jobs = [job for job in self.job_list if (
                job.platform.type == platform_type and job.status == Status.WAITING)]
        return waiting_jobs

    def get_held_jobs(self, platform=None):
        """Returns a list of jobs in the platforms (Held).

        :param platform: job platform
        :type platform: HPCPlatform
        :return: jobs in platforms
        :rtype: list
        """
        return [job for job in self.job_list if (platform is None or
                                                 job.platform.name == platform.name) and job.status == Status.HELD]

    def get_unknown(self, platform=None, wrapper=False):
        """Returns a list of jobs on unknown state.

        :param wrapper:
        :param platform: job platform
        :type platform: HPCPlatform
        :return: unknown state jobs
        :rtype: list
        """
        submitted = [job for job in self.job_list if (platform is None or
                                                      job.platform.name == platform.name) and job.status == Status.UNKNOWN]
        if wrapper:
            return [job for job in submitted if job.packed is False]
        return submitted

    def get_in_queue(self, platform=None, wrapper=False):
        """Returns a list of jobs in the platforms (Submitted, Running, Queuing, Unknown,Held).

        :param wrapper:
        :param platform: job platform
        :type platform: HPCPlatform
        :return: jobs in platforms
        :rtype: list
        """

        in_queue = self.get_submitted(platform) + self.get_running(platform) + self.get_queuing(
            platform) + self.get_unknown(platform) + self.get_held_jobs(platform)
        if wrapper:
            return [job for job in in_queue if job.packed is False]
        return in_queue

    def update_wrappers_references(self):
        """Updates the job references after a graph recreation to point to the actual Job objects and remove completed wrappers"""

        for name, jobs in list(self.packages_dict.items()):
            new_jobs = []
            wrapper_id = int(jobs[0].id)
            if wrapper_id in self.job_package_map:
                for job in (job for job in jobs):
                    job_ref = self.get_job_by_name(job.name)
                    if job_ref:
                        new_jobs.append(job_ref)
                if len(new_jobs) == 0:
                    self.packages_dict.pop(name, None)
                    self.job_package_map.pop(wrapper_id, None)
                else:
                    self.packages_dict[name] = new_jobs
                    self.job_package_map[wrapper_id].job_list = new_jobs
            else:
                raise AutosubmitCritical(f"Wrapper job with id {wrapper_id} not found in job_package_map", 7001)

    def continue_run(self) -> bool:
        """Loads the next possible jobs and edges from the database.

        :rtype : bool
        :return: True if there are active jobs to run, False otherwise.
        """
        # Updates job_list
        self.recover_logs()
        save_jobs, save_edges = self.update_list(self._as_conf)
        if save_jobs:
            self.save_jobs()
        if save_edges:
            self.save_edges()

        self._load_graph(full_load=False)
        self.unload_finished_jobs()
        for job in self.job_list:
            if not job.updated:
                job.submitter = self.submitter
                job.update_parameters(self._as_conf, set_attributes=True, reset_logs=False if job.status in (
                        self._IN_SCHEDULER + self._FINAL_STATUSES) else True)
        Log.debug(f"Jobs loaded: {len(self.job_list)}")
        Log.debug(f"Edges loaded: {len(self.graph_dict)}")
        self.update_wrappers_references()
        return len(self.get_active()) > 0

    def unload_finished_jobs(self):
        """Unloads finished jobs and edges from the memory"""
        jobs_to_unload = [
            job for job in self.job_list
            if (
                    (job.status == Status.FAILED
                     and job.fail_count >= job.retrials
                     and job.log_recovery_call_count > job.fail_count)
                    or
                    (job.status in (Status.COMPLETED, Status.SKIPPED)
                     and job.log_recovery_call_count > job.fail_count)
            )
        ]
        # update edges completion status before removing them
        for job in (job for job in jobs_to_unload):
            for child in job.children:
                self.graph.edges[job.name, child.name]['completion_status'] = "COMPLETED"
            for parent in job.parents:
                if self.graph.has_edge(parent.name, job.name):
                    self.graph.edges[parent.name, job.name]['completion_status'] = "COMPLETED"
        if jobs_to_unload:
            self.save_edges()
        for job in (job for job in jobs_to_unload):
            for child in job.children:
                if self.graph.has_edge(job.name, child.name):
                    self.graph.remove_edge(job.name, child.name)
                child.parents.discard(job)


            for parent in list(job.parents):
                if self.graph.has_edge(parent.name, job.name):
                    self.graph.remove_edge(parent.name, job.name)
                parent.children.discard(job)
            job.children.clear()
            job.parents.clear()
            job.platform = None
            self.graph.remove_node(job.name)

    def get_active(self, platform=None, wrapper=False):
        """Returns a list of active jobs (In platforms queue + Ready).

        :param wrapper:
        :param platform: job platform
        :type platform: HPCPlatform
        :return: active jobs
        :rtype: list
        """

        active = (self.get_in_queue(platform) + self.get_ready(
            platform=platform, hold=True) + self.get_ready(platform=platform, hold=False) +
                  self.get_delayed(platform=platform) + [failed_job for failed_job in self.get_failed(platform=platform)
                                                         if failed_job.fail_count < failed_job.retrials])

        tmp = [job for job in active if job.hold and not (job.status ==
                                                          Status.SUBMITTED or job.status == Status.READY or job.status == Status.DELAYED)]
        if len(tmp) == len(active):  # IF only held jobs left without dependencies satisfied
            if len(tmp) != 0 and len(active) != 0:
                raise AutosubmitCritical(
                    "Only Held Jobs active. Exiting Autosubmit (TIP: "
                    "This can happen if suspended or/and Failed jobs are found on the workflow)",
                    7066)
            active = []
        return active

    def get_job_by_name(self, name: str) -> Optional[Job]:
        """Returns the job that its name matches parameter name.

        :parameter name: name to look for
        :type name: str
        :return: found job
        :rtype: job
        """
        for job in self.job_list:
            if job.name == name:
                return job
        return None

    def get_jobs_by_section(self, section_list: list, banned_jobs: list = None,
                            get_only_non_completed: bool = False) -> list:
        """Get jobs by section.

        This method filters jobs based on the provided section list and banned jobs list.
        It can also filter out completed jobs if specified.

        Parameters:
        section_list (list): List of sections to filter jobs by.
        banned_jobs (list, optional): List of jobs names to exclude from the result.
        Defaults to an empty list.
        get_only_non_completed (bool, optional): If True, only non-completed jobs are included.
        Defaults to False.

        Returns:
        list: List of jobs that match the criteria.
        """
        if banned_jobs is None:
            banned_jobs = []

        jobs = []
        for job in self.job_list:
            if job.section.upper() in section_list and job.name not in banned_jobs:
                if get_only_non_completed:
                    if job.status != Status.COMPLETED:
                        jobs.append(job)
                else:
                    jobs.append(job)
        return jobs

    def sort_by_name(self):
        """Returns a list of jobs sorted by name.

        :return: jobs sorted by name
        :rtype: list
        """
        return sorted(self.job_list, key=lambda k: k.name)

    def sort_by_id(self):
        """Returns a list of jobs sorted by id.

        :return: jobs sorted by ID
        :rtype: list
        """
        return sorted(self.job_list, key=lambda k: k.id)

    def sort_by_type(self):
        """Returns a list of jobs sorted by type.

        :return: job sorted by type
        :rtype: list
        """
        return sorted(self.job_list, key=lambda k: k.type)

    def sort_by_status(self):
        """Returns a list of jobs sorted by status.

        :return: job sorted by status
        :rtype: list
        """
        return sorted(self.job_list, key=lambda k: k.status)

    def save_jobs(self, jobs_to_save: Optional[List[Job]] = None):
        """Persists the job list"""

        if not self.disable_save:
            Log.info("Saving jobs to the database...")
            if not jobs_to_save:
                self.update_status_log()
                self.dbmanager.save_jobs(self.job_list)
            else:
                self.dbmanager.save_jobs(jobs_to_save)
            Log.info("Jobs saved.")

    def load_jobs(self, full_load: bool = False, load_failed_jobs: bool = False,
                  only_finished: bool = False) -> list[Job]:
        """Load jobs from the database.

        Load job nodes from persistent storage into memory and return them as a list
        of ``Job`` instances.

        :param full_load: If ``True``, load all jobs and edges; otherwise load only the
            subset necessary for continued execution.
        :param load_failed_jobs: If ``True``, include jobs in failed states when loading.
        :param only_finished: If ``True``, load only finished jobs (completed, failed or skipped).
        :return: A list of loaded ``Job`` objects.
        :rtype: List[Job]
        :raises Exception: If a database access error occurs while loading jobs.
        """
        nodes = self.dbmanager.load_jobs(full_load, load_failed_jobs,
                                         only_finished=only_finished,
                                         members=self.run_members)
        self.total_size, self.completed_size, self.failed_size = self.dbmanager.get_job_list_size()
        return nodes

    def load_job_by_name(self, job_name: str) -> Optional[Job]:
        """
        Loads a job by its name from the database.

        :param job_name: Name of the job to load.
        :type job_name: str
        :return: The loaded job object or None if not found.
        :rtype: Job or None
        """
        node = self.dbmanager.load_job_by_name(job_name)
        if node:
            edges = self.dbmanager.load_edges([node], full_load=False)
            self._add_job_node_with_platform(node)
            for edge in (edge for edge in edges if
                         edge.get("e_from", "") in self.graph.nodes and edge.get("e_to", "") in self.graph.nodes):
                self._add_edge_and_parent(edge)
        # get node from the graph
        return self.graph.nodes.get(job_name, None)['job']

    def save_edges(self):
        """
        Persists the job edges
        """
        if not self.disable_save:
            Log.info("Saving edges to the database...")
            self.dbmanager.save_edges(self.graph_dict)
            Log.info("Edges saved.")

    def load_edges(self, job_list, full_load=False) -> Dict[str, Any]:
        """Loads the job edges"""
        return self.dbmanager.load_edges(job_list, full_load)

    def update_status_log(self):

        exp_path = os.path.join(BasicConfig.LOCAL_ROOT_DIR, self.expid)
        tmp_path = os.path.join(exp_path, BasicConfig.LOCAL_TMP_DIR)
        aslogs_path = os.path.join(tmp_path, BasicConfig.LOCAL_ASLOG_DIR)
        Log.reset_status_file(os.path.join(aslogs_path, "jobs_active_status.log"), "status")
        Log.reset_status_file(os.path.join(aslogs_path, "jobs_failed_status.log"), "status_failed")
        job_list = self.get_completed()[-5:] + self.get_in_queue() + self.get_failed()
        if len(job_list) > 0:
            Log.status("\n{0:<35}{1:<15}{2:<15}{3:<20}{4:<15}", "Job Name",
                       "Job Id", "Job Status", "Job Platform", "Job Queue")
            Log.status_failed("\n{0:<35}{1:<15}{2:<15}{3:<20}{4:<15}", "Job Name",
                              "Job Id", "Job Status", "Job Platform", "Job Queue")

        for job in job_list:
            platform_name = job.platform_name if job.platform_name else "no-platform"
            if job.platform_name and job.platform:
                queue = job.queue if job.queue else "no-scheduler"
            else:
                queue = "no-scheduler"
            job_id = job.id if job.id else "no-id"
            if job.status == Status.FAILED:
                try:
                    Log.status_failed("{0:<35}{1:<15}{2:<15}{3:<20}{4:<15}", job.name, job_id, Status(
                    ).VALUE_TO_KEY[job.status], platform_name, queue)
                except Exception:
                    Log.debug(f"Couldn't print job status for job {job.name}")
            else:
                try:
                    Log.status("{0:<35}{1:<15}{2:<15}{3:<20}{4:<15}", job.name, job_id, Status(
                    ).VALUE_TO_KEY[job.status], platform_name, queue)
                except Exception:
                    Log.debug(f"Couldn't print job status for job {job.name}")

    def update_from_file(self, store_change: bool = True) -> None:
        """Update jobs status  from an external status file.

        Reads job status updates from a file and applies them to the job list.
        If `store_change` is True, the update file is archived after processing.

        :param store_change: If True, rename the update file after processing to prevent reloading.
        :raises FileNotFoundError: If the status directory cannot be created.
        :raises ValueError: If a line in the update file has invalid format.
        """
        # TODO: Warn users that this file has changed it's location ( from pkl/ -> status/)

        if not self._update_file_path.exists():
            return

        Log.info(f"Loading updated list: {self._update_file_path.name}")
        try:
            with open(self._update_file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, start=1):
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue

                    parts = line.split()
                    if len(parts) < 2:
                        Log.warning(f"Skipping invalid line {line_num} in {self._update_file_path.name}: '{line}'")
                        continue

                    job_name, status_str = parts[0], parts[1]
                    status_str = status_str.upper()

                    if status_str not in Status.VALUE_TO_KEY:
                        Log.warning(f"Invalid status '{status_str}' on line {line_num}")
                        continue

                    job = self.get_job_by_name(job_name)

                    if not job:
                        Log.warning(f"Job '{job_name}' not found (line {line_num})")
                        continue

                    try:
                        new_status = self._stat_val.retval(status_str)
                        job.status = new_status
                        job._fail_count = 0
                        Log.result(f"Updated job '{job_name}' to status '{status_str}'")
                    except (ValueError, AttributeError) as e:
                        Log.warning(f"Invalid status '{status_str}' for job '{job_name}' (line {line_num}): {e}")
        except (IOError, OSError) as e:
            Log.warning(f"Failed to read update file {self._update_file_path}: {e}")

        if store_change:
            output_date = strftime("%Y%m%d_%H%M", localtime())
            archived_file_path = self._set_status_path / f"{self._update_file}_{output_date}"
            self._update_file_path.rename(archived_file_path)
            Log.result(f"Renamed update file to prevent reloading in each iteration: {archived_file_path.name}")

    def get_skippable_jobs(self, jobs_in_wrapper):
        job_list_skip = [job for job in self.get_job_list() if job.skippable == "true" and
                         (job.status == Status.QUEUING or job.status == Status.RUNNING or
                          job.status == Status.COMPLETED or job.status == Status.READY) and
                         jobs_in_wrapper.find(job.section) == -1]
        skip_by_section = dict()
        for job in job_list_skip:
            if job.section not in skip_by_section:
                skip_by_section[job.section] = [job]
            else:
                skip_by_section[job.section].append(job)
        return skip_by_section

    @property
    def parameters(self):
        """
        List of parameters common to all jobs
        :return: parameters
        :rtype: dict
        """
        return self._parameters

    @parameters.setter
    def parameters(self, value):
        self._parameters = value

    def check_checkpoint(self, job, parent):
        """ Check if a checkpoint step exists for this edge"""
        return job.get_checkpoint_files(parent.name)

    def check_special_status(self) -> list[Job]:
        """Check if all parents of a job have the correct status for checkpointing.

        :returns: List of jobs that fulfill the special conditions for checkpointing.
        :rtype: list[Job]
        """
        jobs_to_check: list[Job] = []
        for current_job in [current_job for current_job in self.job_list if current_job.status == Status.WAITING]:
            self._check_checkpoint(current_job)
            parents_edge_info = self.get_parents_edges(current_job.name)
            parents_nodes = {parent_name: self.graph.nodes[parent_name]["job"] for parent_name in
                             parents_edge_info.keys()}
            non_completed, completed = self._count_parents_status(current_job, parents_edge_info, parents_nodes)
            self._update_db_edges_completion_status(completed, non_completed, current_job)
            if len(non_completed) == 0 and len(completed) > 0:
                # If all parents are completed, we can run the job
                jobs_to_check.append(current_job)
        return jobs_to_check

    @staticmethod
    def _check_checkpoint(job: Job) -> None:
        """
        Check if a job has a checkpoint.

        :param job: The job to check.
        """
        # This will be true only when used under setstatus/run
        if job.platform and job.platform.connected:
            job.get_checkpoint_files()

    def _count_parents_status(
            self,
            job: Job,
            parents_edge_info: dict,
            parents_nodes: dict
    ) -> Tuple[List[Job], List[Job]]:
        """Count the number of completed and non-completed parent jobs for a given job.

        :param job: The job whose parent statuses are to be checked.
        :type job: Job
        :param parents_edge_info: Dictionary or list containing information about the edges from parent jobs.
        :type parents_edge_info: dict
        :param parents_nodes: Dictionary mapping parent job names to Job objects.
        :type parents_nodes: dict
        :returns: A tuple containing two lists:
            - non_completed_parents: List of parent jobs that are not completed.
            - completed_parents: List of parent jobs that are completed.
        :rtype: Tuple[List[Job], List[Job]]
        """
        non_completed = []
        completed = []
        for parent_name, edge_info in parents_edge_info.items():
            parent = parents_nodes[parent_name]
            p_status = parent.status
            edge_status = Status.KEY_TO_VALUE[edge_info["min_trigger_status"].upper()]
            fail_ok = edge_info.get("fail_ok", False)
            from_step = edge_info.get("from_step", 0)

            # SUSPENDED
            if p_status == Status.SUSPENDED:
                non_completed.append(parent)
            # COMPLETED or SKIPPED
            elif p_status in [Status.COMPLETED, Status.SKIPPED]:
                if edge_status in [Status.COMPLETED, Status.SKIPPED]:
                    completed.append(parent)
                elif edge_status == Status.FAILED and fail_ok or (job.current_checkpoint_step >= from_step > 0):
                    completed.append(parent)
                else:
                    non_completed.append(parent)
            # FAILED
            elif p_status == Status.FAILED:
                if edge_status == Status.FAILED:
                    completed.append(parent)
                elif edge_status in [Status.COMPLETED, Status.SKIPPED]:
                    if fail_ok or (job.current_checkpoint_step >= from_step > 0):
                        completed.append(parent)
                    else:
                        non_completed.append(parent)
                else:
                    non_completed.append(parent)
            # RUNNING
            elif p_status == Status.RUNNING:
                if edge_status == Status.RUNNING:
                    if job.current_checkpoint_step >= from_step > 0:
                        completed.append(parent)
                    elif from_step == 0:
                        completed.append(parent)
                    else:
                        non_completed.append(parent)
                else:
                    non_completed.append(parent)
            # Other statuses
            else:
                if p_status == edge_status:
                    completed.append(parent)
                elif Status.VALUE_TO_KEY[p_status] in Status.LOGICAL_ORDER_SUCCESS_WORKFLOW:
                    idx_parent = Status.LOGICAL_ORDER.index(Status.VALUE_TO_KEY[p_status])
                    idx_edge = Status.LOGICAL_ORDER.index(Status.VALUE_TO_KEY[edge_status])
                    if idx_parent >= idx_edge:
                        completed.append(parent)
                    else:
                        non_completed.append(parent)
                else:
                    non_completed.append(parent)

        return non_completed, completed

    def _update_db_edges_completion_status(self, finished_parents: List[Job], non_finished_parents: List[Job], child: Job) -> None:
        """ Update the completion status of edges in the database.

        :param finished_parents: List of parent jobs that have finished.
        :type finished_parents: List[Job]
        :param non_finished_parents: List of parent jobs that have not finished.
        :type non_finished_parents: List[Job]
        :param child: The child job whose edges are being updated.
        :type child: Job
        """
        for parent in finished_parents:
            if self.graph.edges.get((parent.name, child.name)):
                self.graph.edges[parent.name, child.name]['completion_status'] = "COMPLETED"

        for parent in non_finished_parents:
            if self.graph.edges.get((parent.name, child.name)):
                status = "RUNNING" if parent.status == Status.RUNNING else "WAITING"
                self.graph.edges[parent.name, child.name]['completion_status'] = status

    def _recover_log(self, job: Job) -> None:
        """Recover the log for a given job.
        :param job: The job object to recover the log for.
        :type job: Job
        """
        if str(self._as_conf.platforms_data.get(job.name, {}).get('DISABLE_RECOVERY_THREADS', "false")).lower() == "true":
            job.retrieve_logfiles()
        else:
            # Submit time is not stored in the _STAT, so failures in the log recovery can lead to missing the submit time
            job.write_submit_time()
            job.platform.add_job_to_log_recover(job)

        job.log_recovery_call_count += 1

    def recover_logs(self, new_run: bool = False):
        """Update jobs' log recovered status.

        Iterate over the current job list and mark jobs whose stdout/stderr logs
        have been recovered.

        :param new_run: If True, also mark the job as updated for a new run.
        :type new_run: bool
        """
        if new_run:
            # load all jobs without updated_log from db
            jobs_to_recover = self.load_jobs(full_load=True, only_finished=True)
            for job in jobs_to_recover:
                ref_fail_count = copy.copy(job.fail_count)
                # Updated_log indicates the last downloaded log
                # Failed jobs could missed some log in between if run was force stopped
                job.fail_count = copy.copy(job.updated_log)
                for i in range(job.updated_log, ref_fail_count):
                    self._recover_log(job)
                    job.fail_count += 1
                job.fail_count = ref_fail_count

        else:
            jobs_to_recover = [job for job in self.job_list if
                               not getattr(job, "x11", False) and job.status in self._FINAL_STATUSES and job.log_recovery_call_count <= job.fail_count]
            for job in jobs_to_recover:
                self._recover_log(job)

    def check_completed_jobs_after_recovery(self):
        for job in (job for job in self.job_list if job.status == Status.COMPLETED):
            if any(parent.status == Status.WAITING for parent in job.parents):
                job.status = Status.WAITING
                job.id = None
                Log.info(f"Job {job.name} was marked as COMPLETED but has WAITING parents. Resetting to WAITING.")

    def update_list(
            self,
            as_conf: AutosubmitConfig,
            store_change: bool = True,
            fromSetStatus: bool = False,
    ) -> bool:
        """Update the job list, resetting failed jobs and changing to READY all WAITING jobs with all parents in their target status.

        :param as_conf: Autosubmit configuration object.
        :type as_conf: AutosubmitConfig
        :param store_change: Whether to store changes after update.
        :type store_change: bool, optional
        :param fromSetStatus: If called from set status.
        :type fromSetStatus: bool, optional
        :param submitter: Submitter object (unused).
        :type submitter: object, optional
        :param first_time: If this is the first run.
        :type first_time: bool, optional
        :return: True if any job status was updated, False otherwise.
        :rtype: bool
        """
        save_jobs = False
        if self.update_from_file(store_change):
            save_jobs = store_change
        Log.debug('Updating FAILED jobs')
        save_jobs |= self._update_failed_jobs(as_conf)
        save_jobs_, save_edges = self._handle_special_checkpoint_jobs()
        save_jobs |= save_jobs_
        save_jobs |= self._sync_completed_jobs()
        if not fromSetStatus:
            save_jobs |= self._update_waiting_and_delayed_jobs()
            save_jobs |= self._skip_jobs(as_conf)
        for job in self.get_ready():
            save_edges = True
            self._update_db_edges_completion_status(job.parents, [], job)
            job.set_ready_date()
            job.updated_log = 0

        self.update_two_step_jobs()

        Log.debug('Update finished')
        return save_jobs, save_edges

    def is_wrapper_still_running(self, job: Job) -> bool:
        """
        Check if the wrapper job for a given job is still running.

        :param job: The job to check.
        :type job: Job
        :return: True if the wrapper job is still running, False otherwise.
        :rtype: bool
        """
        job.packed = False
        if self.job_package_map and int(job.id) in self.job_package_map:
            job.packed = True
        return job.packed

    def _update_failed_jobs(self, as_conf: AutosubmitConfig) -> bool:
        """
        Update failed jobs, retrying them if possible or marking as FAILED.

        :param as_conf: Autosubmit configuration object.
        :type as_conf: AutosubmitConfig
        :return: True if any job status was updated, False otherwise.
        :rtype: bool
        """
        save = False
        for job in (job for job in self.get_failed() if not self.is_wrapper_still_running(job)):
            if as_conf.jobs_data[job.section].get("RETRIALS", None) is None:
                retrials = int(as_conf.get_retrials())
            else:
                retrials = int(job.retrials)
            if job.fail_count < retrials:
                job.inc_fail_count()
                tmp = [parent for parent in job.parents if parent.status == Status.COMPLETED]
                if len(tmp) == len(job.parents):
                    aux_job_delay = 0
                    if job.delay_retrials:
                        if ("+" == str(job.delay_retrials)[0] or "*" == str(job.delay_retrials)[0]):
                            aux_job_delay = int(job.delay_retrials[1:])
                        else:
                            aux_job_delay = int(job.delay_retrials)
                    if (as_conf.jobs_data[job.section].get("DELAY_RETRY_TIME", None) or aux_job_delay <= 0):
                        delay_retry_time = str(as_conf.get_delay_retry_time())
                    else:
                        delay_retry_time = job.retry_delay
                    if "+" in delay_retry_time:
                        retry_delay = (job.fail_count * int(delay_retry_time[:-1]) + int(delay_retry_time[:-1]))
                    elif "*" in delay_retry_time:
                        retry_delay = int(delay_retry_time[1:])
                        for retrial_amount in range(0, job.fail_count):
                            retry_delay += retry_delay * 10
                    else:
                        retry_delay = int(delay_retry_time)
                    if retry_delay > 0:
                        job.status = Status.DELAYED
                        job.delay_end = (datetime.datetime.now() + datetime.timedelta(seconds=retry_delay))
                        Log.debug(f"Resetting job: {job.name} status to: DELAYED for retrial...")
                    else:
                        job.status = Status.READY
                        Log.debug(f"Resetting job: {job.name} status to: READY for retrial...")
                    job.id = None
                    save = True
                else:
                    job.status = Status.WAITING
                    save = True
                    Log.debug(f"Resetting job: {job.name} status to: WAITING for parents completion...")
            else:
                job.status = Status.FAILED
                save = True
        return save

    def reset_jobs_on_first_run(self) -> None:
        """Reset fail count for jobs in WAITING, READY, DELAYED, or PREPARED status."""

        for job in [job for job in self.job_list if job.status in
                                                    [Status.WAITING, Status.READY, Status.DELAYED, Status.PREPARED, Status.FAILED]]:
            job.fail_count = 0
            if job.status == Status.FAILED:
                job.status = Status.WAITING
                Log.debug(f"Resetting job: {job.name} status to: WAITING on first run...")

    def _handle_special_checkpoint_jobs(self) -> Tuple[bool, bool]:
        """Set jobs that fulfill special checkpoint conditions to READY.

        Return a tuple with two booleans: (save_jobs, save_edges).
        """
        save_all = False
        for job in self.check_special_status():
            job.status = Status.READY
            job.id = None
            job.wrapper_type = None
            save_all = True
            Log.result(f"JOB: {job.name} was set to READY all parents are in the desired status.")
        return save_all, save_all

    def _sync_completed_jobs(self) -> bool:
        """Synchronize jobs with parents' completion status. If

        :return: True if any job status was updated, False otherwise.
        :rtype: bool
        """
        save = False
        for job in self.get_completed():
            if job.synchronize is not None and len(str(job.synchronize)) > 0:
                tmp = [parent for parent in job.parents if parent.status == Status.COMPLETED]
                if len(tmp) != len(job.parents):
                    tmp2 = [parent for parent in job.parents if
                            parent.status == Status.COMPLETED or
                            parent.status == Status.SKIPPED or parent.status == Status.FAILED]
                    if len(tmp2) == len(job.parents):
                        for parent in job.parents:
                            if parent.status != Status.COMPLETED:
                                job.status = Status.WAITING
                                save = True
                                Log.debug(
                                    f"Resetting sync job: {job.name} status to: WAITING for parents completion...")
                                break
                    else:
                        job.status = Status.WAITING
                        save = True
                        Log.debug(f"Resetting sync job: {job.name} status to: WAITING for parents completion...")
        return save

    def _update_waiting_and_delayed_jobs(self) -> bool:
        """Update jobs in WAITING or DELAYED status based on parent completion and delay timers.

        :param as_conf: Autosubmit configuration object.
        :type as_conf: AutosubmitConfig
        :return: True if any job status was updated, False otherwise.
        :rtype: bool
        """
        save = False
        for job in self.get_delayed():
            if datetime.datetime.now() >= job.delay_end:
                job.status = Status.READY

        # At this point, we already computed jobs with STATUS != COMPLETED.

        for job in self.get_waiting():
            tmp = [parent for parent in job.parents if
                   parent.status == Status.COMPLETED or parent.status == Status.SKIPPED]

            if not job.parents or (len(tmp) == len(job.parents) and self.check_all_edges_fail_ok(job)):
                Log.debug(f"Setting job: {job.name} status to: READY (all parents completed)...")
                job.status = Status.READY
                job.hold = False

        return save

    def check_all_edges_fail_ok(self, current_job: Job) -> bool:
        """Check if all edges have fail_ok set to True for a given job.

        Jobs:
          Job:
            DEPENDENCIES:
             JOB-1:
               STATUS: FAILED? -> ok
               STATUS: FAILED -> not ok

        :param current_job: The job to check.
        :type current_job: Job
        :return: True if all edges have fail_ok set to True, False otherwise.
        :rtype: bool
        """
        for parent in current_job.parents:
            edge_info = self.graph.edges.get((parent.name, current_job.name), {})
            if not edge_info.get("fail_ok", False):
                return False
        return True

    def _skip_jobs(self, as_conf: AutosubmitConfig) -> bool:
        """Skip jobs that meet the skipping criteria.

        :param as_conf: Autosubmit configuration object.
        :type as_conf: AutosubmitConfig
        :return: True if any job was skipped, False otherwise.
        :rtype: bool
        """
        save = False
        jobs_to_skip = self.get_skippable_jobs(as_conf.get_wrapper_jobs())
        for section in jobs_to_skip:
            for job in jobs_to_skip[section]:
                if job.status == Status.READY or job.status == Status.QUEUING:
                    jobdate = date2str(job.date, job.date_format)
                    if job.running == 'chunk':
                        for related_job in jobs_to_skip[section]:
                            if (job.chunk < related_job.chunk and job.member == related_job.member and
                                    jobdate == date2str(related_job.date, related_job.date_format)):
                                try:
                                    if job.status == Status.QUEUING:
                                        job.platform.send_command(job.platform.cancel_cmd +
                                                                  " " + str(job.id), ignore_log=True)
                                except Exception:
                                    pass  # jobid finished already
                                job.status = Status.SKIPPED
                                save = True
                    elif job.running == 'member':
                        members = as_conf.get_member_list()
                        for related_job in jobs_to_skip[section]:
                            if (members.index(job.member) < members.index(related_job.member)
                                    and job.chunk == related_job.chunk and jobdate ==
                                    date2str(related_job.date, related_job.date_format)):
                                try:
                                    if job.status == Status.QUEUING:
                                        job.platform.send_command(job.platform.cancel_cmd +
                                                                  " " + str(job.id), ignore_log=True)
                                except Exception:
                                    pass  # job_id finished already
                                job.status = Status.SKIPPED
                                save = True
        return save

    def fill_parents_children(self):
        """Fill the job._parents and job._children attributes"""
        for u in self.graph:
            self.graph.nodes[u]["job"].parents = set()
            self.graph.nodes[u]["job"].children = set()
        for u in self.graph:
            self.graph.nodes[u]["job"].add_children([self.graph.nodes[v]["job"] for v in self.graph[u]])

    def update_genealogy(self):
        """When we have created the job list, every type of job is created.
        Update genealogy remove jobs that have no templates
        """
        self.fill_parents_children()

    def assign_unique_fake_id(self, package: Any, max_retries: int = 2) -> None:
        """Assign a unique fake ID to all jobs in the package, ensuring no collision with existing IDs.

        This is for the -CW flag, as the wrapper won't be really submitted.

        the wrappers.id db field has a non-nullable flag

        :param package: The job package containing jobs to assign the ID.
        :type package: Anyh
        :param max_retries: Maximum number of attempts to generate a unique ID.
        :type max_retries: int
        :raises RuntimeError: If a unique ID cannot be generated after max_retries.
        """
        import secrets
        retries = max_retries
        while retries > 0:
            new_id = secrets.randbelow(100000)
            if new_id not in self.check_wrapper_fake_ids:
                for job in package.jobs:
                    job.id = new_id
                self.check_wrapper_fake_ids.add(new_id)
                break
            retries -= 1
        else:
            raise RuntimeError("Failed to generate a unique fake ID after multiple attempts.")

    def save_wrappers(
            self,
            packages_to_save: List[Any],
            failed_packages: Set[int],
            as_conf: Any,
            preview: bool = False
    ) -> None:
        """Save wrapper jobs for job packages that are not in the failed set.

        :param packages_to_save: List of job package objects to process.
        :type packages_to_save: List[Any]
        :param failed_packages: Set of job IDs that failed and should be skipped.
        :type failed_packages: Set[int]
        :param as_conf: Autosubmit configuration object.
        :type as_conf: Any
        :param preview: Whether to run in preview mode.
        :type preview: bool
        :return: None
        :rtype: None
        """
        packages_to_save_gen = (
            package for package in packages_to_save
            if isinstance(package, JobPackageThread)
               and package.jobs[0].id not in failed_packages
               and hasattr(package, "name")
        )
        wrappers = []
        initial_status = Status.SUBMITTED if not preview else Status.COMPLETED
        for package in packages_to_save_gen:
            # Add a fake id while using inspect -cw, create -cw or monitor -cw
            if preview:
                self.assign_unique_fake_id(package)
            # TODO: For another PR, tried to change this, results in a circular import
            # This is due the fact that the WRAPPERJOB class is derived from the JOB class, when it shouldn't as it contains JOB'S
            from ..job.job import WrapperJob
            wrapper_job = WrapperJob(
                name=package.name,
                job_id=package.jobs[0].id,
                status=initial_status,
                priority=0,
                job_list=package.jobs,
                total_wallclock=package._wallclock,
                num_processors=package._num_processors,
                platform=package.platform,
                as_config=as_conf,
            )
            self.job_package_map[int(wrapper_job.id)] = wrapper_job
            self.packages_dict[package.name] = wrapper_job.job_list

            wrappers.append(self._wrapper_job_dict(wrapper_job))
        if wrappers:
            self.dbmanager.save_wrappers(wrappers, preview=preview)

    def load_wrappers(self, preview: bool = False) -> None:
        """ Load wrapper jobs and their inner jobs from the database, and populate the job package map.

        :param preview: If True, load wrappers in preview mode.
        :type preview: bool

        :return: None
        :rtype: None
        """
        un_mapped_wrapper_info, un_mapped_inner_jobs = self.dbmanager.load_wrappers(preview, self.job_list)

        # Build a dictionary of wrapper info indexed by wrapper name
        wrappers_info: Dict[str, dict] = {
            dict(wrapper)['name']: dict(wrapper) for wrapper in un_mapped_wrapper_info
        }
        # Group inner jobs by package id
        inner_jobs_by_package: Dict[str, list] = {}
        for job in un_mapped_inner_jobs:
            if job['package_name'] not in inner_jobs_by_package:
                inner_jobs_by_package[job['package_name']] = []
            inner_jobs_by_package[job['package_name']].append(job['job_name'])

        # Attach job objects to each wrapper's job list and update packages_dict
        for package_name, job_names in inner_jobs_by_package.items():
            if package_name in wrappers_info:
                if not wrappers_info[package_name].get("job_list", None):
                    wrappers_info[package_name]["job_list"] = []
                for job_name in job_names:
                    job = self.get_job_by_name(job_name)
                    if not job:
                        job = self.load_job_by_name(job_name)
                    if job.id == wrappers_info[package_name]["id"]:
                        wrappers_info[package_name]["job_list"].append(job)
                if wrappers_info[package_name]["job_list"]:
                    self.packages_dict[package_name] = wrappers_info[package_name]["job_list"]
        # Create WrapperJob objects and populate job_package_map
        from ..job.job import WrapperJob
        for wrapper_info in wrappers_info.values():

            if not wrapper_info.get("job_list", None):  # to delete TODO (horizontal-vertical issue)
                continue
            wrapper_job = WrapperJob(
                name=wrapper_info["name"],
                job_id=wrapper_info["id"],
                status=wrapper_info["status"],
                priority=wrapper_info.get("priority", 0),
                total_wallclock=wrapper_info["wallclock"],
                job_list=wrapper_info["job_list"],
                num_processors=wrapper_info["num_processors"],
                platform=wrapper_info["job_list"][0].platform,
                as_config=self._as_conf,
            )
            wrapper_job.platform_name = wrapper_job.job_list[0].platform_name

            self.job_package_map[int(wrapper_job.id)] = wrapper_job

    def _wrapper_job_dict(self, wrapper_job: 'WrapperJob') -> Tuple[Dict[str, Any], List[str]]:
        """Return a dictionary representation of a WrapperJob and its inner jobs for database insertion.

        :param wrapper_job: The wrapper job instance to serialize.
        :type wrapper_job: WrapperJob
        :return: Tuple containing a dictionary of wrapper job attributes and a list of inner job names.
        :rtype: Tuple[Dict[str, Any], List[str]]
        """
        wrapper_info = {
            "name": wrapper_job.name,
            "id": wrapper_job.id,
            "script_name": getattr(wrapper_job, "script_name", None),
            "status": wrapper_job.status,
            "local_logs_out": getattr(wrapper_job, "local_logs_out", None),
            "local_logs_err": getattr(wrapper_job, "local_logs_err", None),
            "remote_logs_out": getattr(wrapper_job, "remote_logs_out", None),
            "remote_logs_err": getattr(wrapper_job, "remote_logs_err", None),
            "updated_log": getattr(wrapper_job, "updated_log", 0),
            "platform_name": wrapper_job.platform.name if wrapper_job.platform else None,
            "wallclock": wrapper_job.wallclock,
            "num_processors": wrapper_job.num_processors,
            "type": getattr(wrapper_job, "type", None),
            "sections": getattr(wrapper_job, "sections", None),
            "method": getattr(wrapper_job, "method", None),
        }
        wrapper_inner_jobs = [
            {
                'package_id': wrapper_job.id,
                'package_name': wrapper_job.name,
                'job_name': job.name,
                'timestamp': datetime.datetime.now().isoformat()
            }
            for job in wrapper_job.job_list
        ]
        return wrapper_info, wrapper_inner_jobs

    def check_scripts(self, as_conf: AutosubmitConfig) -> bool:
        """When we have created the scripts, all parameters should have been substituted.
        %PARAMETER% handlers are not allowed.

        :param as_conf: experiment configuration
        :type as_conf: AutosubmitConfig
        """
        Log.info("Checking scripts...")
        out = True
        # Implementing checking scripts feedback to the users in a minimum of 4 messages
        count = stage = 0
        for job in (job for job in self.job_list):
            job.update_check_variables(as_conf)
            count += 1
            if (count >= len(self.job_list) / 4 * (stage + 1)) or count == len(self.job_list):
                stage += 1
                Log.info(f"{count} of {len(self.job_list)} checked")

            show_logs = str(job.check_warnings).lower()
            if str(job.check).lower() in ['on_submission', 'false']:
                continue
            else:
                if job.section in self.sections_checked:
                    show_logs = "false"
            if not job.check_script(as_conf, show_logs):
                out = False
            self.sections_checked.add(job.section)
        if out:
            Log.result("Scripts OK")
        else:
            Log.printlog(
                "Scripts check failed\n Running after failed scripts is at your own risk!", 3000)
        return out

    def _remove_job(self, job):
        """Remove a job from the list.

        :param job: job to remove
        :type job: Job
        """
        for child in job.children:
            for parent in job.parents:
                child.add_parent(parent)
            child.delete_parent(job)

        for parent in job.parents:
            parent.children.remove(job)
        self.graph.remove_node(job.name)

    def rerun(self, job_list_unparsed, as_conf, monitor=False):
        """Updates job list to rerun the jobs specified by a job list.

        :param job_list_unparsed: list of jobs to rerun
        :type job_list_unparsed: str
        :param as_conf: experiment configuration
        :type as_conf: AutosubmitConfig
        :param monitor: if True, the job list will be monitored
        :type monitor: bool

        """
        self.parse_jobs_by_filter(job_list_unparsed, two_step_start=False)
        member_list = set()
        chunk_list = set()
        date_list = set()
        job_sections = set()
        for job in self.get_all():
            if not monitor:
                job.status = Status.COMPLETED
            if job in self.rerun_job_list:
                job_sections.add(job.section)
                if not monitor:
                    job.status = Status.WAITING
                if job.member is not None and len(str(job.member)) > 0:
                    member_list.add(job.member)
                if job.chunk is not None and len(str(job.chunk)) > 0:
                    chunk_list.add(job.chunk)
                if job.date is not None and len(str(job.date)) > 0:
                    date_list.add(job.date)
            else:
                self._remove_job(job)
        self._member_list = list(member_list)
        self._chunk_list = list(chunk_list)
        self._date_list = list(date_list)
        Log.info("Adding dependencies...")

        for job_section in job_sections:
            Log.debug(f"Reading rerun dependencies for {job_section} jobs")
            if as_conf.jobs_data[job_section].get('DEPENDENCIES', None) is not None:
                dependencies_keys = as_conf.jobs_data[job_section].get('DEPENDENCIES', {})
                if type(dependencies_keys) is str:
                    dependencies_keys = dependencies_keys.upper().split()
                if dependencies_keys is None:
                    dependencies_keys = []
                dependencies = JobList._manage_dependencies(dependencies_keys, self._dic_jobs)
                for job in self.get_jobs_by_section(job_section):
                    for key in dependencies_keys:
                        dependency = dependencies[key]
                        skip, (_, _, _) = JobList._calculate_dependency_metadata(
                            job.chunk, self._chunk_list, job.member, self._member_list, job.date,
                            self._date_list, dependency)
                        if skip:
                            continue
                        section_name = dependencies[key].section
                        for parent in self._dic_jobs.get_jobs(
                                section_name, job.date, job.member, job.chunk):
                            if not monitor:
                                parent.status = Status.WAITING
                            Log.debug("Parent: " + parent.name)

    def _get_jobs_parser(self):
        jobs_parser = self._parser_factory.create_parser()
        jobs_parser.optionxform = str
        jobs_parser.load(
            os.path.join(self._as_conf.LOCAL_ROOT_DIR, self._expid,
                         'conf', "jobs_" + self._expid + ".yaml"))
        return jobs_parser

    def remove_rerun_only_jobs(self) -> None:
        """Removes all jobs to be run only in reruns. """
        flag = False
        for job in self.job_list[:]:
            if job.rerun_only == "true":
                self._remove_job(job)
                flag = True
        if flag:
            self.update_genealogy()

        del self._dic_jobs

    def print_with_status(self, status_change: Optional[dict[Any, Any]] = None, nocolor=False,
                          existing_list=None) -> str:
        """Returns the string representation of the dependency tree of the Job List

        :param status_change: List of changes in the list, supplied in set status
        :type status_change: dict
        :param nocolor: True if the result should not include color codes
        :type nocolor: Boolean
        :param existing_list: External List of Jobs that will be printed, this excludes the inner list of jobs.
        :type existing_list: List of Job Objects
        :return: String representation of the Job List
        :rtype: String
        """

        # nocolor = True
        all_jobs = self.get_all() if existing_list is None else existing_list
        # Header
        result = (bcolors.BOLD if nocolor is False else '') + \
                 "## String representation of Job List [" + str(len(all_jobs)) + "] "
        if status_change is not None and len(str(status_change)) > 0:
            result += ("with " + (bcolors.OKGREEN if nocolor is False else '') +
                       str(len(list(status_change.keys()))) + " Change(s) ##" +
                       (bcolors.ENDC + bcolors.ENDC if nocolor is False else ''))
        else:
            result += " ## "

        # Find root
        roots = []
        for job in all_jobs:
            if len(job.parents) == 0:
                roots.append(job)
        visited = list()
        # print(root)
        # root exists
        for root in roots:
            if root is not None and len(str(root)) > 0:
                result += self._recursion_print(root, 0, visited,
                                                statusChange=status_change, nocolor=nocolor)
            else:
                result += "\nCannot find root."
        return result

    def __repr__(self):
        """Returns the string representation of the class.

        :return: String representation.
        :rtype: String
        """
        try:
            results = [f"## String representation of Job List [{len(self.jobs)}] ##"]
            # Find root
            roots = [job for job in self.get_all()
                     if len(job.parents) == 0
                     and job is not None and len(str(job)) > 0]
            visited = list()
            # root exists
            for root in roots:
                if root is not None and len(str(root)) > 0:
                    results.append(self._recursion_print(root, 0, visited, nocolor=True))
                else:
                    results.append("Cannot find root.")
        except Exception:
            return 'Job List object'
        return "\n".join(results)

    def _recursion_print(self, job, level, visited=[], statusChange=None, nocolor=False):
        """
        Returns the list of children in a recursive way
        Traverses the dependency tree
        :param job: Job object
        :type job: Job
        :param level: Level of the tree
        :type level: int
        :param visited: List of visited jobs
        :type visited: list
        :param statusChange: List of changes in the list, supplied in set status
        :type statusChange: List of strings

        :return: parent + list of children
        :rtype: String
        """
        result = ""
        if job.name not in visited:
            visited.append(job.name)
            prefix = ""
            for i in range(level):
                prefix += "|  "
            # Prefix + Job Name
            result = "\n" + prefix + \
                     (bcolors.BOLD + bcolors.CODE_TO_COLOR[job.status]
                      if nocolor is False else '') + job.name + \
                     (bcolors.ENDC + bcolors.ENDC if nocolor is False else '')
            if len(job._children) > 0:
                level += 1
                children = job._children
                total_children = len(job._children)
                # Writes children number and status if color are not being showed
                result += (" ~ [" + str(total_children) +
                           (" children] " if total_children > 1 else " child] ") +
                           ("[" + Status.VALUE_TO_KEY[job.status] + "] " if nocolor is True else "")
                           )
                if statusChange is not None and len(str(statusChange)) > 0:
                    # Writes change if performed
                    result += (bcolors.BOLD +
                               bcolors.OKGREEN if nocolor is False else '')
                    result += (statusChange[job.name]
                               if job.name in statusChange else "")
                    result += (bcolors.ENDC +
                               bcolors.ENDC if nocolor is False else "")
                # order by name, this is for compare 4.0 with 4.1 as the children order is different
                for child in sorted(children, key=lambda x: x.name):
                    # Continues recursion
                    result += self._recursion_print(
                        child, level, visited, statusChange=statusChange, nocolor=nocolor)
            else:
                result += (" [" + Status.VALUE_TO_KEY[job.status] +
                           "] " if nocolor is True else "")

        return result

    def retrieve_symbols(self):
        """Retrieves dictionaries that map the collection of packages in the experiment
        to symbols for plotting. (Used by the autosubmit stats command).
        :return: Dictionary mapping package names to symbols.
        :rtype: Dictionary
        """
        self.load_wrappers()
        package_to_symbol = dict()
        i = 0
        for package_name, wrapped_job in self.packages_dict.items():
            if i % 2 == 0:
                package_to_symbol[package_name] = 'square'
            else:
                package_to_symbol[package_name] = 'hexagon'
            i += 1

        return package_to_symbol

    @staticmethod
    def retrieve_times(status_code, name, tmp_path, make_exception=False, job_times=None,
                       seconds=False, job_data_collection=None) -> Union[None, JobRow]:
        """
        Retrieve job timestamps from database.
        :param job_data_collection:
        :param seconds:
        :param status_code: Code of the Status of the job
        :type status_code: Integer
        :param name: Name of the job
        :type name: String
        :param tmp_path: Path to the tmp folder of the experiment
        :type tmp_path: String
        :param make_exception: flag for testing purposes
        :type make_exception: Boolean
        :param job_times: Detail from as_times.job_times for the experiment
        :type job_times: Dictionary Key: job name, Value: 5-tuple (submit time,
        start time, finish time, status, detail id)
        :return: minutes the job has been queuing, minutes the job has been running,
        and the text that represents it
        :rtype: int, int, str
        """
        status = "NA"
        energy = 0
        seconds_queued = 0
        seconds_running = 0
        submit_time = datetime.timedelta()
        start_time = datetime.timedelta()
        finish_time = datetime.timedelta()

        try:
            # Getting data from new job database
            if job_data_collection is not None:
                job_data = next(
                    (job for job in job_data_collection if job.job_name == name), None)
                if job_data:
                    status = Status.VALUE_TO_KEY[status_code]
                    if status == job_data.status:
                        energy = job_data.energy
                        t_submit = job_data.submit
                        t_start = job_data.start
                        t_finish = job_data.finish
                        # Test if start time does not make sense
                        if t_start >= t_finish:
                            if job_times:
                                _, c_start, c_finish, _, _ = job_times.get(
                                    name, (0, t_start, t_finish, 0, 0))
                                t_start = c_start if t_start > c_start else t_start
                                job_data.start = t_start

                        if seconds is False:
                            queue_time = math.ceil(
                                job_data.queuing_time() / 60)
                            running_time = math.ceil(
                                job_data.running_time() / 60)
                        else:
                            queue_time = job_data.queuing_time()
                            running_time = job_data.running_time()

                        if status_code in [Status.SUSPENDED]:
                            t_submit = t_start = t_finish = 0

                        return JobRow(job_data.job_name, int(queue_time), int(running_time), status,
                                      energy, JobList.ts_to_datetime(t_submit),
                                      JobList.ts_to_datetime(t_start), JobList.ts_to_datetime(t_finish),
                                      job_data.ncpus, job_data.run_id)

            # Using standard procedure
            if status_code in [Status.RUNNING, Status.SUBMITTED, Status.QUEUING,
                               Status.FAILED] or make_exception is True:
                # COMPLETED adds too much overhead so these values are now
                # stored in a database and retrieved separately
                submit_time, start_time, finish_time, status = JobList._job_running_check(
                    status_code, name, tmp_path)
                if status_code in [Status.RUNNING, Status.FAILED, Status.COMPLETED]:
                    running_for_min = (finish_time - start_time)
                    queuing_for_min = (start_time - submit_time)
                    submit_time = mktime(submit_time.timetuple())
                    start_time = mktime(start_time.timetuple())
                    finish_time = mktime(finish_time.timetuple()) if status_code in [
                        Status.FAILED, Status.COMPLETED] else 0
                else:
                    queuing_for_min = (
                            datetime.datetime.now() - submit_time)
                    running_for_min = datetime.datetime.now() - datetime.datetime.now()
                    submit_time = mktime(submit_time.timetuple())
                    start_time = 0
                    finish_time = 0

                submit_time = int(submit_time)
                start_time = int(start_time)
                finish_time = int(finish_time)
                seconds_queued = queuing_for_min.total_seconds()
                seconds_running = running_for_min.total_seconds()

            else:
                # For job times completed we no longer use time-deltas, but timestamps
                status = Status.VALUE_TO_KEY[status_code]
                if job_times and status_code not in [Status.READY,
                                                     Status.WAITING, Status.SUSPENDED]:
                    if name in list(job_times.keys()):
                        submit_time, start_time, finish_time, status, detail_id = job_times[
                            name]
                        seconds_running = finish_time - start_time
                        seconds_queued = start_time - submit_time
                        submit_time = int(submit_time)
                        start_time = int(start_time)
                        finish_time = int(finish_time)
                else:
                    submit_time = 0
                    start_time = 0
                    finish_time = 0

        except Exception:
            print((traceback.format_exc()))
            return None

        seconds_queued = seconds_queued * (-1) if seconds_queued < 0 else seconds_queued
        seconds_running = seconds_running * (-1) if seconds_running < 0 else seconds_running
        if seconds is False:
            queue_time = math.ceil(
                seconds_queued / 60) if seconds_queued > 0 else 0
            running_time = math.ceil(
                seconds_running / 60) if seconds_running > 0 else 0
        else:
            queue_time = seconds_queued
            running_time = seconds_running

        return JobRow(name,
                      int(queue_time),
                      int(running_time),
                      status,
                      energy,
                      JobList.ts_to_datetime(submit_time),
                      JobList.ts_to_datetime(start_time),
                      JobList.ts_to_datetime(finish_time),
                      0,
                      0)

    @staticmethod
    def _job_running_check(status_code, name, tmp_path):
        """Receives job data and returns the data from its TOTAL_STATS file in an ordered way.

        :param status_code: Status of job
        :type status_code: Integer
        :param name: Name of job
        :type name: String
        :param tmp_path: Path to the tmp folder of the experiment
        :type tmp_path: String
        :return: submit time, start time, end time, status
        :rtype: 4-tuple in datetime format
        """
        # name = "a2d0_20161226_001_124_ARCHIVE"
        values = list()
        status_from_job = str(Status.VALUE_TO_KEY[status_code])
        now = datetime.datetime.now()
        submit_time = now
        start_time = now
        finish_time = now
        current_status = status_from_job
        path = os.path.join(tmp_path, name + '_TOTAL_STATS')
        # print("Looking in " + path)
        if os.path.exists(path):
            request = 'tail -1 ' + path
            last_line = os.popen(request).readline()
            # print(last_line)

            values = last_line.split()
            # print(last_line)
            try:
                if status_code in [Status.RUNNING]:
                    submit_time = parse_date(
                        values[0]) if len(values) > 0 else now
                    start_time = parse_date(values[1]) if len(
                        values) > 1 else submit_time
                    finish_time = now
                elif status_code in [Status.QUEUING, Status.SUBMITTED, Status.HELD]:
                    submit_time = parse_date(
                        values[0]) if len(values) > 0 else now
                    start_time = parse_date(
                        values[1]) if len(values) > 1 and values[0] != values[1] else now
                elif status_code in [Status.COMPLETED]:
                    submit_time = parse_date(
                        values[0]) if len(values) > 0 else now
                    start_time = parse_date(
                        values[1]) if len(values) > 1 else submit_time
                    if len(values) > 3:
                        finish_time = parse_date(values[len(values) - 2])
                    else:
                        finish_time = submit_time
                else:
                    submit_time = parse_date(
                        values[0]) if len(values) > 0 else now
                    start_time = parse_date(values[1]) if len(
                        values) > 1 else submit_time
                    finish_time = parse_date(values[2]) if len(
                        values) > 2 else start_time
            except Exception:
                start_time = now
                finish_time = now
                # NA if reading fails
                current_status = "NA"

        current_status = values[3] if (len(values) > 3 and len(
            values[3]) != 14) else status_from_job
        # TOTAL_STATS last line has more than 3 items, status is different from pkl,
        # and status is not "NA"
        if len(values) > 3 and current_status != status_from_job and current_status != "NA":
            current_status = "SUSPICIOUS"
        return submit_time, start_time, finish_time, current_status

    @staticmethod
    def ts_to_datetime(timestamp):
        if timestamp and timestamp > 0:
            # print(datetime.datetime.utcfromtimestamp(
            #     timestamp).strftime('%Y-%m-%d %H:%M:%S'))
            return datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        else:
            return None

    def update_as_conf(self, as_conf: 'AutosubmitConfig') -> None:
        self._as_conf = as_conf

    def add_job(self, job: Job):
        self.graph.add_node(job.name, job=job)

    def recover_last_data(self, finished_jobs: Optional[list["Job"]] = None) -> None:
        """Recover job IDs and log names for completed, failed, and skipped jobs from experiment history.
        :param finished_jobs: Optional list of finished Job objects to recover data for.
        :return: None
        :rtype: None
        """
        jobs_ran_atleast_once = False
        if not finished_jobs:
            jobs_ran_atleast_once = True
            finished_jobs: list["Job"] = self._get_jobs_by_name(status=[Status.COMPLETED, Status.FAILED, Status.SKIPPED], return_only_names=False)
        # Recover job_id and log name if missing
        if finished_jobs:
            exp_history = ExperimentHistory(self.expid, jobdata_dir_path=BasicConfig.JOBDATA_DIR,
                                            historiclog_dir_path=BasicConfig.HISTORICAL_LOG_DIR, force_sql_alchemy=True)
            jobs_data = exp_history.manager.get_jobs_data_last_row([job.name for job in finished_jobs])
            # Only if we have information already stored, otherwise the job will be downloaded later
            for job in [job for job in finished_jobs if job.name in jobs_data]:
                job.id = int(jobs_data[job.name]["job_id"])
                job.local_logs = jobs_data[job.name]["out"]
                job.remote_logs = jobs_data[job.name]["err"]
                job.updated_log = True

        for job in finished_jobs:
            # TODO: Another fix will come in 4.2. Currently, if the job has no id, the log will not be recovered properly.
            if not job.id:
                job.id = 1
            # Fixes: https://github.com/BSC-ES/autosubmit/pull/2700#issuecomment-3563572977
            if not jobs_ran_atleast_once:
                job.updated_log = True

    def _get_jobs_by_name(self, status: Optional[list[int]] = None, platform: Platform = None, return_only_names=False) -> Union[List[str], List["Job"]]:
        """Return jobs filtered by status and/or platform as names or Job objects.

        :param status: Optional list of job statuses to filter by.
        :param platform: Optional Platform to filter by.
        :param return_only_names: If True return list of job names, otherwise Job objects.
        :return: List of job names or List of Job objects.
        """
        if not status:
            status = []
        if return_only_names:
            return [job.name for job in self.get_job_list() if (not status or job.status in status) and (not platform or job.platform_name == platform.name)]
        else:
            return [job for job in self.get_job_list() if (not status or job.status in status) and (not platform or job.platform_name == platform.name)]

    def recover_all_completed_jobs_from_exp_history(self, platform: Platform = None) -> set[str]:
        """Recover all completed jobs from experiment history
        :param platform: Platform to filter by.
        :return: Set of completed job names.
        """

        job_names: list[str] = self._get_jobs_by_name(platform=platform, return_only_names=True)
        if job_names:
            exp_history = ExperimentHistory(self.expid, jobdata_dir_path=BasicConfig.JOBDATA_DIR,
                                            historiclog_dir_path=BasicConfig.HISTORICAL_LOG_DIR, force_sql_alchemy=True)
            jobs_data = exp_history.manager.get_jobs_data_last_row(job_names)  # This gets only the last row
            return {name for name, data in jobs_data.items() if data["status"] == "COMPLETED"}

        return set()

    def update_db_wrappers(self):
        """Update the wrapper jobs in the database with the current status of the wrapper jobs in memory."""
        if self.job_package_map:
            wrappers = []
            for wrapper_job in self.job_package_map.values():
                wrappers.append(self._wrapper_job_dict(wrapper_job)[0])
            if wrappers:
                self.dbmanager.update_wrapper_status(wrappers)

    def get_wrappers_id_from_db(self) -> List[int]:
        """Get a list of all wrapper job IDs from the database.

        :return: List of wrapper job IDs.
        :rtype: List[int]
        """
        return [id for _, id in self.dbmanager.get_wrappers_id_from_db()]

    def get_failed_from_db(self):
        return self.dbmanager.get_failed_job_data()
