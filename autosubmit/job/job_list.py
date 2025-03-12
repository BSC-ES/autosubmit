#!/usr/bin/env python3

# Copyright 2017-2020 Earth Sciences Department, BSC-CNS

# This file is part of Autosubmit.

# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.
import copy
import datetime
import os
import pickle
import re
import traceback
from contextlib import suppress
from shutil import move
from threading import Thread
from typing import List, Dict, Tuple, Any
from pathlib import Path

from time import strftime, localtime, mktime

import math
from bscearth.utils.date import date2str, parse_date
from networkx import DiGraph

from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from log.log import AutosubmitCritical, AutosubmitError, Log
from autosubmit.job.job_utils import transitive_reduction
from autosubmit.helpers.data_transfer import JobRow
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status, bcolors
from autosubmit.job.job_dict import DicJobs
from autosubmit.job.job_package_persistence import JobPackagePersistence
from autosubmit.job.job_packages import JobPackageThread
from autosubmit.job.job_utils import Dependency, _get_submitter
import autosubmit.database.db_structure as DbStructure


def threaded(fn):
    def wrapper(*args, **kwargs):
        thread = Thread(target=fn, args=args, kwargs=kwargs)
        thread.name = "data_processing"
        thread.start()
        return thread

    return wrapper


class JobList(object):
    """
    Class to manage the list of jobs to be run by autosubmit

    """

    def __init__(self, expid, config, parser_factory, job_list_persistence):
        self._persistence_path = os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "pkl")
        self._update_file = "updated_list_" + expid + ".txt"
        self._failed_file = "failed_job_list_" + expid + ".pkl"
        self._persistence_file = "job_list_" + expid
        self._job_list = list()
        self._base_job_list = list()
        self.jobs_edges = {}
        self._expid = expid
        self._config = config
        self._parser_factory = parser_factory
        self._stat_val = Status()
        self._parameters = []
        self._date_list = []
        self._member_list = []
        self._chunk_list = []
        self._dic_jobs = dict()
        self._persistence = job_list_persistence
        self.packages_dict = dict()
        self._ordered_jobs_by_date_member = dict()

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
    def run_members(self, value):
        if value is not None and len(str(value)) > 0:
            self._run_members = value
            self._base_job_list = [job for job in self._job_list]
            found_member = False
            processed_job_list = []
            # We are assuming that the jobs are sorted in topological order (which is the default)
            for job in self._job_list:
                if ((job.member is None and not found_member) or job.member in self._run_members or
                        job.status not in [Status.WAITING, Status.READY]):
                    processed_job_list.append(job)
                if job.member is not None and len(str(job.member)) > 0:
                    found_member = True
            self._job_list = processed_job_list

    def create_dictionary(self, date_list, member_list, num_chunks, chunk_ini,
                          date_format, default_retrials, wrapper_jobs, as_conf):
        chunk_list = list(range(chunk_ini, num_chunks + 1))

        dic_jobs = DicJobs(date_list, member_list, chunk_list,
                           date_format, default_retrials, as_conf)
        self._dic_jobs = dic_jobs
        for wrapper_section in wrapper_jobs:
            if str(wrapper_jobs[wrapper_section]).lower() != 'none':
                self._ordered_jobs_by_date_member[wrapper_section] = self._create_sorted_dict_jobs(
                    wrapper_jobs[wrapper_section])
            else:
                self._ordered_jobs_by_date_member[wrapper_section] = {}


    def _delete_edgeless_jobs(self):
        # indices to delete
        for job in self._job_list[:]:
            if job.dependencies is not None and job.dependencies not in ["{}", "[]"]:
                if ((len(job.dependencies) > 0 and not job.has_parents() and not
                job.has_children()) and str(job.delete_when_edgeless).casefold() ==
                        "true".casefold()):
                    self._job_list.remove(job)
                    self.graph.remove_node(job.name)

    @staticmethod
    def check_split_set_to_auto(as_conf):
        # If this is true, the workflow needs to be recreated on create
        for job_name, values in as_conf.experiment_data.get("JOBS", {}).items():
            if values.get("SPLITS", None) == "auto":
                return True
        return False

    def generate(self, as_conf, date_list, member_list, num_chunks, chunk_ini, parameters,
                 date_format, default_retrials, default_job_type, wrapper_jobs=dict(), new=True,
                 run_only_members=[], show_log=True, monitor=False, force=False, create=False):
        """
        Creates all jobs needed for the current workflow.
        :param create:
        :type create: bool
        :param force:
        :type force: bool
        :param as_conf: AutosubmitConfig object
        :type as_conf: AutosubmitConfig
        :param date_list: list of dates
        :type date_list: list
        :param member_list: list of members
        :type member_list: list
        :param num_chunks: number of chunks
        :type num_chunks: int
        :param chunk_ini: initial chunk
        :type chunk_ini: int
        :param parameters: parameters
        :type parameters: dict
        :param date_format: date format ( D/M/Y )
        :type date_format: str
        :param default_retrials: default number of retrials
        :type default_retrials: int
        :param default_job_type: default job type
        :type default_job_type: str
        :param wrapper_jobs: wrapper jobs
        :type wrapper_jobs: dict
        :param new: new
        :type new: bool
        :param run_only_members: run only members
        :type run_only_members: list
        :param show_log: show log
        :type show_log: bool
        :param monitor: monitor
        :type monitor: bool
        """
        if create and self.check_split_set_to_auto(as_conf):
            force = True
        if force:
            Log.debug("Resetting the workflow graph to a zero state")
            if os.path.exists(os.path.join(self._persistence_path, self._persistence_file + ".pkl")):
                os.remove(os.path.join(self._persistence_path, self._persistence_file + ".pkl"))
            if os.path.exists(os.path.join(self._persistence_path, self._persistence_file + "_backup.pkl")):
                os.remove(os.path.join(self._persistence_path, self._persistence_file + "_backup.pkl"))
        self._parameters = parameters
        self._date_list = date_list
        self._member_list = member_list
        chunk_list = list(range(chunk_ini, num_chunks + 1))
        self._chunk_list = chunk_list
        self._dic_jobs = DicJobs(date_list, member_list, chunk_list, date_format,
                                 default_retrials, as_conf)

        try:
            loaded_job_list = self.load(create)
            Log.result("Load finished")
        except BaseException as e:
            Log.warning(f"Couldn't load the old job_list {e}")
            loaded_job_list = None


        if (not loaded_job_list and not create) or (loaded_job_list and
                                                    len(loaded_job_list) == 0 and not create):
            raise AutosubmitCritical(
                "Autosubmit couldn't load the workflow graph. Please run autosubmit create first."
                "If the pkl file exists and was generated with Autosubmit v4.1+, try again.",
                7013)
        elif loaded_job_list and len(loaded_job_list) == 0 and create:
            new = True
            Log.info(
                "Removing previous pkl file due to empty graph, "
                "likely due using an Autosubmit 4.0.XXX version")
            with suppress(FileNotFoundError):
                os.remove(os.path.join(self._persistence_path, self._persistence_file + ".pkl"))
            with suppress(FileNotFoundError):
                os.remove(os.path.join(self._persistence_path, self._persistence_file + "_backup.pkl"))
        if loaded_job_list:
            self._dic_jobs._job_list = loaded_job_list

        self.graph = DiGraph()

        # This generates the job object and also finds if dic_jobs has modified from previous 
        # iteration in order to expand the workflow
        if show_log:
            Log.info("Creating jobs...")
        self._create_jobs(self._dic_jobs, 0, default_job_type)
        # This dic_job is key to the dependencies management as they're ordered 
        # by date[member[chunk]]
        if show_log:
            Log.info("Adding dependencies to the graph..")
        self._add_dependencies(date_list, member_list, chunk_list, self._dic_jobs)

        if show_log:
            Log.info("Adding dependencies to the job..")
        self.update_genealogy()
        # Checking for member constraints
        if len(run_only_members) > 0:
            # Found
            if show_log:
                Log.info("Considering only members {0}".format(
                    str(run_only_members)))
            old_job_list = [job for job in self._job_list]
            self._job_list = [
                job for job in old_job_list if
                job.member is None or job.member in run_only_members or job.status
                not in [Status.WAITING, Status.READY]]
            for job in self._job_list:
                for jobp in job.parents:
                    if jobp in self._job_list:
                        job.parents.add(jobp)
                for jobc in job.children:
                    if jobc in self._job_list:
                        job.children.add(jobc)
        if show_log:
            Log.info("Looking for edgeless jobs...")

        # This if allows to have jobs with dependencies set to themselves even if there are no more than one chunk, member or split.
        if len(self.graph.edges) > 0:
            self._delete_edgeless_jobs()
        if new:
            for job in self._job_list:
                job._fail_count = 0
                if not job.has_parents():
                    job.status = Status.READY
                else:
                    job.status = Status.WAITING

        for wrapper_section in wrapper_jobs:
            try:
                if (wrapper_jobs[wrapper_section] is not None and
                        len(str(wrapper_jobs[wrapper_section])) > 0):
                    self._ordered_jobs_by_date_member[wrapper_section] = (
                        self._create_sorted_dict_jobs(wrapper_jobs[wrapper_section]))
                else:
                    self._ordered_jobs_by_date_member[wrapper_section] = {}
            except BaseException as e:
                raise AutosubmitCritical(
                    "Some section jobs of the wrapper:{0} are missing from your "
                    "JOBS definition in YAML".format(wrapper_section), 7014, str(e))
        # divide job_list per platform name
        job_list_per_platform = self.split_by_platform()
        submitter = _get_submitter(as_conf)
        submitter.load_platforms(as_conf)

        for platform in job_list_per_platform:
            for job in job_list_per_platform[platform]:
                if create or new:
                    job.reset_logs(as_conf)
                    # The platform mayn't exist. ( The Autosubmit config parser should check this )
                    if job.platform_name and job.platform_name in submitter.platforms:
                        job.platform = submitter.platforms[job.platform_name]

    def clear_generate(self):
        self.dependency_map = {}
        self.parameters = {}
        self._parameters = {}
        self.graph.clear()
        self.graph = None

    def split_by_platform(self):
        """
        Splits the job list by platform name
        :return: job list per platform
        :rtype: dict
        """
        job_list_per_platform = dict()
        for job in self._job_list:
            if job.platform_name not in job_list_per_platform:
                job_list_per_platform[job.platform_name] = []
            job_list_per_platform[job.platform_name].append(job)
        return job_list_per_platform

    def _add_all_jobs_edge_info(self, dic_jobs, option="DEPENDENCIES"):
        jobs_data = dic_jobs.experiment_data.get("JOBS", {})
        sections_gen = (section for section in jobs_data.keys())
        for job_section in sections_gen:
            jobs_gen = (job for job in dic_jobs.get_jobs(job_section))
            dependencies_keys = jobs_data.get(job_section, {}).get(option, None)
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

    def _add_dependencies(self, date_list, member_list, chunk_list, dic_jobs,
                          option="DEPENDENCIES"):
        jobs_data = dic_jobs.experiment_data.get("JOBS", {})
        problematic_jobs = {}
        # map dependencies
        self.dependency_map = dict()
        for section in jobs_data.keys():
            self.dependency_map[section] = self._deep_map_dependencies(section,
                                                jobs_data, option, set(), strip_keys=True)
            self.dependency_map[section].remove(section)
        # map dependencies
        self.dependency_map_with_distances = dict()
        for section in jobs_data.keys():
            self.dependency_map_with_distances[section] = self._deep_map_dependencies(section,
                                                    jobs_data, option, set(), strip_keys=False)
            self.dependency_map_with_distances[section].remove(section)

        changes = False
        if len(self.graph.out_edges) > 0:
            sections_gen = (section for section in jobs_data.keys())
            # Room for improvement: Do changes only to jobs affected by the yaml changes
            for job_section in sections_gen:
                if (dic_jobs.changes.get(job_section, None) or
                        dic_jobs.changes.get("EXPERIMENT", None) or
                        dic_jobs.changes.get("NEWJOBS", False)):
                    changes = True
                    break
            Log.debug("Looking if there are changes in the workflow")
            if changes:
                Log.debug("Changes detected, removing all dependencies")
                self.graph.clear_edges()  # reset edges of all jobs as they need to be recalculated
                Log.debug("Dependencies deleted, recalculating dependencies")
            else:
                Log.debug("No changes detected, keeping edges")
        else:
            changes = True
            Log.debug("Changes detected, calculating dependencies")
        # Generate all graph before adding dependencies.
        for job_section in (section for section in jobs_data.keys()):
            for job in (job for job in dic_jobs.get_jobs(job_section, sort_string=True)):
                if job.name not in self.graph.nodes:
                    self.graph.add_node(job.name, job=job)
                # Old versions of autosubmit needs re-adding the job to the graph
                elif (job.name in self.graph.nodes and
                      self.graph.nodes.get(job.name).get("job", None) is None):
                    self.graph.nodes.get(job.name)["job"] = job

        for job_section in (section for section in jobs_data.keys()):
            # Changes when all jobs of a section are added
            self.depends_on_previous_chunk = dict()
            self.depends_on_previous_split = dict()
            self.depends_on_previous_special_section = dict()
            self.actual_job_depends_on_previous_chunk = False
            self.actual_job_depends_on_previous_member = False
            # No changes, no need to recalculate dependencies
            Log.debug("Adding dependencies for {0} jobs".format(job_section))
            # If it does not have dependencies, just append it to job_list and continue
            dependencies_keys = jobs_data.get(job_section, {}).get(option, None)
            # call function if dependencies_key is not None
            dependencies = JobList._manage_dependencies(dependencies_keys, dic_jobs) \
                if dependencies_keys else {}
            self.job_names = set()
            for job in (job for job in dic_jobs.get_jobs(job_section, sort_string=True)):
                self.actual_job_depends_on_special_chunk = False
                if dependencies and changes:
                    job = self.graph.nodes.get(job.name)['job']
                    # Adds the dependencies to the job, and if not possible,
                    # adds the job to the problematic_dependencies
                    problematic_dependencies = self._manage_job_dependencies(dic_jobs, job,
                        date_list, member_list, chunk_list, dependencies_keys, dependencies,
                        self.graph)
                    if len(problematic_dependencies) > 1:
                        if job_section not in problematic_jobs.keys():
                            problematic_jobs[job_section] = {}
                        problematic_jobs[job_section].update({job.name: problematic_dependencies})
        if changes:
            self.find_and_delete_redundant_relations(problematic_jobs)
        self._add_all_jobs_edge_info(dic_jobs, option)

    def find_and_delete_redundant_relations(self, problematic_jobs: dict) -> None:
        """
        Jobs with intrisic rules than can't be safelty not added without messing other workflows.
        The graph will have the least amount of edges added as much as safely possible
        before this function.
        Structure:
        problematic_jobs structure is {section: {child_name: [parent_names]}}

        :return:
        """
        delete_relations = set()
        for section, jobs in problematic_jobs.items():
            for child_name, parents in jobs.items():
                for parent_name in parents:
                    for another_parent_name in list(parents)[1:]:
                        if self.graph.has_successor(parent_name, another_parent_name):
                            delete_relations.add((parent_name, child_name))
                        elif self.graph.has_successor(another_parent_name, parent_name):
                            delete_relations.add((another_parent_name, child_name))

        for relation_to_delete in delete_relations:
            with suppress(Exception):
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
    def _calculate_splits_dependencies(section, max_splits):
        splits_list = section[section.find("[") + 1:section.find("]")]
        splits = []
        for str_split in splits_list.split(","):
            if str_split.find(":") != -1:
                numbers = str_split.split(":")
                # change this to be checked in job_common.py
                max_splits = min(int(numbers[1]), max_splits)
                for count in range(int(numbers[0]), max_splits + 1):
                    splits.append(int(str(count).zfill(len(numbers[0]))))
            else:
                if int(str_split) <= max_splits:
                    splits.append(int(str_split))
        return splits

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
        ## values to return
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
        status = relationship.pop("STATUS", relationships.get("STATUS", None))
        from_step = relationship.pop("FROM_STEP", relationships.get("FROM_STEP", None))
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
                if not filter_data.get("STATUS", None):
                    filter_data["STATUS"] = status
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
        if filter_type == "DATES_TO":
            value_list = self._date_list
            level_to_check = "DATES_FROM"
        elif filter_type == "MEMBERS_TO":
            value_list = self._member_list
            level_to_check = "MEMBERS_FROM"
        elif filter_type == "CHUNKS_TO":
            value_list = self._chunk_list
            level_to_check = "CHUNKS_FROM"
        elif filter_type == "SPLITS_TO":
            value_list = []
            level_to_check = "SPLITS_FROM"
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
                                value_list=value_list, level_to_check=filter_type, splits=splits)
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
        """
        Unify all filter_to filters into a single dictionary ( of current selection )
        :param filter_to_apply: Filters to apply
        :return: Single dictionary with all filters_to
        """
        unified_filter = {"DATES_TO": "", "MEMBERS_TO": "", "CHUNKS_TO": "", "SPLITS_TO": ""}
        for filter_to in filter_to_apply:
            if "STATUS" not in unified_filter and filter_to.get("STATUS", None):
                unified_filter["STATUS"] = filter_to["STATUS"]
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

    def _filter_current_job(self, current_job, relationships):
        '''
        This function will filter the current job based on the relationships given
        :param current_job: Current job to filter
        :param relationships: Relationships to apply
        :return: dict() with the filters to apply, or empty dict() if no filters to apply
        '''

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
            # Look for a starting point, this can be if else becasue they're exclusive as a
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

    def _add_edges_map_info(self, job, special_status):
        """
        Special relations to be check in the update_list method
        :param job: Current job
        :param parent: parent jobs to check
        :return:
        """
        if special_status not in self.jobs_edges:
            self.jobs_edges[special_status] = set()
        self.jobs_edges[special_status].add(job)
        if "ALL" not in self.jobs_edges:
            self.jobs_edges["ALL"] = set()
        self.jobs_edges["ALL"].add(job)

    def add_special_conditions(self, job, special_conditions, filters_to_apply, parent):
        """
        Add special conditions to the job edge
        :param job: Job
        :param special_conditions: dict
        :param filters_to_apply: dict
        :param parent: parent job
        :return:
        """
        if special_conditions.get("STATUS", None):

            if special_conditions.get("FROM_STEP", None):
                job.max_checkpoint_step = int(special_conditions.get("FROM_STEP", 0)) \
                    if int(special_conditions.get("FROM_STEP", 0)) > job.max_checkpoint_step \
                    else job.max_checkpoint_step
            self._add_edges_map_info(job, special_conditions["STATUS"])  # job_list map
            job.add_edge_info(parent, special_conditions)  # this job

    def _apply_jobs_edge_info(self, job, dependencies):
        # prune first
        job.edge_info = {}
        # get dependency that has special conditions set
        filters_to_apply_by_section = dict()
        for key, dependency in dependencies.items():
            filters_to_apply = self._filter_current_job(job,
                                                        copy.deepcopy(dependency.relationships))
            if "STATUS" in filters_to_apply:
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
                (parents_by_section[self.graph.nodes[parent]['job'].section].
                 add(self.graph.nodes[parent]['job']))
        for key, list_of_parents in parents_by_section.items():
            special_conditions = dict()
            special_conditions["STATUS"] = filters_to_apply_by_section[key].pop("STATUS", None)
            special_conditions["FROM_STEP"] = (filters_to_apply_by_section[key].
                                               pop("FROM_STEP", None))
            special_conditions["ANY_FINAL_STATUS_IS_VALID"] = (filters_to_apply_by_section[key].
                                                        pop("ANY_FINAL_STATUS_IS_VALID", False))

            for parent in list_of_parents:
                self.add_special_conditions(job, special_conditions,
                                            filters_to_apply_by_section[key], parent)

    def find_current_section(self, job_section, section, dic_jobs, distance, visited_section=[]):
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

    def _calculate_natural_dependencies(self, dic_jobs, job, dependency, date, member, chunk,
            graph, dependencies_keys_without_special_chars, distances_of_current_section,
            distances_of_current_section_members, key, dependencies_of_that_section, chunk_list,
            date_list, member_list, special_dependencies, max_distance, problematic_dependencies):
        """
        Calculate natural dependencies and add them to the graph if they're necessary.
        :param dic_jobs: JobList
        :param job: Current job
        :param dependency: Dependency
        :param date: Date
        :param member: Member
        :param chunk: Chunk
        :param graph: Graph
        :param dependencies_keys_without_special_chars: Dependencies of current job without
        special chars ( without SIM-10 -> SIM )
        :param distances_of_current_section: Distances of current section
        :param distances_of_current_section_members: Distances of current section members
        :param key: Key
        :param dependencies_of_that_section: Dependencies of that section ( Dependencies of
        target parent )
        :param chunk_list: Chunk list
        :param date_list: Date list
        :param member_list: Member list
        :param special_dependencies: Special dependencies ( dependencies that comes from
        dependency: special_filters )
        :param max_distance: Max distance ( if a dependency has CLEAN-5 SIM-10, this value
        would be 10 )
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
                found = [aux for aux in dic_jobs.as_conf.jobs_data[parent.section].
                    get("DEPENDENCIES", {}).keys() if job.section == aux]
                if found:
                    continue
            if distances_of_current_section.get(dependency.section, 0) == 0:
                if job.section == parent.section:
                    if not self.actual_job_depends_on_previous_chunk:
                        if parent.section not in self.dependency_map[job.section]:
                            graph.add_edge(parent.name, job.name)
                else:
                    if (self.actual_job_depends_on_special_chunk and not
                        self.actual_job_depends_on_previous_chunk):
                        if parent.section not in self.dependency_map[job.section]:
                            if parent.running == job.running:
                                graph.add_edge(parent.name, job.name)
                    elif not self.actual_job_depends_on_previous_chunk:
                        graph.add_edge(parent.name, job.name)
                    elif (not self.actual_job_depends_on_special_chunk and
                          self.actual_job_depends_on_previous_chunk):
                        if (job.running == "chunk" and job.chunk == 1 or job.running == "member" and
                                parent.running == "member" or job.running == "chunk" and
                                parent.running == "chunk"):
                            graph.add_edge(parent.name, job.name)
            else:
                if job.section == parent.section:
                    if self.actual_job_depends_on_previous_chunk:
                        skip = False
                        for aux in [aux for aux in self.dependency_map[job.section]
                                    if aux != job.section]:
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
                            graph.add_edge(parent.name, job.name)
                else:
                    if job.running == parent.running:
                        skip = False
                        problematic_dependencies.add(parent.name)
                        graph.add_edge(parent.name, job.name)
                    if parent.running == "chunk":
                        if parent.chunk > (len(chunk_list) - max_distance):
                            graph.add_edge(parent.name, job.name)
        JobList.handle_frequency_interval_dependencies(chunk, chunk_list, date, date_list,
                    dic_jobs, job, member, member_list, dependency.section, natural_parents)
        return problematic_dependencies

    def _calculate_filter_dependencies(self, filters_to_apply, dic_jobs, job, dependency, date,
            member, chunk, graph, dependencies_keys_without_special_chars,
            distances_of_current_section, distances_of_current_section_members, key,
            dependencies_of_that_section, chunk_list, date_list, member_list, special_dependencies,
            problematic_dependencies):
        """
        Calculate dependencies that has any kind of filter set and add them to the graph if
        they're necessary.
        :param filters_to_apply: Filters to apply
        :param dic_jobs: JobList
        :param job: Current job
        :param dependency: Dependency
        :param date: Date
        :param member: Member
        :param chunk: Chunk
        :param graph: Graph
        :param dependencies_keys_without_special_chars: Dependencies keys without special chars
        :param distances_of_current_section: Distances of current section
        :param distances_of_current_section_members: Distances of current section members
        :param key: Key
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
        possible_parents = [possible_parent for possible_parent in dic_jobs.get_jobs_filtered(
            dependency.section, job, filters_to_apply, date, member, chunk,
            filters_to_apply_of_parent) if possible_parent.name != job.name]
        for parent in possible_parents:
            edge_added = False
            if any_all_filter:
                if (parent.chunk and parent.chunk != self.depends_on_previous_chunk.get(
                    parent.section, parent.chunk) or (parent.running == "chunk" and
                    parent.chunk != chunk_list[-1] and not filters_to_apply_of_parent) or
                    self.actual_job_depends_on_previous_chunk or
                    self.actual_job_depends_on_special_chunk or parent.name in special_dependencies
                ):
                    continue

            if parent.section == job.section:
                if not job.splits or int(job.splits) > 0:
                    self.depends_on_previous_split[job.section] = int(parent.split)
            if self.actual_job_depends_on_previous_chunk and parent.section == job.section:
                graph.add_edge(parent.name, job.name)
                edge_added = True
            else:
                if parent.name not in self.depends_on_previous_special_section.get(
                        job.section, set()) or job.split > 0:
                    graph.add_edge(parent.name, job.name)
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
                    dic_jobs, job, member, member_list, dependency.section, possible_parents)

        return special_dependencies, problematic_dependencies

    def get_filters_to_apply(self, job, dependency):
        filters_to_apply = self._filter_current_job(job, copy.deepcopy(dependency.relationships))
        filters_to_apply.pop("STATUS", None)
        # Don't do perform special filter if only "FROM_STEP" is applied
        if "FROM_STEP" in filters_to_apply:
            if (filters_to_apply.get("CHUNKS_TO", "none") == "none" and filters_to_apply.
                get("MEMBERS_TO", "none") == "none" and filters_to_apply.get("DATES_TO", "none")
                == "none" and filters_to_apply.get("SPLITS_TO", "none") == "none"):
                filters_to_apply = {}
        filters_to_apply.pop("FROM_STEP", None)
        filters_to_apply.pop("ANY_FINAL_STATUS_IS_VALID", None)

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
        """
        Normalize the 'auto' keyword in the dependency relationships for a job.

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
            auto_splits = str(self.graph.nodes[auto_job_name]['job'].splits)
            for filters_to_keys, filters_to in (
                    dependency.relationships.get("SPLITS_FROM", {}).items()):
                if "auto" in filters_to.get("SPLITS_TO", "").lower():
                    filters_to["SPLITS_TO"] = filters_to["SPLITS_TO"].lower()
                    filters_to["SPLITS_TO"] = filters_to["SPLITS_TO"].replace("auto", auto_splits)
            job.splits = auto_splits
        return dependency

    def _manage_job_dependencies(self, dic_jobs, job, date_list, member_list, chunk_list,
                                 dependencies_keys, dependencies, graph):
        """
        Manage job dependencies
        :param dic_jobs: JobList
        :param job: Current job
        :param date_list: Date list
        :param member_list: Member list
        :param chunk_list: Chunk list
        :param dependencies_keys: Dependencies keys
        :param dependencies: Dependencies
        :param graph: Graph
        :return: problematic_dependencies
        """
        # Initialize variables
        depends_on_previous_section = set()
        distances_of_current_section = {}
        distances_of_current_section_member = {}
        problematic_dependencies = set()
        special_dependencies = set()
        dependencies_to_del = set()
        dependencies_non_natural_to_del = set()
        max_distance = 0
        dependencies_keys_aux = []
        dependencies_keys_without_special_chars = []
        depends_on_itself = None
        if not job.splits:
            child_splits = 0
        elif job.splits != "auto":
            child_splits = int(job.splits)
        parsed_date_list = []
        for dat in date_list:
            parsed_date_list.append(date2str(dat))
        # It is faster to check the conf instead of  calculate 90000000 tasks
        # Prune number of dependencies to check, to reduce the transitive reduction complexity
        for dependency in dependencies_keys.keys():
            if (("-" in dependency and job.section == dependency.split("-")[0]) or (
                    "+" in dependency and job.section == dependency.split("+")[0]) or
                    (job.section == dependency)):
                depends_on_itself = dependency
            else:
                dependencies_keys_aux.append(dependency)
        if depends_on_itself:
            dependencies_keys_aux = dependencies_keys_aux + [depends_on_itself]

        for key_aux_stripped in dependencies_keys_aux:
            if "-" in key_aux_stripped:
                key_aux_stripped = key_aux_stripped.split("-")[0]
            elif "+" in key_aux_stripped:
                key_aux_stripped = key_aux_stripped.split("+")[0]
            dependencies_keys_without_special_chars.append(key_aux_stripped)
        self.dependency_map[job.section] = (self.dependency_map[job.section].
                                            difference(set(dependencies_keys_aux)))
        # If parent already has defined that dependency,
        # skip it to reduce the transitive reduction complexity
        # Calcule distances ( SIM-1, ClEAN-2..)
        for dependency_key in dependencies_keys_aux:
            if "-" in dependency_key:
                aux_key = dependency_key.split("-")[0]
                distance = int(dependency_key.split("-")[1])
            elif "+" in dependency_key:
                aux_key = dependency_key.split("+")[0]
                distance = int(dependency_key.split("+")[1])
            else:
                aux_key = dependency_key
                distance = 0
            if dic_jobs.as_conf.jobs_data.get(aux_key, {}).get("RUNNING", "once") == "chunk":
                distances_of_current_section[aux_key] = distance
            elif dic_jobs.as_conf.jobs_data.get(aux_key, {}).get("RUNNING", "once") == "member":
                distances_of_current_section_member[aux_key] = distance
            if distance != 0:
                if job.running == "chunk":
                    if int(job.chunk) > 1:
                        if (job.section == aux_key or dic_jobs.as_conf.jobs_data.get(aux_key, {}).
                                get("RUNNING", "once") == "chunk"):
                            self.actual_job_depends_on_previous_chunk = True
                if job.running == "member" or job.running == "chunk":
                    # find member in member_list
                    if job.member:
                        if member_list.index(job.member) > 0:
                            if (job.section == aux_key or dic_jobs.as_conf.jobs_data.
                                    get(aux_key, {}).get("RUNNING", "once") == "member"):
                                self.actual_job_depends_on_previous_member = True
            if aux_key != job.section:
                dependencies_of_that_section = (dic_jobs.as_conf.jobs_data[aux_key].
                                                get("DEPENDENCIES", {}))
                for key in dependencies_of_that_section.keys():
                    if "-" in key:
                        stripped_key = key.split("-")[0]
                        distance_ = int(key.split("-")[1])
                    elif "+" in key:
                        stripped_key = key.split("+")[0]
                        distance_ = int(key.split("+")[1])
                    else:
                        stripped_key = key
                        distance_ = 0
                    if (stripped_key in dependencies_keys_without_special_chars and
                            stripped_key != job.section):
                        # Fix delay
                        if (job.running == "chunk" and dic_jobs.as_conf.jobs_data[aux_key].
                                get("DELAY", None)):
                            if job.chunk <= int(dic_jobs.as_conf.jobs_data[aux_key].
                                                        get("DELAY", 0)):
                                continue
                        # check doc example
                        if (dependencies.get(stripped_key, None) and not
                            dependencies[stripped_key].relationships):
                            dependencies_to_del.add(key)

        max_distance = 0
        for key in self.dependency_map_with_distances[job.section]:
            if "-" in key:
                aux_key = key.split("-")[0]
                distance = int(key.split("-")[1])
            elif "+" in key:
                aux_key = key.split("+")[0]
                distance = int(key.split("+")[1])
            else:
                distance = 0
            max_distance = max(max_distance, distance)
            if dic_jobs.as_conf.jobs_data.get(aux_key, {}).get("RUNNING", "once") == "chunk":
                if aux_key in distances_of_current_section:
                    if distance > distances_of_current_section[aux_key]:
                        distances_of_current_section[aux_key] = distance
            elif dic_jobs.as_conf.jobs_data.get(aux_key, {}).get("RUNNING", "once") == "member":
                if aux_key in distances_of_current_section_member:
                    if distance > distances_of_current_section_member[aux_key]:
                        distances_of_current_section_member[aux_key] = distance
        ### Adding the dependency to the graph if possible
        sections_to_calculate = [key for key in dependencies_keys_aux
                                 if key not in dependencies_to_del]
        natural_sections = list()
        # Parse first sections with special filters if any
        for key in sections_to_calculate:
            dependency = dependencies[key]
            skip, (chunk, member, date) = JobList._calculate_dependency_metadata(
                job.chunk, chunk_list, job.member, member_list, job.date, date_list, dependency)
            if skip:
                continue
            self._normalize_auto_keyword(job, dependency)
            filters_to_apply = self.get_filters_to_apply(job, dependency)

            if len(filters_to_apply) > 0:
                dependencies_of_that_section = (dic_jobs.as_conf.jobs_data[dependency.section].
                                                get("DEPENDENCIES", {}))
                # Adds the dependencies to the job, and if not possible, adds the job to the
                # problematic_dependencies
                special_dependencies, problematic_dependencies = (
                    self._calculate_filter_dependencies(filters_to_apply, dic_jobs, job, dependency,
                        date, member, chunk, graph, dependencies_keys_without_special_chars,
                        distances_of_current_section, distances_of_current_section_member, key,
                        dependencies_of_that_section, chunk_list, date_list, member_list,
                        special_dependencies, problematic_dependencies))
            if key in dependencies_non_natural_to_del:
                continue
            natural_sections.append(key)
        for key in natural_sections:
            dependency = dependencies[key]
            skip, (chunk, member, date) = JobList._calculate_dependency_metadata(
                job.chunk, chunk_list, job.member, member_list, job.date, date_list, dependency)
            if skip:
                continue
            aux = dic_jobs.as_conf.jobs_data[dependency.section].get("DEPENDENCIES", {})
            dependencies_of_that_section = []
            for key_aux_stripped in aux.keys():
                if "-" in key_aux_stripped:
                    key_aux_stripped = key_aux_stripped.split("-")[0]
                elif "+" in key_aux_stripped:
                    key_aux_stripped = key_aux_stripped.split("+")[0]

                dependencies_of_that_section.append(key_aux_stripped)
            # Adds the dependencies to the job, and if not possible, adds the job to the
            # problematic_dependencies
            problematic_dependencies = self._calculate_natural_dependencies(dic_jobs, job,
                dependency, date, member, chunk, graph, dependencies_keys_without_special_chars,
                distances_of_current_section, distances_of_current_section_member, key,
                dependencies_of_that_section, chunk_list, date_list, member_list,
                special_dependencies, max_distance, problematic_dependencies)
        return problematic_dependencies

    @staticmethod
    def _calculate_dependency_metadata(chunk, chunk_list, member, member_list, date,
                                       date_list, dependency):
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
            Log.debug("Creating {0} jobs".format(section))
            dic_jobs.read_section(section, priority, default_job_type)
            priority += 1

    def _create_sorted_dict_jobs(self, wrapper_jobs):
        """
        Creates a sorting of the jobs whose job.section is in wrapper_jobs, according to the
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
            if type(wrapper_jobs) is not list:
                if "&" in wrapper_jobs:
                    char = "&"
                else:
                    char = " "
                wrapper_jobs = wrapper_jobs.split(char)

            for section in wrapper_jobs:
                # RUNNING = once, as default. This value comes from jobs_.yml
                sections_running_type_map[section] = str(self._config.experiment_data["JOBS"].
                                                         get(section, {}).get("RUNNING", 'once'))

            # Select only relevant jobs, those belonging to the sections defined in the wrapper

        sections_to_filter = ""
        for section in sections_running_type_map:
            sections_to_filter += section

        filtered_jobs_list = [job for job in self._job_list if
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
                            section_running_type = sections_running_type_map[job.section]
                    # Test if RUNNING is different between sections, or if we have reached
                    # the last item in sorted_jobs_list
                    if ((previous_section_running_type is not None and
                            previous_section_running_type != section_running_type) or
                            index == len(sorted_jobs_list)):

                        # Sorting by date, member, chunk number if it is a chunk job otherwise
                        # num_chunks from JOB TYPE (section)
                        # Important to note that the only differentiating factor would be chunk
                        # OR num_chunks
                        jobs_to_sort = sorted(jobs_to_sort, key=lambda k: (
                            k.name.split('_')[1], (k.name.split('_')[2]), (int(k.name.split('_')[3])
                            if len(k.name.split('_')) == 5 else num_chunks + 1)))

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

        return dict_jobs

    def _create_fake_dates_members(self, filtered_jobs_list):
        """
        Using the list of jobs provided, creates clones of these jobs and modifies names conditioned
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
                fake_job = copy.deepcopy(job)
                # Use it to modify name of fake job
                fake_job.name = fake_job.name.split('_', 2)[0] + "_" + fake_job.name.split('_', 2)[
                    1] + "_" + member + "_" + fake_job.name.split("_", 2)[2]
                # Filling list of fake jobs, only difference is the name
                filtered_jobs_fake_date_member.append(fake_job)
                # Mapping fake jobs to original ones
                fake_original_job_map[fake_job] = job
            # There was no result
            if fake_job is None:
                filtered_jobs_fake_date_member.append(job)

        return filtered_jobs_fake_date_member, fake_original_job_map

    def _get_date(self, date):
        """
        Parses a user defined Date (from [experiment] DATELIST)
        to return a special String representation of that Date

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
        return self._job_list.__len__()

    def get_date_list(self):
        """
        Get inner date list

        :return: date list
        :rtype: list
        """
        return self._date_list

    def get_member_list(self):
        """
        Get inner member list

        :return: member list
        :rtype: list
        """
        return self._member_list

    def get_chunk_list(self):
        """
        Get inner chunk list

        :return: chunk list
        :rtype: list
        """
        return self._chunk_list

    def get_job_list(self):
        """
        Get inner job list

        :return: job list
        :rtype: list
        """
        return self._job_list

    def get_date_format(self):
        date_format = ''
        for date in self.get_date_list():
            if date.hour > 1:
                date_format = 'H'
            if date.minute > 1:
                date_format = 'M'
        return date_format

    def copy_ordered_jobs_by_date_member(self):
        pass

    def get_ordered_jobs_by_date_member(self, section):
        """
        Get the dictionary of jobs ordered according to wrapper's
        expression divided by date and member

        :return: jobs ordered divided by date and member
        :rtype: dict
        """
        if len(self._ordered_jobs_by_date_member) > 0:
            return self._ordered_jobs_by_date_member[section]

    def get_completed(self, platform=None, wrapper=False):
        """
        Returns a list of completed jobs

        :param wrapper:
        :param platform: job platform
        :type platform: HPCPlatform
        :return: completed jobs
        :rtype: list
        """

        completed_jobs = [job for job in self._job_list if (platform is None or
                        job.platform.name == platform.name) and job.status == Status.COMPLETED]
        if wrapper:
            return [job for job in completed_jobs if job.packed is False]
        return completed_jobs

    def get_completed_failed_without_logs(self, platform: Any = None) -> List[Any]:
        """
        Returns a list of completed or failed jobs without updated logs.

        Args:
            platform Platform: Job platform, defaults to None.

        Returns:
            List[Job]: List of completed and failed jobs without updated logs.
        """

        completed_failed_jobs = [job for job in self._job_list if
                             (platform is None or job.platform.name == platform.name) and
                             (job.status == Status.COMPLETED or job.status == Status.FAILED) and
                             job.updated_log is False]

        return completed_failed_jobs

    def get_uncompleted(self, platform=None, wrapper=False):
        """
        Returns a list of completed jobs

        :param wrapper:
        :param platform: job platform
        :type platform: HPCPlatform
        :return: completed jobs
        :rtype: list
        """
        uncompleted_jobs = [job for job in self._job_list if
                            (platform is None or job.platform.name == platform.name) and
                            job.status != Status.COMPLETED]

        if wrapper:
            return [job for job in uncompleted_jobs if job.packed is False]
        return uncompleted_jobs

    def get_uncompleted_and_not_waiting(self, platform=None, wrapper=False):
        """
        Returns a list of completed jobs and waiting

        :param wrapper:
        :param platform: job platform
        :type platform: HPCPlatform
        :return: completed jobs
        :rtype: list
        """
        uncompleted_jobs = [job for job in self._job_list if
                            (platform is None or job.platform.name == platform.name) and
                            job.status != Status.COMPLETED and job.status != Status.WAITING]

        if wrapper:
            return [job for job in uncompleted_jobs if job.packed is False]
        return uncompleted_jobs

    def get_submitted(self, platform=None, hold=False, wrapper=False):
        """
        Returns a list of submitted jobs

        :param wrapper:
        :param hold:
        :param platform: job platform
        :type platform: HPCPlatform
        :return: submitted jobs
        :rtype: list
        """
        submitted = list()
        if hold:
            submitted = [job for job in self._job_list if (platform is None or
                        job.platform.name == platform.name) and job.status == Status.SUBMITTED
                         and job.hold == hold]
        else:
            submitted = [job for job in self._job_list if (platform is None or
                        job.platform.name == platform.name) and job.status == Status.SUBMITTED]
        if wrapper:
            return [job for job in submitted if job.packed is False]
        return submitted

    def get_running(self, platform=None, wrapper=False):
        """
        Returns a list of jobs running

        :param wrapper:
        :param platform: job platform
        :type platform: HPCPlatform
        :return: running jobs
        :rtype: list
        """
        running = [job for job in self._job_list if (platform is None or
                job.platform.name == platform.name) and job.status == Status.RUNNING]
        if wrapper:
            return [job for job in running if job.packed is False]
        return running

    def get_queuing(self, platform=None, wrapper=False):
        """
        Returns a list of jobs queuing

        :param wrapper:
        :param platform: job platform
        :type platform: HPCPlatform
        :return: queuedjobs
        :rtype: list
        """
        queuing = [job for job in self._job_list if (platform is None or
                job.platform.name == platform.name) and job.status == Status.QUEUING]
        if wrapper:
            return [job for job in queuing if job.packed is False]
        return queuing

    def get_failed(self, platform=None, wrapper=False):
        """
        Returns a list of failed jobs

        :param wrapper:
        :param platform: job platform
        :type platform: HPCPlatform
        :return: failed jobs
        :rtype: list
        """
        failed = [job for job in self._job_list if (platform is None or
                job.platform.name == platform.name) and job.status == Status.FAILED]
        if wrapper:
            return [job for job in failed if job.packed is False]
        return failed

    def get_unsubmitted(self, platform=None, wrapper=False):
        """
        Returns a list of unsubmitted jobs

        :param wrapper:
        :param platform: job platform
        :type platform: HPCPlatform
        :return: all jobs
        :rtype: list
        """
        unsubmitted = [job for job in self._job_list if (platform is None or
                    job.platform.name == platform.name) and (job.status != Status.SUBMITTED and
                    job.status != Status.QUEUING and job.status != Status.RUNNING)]

        if wrapper:
            return [job for job in unsubmitted if job.packed is False]
        else:
            return unsubmitted

    def get_all(self, platform=None, wrapper=False):
        """
        Returns a list of all jobs

        :param wrapper:
        :param platform: job platform
        :type platform: HPCPlatform
        :return: all jobs
        :rtype: list
        """
        all_jobs = [job for job in self._job_list]

        if wrapper:
            return [job for job in all_jobs if job.packed is False]
        else:
            return all_jobs

    def get_job_names(self, lower_case=False):
        """
        Returns a list of all job names
        :param: lower_case: if true, returns lower case job names
        :type: lower_case: bool


        :return: all job names
        :rtype: list

        """
        if lower_case:
            all_jobs = [job.name.lower() for job in self._job_list]
        else:
            all_jobs = [job.name for job in self._job_list]

        return all_jobs

    def update_two_step_jobs(self):
        prev_jobs_to_run_first = self.jobs_to_run_first
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
                "but they're unable to run without their parents or there are failed jobs.", 7014)

    def parse_jobs_by_filter(self, unparsed_jobs, two_step_start=True):
        jobs_to_run_first = list()
        select_jobs_by_name = ""  # job_name
        select_all_jobs_by_section = ""  # all
        filter_jobs_by_section = ""  # Select, chunk / member
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
            except Exception as e:
                raise AutosubmitCritical("Check the {0} format."
                                         "\nFirst filter is optional ends with '&'."
                                         "\nSecond filter ends with ';'."
                                         "\nThird filter must contain '['. ".format(unparsed_jobs))
        else:
            try:
                self.rerun_job_list = self.get_job_related(select_jobs_by_name=select_jobs_by_name,
                                           select_all_jobs_by_section=select_all_jobs_by_section,
                                           filter_jobs_by_section=filter_jobs_by_section,
                                           two_step_start=two_step_start)
            except Exception as e:
                raise AutosubmitCritical("Check the {0} format."
                                         "\nFirst filter is optional ends with '&'."
                                         "\nSecond filter ends with ';'."
                                         "\nThird filter must contain '['. ".format(unparsed_jobs))

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
            jobs_by_name = [job for job in self._job_list if
                            re.search("(^|[^0-9a-z_])" + job.name.lower() + "([^a-z0-9_]|$)",
                                      select_jobs_by_name.lower()) is not None]
            jobs_by_name_no_expid = [job for job in self._job_list if
                                     re.search("(^|[^0-9a-z_])" + job.name.lower()[5:] +
                                    "([^a-z0-9_]|$)", select_jobs_by_name.lower()) is not None]
            ultimate_jobs_list.extend(jobs_by_name)
            ultimate_jobs_list.extend(jobs_by_name_no_expid)

        # Second Filter { select all }
        if select_all_jobs_by_section != "":
            all_jobs_by_section = [job for job in self._job_list if
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
                    jobs_filtered = [job for job in self._job_list if
                                     re.search("(^|[^0-9a-z_])" + job.section.upper() +
                                     "([^a-z0-9_]|$)", section_name.upper()) is not None]
                if section_dates != "":
                    jobs_date = [job for job in jobs_filtered if
                                 re.search("(^|[^0-9a-z_])" + date2str(job.date, job.date_format) +
                                 "([^a-z0-9_]|$)", section_dates.lower()) is not None or
                                 job.date is None]

                if section_chunks != "" or section_members != "":
                    jobs_final = [job for job in jobs_date if ( section_chunks == "" or
                            re.search("(^|[^0-9a-z_])" + str(job.chunk) + "([^a-z0-9_]|$)",
                            section_chunks) is not None) and (section_members == "" or
                            re.search( "(^|[^0-9a-z_])" + str(job.member) + "([^a-z0-9_]|$)",
                            section_members.lower()) is not None)]
                ultimate_jobs_list.extend(jobs_final)
        # Duplicates out
        ultimate_jobs_list = list(set(ultimate_jobs_list))
        Log.debug("List of jobs filtered by TWO_STEP_START parameter:\n{0}".
                  format([job.name for job in ultimate_jobs_list]))
        return ultimate_jobs_list

    def get_logs(self):
        """
        Returns a dict of logs by jobs_name jobs

        :return: logs
        :rtype: dict(tuple)
        """
        logs = dict()
        for job in self._job_list:
            logs[job.name] = (job.local_logs, job.remote_logs)
        return logs

    def add_logs(self, logs):
        """
        add logs to the current job_list
        :return: logs
        :rtype: dict(tuple)
        """

        for job in self._job_list:
            if job.name in logs:
                job.local_logs = logs[job.name][0]
                job.remote_logs = logs[job.name][1]

    def get_ready(self, platform=None, hold=False, wrapper=False):
        """
        Returns a list of ready jobs

        :param wrapper:
        :param hold:
        :param platform: job platform
        :type platform: HPCPlatform
        :return: ready jobs
        :rtype: list
        """
        ready = [job for job in self._job_list if
                 (platform is None or platform == "" or job.platform.name == platform.name) and
                 job.status == Status.READY and job.hold is hold]

        if wrapper:
            return [job for job in ready if job.packed is False]
        return ready

    def get_prepared(self, platform=None):
        """
        Returns a list of prepared jobs

        :param platform: job platform
        :type platform: HPCPlatform
        :return: prepared jobs
        :rtype: list
        """
        prepared = [job for job in self._job_list if (platform is None or
                    job.platform.name == platform.name) and job.status == Status.PREPARED]
        return prepared

    def get_delayed(self, platform=None):
        """
        Returns a list of delayed jobs

        :param platform: job platform
        :type platform: HPCPlatform
        :return: delayed jobs
        :rtype: list
        """
        delayed = [job for job in self._job_list if (platform is None or
                    job.platform.name == platform.name) and job.status == Status.DELAYED]
        return delayed

    def get_skipped(self, platform=None):
        """
        Returns a list of skipped jobs

        :param platform: job platform
        :type platform: HPCPlatform
        :return: skipped jobs
        :rtype: list
        """
        skipped = [job for job in self._job_list if (platform is None or
                    job.platform.name == platform.name) and job.status == Status.SKIPPED]
        return skipped

    def get_waiting(self, platform=None, wrapper=False):
        """
        Returns a list of jobs waiting

        :param wrapper:
        :param platform: job platform
        :type platform: HPCPlatform
        :return: waiting jobs
        :rtype: list
        """
        waiting_jobs = [job for job in self._job_list if (platform is None or
                        job.platform.name == platform.name) and job.status == Status.WAITING]
        if wrapper:
            return [job for job in waiting_jobs if job.packed is False]
        return waiting_jobs

    def get_waiting_remote_dependencies(self, platform_type='slurm'.lower()):
        """
        Returns a list of jobs waiting on slurm scheduler
        :param platform_type: platform type
        :type platform_type: str
        :return: waiting jobs
        :rtype: list

        """
        waiting_jobs = [job for job in self._job_list if (
                job.platform.type == platform_type and job.status == Status.WAITING)]
        return waiting_jobs

    def get_held_jobs(self, platform=None):
        """
        Returns a list of jobs in the platforms (Held)

        :param platform: job platform
        :type platform: HPCPlatform
        :return: jobs in platforms
        :rtype: list
        """
        return [job for job in self._job_list if (platform is None or
                job.platform.name == platform.name) and job.status == Status.HELD]

    def get_unknown(self, platform=None, wrapper=False):
        """
        Returns a list of jobs on unknown state

        :param wrapper:
        :param platform: job platform
        :type platform: HPCPlatform
        :return: unknown state jobs
        :rtype: list
        """
        submitted = [job for job in self._job_list if (platform is None or
                    job.platform.name == platform.name) and job.status == Status.UNKNOWN]
        if wrapper:
            return [job for job in submitted if job.packed is False]
        return submitted

    def get_suspended(self, platform=None, wrapper=False):
        """
        Returns a list of jobs on unknown state

        :param wrapper:
        :param platform: job platform
        :type platform: HPCPlatform
        :return: unknown state jobs
        :rtype: list
        """
        suspended = [job for job in self._job_list if (platform is None or
                    job.platform.name == platform.name) and job.status == Status.SUSPENDED]
        if wrapper:
            return [job for job in suspended if job.packed is False]
        return suspended

    def get_in_queue(self, platform=None, wrapper=False):
        """
        Returns a list of jobs in the platforms (Submitted, Running, Queuing, Unknown,Held)

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

    def get_not_in_queue(self, platform=None, wrapper=False):
        """
        Returns a list of jobs NOT in the platforms (Ready, Waiting)

        :param wrapper:
        :param platform: job platform
        :type platform: HPCPlatform
        :return: jobs not in platforms
        :rtype: list
        """
        not_queued = self.get_ready(platform) + self.get_waiting(platform)
        if wrapper:
            return [job for job in not_queued if job.packed is False]
        return not_queued

    def get_finished(self, platform=None, wrapper=False):
        """
        Returns a list of jobs finished (Completed, Failed)


        :param wrapper:
        :param platform: job platform
        :type platform: HPCPlatform
        :return: finished jobs
        :rtype: list
        """
        finished = self.get_completed(platform) + self.get_failed(platform)
        if wrapper:
            return [job for job in finished if job.packed is False]
        return finished

    def get_active(self, platform=None, wrapper=False):
        """
        Returns a list of active jobs (In platforms queue + Ready)

        :param wrapper:
        :param platform: job platform
        :type platform: HPCPlatform
        :return: active jobs
        :rtype: list
        """

        active = (self.get_in_queue(platform) + self.get_ready(
            platform=platform, hold=True) + self.get_ready(platform=platform, hold=False) +
                  self.get_delayed(platform=platform))
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

    def get_job_by_name(self, name):
        """
        Returns the job that its name matches parameter name

        :parameter name: name to look for
        :type name: str
        :return: found job
        :rtype: job
        """
        for job in self._job_list:
            if job.name == name:
                return job

    def get_jobs_by_section(self, section_list: list, banned_jobs: list = None,
                            get_only_non_completed: bool = False) -> list:
        """
        Get jobs by section.

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
        for job in self._job_list:
            if job.section.upper() in section_list and job.name not in banned_jobs:
                if get_only_non_completed:
                    if job.status != Status.COMPLETED:
                        jobs.append(job)
                else:
                    jobs.append(job)
        return jobs

    def get_in_queue_grouped_id(self, platform):
        # type: (object) -> Dict[int, List[Job]]
        jobs = self.get_in_queue(platform)
        jobs_by_id = dict()
        for job in jobs:
            if job.id not in jobs_by_id:
                jobs_by_id[job.id] = list()
            jobs_by_id[job.id].append(job)
        for job_id in jobs_by_id.keys():
            if len(jobs_by_id[job_id]) == 1:
                jobs_by_id[job_id] = jobs_by_id[job_id][0]
        return jobs_by_id

    def get_in_ready_grouped_id(self, platform):
        jobs = []
        [jobs.append(job) for job in jobs if (
                platform is None or job.platform.name is platform.name)]

        jobs_by_id = dict()
        for job in jobs:
            if job.id not in jobs_by_id:
                jobs_by_id[job.id] = list()
            jobs_by_id[job.id].append(job)
        return jobs_by_id

    def sort_by_name(self):
        """
        Returns a list of jobs sorted by name

        :return: jobs sorted by name
        :rtype: list
        """
        return sorted(self._job_list, key=lambda k: k.name)

    def sort_by_id(self):
        """
        Returns a list of jobs sorted by id

        :return: jobs sorted by ID
        :rtype: list
        """
        return sorted(self._job_list, key=lambda k: k.id)

    def sort_by_type(self):
        """
        Returns a list of jobs sorted by type

        :return: job sorted by type
        :rtype: list
        """
        return sorted(self._job_list, key=lambda k: k.type)

    def sort_by_status(self):
        """
        Returns a list of jobs sorted by status

        :return: job sorted by status
        :rtype: list
        """
        return sorted(self._job_list, key=lambda k: k.status)

    @staticmethod
    def load_file(filename):
        """
        Recreates a stored joblist from the pickle file

        :param filename: pickle file to load
        :type filename: str
        :return: loaded joblist object
        :rtype: JobList
        """
        try:
            if os.path.exists(filename):
                fd = open(filename, 'rb')
                return pickle.load(fd)
            else:
                return list()
        except IOError:
            Log.printlog(
                "Autosubmit will use a backup for recover the job_list", 6010)
            return list()

    def load(self, create=False, backup=False):
        """
        Recreates a stored job list from the persistence

        :return: loaded job list object
        :rtype: JobList
        """
        try:
            if not backup:
                Log.info("Loading JobList")
                return self._persistence.load(self._persistence_path, self._persistence_file)
            else:
                return self._persistence.load(self._persistence_path,
                                              self._persistence_file + "_backup")
        except ValueError as e:
            if not create:
                raise AutosubmitCritical(
                    f'JobList could not be loaded due pkl being saved with a different version '
                    f'of Autosubmit or Python version. {e}')
            else:
                Log.warning(
                    f'Job list will be created from scratch due pkl being saved with a different '
                    f'version of Autosubmit or Python version. {e}')
        except PermissionError as e:
            if not create:
                raise AutosubmitCritical(f'JobList could not be loaded due to permission error.{e}')
            else:
                Log.warning(f'Job list will be created from scratch due to permission error. {e}')
        except BaseException as e:
            if not backup:
                Log.debug("Autosubmit will use a backup to recover the job_list")
                return self.load(create, True)
            else:
                if not create:
                    raise AutosubmitCritical(f"JobList could not be loaded due: "
                                             f"{e}\nAutosubmit won't do anything")
                else:
                    Log.warning(f'Joblist will be created from scratch due: {e}')

    def save(self):
        """
        Persists the job list
        """

        try:
            job_list = None
            if self.run_members is not None and len(str(self.run_members)) > 0:
                job_names = [job.name for job in self._job_list]
                job_list = [job for job in self._job_list]
                for job in self._base_job_list:
                    if job.name not in job_names:
                        job_list.append(job)
            self.update_status_log()

            try:
                self._persistence.save(self._persistence_path, self._persistence_file,
                                       self._job_list if self.run_members is None or
                                       job_list is None else job_list, self.graph)
            except BaseException as e:
                raise AutosubmitError(str(e), 6040, "Failure while saving the job_list")
        except AutosubmitError as e:
            raise
        except BaseException as e:
            raise AutosubmitError(str(e), 6040, "Unknown failure while saving the job_list")

    def backup_save(self):
        """
        Persists the job list
        """
        self._persistence.save(self._persistence_path,
                               self._persistence_file + "_backup", self._job_list)

    def update_status_log(self):

        exp_path = os.path.join(BasicConfig.LOCAL_ROOT_DIR, self.expid)
        tmp_path = os.path.join(exp_path, BasicConfig.LOCAL_TMP_DIR)
        aslogs_path = os.path.join(tmp_path, BasicConfig.LOCAL_ASLOG_DIR)
        Log.reset_status_file(os.path.join(aslogs_path, "jobs_active_status.log"), "status")
        Log.reset_status_file(os.path.join(aslogs_path, "jobs_failed_status.log"), "status_failed")
        job_list = self.get_completed()[-5:] + self.get_in_queue()
        failed_job_list = self.get_failed()
        if len(job_list) > 0:
            Log.status("\n{0:<35}{1:<15}{2:<15}{3:<20}{4:<15}", "Job Name",
                       "Job Id", "Job Status", "Job Platform", "Job Queue")
        if len(failed_job_list) > 0:
            Log.status_failed("\n{0:<35}{1:<15}{2:<15}{3:<20}{4:<15}", "Job Name",
                              "Job Id", "Job Status", "Job Platform", "Job Queue")
        for job in job_list:
            if job.platform and len(job.queue) > 0 and str(job.platform.queue).lower() != "none":
                queue = job.queue
            elif (job.platform and len(job.platform.queue) > 0 and
                  str(job.platform.queue).lower() != "none"):
                queue = job.platform.queue
            else:
                queue = job.queue
            platform_name = job.platform.name if job.platform else "no-platform"
            if job.id is None:
                job_id = "no-id"
            else:
                job_id = job.id
            try:
                Log.status("{0:<35}{1:<15}{2:<15}{3:<20}{4:<15}", job.name, job_id, Status(
                ).VALUE_TO_KEY[job.status], platform_name, queue)
            except Exception:
                Log.debug("Couldn't print job status for job {0}".format(job.name))
        for job in failed_job_list:
            if len(job.queue) < 1:
                queue = "no-scheduler"
            else:
                queue = job.queue
            Log.status_failed("{0:<35}{1:<15}{2:<15}{3:<20}{4:<15}", job.name, job.id, Status(
            ).VALUE_TO_KEY[job.status], job.platform.name, queue)

    def update_from_file(self, store_change=True):
        """
        Updates jobs list on the fly from and update file
        :param store_change: if True, renames the update
        file to avoid reloading it at the next iteration
        """
        if os.path.exists(os.path.join(self._persistence_path, self._update_file)):
            Log.info("Loading updated list: {0}".format(
                os.path.join(self._persistence_path, self._update_file)))
            for line in open(os.path.join(self._persistence_path, self._update_file)):
                if line.strip() == '':
                    continue
                job = self.get_job_by_name(line.split()[0])
                if job:
                    job.status = self._stat_val.retval(line.split()[1])
                    job._fail_count = 0
            now = localtime()
            output_date = strftime("%Y%m%d_%H%M", now)
            if store_change:
                move(os.path.join(self._persistence_path, self._update_file),
                     os.path.join(self._persistence_path, self._update_file +
                                  "_" + output_date))

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

    def check_special_status(self) -> List[Job]:
        """
        Check if all parents of a job have the correct status for checkpointing.

        :returns: jobs_to_check - Jobs that fulfill the special conditions.
        """
        jobs_to_check = []
        jobs_to_skip = []
        for target_status, sorted_job_list in self.jobs_edges.items():
            if target_status == "ALL":
                continue
            for job in sorted_job_list:
                if job.status != Status.WAITING:
                    continue
                if target_status in ["RUNNING", "FAILED"]:
                    self._check_checkpoint(job)
                non_completed_parents_current, completed_parents = (
                    self._count_parents_status(job, target_status))
                if ((len(non_completed_parents_current) + len(completed_parents)) ==
                        len(job.parents)):
                    if job not in jobs_to_skip:
                        jobs_to_check.append(job)
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

    @staticmethod
    def _count_parents_status(job: Job, target_status: str) -> Tuple[List[Job], List[Job]]:
        """
        Count the number of completed and non-completed parents.

        :param job: The job to check.
        :param target_status: The target status to compare against.
        :return: A tuple containing two lists:
            - non_completed_parents_current: Non-completed parents.
            - completed_parents: Completed parents.
        """
        non_completed_parents_current = []
        completed_parents = [parent for parent in job.parents if parent.status == Status.COMPLETED]
        for parent in job.edge_info[target_status].values():
            if (target_status in ["RUNNING", "FAILED"] and parent[1] and int(parent[1]) >=
                    job.current_checkpoint_step):
                continue
            current_status = Status.VALUE_TO_KEY[parent[0].status]
            if (Status.LOGICAL_ORDER.index(current_status) >=
                    Status.LOGICAL_ORDER.index(target_status)):
                if parent[0] not in completed_parents:
                    non_completed_parents_current.append(parent[0])
        return non_completed_parents_current, completed_parents

    def update_log_status(self, job, as_conf, new_run=False):
        """
        Updates the log err and log out.
        """
        # hasattr for backward compatibility (job.updated_logs is only for newer jobs,
        # as the loaded ones may not have this set yet)
        if not hasattr(job, "updated_log"):
            job.updated_log = False
        elif job.updated_log:
            return
        # X11 has it log written in the run.out file.
        # No need to check for log files as there are none
        if hasattr(job, "x11") and job.x11:
            job.updated_log = True
            return
        log_recovered = self.check_if_log_is_recovered(job)
        if log_recovered:
            job.updated_log = True
            # TODO in pickle -> db/yaml migration(I): 
            #  Do the save of the job here then clean attributes from mem ( or even the full job )
            job.clean_attributes()
            # TODO in pickle -> db/yaml migration(II): 
            #  And remove these two lines
            # we only want the last one
            job.local_logs = (log_recovered.name, log_recovered.name[:-4] + ".err")
            job.updated_log = True
        elif new_run and not job.updated_log and str(
                as_conf.platforms_data.get(job.platform.name, {}).get('DISABLE_RECOVERY_THREADS',
                                                                      "false")).lower() == "false":
            job.platform.add_job_to_log_recover(job)
        return log_recovered

    def check_if_log_is_recovered(self, job: Job) -> Path:
        """
        Check if the log is recovered.

        Conditions:
        - File must exist.
        - File timestamp should be greater than the job ready_date,
        otherwise it is from a previous run.

        Args:
            job (Job): The job object to check the log for.

        Returns:
            Path: The path to the recovered log file if found, otherwise None.
        """

        if not hasattr(job, "updated_log") or not job.updated_log:
            for log_recovered in self.path_to_logs.glob(f"{job.name}.*.out"):
                file_timestamp = int(
                    datetime.datetime.fromtimestamp(log_recovered.stat().st_mtime).
                    strftime("%Y%m%d%H%M%S"))
                if job.ready_date and file_timestamp >= int(job.ready_date):
                    return log_recovered
        return None

    def update_list(self, as_conf: AutosubmitConfig, store_change: bool = True,
                    fromSetStatus: bool = False, submitter: object = None,
                    first_time: bool = False) -> bool:
        """
        Updates job list, resetting failed jobs and changing to READY
        all WAITING jobs with all parents COMPLETED

        :param first_time:
        :param submitter:
        :param fromSetStatus:
        :param store_change:
        :param as_conf: autosubmit config object
        :type as_conf: AutosubmitConfig
        :return: True if job status were modified, False otherwise
        :rtype: bool
        """
        # load updated file list
        save = False
        if self.update_from_file(store_change):
            save = store_change
        Log.debug('Updating FAILED jobs')
        if not first_time:
            for job in self.get_failed():
                if as_conf.jobs_data[job.section].get("RETRIALS", None) is None:
                    retrials = int(as_conf.get_retrials())
                else:
                    retrials = int(job.retrials)
                if job.fail_count < retrials:
                    job.inc_fail_count()
                    tmp = [
                        parent for parent in job.parents if parent.status == Status.COMPLETED]
                    if len(tmp) == len(job.parents):
                        aux_job_delay = 0
                        if job.delay_retrials:
                            if ("+" == str(job.delay_retrials)[0] or "*" ==
                                    str(job.delay_retrials)[0]):
                                aux_job_delay = int(job.delay_retrials[1:])
                            else:
                                aux_job_delay = int(job.delay_retrials)

                        if (as_conf.jobs_data[job.section].get("DELAY_RETRY_TIME", None) or 
                                aux_job_delay <= 0):
                            delay_retry_time = str(as_conf.get_delay_retry_time())
                        else:
                            delay_retry_time = job.retry_delay
                        if "+" in delay_retry_time:
                            retry_delay = (job.fail_count * int(delay_retry_time[:-1]) +
                                           int(delay_retry_time[:-1]))
                        elif "*" in delay_retry_time:
                            retry_delay = int(delay_retry_time[1:])
                            for retrial_amount in range(0, job.fail_count):
                                retry_delay += retry_delay * 10
                        else:
                            retry_delay = int(delay_retry_time)
                        if retry_delay > 0:
                            job.status = Status.DELAYED
                            job.delay_end = (datetime.datetime.now() +
                                             datetime.timedelta(seconds=retry_delay))
                            Log.debug("Resetting job: {0} status to: DELAYED for retrial...".
                                      format(job.name))
                        else:
                            job.status = Status.READY
                            Log.debug(f"Resetting job: {job.name} status to: READY for retrial...")
                        job.id = None
                        save = True

                    else:
                        job.status = Status.WAITING
                        save = True
                        Log.debug(f"Resetting job: {job.name} status to: "
                                  f"WAITING for parents completion...")
                else:
                    job.status = Status.FAILED
                    save = True
        else:
            for job in [job for job in self._job_list if job.status in
                            [Status.WAITING, Status.READY, Status.DELAYED, Status.PREPARED]]:
                job.fail_count = 0
        # Check checkpoint jobs, the status can be Any
        for job in self.check_special_status():
            job.status = Status.READY
            # Run start time in format (YYYYMMDDHH:MM:SS) from current time
            job.id = None
            job.wrapper_type = None
            save = True
            Log.debug(f"Special condition fulfilled for job {job.name}")
        # if waiting jobs has all parents completed change its State to READY
        for job in self.get_completed():
            # Log name has this format:
            # a02o_20000101_fc0_2_SIM.20240212115021.err
            # $jobname.$(YYYYMMDDHHMMSS).err or .out
            if job.synchronize is not None and len(str(job.synchronize)) > 0:
                tmp = [parent for parent in job.parents if parent.status == Status.COMPLETED]
                if len(tmp) != len(job.parents):
                    tmp2 = [parent for parent in job.parents if
                            parent.status == Status.COMPLETED or
                            parent.status == Status.SKIPPED or parent.status == Status.FAILED]
                    if len(tmp2) == len(job.parents):
                        for parent in job.parents:
                            if () and parent.status != Status.COMPLETED:
                                job.status = Status.WAITING
                                save = True
                                Log.debug("Resetting sync job: {0} status to: WAITING "
                                          "for parents completion...".format(job.name))
                                break
                    else:
                        job.status = Status.WAITING
                        save = True
                        Log.debug("Resetting sync job: {0} status to: WAITING "
                                    "for parents completion...".format(job.name))
        Log.debug('Updating WAITING jobs')
        if not fromSetStatus:
            all_parents_completed = []
            for job in self.get_delayed():
                if datetime.datetime.now() >= job.delay_end:
                    job.status = Status.READY
            for job in self.get_waiting():
                tmp = [parent for parent in job.parents if
                       parent.status == Status.COMPLETED or parent.status == Status.SKIPPED]
                tmp2 = [parent for parent in job.parents if
                        parent.status == Status.COMPLETED or parent.status == Status.SKIPPED
                        or parent.status == Status.FAILED]
                tmp3 = [parent for parent in job.parents if
                        parent.status == Status.SKIPPED or parent.status == Status.FAILED]
                failed_ones = [parent for parent in job.parents if parent.status == Status.FAILED]
                if job.parents is None or len(tmp) == len(job.parents):
                    job.status = Status.READY
                    job.hold = False
                    Log.debug("Setting job: {0} status to: READY "
                              "(all parents completed)...".format(job.name))
                    if as_conf.get_remote_dependencies() == "true":
                        all_parents_completed.append(job.name)
                if job.status != Status.READY:
                    if len(tmp3) != len(job.parents):
                        if len(tmp2) == len(job.parents):
                            strong_dependencies_failure = False
                            weak_dependencies_failure = False
                            for parent in failed_ones:
                                if (parent.name in job.edge_info and job.edge_info[parent.name].
                                        get('optional', False)):
                                    weak_dependencies_failure = True
                                elif parent.section in job.dependencies:
                                    if parent.status not in [Status.COMPLETED, Status.SKIPPED]:
                                        strong_dependencies_failure = True
                                    break
                            if not strong_dependencies_failure and weak_dependencies_failure:
                                job.status = Status.READY
                                job.hold = False
                                Log.debug("Setting job: {0} status to: READY "
                                    "(conditional jobs are completed/failed)...".format(job.name))
                                break
                            if as_conf.get_remote_dependencies() == "true":
                                all_parents_completed.append(job.name)
                    else:
                        if len(tmp3) == 1 and len(job.parents) == 1:
                            for parent in job.parents:
                                if (parent.name in job.edge_info and job.edge_info[parent.name].
                                        get('optional', False)):
                                    job.status = Status.READY
                                    job.hold = False
                                    Log.debug("Setting job: {0} status to: READY"
                                    " (conditional jobs are completed/failed)...".format(job.name))
                                    break
            if as_conf.get_remote_dependencies() == "true":
                for job in self.get_prepared():
                    tmp = [
                        parent for parent in job.parents if parent.status == Status.COMPLETED]
                    tmp2 = [parent for parent in job.parents if
                            parent.status == Status.COMPLETED or parent.status == Status.SKIPPED
                            or parent.status == Status.FAILED]
                    tmp3 = [parent for parent in job.parents if
                            parent.status == Status.SKIPPED or parent.status == Status.FAILED]
                    if len(tmp2) == len(job.parents) and len(tmp3) != len(job.parents):
                        job.status = Status.READY
                        # Run start time in format (YYYYMMDDHH:MM:SS) from current time
                        job.hold = False
                        save = True
                        Log.debug("A job in prepared status has all parent completed, job:"
                                  "{0} status set to: READY ...".format(job.name))
                Log.debug('Updating WAITING jobs eligible for be prepared')
                # Setup job name should be a variable
                for job in self.get_waiting_remote_dependencies('slurm'):
                    if job.name not in all_parents_completed:
                        tmp = [parent for parent in job.parents if (
                            (parent.status == Status.SKIPPED or parent.status == Status.COMPLETED
                             or parent.status == Status.QUEUING or parent.status == Status.RUNNING)
                            and "setup" not in parent.name.lower())]
                        if len(tmp) == len(job.parents):
                            job.status = Status.PREPARED
                            job.hold = True
                            Log.debug(
                                "Setting job: {0} status to: Prepared for be held ("
                                "all parents queuing, running or completed)...".format(job.name))

                Log.debug('Updating Held jobs')
                if self.job_package_map:
                    held_jobs = [job for job in self.get_held_jobs() if (
                            job.id not in list(self.job_package_map.keys()))]
                    held_jobs += [wrapper_job for wrapper_job in list(self.job_package_map.values())
                                  if wrapper_job.status == Status.HELD]
                else:
                    held_jobs = self.get_held_jobs()

                for job in held_jobs:
                    # Wrappers and inner jobs
                    if self.job_package_map and job.id in list(self.job_package_map.keys()):
                        hold_wrapper = False
                        for inner_job in job.job_list:
                            valid_parents = [parent
                                    for parent in inner_job.parents if parent not in job.job_list]
                            tmp = [parent
                                   for parent in valid_parents if parent.status == Status.COMPLETED]
                            if len(tmp) < len(valid_parents):
                                hold_wrapper = True
                        job.hold = hold_wrapper
                        if not job.hold:
                            for inner_job in job.job_list:
                                inner_job.hold = False
                            Log.debug(
                                "Setting job: {0} status to: Queuing (all parents completed)...".
                                format(job.name))
                    else:  # Non-wrapped jobs
                        tmp = [
                            parent for parent in job.parents if parent.status == Status.COMPLETED]
                        if len(tmp) == len(job.parents):
                            job.hold = False
                            Log.debug(
                                "Setting job: {0} status to: Queuing (all parents completed)...".
                                format(job.name))
                        else:
                            job.hold = True
            jobs_to_skip = self.get_skippable_jobs(
                as_conf.get_wrapper_jobs())  # Get A Dict with all jobs that are listed as skippable

            for section in jobs_to_skip:
                for job in jobs_to_skip[section]:
                    # Check only jobs to be pending of canceled if not started
                    if job.status == Status.READY or job.status == Status.QUEUING:
                        jobdate = date2str(job.date, job.date_format)
                        if job.running == 'chunk':
                            for related_job in jobs_to_skip[section]:
                                if (job.chunk < related_job.chunk and job.member ==
                                        related_job.member and jobdate == date2str(
                                        related_job.date, related_job.date_format)):
                                    try:
                                        if job.status == Status.QUEUING:
                                            job.platform.send_command(job.platform.cancel_cmd +
                                                            " " + str(job.id), ignore_log=True)
                                    except Exception as e:
                                        pass  # jobid finished already
                                    job.status = Status.SKIPPED
                                    save = True
                        elif job.running == 'member':
                            members = as_conf.get_member_list()
                            for related_job in jobs_to_skip[section]:
                                if (members.index(job.member) < members.index(related_job.member)
                                        and job.chunk == related_job.chunk and jobdate ==
                                        date2str( related_job.date, related_job.date_format)):
                                    try:
                                        if job.status == Status.QUEUING:
                                            job.platform.send_command(job.platform.cancel_cmd +
                                                        " " + str(job.id), ignore_log=True)
                                    except Exception as e:
                                        pass  # job_id finished already
                                    job.status = Status.SKIPPED
                                    save = True
            # save = True
        # Needed so the main process can know if the job was downloaded
        for job in self.get_ready():
            job.set_ready_date()
        self.update_two_step_jobs()
        Log.debug('Update finished')
        return save

    def update_genealogy(self):
        """
        When we have created the job list, every type of job is created.
        Update genealogy remove jobs that have no templates
        """
        Log.info("Transitive reduction...")
        # This also adds the jobs edges to the job itself (job._parents and job._children)
        self.graph = transitive_reduction(self.graph)
        # update job list view as transitive_Reduction also fills
        # job._parents and job._children if recreate is set
        self._job_list = [job["job"] for job in self.graph.nodes().values()]
        try:
            DbStructure.save_structure(self.graph, self.expid, self._config.experiment_data["STRUCTURES_DIR"])
        except Exception as exp:
            Log.warning(str(exp))

    @threaded
    def check_scripts_threaded(self, as_conf):
        """
        When we have created the scripts, all parameters should have been substituted.
        %PARAMETER% handlers not allowed (thread test)

        :param as_conf: experiment configuration
        :type as_conf: AutosubmitConfig
        """
        as_conf.reload(force_load=True)
        out = True
        for job in self._job_list:
            show_logs = job.check_warnings
            if not job.check_script(as_conf, show_logs):
                out = False
        return out

    def save_wrappers(self, packages_to_save, failed_packages, as_conf, packages_persistence,
                      hold=False, inspect=False):
        for package in packages_to_save:
            if package.jobs[0].id not in failed_packages:
                if hasattr(package, "name"):
                    self.packages_dict[package.name] = package.jobs
                    from ..job.job import WrapperJob
                    wrapper_job = WrapperJob(package.name, package.jobs[0].id, Status.SUBMITTED, 0,
                                  package.jobs, package._wallclock, package._num_processors,
                                  package.platform, as_conf, hold)
                    self.job_package_map[package.jobs[0].id] = wrapper_job
                    if isinstance(package, JobPackageThread):
                        # Saving only when it is a real multi job package
                        # Need to store the wallclock for the is_overwallclock function
                        packages_persistence.save(package, inspect)

    def check_scripts(self, as_conf):
        """
        When we have created the scripts, all parameters should have been substituted.
        %PARAMETER% handlers not allowed

        :param as_conf: experiment configuration
        :type as_conf: AutosubmitConfig
        """
        Log.info("Checking scripts...")
        out = True
        # Implementing checking scripts feedback to the users in a minimum of 4 messages
        count = stage = 0
        for job in (job for job in self._job_list):
            job.update_check_variables(as_conf)
            count += 1
            if (count >= len(self._job_list) / 4 * (stage + 1)) or count == len(self._job_list):
                stage += 1
                Log.info("{} of {} checked".format(count, len(self._job_list)))

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
        """
        Remove a job from the list

        :param job: job to remove
        :type job: Job
        """
        for child in job.children:
            for parent in job.parents:
                child.add_parent(parent)
            child.delete_parent(job)

        for parent in job.parents:
            parent.children.remove(job)

        self._job_list.remove(job)

    def rerun(self, job_list_unparsed, as_conf, monitor=False):
        """
        Updates job list to rerun the jobs specified by a job list
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
        dependencies = dict()

        for job_section in job_sections:
            Log.debug(
                "Reading rerun dependencies for {0} jobs".format(job_section))
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
                        skip, (chunk, member, date) = JobList._calculate_dependency_metadata(
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
            os.path.join(self._config.LOCAL_ROOT_DIR, self._expid,
                         'conf', "jobs_" + self._expid + ".yaml"))
        return jobs_parser

    def remove_rerun_only_jobs(self, notransitive=False):
        """
        Removes all jobs to be run only in reruns
        """
        flag = False
        for job in self._job_list[:]:
            if job.rerun_only == "true":
                self._remove_job(job)
                flag = True

        if flag:
            self.update_genealogy()
        del self._dic_jobs

    def print_with_status(self, statusChange=None, nocolor=False, existingList=None):
        """
        Returns the string representation of the dependency tree of
        the Job List

        :param statusChange: List of changes in the list, supplied in set status
        :type statusChange: List of strings
        :param nocolor: True if the result should not include color codes
        :type nocolor: Boolean
        :param existingList: External List of Jobs that will be printed,
        this excludes the inner list of jobs.
        :type existingList: List of Job Objects
        :return: String representation
        :rtype: String
        """
        # nocolor = True
        allJobs = self.get_all() if existingList is None else existingList
        # Header
        result = (bcolors.BOLD if nocolor is False else '') + \
                 "## String representation of Job List [" + str(len(allJobs)) + "] "
        if statusChange is not None and len(str(statusChange)) > 0:
            result += ("with " + (bcolors.OKGREEN if nocolor is False else '') +
                       str(len(list(statusChange.keys()))) + " Change(s) ##" +
                       (bcolors.ENDC + bcolors.ENDC if nocolor is False else ''))
        else:
            result += " ## "

        # Find root
        roots = []
        for job in allJobs:
            if len(job.parents) == 0:
                roots.append(job)
        visited = list()
        # print(root)
        # root exists
        for root in roots:
            if root is not None and len(str(root)) > 0:
                result += self._recursion_print(root, 0, visited,
                                                statusChange=statusChange, nocolor=nocolor)
            else:
                result += "\nCannot find root."

        return result

    def __repr__(self):
        """
        Returns the string representation of the class.
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
            return f'Job List object'
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
                # order by name, this is for compare 4.0 with 4.1 as the children orden is different
                for child in sorted(children, key=lambda x: x.name):
                    # Continues recursion
                    result += self._recursion_print(
                        child, level, visited, statusChange=statusChange, nocolor=nocolor)
            else:
                result += (" [" + Status.VALUE_TO_KEY[job.status] +
                           "] " if nocolor is True else "")

        return result

    @staticmethod
    def retrieve_packages(BasicConfig, expid, current_jobs=None):
        """
        Retrieves dictionaries that map the collection of packages in the experiment

        :param BasicConfig: Basic configuration
        :type BasicConfig: Configuration Object
        :param expid: Experiment ID
        :type expid: String
        :param current_jobs: list of names of current jobs
        :type current_jobs: list
        :return: job to package, package to job, package to package_id, package to symbol
        :rtype: Dictionary(Job Object, Package), Dictionary(Package, List of Job Objects),
        Dictionary(String, String), Dictionary(String, String)
        """
        # monitor = Monitor()
        packages = None
        try:
            packages = JobPackagePersistence(os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "pkl"),
                                             "job_packages_" + expid).load(wrapper=False)
        except Exception as ex:
            print("Wrapper table not found, trying packages.")
            packages = None
            try:
                packages = JobPackagePersistence(os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid,
                                                "pkl"), "job_packages_" + expid).load(wrapper=True)
            except Exception as exp2:
                packages = None
                pass
            pass

        job_to_package = dict()
        package_to_jobs = dict()
        package_to_package_id = dict()
        package_to_symbol = dict()
        if packages:
            try:
                for exp, package_name, job_name in packages:
                    if len(str(package_name).strip()) > 0:
                        if current_jobs:
                            if job_name in current_jobs:
                                job_to_package[job_name] = package_name
                        else:
                            job_to_package[job_name] = package_name
                    # list_packages.add(package_name)
                for name in job_to_package:
                    package_name = job_to_package[name]
                    package_to_jobs.setdefault(package_name, []).append(name)
                    # if package_name not in package_to_jobs.keys():
                    #     package_to_jobs[package_name] = list()
                    # package_to_jobs[package_name].append(name)
                for key in package_to_jobs:
                    package_to_package_id[key] = key.split("_")[2]
                list_packages = list(job_to_package.values())
                for i in range(len(list_packages)):
                    if i % 2 == 0:
                        package_to_symbol[list_packages[i]] = 'square'
                    else:
                        package_to_symbol[list_packages[i]] = 'hexagon'
            except Exception as ex:
                print((traceback.format_exc()))

        return job_to_package, package_to_jobs, package_to_package_id, package_to_symbol

    @staticmethod
    def retrieve_times(status_code, name, tmp_path, make_exception=False, job_times=None,
                       seconds=False, job_data_collection=None):
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
        queue_time = running_time = 0
        submit_time = datetime.timedelta()
        start_time = datetime.timedelta()
        finish_time = datetime.timedelta()
        running_for_min = datetime.timedelta()
        queuing_for_min = datetime.timedelta()

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

        except Exception as exp:
            print((traceback.format_exc()))
            return

        seconds_queued = seconds_queued * \
                         (-1) if seconds_queued < 0 else seconds_queued
        seconds_running = seconds_running * \
                          (-1) if seconds_running < 0 else seconds_running
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
        """
        Receives job data and returns the data from its TOTAL_STATS file in an ordered way.
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
            except Exception as exp:
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
