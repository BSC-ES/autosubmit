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
import networkx as nx
import re
import os
import pickle
from contextlib import suppress
import traceback
from bscearth.utils.date import date2str, parse_date
from networkx import DiGraph
from shutil import move
from threading import Thread
from time import localtime, strftime, mktime
from typing import List, Dict

import autosubmit.database.db_structure as DbStructure
from autosubmit.helpers.data_transfer import JobRow
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status, bcolors
from autosubmit.job.job_dict import DicJobs
from autosubmit.job.job_package_persistence import JobPackagePersistence
from autosubmit.job.job_packages import JobPackageThread
from autosubmit.job.job_utils import Dependency
from autosubmit.job.job_utils import transitive_reduction
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from log.log import AutosubmitCritical, AutosubmitError, Log


# Log.get_logger("Log.Autosubmit")


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

    def __init__(self, expid, config, parser_factory, job_list_persistence, as_conf):
        self._persistence_path = os.path.join(
            config.LOCAL_ROOT_DIR, expid, "pkl")
        self._update_file = "updated_list_" + expid + ".txt"
        self._failed_file = "failed_job_list_" + expid + ".pkl"
        self._persistence_file = "job_list_" + expid
        self._job_list = list()
        self._base_job_list = list()
        self.jobs_edges = {}
        self._expid = expid
        self._config = config
        self.experiment_data = as_conf.experiment_data
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
        self.depends_on_previous_special = dict()
    @property
    def expid(self):
        """
        Returns the experiment identifier

        :return: experiment's identifier
        :rtype: str
        """
        return self._expid


    @property
    def jobs_data(self):
        return self.experiment_data["JOBS"]

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
            for job in self._job_list:  # We are assuming that the jobs are sorted in topological order (which is the default)
                if (job.member is None and not found_member) or job.member in self._run_members or job.status not in [Status.WAITING, Status.READY]:
                    processed_job_list.append(job)
                if job.member is not None and len(str(job.member)) > 0:
                    found_member = True
            self._job_list = processed_job_list
            # Old implementation that also considered children of the members.        
            # self._job_list = [job for job in old_job_list if len(
            #     job.parents) == 0 or len(set(old_job_list_names).intersection(set([jobp.name for jobp in job.parents]))) == len(job.parents)]

    def create_dictionary(self, date_list, member_list, num_chunks, chunk_ini, date_format, default_retrials,
                          wrapper_jobs, as_conf):
        chunk_list = list(range(chunk_ini, num_chunks + 1))


        dic_jobs = DicJobs(date_list, member_list, chunk_list, date_format, default_retrials, as_conf)
        self._dic_jobs = dic_jobs
        for wrapper_section in wrapper_jobs:
            if str(wrapper_jobs[wrapper_section]).lower() != 'none':
                self._ordered_jobs_by_date_member[wrapper_section] = self._create_sorted_dict_jobs(
                    wrapper_jobs[wrapper_section])
            else:
                self._ordered_jobs_by_date_member[wrapper_section] = {}
        pass

    def _delete_edgeless_jobs(self):
        jobs_to_delete = []
        # indices to delete
        for i, job in enumerate(self._job_list):
            if job.dependencies is not None and job.dependencies not in ["{}","[]"]:
                if (len(job.dependencies) > 0 and not job.has_parents() and not job.has_children()) and str(job.delete_when_edgeless).casefold() == "true".casefold():
                    jobs_to_delete.append(job)
        # delete jobs by indices
        for i in jobs_to_delete:
            self._job_list.remove(i)
            self.graph.remove_node(i.name)


    def generate(self, as_conf, date_list, member_list, num_chunks, chunk_ini, parameters, date_format, default_retrials,
                 default_job_type, wrapper_jobs=dict(), new=True, run_only_members=[], show_log=True, monitor=False, force=False):
        """
        Creates all jobs needed for the current workflow.
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
        if force:
            if os.path.exists(os.path.join(self._persistence_path, self._persistence_file + ".pkl")):
                os.remove(os.path.join(self._persistence_path, self._persistence_file + ".pkl"))
            if os.path.exists(os.path.join(self._persistence_path, self._persistence_file + "_backup.pkl")):
                os.remove(os.path.join(self._persistence_path, self._persistence_file + "_backup.pkl"))
        self._parameters = parameters
        self._date_list = date_list
        self._member_list = member_list
        chunk_list = list(range(chunk_ini, num_chunks + 1))
        self._chunk_list = chunk_list
        try:
            self.graph = self.load()
            if type(self.graph) is not DiGraph:
                self.graph = nx.DiGraph()
        except:
            self.graph = nx.DiGraph()
        self._dic_jobs = DicJobs(date_list, member_list, chunk_list, date_format, default_retrials, as_conf)
        self._dic_jobs.graph = self.graph
        if show_log:
            Log.info("Creating jobs...")

        if len(self.graph.nodes) > 0:
            if show_log:
                Log.info("Load finished")
            if monitor:
                as_conf.experiment_data = as_conf.last_experiment_data
                as_conf.data_changed = False
            if as_conf.data_changed:
                self._dic_jobs.compare_experiment_section()
                # fast-look if graph existed, skips some steps
            if not as_conf.data_changed or (as_conf.data_changed and not new):
                self._dic_jobs._job_list = {job["job"].name: job["job"] for _, job in self.graph.nodes.data() if
                                            job.get("job", None)}
            # Force to use the last known job_list when autosubmit monitor is running.

            self._dic_jobs.last_experiment_data = as_conf.last_experiment_data
        else:
            # Remove the previous pkl, if it exists.
            if not new:
                Log.info("Removing previous pkl file due to empty graph, likely due using an Autosubmit 4.0.XXX version")
            with suppress(FileNotFoundError):
                os.remove(os.path.join(self._persistence_path, self._persistence_file + ".pkl"))
            with suppress(FileNotFoundError):
                os.remove(os.path.join(self._persistence_path, self._persistence_file + "_backup.pkl"))
            new = True
        # This generates the job object and also finds if dic_jobs has modified from previous iteration in order to expand the workflow
        self._create_jobs(self._dic_jobs, 0, default_job_type)
        # not needed anymore all data is inside their correspondent sections in dic_jobs
        # This dic_job is key to the dependencies management as they're ordered by date[member[chunk]]
        del self._dic_jobs._job_list
        if show_log:
            Log.info("Adding dependencies to the graph..")
        # del all nodes that are only in the current graph
        if len(self.graph.nodes) > 0:
            gen = (name for name in set(self.graph.nodes).symmetric_difference(set(self._dic_jobs.workflow_jobs)))
            for name in gen:
                if name in self.graph.nodes:
                    self.graph.remove_node(name)
        # This actually, also adds the node to the graph if it isn't already there
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
                job for job in old_job_list if job.member is None or job.member in run_only_members or job.status not in [Status.WAITING, Status.READY]]
            for job in self._job_list:
                for jobp in job.parents:
                    if jobp in self._job_list:
                        job.parents.add(jobp)
                for jobc in job.children:
                    if jobc in self._job_list:
                        job.children.add(jobc)
        if show_log:
            Log.info("Looking for edgeless jobs...")
        self._delete_edgeless_jobs()
        if new:
            for job in self._job_list:
                job.parameters = parameters
                if not job.has_parents():
                    job.status = Status.READY

        for wrapper_section in wrapper_jobs:
            try:
                if wrapper_jobs[wrapper_section] is not None and len(str(wrapper_jobs[wrapper_section])) > 0:
                    self._ordered_jobs_by_date_member[wrapper_section] = self._create_sorted_dict_jobs(
                        wrapper_jobs[wrapper_section])
                else:
                    self._ordered_jobs_by_date_member[wrapper_section] = {}
            except BaseException as e:
                raise AutosubmitCritical(
                    "Some section jobs of the wrapper:{0} are not in the current job_list defined in jobs.conf".format(
                        wrapper_section), 7014, str(e))


    def _add_dependencies(self,date_list, member_list, chunk_list, dic_jobs, option="DEPENDENCIES"):
        jobs_data = dic_jobs.experiment_data.get("JOBS",{})
        sections_gen = (section for section in jobs_data.keys())
        for job_section in sections_gen:
            # No changes, no need to recalculate dependencies
            if len(self.graph.out_edges) > 0 and not dic_jobs.changes.get(job_section, None) and not dic_jobs.changes.get("EXPERIMENT", None) and not dic_jobs.changes.get("NEWJOBS", False):
                 continue
            Log.debug("Adding dependencies for {0} jobs".format(job_section))
            # If it does not have dependencies, just append it to job_list and continue
            dependencies_keys = jobs_data.get(job_section,{}).get(option,None)
            # call function if dependencies_key is not None
            dependencies = JobList._manage_dependencies(dependencies_keys, dic_jobs) if dependencies_keys else {}
            jobs_gen = (job for job in dic_jobs.get_jobs(job_section))
            for job in jobs_gen:
                self.graph.remove_edges_from(self.graph.nodes(job.name))
                if job.name not in self.graph.nodes:
                    self.graph.add_node(job.name,job=job)
                elif job.name in self.graph.nodes and self.graph.nodes.get(job.name).get("job",None) is None: # Old versions of autosubmit needs re-adding the job to the graph
                    self.graph.nodes.get(job.name)["job"] = job
                if dependencies:
                    job = self.graph.nodes.get(job.name)['job']
                    self._manage_job_dependencies(dic_jobs, job, date_list, member_list, chunk_list, dependencies_keys,
                                                     dependencies, self.graph)

    @staticmethod
    def _manage_dependencies(dependencies_keys, dic_jobs):
        parameters = dic_jobs.experiment_data["JOBS"]
        dependencies = dict()
        keys_to_erase = []
        for key in dependencies_keys:
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
            if parameters.get(section,None):
                dependency_running_type = str(parameters[section].get('RUNNING', 'once')).lower()
                delay = int(parameters[section].get('DELAY', -1))
                dependency = Dependency(section, distance, dependency_running_type, sign, delay, splits,relationships=dependencies_keys[key])
                dependencies[key] = dependency
            else:
                keys_to_erase.append(key)
        for key in keys_to_erase:
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
    def _apply_filter_1_to_1_splits(parent_value, filter_value, associative_list, child=None, parent=None):
        """
        Check if the current_job_value is included in the filter_value
        :param parent_value:
        :param filter_value: filter
        :param associative_list: dates, members, chunks, splits.
        :param filter_type: dates, members, chunks, splits .
        :return:
        """
        lesser_group = None
        lesser_value = "parent"
        greater = "-1"
        if "NONE".casefold() in str(parent_value).casefold():
            return False
        if parent and child:
            if not parent.splits:
                parent_splits = -1
            else:
                parent_splits = int(parent.splits)
            if not child.splits:
                child_splits = -1
            else:
                child_splits = int(child.splits)
            if parent_splits == child_splits:
                greater = str(child_splits)
            else:
                if parent_splits > child_splits:
                    lesser = str(child_splits)
                    greater = str(parent_splits)
                    lesser_value = "child"
                else:
                    lesser = str(parent_splits)
                    greater = str(child_splits)
                to_look_at_lesser = [associative_list[i:i + 1] for i in range(0, int(lesser), 1)]
                for lesser_group in range(len(to_look_at_lesser)):
                    if lesser_value == "parent":
                        if str(parent_value) in to_look_at_lesser[lesser_group]:
                            break
                    else:
                        if str(child.split) in to_look_at_lesser[lesser_group]:
                            break
        if "?" in filter_value:
            # replace all ? for ""
            filter_value = filter_value.replace("?", "")
        if "*" in filter_value:
            aux_filter = filter_value
            filter_value = ""
            for filter_ in aux_filter.split(","):
                if "*" in filter_:
                    filter_, split_info = filter_.split("*")
                    # If parent and children has the same amount of splits \\ doesn't make sense so it is disabled
                    if "\\" in split_info:
                        split_info = int(split_info.split("\\")[-1])
                    else:
                        split_info = 1
                    # split_info: if a value is 1, it means that the filter is 1-to-1, if it is 2, it means that the filter is 1-to-2, etc.
                    if child and parent:
                        if split_info == 1 :
                            if child.split == parent_value:
                                return True
                        elif split_info > 1:
                            # 1-to-X filter
                            to_look_at_greater = [associative_list[i:i + split_info] for i in
                                                  range(0, int(greater), split_info)]
                            if not lesser_group:
                                if str(child.split) in associative_list:
                                    return True
                            else:
                                if lesser_value == "parent":
                                    if child.split in to_look_at_greater[lesser_group]:
                                        return True
                                else:
                                    if parent_value in to_look_at_greater[lesser_group]:
                                        return True
                    else:
                        filter_value += filter_ + ","
                else:
                    filter_value += filter_ + ","
            filter_value = filter_value[:-1]
        to_filter = JobList._parse_filters_to_check(filter_value, associative_list, "splits")
        if to_filter is None:
            return False
        elif not to_filter or len(to_filter) == 0 or ( len(to_filter) == 1 and not to_filter[0] ):
            return False
        elif "ALL".casefold() == str(to_filter[0]).casefold():
            return True
        elif "NATURAL".casefold() == str(to_filter[0]).casefold():
            if parent_value is None or parent_value in associative_list:
                return True
        elif "NONE".casefold() == str(to_filter[0]).casefold():
            return False
        elif len([filter_ for filter_ in to_filter if
                  str(parent_value).strip(" ").casefold() == str(filter_).strip(" ").casefold()]) > 0:
            return True
        else:
            return False


    @staticmethod
    def _parse_filters_to_check(list_of_values_to_check,value_list=[],level_to_check="DATES_FROM"):
        final_values = []
        list_of_values_to_check = str(list_of_values_to_check).upper()
        if list_of_values_to_check is None:
            return None
        elif list_of_values_to_check.casefold() == "ALL".casefold() :
            return ["ALL"]
        elif list_of_values_to_check.casefold() == "NONE".casefold():
            return ["NONE"]
        elif list_of_values_to_check.casefold() == "NATURAL".casefold():
            return ["NATURAL"]
        elif "," in list_of_values_to_check:
            for value_to_check in list_of_values_to_check.split(","):
                final_values.extend(JobList._parse_filter_to_check(value_to_check,value_list,level_to_check))
        else:
            final_values = JobList._parse_filter_to_check(list_of_values_to_check,value_list,level_to_check)
        return final_values


    @staticmethod
    def _parse_filter_to_check(value_to_check,value_list=[],level_to_check="DATES_FROM"):
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
        if value_to_check.count(":") == 1:
            # range
            if value_to_check[1] == ":":
                # [:N]
                # Find N index in the list
                start = None
                end = value_to_check.split(":")[1].strip("[]")
                if level_to_check in ["CHUNKS_FROM","SPLITS_FROM"]:
                    end = int(end)
            elif value_to_check[-2] == ":":
                # [N:]
                # Find N index in the list
                start = value_to_check.split(":")[0].strip("[]")
                if level_to_check in ["CHUNKS_FROM","SPLITS_FROM"]:
                    start = int(start)
                end = None
            else:
                # [N:M]
                # Find N index in the list
                start = value_to_check.split(":")[0].strip("[]")
                end = value_to_check.split(":")[1].strip("[]")
                step = 1
                if level_to_check in ["CHUNKS_FROM","SPLITS_FROM"]:
                    start = int(start)
                    end = int(end)
        elif value_to_check.count(":") == 2:
            # range with step
            if value_to_check[-2] == ":" and value_to_check[-3] == ":":  # [N::]
                # Find N index in the list
                start = value_to_check.split(":")[0].strip("[]")
                end = None
                step = 1
                if level_to_check in ["CHUNKS_FROM","SPLITS_FROM"]:
                    start = int(start)
            elif value_to_check[1] == ":" and value_to_check[2] == ":":  # [::S]
                # Find N index in the list
                start = None
                end = None
                step = value_to_check.split(":")[-1].strip("[]")
                # get index in the value_list
                step = int(step)
            elif value_to_check[1] == ":" and value_to_check[-2] == ":": # [:M:]
                # Find N index in the list
                start = None
                end = value_to_check.split(":")[1].strip("[]")
                if level_to_check in ["CHUNKS_FROM","SPLITS_FROM"]:
                    end = int(end)
                step = 1
            else: # [N:M:S]
                # Find N index in the list
                start = value_to_check.split(":")[0].strip("[]")
                end = value_to_check.split(":")[1].strip("[]")
                step = value_to_check.split(":")[2].strip("[]")
                step = int(step)
                if level_to_check in ["CHUNKS_FROM","SPLITS_FROM"]:
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
                return value_list[slice(value_list.index(start), value_list.index(end)+1, int(step))]
            except ValueError:
                return value_list[slice(0,len(value_list)-1,int(step))]
        else:
            if not start:
                start = 0
            if end is None:
                return []
            return [number_gen for number_gen in range(int(start), int(end)+1, int(step))]

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
            if type(value_to_check) != str:
                value_to_check = date2str(value_to_check, "%Y%m%d")  # need to convert in some cases
            try:
                values_list = [date2str(date_, "%Y%m%d") for date_ in self._date_list]  # need to convert in some cases
            except:
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
            selected_filter = JobList._parse_filters_to_check(filter_range, values_list, level_to_check)
            if filter_range.casefold() in ["ALL".casefold(),"NATURAL".casefold(),"NONE".casefold()] or not value_to_check:
                included = True
            else:
                included = False
                for value in selected_filter:
                    if str(value).strip(" ").casefold() == str(value_to_check).strip(" ").casefold():
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


    def _check_dates(self, relationships, current_job):
        """
        Check if the current_job_value is included in the filter_from and retrieve filter_to value
        :param relationships: Remaining filters to apply.
        :param current_job: Current job to check.
        :return:  filters_to_apply
        """
        # Check the test_dependencies.py to see how to use this function
        filters_to_apply = self._check_relationship(relationships, "DATES_FROM", date2str(current_job.date))
        for i, filter in enumerate(filters_to_apply):
            if "MEMBERS_FROM" in filter:
                filters_to_apply_m = self._check_members({"MEMBERS_FROM": (filter.pop("MEMBERS_FROM"))}, current_job)
                if len(filters_to_apply_m) > 0:
                    filters_to_apply[i].update(filters_to_apply_m)
            # Will enter chunks_from, and obtain [{DATES_TO: "20020201", MEMBERS_TO: "fc2", CHUNKS_TO: "ALL", SPLITS_TO: "2"]
            if "CHUNKS_FROM" in filter:
                filters_to_apply_c = self._check_chunks({"CHUNKS_FROM": (filter.pop("CHUNKS_FROM"))}, current_job)
                if len(filters_to_apply_c) > 0 and ( type(filters_to_apply_c) != list or ( type(filters_to_apply_c) == list and len(filters_to_apply_c[0]) > 0 ) ):
                    filters_to_apply[i].update(filters_to_apply_c)
            # IGNORED
            if "SPLITS_FROM" in filter:
                filters_to_apply_s = self._check_splits({"SPLITS_FROM": (filter.pop("SPLITS_FROM"))}, current_job)
                if len(filters_to_apply_s) > 0:
                    filters_to_apply[i].update(filters_to_apply_s)
        # Unify filters from all filters_from where the current job is included to have a single SET of filters_to
        filters_to_apply = self._unify_to_filters(filters_to_apply)
        # {DATES_TO: "20020201", MEMBERS_TO: "fc2", CHUNKS_TO: "ALL", SPLITS_TO: "2"}
        return filters_to_apply


    def _check_members(self,relationships, current_job):
        """
        Check if the current_job_value is included in the filter_from and retrieve filter_to value
        :param relationships: Remaining filters to apply.
        :param current_job: Current job to check.
        :return: filters_to_apply
        """
        filters_to_apply = self._check_relationship(relationships, "MEMBERS_FROM", current_job.member)
        for i, filter_ in enumerate(filters_to_apply):
            if "CHUNKS_FROM" in filter_:
                filters_to_apply_c = self._check_chunks({"CHUNKS_FROM": (filter_.pop("CHUNKS_FROM"))}, current_job)
                if len(filters_to_apply_c) > 0:
                    filters_to_apply[i].update(filters_to_apply_c)
            if "SPLITS_FROM" in filter_:
                filters_to_apply_s = self._check_splits({"SPLITS_FROM": (filter_.pop("SPLITS_FROM"))}, current_job)
                if len(filters_to_apply_s) > 0:
                    filters_to_apply[i].update(filters_to_apply_s)
        filters_to_apply = self._unify_to_filters(filters_to_apply)
        return filters_to_apply

    def _check_chunks(self,relationships, current_job):
        """
        Check if the current_job_value is included in the filter_from and retrieve filter_to value
        :param relationships: Remaining filters to apply.
        :param current_job: Current job to check.
        :return: filters_to_apply
        """

        filters_to_apply = self._check_relationship(relationships, "CHUNKS_FROM", current_job.chunk)
        for i, filter in enumerate(filters_to_apply):
            if "SPLITS_FROM" in filter:
                filters_to_apply_s = self._check_splits({"SPLITS_FROM": (filter.pop("SPLITS_FROM"))}, current_job)
                if len(filters_to_apply_s) > 0:
                    filters_to_apply[i].update(filters_to_apply_s)
        filters_to_apply = self._unify_to_filters(filters_to_apply)
        return filters_to_apply

    def _check_splits(self,relationships, current_job):
        """
        Check if the current_job_value is included in the filter_from and retrieve filter_to value
        :param relationships: Remaining filters to apply.
        :param current_job: Current job to check.
        :return: filters_to_apply
        """

        filters_to_apply = self._check_relationship(relationships, "SPLITS_FROM", current_job.split)
        # No more FROM sections to check, unify _to FILTERS and return
        filters_to_apply = self._unify_to_filters(filters_to_apply)
        return filters_to_apply

    def _unify_to_filter(self,unified_filter, filter_to, filter_type):
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
            aux = filter_to.pop(filter_type, None)
            if aux:
                aux = aux.split(",")
                for element in aux:
                    if element == "":
                        continue
                    # Get only the first alphanumeric part and [:] chars
                    parsed_element = re.findall(r"([\[:\]a-zA-Z0-9]+)", element)[0].lower()
                    extra_data = element[len(parsed_element):]
                    parsed_element = JobList._parse_filter_to_check(parsed_element, value_list = value_list, level_to_check = filter_type)
                    # convert list to str
                    skip = False
                    if isinstance(parsed_element, list):
                        # check if any element is natural or none
                        for ele in parsed_element:
                            if type(ele) is str and ele.lower() in ["natural", "none"]:
                                skip = True
                    else:
                        if type(parsed_element) is str and parsed_element.lower() in ["natural", "none"]:
                            skip = True
                    if skip and len(unified_filter[filter_type]) > 0:
                        continue
                    else:
                        for ele in parsed_element:
                            if extra_data:
                                check_whole_string = str(ele)+extra_data+","
                            else:
                                check_whole_string = str(ele)+","
                            if str(check_whole_string) not in unified_filter[filter_type]:
                                unified_filter[filter_type] += check_whole_string
        return unified_filter

    @staticmethod
    def _normalize_to_filters(filter_to, filter_type):
        """
        Normalize filter_to filters to a single string or "all"
        :param filter_to: Unified filter_to dictionary
        :param filter_type: "DATES_TO", "MEMBERS_TO", "CHUNKS_TO", "SPLITS_TO"
        :return:
        """
        if len(filter_to[filter_type]) == 0 or ("," in filter_to[filter_type] and len(filter_to[filter_type]) == 1):
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

    def _unify_to_filters(self,filter_to_apply):
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
                self._unify_to_filter(unified_filter, filter_to, "SPLITS_TO")

        JobList._normalize_to_filters(unified_filter, "DATES_TO")
        JobList._normalize_to_filters(unified_filter, "MEMBERS_TO")
        JobList._normalize_to_filters(unified_filter, "CHUNKS_TO")
        JobList._normalize_to_filters(unified_filter, "SPLITS_TO")
        return unified_filter

    def _filter_current_job(self,current_job, relationships):
        '''
        This function will filter the current job based on the relationships given
        :param current_job: Current job to filter
        :param relationships: Relationships to apply
        :return: dict() with the filters to apply, or empty dict() if no filters to apply
        '''

        # This function will look if the given relationship is set for the given job DATEs,MEMBER,CHUNK,SPLIT ( _from filters )
        # And if it is, it will return the dependencies that need to be activated (_TO filters)
        # _FROM behavior:
        # DATES_FROM can contain MEMBERS_FROM,CHUNKS_FROM,SPLITS_FROM
        # MEMBERS_FROM can contain CHUNKS_FROM,SPLITS_FROM
        # CHUNKS_FROM can contain SPLITS_FROM
        # SPLITS_FROM can contain nothing
        # _TO behavior:
        # TO keywords, can be in any of the _FROM filters and they will only affect the _FROM filter they are in.
        # There are 4 keywords:
        # 1. ALL: all the dependencies will be activated of the given filter type (dates, members, chunks or/and splits)
        # 2. NONE: no dependencies will be activated of the given filter type (dates, members, chunks or/and splits)
        # 3. NATURAL: this is the normal behavior, represents a way of letting the job to be activated if they would normally be activated.
        # 4. ? : this is a weak dependency activation flag, The dependency will be activated but the job can fail without affecting the workflow.

        filters_to_apply = {}
        # Check if filter_from-filter_to relationship is set
        if relationships is not None and len(relationships) > 0:
            # Look for a starting point, this can be if else becasue they're exclusive as a DATE_FROM can't be in a MEMBER_FROM and so on
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

    def _add_edge_info(self, job, special_status):
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

    def add_special_conditions(self, job, special_conditions, only_marked_status, filters_to_apply, parent):
        """
        Add special conditions to the job edge
        :param job: Job
        :param special_conditions: dict
        :param only_marked_status: bool
        :param filters_to_apply: dict
        :param parent: parent job
        :return:
        """
        if special_conditions.get("STATUS", None):
            if only_marked_status:
                if str(job.split) + "?" in filters_to_apply.get("SPLITS_TO", "") or str(
                        job.chunk) + "?" in filters_to_apply.get("CHUNKS_TO", "") or str(
                    job.member) + "?" in filters_to_apply.get("MEMBERS_TO", "") or str(
                    job.date) + "?" in filters_to_apply.get("DATES_TO", ""):
                    selected = True
                else:
                    selected = False
            else:
                selected = True
            if selected:
                if special_conditions.get("FROM_STEP", None):
                    job.max_checkpoint_step = int(special_conditions.get("FROM_STEP", 0)) if int(special_conditions.get("FROM_STEP",0)) > job.max_checkpoint_step else job.max_checkpoint_step
                self._add_edge_info(job, special_conditions["STATUS"])  # job_list map
                job.add_edge_info(parent, special_conditions) # this job

    def _calculate_special_dependencies(self, parent, dependencies_keys_without_special_chars):
        depends_on_previous_non_current_section = [aux_section for aux_section in self.depends_on_previous_chunk.items()
                                                   if aux_section[0] != parent.section]
        if len(depends_on_previous_non_current_section) > 0:
            depends_on_previous_non_current_section_aux = copy.copy(depends_on_previous_non_current_section)
            for aux_section in depends_on_previous_non_current_section_aux:
                if aux_section[0] not in dependencies_keys_without_special_chars:
                    depends_on_previous_non_current_section.remove(aux_section)
        return depends_on_previous_non_current_section
    def _manage_job_dependencies(self, dic_jobs, job, date_list, member_list, chunk_list, dependencies_keys,
                                 dependencies,
                                 graph):
        '''
        Manage the dependencies of a job
        :param dic_jobs:
        :param job:
        :param date_list:
        :param member_list:
        :param chunk_list:
        :param dependencies_keys:
        :param dependencies:
        :param graph:
        :return:
        '''
        self.depends_on_previous_special_section = dict()
        if not job.splits:
            child_splits = 0
        else:
            child_splits = int(job.splits)
        parsed_date_list = []
        for dat in date_list:
            parsed_date_list.append(date2str(dat))
        special_conditions = dict()

        dependencies_to_del = set()
        dependencies_non_natural_to_del = set()

        # It is faster to check the conf instead of  calculate 90000000 tasks
        # Prune number of dependencies to check, to reduce the transitive reduction complexity
        dependencies_keys_aux = [key for key in dependencies_keys if key in dependencies]
        dependencies_keys_without_special_chars = []
        for key_aux_stripped in dependencies_keys_aux:
            if "-" in key_aux_stripped:
                key_aux_stripped = key_aux_stripped.split("-")[0]
            elif "+" in key_aux_stripped:
                key_aux_stripped = key_aux_stripped.split("+")[0]
            dependencies_keys_without_special_chars.append(key_aux_stripped)
        # If parent already has defined that dependency, skip it to reduce the transitive reduction complexity
        actual_job_depends_on_previous_chunk = False
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
            if job.chunk and int(job.chunk) > 1 and job.split <= 0:
                if job.section == aux_key:
                    actual_job_depends_on_previous_chunk = True
                    if job.chunk > self.depends_on_previous_chunk.get(aux_key,-1):
                        self.depends_on_previous_chunk[aux_key] = job.chunk
                elif distance != 0:
                    actual_job_depends_on_previous_chunk = True
                    if job.chunk > self.depends_on_previous_chunk.get(aux_key, -1):
                        self.depends_on_previous_chunk[aux_key] = job.chunk

            dependencies_of_that_section = dic_jobs.as_conf.jobs_data[aux_key].get("DEPENDENCIES",{})
            if job.section not in dependencies_keys_without_special_chars:
                stripped_dependencies_of_that_section = dict()
                for key in dependencies_of_that_section.keys():
                    if "-" in key:
                        stripped_key = key.split("-")[0]
                    elif "+" in key:
                        stripped_key = key.split("+")[0]
                    else:
                        stripped_key = key
                    if stripped_key in dependencies_keys_without_special_chars:
                        if not dependencies_keys[dependency_key]:
                            dependencies_to_del.add(key)
                        else:
                            dependencies_non_natural_to_del.add(key)

        pass
        dependencies_keys_aux = [key for key in dependencies_keys_aux if key not in dependencies_to_del]
        # parse self first
        if job.section in dependencies_keys_aux:
            dependencies_keys_aux.remove(job.section)
            dependencies_keys_aux = [job.section] + dependencies_keys_aux

        for key in dependencies_keys_aux:
            dependency = dependencies[key]
            skip, (chunk, member, date) = JobList._calculate_dependency_metadata(job.chunk, chunk_list,
                                                                                 job.member, member_list,
                                                                                 job.date, date_list,
                                                                                 dependency)
            if skip:
                continue
            filters_to_apply = self._filter_current_job(job, copy.deepcopy(dependency.relationships))
            special_conditions["STATUS"] = filters_to_apply.pop("STATUS", None)
            special_conditions["FROM_STEP"] = filters_to_apply.pop("FROM_STEP", None)
            # Get dates_to, members_to, chunks_to of the deepest level of the relationship.

            if len(filters_to_apply) == 0:
                if key in dependencies_non_natural_to_del:
                    continue
                natural_parents = dic_jobs.get_jobs(dependency.section, date, member, chunk)
                # Natural jobs, no filters to apply we can safely add the edge
                for parent in natural_parents:
                    if parent.name == job.name:
                        continue
                    if parent.section != job.section:
                        if job.section in self.depends_on_previous_special_section:
                            if job.running != parent.running or ( job.running == parent.running and ( not job.chunk or job.chunk > 1) ):
                                if self.depends_on_previous_special_section[job.section].get(job.name, False):
                                    continue
                    if not actual_job_depends_on_previous_chunk:
                        if job.running == "chunk" or parent.chunk == self.depends_on_previous_chunk.get(parent.section, parent.chunk):
                            graph.add_edge(parent.name, job.name)
                    else:
                        if parent.section == job.section:
                            depends_on_previous_non_current_section = self._calculate_special_dependencies(job,dependencies_keys_without_special_chars)
                            if not depends_on_previous_non_current_section:
                                graph.add_edge(parent.name, job.name)
                            else:
                                for a_parent_section in depends_on_previous_non_current_section:
                                    if parent.chunk == a_parent_section[1]:
                                        graph.add_edge(parent.name, job.name)
                                        break
                        elif (job.running == "chunk" and parent.running == "chunk"):
                            graph.add_edge(parent.name, job.name)
                JobList.handle_frequency_interval_dependencies(chunk, chunk_list, date, date_list, dic_jobs, job,
                                                               member,
                                                               member_list, dependency.section, natural_parents)
            else:
                all_none = True
                for filter_value in filters_to_apply.values():
                    if str(filter_value).lower() != "none":
                        all_none = False
                        break
                if all_none:
                    continue
                any_all_filter = False
                for filter_value in filters_to_apply.values():
                    if str(filter_value).lower() == "all":
                        any_all_filter = True
                        break
                if any_all_filter:
                    if actual_job_depends_on_previous_chunk:
                        continue
                possible_parents =  dic_jobs.get_jobs_filtered(dependency.section,job,filters_to_apply,date,member,chunk)
                if "?" in filters_to_apply.get("SPLITS_TO", "") or "?" in filters_to_apply.get("DATES_TO",
                                                                                               "") or "?" in filters_to_apply.get(
                        "MEMBERS_TO", "") or "?" in filters_to_apply.get("CHUNKS_TO", ""):
                    only_marked_status = True
                else:
                    only_marked_status = False
                for parent in possible_parents:
                    if parent.name == job.name:
                        continue
                    if any_all_filter:
                        if parent.chunk and parent.chunk != self.depends_on_previous_chunk.get(parent.section,parent.chunk):
                            continue
                        elif parent.section != job.section :
                            depends_on_previous_non_current_section = self._calculate_special_dependencies(job,dependencies_keys_without_special_chars)
                            skip = True
                            if job.section in self.depends_on_previous_special_section:
                                skip = self.depends_on_previous_special_section[job.section].get(job.name,False)
                            else:
                                for a_parent_section in depends_on_previous_non_current_section:
                                    if parent.chunk == a_parent_section[1]:
                                        skip = False
                            if skip:
                                continue

                    splits_to = filters_to_apply.get("SPLITS_TO", None)
                    if splits_to:
                        if not parent.splits:
                            parent_splits = 0
                        else:
                            parent_splits = int(parent.splits)
                        splits = max(child_splits, parent_splits)
                        if splits > 0:
                            associative_list_splits = [str(split) for split in range(1, splits + 1)]
                        else:
                            associative_list_splits = None
                        if not self._apply_filter_1_to_1_splits(parent.split, splits_to, associative_list_splits, job, parent):
                            continue # if the parent is not in the filter_to, skip it
                    graph.add_edge(parent.name, job.name)
                    # Do parse checkpoint
                    self.add_special_conditions(job,special_conditions,only_marked_status,filters_to_apply,parent)
                    if job.section == key:
                        if job.section not in self.depends_on_previous_special_section:
                            self.depends_on_previous_special_section[key] = {}
                        self.depends_on_previous_special_section[key][job.name] = True
                JobList.handle_frequency_interval_dependencies(chunk, chunk_list, date, date_list, dic_jobs, job, member,
                                                               member_list, dependency.section, possible_parents)

    @staticmethod
    def _calculate_dependency_metadata(chunk, chunk_list, member, member_list, date, date_list, dependency):
        skip = False
        if dependency.sign == '-':
            if chunk is not None and len(str(chunk)) > 0 and dependency.running == 'chunk':
                chunk_index = chunk-1
                if chunk_index >= dependency.distance:
                    chunk = chunk_list[chunk_index - dependency.distance]
                else:
                    skip = True
            elif member is not None and len(str(member)) > 0 and dependency.running in ['chunk', 'member']:
                #improve this TODO
                member_index = member_list.index(member)
                if member_index >= dependency.distance:
                    member = member_list[member_index - dependency.distance]
                else:
                    skip = True
            elif date is not None and len(str(date)) > 0 and dependency.running in ['chunk', 'member', 'startdate']:
                #improve this TODO
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

            elif member is not None and len(str(member)) > 0 and dependency.running in ['chunk', 'member']:
                member_index = member_list.index(member)
                if (member_index + dependency.distance) < len(member_list):
                    member = member_list[member_index + dependency.distance]
                else:
                    skip = True
            elif date is not None and len(str(date)) > 0 and dependency.running in ['chunk', 'member', 'startdate']:
                date_index = date_list.index(date)
                if (date_index + dependency.distance) < len(date_list):
                    date = date_list[date_index - dependency.distance]
                else:
                    skip = True
        return skip, (chunk, member, date)

    @staticmethod
    def handle_frequency_interval_dependencies(chunk, chunk_list, date, date_list, dic_jobs, job, member, member_list,
                                               section_name,visited_parents):
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
        for section in (job for job in dic_jobs.experiment_data.get("JOBS",{}).keys() ):
            Log.debug("Creating {0} jobs".format(section))
            dic_jobs.read_section(section, priority, default_job_type)
            priority += 1

    def _create_sorted_dict_jobs(self, wrapper_jobs):
        """
        Creates a sorting of the jobs whose job.section is in wrapper_jobs, according to the following filters in order of importance:
        date, member, RUNNING, and chunk number; where RUNNING is defined in jobs_.yml for each section.

        If the job does not have a chunk number, the total number of chunks configured for the experiment is used.

        :param wrapper_jobs: User defined job types in autosubmit_,conf [wrapper] section to be wrapped. \n
        :type wrapper_jobs: String \n
        :return: Sorted Dictionary of List that represents the jobs included in the wrapping process. \n
        :rtype: Dictionary Key: date, Value: (Dictionary Key: Member, Value: List of jobs that belong to the date, member, and are ordered by chunk number if it is a chunk job otherwise num_chunks from JOB TYPE (section)
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
                try:
                    sections_running_type_map[section] = str(self.jobs_data[section].get("RUNNING", 'once'))
                except BaseException as e:
                    raise AutosubmitCritical("Key {0} doesn't exists.".format(section), 7014, str(e))

            # Select only relevant jobs, those belonging to the sections defined in the wrapper

        sections_to_filter = ""
        for section in sections_running_type_map:
            sections_to_filter += section

        filtered_jobs_list = [job for job in self._job_list if job.section in sections_running_type_map]

        filtered_jobs_fake_date_member, fake_original_job_map = self._create_fake_dates_members(
            filtered_jobs_list)

        for date in self._date_list:
            str_date = self._get_date(date)
            for member in self._member_list:
                # Filter list of fake jobs according to date and member, result not sorted at this point
                sorted_jobs_list = [job for job in filtered_jobs_fake_date_member if job.name.split("_")[1] == str_date and
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
                    # Test if RUNNING is different between sections, or if we have reached the last item in sorted_jobs_list
                    if (
                            previous_section_running_type is not None and previous_section_running_type != section_running_type) \
                            or index == len(sorted_jobs_list):

                        # Sorting by date, member, chunk number if it is a chunk job otherwise num_chunks from JOB TYPE (section)
                        # Important to note that the only differentiating factor would be chunk OR num_chunks
                        jobs_to_sort = sorted(jobs_to_sort, key=lambda k: (k.name.split('_')[1], (k.name.split('_')[2]),
                                                                           (int(k.name.split('_')[3])
                                                                            if len(k.name.split(
                                                                               '_')) == 5 else num_chunks + 1)))

                        # Bringing back original job if identified
                        for idx in range(0, len(jobs_to_sort)):
                            # Test if it is a fake job
                            if jobs_to_sort[idx] in fake_original_job_map:
                                fake_job = jobs_to_sort[idx]
                                # Get original
                                jobs_to_sort[idx] = fake_original_job_map[fake_job]
                        # Add to result, and reset jobs_to_sort
                        # By adding to the result at this step, only those with the same RUNNING have been added.
                        dict_jobs[date][member] += jobs_to_sort
                        jobs_to_sort = []
                    if len(sorted_jobs_list) > 1:
                        jobs_to_sort.append(job)
                        previous_job = job

        return dict_jobs

    def _create_fake_dates_members(self, filtered_jobs_list):
        """
        Using the list of jobs provided, creates clones of these jobs and modifies names conditioned on job.date, job.member values (testing None).
        The purpose is that all jobs share the same name structure.

        :param filtered_jobs_list: A list of jobs of only those that comply with certain criteria, e.g. those belonging to a user defined job type for wrapping. \n
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
        Parses a user defined Date (from [experiment] DATELIST) to return a special String representation of that Date

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
        Get the dictionary of jobs ordered according to wrapper's expression divided by date and member

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

        completed_jobs = [job for job in self._job_list if (platform is None or job.platform.name == platform.name) and
                          job.status == Status.COMPLETED]
        if wrapper:
            return [job for job in completed_jobs if job.packed is False]

        else:
            return completed_jobs

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
        else:
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
        else:
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
            submitted = [job for job in self._job_list if (platform is None or job.platform.name == platform.name) and
                         job.status == Status.SUBMITTED and job.hold == hold]
        else:
            submitted = [job for job in self._job_list if (platform is None or job.platform.name == platform.name) and
                         job.status == Status.SUBMITTED]
        if wrapper:
            return [job for job in submitted if job.packed is False]
        else:
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
        running = [job for job in self._job_list if (platform is None or job.platform.name == platform.name) and
                   job.status == Status.RUNNING]
        if wrapper:
            return [job for job in running if job.packed is False]
        else:
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
        queuing = [job for job in self._job_list if (platform is None or job.platform.name == platform.name) and
                   job.status == Status.QUEUING]
        if wrapper:
            return [job for job in queuing if job.packed is False]
        else:
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
        failed = [job for job in self._job_list if (platform is None or job.platform.name == platform.name) and
                  job.status == Status.FAILED]
        if wrapper:
            return [job for job in failed if job.packed is False]
        else:
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
        unsubmitted = [job for job in self._job_list if (platform is None or job.platform.name == platform.name) and
                       (
                                   job.status != Status.SUBMITTED and job.status != Status.QUEUING and job.status == Status.RUNNING and job.status == Status.COMPLETED)]

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
            self.jobs_to_run_first = [job for job in self.jobs_to_run_first if job.status != Status.COMPLETED]
            keep_running = False
            for job in self.jobs_to_run_first:
                running_parents = [parent for parent in job.parents if
                                   parent.status != Status.WAITING and parent.status != Status.FAILED]  # job is parent of itself
                if len(running_parents) == len(job.parents):
                    keep_running = True
            if len(self.jobs_to_run_first) > 0 and keep_running is False:
                raise AutosubmitCritical(
                    "No more jobs to run first, there were still pending jobs but they're unable to run without their parents or there are failed jobs.",
                    7014)

    def parse_jobs_by_filter(self, unparsed_jobs, two_step_start=True):
        jobs_to_run_first = list()
        select_jobs_by_name = ""  # job_name
        select_all_jobs_by_section = ""  # all
        filter_jobs_by_section = ""  # Select, chunk / member
        if "&" in unparsed_jobs:  # If there are explicit jobs add them
            jobs_to_check = unparsed_jobs.split("&")
            select_jobs_by_name = jobs_to_check[0]
            unparsed_jobs = jobs_to_check[1]
        if not ";" in unparsed_jobs:
            if '[':
                select_all_jobs_by_section = unparsed_jobs
                filter_jobs_by_section = ""
            else:
                select_all_jobs_by_section = ""
                filter_jobs_by_section = unparsed_jbos
        else:
            aux = unparsed_jobs.split(';')
            select_all_jobs_by_section = aux[0]
            filter_jobs_by_section = aux[1]
        if two_step_start:
            try:
                self.jobs_to_run_first = self.get_job_related(select_jobs_by_name=select_jobs_by_name,
                                                              select_all_jobs_by_section=select_all_jobs_by_section,
                                                              filter_jobs_by_section=filter_jobs_by_section)
            except Exception as e:
                raise AutosubmitCritical(
                    "Check the {0} format.\nFirst filter is optional ends with '&'.\nSecond filter ends with ';'.\nThird filter must contain '['. ".format(
                        unparsed_jobs))
        else:
            try:
                self.rerun_job_list = self.get_job_related(select_jobs_by_name=select_jobs_by_name,
                                                           select_all_jobs_by_section=select_all_jobs_by_section,
                                                           filter_jobs_by_section=filter_jobs_by_section,
                                                           two_step_start=two_step_start)
            except Exception as e:
                raise AutosubmitCritical(
                    "Check the {0} format.\nFirst filter is optional ends with '&'.\nSecond filter ends with ';'.\nThird filter must contain '['. ".format(
                        unparsed_jobs))

    def get_job_related(self, select_jobs_by_name="", select_all_jobs_by_section="", filter_jobs_by_section="",
                        two_step_start=True):
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
                                     re.search("(^|[^0-9a-z_])" + job.name.lower()[5:] + "([^a-z0-9_]|$)",
                                               select_jobs_by_name.lower()) is not None]
            ultimate_jobs_list.extend(jobs_by_name)
            ultimate_jobs_list.extend(jobs_by_name_no_expid)

        # Second Filter { select all }
        if select_all_jobs_by_section != "":
            all_jobs_by_section = [job for job in self._job_list if
                                   re.search("(^|[^0-9a-z_])" + job.section.upper() + "([^a-z0-9_]|$)",
                                             select_all_jobs_by_section.upper()) is not None]
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
                                     re.search("(^|[^0-9a-z_])" + job.section.upper() + "([^a-z0-9_]|$)",
                                               section_name.upper()) is not None]
                if section_dates != "":
                    jobs_date = [job for job in jobs_filtered if
                                 re.search("(^|[^0-9a-z_])" + date2str(job.date, job.date_format) + "([^a-z0-9_]|$)",
                                           section_dates.lower()) is not None or job.date is None]

                if section_chunks != "" or section_members != "":
                    jobs_final = [job for job in jobs_date if (
                                section_chunks == "" or re.search("(^|[^0-9a-z_])" + str(job.chunk) + "([^a-z0-9_]|$)",
                                                                  section_chunks) is not None) and (
                                              section_members == "" or re.search(
                                          "(^|[^0-9a-z_])" + str(job.member) + "([^a-z0-9_]|$)",
                                          section_members.lower()) is not None)]
                ultimate_jobs_list.extend(jobs_final)
        # Duplicates out
        ultimate_jobs_list = list(set(ultimate_jobs_list))
        Log.debug(
            "List of jobs filtered by TWO_STEP_START parameter:\n{0}".format([job.name for job in ultimate_jobs_list]))
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
        else:
            return ready

    def get_prepared(self, platform=None):
        """
        Returns a list of prepared jobs

        :param platform: job platform
        :type platform: HPCPlatform
        :return: prepared jobs
        :rtype: list
        """
        prepared = [job for job in self._job_list if (platform is None or job.platform.name == platform.name) and
                    job.status == Status.PREPARED]
        return prepared

    def get_delayed(self, platform=None):
        """
        Returns a list of delayed jobs

        :param platform: job platform
        :type platform: HPCPlatform
        :return: delayed jobs
        :rtype: list
        """
        delayed = [job for job in self._job_list if (platform is None or job.platform.name == platform.name) and
                   job.status == Status.DELAYED]
        return delayed

    def get_skipped(self, platform=None):
        """
        Returns a list of skipped jobs

        :param platform: job platform
        :type platform: HPCPlatform
        :return: skipped jobs
        :rtype: list
        """
        skipped = [job for job in self._job_list if (platform is None or job.platform.name == platform.name) and
                   job.status == Status.SKIPPED]
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
        waiting_jobs = [job for job in self._job_list if (platform is None or job.platform.name == platform.name) and
                        job.status == Status.WAITING]
        if wrapper:
            return [job for job in waiting_jobs if job.packed is False]
        else:
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
        return [job for job in self._job_list if (platform is None or job.platform.name == platform.name) and
                job.status == Status.HELD]

    def get_unknown(self, platform=None, wrapper=False):
        """
        Returns a list of jobs on unknown state

        :param wrapper:
        :param platform: job platform
        :type platform: HPCPlatform
        :return: unknown state jobs
        :rtype: list
        """
        submitted = [job for job in self._job_list if (platform is None or job.platform.name == platform.name) and
                     job.status == Status.UNKNOWN]
        if wrapper:
            return [job for job in submitted if job.packed is False]
        else:
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
        suspended = [job for job in self._job_list if (platform is None or job.platform.name == platform.name) and
                     job.status == Status.SUSPENDED]
        if wrapper:
            return [job for job in suspended if job.packed is False]
        else:
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
        else:
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
        else:
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
        else:
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

        active = self.get_in_queue(platform) + self.get_ready(
            platform=platform, hold=True) + self.get_ready(platform=platform, hold=False) + self.get_delayed(
            platform=platform)
        tmp = [job for job in active if job.hold and not (job.status ==
                                                          Status.SUBMITTED or job.status == Status.READY or job.status == Status.DELAYED)]
        if len(tmp) == len(active):  # IF only held jobs left without dependencies satisfied
            if len(tmp) != 0 and len(active) != 0:
                raise AutosubmitCritical(
                    "Only Held Jobs active. Exiting Autosubmit (TIP: This can happen if suspended or/and Failed jobs are found on the workflow)",
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

    def get_jobs_by_section(self, section_list):
        """
        Returns the job that its name matches parameter section
        :parameter section_list: list of sections to look for
        :type section_list: list
        :return: found job
        :rtype: job
        """
        jobs_by_section = list()
        for job in self._job_list:
            if job.section in section_list:
                jobs_by_section.append(job)
        return jobs_by_section

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

    def load(self):
        """
        Recreates a stored job list from the persistence

        :return: loaded job list object
        :rtype: JobList
        """
        Log.info("Loading JobList")
        try:
            return self._persistence.load(self._persistence_path, self._persistence_file)
        except:
            Log.printlog(
                "Autosubmit will use a backup for recover the job_list", 6010)
            return self.backup_load()
    def backup_load(self):
        """
        Recreates a stored job list from the persistence

        :return: loaded job list object
        :rtype: JobList
        """
        Log.info("Loading backup JobList")
        return self._persistence.load(self._persistence_path, self._persistence_file + "_backup")

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
                self._persistence.save(self._persistence_path,
                                       self._persistence_file, self._job_list if self.run_members is None or job_list is None else job_list,self.graph)
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
            elif job.platform and len(job.platform.queue) > 0 and str(job.platform.queue).lower() != "none":
                queue = job.platform.queue
            else:
                queue = job.queue
            platform_name = job.platform.name if job.platform else "no-platform"
            Log.status("{0:<35}{1:<15}{2:<15}{3:<20}{4:<15}", job.name, job.id, Status(
            ).VALUE_TO_KEY[job.status], platform_name, queue)
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
        :param store_change: if True, renames the update file to avoid reloading it at the next iteration
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
                    job.fail_count = 0
            now = localtime()
            output_date = strftime("%Y%m%d_%H%M", now)
            if store_change:
                move(os.path.join(self._persistence_path, self._update_file),
                     os.path.join(self._persistence_path, self._update_file +
                                  "_" + output_date))

    def get_skippable_jobs(self, jobs_in_wrapper):
        job_list_skip = [job for job in self.get_job_list() if
                         job.skippable == "true" and (job.status == Status.QUEUING or job.status ==
                                                      Status.RUNNING or job.status == Status.COMPLETED or job.status == Status.READY) and jobs_in_wrapper.find(
                             job.section) == -1]
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

    def check_special_status(self):
        """
        Check if all parents of a job have the correct status for checkpointing
        :return: jobs that fullfill the special conditions """
        jobs_to_check = []
        for status, sorted_job_list in self.jobs_edges.items():
            if status == "ALL":
                continue
            for job in sorted_job_list:
                if job.status != Status.WAITING:
                    continue
                if status in ["RUNNING", "FAILED"]:
                    # check checkpoint if any
                    if job.platform.connected:  # This will be true only when used under setstatus/run
                        job.get_checkpoint_files()
                non_completed_parents_current = 0
                completed_parents = len([parent for parent in job.parents if parent.status == Status.COMPLETED])
                for parent in job.edge_info[status].values():
                    if status in ["RUNNING", "FAILED"] and parent[1] and int(parent[1]) >= job.current_checkpoint_step:
                        continue
                    else:
                        status_str = Status.VALUE_TO_KEY[parent[0].status]
                        if Status.LOGICAL_ORDER.index(status_str) >= Status.LOGICAL_ORDER.index(status):
                            non_completed_parents_current += 1
                if ( non_completed_parents_current + completed_parents ) == len(job.parents):
                    jobs_to_check.append(job)

        return jobs_to_check


    def update_list(self, as_conf, store_change=True, fromSetStatus=False, submitter=None, first_time=False):
        # type: (AutosubmitConfig, bool, bool, object, bool) -> bool
        """
        Updates job list, resetting failed jobs and changing to READY all WAITING jobs with all parents COMPLETED

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
        write_log_status = False
        if not first_time:
            for job in self.get_failed():
                if self.jobs_data[job.section].get("RETRIALS", None) is None:
                    retrials = int(as_conf.get_retrials())
                else:
                    retrials = int(job.retrials)
                if job.fail_count < retrials:
                    job.inc_fail_count()
                    tmp = [
                        parent for parent in job.parents if parent.status == Status.COMPLETED]
                    if len(tmp) == len(job.parents):
                        if "+" == str(job.delay_retrials)[0] or "*" == str(job.delay_retrials)[0]:
                            aux_job_delay = int(job.delay_retrials[1:])
                        else:
                            aux_job_delay = int(job.delay_retrials)

                        if self.jobs_data[job.section].get("DELAY_RETRY_TIME", None) or aux_job_delay <= 0:
                            delay_retry_time = str(as_conf.get_delay_retry_time())
                        else:
                            delay_retry_time = job.retry_delay
                        if "+" in delay_retry_time:
                            retry_delay = job.fail_count * int(delay_retry_time[:-1]) + int(delay_retry_time[:-1])
                        elif "*" in delay_retry_time:
                            retry_delay = int(delay_retry_time[1:])
                            for retrial_amount in range(0, job.fail_count):
                                retry_delay += retry_delay * 10
                        else:
                            retry_delay = int(delay_retry_time)
                        if retry_delay > 0:
                            job.status = Status.DELAYED
                            job.delay_end = datetime.datetime.now() + datetime.timedelta(seconds=retry_delay)
                            Log.debug(
                                "Resetting job: {0} status to: DELAYED for retrial...".format(job.name))
                        else:
                            job.status = Status.READY
                            Log.debug(
                                "Resetting job: {0} status to: READY for retrial...".format(job.name))
                        job.id = None
                        job.packed = False
                        save = True

                    else:
                        job.status = Status.WAITING
                        save = True
                        job.packed = False
                        Log.debug(
                            "Resetting job: {0} status to: WAITING for parents completion...".format(job.name))
                else:
                    job.status = Status.FAILED
                    job.packed = False
                    save = True
        # Check checkpoint jobs, the status can be Any
        for job in self.check_special_status():
            job.status = Status.READY
            job.id = None
            job.packed = False
            job.wrapper_type = None
            save = True
            Log.debug(f"Special condition fullfilled for job {job.name}")
        # if waiting jobs has all parents completed change its State to READY
        for job in self.get_completed():
            if job.synchronize is not None and len(str(job.synchronize)) > 0:
                tmp = [parent for parent in job.parents if parent.status == Status.COMPLETED]
                if len(tmp) != len(job.parents):
                    tmp2 = [parent for parent in job.parents if
                            parent.status == Status.COMPLETED or parent.status == Status.SKIPPED or parent.status == Status.FAILED]
                    if len(tmp2) == len(job.parents):
                        for parent in job.parents:
                            if () and parent.status != Status.COMPLETED:
                                job.status = Status.WAITING
                                save = True
                                Log.debug(
                                    "Resetting sync job: {0} status to: WAITING for parents completion...".format(
                                        job.name))
                                break
                    else:
                        job.status = Status.WAITING
                        save = True
                        Log.debug(
                            "Resetting sync job: {0} status to: WAITING for parents completion...".format(
                                job.name))
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
                        parent.status == Status.COMPLETED or parent.status == Status.SKIPPED or parent.status == Status.FAILED]
                tmp3 = [parent for parent in job.parents if
                        parent.status == Status.SKIPPED or parent.status == Status.FAILED]
                failed_ones = [parent for parent in job.parents if parent.status == Status.FAILED]
                if job.parents is None or len(tmp) == len(job.parents):
                    job.status = Status.READY
                    job.hold = False
                    Log.debug(
                        "Setting job: {0} status to: READY (all parents completed)...".format(job.name))
                    if as_conf.get_remote_dependencies() == "true":
                        all_parents_completed.append(job.name)
                if job.status != Status.READY:
                    if len(tmp3) != len(job.parents):
                        if len(tmp2) == len(job.parents):
                            strong_dependencies_failure = False
                            weak_dependencies_failure = False
                            for parent in failed_ones:
                                if parent.name in job.edge_info and job.edge_info[parent.name].get('optional', False):
                                    weak_dependencies_failure = True
                                elif parent.section in job.dependencies:
                                    if parent.status not in [Status.COMPLETED, Status.SKIPPED]:
                                        strong_dependencies_failure = True
                                    break
                            if not strong_dependencies_failure and weak_dependencies_failure:
                                job.status = Status.READY
                                job.hold = False
                                Log.debug(
                                    "Setting job: {0} status to: READY (conditional jobs are completed/failed)...".format(
                                        job.name))
                                break
                            if as_conf.get_remote_dependencies() == "true":
                                all_parents_completed.append(job.name)
                    else:
                        if len(tmp3) == 1 and len(job.parents) == 1:
                            for parent in job.parents:
                                if parent.name in job.edge_info and job.edge_info[parent.name].get('optional', False):
                                    job.status = Status.READY
                                    job.hold = False
                                    Log.debug(
                                        "Setting job: {0} status to: READY (conditional jobs are completed/failed)...".format(
                                            job.name))
                                    break
            if as_conf.get_remote_dependencies() == "true":
                for job in self.get_prepared():
                    tmp = [
                        parent for parent in job.parents if parent.status == Status.COMPLETED]
                    tmp2 = [parent for parent in job.parents if
                            parent.status == Status.COMPLETED or parent.status == Status.SKIPPED or parent.status == Status.FAILED]
                    tmp3 = [parent for parent in job.parents if
                            parent.status == Status.SKIPPED or parent.status == Status.FAILED]
                    if len(tmp2) == len(job.parents) and len(tmp3) != len(job.parents):
                        job.status = Status.READY
                        job.packed = False
                        job.hold = False
                        save = True
                        Log.debug(
                            "A job in prepared status has all parent completed, job: {0} status set to: READY ...".format(
                                job.name))
                Log.debug('Updating WAITING jobs eligible for be prepared')
                # Setup job name should be a variable
                for job in self.get_waiting_remote_dependencies('slurm'):
                    if job.name not in all_parents_completed:
                        tmp = [parent for parent in job.parents if (
                                (
                                            parent.status == Status.SKIPPED or parent.status == Status.COMPLETED or parent.status == Status.QUEUING or parent.status == Status.RUNNING) and "setup" not in parent.name.lower())]
                        if len(tmp) == len(job.parents):
                            job.status = Status.PREPARED
                            job.hold = True
                            Log.debug(
                                "Setting job: {0} status to: Prepared for be held (all parents queuing, running or completed)...".format(
                                    job.name))

                Log.debug('Updating Held jobs')
                if self.job_package_map:
                    held_jobs = [job for job in self.get_held_jobs() if (
                            job.id not in list(self.job_package_map.keys()))]
                    held_jobs += [wrapper_job for wrapper_job in list(self.job_package_map.values())
                                  if wrapper_job.status == Status.HELD]
                else:
                    held_jobs = self.get_held_jobs()

                for job in held_jobs:
                    if self.job_package_map and job.id in list(self.job_package_map.keys()):  # Wrappers and inner jobs
                        hold_wrapper = False
                        for inner_job in job.job_list:
                            valid_parents = [
                                parent for parent in inner_job.parents if parent not in job.job_list]
                            tmp = [
                                parent for parent in valid_parents if parent.status == Status.COMPLETED]
                            if len(tmp) < len(valid_parents):
                                hold_wrapper = True
                        job.hold = hold_wrapper
                        if not job.hold:
                            for inner_job in job.job_list:
                                inner_job.hold = False
                            Log.debug(
                                "Setting job: {0} status to: Queuing (all parents completed)...".format(
                                    job.name))
                    else:  # Non-wrapped jobs
                        tmp = [
                            parent for parent in job.parents if parent.status == Status.COMPLETED]
                        if len(tmp) == len(job.parents):
                            job.hold = False
                            Log.debug(
                                "Setting job: {0} status to: Queuing (all parents completed)...".format(
                                    job.name))
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
                                if job.chunk < related_job.chunk and job.member == related_job.member and jobdate == date2str(
                                        related_job.date,
                                        related_job.date_format):  # Check if there is some related job with a higher chunk
                                    try:
                                        if job.status == Status.QUEUING:
                                            job.platform.send_command(job.platform.cancel_cmd + " " + str(job.id),
                                                                      ignore_log=True)
                                    except Exception as e:
                                        pass  # jobid finished already
                                    job.status = Status.SKIPPED
                                    save = True
                        elif job.running == 'member':
                            members = as_conf.get_member_list()
                            for related_job in jobs_to_skip[section]:
                                if members.index(job.member) < members.index(
                                        related_job.member) and job.chunk == related_job.chunk and jobdate == date2str(
                                    related_job.date, related_job.date_format):
                                    try:
                                        if job.status == Status.QUEUING:
                                            job.platform.send_command(job.platform.cancel_cmd + " " + str(job.id),
                                                                      ignore_log=True)
                                    except Exception as e:
                                        pass  # job_id finished already
                                    job.status = Status.SKIPPED
                                    save = True
            # save = True
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
        # update job list view as transitive_Reduction also fills job._parents and job._children if recreate is set
        self._job_list = [ job["job"] for job in self.graph.nodes().values() ]
        try:
            DbStructure.save_structure(self.graph, self.expid, self._config.STRUCTURES_DIR)
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
            if not job.check_script(as_conf, self.parameters, show_logs):
                out = False
        return out

    def save_wrappers(self, packages_to_save, failed_packages, as_conf, packages_persistence, hold=False,
                      inspect=False):
        for package in packages_to_save:
            if package.jobs[0].id not in failed_packages:
                if hasattr(package, "name"):
                    self.packages_dict[package.name] = package.jobs
                    from ..job.job import WrapperJob
                    wrapper_job = WrapperJob(package.name, package.jobs[0].id, Status.SUBMITTED, 0,
                                             package.jobs,
                                             package._wallclock, package._num_processors,
                                             package.platform, as_conf, hold)
                    self.job_package_map[package.jobs[0].id] = wrapper_job
                    if isinstance(package, JobPackageThread):
                        # Saving only when it is a real multi job package
                        packages_persistence.save(
                            package.name, package.jobs, package._expid, inspect)

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
        for job in self._job_list:
            count += 1
            if (count >= len(self._job_list) / 4 * (stage + 1)) or count == len(self._job_list):
                stage += 1
                Log.info("{} of {} checked".format(count, len(self._job_list)))

            show_logs = str(job.check_warnings).lower()
            if job.check == 'on_submission':
                Log.info(
                    'Template {0} will be checked in running time'.format(job.section))
                continue
            elif job.check == "false":
                Log.info(
                    'Template {0} will not be checked'.format(job.section))
                continue
            else:
                if job.section in self.sections_checked:
                    show_logs = "false"
            if not job.check_script(as_conf, self.parameters, show_logs):
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
                        skip, (chunk, member, date) = JobList._calculate_dependency_metadata(job.chunk,
                                                                                             self._chunk_list,
                                                                                             job.member,
                                                                                             self._member_list,
                                                                                             job.date, self._date_list,
                                                                                             dependency)
                        if skip:
                            continue
                        section_name = dependencies[key].section
                        for parent in self._dic_jobs.get_jobs(section_name, job.date, job.member, job.chunk):
                            if not monitor:
                                parent.status = Status.WAITING
                            Log.debug("Parent: " + parent.name)

    def _get_jobs_parser(self):
        jobs_parser = self._parser_factory.create_parser()
        jobs_parser.optionxform = str
        jobs_parser.load(
            os.path.join(self._config.LOCAL_ROOT_DIR, self._expid, 'conf', "jobs_" + self._expid + ".yaml"))
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
        :param existingList: External List of Jobs that will be printed, this excludes the inner list of jobs.
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
            result += "with " + (bcolors.OKGREEN if nocolor is False else '') + str(len(list(statusChange.keys()))
                                                                                    ) + " Change(s) ##" + (
                          bcolors.ENDC + bcolors.ENDC if nocolor is False else '')
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

    def __str__(self,nocolor = False,get_active=False):
        """
        Returns the string representation of the class.
        Usage print(class)

        :return: String representation.
        :rtype: String
        """
        if get_active:
            jobs = self.get_active()
        else:
            jobs = self.get_all()
        # Find root
        roots = []
        if get_active:
            for job in jobs:
                if len(job.parents) == 0 and job.status in (Status.READY, Status.RUNNING):
                    roots.append(job)
        else:
            for job in jobs:
                if len(job.parents) == 0:
                    roots.append(job)
        visited = list()
        results = [f"## String representation of Job List [{len(jobs)}] ##"]
        # root exists
        for root in roots:
            if root is not None and len(str(root)) > 0:
                results.append(self._recursion_print(root, 0, visited,nocolor=nocolor))
            else:
                results.append("Cannot find root.")
        return "\n".join(results)

    def __repr__(self):
        return self.__str__(True,True)

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
                     (bcolors.BOLD + bcolors.CODE_TO_COLOR[job.status] if nocolor is False else '') + \
                     job.name + \
                     (bcolors.ENDC + bcolors.ENDC if nocolor is False else '')
            if len(job._children) > 0:
                level += 1
                children = job._children
                total_children = len(job._children)
                # Writes children number and status if color are not being showed
                result += " ~ [" + str(total_children) + (" children] " if total_children > 1 else " child] ") + \
                          ("[" + Status.VALUE_TO_KEY[job.status] +
                           "] " if nocolor is True else "")
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
        :rtype: Dictionary(Job Object, Package), Dictionary(Package, List of Job Objects), Dictionary(String, String), Dictionary(String, String)
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
                packages = JobPackagePersistence(os.path.join(BasicConfig.LOCAL_ROOT_DIR, expid, "pkl"),
                                                 "job_packages_" + expid).load(wrapper=True)
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
    def retrieve_times(status_code, name, tmp_path, make_exception=False, job_times=None, seconds=False,
                       job_data_collection=None):
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
        :type job_times: Dictionary Key: job name, Value: 5-tuple (submit time, start time, finish time, status, detail id)
        :return: minutes the job has been queuing, minutes the job has been running, and the text that represents it  
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

                        return JobRow(job_data.job_name, int(queue_time), int(running_time), status, energy,
                                      JobList.ts_to_datetime(t_submit), JobList.ts_to_datetime(t_start),
                                      JobList.ts_to_datetime(t_finish), job_data.ncpus, job_data.run_id)

            # Using standard procedure
            if status_code in [Status.RUNNING, Status.SUBMITTED, Status.QUEUING,
                               Status.FAILED] or make_exception is True:
                # COMPLETED adds too much overhead so these values are now stored in a database and retrieved separately
                submit_time, start_time, finish_time, status = JobList._job_running_check(
                    status_code, name, tmp_path)
                if status_code in [Status.RUNNING, Status.FAILED]:
                    running_for_min = (finish_time - start_time)
                    queuing_for_min = (start_time - submit_time)
                    submit_time = mktime(submit_time.timetuple())
                    start_time = mktime(start_time.timetuple())
                    finish_time = mktime(finish_time.timetuple()) if status_code in [
                        Status.FAILED] else 0
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
                if job_times and status_code not in [Status.READY, Status.WAITING, Status.SUSPENDED]:
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
        # TOTAL_STATS last line has more than 3 items, status is different from pkl, and status is not "NA"
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
