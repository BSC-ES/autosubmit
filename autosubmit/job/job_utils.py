# Copyright 2017-2020 Earth Sciences Department, BSC-CNS
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

import math
import os
from typing import Dict, Optional, TYPE_CHECKING

from bscearth.utils.date import date2str, chunk_end_date, chunk_start_date, subs_dates
from networkx.classes import DiGraph

from autosubmit.job.job_common import Status
from autosubmit.job.job_package_persistence import JobPackagePersistence
from autosubmit.platforms.paramiko_submitter import ParamikoSubmitter
from autosubmitconfigparser.config.basicconfig import BasicConfig
from log.log import Log, AutosubmitCritical

if TYPE_CHECKING:
    from autosubmit.job.job_list import JobList


CALENDAR_UNITSIZE_ENUM = {
    "hour": 0,
    "day": 1,
    "month": 2,
    "year": 3
}


def _get_submitter(as_conf) -> ParamikoSubmitter:
    """
    Returns the submitter corresponding to the communication defined on autosubmit's config file

    :return: submitter
    :rtype: Submitter
    """
    try:
        communications_library = as_conf.get_communications_library()
    except Exception as e:
        communications_library = 'paramiko'
    if communications_library == 'paramiko':
        return ParamikoSubmitter()
    else:
        # only paramiko is available right now.
        return ParamikoSubmitter()


def is_leap_year(year) -> bool:
    """Determine whether a year is a leap year."""
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


def calendar_unitsize_isgreater(split_unit,chunk_unit) -> bool:
    """
    Check if the split unit is greater than the chunk unit
    :param split_unit:
    :param chunk_unit:
    :return: boolean
    """
    split_unit = split_unit.lower()
    chunk_unit = chunk_unit.lower()
    try:
        return CALENDAR_UNITSIZE_ENUM[split_unit] > CALENDAR_UNITSIZE_ENUM[chunk_unit]
    except KeyError:
        raise AutosubmitCritical(f"Invalid calendar unit size")


def calendar_unitsize_getlowersize(unitsize) -> str:
    """
    Get the lower size of a calendar unit
    :return: str
    """
    unit_size = unitsize.lower()
    unit_value = CALENDAR_UNITSIZE_ENUM[unit_size]
    if unit_value == 0:
        return "hour"
    else:
        return list(CALENDAR_UNITSIZE_ENUM.keys())[unit_value - 1]


def calendar_get_month_days(date_str) -> int:
    """
    Get the number of days in a month
    :param date_str: Date in string format (YYYYMMDD)
    :return: int
    """
    year = int(date_str[0:4])
    month = int(date_str[4:6])
    if month == 2:
        if is_leap_year(year):
            return 29
        else:
            return 28
    elif month in [4, 6, 9, 11]:
        return 30
    else:
        return 31


def get_chunksize_in_hours(date_str,chunk_unit,chunk_length) -> int:

    if is_leap_year(int(date_str[0:4])):
        num_days_in_a_year = 366
    else:
        num_days_in_a_year = 365
    if chunk_unit == "year":
        chunk_size_in_hours = num_days_in_a_year * 24 * chunk_length
    elif chunk_unit == "month":
        chunk_size_in_hours = calendar_get_month_days(date_str) * 24 * chunk_length
    elif chunk_unit == "day":
        chunk_size_in_hours = 24 * chunk_length
    else:
        chunk_size_in_hours = chunk_length
    return chunk_size_in_hours


def calendar_split_size_isvalid(date_str, split_size, split_unit,
                                chunk_size_in_hours) -> bool:
    """
    Check if the split size is valid for the calendar
    :param date_str: Date in string format (YYYYMMDD)
    :param split_size: Size of the split
    :param split_unit: Unit of the split
    :param chunk_size_in_hours: chunk size in hours
    :return: bool
    """
    if is_leap_year(int(date_str[0:4])):
        num_days_in_a_year = 366
    else:
        num_days_in_a_year = 365

    if split_unit == "year":
        split_size_in_hours = num_days_in_a_year * 24 * split_size
    elif split_unit == "month":
        split_size_in_hours = calendar_get_month_days(date_str) * 24 * split_size
    elif split_unit == "day":
        split_size_in_hours = 24 * split_size
    else:
        split_size_in_hours = split_size

    if split_size_in_hours != chunk_size_in_hours:
        Log.warning(f"After calculations, the total sizes are: SplitSize*SplitUnitSize:{split_size_in_hours} hours, ChunkSize*ChunkUnitsize:{chunk_size_in_hours} hours.")
    else:
        Log.debug(f"Split size in hours: {split_size_in_hours}, Chunk size in hours: {chunk_size_in_hours}")
    return split_size_in_hours <= chunk_size_in_hours


def calendar_chunk_section(exp_data, section, date, chunk) -> int:
    """
    Calendar for chunks
    :param section:
    :param parameters:
    :return: int
    """
    #next_auto_date = date
    splits = 0
    jobs_data = exp_data.get('JOBS', {})
    split_unit = str(exp_data.get("EXPERIMENT", {}).get('SPLITSIZEUNIT', jobs_data.get(section,{}).get("SPLITSIZEUNIT", None))).lower()
    chunk_unit = str(exp_data.get("EXPERIMENT", {}).get('CHUNKSIZEUNIT', "day")).lower()
    split_policy = str(exp_data.get("EXPERIMENT", {}).get('SPLITPOLICY', jobs_data.get(section,{}).get("SPLITPOLICY", "flexible"))).lower()
    if chunk_unit == "hour":
        raise AutosubmitCritical("Chunk unit is hour, Autosubmit doesn't support lower than hour splits. Please change the chunk unit to day or higher. Or don't use calendar splits.")
    if jobs_data.get(section,{}).get("RUNNING","once") != "once":
        chunk_length = int(exp_data.get("EXPERIMENT", {}).get('CHUNKSIZE', 1))
        cal = str(exp_data.get('CALENDAR', "standard")).lower()
        chunk_start = chunk_start_date(
            date, chunk, chunk_length, chunk_unit, cal)
        chunk_end = chunk_end_date(
            chunk_start, chunk_length, chunk_unit, cal)
        run_days = subs_dates(chunk_start, chunk_end, cal)
        if split_unit == "none":
            split_unit = calendar_unitsize_getlowersize(chunk_unit)
        if calendar_unitsize_isgreater(split_unit,chunk_unit):
            raise AutosubmitCritical("Split unit is greater than chunk unit. Autosubmit doesn't support this configuration. Please change the split unit to day or lower. Or don't use calendar splits.")
        if split_unit == "hour":
            num_max_splits = run_days * 24
        elif split_unit == "month":
            num_max_splits = run_days / 12
        elif split_unit == "year":
            if not is_leap_year(chunk_start.year) or cal == "noleap":
                num_max_splits = run_days / 365
            else:
                num_max_splits = run_days / 366
        else:
            num_max_splits = run_days
        split_size = get_split_size(exp_data, section)
        chunk_size_in_hours = get_chunksize_in_hours(date2str(chunk_start),chunk_unit,chunk_length)
        if not calendar_split_size_isvalid(date2str(chunk_start), split_size, split_unit, chunk_size_in_hours):
            raise AutosubmitCritical(f"Invalid split size for the calendar. The split size is {split_size} and the unit is {split_unit}.")
        splits = num_max_splits / split_size
        if not splits.is_integer() and split_policy == "flexible":
            Log.warning(f"The number of splits:{num_max_splits}/{split_size} is not an integer. The number of splits will be rounded up due the flexible split policy.\n You can modify the SPLITPOLICY parameter in the section {section} to 'strict' to avoid this behavior.")
        elif not splits.is_integer() and split_policy == "strict":
            raise AutosubmitCritical(f"The number of splits is not an integer. Autosubmit can't continue.\nYou can modify the SPLITPOLICY parameter in the section {section} to 'flexible' to roundup the number. Or change the SPLITSIZE parameter to a value in which the division is an integer.")
        splits = math.ceil(splits)
        Log.info(f"For the section {section} with date:{date2str(chunk_start)} the number of splits is {splits}.")
    return splits


def get_split_size_unit(data, section) -> str:
    split_unit = str(data.get('JOBS',{}).get(section,{}).get('SPLITSIZEUNIT', "none")).lower()
    if split_unit == "none":
        split_unit = str(data.get('EXPERIMENT',{}).get("CHUNKSIZEUNIT", "day")).lower()
        if split_unit == "year":
            return "month"
        elif split_unit == "month":
            return "day"
        elif split_unit == "day":
            return "hour"
        else:
            return "day"
    return split_unit


def get_split_size(as_conf, section) -> int:
    job_data = as_conf.get('JOBS', {}).get(section, {})
    return int(job_data.get("SPLITSIZE", 1))


def transitive_reduction(graph) -> DiGraph:
    """

    Returns transitive reduction of a directed graph

    The transitive reduction of G = (V,E) is a graph G- = (V,E-) such that
    for all v,w in V there is an edge (v,w) in E- if and only if (v,w) is
    in E and there is no path from v to w in G with length greater than 1.

    :param graph: A directed acyclic graph (DAG)
    :type graph: NetworkX DiGraph
    :return: The transitive reduction of G
    """
    for u in graph:
        graph.nodes[u]["job"].parents = set()
        graph.nodes[u]["job"].children = set()
    for u in graph:
        graph.nodes[u]["job"].add_children([graph.nodes[v]["job"] for v in graph[u]])
    return graph

def get_job_package_code(expid: str, job_name: str) -> int:
    """
    Finds the package code and retrieves it. None if no package.

    :param job_name: String
    :param expid: Experiment ID
    :type expid: String

    :return: package code, None if not found
    :rtype: int or None
    """
    try:
        basic_conf = BasicConfig()
        basic_conf.read()
        packages_wrapper = JobPackagePersistence(os.path.join(basic_conf.LOCAL_ROOT_DIR, expid, "pkl"),"job_packages_" + expid).load(wrapper=True)
        packages_wrapper_plus = JobPackagePersistence(os.path.join(basic_conf.LOCAL_ROOT_DIR, expid, "pkl"),"job_packages_" + expid).load(wrapper=False)
        if packages_wrapper or packages_wrapper_plus:
            packages = packages_wrapper if len(packages_wrapper) > len(packages_wrapper_plus) else packages_wrapper_plus
            for exp, package_name, _job_name in packages:
                if job_name == _job_name:
                    code = int(package_name.split("_")[-3])
                    return code
    except Exception as e:
        pass
    return 0


class Dependency(object):
    """
    Class to manage the metadata related with a dependency

    """

    def __init__(self, section, distance=None, running=None, sign=None, delay=-1, splits=None, relationships=None) -> None:
        self.section = section
        self.distance = distance
        self.running = running
        self.sign = sign
        self.delay = delay
        self.splits = splits
        self.relationships = relationships


class SimpleJob(object):
    """
    A simple replacement for jobs
    """

    def __init__(self, name, tmppath, statuscode) -> None:
        self.name = name
        self._tmp_path = tmppath
        self.status = statuscode


class SubJob(object):
    """
    Class to manage package times
    """

    def __init__(self, name, package=None, queue=0, run=0, total=0, status="UNKNOWN") -> None:
        self.name = name
        self.package = package
        self.queue = queue
        self.run = run
        self.total = total
        self.status = status
        self.transit = 0
        self.parents = list()
        self.children = list()


class SubJobManager(object):
    """
    Class to manage list of SubJobs
    """

    def __init__(self, subjoblist, job_to_package=None, package_to_jobs=None, current_structure=None) -> None:
        self.subjobList = subjoblist
        # print("Number of jobs in SubManager : {}".format(len(self.subjobList)))
        self.job_to_package = job_to_package
        self.package_to_jobs = package_to_jobs
        self.current_structure = current_structure
        self.subjobindex = dict()
        self.subjobfixes = dict()
        self.process_index()
        self.process_times()

    def process_index(self) -> None:
        """
        Builds a dictionary of jobname -> SubJob object. 
        """
        for subjob in self.subjobList:
            self.subjobindex[subjob.name] = subjob

    def process_times(self) -> None:
        """
        """
        if self.job_to_package and self.package_to_jobs:
            if self.current_structure and len(list(self.current_structure.keys())) > 0:
                # Structure exists
                new_queues = dict()
                fixes_applied = dict()
                for package in self.package_to_jobs:
                    # SubJobs in Package
                    local_structure = dict()
                    # SubJob Name -> SubJob Object
                    local_index = dict()
                    subjobs_in_package = [x for x in self.subjobList if x.package ==
                                          package]
                    local_jobs_in_package = [job for job in subjobs_in_package]
                    # Build index
                    for sub in local_jobs_in_package:
                        local_index[sub.name] = sub
                    # Build structure
                    for sub_job in local_jobs_in_package:
                        # If job in current_structure, store children names in dictionary
                        # local_structure: Job Name -> Children (if present in the Job package)
                        local_structure[sub_job.name] = [v for v in self.current_structure[sub_job.name]
                                                         if v in self.package_to_jobs[
                                                             package]] if sub_job.name in self.current_structure else list()
                        # Assign children to SubJob in local_jobs_in_package
                        sub_job.children = local_structure[sub_job.name]
                        # Assign sub_job Name as a parent of each of its children
                        for child in local_structure[sub_job.name]:
                            local_index[child].parents.append(sub_job.name)

                    # Identify root as the job with no parents in the package
                    roots = [sub for sub in local_jobs_in_package if len(
                        sub.parents) == 0]

                    # While roots exists (consider pop)
                    while len(roots) > 0:
                        sub = roots.pop(0)
                        if len(sub.children) > 0:
                            for sub_children_name in sub.children:
                                if sub_children_name not in new_queues:
                                    # Add children to root to continue the sequence of fixes
                                    roots.append(
                                        local_index[sub_children_name])
                                    fix_size = max(self.subjobindex[sub.name].queue +
                                                   self.subjobindex[sub.name].run, 0)
                                    # fixes_applied.setdefault(sub_children_name, []).append(fix_size) # If we care about repetition
                                    # Retain the greater fix size
                                    if fix_size > fixes_applied.get(sub_children_name, 0):
                                        fixes_applied[sub_children_name] = fix_size
                                    fixed_queue_time = max(
                                        self.subjobindex[sub_children_name].queue - fix_size, 0)
                                    new_queues[sub_children_name] = fixed_queue_time
                                    # print(new_queues[sub_name])

                for key, value in new_queues.items():
                    self.subjobindex[key].queue = value
                    # print("{} : {}".format(key, value))
                for name in fixes_applied:
                    self.subjobfixes[name] = fixes_applied[name]

            else:
                # There is no structure
                for package in self.package_to_jobs:
                    # Filter only jobs in the current package
                    filtered = [x for x in self.subjobList if x.package ==
                                package]
                    # Order jobs by total time (queue + run)
                    filtered = sorted(
                        filtered, key=lambda x: x.total, reverse=False)
                    # Sizes of fixes
                    fixes_applied = dict()
                    if len(filtered) > 1:
                        temp_index = 0
                        filtered[0].transit = 0
                        # Reverse for
                        for i in range(len(filtered) - 1, 0, -1):
                            # Assume that the total time of the next job is always smaller than
                            # the queue time of the current job
                            # because the queue time of the current also considers the
                            # total time of the previous (next because of reversed for) job by default
                            # Confusing? It is.
                            # Assign to transit the adjusted queue time
                            filtered[i].transit = max(filtered[i].queue -
                                                      filtered[i - 1].total, 0)

                        # Positive or zero transit time
                        positive = len(
                            [job for job in filtered if job.transit >= 0])

                        if positive > 1:
                            for i in range(0, len(filtered)):
                                if filtered[i].transit >= 0:
                                    temp_index = i
                                    if i > 0:
                                        # Only consider after the first job
                                        filtered[i].queue = max(filtered[i].queue -
                                                                filtered[i - 1].total, 0)
                                        fixes_applied[filtered[i].name] = filtered[i - 1].total
                                else:
                                    filtered[i].queue = max(filtered[i].queue -
                                                            filtered[temp_index].total, 0)
                                    fixes_applied[filtered[i].name] = filtered[temp_index].total
                                # it is starting of level

                    for sub in filtered:
                        self.subjobindex[sub.name].queue = sub.queue
                        # print("{} : {}".format(sub.name, sub.queue))
                    for name in fixes_applied:
                        self.subjobfixes[name] = fixes_applied[name]

    def get_subjoblist(self) -> set[SubJob]:
        """
        Returns the list of SubJob objects with their corrected queue times
        in the case of jobs that belong to a wrapper.
        """
        return self.subjobList

    def get_collection_of_fixes_applied(self) -> Dict[str, int]:
        """
        """
        return self.subjobfixes


def cancel_jobs(job_list: "JobList", active_jobs_filter=None, target_status=Optional[str]) -> None:
    """Cancel jobs on platforms.

    It receives a list ``active_jobs_filter`` of statuses to filter jobs for their statuses,
    and the ones that pass the filter are treated as "ACTIVE".

    It also receives a target status, ``target_status``, which is used to change the jobs
    that were found (with the filter above) to this new status.

    It raises ``ValueError`` if the target status is invalid, and returns immediately if the
    filter does not find any "ACTIVE" jobs.

    It will iterate the list of active jobs, sending commands to cancel them (varies per platform).
    After the command was issued, regardless whether successful or not, it finishes by changing
    the status of the jobs.

    It finishes saving the job list, to persist the jobs with their updated target statuses.

    NOTE: For consistency, an experiment must be stopped before its jobs are cancelled.

    :param job_list: Autosubmit job list object.
    :type job_list: JobList
    :param active_jobs_filter: Filter used to identify jobs considered active.
    :type active_jobs_filter: List[str]
    :param target_status: Final status of the jobs cancelled.
    :type target_status: str|None
    """
    if not target_status or target_status not in Status.VALUE_TO_KEY.values():
        raise AutosubmitCritical(f'Cancellation target status of jobs is not valid: {target_status}')

    target_status = target_status.upper()

    if active_jobs_filter is None:
        active_jobs_filter = []

    active_jobs = [job for job in job_list.get_job_list() if job.status in active_jobs_filter]

    if not active_jobs:
        Log.info(f"No active jobs found for expid {job_list.expid}")
        return

    for job in active_jobs:
        # Cancel from the remote platform
        Log.info(f'Cancelling job {job.name} on platform {job.platform.name}')
        try:
            job.platform.send_command(f'{job.platform.cancel_cmd} {str(job.id)}', ignore_log=True)
        except Exception as e:
            Log.warning(f"Failed to cancel job {job.name} on platform {job.platform.name}: {str(e)}")

        Log.info(f"Changing status of job {job.name} to {target_status}")
        job.status = Status.KEY_TO_VALUE[target_status]

    job_list.save()
