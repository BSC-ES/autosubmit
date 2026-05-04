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

from datetime import date, datetime
import math
from typing import Dict, Optional, TYPE_CHECKING

from bscearth.utils.date import date2str, chunk_end_date, chunk_start_date
from networkx.classes import DiGraph

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.job.job_common import Status
from autosubmit.job.job_package_persistence import JobPackagePersistence
from autosubmit.log.log import Log, AutosubmitCritical

if TYPE_CHECKING:
    from autosubmit.job.job_list import JobList

CALENDAR_UNITSIZE_ENUM = {
    "hour": 0,
    "day": 1,
    "month": 2,
    "year": 3
}


def is_leap_year(year) -> bool:
    """Determine whether a year is a leap year."""
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


def calendar_unitsize_isgreater(split_unit, chunk_unit) -> bool:
    """ Check if the split unit is greater than the chunk unit
    :param split_unit: The split unit (hour, day, month, year)
    :param chunk_unit: The chunk unit (day, month, year)
    :return: bool
    """
    split_unit = split_unit.lower()
    chunk_unit = chunk_unit.lower()
    try:
        return CALENDAR_UNITSIZE_ENUM[split_unit] > CALENDAR_UNITSIZE_ENUM[chunk_unit]
    except KeyError:
        raise AutosubmitCritical("Invalid calendar unit size")


def calendar_unitsize_getlowersize(unitsize) -> str:
    """ Return the next lower calendar unit
    :param unitsize: The calendar unit (hour, day, month, year)
    :type unitsize: str
    :return: str
    """
    unit_size = unitsize.lower()
    unit_value = CALENDAR_UNITSIZE_ENUM[unit_size]
    if unit_value == 0:
        return "hour"
    else:
        return list(CALENDAR_UNITSIZE_ENUM.keys())[unit_value - 1]


def calendar_get_month_days(date_str, cal: str = "standard") -> int:
    """
    Return the number of days in a month represented by date_str
    :param date_str: Date in string format (YYYYMMDD)
    :type date_str: str
    :param cal: Calendar to consider (standard, noleap)
    :type cal: str
    :return: int
    """
    year = int(date_str[0:4])
    month = int(date_str[4:6])
    if month == 2:
        if cal == "noleap":
            return 28
        else:
            return 29 if is_leap_year(year) else 28
    elif month in (4, 6, 9, 11):
        return 30
    else:
        return 31
    

def _days_in_month(year, month, cal: str = "standard") -> int:
    """ Return the number of days in a month
    :param year: Year of the month
    :type year: int
    :param month: Month for which to return days
    :type month: int
    :param cal: Calendar to consider (standard, noleap)
    :type cal: str
    :return: int
    """
    return calendar_get_month_days(f"{year:04d}{month:02d}01", cal)


def _as_date(value: date | datetime) -> date:
    """Normalize date and datetime inputs to date.
    :param value: A date or datetime object
    :type value: date or datetime
    :return: A date object
    """
    return value.date() if isinstance(value, datetime) else value


def _days_between(start_date: date, end_date: date, cal: str = "standard") -> int:
    """ Return the number of days between two dates, taking into account calendar type (leap year, noleap)
    :param start_date: Starting date
    :type start_date: date
    :param end_date: Ending date
    :type end_date: date
    :param cal: Calendar to consider (standard, noleap)
    :type cal: str
    :return: int
    """
    days = (end_date - start_date).days
    if cal != "noleap":
        return days
    leap_days = 0
    for year in range(start_date.year, end_date.year + 1):
        if is_leap_year(year):
            leap_day = date(year, 2, 29)
            if start_date <= leap_day < end_date:
                leap_days += 1
    return days - leap_days


def add_months(start_date: date, months: int, cal: str = "standard") -> date:
    """
    Add a number of months to a date, taking into account calendar type (leap year, noleap)
    :param start_date: Date object to which months will be added
    :param months: Number of months to add
    :param cal: Calendar to consider (standard, noleap)
    :return: New date object with the added months
    """
    month = start_date.month - 1 + months
    year = start_date.year + month // 12
    month = month % 12 + 1
    day = min(start_date.day, _days_in_month(year, month, cal))
    return date(year, month, day)


def add_years(start_date: date, years: int, cal: str = "standard") -> date:
    """
    Add a number of years to a date, taking into account calendar type (leap year, noleap)
    :param start_date: Date object to which years will be added
    :param years: Number of years to add
    :param cal: Calendar to consider (standard, noleap)
    :return: New date object with the added years
    """
    year = start_date.year + years
    month = start_date.month
    day = start_date.day
    if month == 2 and day == 29 and (cal == "noleap" or not is_leap_year(year)):
        day = 28
    day = min(day, _days_in_month(year, month, cal))
    return date(year, month, day)


def calendar_interval_end(start_date: date, size: int, unit: str, cal: str = "standard") -> date:
    """
    Compute the end date of an interval starting at start_date with a size and unit, taking into account calendar type (leap year, noleap)
    :param start_date: Starting date
    :param size: Size of the interval
    :param unit: Unit of the interval (hour, day, month, year)
    :param cal: Calendar to consider (standard, noleap)
    :return: End date of the interval
    """
    unit = unit.lower()
    if unit == "hour":
        return start_date
    if unit == "day":
        return start_date.fromordinal(start_date.toordinal() + size)
    if unit == "month":
        return add_months(start_date, size, cal)
    if unit == "year":
        return add_years(start_date, size, cal)
    return start_date.fromordinal(start_date.toordinal() + size)


def calendar_units_between(start_date: date, end_date: date, unit: str, cal: str = "standard") -> float:
    """
    Compute the number of units (hours, days, months, years) between
    start_date (included) and end_date (excluded) taking into account
    calendar type (leap year, noleap)
    
    :param start_date: Starting date (included)
    :param end_date: Ending date (excluded)
    :param unit: Unit to compute the number of (hour, day, month, year)
    :param cal: Calendar to consider (standard, noleap)
    :return: Number of units between start_date and end_date
    """
    start_date = _as_date(start_date)
    end_date = _as_date(end_date)
    if unit == "hour":
        return (end_date - start_date).days * 24
    
    if unit == "day":
        return float((end_date - start_date).days)
    
    if unit == "month":
        total = 0.0
        current = start_date.replace(day=1)
        while current < end_date:
            month_start = max(start_date, current)
            next_month = add_months(current, 1, cal).replace(day=1)
            month_end = min(end_date, next_month)
            # check if this is correct
            days_covered = (month_end - month_start).days
            if days_covered > 0:
                total += days_covered / _days_in_month(current.year, current.month, cal)
            current = next_month
        return total

    if unit == "year":
        total = 0.0
        current = start_date.replace(month=1, day=1)
        while current < end_date:
            year_start = max(start_date, current)
            next_year = add_years(current, 1, cal).replace(month=1, day=1)
            year_end = min(end_date, next_year)
            days_in_year = 366 if is_leap_year(current.year) and cal != "noleap" else 365
            days_covered = _days_between(year_start, year_end, cal)
            if days_covered > 0:
                total += days_covered / days_in_year
            current = next_year
        return total

    # Fallback: treat as days
    return float((end_date - start_date).days)
    

def get_chunksize_in_hours(date_str, chunk_unit, chunk_length) -> int:
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


def calendar_unit_interval_hours(start_date: date, size: int, unit: str, cal: str = "standard") -> int:
    """
    Compute the number of hours covered by advancing ``size`` ``unit``s from ``start_date``, taking into account
    calendar type (leap year, noleap)
    
    :param start_date: Starting date
    :param size: Number of units to advance
    :param unit: Unit to advance (hour, day, month, year)
    :param cal: Calendar to consider (standard, noleap)
    :return: Number of hours covered by advancing size units from start_date
    """
    unit = unit.lower()
    if unit == "hour":
        return size
    if unit == "day":
        return size * 24

    end_date = calendar_interval_end(start_date, size, unit, cal)
    return _days_between(start_date, end_date, cal) * 24


def calendar_split_size_isvalid(date_str, split_size, split_unit,
                                chunk_size_in_hours, cal) -> bool:
    """
    Check if the split size is valid for the calendar
    :param date_str: Date in string format (YYYYMMDD)
    :param split_size: Size of the split
    :param split_unit: Unit of the split
    :param chunk_size_in_hours: chunk size in hours
    :param cal: Calendar to consider
    :return: bool
    """
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
        Log.warning(
            f"After calculations, the total sizes are: SplitSize*SplitUnitSize:{split_size_in_hours} hours, ChunkSize*ChunkUnitsize:{chunk_size_in_hours} hours.")
    else:
        Log.debug(f"Split size in hours: {split_size_in_hours}, Chunk size in hours: {chunk_size_in_hours}")
    return split_size_in_hours <= chunk_size_in_hours
    """
    chunk_size_in_hours = int(chunk_size_in_hours)
    start_date = date(int(date_str[0:4]), int(date_str[4:6]), int(date_str[6:8]))
    split_size_in_hours = calendar_unit_interval_hours(start_date, split_size, split_unit, cal)

    return split_size_in_hours <= chunk_size_in_hours

def calendar_chunk_section(exp_data, section, date, chunk) -> int:
    """
    Calendar for chunks
    :param section:
    :param parameters:
    :return: int
    """
    # next_auto_date = date
    splits = 0
    jobs_data = exp_data.get('JOBS', {})
    split_unit = str(exp_data.get("EXPERIMENT", {}).get('SPLITSIZEUNIT',
                                                        jobs_data.get(section, {}).get("SPLITSIZEUNIT", None))).lower()
    chunk_unit = str(exp_data.get("EXPERIMENT", {}).get('CHUNKSIZEUNIT', "day")).lower()
    split_policy = str(exp_data.get("EXPERIMENT", {}).get('SPLITPOLICY', jobs_data.get(section, {}).get("SPLITPOLICY",
                                                                                                        "flexible"))).lower()
    if chunk_unit == "hour":
        raise AutosubmitCritical(
            "Chunk unit is hour, Autosubmit doesn't support lower than hour splits. Please change the chunk unit to day or higher. Or don't use calendar splits.")
    if jobs_data.get(section, {}).get("RUNNING", "once") == "chunk":
        chunk_length = int(exp_data.get("EXPERIMENT", {}).get('CHUNKSIZE', 1))
        cal = str(exp_data.get('CALENDAR', "standard")).lower()
        chunk_start = chunk_start_date(
            date, chunk, chunk_length, chunk_unit, cal)
        chunk_end = chunk_end_date(
            chunk_start, chunk_length, chunk_unit, cal)
        # run_days = subs_dates(chunk_start, chunk_end, cal)
        if split_unit == "none":
            split_unit = calendar_unitsize_getlowersize(chunk_unit)
        if calendar_unitsize_isgreater(split_unit, chunk_unit):
            raise AutosubmitCritical(
                "Split unit is greater than chunk unit. Autosubmit doesn't support this configuration. Please change the split unit to day or lower. Or don't use calendar splits.")
        """
        if split_unit == "hour":
            num_max_splits = run_days * 24
        elif split_unit == "month":
            num_max_splits = run_days / 30
        elif split_unit == "year":
            if not is_leap_year(chunk_start.year) or cal == "noleap":
                num_max_splits = run_days / 365
            else:
                num_max_splits = run_days / 366
        else:
            num_max_splits = run_days
        """
        num_max_splits = calendar_units_between(chunk_start, chunk_end, split_unit, cal)
        split_size = get_split_size(exp_data, section)
        chunk_size_in_hours = get_chunksize_in_hours(date2str(chunk_start), chunk_unit, chunk_length)
        if not calendar_split_size_isvalid(date2str(chunk_start), split_size, split_unit, chunk_size_in_hours, cal):
            raise AutosubmitCritical(
                f"Invalid split size for the calendar. The split size is {split_size} and the unit is {split_unit}.")
        splits = num_max_splits / split_size
        if not splits.is_integer() and split_policy == "flexible":
            Log.warning(
                f"The number of splits:{num_max_splits}/{split_size} is not an integer. The number of splits will be rounded up due the flexible split policy.\n You can modify the SPLITPOLICY parameter in the section {section} to 'strict' to avoid this behavior.")
        elif not splits.is_integer() and split_policy == "strict":
            raise AutosubmitCritical(
                f"The number of splits is not an integer. Autosubmit can't continue.\nYou can modify the SPLITPOLICY parameter in the section {section} to 'flexible' to roundup the number. Or change the SPLITSIZE parameter to a value in which the division is an integer.")
        splits = math.ceil(splits)
        Log.info(f"For the section {section} with date:{date2str(chunk_start)} the number of splits is {splits}.")
    return splits


def get_split_size_unit(data, section) -> str:
    split_unit = str(data.get('JOBS', {}).get(section, {}).get('SPLITSIZEUNIT', "none")).lower()
    if split_unit == "none":
        split_unit = str(data.get('EXPERIMENT', {}).get('CHUNKSIZEUNIT', "day")).lower()
        if split_unit == "year":
            return "month"
        elif split_unit == "month":
            return "day"
        elif split_unit == "day":
            return "hour"
        else:
            return "day"
    return split_unit


def get_split_size(as_conf: dict, section: str) -> int:
    """Return the split size for a given job section.

    Checks ``JOBS.<section>.SPLITSIZE`` first, then falls back to
    ``EXPERIMENT.SPLITSIZE``, and finally defaults to ``1``.

    :param as_conf: Experiment configuration dictionary.
    :param section: Job section name.
    :return: The resolved split size as an integer.
    """
    job_split_size = as_conf.get('JOBS', {}).get(section, {}).get('SPLITSIZE')
    experiment_split_size = as_conf.get('EXPERIMENT', {}).get('SPLITSIZE')
    return int(job_split_size or experiment_split_size or 1)


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
        packages_wrapper = JobPackagePersistence(expid).load(wrapper=True)
        packages_wrapper_plus = JobPackagePersistence(expid).load(wrapper=False)
        if packages_wrapper or packages_wrapper_plus:
            packages = packages_wrapper if len(packages_wrapper) > len(packages_wrapper_plus) else packages_wrapper_plus
            for exp, package_name, _job_name in packages:
                if job_name == _job_name:
                    code = int(package_name.split("_")[-3])
                    return code
    except Exception:
        pass
    return 0


class Dependency(object):
    """
    Class to manage the metadata related with a dependency

    """

    def __init__(self, section, distance=None, running=None, sign=None, delay=-1, splits=None,
                 relationships=None) -> None:
        self.section = section
        self.distance = distance
        self.running = running
        self.sign = sign
        self.delay = delay
        self.splits = splits
        self.relationships = relationships


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
        """Builds a dictionary of jobname -> SubJob object."""
        for subjob in self.subjobList:
            self.subjobindex[subjob.name] = subjob

    def process_times(self) -> None:
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
                                if i > 0:
                                    # Only consider after the first job
                                    filtered[i].queue = max(filtered[i].queue -
                                                            filtered[i - 1].total, 0)
                                    fixes_applied[filtered[i].name] = filtered[i - 1].total
                    for sub in filtered:
                        self.subjobindex[sub.name].queue = sub.queue
                        # print("{} : {}".format(sub.name, sub.queue))
                    for name in fixes_applied:
                        self.subjobfixes[name] = fixes_applied[name]

    def get_subjoblist(self) -> set[SubJob]:
        """Returns the list of SubJob objects with their corrected queue times
        in the case of jobs that belong to a wrapper. """
        return self.subjobList

    def get_collection_of_fixes_applied(self) -> Dict[str, int]:
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

    jobs_by_platform = {}
    for job in active_jobs:
        jobs_by_platform.setdefault(job.platform, []).append(job)

    for platform, jobs in jobs_by_platform.items():
        job_ids = [str(job.id) for job in jobs]
        Log.info(f'Cancelling jobs {", ".join(job_ids)} on platform {platform.name}')

        try:
            platform.cancel_jobs(job_ids)
        except Exception as e:
            Log.warning(f"Failed to cancel jobs {', '.join(job_ids)} on platform {platform.name}: {str(e)}")

        for job in jobs:
            Log.info(f"Changing status of job {job.name} to {target_status}")
            job.status = Status.KEY_TO_VALUE[target_status]

    job_list.save()
