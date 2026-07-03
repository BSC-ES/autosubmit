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

import math
from datetime import datetime
from typing import Optional, Any, TYPE_CHECKING

from bscearth.utils.date import date2str, chunk_end_date, chunk_start_date
from networkx.classes import DiGraph

from autosubmit.helpers.enums import ChunkUnit
from autosubmit.job.job_common import Status
from autosubmit.log.log import Log, AutosubmitCritical

if TYPE_CHECKING:
    from autosubmit.job.job_list import JobList

CALENDAR_UNITSIZE_ENUM = {
    "hour": 0,
    "day": 1,
    "month": 2,
    "year": 3
}

VALID_CALENDARS = ["standard", "noleap"]
SPLIT_POLICIES = ["flexible", "strict"]


def is_leap_year(year: int, cal: str = "standard") -> bool:
    """Determine whether a year is a leap year, respecting the calendar type.

    Under "noleap" calendar, leap years are not considered and the
    function always returns False. Under "standard", the Gregorian
    rules apply.

    :param year: Year to check
    :type year: int
    :param cal: Calendar type (standard, noleap)
    :type cal: str
    :return: True if the year is a leap year
    :rtype: bool
    """
    if cal == "noleap":
        return False
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


def calendar_unitsize_isgreater(split_unit: str, chunk_unit: str) -> bool:
    """Check if the split unit is greater than the chunk unit.

    :param split_unit: Unit of the split
    :type split_unit: str
    :param chunk_unit: Unit of the chunk
    :type chunk_unit: str
    :return: True if the split unit is greater than the chunk unit
    :rtype: bool
    :raises: AutosubmitCritical if any of the units are invalid
    """
    split_unit = split_unit.lower()
    chunk_unit = chunk_unit.lower()
    try:
        return CALENDAR_UNITSIZE_ENUM[split_unit] > CALENDAR_UNITSIZE_ENUM[chunk_unit]
    except KeyError:
        raise AutosubmitCritical("Invalid calendar unit size")


def calendar_unitsize_getlowersize(unitsize: str) -> str:
    """Given a calendar unit size, return the lower unit size.

    For example, if the input is "month", the output will be "day".

    :param unitsize: Unit size to get the lower unit size for
    :type unitsize: str
    :return: The lower unit size
    :rtype: str
    :raises: AutosubmitCritical if the unit size is invalid
    """
    unit_size = unitsize.lower()
    try:
        unit_value = CALENDAR_UNITSIZE_ENUM[unit_size]
    except KeyError:
        raise AutosubmitCritical("Invalid calendar unit size")
    if unit_value == 0:
        return ChunkUnit.HOUR
    else:
        return list(CALENDAR_UNITSIZE_ENUM.keys())[unit_value - 1]


def calendar_get_month_days(date_str: str, cal: str = "standard") -> int:
    """Get the number of days in a month, respecting the calendar type (leap year or not).

    :param date_str: Date in string format (YYYYMMDD)
    :type date_str: str
    :param cal: Calendar type (standard, noleap)
    :type cal: str
    :return: Number of days in the month
    :rtype: int
    """
    year = int(date_str[0:4])
    month = int(date_str[4:6])
    if month == 2:
        if is_leap_year(year, cal):
            return 29
        return 28
    elif month in [4, 6, 9, 11]:
        return 30
    else:
        return 31


def get_chunksize_in_hours(date_str: str, chunk_unit: str, chunk_length: int, cal: str = "standard") -> int:
    """Get the chunk size in hours.

    :param date_str: Date in string format (YYYYMMDD)
    :type date_str: str
    :param chunk_unit: Unit of the chunk
    :type chunk_unit: str
    :param chunk_length: Length of the chunk
    :type chunk_length: int
    :param cal: Calendar type (standard, noleap)
    :type cal: str
    :return: The chunk size in hours
    :rtype: int
    """
    if is_leap_year(int(date_str[0:4]), cal):
        num_days_in_a_year = 366
    else:
        num_days_in_a_year = 365
    if chunk_unit == ChunkUnit.YEAR:
        chunk_size_in_hours = num_days_in_a_year * 24 * chunk_length
    elif chunk_unit == ChunkUnit.MONTH:
        chunk_size_in_hours = calendar_get_month_days(date_str, cal) * 24 * chunk_length
    elif chunk_unit == ChunkUnit.DAY:
        chunk_size_in_hours = 24 * chunk_length
    else:
        chunk_size_in_hours = chunk_length
    return chunk_size_in_hours


def calendar_split_size_isvalid(date_str: str, split_size: int, split_unit: str,
                                chunk_size_in_hours: int, cal: str = "standard") -> bool:
    """Check if the split size is valid for the calendar.

    :param date_str: Date in string format (YYYYMMDD)
    :type date_str: str
    :param split_size: Size of the split
    :type split_size: int
    :param split_unit: Unit of the split
    :type split_unit: str
    :param chunk_size_in_hours: chunk size in hours
    :type chunk_size_in_hours: int
    :param cal: Calendar type (standard, noleap)
    :type cal: str
    :return: True if the split size is valid for the calendar
    :rtype: bool
    """
    if is_leap_year(int(date_str[0:4]), cal):
        num_days_in_a_year = 366
    else:
        num_days_in_a_year = 365

    if split_unit == ChunkUnit.YEAR:
        split_size_in_hours = num_days_in_a_year * 24 * split_size
    elif split_unit == ChunkUnit.MONTH:
        split_size_in_hours = calendar_get_month_days(date_str, cal) * 24 * split_size
    elif split_unit == ChunkUnit.DAY:
        split_size_in_hours = 24 * split_size
    else:
        split_size_in_hours = split_size

    if split_size_in_hours != chunk_size_in_hours:
        Log.warning(
            f"After calculations, the total sizes are: SplitSize*SplitUnitSize:{split_size_in_hours} hours, ChunkSize*ChunkUnitsize:{chunk_size_in_hours} hours.")
    else:
        Log.debug(f"Split size in hours: {split_size_in_hours}, Chunk size in hours: {chunk_size_in_hours}")
    return split_size_in_hours <= chunk_size_in_hours


def _validate_calendar_inputs(
    cal: str, chunk_unit: str, split_unit: str, split_policy: str
) -> None:
    """Validate the calendar inputs.

    :param cal: Calendar type
    :type cal: str
    :param chunk_unit: Unit of the chunk
    :type chunk_unit: str
    :param split_unit: Unit of the split
    :type split_unit: str
    :param split_policy: Policy for handling split sizes
    :type split_policy: str
    :return: None
    :raises: AutosubmitCritical if any of the inputs are invalid
    """
    if chunk_unit == ChunkUnit.HOUR:
        raise AutosubmitCritical(
            "Chunk unit is hour, Autosubmit doesn't support lower than hour splits. Please change the chunk unit to day or higher. Or don't use calendar splits."
        )
    if cal not in VALID_CALENDARS:
        raise AutosubmitCritical(
            f"Invalid calendar type {cal}. Autosubmit only supports 'standard' and 'noleap' calendars."
        )
    if chunk_unit not in CALENDAR_UNITSIZE_ENUM:
        raise AutosubmitCritical(
            f"Invalid chunk unit {chunk_unit}. Autosubmit only supports 'hour', 'day', 'month' and 'year' units."
        )
    if split_unit not in CALENDAR_UNITSIZE_ENUM:
        raise AutosubmitCritical(
            f"Invalid split unit {split_unit}. Autosubmit only supports 'hour', 'day', 'month' and 'year' units, or 'none' to automatically select the lower unit of the chunk unit."
        )
    if calendar_unitsize_isgreater(split_unit, chunk_unit):
        raise AutosubmitCritical(
            f"Split unit {split_unit} is greater than chunk unit {chunk_unit}. Autosubmit doesn't support this configuration. Please change the split unit to day or lower. Or don't use calendar splits."
        )
    if split_policy not in SPLIT_POLICIES:
        raise AutosubmitCritical(
            f"Invalid split policy {split_policy}. Autosubmit only supports 'flexible' and 'strict' split policies."
        )


def _count_units_between_dates(start_date: datetime, end_date: datetime, unit: str, cal: str) -> float:
    """Count the number of units between two dates.

    :param start_date: Start date
    :type start_date: datetime
    :param end_date: End date
    :type end_date: datetime
    :param unit: Unit to count (hour, day, month, year)
    :type unit: str
    :param cal: Calendar type (standard, noleap)
    :type cal: str
    :return: Number of units between the two dates
    :rtype: float
    :raises: AutosubmitCritical if the unit is invalid
    """
    if unit == ChunkUnit.HOUR:
        return float((end_date - start_date).days * 24)
    elif unit == ChunkUnit.DAY:
        return float((end_date - start_date).days)
    elif unit == ChunkUnit.MONTH:
        total = 0.0
        current = start_date.replace(day=1)
        while current < end_date:
            days_in_month = calendar_get_month_days(f"{current.year:04d}{current.month:02d}01", cal)
            next_year = current.year + current.month // 12
            next_month_num = (current.month % 12) + 1
            next_month = current.replace(year=next_year, month=next_month_num, day=1)

            start_day = start_date.day if current == start_date.replace(day=1) else 1
            end_day = end_date.day if end_date < next_month else days_in_month + 1
            days_covered = end_day - start_day
            if days_covered > 0:
                total += days_covered / days_in_month
            current = next_month
        return total
    elif unit == ChunkUnit.YEAR:
        total = 0.0
        current = start_date
        while current < end_date:
            next_year = current.replace(year=current.year + 1, month=1, day=1)
            year_end = min(end_date, next_year)

            if is_leap_year(current.year, cal):
                days_in_year = 366
            else:
                days_in_year = 365
            
            start_doy = 1 if current == current.replace(month=1, day=1) else (current - current.replace(month=1, day=1)).days + 1
            end_doy = (days_in_year + 1) if year_end >= next_year else (year_end - current.replace(month=1, day=1)).days + 1
            days_covered = end_doy - start_doy

            if days_covered > 0:
                total += days_covered / days_in_year
            current = next_year
        return total
    else:
        raise AutosubmitCritical(
            f"Invalid split unit {unit}. Autosubmit only supports 'hour', 'day', 'month' and 'year' units, or 'none' to automatically select the lower unit of the chunk unit."
        )


def calendar_chunk_section(exp_data: dict[str, Any], section: str, date: datetime, chunk: int) -> int:
    """Calculate the number of splits for a chunk based on the calendar configuration.

    :param exp_data: Experiment configuration dictionary
    :type exp_data: dict[str, Any]
    :param section: Job section name
    :type section: str
    :param date: Start date of the experiment
    :type date: datetime
    :param chunk: Chunk number
    :type chunk: int
    :return: Number of splits for the chunk
    :rtype: int
    :raises AutosubmitCritical: If the calendar configuration is invalid or if the split size doesn't fit in the chunk size.
    """
    jobs_data = exp_data.get("JOBS", {})

    if jobs_data.get(section, {}).get("RUNNING", "once") != "chunk":
        return 0

    chunk_unit = str(exp_data.get("EXPERIMENT", {}).get("CHUNKSIZEUNIT", "day")).lower()
    split_unit = str(
        exp_data.get("EXPERIMENT", {}).get(
            "SPLITSIZEUNIT", jobs_data.get(section, {}).get("SPLITSIZEUNIT", None)
        )
    ).lower()
    split_policy = str(
        exp_data.get("EXPERIMENT", {}).get(
            "SPLITPOLICY", jobs_data.get(section, {}).get("SPLITPOLICY", "flexible")
        )
    ).lower()
    chunk_length = int(exp_data.get("EXPERIMENT", {}).get("CHUNKSIZE", 1))
    cal = str(exp_data.get("CALENDAR", "standard")).lower()
    split_length = get_split_size(exp_data, section)

    if split_unit == "none":
        split_unit = calendar_unitsize_getlowersize(chunk_unit)
        Log.info(
            f"The split unit for the section {section} is set to 'none'. The split unit will be automatically set to {split_unit}, which is the lower unit of the chunk unit {chunk_unit}."
        )

    _validate_calendar_inputs(cal, chunk_unit, split_unit, split_policy)

    chunk_start = chunk_start_date(date, chunk, chunk_length, chunk_unit, cal)
    chunk_end = chunk_end_date(chunk_start, chunk_length, chunk_unit, cal)

    num_max_splits = _count_units_between_dates(chunk_start, chunk_end, split_unit, cal)
    chunk_size_in_hours = get_chunksize_in_hours(
        date2str(chunk_start), chunk_unit, chunk_length, cal
    )

    if not calendar_split_size_isvalid(
        date2str(chunk_start), split_length, split_unit, chunk_size_in_hours, cal
    ):
        raise AutosubmitCritical(
            f"Invalid split size for the calendar. The split size is {split_length} and the unit is {split_unit}. It doesn't fit in the chunk size of {chunk_size_in_hours} hours."
        )

    splits = num_max_splits / split_length

    if not splits.is_integer():
        if split_policy == "flexible":
            Log.warning(
                f"The number of splits:{num_max_splits}/{split_length} is not an integer. The number of splits will be rounded up due the flexible split policy.\n You can modify the SPLITPOLICY parameter in the section {section} to 'strict' to avoid this behavior."
            )
        elif split_policy == "strict":
            raise AutosubmitCritical(
                f"The number of splits is not an integer. Autosubmit can't continue.\nYou can modify the SPLITPOLICY parameter in the section {section} to 'flexible' to roundup the number. Or change the SPLITSIZE parameter to a value in which the division is an integer."
            )

    splits = math.ceil(splits)
    Log.info(
        f"For the section {section} with date:{date2str(chunk_start)} the number of splits is {splits}."
    )
    return splits


def get_split_size_unit(data: dict[str, Any], section: str) -> str:
    """Return the split size unit for a given job section.

    Checks ``JOBS.<section>.SPLITSIZEUNIT`` first. If it is set to "none"
    or is not defined, it falls back to ``EXPERIMENT.CHUNKSIZEUNIT`` and returns
    the next lower unit size. If ``EXPERIMENT.CHUNKSIZEUNIT`` is also not defined,
    it defaults to "day".

    :param data: Experiment configuration dictionary.
    :type data: dict[str, Any]
    :param section: Job section name.
    :type section: str
    :return: The resolved split size unit as a string.
    :rtype: str
    """
    split_unit = str(data.get('JOBS', {}).get(section, {}).get('SPLITSIZEUNIT', "none")).lower()
    if split_unit == "none":
        split_unit = str(data.get('EXPERIMENT', {}).get('CHUNKSIZEUNIT', "day")).lower()
        if split_unit == ChunkUnit.YEAR:
            return ChunkUnit.MONTH
        elif split_unit == ChunkUnit.MONTH:
            return ChunkUnit.DAY
        elif split_unit == ChunkUnit.DAY:
            return ChunkUnit.HOUR
        else:
            return ChunkUnit.DAY
    return split_unit


def get_split_size(as_conf: dict[str, Any], section: str) -> int:
    """Return the split size for a given job section.

    Checks ``JOBS.<section>.SPLITSIZE`` first, then falls back to
    ``EXPERIMENT.SPLITSIZE``, and finally defaults to ``1``.

    :param as_conf: Experiment configuration dictionary.
    :type as_conf: dict[str, Any]
    :param section: Job section name.
    :type section: str
    :return: The resolved split size as an integer.
    :rtype: int
    """
    job_split_size = as_conf.get('JOBS', {}).get(section, {}).get('SPLITSIZE')
    experiment_split_size = as_conf.get('EXPERIMENT', {}).get('SPLITSIZE')
    return int(job_split_size or experiment_split_size or 1)


def transitive_reduction(graph) -> DiGraph:
    """Returns transitive reduction of a directed graph

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

    def get_collection_of_fixes_applied(self) -> dict[str, int]:
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
