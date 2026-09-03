# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
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

"""Job filters."""

import json
from collections.abc import Iterable
from typing import TYPE_CHECKING

from bscearth.utils.date import date2str

from autosubmit.job.job_common import separate_section_entries
from autosubmit.job.job_list import JobList
from autosubmit.log.log import Log
from autosubmit.utils import create_json, expand_values

if TYPE_CHECKING:
    from collections.abc import Callable

    from autosubmit.job.job import Job


__all__ = [
    "apply_job_filters",
    "filter_chunks",
    "filter_jobs_by_chunks_splits",
    "filter_sections_splits",
]


def apply_job_filters(
    job_list,  # should be type JobList, avoid circular imports
    base_job_names: set[str],
    filter_section: str | None,
    filter_chunk: str | None,
    filter_status: str | None,
    filter_list: str | None,
    filter_sections_splits_fn: "Callable",
    filter_chunks_fn: "Callable",
    status_from_str_fn: "Callable",
) -> set[str]:
    """Apply filters and return selected job names.

    All provided filters are combined using intersection (AND). Jobs must match all filters.
    :param job_list: job list object
    :param base_job_names: set of job names before filtering
    :param filter_section: section filter
    :param filter_chunk: chunk filter
    :param filter_status: status filter
    :param filter_list: list filter
    :param filter_sections_splits_fn: function to filter sections and splits
    :param filter_chunks_fn: function to filter chunks
    :param status_from_str_fn: function to convert status from string
    :return: set of selected job names
    """
    jobs_scope = job_list.get_job_list()
    selected_job_names = set(base_job_names)

    if filter_section:
        ft_entries = separate_section_entries(filter_section)
        if not (len(ft_entries) == 1 and ft_entries[0].upper() == "ANY"):
            section_filtered_jobs = filter_sections_splits_fn(ft_entries, jobs_scope)
            selected_job_names &= {
                job.name for job in jobs_scope if job in section_filtered_jobs
            }

    if filter_chunk:
        chunk_filtered_jobs = filter_chunks_fn(job_list, filter_chunk)
        selected_job_names &= {job.name for job in chunk_filtered_jobs}

    if filter_status:
        status_list = filter_status.split()
        if not (len(status_list) == 1 and status_list[0].upper() == "ANY"):
            allowed_statuses = {status_from_str_fn(s) for s in status_list}
            selected_job_names &= {
                job.name for job in jobs_scope if job.status in allowed_statuses
            }

    if filter_list:
        jobs = filter_list.split()
        if not (len(jobs) == 1 and jobs[0].upper() == "ANY"):
            selected_job_names &= {job.name for job in jobs_scope if job.name in jobs}

    return selected_job_names


def _split_match(j: "Job", split_list: Iterable[str]) -> bool:
    """Check if a job matches a split filter.

    If no split filter is provided (or ``ANY``), all jobs match.
    When specific splits are provided, only real split jobs (``splits >= 2``)
    can match.

    :param j: Job object to check.
    :param split_list: list of splits to match against.
    :return: True if the job matches the split filter, False otherwise.
    """
    if not split_list or "ANY" in split_list:
        return True

    if not j.splits or int(j.splits) < 2:
        return False

    return str(j.split) in split_list


def filter_sections_splits(
    filter_section_splits: list[str], jobs: list["Job"]
) -> list["Job"]:
    """Filter jobs by sections and splits.

    :param filter_section_splits: filter sections and splits
    :param jobs: list of jobs
    :return: list of jobs matching the filter
    """
    section_matching_jobs: list["Job"] = []
    all_splits = list(
        {
            str(job.split).upper()
            for job in jobs
            if job.splits and int(job.splits) >= 2 and job.split is not None
        }
    )

    for section in filter_section_splits:
        section_name = section.strip().split("[")[0].strip()
        section_name_upper = section_name.upper()
        has_split_selector = "[" in section and "]" in section
        job_splits_str = ""

        if has_split_selector:
            job_splits_str = section.strip().split("[")[1].strip(" ]")
            # splits: can be: [ 1:15 ] [ 1-15 ] [ 1 2 3 4 5 6 ] [ Any ]
            job_splits = expand_values(job_splits_str, all_splits)
        else:
            job_splits = set(all_splits)

        if (
            has_split_selector
            and job_splits_str.strip()
            and job_splits_str.strip().upper() != "ANY"
        ):
            if section_name_upper == "ANY":
                available_section_splits = set(all_splits)
            else:
                available_section_splits = {
                    str(job.split).upper()
                    for job in jobs
                    if job.section.upper() == section_name_upper
                    and job.splits
                    and int(job.splits) >= 2
                    and job.split is not None
                }

            missing_splits = set(job_splits) - available_section_splits
            if missing_splits:
                Log.warning(
                    f"Some jobs do not exist in section '{section_name_upper}' with the requested splits."
                )

        # Filter jobs by section and split
        if section_name_upper == "ANY":
            filtered_jobs = [j for j in jobs if _split_match(j, job_splits)]
        else:
            filtered_jobs = [
                j
                for j in jobs
                if j.section.upper() == section_name_upper
                and _split_match(j, job_splits)
            ]

        section_matching_jobs.extend(filtered_jobs)
        # Deduplicate
        section_matching_jobs = list(dict.fromkeys(section_matching_jobs))

        # All jobs matched, no need to continue
        if len(section_matching_jobs) == len(jobs):
            break

    return section_matching_jobs


def filter_chunks(
    filter_chunk_str: str, job_list: "JobList", matching_jobs: list["Job"]
) -> list["Job"]:
    """Filter jobs by exact date, member and chunk matches.

    ``ANY`` acts as a wildcard for the corresponding field. Jobs that do not
    have a value for a required field are excluded.

    :param filter_chunk_str: filter chunks
    :param job_list: JobList object
    :param matching_jobs: list of jobs to filter
    :return: list of jobs matching the filter
    """
    final_list = []
    data = json.loads(create_json(filter_chunk_str))

    dates = "sds"
    members = "ms"
    chunks = "cs"
    date = "sd"
    member = "m"

    # Precompute normalised dates, members and chunks
    normalized_jobs = []
    for job in matching_jobs:
        if job.date is None or job.member is None or job.chunk is None:
            continue
        normalized_jobs.append(
            (
                job,
                date2str(job.date).upper(),
                str(job.member).upper(),
                str(job.chunk).upper(),
            )
        )

    all_dates = [date2str(d).upper() for d in job_list._date_list]
    all_members = [str(m).upper() for m in job_list._member_list]
    all_chunks = [str(c).upper() for c in job_list._chunk_list]

    selected_dates: set[str] = set()
    selected_members: set[str] = set()

    # Prune first to reduce the amount of jobs that the chunk filter (last one) has to interate with
    # Here we want a reduced list of jobs that matches any date or member selected and remove the rest.
    for date_json in data[dates]:
        selected_dates.update(expand_values(date_json[date], all_dates))
        for member_json in date_json[members]:
            selected_members.update(expand_values(member_json[member], all_members))

    pruned_jobs = [
        job_tuple
        for job_tuple in normalized_jobs
        if job_tuple[1] in selected_dates and job_tuple[2] in selected_members
    ]

    # Now, build final list according to the structure in data
    for date_json in data[dates]:
        date_values = expand_values(date_json[date], all_dates)
        for member_json in date_json[members]:
            member_values = expand_values(member_json[member], all_members)
            for chunk_value in member_json[chunks]:
                chunk_values = expand_values(chunk_value, all_chunks)
                for job_tuple in pruned_jobs:
                    job, job_date, job_member, job_chunk = job_tuple
                    if (
                        job_date in date_values
                        and job_member in member_values
                        and job_chunk in chunk_values
                    ):
                        final_list.append(job)

    return list(set(final_list))


def filter_jobs_by_chunks_splits(
    job_list: "JobList", chunk_filters: str
) -> list["Job"]:
    """Select jobs from *job_list* according to *filter_chunks* specification.

    Expected format:
        - "[ DATE|Any [ MEMBER|Any [ CHUNKS|Any ] ... ] ... ], SECTION1|Any [SPLITS|Any], ..."

    :param job_list: Job list object.
    :param chunk_filters: The filters used in the chunks.
    :return: List of jobs matching the filter.
    """
    chunk_filters = chunk_filters.upper()
    if "," in chunk_filters:
        split_filters = chunk_filters.split(",")
        fc = split_filters[0]
        matching_jobs = filter_sections_splits(
            split_filters[1:], job_list.get_job_list()
        )
    else:
        fc = chunk_filters
        matching_jobs = job_list.get_job_list()

    final_list = filter_chunks(fc, job_list, matching_jobs)

    return list(set(final_list))
