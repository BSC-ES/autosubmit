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

"""Utility code for the statistics package of Autosubmit."""

from datetime import datetime, timedelta
from math import ceil
from typing import Optional

from autosubmit.job.job import Job
from log.log import AutosubmitCritical


def filter_by_section(jobs: list[Job], section: Optional[str]) -> list[Job]:
    """Filter the list of jobs using the optional section.

    If a section is not provided, or if the section is ``"Any"``, then the
    filter is not applied, and the complete list of jobs is returned.

    Otherwise, the code will return all the jobs with ``job.section`` matching
    the section name.

    :param jobs: List of jobs.
    :param section: A job section name.
    :return: List of jobs, filtered by the optional section name (``"Any"`` section is ignored).
    """
    if section and section.lower() != "any":
        return [job for job in jobs if job.section == section]
    return jobs


def filter_by_time_period(jobs: list[Job], hours_span: Optional[int]) -> tuple[list[Job], Optional[datetime], datetime]:
    """Filter the list of jobs using the specified time period.

    The time period is used to compute the start time. It subtracts the given
    time period from the current time.

    If no time period is given, it returns a tuple with the list of jobs,
    ``None``, and the current time.

    If the time period given is less than or equal to zero, then it raises an
    ``AutosubmitCritical`` exception.

    Otherwise, it will filter the list of jobs using the specified time period,
    and return the filtered list of jobs, the start time, and the current time.

    :param jobs: List of jobs.
    :param hours_span: the amount of hours.
    :return: The list of jobs filtered by the time period.
    :raises AutosubmitCritical: If the ``hours_span`` is less than or equal to zero.
    """
    current_time = datetime.now().replace(second=0, microsecond=0)

    if hours_span is None:
        return jobs, None, current_time

    if hours_span <= 0:
        raise AutosubmitCritical(f"{hours_span} is not a valid input for the statistics filter -fp.")

    start_time = current_time - timedelta(hours=int(hours_span))

    filtered_jobs = [
        job for job in jobs
        if job.check_started_after(start_time) or job.check_running_after(start_time)
    ]

    return filtered_jobs, start_time, current_time


def timedelta2hours(delta_time: timedelta) -> float:
    """Convert a Python timedelta to a number of hours.

    It takes into account the number of days too. So if you have
    a time delta with 3 hours, and 3600 seconds, it will estimate
    that you have one whole hour from the 3600 seconds, plus 24
    hours from the number of days in the time delta.

    :param delta_time: A Python timedelta object.
    :return: The time in hours, taking into account the number of days too.
    """
    return delta_time.days * 24 + delta_time.seconds / 3600.0


def parse_number_processors(processors: str) -> int:
    """Parse the number of processors.

    If the given value does not have ``:``, then if the value is a valid digit
    in Python, we return its integer value.

    If no value is provided, we return the default of ``1``.

    Accepts values that contain colons (``:``). When such values are provided,
    the function will split the string value by ``:`` and for each value will
    compute the ceiling value of the parsed float value, divided by ``36.0``,
    and will multiply the resulting ceiling value by ``36.0``. The function
    then returns the integer value of the sum of all these floats.

    The ``36.0`` value used is the number of cores in an ECMWF supercomputer.
    If you are using this value, or planning to use it, refer to the
    :class:`autosubmit.platforms.ecplatform.EcPlatform` and to documentation such
    as `this one`_.

    .. _`this one`: https://web.archive.org/web/20231125131227/https://valcap74.blogspot.com/2017/10/how-to-compute-sbus-usage-on-ecmwf.html

    :param processors: A value representing the processors used in a task of the workflow.
    :return: The number of processors.
    """
    if not processors:
        return 1

    processors = processors.strip()

    if ':' in processors:
        components = processors.split(":")
        return int(sum([ceil(float(x) / 36.0) * 36.0 for x in components]))

    if processors.isdigit() and '0' != processors:
        return int(processors)

    return 1
