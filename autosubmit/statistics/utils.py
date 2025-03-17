#!/bin/env/python

""" Statistics Utils"""

import math
from datetime import datetime, timedelta
from typing import List, Union
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status

from log.log import AutosubmitCritical


def filter_by_section(jobs: List[Job], section: str) -> List[Job]:
    """
    filter_by_section from Statistics Utils
    :param jobs: List[Job]
    :param section: str
    :return: List[Job]
    """
    if section and section != "Any":
        return [job for job in jobs if job.section == section]
    return jobs


def discard_ready_and_waiting(jobs: List[Job]) -> List[Job]:
    """
    discard_ready_and_waiting from Statistics Utils
    :param jobs: List[Job]
    :return: List[Job]
    """
    if jobs and len(jobs) > 0:
        return [job for job in jobs if job.status not in [Status.READY, Status.WAITING]]
    return jobs


def filter_by_time_period(jobs: List[Job], hours_span: int) -> (
        Union)[tuple[list[Job], datetime, datetime], tuple[list[Job], None, datetime]]:
    """
    filter_by_time_period from Statistics Utils
    :param jobs: List[Job]
    :param hours_span: int
    :return: Union[tuple[list[Job], datetime, datetime], tuple[list[Job], None, datetime]
    """
    current_time = datetime.now().replace(second=0, microsecond=0)
    start_time = None
    if hours_span:
        if hours_span <= 0:
            raise AutosubmitCritical(f"{hours_span} is not a valid input for the "
                                     f"statistics filter -fp.")
        start_time = current_time - timedelta(hours=int(hours_span))
        return [job for job in jobs if job.check_started_after(start_time) or
                job.check_running_after(start_time)], start_time, current_time
    return jobs, start_time, current_time


def timedelta2hours(deltatime: timedelta) -> float:
    """
    timedelta2hours from Statistics Utils
    :param deltatime: timedelta
    :return: float
    """

    return deltatime.days * 24 + deltatime.seconds / 3600.0


def parse_number_processors(processors_str: str) -> int:
    """
    Defaults to 1 in case of error
    :param processors_str: str
    :return: int
    """
    if ':' in processors_str:
        components = processors_str.split(":")
        processors = int(sum(
            [math.ceil(float(x) / 36.0) * 36.0 for x in components]))
        return processors
    try:
        if processors_str == "":
            return 1
        processors = int(processors_str)
        return processors
    except Exception:
        return 1
