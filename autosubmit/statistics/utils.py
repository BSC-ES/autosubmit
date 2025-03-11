#!/bin/env/python
import math
from autosubmit.job.job import Job
from datetime import datetime, timedelta
from autosubmit.job.job_common import Status
from typing import List, Tuple

from log.log import AutosubmitCritical

def filter_by_section(jobs, section):
  # type: (List[Job], str) -> List[Job]
  """ Filter jobs by provided section """
  if section and section != "Any":
    return [job for job in jobs if job.section == section]
  return jobs


def filter_by_time_period(jobs, hours_span):
  # type: (List[Job], int) -> Tuple[List[Job], datetime, datetime]
  current_time = datetime.now().replace(second=0, microsecond=0)
  start_time = None
  if hours_span:
    if hours_span <= 0:
      raise AutosubmitCritical("{} is not a valid input for the statistics filter -fp.".format(hours_span))
    start_time = current_time - timedelta(hours=int(hours_span))
    return [job for job in jobs if job.check_started_after(start_time) or job.check_running_after(start_time)], start_time, current_time
  return jobs, start_time, current_time


def timedelta2hours(deltatime):
    # type: (timedelta) -> float
    return deltatime.days * 24 + deltatime.seconds / 3600.0

def parse_number_processors(processors_str):
  """ Defaults to 1 in case of error """
  if ':' in processors_str:
    components = processors_str.split(":")
    processors = int(sum(
        [math.ceil(float(x) / 36.0) * 36.0 for x in components]))
    return processors
  else:
    try:
      if processors_str == "":
        return 1
      else:
        processors = int(processors_str)
        return processors
    except Exception as e:
      return 1
  