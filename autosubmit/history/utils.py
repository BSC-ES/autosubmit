#!/usr/bin/python

# Copyright 2015-2020 Earth Sciences Department, BSC-CNS

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

import os
from datetime import datetime

DATETIME_FORMAT = '%Y-%m-%d-%H:%M:%S'

def get_fields_as_comma_str(model):
  """ Get the fields of a namedtumple as a comma separated string. """
  return ",".join(model._fields)

def calculate_queue_time_in_seconds(submit_time, start_time):
  # type : (float, float) -> int
  """ Calculates queue time in seconds based on submit and start timestamps. """
  if submit_time > 0 and start_time > 0 and (start_time - submit_time) > 0:
    return int(start_time - submit_time)
  return 0

def calculate_run_time_in_seconds(start_time, finish_time):
  # type : (float, float) -> int  
  """ Calculates run time in seconds based on start and finish timestamps. """
  if finish_time > 0 and start_time > 0 and (finish_time - start_time) > 0:
    return int(finish_time - start_time)
  return 0

def get_current_datetime():
  # type : () -> str
  """ Returns the current time in format '%Y-%m-%d-%H:%M:%S' """
  return datetime.today().strftime(DATETIME_FORMAT)

def get_current_datetime_if_none(argument):
  # type : (Any) -> Union[Any, str]
  """ Returns the current time in format '%Y-%m-%d-%H:%M:%S' if the supplied argument is None, else return argument. """
  if argument is None:
    return get_current_datetime()
  else:
    return argument

def create_file_with_full_permissions(path):
  # type : (str) -> None
  """ creates a database files with full permissions """
  os.umask(0)
  os.open(path, os.O_WRONLY | os.O_CREAT, 0o777)

class SupportedStatus:
  COMPLETED = "COMPLETED"
  FAILED = "FAILED"
  QUEUING = "QUEUING"
  SUBMITTED = "SUBMITTED"
  RUNNING = "RUNNING"
  SUSPENDED = "SUSPENDED"

# if __name__ == "__main__":
#   print(get_fields_as_comma_str())