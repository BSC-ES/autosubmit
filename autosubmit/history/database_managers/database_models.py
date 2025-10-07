#!/usr/bin/env python3

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

import collections

JobDataRow = collections.namedtuple('JobDataRow', ['id', 'counter', 'job_name', 'created', 'modified', 'submit', 'start', 'finish', 
                                                  'status', 'rowtype', 'ncpus', 'wallclock', 'qos', 'energy', 'date', 'section', 'member', 
                                                  'chunk', 'last', 'platform', 'job_id', 'extra_data', 'nnodes', 'run_id', 'MaxRSS', 'AveRSS', 
                                                  'out', 'err', 'rowstatus', 'children', 'platform_output', 'workflow_commit'])

ExperimentRunRow = collections.namedtuple('ExperimentRunRow', [
                                           'run_id', 'created', 'modified', 'start', 'finish', 'chunk_unit', 'chunk_size', 'completed', 'total', 'failed', 'queuing', 'running', 'submitted', 'suspended', 'metadata'])

ExperimentStatusRow = collections.namedtuple(
    'ExperimentStatusRow', ['exp_id', 'name', 'status', 'seconds_diff', 'modified'])

ExperimentRow = collections.namedtuple('ExperimentRow', ["id", "name", "autosubmit_version", "description"])

PragmaVersion = collections.namedtuple('PragmaVersion', ['version'])
MaxCounterRow = collections.namedtuple('MaxCounter', ['maxcounter'])

class RunningStatus:
  RUNNING = "RUNNING"
  NOT_RUNNING = "NOT RUNNING"

class RowType:
    NORMAL = 2
    # PACKED = 2

class RowStatus:
    INITIAL = 0
    COMPLETED = 1    
    PROCESSED = 2
    FAULTY = 3
    CHANGED = 4
    PENDING_PROCESS = 5

table_name_to_model = {
  "experiment" : ExperimentRow,
  "experiment_status" : ExperimentStatusRow,
  "job_data" : JobDataRow,
  "experiment_run" : ExperimentRunRow,
  "pragma_version" : PragmaVersion
}
