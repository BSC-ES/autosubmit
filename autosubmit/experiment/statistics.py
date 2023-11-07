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

import math
import datetime
from autosubmit.job.job import Job
from autosubmit.monitor.utils import FixedSizeList
from log.log import Log


def timedelta2hours(deltatime):
    return deltatime.days * 24 + deltatime.seconds / 3600.0


class ExperimentStats(object):

    def __init__(self, jobs_list, start, end):
        self._jobs_list = jobs_list
        self._start = start
        self._end = end
        # Max variables
        self._max_timedelta = 0
        self._max_time = 0
        self._max_fail = 0
        # Totals variables
        self._total_jobs_submitted = 0
        self._total_jobs_run = 0
        self._total_jobs_failed = 0
        self._total_jobs_completed = 0
        self._total_queueing_time = datetime.timedelta()
        self._cpu_consumption = datetime.timedelta()
        self._real_consumption = datetime.timedelta()
        self._expected_cpu_consumption = 0
        self._expected_real_consumption = 0
        self._threshold = 0
        # Totals arrays
        self._totals = []
        self._start_times = [datetime.timedelta()] * len(jobs_list)
        self._end_times = [datetime.timedelta()] * len(jobs_list)
        self._run = [datetime.timedelta()] * len(jobs_list)
        self._queued = [datetime.timedelta()] * len(jobs_list)
        self._failed_jobs = [0] * len(jobs_list)
        self._fail_queued = [datetime.timedelta()] * len(jobs_list)
        self._fail_run = [datetime.timedelta()] * len(jobs_list)
        # Do calculations
        self._calculate_stats()
        self._calculate_maxs()
        self._calculate_totals()
        self._format_stats()

    @property
    def totals(self):
        return self._totals

    @property
    def max_time(self):
        return self._max_time

    @property
    def max_fail(self):
        return self._max_fail

    @property
    def threshold(self):
        return self._threshold

    @property
    def start_times(self):
        return self._start_times

    @property
    def end_times(self):
        return self._end_times

    @property
    def run(self):
        return FixedSizeList(self._run, 0.0)

    @property
    def queued(self):
        return FixedSizeList(self._queued, 0.0)

    @property
    def failed_jobs(self):
        return FixedSizeList(self._failed_jobs, 0.0)

    @property
    def fail_queued(self):
        return FixedSizeList(self._fail_queued, 0.0)

    @property
    def fail_run(self):
        return FixedSizeList(self._fail_run, 0.0)

    def _estimate_requested_nodes(self,nodes,processors,tasks,processors_per_node) -> int:
        if str(nodes).isdigit():
            return int(nodes)
        elif str(tasks).isdigit():
            return math.ceil(int(processors) / int(tasks))
        elif str(processors_per_node).isdigit() and int(processors) > int(processors_per_node):
            return math.ceil(int(processors) / int(processors_per_node))
        else:
            return 1

    def _calculate_processing_elements(self,nodes,processors,tasks,processors_per_node,exclusive) -> int:
        if str(processors_per_node).isdigit():
            if str(nodes).isdigit():
                return int(nodes) * int(processors_per_node)
            else:
                estimated_nodes = self._estimate_requested_nodes(nodes,processors,tasks,processors_per_node)
                if not exclusive and estimated_nodes <= 1 and int(processors) <= int(processors_per_node):
                    return int(processors)
                else:
                    return estimated_nodes * int(processors_per_node)
        elif (str(tasks).isdigit() or str(nodes).isdigit()):
            Log.warning(f'Missing PROCESSORS_PER_NODE. Should be set if TASKS or NODES are defined. The PROCESSORS will used instead.')
        return int(processors)


    def _calculate_stats(self):
        """
        Main calculation
        """
        queued_by_id = dict()
        # Start enumeration of job objects
        for i, job in enumerate(self._jobs_list):
            last_retrials = job.get_last_retrials()
            processors = job.total_processors
            nodes = job.nodes
            tasks = job.tasks
            processors_per_node = job.processors_per_node
            processors = self._calculate_processing_elements(nodes, processors, tasks, processors_per_node, job.exclusive)
            for retrial in last_retrials:
                if Job.is_a_completed_retrial(retrial):
                    # The retrial has all necessary values and is status COMPLETED
                    # This IF block appears to be an attempt to normalize the queuing times for wrapped job.
                    # However, considering the current implementation of wrappers, it does not work.
                    if job.id not in queued_by_id:
                        self._queued[i] += retrial[1] - \
                            retrial[0]  # Queue time
                        # Job -> Queue time
                        queued_by_id[job.id] = self._queued[i]
                    else:
                        self._queued[i] += queued_by_id[job.id]
                    self._start_times[i] = retrial[1]
                    self._end_times[i] = retrial[2]
                    # RUN time
                    self._run[i] += retrial[2] - retrial[1]
                    # CPU consumption = run time (COMPLETED retrial) * number of processors requested (accumulated)
                    self._cpu_consumption += self.run[i] * int(processors)
                    # REAL CONSUMPTION = run time (accumulated)
                    self._real_consumption += self.run[i]
                    # Count as COMPLETED job
                    self._total_jobs_completed += 1
                else:
                    # Not COMPLETED status
                    if len(retrial) > 2:
                        # Consider it as a FAILED run
                        # Accumulate RUN time
                        self._fail_run[i] += retrial[2] - retrial[1]
                    if len(retrial) > 1:
                        # It only QUEUED
                        # Accumulate QUEUE time
                        self._fail_queued[i] += retrial[1] - retrial[0]
                    # CPU consumption = run time (FAILED retrial) * number of processors requested (accumulated)
                    self._cpu_consumption += self.fail_run[i] * int(processors)
                    # REAL CONSUMPTION = run time (accumulated)
                    self._real_consumption += self.fail_run[i]
                    # Count as FAILED job
                    self._failed_jobs[i] += 1
            self._total_jobs_submitted += len(last_retrials)
            self._total_jobs_run += len(last_retrials)
            self._total_jobs_failed += self.failed_jobs[i]
            self._threshold = max(self._threshold, job.total_wallclock)
            self._expected_cpu_consumption += job.total_wallclock * int(processors)
            self._expected_real_consumption += job.total_wallclock
            self._total_queueing_time += self._queued[i]

    def _calculate_maxs(self):
        max_run = max(max(self._run), max(self._fail_run))
        max_queued = max(max(self._queued), max(self._fail_queued))
        self._max_timedelta = max(
            max_run, max_queued, datetime.timedelta(hours=self._threshold))
        self._max_time = max(self._max_time, self._max_timedelta.days *
                             24 + self._max_timedelta.seconds / 3600.0)
        self._max_fail = max(self._max_fail, max(self._failed_jobs))

    def _calculate_totals(self):
        """
        Calculates totals and prints to console.
        """
        percentage_consumption = timedelta2hours(
            self._cpu_consumption) / self._expected_cpu_consumption * 100
        self._totals = ['Period: ' + str(self._start) + " ~ " + str(self._end),
                        'Submitted (#): ' + str(self._total_jobs_submitted),
                        'Run  (#): ' + str(self._total_jobs_run),
                        'Failed  (#): ' + str(self._total_jobs_failed),
                        'Completed (#): ' + str(self._total_jobs_completed),
                        'Queueing time (h): ' +
                        str(round(timedelta2hours(self._total_queueing_time), 2)),
                        'Expected consumption real (h): ' + str(
                            round(self._expected_real_consumption, 2)),
                        'Expected consumption CPU time (h): ' + str(
                            round(self._expected_cpu_consumption, 2)),
                        'Consumption real (h): ' +
                        str(round(timedelta2hours(self._real_consumption), 2)),
                        'Consumption CPU time (h): ' + str(
                            round(timedelta2hours(self._cpu_consumption), 2)),
                        'Consumption (%): ' + str(round(percentage_consumption, 2))]
        Log.result('\n'.join(self._totals))

    def _format_stats(self):
        self._queued = [timedelta2hours(y) for y in self._queued]
        self._run = [timedelta2hours(y) for y in self._run]
        self._fail_queued = [timedelta2hours(y) for y in self._fail_queued]
        self._fail_run = [timedelta2hours(y) for y in self._fail_run]
