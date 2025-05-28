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

from datetime import datetime, timedelta
from math import ceil
from typing import Optional

from autosubmit.statistics.utils import timedelta2hours
from log.log import Log


def _estimate_requested_nodes(nodes, processors, tasks, processors_per_node) -> int:
    """Estimates the number of requested nodes.

    In the past we had ``ZeroDivisionError`` due to the number of ``tasks`` being set
    to ``0`` by default in the ``Job`` class. If that changes, we can remove the checks
    here.

    If ``nodes`` is provided and valid, that's returned immediately.

    If ``tasks`` is provided and valid, and ``tasks`` is greater than ``0`` (to prevent
    the ``ZeroDivisionError``) then we return the ceiling value of ``processors``
    divided by ``tasks``.

    If ``processors_per_node`` is provided and valid, and ``processors_per_node`` is
    greater than zero (``ZeroDivisionError``), and ``processors_per_node`` is not
    greater than ``processors``, then we return the ceiling value of ``processors``
    divided by the number of ``processors_per_node``.

    Else, we return ``1``.
    """
    if str(nodes).isdigit():
        return int(nodes)
    elif str(tasks).isdigit() and int(tasks) > 0:
        return ceil(int(processors) / int(tasks))
    elif str(processors_per_node).isdigit() and 0 < int(processors_per_node) < int(processors):
        return ceil(int(processors) / int(processors_per_node))
    else:
        return 1


def _calculate_processing_elements(nodes, processors, tasks, processors_per_node, exclusive) -> int:
    if str(processors_per_node).isdigit():
        if str(nodes).isdigit():
            return int(nodes) * int(processors_per_node)
        else:
            estimated_nodes = _estimate_requested_nodes(nodes, processors, tasks, processors_per_node)
            if not exclusive and estimated_nodes <= 1 and int(processors) <= int(processors_per_node):
                return int(processors)
            else:
                return estimated_nodes * int(processors_per_node)
    elif str(tasks).isdigit() or str(nodes).isdigit():
        Log.warning(f'Missing PROCESSORS_PER_NODE. Should be set if TASKS or NODES are defined. '
                    f'The PROCESSORS will used instead.')
    return int(processors)


class JobStat(object):
    def __init__(self, name: str, processors: int, wallclock: float, section: str, date: str,
                 member: str, chunk: str, processors_per_node: str, tasks: str, nodes: str,
                 exclusive: str):
        self._name = name
        self._processors = _calculate_processing_elements(nodes, processors, tasks, processors_per_node, exclusive)
        self._wallclock = wallclock
        self.submit_time: Optional[datetime] = None
        self.start_time: Optional[datetime] = None
        self.finish_time: Optional[datetime] = None
        self.completed_queue_time = timedelta()
        self.completed_run_time = timedelta()
        self.failed_queue_time = timedelta()
        self.failed_run_time = timedelta()
        self.retrial_count = 0
        self.completed_retrial_count = 0
        self.failed_retrial_count = 0
        self.section = section
        self.date = date
        self.member = member
        self.chunk = chunk

    def inc_retrial_count(self):
        self.retrial_count += 1

    def inc_completed_retrial_count(self):
        self.completed_retrial_count += 1

    def inc_failed_retrial_count(self):
        self.failed_retrial_count += 1

    @property
    def cpu_consumption(self):
        return timedelta2hours(self._processors * self.completed_run_time) + timedelta2hours(self._processors * self.failed_run_time)

    @property
    def failed_cpu_consumption(self):
        return timedelta2hours(self._processors * self.failed_run_time)

    @property
    def real_consumption(self):
        return timedelta2hours(self.failed_run_time + self.completed_run_time)

    @property
    def expected_real_consumption(self):
        return self._wallclock

    @property
    def expected_cpu_consumption(self):
        return self._wallclock * self._processors

    @property
    def name(self):
        return self._name

    def get_as_dict(self):
        return {
            "name": self._name,
            "processors": self._processors,
            "wallclock": self._wallclock,
            "completedQueueTime": timedelta2hours(self.completed_queue_time),
            "completedRunTime": timedelta2hours(self.completed_run_time),
            "failedQueueTime": timedelta2hours(self.failed_queue_time),
            "failedRunTime": timedelta2hours(self.failed_run_time),
            "cpuConsumption": self.cpu_consumption,
            "failedCpuConsumption": self.failed_cpu_consumption,
            "expectedCpuConsumption": self.expected_cpu_consumption,
            "realConsumption": self.real_consumption,
            "failedRealConsumption": timedelta2hours(self.failed_run_time),
            "expectedConsumption": self.expected_real_consumption,
            "retrialCount": self.retrial_count,
            "submittedCount": self.retrial_count,
            "completedCount": self.completed_retrial_count,
            "failedCount": self.failed_retrial_count
        }
