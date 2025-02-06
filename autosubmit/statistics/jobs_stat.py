#!/bin/env/python
from datetime import datetime, timedelta
from .utils import timedelta2hours
from log.log import Log
import math

class JobStat(object):
    def __init__(self, name, processors, wallclock, section, date, member, chunk, processors_per_node, tasks, nodes, exclusive ):
        # type: (str, int, float, str, str, str, str, str, str , str, str) -> None
        self._name = name
        self._processors = self._calculate_processing_elements(nodes, processors, tasks, processors_per_node, exclusive)
        self._wallclock = wallclock
        self.submit_time = None # type: datetime
        self.start_time = None # type: datetime
        self.finish_time = None # type: datetime
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
            Log.warning('Missing PROCESSORS_PER_NODE. Should be set if TASKS or NODES are defined. The PROCESSORS will used instead.')
        return int(processors)

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
