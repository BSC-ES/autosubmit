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
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/

from datetime import datetime, timedelta
from typing import Union

from autosubmit.job.job import Job
from autosubmit.statistics.jobs_stat import JobStat
from autosubmit.statistics.stats_summary import StatsSummary
from autosubmit.statistics.utils import timedelta2hours, parse_number_processors

_COMPLETED_RETRIAL = 1
_FAILED_RETRIAL = 0


class Statistics:

    def __init__(
            self,
            jobs: list[Job],
            start: datetime,
            end: datetime,
            queue_time_fix: dict[str, int],
            jobs_stat=None
    ) -> None:
        self._jobs = jobs
        self._start = start
        self._end = end
        self._queue_time_fixes = queue_time_fix
        self._name_to_jobstat_dict: dict[str, JobStat] = dict()
        self.jobs_stat = jobs_stat
        # Old format
        self.max_time = 0.0
        self.max_fail = 0
        self.start_times: list[Union[datetime, None]] = []
        self.end_times: list[Union[datetime, None]] = []
        self.queued: list[timedelta] = []
        self.run: list[timedelta] = []
        self.failed_jobs: list[int] = []
        self.fail_queued: list[timedelta] = []
        self.fail_run: list[timedelta] = []
        self.wallclocks: list[float] = []
        self.threshold = 0.0
        self.failed_jobs_dict: dict[str, int] = {}
        self.summary: StatsSummary = StatsSummary()
        self.totals = [" Description text \n", "Line 1"]

    def calculate_statistics(self) -> "Statistics":
        for index, job in enumerate(self._jobs):
            retrials = job.get_last_retrials()
            for retrial in retrials:
                job_stat = self._name_to_jobstat_dict.setdefault(job.name, JobStat(job.name, parse_number_processors(
                    job.processors), job.total_wallclock, job.section, job.date, job.member, job.chunk, job.processors_per_node, job.tasks, job.nodes, job.exclusive))
                job_stat.inc_retrial_count()
                if Job.is_a_completed_retrial(retrial):
                    job_stat.inc_completed_retrial_count()
                    job_stat.submit_time = retrial[0]
                    job_stat.start_time = retrial[1]
                    job_stat.finish_time = retrial[2]
                    adjusted_queue = max(job_stat.start_time - job_stat.submit_time, timedelta()) - timedelta(
                        seconds=self._queue_time_fixes.get(job.name, 0))
                    job_stat.completed_queue_time += max(adjusted_queue, timedelta())
                    job_stat.completed_run_time += max(job_stat.finish_time - job_stat.start_time, timedelta())
                else:
                    job_stat.inc_failed_retrial_count()
                    job_stat.submit_time = retrial[0] if len(retrial) >= 1 and type(retrial[0]) is datetime else None
                    job_stat.start_time = retrial[1] if len(retrial) >= 2 and type(retrial[1]) is datetime else None
                    job_stat.finish_time = retrial[2] if len(retrial) >= 3 and type(retrial[2]) is datetime else None
                    if job_stat.finish_time and job_stat.start_time:
                        job_stat.failed_run_time += max(job_stat.finish_time - job_stat.start_time, timedelta())
                    if job_stat.start_time and job_stat.submit_time:
                        adjusted_failed_queue = max(job_stat.start_time - job_stat.submit_time,
                                                    timedelta()) - timedelta(
                            seconds=self._queue_time_fixes.get(job.name, 0))
                        job_stat.failed_queue_time += max(adjusted_failed_queue, timedelta())
        self.jobs_stat = sorted(list(self._name_to_jobstat_dict.values()), key=lambda x: (
            x.date if x.date else datetime.now(), x.member if x.member else "", x.section if x.section else "", x.chunk))
        return self

    def calculate_summary(self) -> "Statistics":
        stat_summary = StatsSummary()
        for job in self.jobs_stat:
            job_stat_dict = job.get_as_dict()
            # Counter
            stat_summary.submitted_count += job_stat_dict["submittedCount"]
            stat_summary.run_count += job_stat_dict["retrialCount"]
            stat_summary.completed_count += job_stat_dict["completedCount"]
            stat_summary.failed_count += job_stat_dict["failedCount"]
            # Consumption
            stat_summary.expected_consumption += job_stat_dict["expectedConsumption"]
            stat_summary.real_consumption += job_stat_dict["realConsumption"]
            stat_summary.failed_real_consumption += job_stat_dict["failedRealConsumption"]
            # CPU Consumption
            stat_summary.expected_cpu_consumption += job_stat_dict["expectedCpuConsumption"]
            stat_summary.cpu_consumption += job_stat_dict["cpuConsumption"]
            stat_summary.failed_cpu_consumption += job_stat_dict["failedCpuConsumption"]
            stat_summary.total_queue_time += job_stat_dict["completedQueueTime"] + job_stat_dict["failedQueueTime"]
        stat_summary.calculate_consumption_percentage()
        self.summary = stat_summary
        return self

    def make_old_format(self) -> "Statistics":
        """Make the data compatible to the old format."""
        self.start_times = [job.start_time for job in self.jobs_stat]
        self.end_times = [job.finish_time for job in self.jobs_stat]
        self.queued = [timedelta2hours(job.completed_queue_time) for job in self.jobs_stat]
        self.run = [timedelta2hours(job.completed_run_time) for job in self.jobs_stat]
        self.failed_jobs = [job.failed_retrial_count for job in self.jobs_stat]
        if len(self.failed_jobs) == 0:
            self.max_fail = 0
        else:
            self.max_fail = max(self.failed_jobs)
        self.fail_run = [timedelta2hours(job.failed_run_time) for job in self.jobs_stat]
        self.fail_queued = [timedelta2hours(job.failed_queue_time) for job in self.jobs_stat]
        self.wallclocks = [job.expected_real_consumption for job in self.jobs_stat]
        if len(self.wallclocks) == 0:
            self.threshold = 0.0
        else:
            self.threshold = max(self.wallclocks)
        if len(self.queued) == 0:
            max_queue = 0.0
        else:
            max_queue = max(self.queued)
        if len(self.run) == 0:
            max_run = 0.0
        else:
            max_run = max(self.run)
        if len(self.fail_queued) == 0:
            max_fail_queue = 0.0
        else:
            max_fail_queue = max(self.fail_queued)
        if len(self.fail_run) == 0:
            max_fail_run = 0.0
        else:
            max_fail_run = max(self.fail_run)
        self.max_time = max(max_queue, max_run, max_fail_queue, max_fail_run, self.threshold)
        return self

    def build_failed_jobs(self) -> "Statistics":
        for i, job in enumerate(self.jobs_stat):
            if self.failed_jobs[i] > 0:
                self.failed_jobs_dict[job.name] = self.failed_jobs[i]
        return self

    # Built object properties.

    @property
    def summary_list(self):
        return self.summary.get_as_list()
