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
import collections

from autosubmit.job.job import Job
from log.log import Log, AutosubmitCritical
from autosubmit.job.job_common import Status, Type
from bscearth.utils.date import sum_str_hours
from autosubmit.job.job_packages import JobPackageSimple, JobPackageVertical, JobPackageHorizontal, \
    JobPackageSimpleWrapped, JobPackageHorizontalVertical, JobPackageVerticalHorizontal, JobPackageBase
from operator import attrgetter
from math import ceil
import operator
from typing import List
from contextlib import suppress

class JobPackager(object):
    """
    Main class that manages Job wrapping.

    :param as_config: Autosubmit basic configuration.\n
    :type as_config: AutosubmitConfig object.\n
    :param platform: A particular platform we are dealing with, e.g. Slurm Platform.\n
    :type platform: Specific Platform Object, e.g. SlurmPlatform(), EcPlatform(), ...\n
    :param jobs_list: Contains the list of the jobs, along other properties.\n
    :type jobs_list: JobList object.
    """


    def __init__(self, as_config, platform, jobs_list, hold=False):
        self.current_wrapper_section = "WRAPPERS"
        self._as_config = as_config
        self._platform = platform
        self._jobs_list = jobs_list
        self._max_wait_jobs_to_submit = 9999999
        self.hold = hold
        self.max_jobs = None
        self._max_jobs_to_submit = None
        # These are defined in the [wrapper] section of autosubmit_,conf
        self.wrapper_type = dict()
        self.wrapper_policy = dict()
        self.wrapper_method = dict()
        self.jobs_in_wrapper = dict()
        self.extensible_wallclock = dict()
        self.wrapper_info = list()
        self.calculate_job_limits(platform)
        self.special_variables = dict()
        self.wrappers_with_error = {}


        #todo add default values
        #Wrapper building starts here
        for wrapper_section,wrapper_data in self._as_config.experiment_data.get("WRAPPERS",{}).items():
            if isinstance(wrapper_data,collections.abc.Mapping ):
                self.wrapper_type[wrapper_section] = self._as_config.get_wrapper_type(wrapper_data)
                self.wrapper_policy[wrapper_section] = self._as_config.get_wrapper_policy(wrapper_data)
                if self._as_config.get_wrapper_method(wrapper_data) is None:
                    self.wrapper_method[wrapper_section] = "asthread"
                else:
                    self.wrapper_method[wrapper_section] = self._as_config.get_wrapper_method(wrapper_data).lower()
                self.jobs_in_wrapper[wrapper_section] = self._as_config.get_wrapper_jobs(wrapper_data)
                self.extensible_wallclock[wrapper_section] = self._as_config.get_extensible_wallclock(wrapper_data)
        self.wrapper_info = [self.wrapper_type,self.wrapper_policy,self.wrapper_method,self.jobs_in_wrapper,self.extensible_wallclock] # to pass to job_packages
        Log.debug("Number of jobs available: {0}", self._max_wait_jobs_to_submit)
        if self.hold:
            Log.debug("Number of jobs prepared: {0}", len(jobs_list.get_prepared(platform)))
            if len(jobs_list.get_prepared(platform)) > 0:
                Log.debug("Jobs ready for {0}: {1}", self._platform.name, len(jobs_list.get_prepared(platform)))
        else:
            Log.debug("Number of jobs ready: {0}", len(jobs_list.get_ready(platform, hold=False)))
            if len(jobs_list.get_ready(platform)) > 0:
                Log.debug("Jobs ready for {0}: {1}", self._platform.name, len(jobs_list.get_ready(platform)))
        self._maxTotalProcessors = 0

    def compute_weight(self, job_list):
        jobs_by_section = dict()
        held_jobs = self._jobs_list.get_held_jobs()
        jobs_held_by_section = dict()
        for job in held_jobs:
            if job.section not in jobs_held_by_section:
                jobs_held_by_section[job.section] = []
            jobs_held_by_section[job.section].append(job)
        for job in job_list:
            if job.section not in jobs_by_section:
                jobs_by_section[job.section] = []
            if job.status != Status.COMPLETED:
                jobs_by_section[job.section].append(job)

        for section in jobs_by_section:
            if section in list(jobs_held_by_section.keys()):
                weight = len(jobs_held_by_section[section]) + 1
            else:
                weight = 1
            highest_completed = []

            for job in sorted(jobs_by_section[section], key=operator.attrgetter('chunk')):
                weight = weight + 1
                job.distance_weight = weight
                completed_jobs = 9999
                if job.has_parents() > 1:
                    tmp = [
                        parent for parent in job.parents if parent.status == Status.COMPLETED]
                    if len(tmp) > completed_jobs:
                        completed_jobs = len(tmp)
                        highest_completed = [job]
                    else:
                        highest_completed.append(job)
            for job in highest_completed:
                job.distance_weight = job.distance_weight - 1

    def calculate_wrapper_bounds(self, section_list):

        """
        Returns the minimum and maximum number of jobs that can be wrapped

        :param section_list: List of sections to be wrapped
        :type section_list: List of strings
        :return: Minimum and Maximum number of jobs that can be wrapped
        :rtype: Dictionary with keys: min, max, min_v, max_v, min_h, max_h, max_by_section
        """
        wrapper_limits = {'min': 1, 'max': 9999999, 'min_v': 1, 'max_v': 9999999, 'min_h': 1, 'max_h': 9999999, 'max_by_section': dict()}

        # Calculate the min and max based in the wrapper_section wrappers: min_wrapped:2, max_wrapped: 2 { wrapper_section: {min_wrapped: 6, max_wrapped: 6} }
        wrapper_data = self._as_config.experiment_data.get("WRAPPERS", {})
        current_wrapper_data = wrapper_data.get(self.current_wrapper_section, {})
        if len(self._jobs_list.jobs_to_run_first) == 0:
            wrapper_limits['min'] = int(current_wrapper_data.get("MIN_WRAPPED", wrapper_data.get("MIN_WRAPPED", 1)))
            wrapper_limits['max'] = int(current_wrapper_data.get("MAX_WRAPPED", wrapper_data.get("MAX_WRAPPED", 9999999)))
            wrapper_limits['min_v'] = int(current_wrapper_data.get("MIN_WRAPPED_V", wrapper_data.get("MIN_WRAPPED_V", 1)))
            wrapper_limits['max_v'] = int(current_wrapper_data.get("MAX_WRAPPED_V", wrapper_data.get("MAX_WRAPPED_V", 1)))
            wrapper_limits['min_h'] = int(current_wrapper_data.get("MIN_WRAPPED_H", wrapper_data.get("MIN_WRAPPED_H", 1)))
            wrapper_limits['max_h'] = int(current_wrapper_data.get("MAX_WRAPPED_H", wrapper_data.get("MAX_WRAPPED_H", 1)))
            # Max and min calculations
            if wrapper_limits['max'] < wrapper_limits['max_v'] * wrapper_limits['max_h']:
                wrapper_limits['max'] = wrapper_limits['max_v'] * wrapper_limits['max_h']
            if wrapper_limits['min'] < wrapper_limits['min_v'] * wrapper_limits['min_h']:
                wrapper_limits['min'] = max(wrapper_limits['min_v'], wrapper_limits['min_h'])
            # if one dimensional wrapper or value is the default
            if wrapper_limits['max_v'] == 1 or current_wrapper_data.get("TYPE", "") == "vertical":
                wrapper_limits['max_v'] = wrapper_limits['max']

            if wrapper_limits['max_h'] == 1 or current_wrapper_data.get("TYPE", "") == "horizontal":
                wrapper_limits['max_h'] = wrapper_limits['max']

            if wrapper_limits['min_v'] == 1 and current_wrapper_data.get("TYPE", "") == "vertical":
                wrapper_limits['min_v'] = wrapper_limits['min']

            if wrapper_limits['min_h'] == 1 and current_wrapper_data.get("TYPE", "") == "horizontal":
                wrapper_limits['min_h'] = wrapper_limits['min']

        # Calculate the max by section by looking at jobs_data[section].max_wrapped
        for section in section_list:
            wrapper_limits['max_by_section'][section] = self._as_config.jobs_data.get(section,{}).get("MAX_WRAPPED", wrapper_limits['max'])

        wrapper_limits['real_min'] = max(2, wrapper_limits['min'])
        return wrapper_limits

    def check_jobs_to_run_first(self, package):
        """
        Check if the jobs to run first are in the package
        :param package:
        :return:
        """
        run_first = False
        if len(self._jobs_list.jobs_to_run_first) > 0:
            for job in package.jobs[:]:
                job.wrapper_type = package.wrapper_type
                if job in self._jobs_list.jobs_to_run_first:
                    run_first = True
                else:
                    package.jobs.remove(job)
                    if self.wrapper_type[self.current_wrapper_section] not in ["horizontal", "vertical", "vertical-mixed"]:
                        for seq in range(0, len(package.jobs_lists)):
                            with suppress(ValueError):
                                package.jobs_lists[seq].remove(job)
        return package, run_first

    def check_real_package_wrapper_limits(self, package):
        balanced = True
        if self.wrapper_type[self.current_wrapper_section] == 'vertical-horizontal':
            i = 0
            min_h = len(package.jobs_lists)
            min_v = len(package.jobs_lists[0])
            for list_of_jobs in package.jobs_lists[1:-1]:
                min_v = min(min_v, len(list_of_jobs))
            for list_of_jobs in package.jobs_lists[:]:
                i = i + 1
                if min_v != len(list_of_jobs) and i < len(package.jobs_lists):
                    balanced = False
        elif self.wrapper_type[self.current_wrapper_section] == 'horizontal-vertical':
            min_v = len(package.jobs_lists)
            min_h = len(package.jobs_lists[0])
            i = 0
            for list_of_jobs in package.jobs_lists[1:-1]:
                min_h = min(min_h, len(list_of_jobs))
            for list_of_jobs in package.jobs_lists[:]:
                i = i + 1
                if min_h != len(list_of_jobs) and i < len(package.jobs_lists):
                    balanced = False
        elif self.wrapper_type[self.current_wrapper_section] == 'horizontal':
            min_h = len(package.jobs)
            min_v = 1
        elif self.wrapper_type[self.current_wrapper_section] == 'vertical':
            min_v = len(package.jobs)
            min_h = 1
        else:
            min_v = len(package.jobs)
            min_h = len(package.jobs)
        return min_v, min_h, balanced

    def check_packages_respect_wrapper_policy(self, built_packages_tmp, packages_to_submit, max_jobs_to_submit, wrapper_limits, any_simple_packages = False):
        """
        Check if the packages respect the wrapper policy and act in base of it ( submit wrapper, submit sequential, wait for more jobs to form a wrapper)
        :param built_packages_tmp: List of packages to be submitted
        :param packages_to_submit: List of packages to be submitted
        :param max_jobs_to_submit: Maximum number of jobs to submit
        :param wrapper_limits: Dictionary with keys: min, max, min_v, max_v, min_h, max_h, max_by_section
        :return: packages_to_submit, max_jobs_to_submit
        :rtype: List of packages to be submitted, int
        :return: packages_to_submit, max_jobs_to_submit
        """
        not_wrappeable_package_info = list()
        for p in built_packages_tmp:
            if max_jobs_to_submit == 0:
                break
            failed_innerjobs = False
            # Check if the user is using the option to run first some jobs. if so, remove non-first jobs from the package and submit them sequentially following a flexible policy
            if len(self._jobs_list.jobs_to_run_first) > 0:
                p,run_first = self.check_jobs_to_run_first(p)
                if run_first:
                    for job in p.jobs:
                        if max_jobs_to_submit == 0:
                            break
                        if job.status == Status.READY:
                            if job.type == Type.PYTHON and not self._platform.allow_python_jobs:
                                package = JobPackageSimpleWrapped([job])
                            else:
                                package = JobPackageSimple([job])
                            packages_to_submit.append(package)
                            max_jobs_to_submit = max_jobs_to_submit - 1
                continue
            for job in p.jobs:
                if job.fail_count > 0:
                    failed_innerjobs = True
                    break

            min_v, min_h, balanced = self.check_real_package_wrapper_limits(p)
            # if the quantity is enough, make the wrapper
            if len(p.jobs) >= wrapper_limits["real_min"] and min_v >= wrapper_limits["min_v"] and min_h >= wrapper_limits["min_h"] and not failed_innerjobs:
                for job in p.jobs:
                    job.wrapper_type = p.wrapper_type
                    job.packed = True
                packages_to_submit.append(p)
                max_jobs_to_submit = max_jobs_to_submit - 1
            else:
                not_wrappeable_package_info.append([p, min_v, min_h, balanced])

        # It is a deadlock when:
        # 1. There are no more non-wrapped jobs in ready status
        # 2. And there are no more jobs in the queue ( submitted, queuing, running, held )
        # 3. And all current packages are not wrappable but not if there are no more jobs to wrap.
        if self.is_deadlock(any_simple_packages, not_wrappeable_package_info, built_packages_tmp):
            max_jobs_to_submit = self.process_not_wrappeable_packages(not_wrappeable_package_info, packages_to_submit,
                                                                      max_jobs_to_submit, wrapper_limits)
        return packages_to_submit, max_jobs_to_submit

    def is_deadlock(self, any_simple_packages: bool, not_wrappeable_package_info: list, built_packages_tmp: list) -> bool:
        """
        Check if the current state is a deadlock.

        :param any_simple_packages: Flag indicating if there are any simple packages.
        :param not_wrappeable_package_info: List of not wrappable package information.
        :param built_packages_tmp: List of built packages.
        :return: True if it is a deadlock, False otherwise.
        """
        return (
                not any_simple_packages
                and len(self._jobs_list.get_in_queue()) == 0
                and len(not_wrappeable_package_info) == len(built_packages_tmp)
        )

    def submit_remaining_jobs(self, p: JobPackageBase, packages_to_submit: list, max_jobs_to_submit: int) -> int:
        """
        Submit the remaining jobs because there are not enough jobs of this section remaining to form a wrapper.

        :param p: The package to be submitted.
        :param packages_to_submit: List of packages to be submitted.
        :param max_jobs_to_submit: Maximum number of jobs to submit.
        :return: Updated maximum number of jobs to submit.
        """
        Log.warning("There are no more jobs of this section to form a wrapper, submitting the remaining jobs")
        if len(p.jobs) == 1:
            p.jobs[0].wrapper_type = "Simple"
            packages_to_submit.append(JobPackageSimple([p.jobs[0]]))
        else:
            packages_to_submit.append(p)
        return max_jobs_to_submit - 1

    def handle_strict_policy(self, p: JobPackageBase,  err_message: str) -> None:
        """
        Handle the strict policy case by filling self.wrappers_with_error with the error message.
        :param p: The package to be processed.
        :param err_message: The error message to be raised.
        """
        job_names = ','.join([job.name for job in p.jobs])
        self.wrappers_with_error[job_names] = err_message

    def handle_mixed_policy(self, p: JobPackageBase, packages_to_submit: list, max_jobs_to_submit: int, err_message) -> int:
        """
        Handle the mixed policy case by submitting failed jobs sequentially or/and filling self.wrappers_with_error with the error message.

        :param p: The package to be processed.
        :param packages_to_submit: List of packages to be submitted.
        :param max_jobs_to_submit: Maximum number of jobs to submit.
        :return: Updated maximum number of jobs to submit.
        """
        error = True
        for job in p.jobs:
            if max_jobs_to_submit == 0:
                break
            if job.fail_count > 0 and job.status == Status.READY:
                Log.printlog("Wrapper policy is set to mixed, there is a failed job that will be sent sequential")
                error = False
                package = JobPackageSimpleWrapped(
                    [job]) if job.type == Type.PYTHON and not self._platform.allow_python_jobs else JobPackageSimple(
                    [job])
                packages_to_submit.append(package)
                max_jobs_to_submit -= 1
        if error:
            job_names = ','.join([job.name for job in p.jobs])
            self.wrappers_with_error[job_names] = err_message
        return max_jobs_to_submit

    def handle_flexible_policy(self, p: JobPackageBase, packages_to_submit: list, max_jobs_to_submit: int, err_message: str) -> int:
        """
        Handle the flexible policy case by submitting jobs sequentially.

        :param p: The package to be processed.
        :param packages_to_submit: List of packages to be submitted.
        :param max_jobs_to_submit: Maximum number of jobs to submit.
        :param err_message: The error message to be raised.
        :return: Updated maximum number of jobs to submit.
        """
        Log.warning(err_message)
        Log.warning(
            "Wrapper policy is set to flexible and there is a deadlock, Autosubmit will submit the jobs sequentially")
        for job in p.jobs:
            if max_jobs_to_submit == 0:
                break
            if job.status == Status.READY:
                package = JobPackageSimpleWrapped(
                    [job]) if job.type == Type.PYTHON and not self._platform.allow_python_jobs else JobPackageSimple(
                    [job])
                packages_to_submit.append(package)
                max_jobs_to_submit -= 1
        return max_jobs_to_submit

    def process_not_wrappeable_packages(self, not_wrappeable_package_info: list, packages_to_submit: list,
                                        max_jobs_to_submit: int, wrapper_limits: dict):
        """
        Process the not wrappable packages based on the policy.

        :param not_wrappeable_package_info: List of not wrappable package information.
        :param packages_to_submit: List of packages to be submitted.
        :param max_jobs_to_submit: Maximum number of jobs to submit.
        :param wrapper_limits: Dictionary with wrapper limits.
        :return: Updated maximum number of jobs to submit.
        """
        for p, min_v, min_h, balanced in not_wrappeable_package_info:
            err_message = self.error_message_policy(min_h, min_v, wrapper_limits, balanced, p.jobs)
            if not self._jobs_list.get_jobs_by_section(self.jobs_in_wrapper[self.current_wrapper_section], [job.name for job in p.jobs],
                                                       True):
                max_jobs_to_submit = self.submit_remaining_jobs(p, packages_to_submit, max_jobs_to_submit)
            else:
                if self.wrapper_policy[self.current_wrapper_section] == "strict":
                    self.handle_strict_policy(p, err_message)
                elif self.wrapper_policy[self.current_wrapper_section] == "mixed":
                    max_jobs_to_submit = self.handle_mixed_policy(p, packages_to_submit, max_jobs_to_submit, err_message)
                else:
                    max_jobs_to_submit = self.handle_flexible_policy(p, packages_to_submit, max_jobs_to_submit, err_message)
            if self.wrappers_with_error:
                for job_names, err_message in self.wrappers_with_error.items():
                    Log.error(f"Wrapped jobs with deadlock issues: [{job_names}].")
                    Log.error(err_message)
                raise AutosubmitCritical("Critical error in wrapper policy", 7014)
        return max_jobs_to_submit

    def error_message_policy(self, min_h: int, min_v: int, wrapper_limits: dict, balanced: bool,
                             jobs: list) -> str:
        """
        Generate an error message for wrapper policy violations.

        Parameters:
        min_h (int): Minimum horizontal jobs.
        min_v (int): Minimum vertical jobs.
        wrapper_limits (dict): Dictionary containing wrapper limits.
        balanced (bool): Indicates if the packages are balanced.
        jobs (list): List of jobs with issues.

        Returns:
        str: Formatted error message.
        """
        message = (
            f"\nWrapper couldn't be formed under {self.wrapper_policy[self.current_wrapper_section]} POLICY due to minimum limit not being reached:"
            f"\n[package_min_total: {min_h * min_v} < defined: {wrapper_limits['real_min']}]"
            f"\n[package_min_h: {min_h} < defined: {wrapper_limits['min_h']}]"
            f"\n[package_min_v: {min_v} < defined: {wrapper_limits['min_v']}]"
            f"\n[section_wallclock: {max([job.wallclock for job in jobs])}] < [platform_max_wallclock: {self._platform.max_wallclock}]"
        )

        if not balanced:
            message += "\nPackages are not well balanced! (This is not the main cause of the Critical error)"
        jobs_str = ', '.join([job.name for job in jobs])
        message += f"\nJobs with issues:[{jobs_str}].\nRevise these jobs dependencies and try again."
        message += "\nThis message is activated when only jobs_in_wrappers are in active(Ready+) status.\n"
        return message

    def check_if_packages_are_ready_to_build(self):
        """
        Check if the packages are ready to be built
        :return: List of jobs ready to be built, boolean indicating if packages can't be built for other reasons ( max_total_jobs...)
        """
        Log.info("Calculating possible ready jobs for {0}".format(self._platform.name))
        jobs_ready = list()
        if len(self._jobs_list.jobs_to_run_first) > 0:
            jobs_ready = [job for job in self._jobs_list.jobs_to_run_first if
                     ( self._platform is None or job.platform.name.upper() == self._platform.name.upper()) and
                     job.status == Status.READY]
        if len(jobs_ready) == 0:
            if self.hold:
                jobs_ready = self._jobs_list.get_prepared(self._platform)
            else:
                jobs_ready = self._jobs_list.get_ready(self._platform)

        if self.hold and len(jobs_ready) > 0:
            self.compute_weight(jobs_ready)
            sorted_jobs = sorted(
                jobs_ready, key=operator.attrgetter('distance_weight'))
            jobs_in_held_status = self._jobs_list.get_held_jobs() + self._jobs_list.get_submitted(self._platform, hold=self.hold)
            held_by_id = dict()
            for held_job in jobs_in_held_status:
                if held_job.id not in held_by_id:
                    held_by_id[held_job.id] = []
                held_by_id[held_job.id].append(held_job)
            current_held_jobs = len(list(held_by_id.keys()))
            remaining_held_slots = 5 - current_held_jobs
            Log.debug("there are currently {0} held jobs".format(remaining_held_slots))
            try:
                while len(sorted_jobs) > remaining_held_slots:
                    del sorted_jobs[-1]
                for job in sorted_jobs:
                    if job.distance_weight > 3:
                        sorted_jobs.remove(job)
                jobs_ready = sorted_jobs
                pass
            except IndexError:
                pass
        if len(jobs_ready) == 0:
            # If there are no jobs ready, result is tuple of empty
            return jobs_ready,False
        #check if there are jobs listed on calculate_job_limits
        self.calculate_job_limits(self._platform)
        if not (self._max_wait_jobs_to_submit > 0 and self._max_jobs_to_submit > 0):
            # If there is no more space in platform, result is tuple of empty
            Log.debug('Max jobs to submit reached, waiting for more space in platform {0}'.format(self._platform.name))
            return jobs_ready,False
        return jobs_ready,True

    def calculate_job_limits(self,platform,job=None):
        jobs_list = self._jobs_list
        # Submitted + Queuing Jobs for specific Platform
        queuing_jobs = jobs_list.get_queuing(platform)
        # We now consider the running jobs count
        running_jobs = jobs_list.get_running(platform)
        running_by_id = dict()
        for running_job in running_jobs:
            running_by_id[running_job.id] = running_job
        self.running_jobs_len = len(running_by_id.keys())

        queued_by_id = dict()
        for queued_job in queuing_jobs:
            queued_by_id[queued_job.id] = queued_job
        self.queuing_jobs_len = len(list(queued_by_id.keys()))

        submitted_jobs = jobs_list.get_submitted(platform)
        submitted_by_id = dict()
        for submitted_job in submitted_jobs:
            submitted_by_id[submitted_job.id] = submitted_job
        submitted_jobs_len = len(list(submitted_by_id.keys()))

        self.waiting_jobs = submitted_jobs_len + self.queuing_jobs_len
        # Calculate available space in Platform Queue
        if job is not None and job.max_waiting_jobs and platform.max_waiting_jobs and int(job.max_waiting_jobs) != int(platform.max_waiting_jobs):
            self._max_wait_jobs_to_submit = int(job.max_waiting_jobs) - int(self.waiting_jobs)
        else:
            self._max_wait_jobs_to_submit = int(platform.max_waiting_jobs) - int(self.waiting_jobs)
        # .total_jobs is defined in each section of platforms_.yml, if not from there, it comes form autosubmit_.yml
        # .total_jobs Maximum number of jobs at the same time
        if job is not None and job.total_jobs != platform.total_jobs:
            self._max_jobs_to_submit = job.total_jobs - self.queuing_jobs_len
        else:
            self._max_jobs_to_submit = platform.total_jobs - self.queuing_jobs_len
        # Subtracting running jobs
        self._max_jobs_to_submit = self._max_jobs_to_submit - self.running_jobs_len
        self._max_jobs_to_submit = self._max_jobs_to_submit if self._max_jobs_to_submit > 0 else 0
        self.max_jobs = min(self._max_wait_jobs_to_submit,self._max_jobs_to_submit)

    def build_packages(self):
        # type: () -> List[JobPackageBase]
        """
        Returns the list of the built packages to be submitted

        :return: List of packages depending on type of package, JobPackageVertical Object for 'vertical'.
        :rtype: List() of JobPackageVertical
        """
        packages_to_submit = list()
        jobs_ready,ready = self.check_if_packages_are_ready_to_build()
        if not ready:
            return []
        max_jobs_to_submit = min(self._max_wait_jobs_to_submit, self._max_jobs_to_submit)
        section_jobs_to_submit = dict()

        for job in [job for job in jobs_ready]:
            job.update_parameters(self._as_config, set_attributes=True)
            for event in job.platform.worker_events:  # keep alive log retrieval workers.
                if not event.is_set():
                    event.set()

            if job.section not in section_jobs_to_submit: # This is to fix TOTAL_JOBS when is set at job_level # Only for non-wrapped jobs
                if int(job.max_waiting_jobs) != int(job.platform.max_waiting_jobs):
                    section_max_wait_jobs_to_submit = int(job.max_waiting_jobs) - int(self.waiting_jobs)
                else:
                    section_max_wait_jobs_to_submit = None
                if int(job.total_jobs) != int(job.platform.total_jobs):
                    section_max_jobs_to_submit = int(job.total_jobs) - self.queuing_jobs_len - self.running_jobs_len
                else:
                    section_max_jobs_to_submit = None

                if section_max_jobs_to_submit is None:
                    section_max_jobs_to_submit = self._max_jobs_to_submit
                if section_max_wait_jobs_to_submit is None:
                    section_max_wait_jobs_to_submit = self._max_wait_jobs_to_submit

                section_jobs_to_submit ={job.section:min(section_max_wait_jobs_to_submit,section_max_jobs_to_submit)}
                Log.result(f"Section:{job.section} can submit {section_jobs_to_submit[job.section]} jobs at this time")
        jobs_to_submit = sorted(
            jobs_ready, key=lambda k: k.priority, reverse=True)
        jobs_to_wrap = self._divide_list_by_section(jobs_to_submit)
        non_wrapped_jobs = jobs_to_wrap.pop("SIMPLE", [])
        any_simple_packages = len(non_wrapped_jobs) > 0
        # Prepare packages for wrapped jobs
        for wrapper_name, jobs in jobs_to_wrap.items():
            Log.info(f"Building packages for {wrapper_name}")
            if max_jobs_to_submit == 0:
                break
            self.current_wrapper_section = wrapper_name
            section = self._as_config.experiment_data.get("WRAPPERS", {}).get(self.current_wrapper_section, {}).get("JOBS_IN_WRAPPER", "")
            if not self._platform.allow_wrappers and self.wrapper_type[self.current_wrapper_section] in ['horizontal', 'vertical', 'vertical-horizontal', 'horizontal-vertical']:
                Log.warning(
                    "Platform {0} does not allow wrappers, submitting jobs individually".format(self._platform.name))
                for job in jobs:
                    non_wrapped_jobs.append(job)
                continue
            if "&" in section:
                section_list = section.split("&")
            elif "," in section:
                section_list = section.split(",")
            else:
                section_list = section.split(" ")
            wrapper_limits = self.calculate_wrapper_bounds(section_list)
            current_info = list()
            built_packages_tmp = list()
            for param in self.wrapper_info:
                current_info.append(param[self.current_wrapper_section])
            current_info.append(self._as_config)

            if self.wrapper_type[self.current_wrapper_section] == 'vertical':
                built_packages_tmp = self._build_vertical_packages(jobs, wrapper_limits, wrapper_info=current_info)
            elif self.wrapper_type[self.current_wrapper_section] == 'horizontal':
                built_packages_tmp = self._build_horizontal_packages(jobs, wrapper_limits, section, wrapper_info=current_info)
            elif self.wrapper_type[self.current_wrapper_section] in ['vertical-horizontal', 'horizontal-vertical']:
                built_packages_tmp.append(self._build_hybrid_package(jobs, wrapper_limits, section, wrapper_info=current_info))
            else:
                built_packages_tmp = self._build_vertical_packages(jobs, wrapper_limits, wrapper_info=current_info)
            self._propagate_inner_jobs_ready_date(built_packages_tmp)
            # Reset packed_during_building
            for p in built_packages_tmp:
                for job in p.jobs:
                    job.packed_during_building = False
            packages_to_submit, max_jobs_to_submit = self.check_packages_respect_wrapper_policy(built_packages_tmp, packages_to_submit, max_jobs_to_submit, wrapper_limits, any_simple_packages)
            if len(built_packages_tmp) > 0:
                Log.result(f"Built {len(built_packages_tmp)} wrappers for {wrapper_name}")

        # Now, prepare the packages for non-wrapper jobs
        for job in non_wrapped_jobs:
            job.wrapper_type = "Simple"
            job.packed = False
            if job.section in section_jobs_to_submit:
                if section_jobs_to_submit[job.section] == 0:
                    continue
            elif max_jobs_to_submit == 0:
                break
            if len(self._jobs_list.jobs_to_run_first) > 0: # if user wants to run first some jobs, submit them first
                if job not in self._jobs_list.jobs_to_run_first:
                    continue
            if job.type == Type.PYTHON and not self._platform.allow_python_jobs:
                package = JobPackageSimpleWrapped([job])
            else:
                package = JobPackageSimple([job])
            packages_to_submit.append(package)
            max_jobs_to_submit = max_jobs_to_submit - 1
            if job.section in section_jobs_to_submit:
                section_jobs_to_submit[job.section] = section_jobs_to_submit[job.section] - 1


        for package in packages_to_submit:
            self.max_jobs = self.max_jobs - 1
            package.hold = self.hold

        return packages_to_submit

    @staticmethod
    def _propagate_inner_jobs_ready_date(built_packages_tmp: List[JobPackageBase]) -> None:
        """
        Propagate the ready date of the inner jobs to the wrapper job.

        :param built_packages_tmp: List of built packages.
        """
        for package in built_packages_tmp:
            if len(package.jobs) > 1:
                for job in package.jobs[1:]:
                    job.ready_date = package.jobs[0].ready_date

    def _divide_list_by_section(self, jobs_list):
        """
        Returns a dict() with as many keys as 'jobs_list' different sections
        The value for each key is a list() with all the jobs with the key section.

        :param jobs_list: list of jobs to be divided
        :rtype: Dictionary Key: Section Name, Value: List(Job Object)
        """
        # .jobs_in_wrapper defined in .yml, see constructor.
        sections_split = dict()
        jobs_by_section = dict()

        for wrapper_name,jobs_in_wrapper in self.jobs_in_wrapper.items():
            section_name = ""
            for section in jobs_in_wrapper:
                section_name += section+"&"
            section_name = section_name[:-1]
            sections_split[wrapper_name] = section_name
            jobs_by_section[wrapper_name] = list()

        if self.jobs_in_wrapper:
            Log.info(f"Calculating wrapper packages")
        jobs_by_section["SIMPLE"] = []
        for wrapper_name,section_name in sections_split.items():
            for job in jobs_list[:]:
                if job.section.upper() in section_name.split("&"):
                    jobs_by_section[wrapper_name].append(job)
                    jobs_list.remove(job)
        for job in (job for job in jobs_list):
            jobs_by_section["SIMPLE"].append(job)
        for wrappers in list(jobs_by_section.keys()):
            if len(jobs_by_section[wrappers]) == 0:
                del jobs_by_section[wrappers]
        return jobs_by_section


    def _build_horizontal_packages(self, section_list, wrapper_limits, section, wrapper_info={}):
        packages = []
        horizontal_packager = JobPackagerHorizontal(section_list, self._platform.max_processors, wrapper_limits,
                                                    wrapper_limits["max"], self._platform.processors_per_node, self.wrapper_method[self.current_wrapper_section])

        package_jobs = horizontal_packager.build_horizontal_package(wrapper_info=wrapper_info)

        jobs_resources = dict()

        current_package = None
        if package_jobs:
            machinefile_function = self._as_config.get_wrapper_machinefiles()
            if machinefile_function == 'COMPONENTS':
                jobs_resources = horizontal_packager.components_dict
            jobs_resources['MACHINEFILES'] = machinefile_function
            current_package = JobPackageHorizontal(
                package_jobs, jobs_resources=jobs_resources, method=self.wrapper_method[self.current_wrapper_section], configuration=self._as_config, wrapper_section=self.current_wrapper_section)
            packages.append(current_package)

        return packages

    def _build_vertical_packages(self, section_list, wrapper_limits,wrapper_info={}):
        """
        Builds Vertical-Mixed or Vertical

        :param section_list: Jobs defined as wrappable belonging to a common section.\n
        :type section_list: List() of Job Objects. \n
        :param wrapper_limits: All wrapper limitations are inside this dictionary ( min,max,by_section,horizontal and vertical). \n
        :type wrapper_limits: Dict. \n
        :param wrapper_section: Current Section
        :type string
        :return: List of Wrapper Packages, Dictionary that details dependencies. \n
        :rtype: List() of JobPackageVertical(), Dictionary Key: String, Value: (Dictionary Key: Variable Name, Value: String/Int)
        """
        packages = []
        for job in section_list:
            if wrapper_limits["max"] > 0:
                if not job.packed_during_building:
                    dict_jobs = self._jobs_list.get_ordered_jobs_by_date_member(self.current_wrapper_section)
                    job_vertical_packager = JobPackagerVerticalMixed(dict_jobs, job, [job], job.wallclock, wrapper_limits["max"], wrapper_limits, self._platform.max_wallclock,wrapper_info=wrapper_info)
                    jobs_list = job_vertical_packager.build_vertical_package(job, wrapper_info)
                    packages.append(JobPackageVertical(jobs_list, configuration=self._as_config,wrapper_section=self.current_wrapper_section,wrapper_info=wrapper_info))
            else:
                break
        return packages

    def _build_hybrid_package(self, jobs_list, wrapper_limits, section,wrapper_info={}):
        #self.wrapper_info = wrapper_info
        jobs_resources = dict()
        jobs_resources['MACHINEFILES'] = self._as_config.get_wrapper_machinefiles()

        ## READY JOBS ##
        ## Create the horizontal ##
        horizontal_packager = JobPackagerHorizontal(jobs_list, self._platform.max_processors, wrapper_limits,
                                                    wrapper_limits["max"], self._platform.processors_per_node,self.wrapper_method[self.current_wrapper_section])

        if self.wrapper_type[self.current_wrapper_section] == 'vertical-horizontal':
            return self._build_vertical_horizontal_package(horizontal_packager, jobs_resources, wrapper_info)
        else:
            return self._build_horizontal_vertical_package(horizontal_packager, section, jobs_resources, wrapper_info)

    def _build_horizontal_vertical_package(self, horizontal_packager, section, jobs_resources, wrapper_info):
        total_wallclock = '00:00'
        horizontal_package = horizontal_packager.build_horizontal_package(wrapper_info=wrapper_info)
        horizontal_packager.create_sections_order(section)
        horizontal_packager.add_sectioncombo_processors(
            horizontal_packager.total_processors)
        horizontal_package.sort(
            key=lambda job: horizontal_packager.sort_by_expression(job.section))
        job = max(horizontal_package, key=attrgetter('total_wallclock'))
        wallclock = job.wallclock
        current_package = [horizontal_package]
        #current_package = []
        ## Get the next horizontal packages ##
        max_procs = horizontal_packager.total_processors
        new_package = horizontal_packager.get_next_packages(
            section, max_wallclock=self._platform.max_wallclock, horizontal_vertical=True, max_procs=max_procs)

        if new_package is not None and len(str(new_package)) > 0:
            current_package += new_package

        for i in range(len(current_package)):
            total_wallclock = sum_str_hours(total_wallclock, wallclock)
        if len(current_package) > 1:
            for level in range(1, len(current_package)):
                for job in current_package[level]:
                    job.level = level
        return JobPackageHorizontalVertical(current_package, max_procs, total_wallclock,
                                            jobs_resources=jobs_resources, configuration=self._as_config, wrapper_section=self.current_wrapper_section)

    def _build_vertical_horizontal_package(self, horizontal_packager, jobs_resources, wrapper_info):
        total_wallclock = '00:00'
        horizontal_package = horizontal_packager.build_horizontal_package(wrapper_info=wrapper_info)
        total_processors = horizontal_packager.total_processors
        current_package = []
        ## Create the vertical ##
        actual_wrapped_jobs = len(horizontal_package)
        for job in horizontal_package:
            for section in horizontal_packager.wrapper_limits["max_by_section"]:
                if job.section == section:
                    horizontal_packager.wrapper_limits["max_by_section"][section] = horizontal_packager.wrapper_limits["max_by_section"][section] - 1
        horizontal_packager.wrapper_limits["max"] = horizontal_packager.wrapper_limits["max"] - actual_wrapped_jobs
        for job in horizontal_package:
            dict_jobs = self._jobs_list.get_ordered_jobs_by_date_member(self.current_wrapper_section)
            job_list = JobPackagerVerticalMixed(dict_jobs, job, [job], job.wallclock,
                                                             horizontal_packager.wrapper_limits["max"], horizontal_packager.wrapper_limits,
                                                             self._platform.max_wallclock,wrapper_info=self.wrapper_info).build_vertical_package(job, wrapper_info)
            current_package.append(list(set(job_list)))

        for job in current_package[-1]:
            total_wallclock = sum_str_hours(total_wallclock, job.wallclock)
        if len(current_package) > 1:
            for level in range(1, len(current_package)):
                for job in current_package[level]:
                    job.level = level
        return JobPackageVerticalHorizontal(current_package, total_processors, total_wallclock,
                                            jobs_resources=jobs_resources, method=self.wrapper_method[self.current_wrapper_section], configuration=self._as_config, wrapper_section=self.current_wrapper_section )

#TODO rename and unite JobPackerVerticalMixed to JobPackerVertical since the difference between the two is not needed anymore
class JobPackagerVertical(object):
    """
    Vertical Packager Parent Class

    :param jobs_list: Usually there is only 1 job in this list. \n
    :type jobs_list: List() of Job Objects \n
    :param total_wallclock: Wallclock per object. \n
    :type total_wallclock: String  \n
    :param max_jobs: Maximum number of jobs per platform. \n
    :type max_jobs: Integer \n
    :param wrapper_limits: All wrapper limitations are inside this dictionary ( min,max,by_section,horizontal and vertical). \n
    :type wrapper_limits: Dict. \n
    :param max_wallclock: Value from Platform. \n
    :type max_wallclock: Integer

    """

    def __init__(self, jobs_list, total_wallclock, max_jobs, wrapper_limits, max_wallclock, wrapper_info):
        self.jobs_list = jobs_list
        self.total_wallclock = total_wallclock
        self.max_jobs = max_jobs
        self.wrapper_limits = wrapper_limits
        self.max_wallclock = max_wallclock
        self.wrapper_info = wrapper_info

    def build_vertical_package(self, job, wrapper_info):
        """
        Goes through the job and all the related jobs (children, or part of the same date member ordered group), finds those suitable
        and groups them together into a wrapper. (iterative-version)

        :param job: Job to be wrapped.
        :type job: Job Object
        :return: List of jobs that are wrapped together.
        :rtype: List() of Job Object
        """
        self.total_wallclock = job.wallclock # reset total wallclock for package
        stack = [(job, 1)]
        while stack:
            job, level = stack.pop()
            # Less verbose
            if level % 50 == 0 and level > 0:
                Log.info(f"Wrapper package creation is still ongoing. So far {level} jobs have been wrapped.")
                for event in job.platform.worker_events:  # keep alive log retrieval workers.
                    if not event.is_set():
                        event.set()

            if len(self.jobs_list) >= self.wrapper_limits["max_v"] or len(self.jobs_list) >= \
                    self.wrapper_limits["max_by_section"][job.section] or len(self.jobs_list) >= self.wrapper_limits[
                "max"]:
                continue
            child: Job = self.get_wrappable_child(job)
            if child is not None and len(str(child)) > 0:
                child.update_parameters(wrapper_info[-1], set_attributes=True)

                self.total_wallclock = sum_str_hours(self.total_wallclock, child.wallclock)
                # Local jobs could not have a wallclock defined
                if self.total_wallclock <= self.max_wallclock or not self.max_wallclock:
                    child.packed_during_building = True
                    child.level = level
                    self.jobs_list.append(child)
                    stack.append((child, level + 1))
        return self.jobs_list

    def get_wrappable_child(self, job):
        pass

    def _is_wrappable(self, job):
        """
        Determines if a job is wrappable. Basically, the job shouldn't have been packed already and the status must be READY or WAITING,
        Its parents should be COMPLETED.

        :param job: job to be evaluated. \n
        :type job: Job Object \n
        :return: True if wrappable, False otherwise. \n
        :rtype: Boolean
        """
        if not job.packed_during_building and job.status in [Status.WAITING, Status.READY, Status.PREPARED, Status.DELAYED]:
            for parent in job.parents:
                # First part of this conditional is true only if the parent is already on the wrapper package ( job_lists == current_wrapped jobs there )
                # Second part is actually relevant, parents of a wrapper should be COMPLETED
                if parent not in self.jobs_list and parent.status != Status.COMPLETED:
                    return False
            return True
        return False

class JobPackagerVerticalMixed(JobPackagerVertical):
    """
    Vertical Mixed Class. First statement of the constructor builds JobPackagerVertical.

    :param dict_jobs: Jobs sorted by date, member, RUNNING, and chunk number. Only those relevant to the wrapper. \n
    :type dict_jobs: Dictionary Key: date, Value: (Dictionary Key: Member, Value: List of jobs sorted) \n
    :param ready_job: Job to be wrapped. \n
    :type ready_job: Job Object \n
    :param jobs_list: ready_job as a list. \n
    :type jobs_list: List() of Job Object \n
    :param total_wallclock: wallclock time per job. \n
    :type total_wallclock: String \n
    :param max_jobs: Maximum number of jobs per platform. \n
    :type max_jobs: Integer \n
    :param wrapper_limits: All wrapper limitations are inside this dictionary ( min,max,by_section,horizontal and vertical). \n
    :type wrapper_limits: Dict. \n
    :param max_wallclock: Value from Platform. \n
    :type max_wallclock: String \n
    """

    def __init__(self, dict_jobs, ready_job, jobs_list, total_wallclock, max_jobs, wrapper_limits, max_wallclock,wrapper_info={}):
        super(JobPackagerVerticalMixed, self).__init__(
            jobs_list, total_wallclock, max_jobs, wrapper_limits, max_wallclock, wrapper_info)
        self.ready_job = ready_job
        self.dict_jobs = dict_jobs
        # Last date from the ordering
        date = list(dict_jobs.keys())[-1]
        # Last member from the last date from the ordering
        member = list(dict_jobs[date].keys())[-1]
        # If job to be wrapped has date and member, use those
        if ready_job.date is not None and len(str(ready_job.date)) > 0:
            date = ready_job.date
        if ready_job.member is not None and len(str(ready_job.member)) > 0:
            member = ready_job.member
        # Extract list of sorted jobs per date and member
        self.sorted_jobs = dict_jobs[date][member]
        self.index = 0


    def get_wrappable_child(self, job: Job) -> Job:
        """
        Goes through the jobs with the same date and member as the input job, and returns the first that satisfies self._is_wrappable().

        Args:
            job (Job): Job to be evaluated.

        Returns:
            Optional[Any]: Job that is wrappable, or None if no such job is found.
        """
        sorted_jobs = self.sorted_jobs
        child = None
        for index in range(self.index, len(sorted_jobs)):
            child_ = sorted_jobs[index]
            if child_.name != job.name and self._is_wrappable(child_):
                child = child_
                self.index = index + 1
                break
        return child

    def _is_wrappable(self, job):
        """
        Determines if a job is wrappable. Basically, the job shouldn't have been packed already and the status must be READY or WAITING,
        Its parents should be COMPLETED.

        :param job: job to be evaluated. \n
        :type job: Job Object \n
        :return: True if wrappable, False otherwise. \n
        :rtype: Boolean
        """
        if not job.packed_during_building and job.status in [Status.WAITING, Status.READY, Status.PREPARED, Status.DELAYED]:
            for parent in job.parents:
                # First part of this conditional is true only if the parent is already on the wrapper package ( job_lists == current_wrapped jobs there )
                # Second part is actually relevant, parents of a wrapper should be COMPLETED
                if parent not in self.jobs_list and parent.status != Status.COMPLETED:
                    return False
            return True
        return False


class JobPackagerHorizontal(object):
    def __init__(self, job_list, max_processors, wrapper_limits, max_jobs, processors_node, method="ASThread"):
        self.processors_node = processors_node
        self.max_processors = max_processors
        self.wrapper_limits = wrapper_limits
        self.job_list = job_list
        self.max_jobs = max_jobs
        self._current_processors = 0
        self._sort_order_dict = dict()
        self._components_dict = dict()
        self._section_processors = dict()
        self.method = method

        self._maxTotalProcessors = 0
        self._sectionList = list()
        self._package_sections = dict()
        self.wrapper_info = []

    def build_horizontal_package(self, horizontal_vertical=False,wrapper_info=[]):
        self.wrapper_info = wrapper_info
        current_package = []
        current_package_by_section = {}
        if horizontal_vertical:
            self._current_processors = 0
        jobs_by_section = dict()
        for job in self.job_list:
            job.update_parameters(self.wrapper_info[-1], set_attributes=True)
            if job.section not in jobs_by_section:
                jobs_by_section[job.section] = list()
            jobs_by_section[job.section].append(job)
        Log.info(f"Building horizontal package")
        jobs_processed = 0
        for section in jobs_by_section:
            current_package_by_section[section] = 0
            for job in jobs_by_section[section]:
                if jobs_processed % 50 == 0 and jobs_processed > 0:
                    Log.info(f"Wrapper package creation is still ongoing. So far {jobs_processed} jobs have been wrapped.")
                    for event in job.platform.worker_events:  # keep alive log retrieval workers.
                        if not event.is_set():
                            event.set()
                if str(job.processors).isdigit() and str(job.nodes).isdigit() and int(job.nodes) > 0 and int(job.processors) <= 1:
                    job.processors = 0
                if job.total_processors == "":
                    job_total_processors = 0
                else:
                    job_total_processors = int(job.total_processors)
                if len(current_package) < self.wrapper_limits["max_h"] and len(current_package) < self.wrapper_limits["max"]  and current_package_by_section[section] < self.wrapper_limits["max_by_section"][section]:
                    if int(job.tasks) != 0 and int(job.tasks) != int(self.processors_node) and \
                            int(self.processors_node) < int(job_total_processors):
                        nodes = int(
                            ceil(job_total_processors / float(job.tasks)))
                        total_processors = int(self.processors_node) * nodes
                    else:
                        total_processors = job_total_processors
                    if self.max_processors == -1 or (self._current_processors + total_processors) <= int(self.max_processors):
                        job.packed_during_building = True
                        current_package.append(job)
                        self._current_processors += total_processors
                        current_package_by_section[section] += 1
                else:
                    Log.result(f"Wrapper package creation is finished. {jobs_processed} jobs have been wrapped together.")
                    break
                jobs_processed += 1

        self.create_components_dict()

        return current_package

    def create_sections_order(self, jobs_sections):
        for i, section in enumerate(jobs_sections.split('&')):
            self._sort_order_dict[section] = i

    # EXIT FALSE IF A SECTION EXIST AND HAVE LESS PROCESSORS
    def add_sectioncombo_processors(self, total_processors_section):
        keySection = ""

        self._sectionList.sort()
        for section in self._sectionList:
            keySection += str(section)
        if keySection in self._package_sections:
            if self._package_sections[keySection] < total_processors_section:
                return False
        else:
            self._package_sections[keySection] = total_processors_section
        self._maxTotalProcessors = max(
            max(self._package_sections.values()), self._maxTotalProcessors)
        return True

    def sort_by_expression(self, section):
        return self._sort_order_dict[section]

    def get_next_packages(self, jobs_sections, max_wallclock=None, potential_dependency=None, packages_remote_dependencies=list(), horizontal_vertical=False, max_procs=0):
        packages = []
        job = max(self.job_list, key=attrgetter('total_wallclock'))
        wallclock = job.wallclock
        total_wallclock = wallclock

        while self.max_jobs > 0:
            next_section_list = []
            for job in self.job_list:
                for child in job.children:
                    if job.section == child.section or (job.section in jobs_sections and child.section in jobs_sections.split("&")) \
                            and child.status in [Status.READY, Status.WAITING]:
                        wrappable = True
                        for other_parent in child.parents:
                            if other_parent.status != Status.COMPLETED and other_parent not in self.job_list:
                                wrappable = False
                        if wrappable and child not in next_section_list:
                            next_section_list.append(child)

            next_section_list.sort(
                key=lambda job: self.sort_by_expression(job.section))
            self.job_list = next_section_list
            package_jobs = self.build_horizontal_package(horizontal_vertical, wrapper_info=self.wrapper_info)

            if package_jobs:
                sections_aux = set()
                wallclock = package_jobs[0].wallclock
                for job in package_jobs:
                    if job.section not in sections_aux:
                        sections_aux.add(job.section)
                        if job.wallclock > wallclock:
                            wallclock = job.wallclock
                if self._current_processors > max_procs:
                    return packages
                if max_wallclock:
                    total_wallclock = sum_str_hours(total_wallclock, wallclock)
                    if total_wallclock > max_wallclock:
                        return packages
                packages.append(package_jobs)

            else:
                break

        return packages

    @property
    def total_processors(self):
        return self._current_processors

    @property
    def components_dict(self):
        return self._components_dict

    def create_components_dict(self):
        self._sectionList = []
        # it was job.parameters
        parameters = {}  # TODO machinefiles, can wait nobody is using it and I really think this was not working before anyway
        for job in self.job_list:
            if job.section not in self._sectionList:
                self._sectionList.append(job.section)
            if job.section not in self._components_dict:
                self._components_dict[job.section] = dict()
                self._components_dict[job.section]['COMPONENTS'] = {parameter: job.parameters[parameter]
                                                                    for parameter in list(parameters.keys())
                                                                    if '_NUMPROC' in parameter}
