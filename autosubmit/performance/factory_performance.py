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

from typing import TYPE_CHECKING
from autosubmit.job.job_common import Status
from autosubmit.performance.base_performance import BasePerformance
from autosubmit.performance.utils import UtilsPerformance

if TYPE_CHECKING:
    from autosubmit.job.job import Job


class PerformanceFactory:
    
    def create_performance(self, job: "Job", performance_config: dict[str, any]) -> BasePerformance:
        """
        Factory method to create a performance metric calculator for a job.

        :param job: Job instance containing the necessary attributes.
        :type job: Job

        :param performance_config: Autosubmit configuration containing performance settings.
        :type performance_config: dict[str, any]

        :return: An instance of a class derived from BasePerformance.
        :rtype: BasePerformance
        """

        if not performance_config:
            raise ValueError(
                "Performance configuration is not set."
            )

        project = performance_config.get("PROJECT", {})

        if not project:
            raise ValueError("Project configuration is not set in the performance configuration.")

        sections = performance_config.get("SECTION", [])

        if not sections:
            raise ValueError("Sections configuration is not set in the performance configuration.")
        
        notify_on = performance_config.get("NOTIFY_ON", [])

        if not notify_on:
            raise ValueError("Notify on configuration is not set in the performance configuration.")

        job_in_sections = job.section in sections if sections else False

        job_status_string = Status.VALUE_TO_KEY.get(job.status, 'UNKNOWN')
        job_in_status = job_status_string in notify_on if notify_on else False

        if not job_in_sections:
            return None

        if not job_in_status:
            return None

        return self._create_performance_by_type(f"{job.section}_{project}", performance_config)

    @staticmethod
    def _create_performance_by_type(job_type: str, performance_config: dict[str, any]) -> BasePerformance:
        """
        Create a performance calculator based on the job type.

        :param job_type: Type of the job, e.g., "SIM_DESTINE" or "SIM_DEFAULT".
        :type job_type: str

        :param performance_config: Autosubmit configuration containing performance settings.
        :type performance_config: dict[str, any]

        :return: An instance of a class derived from BasePerformance specific to the job type.
        :rtype: BasePerformance
        """

        if job_type == "SIM_DESTINE":
            from autosubmit.performance.type_job.SIM.project.SIM_DestinE import (
                SIMDestinEPerformance,
            )

            return SIMDestinEPerformance(performance_config, UtilsPerformance.get_mail_notifier())
        elif job_type == "SIM_DEFAULT":
            from autosubmit.performance.type_job.SIM.SIM_performance import (
                SIMPerformance,
            )

            return SIMPerformance(performance_config, UtilsPerformance.get_mail_notifier())

        raise ValueError(
            f"Unsupported job type: {job_type}. Cannot create performance calculator."
        )
