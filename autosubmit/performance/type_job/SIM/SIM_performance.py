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
from autosubmit.log.log import Log
from autosubmit.performance.base_performance import BasePerformance,PerformanceMetricInfo

if TYPE_CHECKING:
    from autosubmit.job.job import Job


class SIMPerformance(BasePerformance):
    """
    Class to compute performance metrics for SIM jobs.
    """

    # Metrics thresholds
    SYPD_THRESHOLD: float = 1  # Threshold for SYPD to consider a job as "fast"

    # Errors management for each metric

    def _manage_errors_computation_SYPD(start_timestamp, finish_timestamp, chunk_size, chunk_size_unit):
        """
        Manage errors in the computation of SYPD.

        :param start_timestamp: Start time of the job in Unix timestamp.
        :type start_timestamp: intu
        :param finish_timestamp: Finish time of the job in Unix timestamp.
        :type finish_timestamp: int
        :param chunk_size: Size of the chunk as a string (e.g., '12').
        :type chunk_size: str
        :param chunk_size_unit: Unit of the chunk size as a string (e.g., 'month').
        :type chunk_size_unit: str

        :raises ValueError: If any of the required parameters are missing or invalid.
        :raises TypeError: If the parameters are not of the expected type.
        """

        if not start_timestamp:
            raise ValueError("start_timestamp must be set.")

        if not finish_timestamp:
            raise ValueError("finish_timestamp must be set.")

        if not chunk_size:
            raise ValueError("chunk_size must be set.")

        if not chunk_size_unit:
            raise ValueError("chunk_size_unit must be set.")

        if not isinstance(start_timestamp, int):
            raise TypeError("start_timestamp must be an integer representing Unix timestamp.")

        if not isinstance(finish_timestamp, int):
            raise TypeError("finish_timestamp must be an integer representing Unix timestamp.")

        if finish_timestamp <= start_timestamp:
            raise ValueError(f"Finish timestamp must be greater than start timestamp.")
        
        if not isinstance(chunk_size, int):
            raise TypeError("chunk_size must be an integer representing the size of the chunk (e.g., 12).")

        if chunk_size <= 0:
            raise ValueError("chunk_size must be a positive numeric string (e.g., '12').")

        if not isinstance(chunk_size_unit, str):
            raise TypeError("chunk_size_unit must be a string representing the unit of the chunk size (e.g., 'month').")

    # Computation and check of SYPD

    @staticmethod
    def compute_sypd_from_job(job: "Job") -> float:
        """
        Calculate the simulated time in years based on start and finish timestamps, chunk size, and unit.

        :param job: Job instance containing the necessary attributes.
        :type job: Job

        :return: Simulated time in years.
        :rtype: float
        """

        start_timestamp = job.start_time_timestamp
        finish_timestamp = job.finish_time_timestamp
        chunk_size = job.chunk_length
        chunk_size_unit = job.chunk_unit

        Log.info(f"Computing SYPD for job {job.name} with start timestamp {start_timestamp}, finish timestamp {finish_timestamp}, chunk size {chunk_size}, and chunk size unit {chunk_size_unit}.")

        SIMPerformance._manage_errors_computation_SYPD(start_timestamp, finish_timestamp, chunk_size, chunk_size_unit)

        duration_seconds = finish_timestamp - start_timestamp

        if chunk_size_unit == "year":
            return duration_seconds / (365 * 24 * 3600) * int(chunk_size)
        elif chunk_size_unit == "month":
            return duration_seconds / (30 * 24 * 3600) * int(chunk_size)
        elif chunk_size_unit == "day":
            return duration_seconds / (24 * 3600) * int(chunk_size)
        elif chunk_size_unit == "hour":
            return duration_seconds / 3600 * int(chunk_size)
        raise ValueError(f"Unsupported chunk size unit: {chunk_size_unit}")

    def compute_and_check_SYPD_threshold(self, job: "Job") -> PerformanceMetricInfo:
        """
        Compute the SYPD from a job and check if it is above the threshold.
        Moreover, send an email notification if the SYPD is under the threshold.

        :param job: Job instance containing the necessary attributes.
        :type job: Job

        :return: PerformanceMetricInfo instance containing the SYPD metric details.
        :rtype: PerformanceMetricInfo
        """

        sypd = SIMPerformance.compute_sypd_from_job(job)
        under_threshold = sypd < self.SYPD_THRESHOLD

        return PerformanceMetricInfo(
            metric="SYPD",
            under_threshold=under_threshold,
            value=sypd,
            threshold=self.SYPD_THRESHOLD,
            under_performance=(
                (self.SYPD_THRESHOLD - sypd) / self.SYPD_THRESHOLD * 100
                if under_threshold
                else None
            ),
        )

    # Computation and check of performance metrics

    def compute_and_check_performance_metrics(self, job: "Job") -> list[PerformanceMetricInfo]:
        """
        Compute the performance metrics for a job and check if it is under a threshold.

        :param job: Job instance containing the necessary attributes.
        :type job: Job

        :return: A list of PerformanceMetricInfo instances containing metric details.
        :rtype: list[PerformanceMetricInfo]
        """

        message_parts = ["🚨 Performance Alert\n===================\n"]

        SYPD_info = self.compute_and_check_SYPD_threshold(job)
        if SYPD_info.under_threshold:
            message_parts.append(self._template_metric_message(SYPD_info, job))

        if len(message_parts) > 1:
            message_parts.append(
                "\nℹ️ This notification was auto-generated by Autosubmit."
            )
            complete_message = "\n".join(message_parts)
            subject = f"[Autosubmit] Performance Alert for Job: {job.name}"
            mail_to = self._get_mail_recipients()
            self._mail_notifier.notify_custom_alert(subject, mail_to, complete_message)

        return [SYPD_info]
