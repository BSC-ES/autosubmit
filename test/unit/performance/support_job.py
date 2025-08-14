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

import datetime
from dataclasses import dataclass
from autosubmit.job.job import Job


@dataclass
class JobTestPerformance:
    """
    Class to represent a simplified job for testing purposes.

    The objective of this class is to only contain the necessary attributes
    that make the difference with a default Job.
    """

    name: str  # e.g. 'test_job'
    status: str  # e.g. 'COMPLETED'
    section: str  # e.g. 'SIM'
    chunk_size: str  # e.g. '12'
    chunk_size_unit: str  # e.g. 'year'
    start_timestamp: int = None  # Unix timestamp in seconds
    finish_timestamp: int = None  # Unix timestamp in seconds

    @staticmethod
    def parse_timestamp(timestamp_str: str) -> int:
        """
        Convert timestamp string to Unix timestamp (int).

        Args:
            timestamp_str: String in format "YYYY-MM-DD HH:MM:SS"

        Returns:
            int: Unix timestamp

        Example:
            parse_timestamp("2025-07-17 15:30:25") -> 1737123025
        """
        date = datetime.datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
        return int(date.timestamp())

    def set_start_timestamp(self, timestamp_str: str):
        """Set start timestamp from string format 'YYYY-MM-DD HH:MM:SS'."""
        self.start_timestamp = self.parse_timestamp(timestamp_str)

    def set_finish_timestamp(self, timestamp_str: str):
        """Set finish timestamp from string format 'YYYY-MM-DD HH:MM:SS'."""
        self.finish_timestamp = self.parse_timestamp(timestamp_str)


class TransformToJob:
    """
    Class to transform a JobTestPerformance into a Job object.
    """

    @staticmethod
    def transform(test_job: JobTestPerformance) -> Job:
        """
        Transform a JobTestPerformance into a Job object.

        Args:
            test_job: Instance of JobTestPerformance to transform.

        Returns:
            Job: Transformed Job object.
        """
        job = Job(name=test_job.name, status=test_job.status)

        job.start_time_timestamp = test_job.start_timestamp
        job.finish_time_timestamp = test_job.finish_timestamp

        job._section = test_job.section

        job.chunk_unit = test_job.chunk_size_unit
        job.chunk_length = test_job.chunk_size

        return job
