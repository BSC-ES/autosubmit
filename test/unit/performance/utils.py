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


class Utils:
    """
    Utility class for common operations.
    """

    @staticmethod
    def get_SYPD(start_timestamp: int, finish_timestamp: int, chunk_size: str, chunk_size_unit: str) -> float:
        """
        Calculate the simulated time in years based on start and finish timestamps, chunk size, and unit.

        :param start_timestamp: The start timestamp in seconds.
        :type start_timestamp: int
        :param finish_timestamp: The finish timestamp in seconds.
        :type finish_timestamp: int
        :param chunk_size: The size of the chunk in the specified unit.
        :type chunk_size: str
        :param chunk_size_unit: The unit of the chunk size (e.g., 'year', 'month', 'day').
        :type chunk_size_unit: str

        :return: The simulated time in the specified unit.
        :rtype: float
        """
        duration_seconds = finish_timestamp - start_timestamp
        if chunk_size_unit == "year":
            return duration_seconds / (365 * 24 * 3600) * float(chunk_size)
        elif chunk_size_unit == "month":
            return duration_seconds / (30 * 24 * 3600) * float(chunk_size)
        elif chunk_size_unit == "day":
            return duration_seconds / (24 * 3600) * float(chunk_size)
        raise ValueError(f"Unsupported chunk size unit: {chunk_size_unit}")

    @staticmethod
    def get_under_performance(value: float, threshold: float) -> float:
        """
        Calculate the percentage under performance based on the value and threshold.
        
        :param value: The actual value to compare against the threshold.
        :type value: float 

        :param threshold: The threshold value to compare against.
        :type threshold: float

        :return: The percentage of underperformance, or 0.0 if the value exceeds the threshold.
        :rtype: float
        """
        if value > threshold:
            return 0.0
        return (threshold - value) / threshold * 100
