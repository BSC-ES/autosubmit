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
        
        Args:
            start_timestamp: Start timestamp in Unix format.
            finish_timestamp: Finish timestamp in Unix format.
            chunk_size: Size of the chunk (e.g., '12').
            chunk_size_unit: Unit of the chunk size (e.g., 'month').
        
        Returns:
            float: Simulated time in years.
        """
        duration_seconds = finish_timestamp - start_timestamp
        if chunk_size_unit == 'year':
            return duration_seconds / (365 * 24 * 3600) * float(chunk_size)
        elif chunk_size_unit == 'month':
            return duration_seconds / (30 * 24 * 3600) * float(chunk_size)
        elif chunk_size_unit == 'day':
            return duration_seconds / (24 * 3600) * float(chunk_size)
        else:
            raise ValueError(f"Unsupported chunk size unit: {chunk_size_unit}")
        
    @staticmethod
    def get_under_performance(value: float, threshold: float) -> float:
        """
        Calculate the percentage under performance based on the value and threshold.
        Args:
            value: The computed value of the metric.
            threshold: The threshold value for comparison.
        Returns:
            float: Percentage under performance.
        """
        if value > threshold:
            return 0.0
        return (threshold - value) / threshold * 100
