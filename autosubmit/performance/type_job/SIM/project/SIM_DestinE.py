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
from autosubmit.performance.base_performance import PerformanceMetricInfo
from autosubmit.performance.type_job.SIM.SIM_performance import SIMPerformance

if TYPE_CHECKING:
    from autosubmit.job.job import Job

class SIMDestinEPerformance(SIMPerformance):
    """
    Class to compute performance metrics for SIM DestinE jobs.
    Inherits from SIMPerformance to reuse the SYPD calculation logic.
    """

    SYPD_THRESHOLD: float = 2  # Threshold for SYPD to consider a job as "fast"
    
    def compute_and_check_performance_metrics(self, job: 'Job') -> list[PerformanceMetricInfo]:
        """
        Compute the performance metrics for a SIM DestinE job and check if it is under a threshold.

        Args:
            job: Job instance containing the necessary attributes.
        Returns:
            list: List of PerformanceMetricInfo instances containing the performance metrics.
        """
        return super().compute_and_check_performance_metrics(job)