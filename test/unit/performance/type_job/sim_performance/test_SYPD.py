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

import pytest 

from autosubmit.job.job import Job
from autosubmit.performance.type_job.SIM.SIM_performance import SIMPerformance
from test.unit.performance.support_job import JobTestPerformance
from test.unit.performance.utils import Utils

class TestSIMPerformanceSYPDNaive:
    """
    Test class for the SYPD (Seconds per Year per Day) computation
    using a naive Job instance.
    """

    def test_computation_SYPD(self, sim_performance: SIMPerformance, utils: Utils, naive_job: Job): 
        """
        Test to verify the computation of SYPD (Seconds per Year per Day) for a Job.
        """
        expected_sypd = utils.get_SYPD(
            start_timestamp=naive_job.start_time_timestamp,
            finish_timestamp=naive_job.finish_time_timestamp,
            chunk_size=naive_job.parameters['EXPERIMENT']['CHUNKSIZE'],
            chunk_size_unit=naive_job.parameters['EXPERIMENT']['CHUNKSIZEUNIT']
        )
        sypd = sim_performance.compute_sypd_from_job(naive_job) 

        assert sypd == expected_sypd, f"Expected SYPD: {expected_sypd}, but got: {sypd}"

    def test_no_start_timestamp(self, sim_performance: SIMPerformance, naive_job: Job):
        """
        Test to verify that an error is raised when start_timestamp is not set.
        """
        naive_job.start_time_timestamp = None
        with pytest.raises(ValueError) as exc_info:
            sim_performance.compute_sypd_from_job(naive_job)
        assert str(exc_info.value) == "Job must have start_time_timestamp, finish_time_timestamp, parameters ['EXPERIMENT']['CHUNKSIZE'] and parameters ['EXPERIMENT']['CHUNKSIZEUNIT'] set."

    def test_no_finish_timestamp(self, sim_performance: SIMPerformance, naive_job: Job):
        """
        Test to verify that an error is raised when finish_timestamp is not set.
        """

        naive_job.finish_time_timestamp = None
        with pytest.raises(ValueError) as exc_info:
            sim_performance.compute_sypd_from_job(naive_job)
        assert str(exc_info.value) == "Job must have start_time_timestamp, finish_time_timestamp, parameters ['EXPERIMENT']['CHUNKSIZE'] and parameters ['EXPERIMENT']['CHUNKSIZEUNIT'] set."

    def test_no_chunk_size(self, sim_performance: SIMPerformance, naive_job: Job):
        """
        Test to verify that an error is raised when chunk_size is not set.
        """
        naive_job.parameters['EXPERIMENT']['CHUNKSIZE'] = None
        with pytest.raises(ValueError) as exc_info:
            sim_performance.compute_sypd_from_job(naive_job)
        assert str(exc_info.value) == "Job must have start_time_timestamp, finish_time_timestamp, parameters ['EXPERIMENT']['CHUNKSIZE'] and parameters ['EXPERIMENT']['CHUNKSIZEUNIT'] set."

    def test_no_chunk_size_unit(self, sim_performance: SIMPerformance, naive_job: Job):
        """
        Test to verify that an error is raised when chunk_size_unit is not set.
        """
        naive_job.parameters['EXPERIMENT']['CHUNKSIZEUNIT'] = None
        with pytest.raises(ValueError) as exc_info:
            sim_performance.compute_sypd_from_job(naive_job)
        assert str(exc_info.value) == "Job must have start_time_timestamp, finish_time_timestamp, parameters ['EXPERIMENT']['CHUNKSIZE'] and parameters ['EXPERIMENT']['CHUNKSIZEUNIT'] set."

    def test_invalid_start_timestamp_type(self, sim_performance: SIMPerformance, naive_job: Job):
        """
        Test to verify that an error is raised when start_timestamp is not an integer.
        """
        naive_job.start_time_timestamp = "invalid_timestamp"
        with pytest.raises(TypeError) as exc_info:
            sim_performance.compute_sypd_from_job(naive_job)
        assert str(exc_info.value) == "start_timestamp must be an integer representing Unix timestamp."

    def test_invalid_finish_timestamp_type(self, sim_performance: SIMPerformance, naive_job: Job):
        """
        Test to verify that an error is raised when finish_timestamp is not an integer.
        """
        naive_job.finish_time_timestamp = "invalid_timestamp"
        with pytest.raises(TypeError) as exc_info:
            sim_performance.compute_sypd_from_job(naive_job)
        assert str(exc_info.value) == "finish_timestamp must be an integer representing Unix timestamp."

    def test_invalid_chunk_size_type(self, sim_performance: SIMPerformance, naive_job: Job):
        """
        Test to verify that an error is raised when chunk_size is not a string.
        """
        naive_job.parameters['EXPERIMENT']['CHUNKSIZE'] = 12
        with pytest.raises(TypeError) as exc_info:
            sim_performance.compute_sypd_from_job(naive_job)
        assert str(exc_info.value) == "chunk_size must be a string representing the size of the chunk (e.g., '12')."

    def test_invalid_chunk_size_unit_type(self, sim_performance: SIMPerformance, naive_job: Job):
        """
        Test to verify that an error is raised when chunk_size_unit is not a string.
        """
        naive_job.parameters['EXPERIMENT']['CHUNKSIZEUNIT'] = 12
        with pytest.raises(TypeError) as exc_info:
            sim_performance.compute_sypd_from_job(naive_job)
        assert str(exc_info.value) == "chunk_size_unit must be a string representing the unit of the chunk size (e.g., 'month')."

    def test_invalid_chunk_size_value(self, sim_performance: SIMPerformance, naive_job: Job):
        """
        Test to verify that an error is raised when chunk_size is not a numeric string.
        """
        naive_job.parameters['EXPERIMENT']['CHUNKSIZE'] = "invalid_chunk_size"
        with pytest.raises(ValueError) as exc_info:
            sim_performance.compute_sypd_from_job(naive_job)
        assert str(exc_info.value) == "chunk_size must be a positive numeric string (e.g., '12')."

    def test_negative_chunk_size_unit_value(self, sim_performance: SIMPerformance, naive_job: Job):
        """
        Test to verify that an error is raised when chunk_size is a negative numeric string.
        """
        naive_job.parameters['EXPERIMENT']['CHUNKSIZE'] = "-12"
        with pytest.raises(ValueError) as exc_info:
            sim_performance.compute_sypd_from_job(naive_job)
        assert str(exc_info.value) == "chunk_size must be a positive numeric string (e.g., '12')."


class TestPerformanceSYPDCheckThreshold:
    """
    Test class for the SYPD (Seconds per Year per Day) threshold check
    using a Job instance.
    """

    @pytest.fixture
    def job_above_threshold(self, job_factory):
        """
        Fixture to create a Job instance with SYPD above the threshold.
        """

        test_job = JobTestPerformance(
            name="test_job_001",
            status="COMPLETED",
            section="SIM",
            chunk_size="12",
            chunk_size_unit="hour"
        )

        test_job.set_start_timestamp("2025-07-17 14:30:25")
        test_job.set_finish_timestamp("2025-07-17 14:35:25")
        
        return job_factory.create_job(test_job)

    def test_sypd_threshold_above(self, sim_performance: SIMPerformance, job_above_threshold: Job):
        """
        Test to verify that the SYPD threshold is above the defined limit.
        """
        sypd_metric_info = sim_performance.compute_and_check_SYPD_threshold(job_above_threshold)
        sypd = sypd_metric_info.value
        metric = sypd_metric_info.metric
        threshold_sypd = sypd_metric_info.threshold
        under_threshold = sypd_metric_info.under_threshold
        under_performance = sypd_metric_info.under_performance
        assert metric == "SYPD", "Metric name should be 'SYPD'."
        assert under_threshold is False, f"SYPD should be above threshold. SYPD: {sypd} is lower than threshold: {threshold_sypd}."
        assert under_performance is None, "Under performance should not be calculated when SYPD is above threshold."

    @pytest.fixture
    def job_below_threshold(self, job_factory):
        """
        Fixture to create a Job instance with SYPD below the threshold.
        """
        test_job = JobTestPerformance(
            name="test_job_002",
            status="COMPLETED",
            section="SIM",
            chunk_size="1",
            chunk_size_unit="month"
        )

        test_job.set_start_timestamp("2025-07-17 14:30:25")
        test_job.set_finish_timestamp("2025-07-17 15:30:25")
        
        return job_factory.create_job(test_job)
    
    def test_sypd_threshold_below(self, sim_performance: SIMPerformance, job_below_threshold: Job):
        """
        Test to verify that the SYPD threshold is below the defined limit.
        """
        sypd_metric_info = sim_performance.compute_and_check_SYPD_threshold(job_below_threshold)
        sypd = sypd_metric_info.value
        threshold_sypd = sypd_metric_info.threshold
        under_threshold = sypd_metric_info.under_threshold
        under_performance = sypd_metric_info.under_performance

        assert under_threshold is True, f"SYPD should be below threshold. SYPD: {sypd} is greater than threshold: {threshold_sypd}."
        assert under_performance is not None, "Under performance should be calculated when SYPD is below threshold."


class TestPerformanceSYPDExtremeCases: 
    """
    Test class for the SYPD (Seconds per Year per Day) computation
    using extreme Job instances.
    """

    @pytest.fixture
    def job_with_zero_duration(self, job_factory):
        """
        Fixture to create a Job instance with zero duration.
        """
        test_job = JobTestPerformance(
            name="test_job_003",
            status="COMPLETED",
            section="SIM",
            chunk_size="1",
            chunk_size_unit="day"
        )

        test_job.set_start_timestamp("2025-07-17 14:30:25")
        test_job.set_finish_timestamp("2025-07-17 14:30:25")

        return job_factory.create_job(test_job)
    
    def test_sypd_zero_duration(self, sim_performance: SIMPerformance, job_with_zero_duration: Job):
        """
        Test to verify that SYPD is computed correctly for a Job with zero duration.
        """
        with pytest.raises(ValueError) as exc_info:
            sim_performance.compute_sypd_from_job(job_with_zero_duration)
        assert str(exc_info.value) == "Finish timestamp must be greater than start timestamp."

    @pytest.fixture
    def job_with_negative_duration(self, job_factory):
        """
        Fixture to create a Job instance with negative duration.
        """
        test_job = JobTestPerformance(
            name="test_job_004",
            status="COMPLETED",
            section="SIM",
            chunk_size="1",
            chunk_size_unit="day"
        )

        test_job.set_start_timestamp("2025-07-17 14:30:25")
        test_job.set_finish_timestamp("2025-07-17 14:20:25")

        return job_factory.create_job(test_job)

    def test_sypd_negative_duration(self, sim_performance: SIMPerformance, job_with_negative_duration: Job):
        """
        Test to verify that SYPD raises an error for a Job with negative duration.
        """
        with pytest.raises(ValueError) as exc_info:
            sim_performance.compute_sypd_from_job(job_with_negative_duration)
        assert str(exc_info.value) == "Finish timestamp must be greater than start timestamp."

    @pytest.fixture
    def job_with_invalid_chunk_size(self, job_factory):
        """
        Fixture to create a Job instance with an invalid chunk size.
        """
        test_job = JobTestPerformance(
            name="test_job_005",
            status="COMPLETED",
            section="SIM",
            chunk_size="invalid_chunk_size",
            chunk_size_unit="day"
        )

        test_job.set_start_timestamp("2025-07-17 14:30:25")
        test_job.set_finish_timestamp("2025-07-17 15:30:25")

        return job_factory.create_job(test_job)

    def test_sypd_invalid_chunk_size(self, sim_performance: SIMPerformance, job_with_invalid_chunk_size: Job):
        """
        Test to verify that SYPD raises an error for a Job with an invalid chunk size.
        """
        with pytest.raises(ValueError) as exc_info:
            sim_performance.compute_sypd_from_job(job_with_invalid_chunk_size)
        assert str(exc_info.value) == "chunk_size must be a positive numeric string (e.g., '12')."

    @pytest.fixture
    def job_with_invalid_chunk_size_unit(self, job_factory):
        """
        Fixture to create a Job instance with an invalid chunk size unit.
        """
        test_job = JobTestPerformance(
            name="test_job_006",
            status="COMPLETED",
            section="SIM",
            chunk_size="12",
            chunk_size_unit="invalid_unit"
        )

        test_job.set_start_timestamp("2025-07-17 14:30:25")
        test_job.set_finish_timestamp("2025-07-17 15:30:25")

        return job_factory.create_job(test_job)

    def test_sypd_invalid_chunk_size_unit(self, sim_performance: SIMPerformance, job_with_invalid_chunk_size_unit: Job):
        """
        Test to verify that SYPD raises an error for a Job with an invalid chunk size unit.
        """
        with pytest.raises(ValueError) as exc_info:
            sim_performance.compute_sypd_from_job(job_with_invalid_chunk_size_unit)
        assert str(exc_info.value) == "Unsupported chunk size unit: invalid_unit"

    


