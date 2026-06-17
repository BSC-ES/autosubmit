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

from collections import namedtuple
from pathlib import Path

import os
import re
from autosubmit.history.utils import get_current_datetime
import pytest
import time
import traceback
from shutil import copy2
from sqlalchemy import create_engine

from autosubmit.history.experiment_history import ExperimentHistory
from autosubmit.history.internal_logging import Logging
from autosubmit.history.platform_monitor.slurm_monitor import SlurmMonitor
from autosubmit.history.strategies import StraightWrapperAssociationStrategy, GeneralizedWrapperDistributionStrategy, \
    PlatformInformationHandler
from autosubmit.config.basicconfig import BasicConfig
from test._oldschema import old_job_data_table, old_experiment_run_table

EXPID_TT00_SOURCE = "test_database.db~"
EXPID_TT01_SOURCE = "test_database_no_run.db~"
EXPID = "tt00"
EXPID_NONE = "tt01"
BasicConfig.read()
JOBDATA_DIR = BasicConfig.JOBDATA_DIR
LOCAL_ROOT_DIR = BasicConfig.LOCAL_ROOT_DIR
job = namedtuple("Job", ["name", "date", "member", "status_str", "children"])


def test_get_current_datetime():
    current_datetime = get_current_datetime()
    assert isinstance(current_datetime, str)
    pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{4}$"
    assert re.match(pattern, current_datetime) is not None


@pytest.mark.skip()
@pytest.mark.skip(
    'TODO: another test that uses actual data. See if there is anything useful, and extract into functional/integration/unit tests that run on any machine')
class TestExperimentHistory:
    # @classmethod
    # def setUpClass(cls):
    #   cls.exp = ExperimentHistory("tt00") # example database
    def setup_method(self):
        source_path_tt00 = os.path.join(JOBDATA_DIR, EXPID_TT00_SOURCE)
        self.target_path_tt00 = os.path.join(JOBDATA_DIR, f"job_data_{EXPID}.db")
        copy2(source_path_tt00, self.target_path_tt00)
        source_path_tt01 = os.path.join(JOBDATA_DIR, EXPID_TT01_SOURCE)
        self.target_path_tt01 = os.path.join(JOBDATA_DIR, f"job_data_{EXPID_NONE}.db")
        copy2(source_path_tt01, self.target_path_tt01)
        self.job_list = [
            job("a29z_20000101_fc2_1_POST", "2000-01-01 00:00:00", "POST", "COMPLETED", ""),
            job("a29z_20000101_fc1_1_CLEAN", "2000-01-01 00:00:00", "CLEAN", "COMPLETED", ""),
            job("a29z_20000101_fc3_1_POST", "2000-01-01 00:00:00", "POST", "RUNNING", ""),
            job("a29z_20000101_fc2_1_CLEAN", "2000-01-01 00:00:00", "CLEAN", "COMPLETED", ""),
            job("a29z_20000101_fc0_3_SIM", "2000-01-01 00:00:00", "SIM", "COMPLETED", ""),
            job("a29z_20000101_fc1_2_POST", "2000-01-01 00:00:00", "POST", "QUEUING", ""),
        ]  # 2 differences, all COMPLETED
        self.job_list_large = [
            job("a29z_20000101_fc2_1_POST", "2000-01-01 00:00:00", "POST", "COMPLETED", ""),
            job("a29z_20000101_fc1_1_CLEAN", "2000-01-01 00:00:00", "CLEAN", "COMPLETED", ""),
            job("a29z_20000101_fc3_1_POST", "2000-01-01 00:00:00", "POST", "RUNNING", ""),
            job("a29z_20000101_fc2_1_CLEAN", "2000-01-01 00:00:00", "CLEAN", "COMPLETED", ""),
            job("a29z_20000101_fc0_3_SIM", "2000-01-01 00:00:00", "SIM", "COMPLETED", ""),
            job("a29z_20000101_fc1_2_POST", "2000-01-01 00:00:00", "POST", "QUEUING", ""),
            job("a29z_20000101_fc1_5_POST", "2000-01-01 00:00:00", "POST", "SUSPENDED", ""),
            job("a29z_20000101_fc1_4_POST", "2000-01-01 00:00:00", "POST", "FAILED", ""),
            job("a29z_20000101_fc2_5_CLEAN", "2000-01-01 00:00:00", "CLEAN", "SUBMITTED", ""),
            job("a29z_20000101_fc0_1_POST", "2000-01-01 00:00:00", "POST", "RUNNING", ""),
        ]

    def teardown_method(self):
        os.remove(self.target_path_tt00)
        os.remove(self.target_path_tt01)

    def test_db_exists(self):
        exp_history = ExperimentHistory("tt00")
        exp_history.initialize_database()
        assert exp_history.manager.my_database_exists() is True
        exp_history = ExperimentHistory("tt99")
        assert exp_history.manager.my_database_exists() is False

    def test_is_header_ready(self):
        exp_history = ExperimentHistory("tt00")
        assert exp_history.is_header_ready() is True

    def test_detect_differences_job_list(self):
        exp_history = ExperimentHistory("tt00")
        exp_history.initialize_database()
        differences = exp_history.detect_changes_in_job_list(self.job_list)
        expected_differences = ["a29z_20000101_fc3_1_POST", "a29z_20000101_fc1_2_POST"]
        for job_dc in differences:
            assert job_dc.job_name in expected_differences
        assert len(differences) == 2

    def test_built_list_of_changes(self):
        exp_history = ExperimentHistory("tt00")
        exp_history.initialize_database()
        built_differences = exp_history._get_built_list_of_changes(self.job_list)
        expected_ids_differences = [90, 101]
        for item in built_differences:
            assert item[3] in expected_ids_differences

    def test_get_date_member_count(self):
        exp_history = ExperimentHistory("tt00")
        exp_history.initialize_database()
        dm_count = exp_history._get_date_member_completed_count(self.job_list)
        assert dm_count > 0

    def test_should_we_create_new_run(self):
        exp_history = ExperimentHistory("tt00")
        exp_history.initialize_database()
        CHANGES_COUNT = 1
        TOTAL_COUNT = 6
        current_experiment_run_dc = exp_history.manager.get_experiment_run_dc_with_max_id()
        current_experiment_run_dc.total = TOTAL_COUNT
        should_we = exp_history.should_we_create_a_new_run(self.job_list, CHANGES_COUNT, current_experiment_run_dc,
                                                           current_experiment_run_dc.chunk_unit,
                                                           current_experiment_run_dc.chunk_size)
        assert should_we is False
        TOTAL_COUNT_DIFF = 5
        current_experiment_run_dc.total = TOTAL_COUNT_DIFF
        should_we = exp_history.should_we_create_a_new_run(self.job_list, CHANGES_COUNT, current_experiment_run_dc,
                                                           current_experiment_run_dc.chunk_unit,
                                                           current_experiment_run_dc.chunk_size)
        assert should_we is True
        CHANGES_COUNT = 5
        should_we = exp_history.should_we_create_a_new_run(self.job_list, CHANGES_COUNT, current_experiment_run_dc,
                                                           current_experiment_run_dc.chunk_unit,
                                                           current_experiment_run_dc.chunk_size)
        assert should_we is True
        CHANGES_COUNT = 1
        current_experiment_run_dc.total = TOTAL_COUNT
        should_we = exp_history.should_we_create_a_new_run(self.job_list, CHANGES_COUNT, current_experiment_run_dc,
                                                           current_experiment_run_dc.chunk_unit,
                                                           current_experiment_run_dc.chunk_size * 20)
        assert should_we is True
        should_we = exp_history.should_we_create_a_new_run(self.job_list, CHANGES_COUNT, current_experiment_run_dc,
                                                           current_experiment_run_dc.chunk_unit,
                                                           current_experiment_run_dc.chunk_size)
        assert should_we is False
        should_we = exp_history.should_we_create_a_new_run(self.job_list, CHANGES_COUNT, current_experiment_run_dc,
                                                           "day", current_experiment_run_dc.chunk_size)
        assert should_we is True

    def test_status_counts(self):
        exp_history = ExperimentHistory("tt00")
        exp_history.initialize_database()
        result = exp_history.get_status_counts_from_job_list(self.job_list_large)
        assert result["COMPLETED"] == 4
        assert result["QUEUING"] == 1
        assert result["RUNNING"] == 2
        assert result["FAILED"] == 1
        assert result["SUSPENDED"] == 1
        assert result["TOTAL"] == len(self.job_list_large)

    def test_create_new_experiment_run_with_counts(self):
        exp_history = ExperimentHistory("tt00")
        exp_history.initialize_database()
        exp_run = exp_history.create_new_experiment_run(job_list=self.job_list)
        assert exp_run.chunk_size == 0
        assert exp_run.chunk_unit == "NA"
        assert exp_run.total == len(self.job_list)
        assert exp_run.completed == 4

    def test_finish_current_run(self):
        exp_history = ExperimentHistory("tt00")
        exp_history.initialize_database()
        exp_run = exp_history.finish_current_experiment_run()
        assert len(exp_run.modified) > 0
        assert exp_run.finish > 0

    def test_process_job_list_changes(self):
        exp_history = ExperimentHistory("tt00")
        exp_history.initialize_database()
        exp_run = exp_history.process_job_list_changes_to_experiment_totals(self.job_list)
        assert exp_run.total == len(self.job_list)
        assert exp_run.completed == 4
        assert exp_run.running == 1
        assert exp_run.queuing == 1

    def test_calculated_weights(self):
        exp_history = ExperimentHistory("tt00")
        exp_history.initialize_database()
        job_data_dcs = exp_history.manager.get_all_last_job_data_dcs()
        calculated_weights = GeneralizedWrapperDistributionStrategy().get_calculated_weights_of_jobs_in_wrapper(
            job_data_dcs)
        sum_comp_weight = 0
        for job_name in calculated_weights:
            sum_comp_weight += calculated_weights[job_name]
        assert abs(sum_comp_weight - 1) <= 0.01

    def test_distribute_energy_in_wrapper_1_to_1(self):
        exp_history = ExperimentHistory("tt00")
        exp_history.initialize_database()
        ssh_output = '''                 17857525  COMPLETED         10        1 2021-10-13T15:51:16 2021-10-13T15:51:17 2021-10-13T15:52:47         2.41K                                                     
           17857525.batch  COMPLETED         10        1 2021-10-13T15:51:17 2021-10-13T15:51:17 2021-10-13T15:52:47          1.88K                     6264K                     6264K 
          17857525.extern  COMPLETED         10        1 2021-10-13T15:51:17 2021-10-13T15:51:17 2021-10-13T15:52:47          1.66K                      473K                       68K 
               17857525.0  COMPLETED         10        1 2021-10-13T15:51:21 2021-10-13T15:51:21 2021-10-13T15:51:22            186                      352K                   312.30K 
               17857525.1  COMPLETED         10        1 2021-10-13T15:51:23 2021-10-13T15:51:23 2021-10-13T15:51:24            186                      420K                   306.70K 
               17857525.2  COMPLETED         10        1 2021-10-13T15:51:24 2021-10-13T15:51:24 2021-10-13T15:51:27            188                      352K                   325.80K 
               17857525.3  COMPLETED         10        1 2021-10-13T15:51:28 2021-10-13T15:51:28 2021-10-13T15:51:29            192                      352K                   341.90K                
    '''
        slurm_monitor = SlurmMonitor(ssh_output)
        job_data_dcs = exp_history.manager.get_all_last_job_data_dcs()[:4]  # Get me 4 jobs
        weights = StraightWrapperAssociationStrategy().get_calculated_weights_of_jobs_in_wrapper(job_data_dcs)
        info_handler = PlatformInformationHandler(StraightWrapperAssociationStrategy())
        job_data_dcs_with_data = info_handler.execute_distribution(job_data_dcs[0], job_data_dcs, slurm_monitor)
        assert job_data_dcs_with_data[0].energy == round(
            slurm_monitor.steps[0].energy + weights[job_data_dcs_with_data[0].job_name] * slurm_monitor.extern.energy,
            2)
        assert job_data_dcs_with_data[0].MaxRSS == slurm_monitor.steps[0].MaxRSS
        assert job_data_dcs_with_data[2].energy == round(
            slurm_monitor.steps[2].energy + weights[job_data_dcs_with_data[2].job_name] * slurm_monitor.extern.energy,
            2)
        assert job_data_dcs_with_data[2].AveRSS == slurm_monitor.steps[2].AveRSS

    def test_distribute_energy_in_wrapper_general_case(self):
        exp_history = ExperimentHistory("tt00")
        exp_history.initialize_database()
        ssh_output = '''                 17857525  COMPLETED         10        1 2021-10-13T15:51:16 2021-10-13T15:51:17 2021-10-13T15:52:47         2.41K                                                     
           17857525.batch  COMPLETED         10        1 2021-10-13T15:51:17 2021-10-13T15:51:17 2021-10-13T15:52:47          1.88K                     6264K                     6264K 
          17857525.extern  COMPLETED         10        1 2021-10-13T15:51:17 2021-10-13T15:51:17 2021-10-13T15:52:47          1.66K                      473K                       68K 
               17857525.0  COMPLETED         10        1 2021-10-13T15:51:21 2021-10-13T15:51:21 2021-10-13T15:51:22            186                      352K                   312.30K 
               17857525.1  COMPLETED         10        1 2021-10-13T15:51:23 2021-10-13T15:51:23 2021-10-13T15:51:24            186                      420K                   306.70K 
               17857525.2  COMPLETED         10        1 2021-10-13T15:51:24 2021-10-13T15:51:24 2021-10-13T15:51:27            188                      352K                   325.80K 
               17857525.3  COMPLETED         10        1 2021-10-13T15:51:28 2021-10-13T15:51:28 2021-10-13T15:51:29            192                      352K                   341.90K                
    '''
        slurm_monitor = SlurmMonitor(ssh_output)
        job_data_dcs = exp_history.manager.get_all_last_job_data_dcs()[:5]  # Get me 5 jobs
        weights = GeneralizedWrapperDistributionStrategy().get_calculated_weights_of_jobs_in_wrapper(job_data_dcs)
        # print(sum(weights[k] for k in weights))
        info_handler = PlatformInformationHandler(GeneralizedWrapperDistributionStrategy())
        job_data_dcs_with_data = info_handler.execute_distribution(job_data_dcs[0], job_data_dcs, slurm_monitor)
        assert job_data_dcs_with_data[0].energy == round(
            slurm_monitor.total_energy * weights[job_data_dcs_with_data[0].job_name], 2)
        assert job_data_dcs_with_data[1].energy == round(
            slurm_monitor.total_energy * weights[job_data_dcs_with_data[1].job_name], 2)
        assert job_data_dcs_with_data[2].energy == round(
            slurm_monitor.total_energy * weights[job_data_dcs_with_data[2].job_name], 2)
        assert job_data_dcs_with_data[3].energy == round(
            slurm_monitor.total_energy * weights[job_data_dcs_with_data[3].job_name], 2)
        assert job_data_dcs_with_data[4].energy == round(
            slurm_monitor.total_energy * weights[job_data_dcs_with_data[4].job_name], 2)
        sum_energy = sum(job.energy for job in job_data_dcs_with_data[:5])  # Last 1 is original job_data_dc
        assert abs(sum_energy - slurm_monitor.total_energy) <= 10

    def test_process_status_changes(self):
        exp_history = ExperimentHistory("tt00")
        exp_history.initialize_database()
        CHUNK_UNIT = "month"
        CHUNK_SIZE = 20
        CURRENT_CONFIG = "CURRENT CONFIG"
        current_experiment_run_dc = exp_history.manager.get_experiment_run_dc_with_max_id()
        exp_run = exp_history.process_status_changes(job_list=self.job_list, chunk_unit=CHUNK_UNIT,
                                                     chunk_size=CHUNK_SIZE,
                                                     current_config=CURRENT_CONFIG)  # Generates new run
        assert current_experiment_run_dc.run_id != exp_run.run_id
        assert exp_run.chunk_unit == CHUNK_UNIT
        assert exp_run.metadata == CURRENT_CONFIG
        assert exp_run.total == len(self.job_list)
        current_experiment_run_dc = exp_history.manager.get_experiment_run_dc_with_max_id()
        exp_run = exp_history.process_status_changes(job_list=self.job_list, chunk_unit=CHUNK_UNIT,
                                                     chunk_size=CHUNK_SIZE, current_config=CURRENT_CONFIG)  # Same run
        assert current_experiment_run_dc.run_id == exp_run.run_id
        new_job_list = [
            job("a29z_20000101_fc2_1_POST", "2000-01-01 00:00:00", "POST", "FAILED", ""),
            job("a29z_20000101_fc1_1_CLEAN", "2000-01-01 00:00:00", "CLEAN", "FAILED", ""),
            job("a29z_20000101_fc3_1_POST", "2000-01-01 00:00:00", "POST", "RUNNING", ""),
            job("a29z_20000101_fc2_1_CLEAN", "2000-01-01 00:00:00", "CLEAN", "FAILED", ""),
            job("a29z_20000101_fc0_3_SIM", "2000-01-01 00:00:00", "SIM", "FAILED", ""),
            job("a29z_20000101_fc1_2_POST", "2000-01-01 00:00:00", "POST", "QUEUING", ""),
        ]
        current_experiment_run_dc = exp_history.manager.get_experiment_run_dc_with_max_id()
        exp_run = exp_history.process_status_changes(job_list=new_job_list, chunk_unit=CHUNK_UNIT,
                                                     chunk_size=CHUNK_SIZE,
                                                     current_config=CURRENT_CONFIG)  # Generates new run
        assert current_experiment_run_dc.run_id != exp_run.run_id
        assert exp_run.total == len(new_job_list)
        assert exp_run.failed == 4

    def test_write_submit_time(self):
        exp_history = ExperimentHistory("tt00")
        exp_history.initialize_database()
        JOB_NAME = "a29z_20000101_fc2_1_SIM"
        NCPUS = 128
        PLATFORM_NAME = "marenostrum4"
        JOB_ID = 101
        inserted_job_data_dc = exp_history.write_submit_time(JOB_NAME, time.time(), "SUBMITTED", NCPUS, "00:30",
                                                             "debug", "20000101", "fc2", "SIM", 1, PLATFORM_NAME,
                                                             JOB_ID, "bsc_es", 1, "")
        assert inserted_job_data_dc.job_name == JOB_NAME
        assert inserted_job_data_dc.ncpus == NCPUS
        assert inserted_job_data_dc.children == ""
        assert inserted_job_data_dc.energy == 0
        assert inserted_job_data_dc.platform == PLATFORM_NAME
        assert inserted_job_data_dc.job_id == JOB_ID
        assert inserted_job_data_dc.qos == "debug"

    def test_write_start_time(self):
        exp_history = ExperimentHistory("tt00")
        exp_history.initialize_database()
        JOB_NAME = "a29z_20000101_fc2_1_SIM"
        NCPUS = 128
        PLATFORM_NAME = "marenostrum4"
        JOB_ID = 101
        exp_history.write_submit_time(JOB_NAME, time.time(), "SUBMITTED", NCPUS, "00:30",
                                      "debug", "20000101", "fc2", "SIM", 1, PLATFORM_NAME,
                                      JOB_ID, "bsc_es", 1, "")
        inserted_job_data_dc = exp_history.write_start_time(JOB_NAME, time.time(), "RUNNING", NCPUS, "00:30", "debug",
                                                            "20000101", "fc2", "SIM", 1, PLATFORM_NAME, JOB_ID,
                                                            "bsc_es", 1, "")
        assert inserted_job_data_dc.job_name == JOB_NAME
        assert inserted_job_data_dc.ncpus == NCPUS
        assert inserted_job_data_dc.children == ""
        assert inserted_job_data_dc.energy == 0
        assert inserted_job_data_dc.platform == PLATFORM_NAME
        assert inserted_job_data_dc.job_id == JOB_ID
        assert inserted_job_data_dc.status == "RUNNING"
        assert inserted_job_data_dc.qos == "debug"


@pytest.mark.skip()
class TestLogging:

    def setup_method(self):
        message = "No Message"
        try:
            raise Exception("Setup test exception")
        except Exception:
            message = traceback.format_exc()
        self.log = Logging("tt00")
        self.exp_message = "Exception message"
        self.trace_message = message

    def test_build_message(self):
        message = self.log.build_message(self.exp_message, self.trace_message)
        # print(message)
        assert message is not None
        assert len(message) > 0

    def test_log(self):
        self.log.log(self.exp_message, self.trace_message)


def test_experiment_history_force_sqlalchemy_migrates_old_schema(tmp_path):
    """ExperimentHistory with force_sql_alchemy=True migrates an old-schema database."""
    db_dir = Path(tmp_path) / "metadata" / "data"
    db_dir.mkdir(parents=True, exist_ok=True)
    db_file = db_dir / "job_data_a000.db"

    engine = create_engine(f"sqlite:///{db_file}")
    old_job_data_table.create(engine)
    old_experiment_run_table.create(engine)
    engine.dispose()

    exp_history = ExperimentHistory("a000", force_sql_alchemy=True)
    assert exp_history.manager is not None

    result = exp_history.manager.get_jobs_data_last_row(["nonexistent"])
    assert result == {}


def test_get_finish_data_dc(tmp_path, monkeypatch):
    """Test that get_finish_data_dc retrieves the correct JobData after a full submit/start/finish cycle.

    :param tmp_path: Pytest fixture providing a temporary directory unique to the test invocation.
    :type tmp_path: pathlib.Path
    :param monkeypatch: Pytest fixture for monkeypatching attributes and environment variables.
    :type monkeypatch: pytest.MonkeyPatch
    :raises AssertionError: If the retrieved job data does not match the inserted job data.
    """
    monkeypatch.setattr(BasicConfig, "JOBDATA_DIR", str(tmp_path))
    monkeypatch.setattr(BasicConfig, "HISTORICAL_LOG_DIR", str(tmp_path))

    exp_history = ExperimentHistory("tt00")
    exp_history.initialize_database()
    # An experiment run must exist before job data can be written.
    exp_history.create_new_experiment_run()

    JOB_NAME = "a29z_20000101_fc2_1_SIM"
    NCPUS = 128
    PLATFORM_NAME = "marenostrum5"
    JOB_ID = 101
    FAIL_COUNT = 0

    # write_submit_time maps any non-"COMPLETED" status to "FAILED" internally.
    exp_history.write_submit_time(
        JOB_NAME, time.time(), "COMPLETED", NCPUS, "00:30",
        "debug", "20000101", "fc2", "SIM", 1, PLATFORM_NAME,
        JOB_ID, children="", fail_count=FAIL_COUNT
    )
    exp_history.write_start_time(
        JOB_NAME, start=time.time(), status="RUNNING", qos="debug",
        job_id=JOB_ID, children="", fail_count=FAIL_COUNT
    )
    inserted_job_data_dc = exp_history.write_finish_time(
        JOB_NAME, finish=int(time.time()), status="COMPLETED",
        job_id=JOB_ID, fail_count=FAIL_COUNT
    )
    assert inserted_job_data_dc is not None, "write_finish_time returned None; check for internal errors."

    finish_data_dc = exp_history.get_finish_data_dc(JOB_NAME, fail_count=FAIL_COUNT)
    assert finish_data_dc is not None, "get_finish_data_dc returned None; record not found."

    assert finish_data_dc.job_name == inserted_job_data_dc.job_name
    assert finish_data_dc.ncpus == inserted_job_data_dc.ncpus
    assert finish_data_dc.children == inserted_job_data_dc.children
    assert finish_data_dc.energy == inserted_job_data_dc.energy
    assert finish_data_dc.platform == inserted_job_data_dc.platform
    assert finish_data_dc.job_id == inserted_job_data_dc.job_id
    assert finish_data_dc.status == inserted_job_data_dc.status
    assert finish_data_dc.qos == inserted_job_data_dc.qos


def test_update_submit_time(tmp_path, monkeypatch):
    """Test that update_submit_time correctly updates the submit time of an existing job record.

    :param tmp_path: Pytest fixture providing a temporary directory unique to the test invocation.
    :type tmp_path: pathlib.Path
    :param monkeypatch: Pytest fixture for monkeypatching attributes and environment variables.
    :type monkeypatch: pytest.MonkeyPatch
    :raises AssertionError: If the submit time is not updated correctly.
    """
    monkeypatch.setattr(BasicConfig, "JOBDATA_DIR", str(tmp_path))
    exp_history = ExperimentHistory("tt00")
    exp_history.initialize_database()
    # An experiment run must exist before job data can be written.
    exp_history.create_new_experiment_run()

    JOB_NAME = "a29z_20000101_fc2_1_SIM"
    NCPUS = 128
    PLATFORM_NAME = "marenostrum5"
    JOB_ID = 101
    FAIL_COUNT = 0

    initial_submit_time = int(time.time())
    exp_history.write_submit_time(
        JOB_NAME, initial_submit_time, "COMPLETED", NCPUS, "00:30",
        "debug", "20000101", "fc2", "SIM", 1, PLATFORM_NAME,
        JOB_ID, children="", fail_count=FAIL_COUNT
    )

    new_submit_time = initial_submit_time + 3600  # Add 1 hour
    exp_history.update_submit_time(JOB_NAME, new_submit_time, fail_count=FAIL_COUNT)

    finish_data_dc = exp_history.get_finish_data_dc(JOB_NAME, fail_count=FAIL_COUNT)
    assert finish_data_dc is not None, "get_finish_data_dc returned None; record not found."
    assert finish_data_dc.submit == new_submit_time, f"Expected submit time {new_submit_time}, got {finish_data_dc.submit}"
