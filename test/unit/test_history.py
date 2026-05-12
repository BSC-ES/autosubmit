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

import os
import re
from autosubmit.history.utils import get_current_datetime
import pytest
import time
import traceback
from shutil import copy2

from autosubmit.history.data_classes.job_data import JobData
from autosubmit.history.database_managers import database_models as Models
from autosubmit.history.database_managers.experiment_history_db_manager import ExperimentHistoryDbManager
from autosubmit.history.experiment_history import ExperimentHistory
from autosubmit.history.internal_logging import Logging
from autosubmit.history.platform_monitor.slurm_monitor import SlurmMonitor
from autosubmit.history.strategies import StraightWrapperAssociationStrategy, GeneralizedWrapperDistributionStrategy, \
    PlatformInformationHandler
from autosubmit.config.basicconfig import BasicConfig

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


_JOB_DATA_ROW_BASE = (
    1, 1, "test_job", "2024-01-01T00:00:00+0000", "2024-01-01T00:00:00+0000",
    0, 0, 0, "COMPLETED", 2, 1, "00:30", "debug", 0, "20200101", "SIM", "fc0",
    1, 1, "LOCAL", 101, "{}", 0, 1, 0.0, 0.0, "", "", 0, "", "", "",
)


@pytest.mark.parametrize(
    'kwargs,expected',
    [
        ({}, {'split': None, 'splits': None, 'fail_count': 0}),
        ({'split': '1', 'splits': '1-3', 'fail_count': 2}, {'split': '1', 'splits': '1-3', 'fail_count': 2}),
    ],
    ids=['defaults', 'explicit-v21-fields'],
)
def test_job_data_row_v21_fields(kwargs, expected):
    """JobDataRow must include split, splits, fail_count with correct defaults and explicit values."""
    row = Models.JobDataRow(*_JOB_DATA_ROW_BASE, **kwargs)
    for attr, value in expected.items():
        assert getattr(row, attr) == value


@pytest.mark.parametrize(
    'kwargs,expected',
    [
        ({}, {'split': None, 'splits': None, 'fail_count': 0}),
        ({'split': '2', 'splits': '1-4', 'fail_count': 3}, {'split': '2', 'splits': '1-4', 'fail_count': 3}),
    ],
    ids=['defaults', 'explicit-v21-fields'],
)
def test_job_data_init_v21_fields(kwargs, expected):
    """JobData __init__ must accept and default split, splits, fail_count correctly."""
    job = JobData(_id=1, job_name="test_job", **kwargs)
    for attr, value in expected.items():
        assert getattr(job, attr) == value


@pytest.mark.parametrize(
    'row_kwargs,expected',
    [
        ({}, {'split': None, 'splits': None, 'fail_count': 0}),
        ({'split': '1', 'splits': '1-3', 'fail_count': 2}, {'split': '1', 'splits': '1-3', 'fail_count': 2}),
    ],
    ids=['defaults', 'explicit-v21-fields'],
)
def test_job_data_from_model_v21_fields(row_kwargs, expected):
    """JobData.from_model must correctly parse and default split, splits, fail_count."""
    row = Models.JobDataRow(*_JOB_DATA_ROW_BASE, **row_kwargs)
    job = JobData.from_model(row)
    for attr, value in expected.items():
        assert getattr(job, attr) == value


@pytest.fixture
def fresh_experiment_history(tmp_path, mocker):
    """Return an ExperimentHistory backed by a fresh v21 database with one experiment run."""
    db_file = tmp_path / "job_data_test.db"
    manager = ExperimentHistoryDbManager(
        expid="test",
        jobdata_dir_path=str(tmp_path)
    )
    manager.historicaldb_file_path = str(db_file)
    manager.create_historical_database()
    manager._insert_experiment_run(
        mocker.MagicMock(
            start=0, finish=0, chunk_unit="NA", chunk_size=0,
            completed=0, total=0, failed=0, queuing=0, running=0,
            submitted=0, suspended=0, metadata=""
        )
    )
    exp_history = ExperimentHistory("test")
    exp_history.manager = manager
    return exp_history


def test_write_submit_time_accepts_split_splits_fail_count(fresh_experiment_history):
    job_dc = fresh_experiment_history.write_submit_time(
        "test_job", submit=100, status="SUBMITTED", ncpus=4,
        wallclock="01:00", qos="debug", date="20240101",
        member="fc0", section="SIM", chunk=1, platform="mn4",
        job_id=42, wrapper_queue=None, wrapper_code=None,
        children="", workflow_commit="abc123",
        split="1", splits="1-3", fail_count=2
    )

    assert job_dc is not None
    assert job_dc.split == "1"
    assert job_dc.splits == "1-3"
    assert job_dc.fail_count == 2


def test_get_submit_data_dc_found(fresh_experiment_history):
    fresh_experiment_history.write_submit_time(
        "test_job", submit=100, status="SUBMITTED",
        fail_count=2, split="1", splits="1-3"
    )

    result = fresh_experiment_history.get_submit_data_dc("test_job", 2)
    assert result is not None
    assert result.job_name == "test_job"
    assert result.fail_count == 2
    assert result.submit == 100


def test_get_submit_data_dc_not_found(fresh_experiment_history):
    result = fresh_experiment_history.get_submit_data_dc("missing", 0)
    assert result is None


def test_get_finish_data_dc_found(fresh_experiment_history):
    fresh_experiment_history.write_submit_time(
        "test_job", submit=100, status="SUBMITTED",
        fail_count=2
    )

    result = fresh_experiment_history.get_finish_data_dc("test_job", 2)
    assert result is not None
    assert result.job_name == "test_job"
    assert result.fail_count == 2


def test_get_finish_data_dc_not_found(fresh_experiment_history):
    result = fresh_experiment_history.get_finish_data_dc("missing", 0)
    assert result is None


def test_update_submit_time(fresh_experiment_history):
    fresh_experiment_history.write_submit_time(
        "test_job", submit=100, status="SUBMITTED",
        fail_count=2, split="1", splits="1-3"
    )

    updated = fresh_experiment_history.update_submit_time(
        "test_job", submit=200, status="QUEUING",
        fail_count=2, split="2", splits="2-4",
        job_id=99
    )

    assert updated is not None
    assert updated.submit == 200
    assert updated.status == "QUEUING"
    assert updated.split == "2"
    assert updated.splits == "2-4"
    assert updated.fail_count == 2
    assert updated.job_id == 99


def test_write_start_time_uses_fail_count(fresh_experiment_history):
    fresh_experiment_history.write_submit_time(
        "test_job", submit=100, status="SUBMITTED",
        fail_count=2, job_id=10
    )

    result = fresh_experiment_history.write_start_time(
        "test_job", start=200, status="RUNNING",
        fail_count=2, job_id=10
    )

    assert result is not None
    assert result.start == 200
    assert result.status == "RUNNING"


def test_write_finish_time_uses_fail_count(fresh_experiment_history):
    fresh_experiment_history.write_submit_time(
        "test_job", submit=100, status="SUBMITTED",
        fail_count=2, job_id=10
    )
    fresh_experiment_history.write_start_time(
        "test_job", start=200, status="RUNNING",
        fail_count=2, job_id=10
    )

    result = fresh_experiment_history.write_finish_time(
        "test_job", finish=300, status="COMPLETED",
        fail_count=2, job_id=10
    )

    assert result is not None
    assert result.finish == 300
    assert result.status == "COMPLETED"


def test_verify_slurm_monitor_warns_on_non_final_status(fresh_experiment_history, mocker):
    """_verify_slurm_monitor must log a warning when the monitor shows a non-final SLURM status."""
    exp_history = fresh_experiment_history
    ssh_output = """                 17857525  RUNNING           10        1 2021-10-13T15:51:16 2021-10-13T15:51:17 2021-10-13T15:52:47         2.41K                                                     
    """
    slurm_monitor = SlurmMonitor(ssh_output)
    job_data_dc = mocker.MagicMock()
    job_data_dc.job_name = "test_job"

    mock_log = mocker.patch('autosubmit.history.experiment_history.Log')
    exp_history._verify_slurm_monitor(slurm_monitor, job_data_dc)
    mock_log.warning.assert_called_once()
    args, _ = mock_log.warning.call_args
    assert "RUNNING" in args[0]
