#!/usr/bin/env python3

# Copyright 2015-2020 Earth Sciences Department, BSC-CNS
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

import os
import random
import time
from shutil import copy2

import pytest

import autosubmit.history.utils as HUtils
from autosubmit.history.data_classes.experiment_run import ExperimentRun
from autosubmit.history.data_classes.job_data import JobData
from autosubmit.history.database_managers.experiment_history_db_manager import ExperimentHistoryDbManager
from autosubmit.history.database_managers.experiment_status_db_manager import ExperimentStatusDbManager
from autosubmitconfigparser.config.basicconfig import BasicConfig

EXPID_TT00_SOURCE = "test_database.db~"
EXPID_TT01_SOURCE = "test_database_no_run.db~"
EXPID = "t024"
EXPID_NONE = "t027"
BasicConfig.read()
JOBDATA_DIR = BasicConfig.JOBDATA_DIR
LOCAL_ROOT_DIR = BasicConfig.LOCAL_ROOT_DIR


@pytest.mark.skip()
@pytest.mark.skip('TODO: looks like another test that used actual experiments data')
class TestExperimentStatusDatabaseManager:
    """ Covers Experiment Status Database Manager """

    def setup_method(self):
        self.exp_status_db = ExperimentStatusDbManager(EXPID, BasicConfig.DB_DIR, BasicConfig.DB_FILE, LOCAL_ROOT_DIR)

    def test_get_current_experiment_status_row(self):
        exp_status_row = self.exp_status_db.get_experiment_status_row_by_expid(EXPID)
        assert exp_status_row is not None
        exp_status_row_none = self.exp_status_db.get_experiment_status_row_by_expid(EXPID_NONE)
        assert exp_status_row_none is None
        exp_row_direct = self.exp_status_db.get_experiment_status_row_by_exp_id(exp_status_row.exp_id)
        assert exp_status_row.exp_id == exp_row_direct.exp_id

    def test_update_exp_status(self):
        self.exp_status_db.update_exp_status(EXPID, "RUNNING")
        exp_status_row_current = self.exp_status_db.get_experiment_status_row_by_expid(EXPID)
        assert exp_status_row_current.status == "RUNNING"
        self.exp_status_db.update_exp_status(EXPID, "NOT RUNNING")
        exp_status_row_current = self.exp_status_db.get_experiment_status_row_by_expid(EXPID)
        assert exp_status_row_current.status == "NOT RUNNING"

    def test_create_exp_status(self):
        experiment = self.exp_status_db.get_experiment_row_by_expid(EXPID_NONE)
        self.exp_status_db.create_experiment_status_as_running(experiment)
        experiment_status = self.exp_status_db.get_experiment_status_row_by_expid(EXPID_NONE)
        assert experiment_status is not None
        self.exp_status_db.delete_exp_status(EXPID_NONE)
        experiment_status = self.exp_status_db.get_experiment_status_row_by_expid(EXPID_NONE)
        assert experiment_status is None


@pytest.mark.skip('TODO: looks like another test that used actual experiments data')
class TestExperimentHistoryDbManager:
    """ Covers Experiment History Database Manager and Data Models """

    def setup_method(self):
        self.experiment_database = ExperimentHistoryDbManager(EXPID, JOBDATA_DIR)
        source_path_tt00 = os.path.join(JOBDATA_DIR, EXPID_TT00_SOURCE)
        self.target_path_tt00 = os.path.join(JOBDATA_DIR, "job_data_{0}.db".format(EXPID))
        copy2(source_path_tt00, self.target_path_tt00)
        source_path_tt01 = os.path.join(JOBDATA_DIR, EXPID_TT01_SOURCE)
        self.target_path_tt01 = os.path.join(JOBDATA_DIR, "job_data_{0}.db".format(EXPID_NONE))
        copy2(source_path_tt01, self.target_path_tt01)
        self.experiment_database.initialize()

    def teardown_method(self):
        os.remove(self.target_path_tt00)
        os.remove(self.target_path_tt01)

    def test_get_max_id(self):
        max_item = self.experiment_database.get_experiment_run_dc_with_max_id()
        assert max_item.run_id > 0
        assert max_item.run_id >= 18  # Max is 18

    def test_pragma(self):
        assert self.experiment_database._get_pragma_version() == 17  # Update version on changes

    def test_get_job_data(self):
        job_data = self.experiment_database._get_job_data_last_by_name("a29z_20000101_fc0_1_SIM")
        assert len(job_data) > 0
        assert job_data[0].last == 1
        assert job_data[0].job_name == "a29z_20000101_fc0_1_SIM"

        job_data = self.experiment_database.get_job_data_by_name("a29z_20000101_fc0_1_SIM")
        assert job_data[0].job_name == "a29z_20000101_fc0_1_SIM"

        job_data = self.experiment_database._get_job_data_last_by_run_id(18)  # Latest
        assert len(job_data) > 0

        job_data = self.experiment_database._get_job_data_last_by_run_id_and_finished(18)
        assert len(job_data) > 0

        job_data = self.experiment_database.get_job_data_all()
        assert len(job_data) > 0

    def test_insert_and_delete_experiment_run(self):
        new_run = ExperimentRun(19)
        new_id = self.experiment_database._insert_experiment_run(new_run)
        assert new_id is not None
        last_experiment_run = self.experiment_database.get_experiment_run_dc_with_max_id()
        assert new_id == last_experiment_run.run_id
        self.experiment_database.delete_experiment_run(new_id)
        last_experiment_run = self.experiment_database.get_experiment_run_dc_with_max_id()
        assert new_id != last_experiment_run.run_id

    def test_insert_and_delete_job_data(self):
        max_run_id = self.experiment_database.get_experiment_run_dc_with_max_id().run_id
        new_job_data_name = "test_001_name_{0}".format(int(time.time()))
        new_job_data = JobData(_id=1, job_name=new_job_data_name, run_id=max_run_id)
        new_job_data_id = self.experiment_database._insert_job_data(new_job_data)
        assert new_job_data_id is not None
        self.experiment_database.delete_job_data(new_job_data_id)
        job_data = self.experiment_database.get_job_data_by_name(new_job_data_name)
        assert len(job_data) == 0

    def test_update_experiment_run(self):
        experiment_run_data_class = self.experiment_database.get_experiment_run_dc_with_max_id()  # 18
        backup_run = self.experiment_database.get_experiment_run_dc_with_max_id()
        experiment_run_data_class.chunk_unit = "unouno"
        experiment_run_data_class.running = random.randint(1, 100)
        experiment_run_data_class.queuing = random.randint(1, 100)
        experiment_run_data_class.suspended = random.randint(1, 100)
        self.experiment_database._update_experiment_run(experiment_run_data_class)
        last_experiment_run = self.experiment_database.get_experiment_run_dc_with_max_id()  # 18
        assert last_experiment_run.chunk_unit == experiment_run_data_class.chunk_unit
        assert last_experiment_run.running == experiment_run_data_class.running
        assert last_experiment_run.queuing == experiment_run_data_class.queuing
        assert last_experiment_run.suspended == experiment_run_data_class.suspended
        self.experiment_database._update_experiment_run(backup_run)

    def test_job_data_from_model(self):
        job_data_rows = self.experiment_database._get_job_data_last_by_name("a29z_20000101_fc0_1_SIM")
        job_data_row_first = job_data_rows[0]
        job_data_data_class = JobData.from_model(job_data_row_first)
        assert job_data_row_first.job_name == job_data_data_class.job_name

    def test_update_job_data_processed(self):
        current_time = time.time()
        job_data_rows = self.experiment_database._get_job_data_last_by_name("a29z_20000101_fc0_1_SIM")
        job_data_row_first = job_data_rows[0]
        job_data_data_class = JobData.from_model(job_data_row_first)
        backup_job_dc = JobData.from_model(job_data_row_first)
        job_data_data_class.nnodes = random.randint(1, 1000)
        job_data_data_class.ncpus = random.randint(1, 1000)
        job_data_data_class.status = "DELAYED"
        job_data_data_class.finish = current_time
        self.experiment_database._update_job_data_by_id(job_data_data_class)
        job_data_rows_current = self.experiment_database._get_job_data_last_by_name("a29z_20000101_fc0_1_SIM")
        job_data_row_first = job_data_rows_current[0]
        assert job_data_row_first.nnodes == job_data_data_class.nnodes
        assert job_data_row_first.ncpus == job_data_data_class.ncpus
        assert job_data_row_first.status == job_data_data_class.status
        assert job_data_row_first.finish == job_data_data_class.finish
        self.experiment_database._update_job_data_by_id(backup_job_dc)

    def test_bulk_update(self):
        current_time = time.time()
        all_job_data_rows = self.experiment_database.get_job_data_all()
        job_data_rows_test = [job for job in all_job_data_rows if job.run_id == 3]
        backup = [JobData.from_model(job) for job in job_data_rows_test]
        list_job_data_class = [JobData.from_model(job) for job in job_data_rows_test]
        backup_changes = [(HUtils.get_current_datetime(), job.status, job.rowstatus, job._id) for job in
                          list_job_data_class]
        changes = [(HUtils.get_current_datetime(), "DELAYED", job.rowstatus, job._id) for job in list_job_data_class]
        self.experiment_database.update_many_job_data_change_status(changes)
        all_job_data_rows = self.experiment_database.get_job_data_all()
        job_data_rows_validate = [job for job in all_job_data_rows if job.run_id == 3]
        for (job_val, change_item) in zip(job_data_rows_validate, changes):
            modified, status, rowstatus, _id = change_item
            # self.assertTrue(job_val.finish == finish)
            assert job_val.modified == modified
            assert job_val.status == status
            assert job_val.rowstatus == rowstatus
            assert job_val.id == _id
        self.experiment_database.update_many_job_data_change_status(backup_changes)

    def test_job_data_maxcounter(self):
        new_job_data = ExperimentHistoryDbManager(EXPID_NONE, JOBDATA_DIR)
        new_job_data.initialize()
        max_empty_table_counter = new_job_data.get_job_data_max_counter()
        assert max_empty_table_counter == 0
        max_existing_counter = self.experiment_database.get_job_data_max_counter()
        assert max_existing_counter > 0

    def test_register_submitted_job_data_dc(self):
        job_data_dc = self.experiment_database.get_job_data_dc_unique_latest_by_job_name("a29z_20000101_fc0_1_SIM")
        max_counter = self.experiment_database.get_job_data_max_counter()
        assert max_counter > 0
        assert job_data_dc.counter > 0
        next_counter = max(max_counter, job_data_dc.counter + 1)
        assert next_counter >= max_counter
        assert next_counter >= job_data_dc.counter + 1
        job_data_dc.counter = next_counter
        job_data_dc_current = self.experiment_database.register_submitted_job_data_dc(job_data_dc)
        assert job_data_dc._id < job_data_dc_current._id
        job_data_last_list = self.experiment_database._get_job_data_last_by_name(job_data_dc.job_name)
        assert len(job_data_last_list) == 1
        self.experiment_database.delete_job_data(job_data_last_list[0].id)
        job_data_dc.last = 1
        updated_job_data_dc = self.experiment_database.update_job_data_dc_by_id(job_data_dc)
        assert job_data_dc._id == updated_job_data_dc._id
        job_data_dc = self.experiment_database.get_job_data_dc_unique_latest_by_job_name("a29z_20000101_fc0_1_SIM")
        assert job_data_dc._id == updated_job_data_dc._id

    def test_update_children_and_platform_output(self):
        job_data_dc = self.experiment_database.get_job_data_dc_unique_latest_by_job_name("a29z_20000101_fc0_1_SIM")
        children_str = "a00, a01, a02"
        platform_output_str = " SLURM OUTPUT "
        job_data_dc.children = children_str
        job_data_dc.platform_output = platform_output_str
        self.experiment_database.update_job_data_dc_by_id(job_data_dc)
        job_data_dc_updated = self.experiment_database.get_job_data_dc_unique_latest_by_job_name(
            "a29z_20000101_fc0_1_SIM")
        assert job_data_dc_updated.children == children_str
        assert job_data_dc_updated.platform_output == platform_output_str
        # Back to normal
        job_data_dc.children = ""
        job_data_dc.platform_output = "NO OUTPUT"
        self.experiment_database.update_job_data_dc_by_id(job_data_dc)
        job_data_dc_updated = self.experiment_database.get_job_data_dc_unique_latest_by_job_name(
            "a29z_20000101_fc0_1_SIM")
        assert job_data_dc_updated.children == ""
        assert job_data_dc_updated.platform_output == "NO OUTPUT"

    def test_experiment_run_dc(self):
        experiment_run = self.experiment_database.get_experiment_run_dc_with_max_id()
        assert experiment_run is not None

    def test_if_database_exists(self):
        exp_manager = ExperimentHistoryDbManager("0000")
        assert exp_manager.my_database_exists() is False
