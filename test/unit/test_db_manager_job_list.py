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

from unittest.mock import patch

from autosubmit.database.db_manager_job_list import JobsDbManager
from autosubmit.database.tables import ExperimentStructureTable, JobsTable


def test_save_job_log_includes_updated_stats(tmp_path):
    """save_job_log persists and loads updated_stats correctly."""
    with patch("autosubmit.config.basicconfig.BasicConfig.LOCAL_ROOT_DIR", str(tmp_path)):
        from autosubmit.job.job import Job
        from autosubmit.job.job_common import Status
        mgr = JobsDbManager(schema="test_schema_stats")
        mgr.create_table(JobsTable.name)

        table = mgr.table_registry.get(JobsTable.name)
        mgr.upsert_many(table.name, [{"name": "dummy", "status": "COMPLETED", "updated_log": 0, "updated_stats": 0, "fail_count": 0}], ["name"])

        job = Job("dummy", 1, Status.WAITING, 0)
        job.updated_log = 2
        job.updated_stats = 1
        job.status = Status.COMPLETED
        job.local_logs = ("out", "err")
        job.remote_logs = ("rout", "rerr")

        mgr.save_job_log(job)

        loaded = mgr.load_job_by_name("dummy")
        assert loaded is not None
        assert loaded["updated_log"] == 2
        assert loaded["updated_stats"] == 1


def test_save_jobs_preserves_log_counters_for_non_waiting_ready(tmp_path):
    """save_jobs preserves updated_log / updated_stats for non-WAITING/READY jobs."""
    with patch("autosubmit.config.basicconfig.BasicConfig.LOCAL_ROOT_DIR", str(tmp_path)):
        from autosubmit.job.job import Job
        from autosubmit.job.job_common import Status
        mgr = JobsDbManager(schema="test_schema_preserve")
        mgr.create_table(JobsTable.name)

        table = mgr.table_registry.get(JobsTable.name)
        mgr.upsert_many(table.name, [{
            "name": "dummy", "status": "COMPLETED",
            "local_logs_out": "out", "local_logs_err": "err",
            "remote_logs_out": "rout", "remote_logs_err": "rerr",
            "updated_log": 1, "updated_stats": 2, "fail_count": 0,
        }], ["name"])

        job = Job("dummy", 1, Status.COMPLETED, 0)
        job.status = Status.COMPLETED
        mgr.save_jobs([job])

        loaded = mgr.load_job_by_name("dummy")
        assert loaded["updated_log"] == 1
        assert loaded["updated_stats"] == 2


def test_save_jobs_resets_log_counters_for_waiting(tmp_path):
    """save_jobs resets updated_log / updated_stats to 0 for WAITING jobs."""
    with patch("autosubmit.config.basicconfig.BasicConfig.LOCAL_ROOT_DIR", str(tmp_path)):
        from autosubmit.job.job import Job
        from autosubmit.job.job_common import Status
        mgr = JobsDbManager(schema="test_schema_waiting")
        mgr.create_table(JobsTable.name)

        table = mgr.table_registry.get(JobsTable.name)
        mgr.upsert_many(table.name, [{
            "name": "dummy", "status": "WAITING",
            "local_logs_out": "out", "local_logs_err": "err",
            "remote_logs_out": "rout", "remote_logs_err": "rerr",
            "updated_log": 1, "updated_stats": 2, "fail_count": 0,
        }], ["name"])

        job = Job("dummy", 1, Status.WAITING, 0)
        job.status = Status.WAITING
        mgr.save_jobs([job], reset_log_counters=True)

        loaded = mgr.load_job_by_name("dummy")
        assert loaded["updated_log"] == 0
        assert loaded["updated_stats"] == 0


def test_reset_updated_logs_resets_only_fail_count_zero(tmp_path):
    """reset_updated_logs resets counters for fail_count==0, leaves others untouched."""
    with patch("autosubmit.config.basicconfig.BasicConfig.LOCAL_ROOT_DIR", str(tmp_path)):
        mgr = JobsDbManager(schema="test_schema_reset_logs")
        mgr.create_table(JobsTable.name)

        table = mgr.table_registry.get(JobsTable.name)
        mgr.upsert_many(table.name, [
            {"name": "clean_job", "status": "COMPLETED",
             "updated_log": 5, "updated_stats": 3, "fail_count": 0},
            {"name": "failed_job", "status": "FAILED",
             "updated_log": 5, "updated_stats": 3, "fail_count": 2},
        ], ["name"])

        mgr.reset_updated_logs()

        clean = mgr.load_job_by_name("clean_job")
        assert clean["updated_log"] == 0
        assert clean["updated_stats"] == 0

        failed = mgr.load_job_by_name("failed_job")
        assert failed["updated_log"] == 5
        assert failed["updated_stats"] == 3


def test_save_jobs_resets_log_counters_for_ready(tmp_path):
    """save_jobs resets updated_log / updated_stats to 0 for READY jobs."""
    with patch("autosubmit.config.basicconfig.BasicConfig.LOCAL_ROOT_DIR", str(tmp_path)):
        from autosubmit.job.job import Job
        from autosubmit.job.job_common import Status
        mgr = JobsDbManager(schema="test_schema_ready")
        mgr.create_table(JobsTable.name)

        table = mgr.table_registry.get(JobsTable.name)
        mgr.upsert_many(table.name, [{
            "name": "dummy", "status": "READY",
            "local_logs_out": "out", "local_logs_err": "err",
            "remote_logs_out": "rout", "remote_logs_err": "rerr",
            "updated_log": 1, "updated_stats": 2, "fail_count": 0,
        }], ["name"])

        job = Job("dummy", 1, Status.READY, 0)
        job.status = Status.READY
        mgr.save_jobs([job], reset_log_counters=True)

        loaded = mgr.load_job_by_name("dummy")
        assert loaded["updated_log"] == 0
        assert loaded["updated_stats"] == 0


def test_select_job_names_by_sections(tmp_path):
    """select_job_names_by_sections returns job names from DB filtered by section and status."""
    with patch("autosubmit.config.basicconfig.BasicConfig.LOCAL_ROOT_DIR", str(tmp_path)):
        mgr = JobsDbManager(schema="test_schema_names_by_sections")
        mgr.create_table(JobsTable.name)

        table = mgr.table_registry.get(JobsTable.name)
        mgr.upsert_many(table.name, [
            {"name": "job1", "section": "SEC1", "status": "COMPLETED", "fail_count": 0},
            {"name": "job2", "section": "SEC1", "status": "READY", "fail_count": 0},
            {"name": "job3", "section": "SEC2", "status": "COMPLETED", "fail_count": 0},
        ], ["name"])

        assert mgr.select_job_names_by_sections(["SEC1"]) == {"job1", "job2"}

        assert mgr.select_job_names_by_sections(["SEC2"]) == {"job3"}

        assert mgr.select_job_names_by_sections(["SEC1"], exclude_completed=True) == {"job2"}

        assert mgr.select_job_names_by_sections(["SEC1"], exclude_names={"job1"}) == {"job2"}

        assert mgr.select_job_names_by_sections(["SEC1"], status_filter="READY") == {"job2"}

        assert mgr.select_job_names_by_sections(["SEC1", "SEC2"]) == {"job1", "job2", "job3"}


def test_remaining_blocked_by_package(tmp_path):
    """remaining_blocked_by_package returns True only when all parents are within package/remaining/COMPLETED."""
    with patch("autosubmit.config.basicconfig.BasicConfig.LOCAL_ROOT_DIR", str(tmp_path)):
        mgr = JobsDbManager(schema="test_schema_blocked")
        mgr.create_table(JobsTable.name)
        mgr.create_table(ExperimentStructureTable.name)

        jobs_table = mgr.table_registry.get(JobsTable.name)
        mgr.upsert_many(jobs_table.name, [
            {"name": "pkg_job", "status": "READY", "fail_count": 0},
            {"name": "rem_job", "status": "WAITING", "fail_count": 0},
            {"name": "chain_job", "status": "WAITING", "fail_count": 0},
            {"name": "ext_failed", "status": "FAILED", "fail_count": 0},
            {"name": "ext_completed", "status": "COMPLETED", "fail_count": 0},
        ], ["name"])

        structure = mgr.table_registry.get(ExperimentStructureTable.name)
        mgr.insert_many(structure.name, [
            {"e_from": "pkg_job", "e_to": "rem_job"},
            {"e_from": "rem_job", "e_to": "chain_job"},
        ])

        # rem_job's parent is in package → blocked
        assert mgr.remaining_blocked_by_package({"rem_job"}, {"pkg_job"}) is True

        # chain_job's parent (rem_job) is blocked only by remaining → blocked
        assert mgr.remaining_blocked_by_package({"chain_job"}, {"rem_job"}) is True

        # empty remaining_names → vacuously blocked
        assert mgr.remaining_blocked_by_package(set(), {"pkg_job"}) is True

        # add external parent for rem_job
        mgr.insert_many(structure.name, [
            {"e_from": "ext_failed", "e_to": "rem_job"},
        ])

        # rem_job now has external FAILED parent → not blocked
        assert mgr.remaining_blocked_by_package({"rem_job"}, {"pkg_job"}) is False

        # replace external FAILED with COMPLETED
        mgr.upsert_many(jobs_table.name, [
            {"name": "ext_failed", "status": "COMPLETED", "fail_count": 0},
        ], ["name"])

        # COMPLETED parent is allowed → blocked
        assert mgr.remaining_blocked_by_package({"rem_job"}, {"pkg_job"}) is True
