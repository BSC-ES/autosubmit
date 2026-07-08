from unittest.mock import patch


from autosubmit.database.db_manager_job_list import JobsDbManager
from autosubmit.database.tables import JobsTable


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
