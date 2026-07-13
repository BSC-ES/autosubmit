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
