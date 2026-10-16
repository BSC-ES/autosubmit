# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
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

import multiprocessing as mp
import os
import pwd
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.database.db_manager_job_list import JobsDbManager
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.log.log import AutosubmitCritical
from autosubmit.platforms.locplatform import LocalPlatform
from autosubmit.platforms.platform import CopyQueue, Platform


@pytest.fixture
def as_conf(prepare_test, mocker):
    mocker.patch("pathlib.Path.exists", return_value=True)
    as_conf = AutosubmitConfig("test")
    as_conf.experiment_data = as_conf.load_config_file(
        as_conf.experiment_data, Path(prepare_test.join("platforms_t000.yml"))
    )
    as_conf.misc_data = {"AS_COMMAND": "run"}
    return as_conf


@pytest.fixture
def real_as_conf(prepare_test):
    """Like ``as_conf`` but ``Path.exists`` is only patched while loading the configuration.

    Patching ``Path.exists`` globally (as ``as_conf`` does) makes file assertions and the retrieval
    chain pass vacuously, so the real recovery tests use this fixture instead.
    """
    with patch("pathlib.Path.exists", return_value=True):
        as_conf = AutosubmitConfig("test")
        as_conf.experiment_data = as_conf.load_config_file(
            as_conf.experiment_data, Path(prepare_test.join("platforms_t000.yml"))
        )
        as_conf.misc_data = {"AS_COMMAND": "run"}
    return as_conf


def _prepare_real_recovery(base: str, platform: LocalPlatform, expid: str = "t000", attempt: int = 0):
    """Create the real STAT and log files and a job aligned with the platform's tmp layout."""
    log_dir = Path(base, expid, "tmp", f"LOG_{expid}")
    log_dir.mkdir(parents=True, exist_ok=True)
    (log_dir / f"{expid}_STAT_{attempt}").write_text("1700000000\n1700000001\n1700000002\nCOMPLETED\n")
    (log_dir / f"{expid}.cmd.out.{attempt}").write_text("[INFO] JOBID=123\nout\n")
    (log_dir / f"{expid}.cmd.err.{attempt}").write_text("err\n")

    job = Job(expid, "123", Status.COMPLETED, 0)
    job.platform = platform
    job.platform_name = "local"
    job.submit_time_timestamp = "20200101120000"
    job.retrials = 0
    job.updated_log = attempt
    job.updated_stats = 0
    job.id = 123
    # ``BasicConfig.read()`` runs in ``Job.__init__`` and would reset the patched dirs, so align here.
    job._tmp_path = os.path.join(base, expid, "tmp")
    return job, log_dir


def test_log_recovery_no_keep_alive(prepare_test, local, mocker, as_conf):
    # Exercise the production start method. Spawn starts a fresh interpreter,
    # which is also the path that needs separate coverage collection.
    mocker.patch(
        "autosubmit.platforms.platform.Platform.get_mp_context",
        return_value=mp.get_context("spawn"),
    )
    local.config["LOG_RECOVERY_TIMEOUT"] = 1

    local.spawn_log_retrieval_process(as_conf)
    is_alive = local.log_recovery_process.is_alive()
    assert is_alive, "Log recovery process is not alive"

    start = time.time()
    while local.log_recovery_process.is_alive() and time.time() - start < 10:
        time.sleep(1)

    assert not local.log_recovery_process.is_alive(), (
        "Log recovery process is still alive"
    )
    local.cleanup_event.set()


def test_log_recovery_keep_alive(prepare_test, local, mocker, as_conf):
    mocker.patch(
        "autosubmit.platforms.platform.Platform.get_mp_context",
        return_value=mp.get_context("fork"),
    )
    local.config["LOG_RECOVERY_TIMEOUT"] = 1
    local.spawn_log_retrieval_process(as_conf)
    assert local.log_recovery_process.is_alive()
    local.work_event.set()
    assert local.log_recovery_process.is_alive()
    # Give the child enough time to drain the queue and exit after the keep-alive timeout.
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and local.log_recovery_process.is_alive():
        time.sleep(0.1)
    assert local.log_recovery_process.is_alive() is False


def test_log_recovery_keep_alive_cleanup(prepare_test, local, mocker, as_conf):
    mocker.patch(
        "autosubmit.platforms.platform.Platform.get_mp_context",
        return_value=mp.get_context("fork"),
    )
    local.config["LOG_RECOVERY_TIMEOUT"] = 0
    local.spawn_log_retrieval_process(as_conf)
    local.cleanup_event.set()
    # The child still has to load the configuration and open its connection before it reads
    # the cleanup event, which can take longer than a second on a busy CI runner. join()
    # returns as soon as the child exits, so the timeout only bounds a hang.
    local.log_recovery_process.join(timeout=30)
    assert not local.log_recovery_process.is_alive()


def test_log_recovery_recover_log(prepare_test, real_as_conf, mocker):
    """The recovery retrieval really moves the finished job logs and writes its stats.

    This is the exact recovery function the child process runs for each queued job.
    """
    base = f"{prepare_test.strpath}/scratch"
    platform = LocalPlatform("t000", "local", {"LOCAL_ROOT_DIR": base, "LOCAL_TMP_DIR": "tmp"})
    mocker.patch.object(Job, "stat_registered", return_value=False)
    mocker.patch.object(Job, "write_stats")
    job, log_dir = _prepare_real_recovery(base, platform)

    report = job.retrieve_logfiles()

    assert report.all_succeeded
    assert job.updated_log == 1
    assert (log_dir / "t000.20200101120000.out").exists()
    assert (log_dir / "t000.20200101120000.err").exists()
    # The raw attempt-0 logs were moved, not left behind.
    assert not (log_dir / "t000.cmd.out.0").exists()
    assert not (log_dir / "t000.cmd.err.0").exists()


def test_log_recovery_inline_without_process(prepare_test, real_as_conf, mocker):
    """With DISABLE_RECOVERY_THREADS the logs are really recovered in the main process, no child."""
    base = f"{prepare_test.strpath}/scratch"
    platform = LocalPlatform("t000", "local", {"LOCAL_ROOT_DIR": base, "LOCAL_TMP_DIR": "tmp"})
    mocker.patch.object(Job, "stat_registered", return_value=False)
    mocker.patch.object(Job, "write_stats")
    real_as_conf.experiment_data.setdefault("PLATFORMS", {})["local"] = {"DISABLE_RECOVERY_THREADS": True}
    job, log_dir = _prepare_real_recovery(base, platform)

    job.recover_log(real_as_conf)

    assert platform.log_recovery_process is None  # no separate process was started
    assert job.updated_log == 1
    assert (log_dir / "t000.20200101120000.out").exists()
    assert (log_dir / "t000.20200101120000.err").exists()


def test_recover_log_routes_inline_when_threads_disabled(local, as_conf, mocker):
    """With DISABLE_RECOVERY_THREADS ``recover_log`` recovers inline instead of queuing for a process.

    The real ``platforms_data`` lookup is exercised here (the option is keyed by the platform name),
    which is what the fix for #3342 corrects in ``Job.recover_log``.
    """
    as_conf.experiment_data.setdefault("PLATFORMS", {})["local"] = {"DISABLE_RECOVERY_THREADS": True}
    retrieve = mocker.patch.object(Job, "retrieve_logfiles")
    mocker.patch.object(Job, "send_cpmip_notification")
    add_to_recover = mocker.patch.object(local, "add_job_to_log_recover")

    job = Job("t000", "0000", Status.COMPLETED, 0)
    job.name = "test_job"
    job.platform = local
    job.platform_name = "local"

    job.recover_log(as_conf)

    retrieve.assert_called_once()  # recovered inline
    add_to_recover.assert_not_called()  # nothing queued for a separate process
    assert local.log_recovery_process is None
    assert job.log_recovery_call_count == 1


def test_recover_pending_inline_recovers_logs(local, as_conf, mocker):
    """The inline fallback drives the real retrieval for every pending job and clears them."""
    recover_single = mocker.patch.object(local, "_recover_single_log")
    local._pending_recovery = {
        "job_a": _job_payload("job_a", 1),
        "job_b": _job_payload("job_b", 2),
    }

    local._recover_pending_inline(as_conf)

    assert recover_single.call_count == 2
    assert {call.args[0]["name"] for call in recover_single.call_args_list} == {"job_a", "job_b"}
    assert local._pending_recovery == {}


def test_inline_fallback_recovers_real_logs(mocker):
    """The inline fallback really recovers the pending logs (DB load + retrieval), without a child.

    Unlike ``test_recover_pending_inline_recovers_logs`` this does not mock ``_recover_single_log``:
    the job is persisted and loaded from the database, the retrieval runs for real, and the moved
    logs plus the persisted ``updated_log`` are asserted.
    """
    base = BasicConfig.LOCAL_ROOT_DIR
    Path(base, "t000", "db").mkdir(parents=True, exist_ok=True)
    platform = LocalPlatform("t000", "local", {"LOCAL_ROOT_DIR": base, "LOCAL_TMP_DIR": "tmp"})
    mocker.patch.object(Job, "stat_registered", return_value=False)
    mocker.patch.object(Job, "write_stats")
    mocker.patch.object(Job, "send_cpmip_notification")
    as_conf = mocker.MagicMock()
    as_conf.platforms_data = {"local": {}}
    job, log_dir = _prepare_real_recovery(base, platform)

    db = JobsDbManager(schema="t000")
    db.save_jobs([job])
    platform._pending_recovery = {"t000": _job_payload("t000", 123)}

    platform._recover_pending_inline(as_conf)

    assert platform._pending_recovery == {}
    assert (log_dir / "t000.20200101120000.out").exists()
    assert (log_dir / "t000.20200101120000.err").exists()
    assert not (log_dir / "t000.cmd.out.0").exists()
    assert db.load_job_by_name("t000")["updated_log"] == 1


def _persist_unrecovered_job(db, job):
    """Insert the job and persist ``updated_log`` (the insert upsert excludes the log columns)."""
    db.save_jobs([job])
    db.save_job_log(job)


def test_filter_pending_keeps_unrecovered_drops_recovered():
    """The filter keeps jobs the DB still reports as needing recovery, and drops the recovered ones."""
    base = BasicConfig.LOCAL_ROOT_DIR
    Path(base, "t000", "db").mkdir(parents=True, exist_ok=True)
    platform = LocalPlatform("t000", "local", {"LOCAL_ROOT_DIR": base, "LOCAL_TMP_DIR": "tmp"})

    needing = Job("needing", "1", Status.COMPLETED, 0)
    needing.fail_count = 0
    needing.updated_log = 0
    recovered = Job("recovered", "2", Status.COMPLETED, 0)
    recovered.fail_count = 0
    recovered.updated_log = 1
    db = JobsDbManager(schema="t000")
    db.save_jobs([needing, recovered])
    db.save_job_log(needing)
    db.save_job_log(recovered)

    platform._pending_recovery = {
        "needing": _job_payload("needing", 1),
        "recovered": _job_payload("recovered", 2),
    }

    assert set(platform._filter_pending_needing_recovery()) == {"needing"}


def test_filter_pending_keeps_all_when_db_unavailable(local, mocker):
    """When the database cannot be queried the filter keeps every pending job (fail-safe, no loss)."""
    local._pending_recovery = {"a": _job_payload("a", 1), "b": _job_payload("b", 2)}
    mocker.patch("autosubmit.platforms.platform.JobsDbManager", side_effect=Exception("db down"))

    assert local._filter_pending_needing_recovery() == local._pending_recovery


def test_recovery_persisted_before_crash_is_not_requeued(mocker):
    """Once the recovery is persisted, the filter drops the job instead of re-queuing it (W3)."""
    base = BasicConfig.LOCAL_ROOT_DIR
    Path(base, "t000", "db").mkdir(parents=True, exist_ok=True)
    platform = LocalPlatform("t000", "local", {"LOCAL_ROOT_DIR": base, "LOCAL_TMP_DIR": "tmp"})
    mocker.patch.object(Job, "stat_registered", return_value=False)
    mocker.patch.object(Job, "write_stats")
    mocker.patch.object(Job, "send_cpmip_notification")
    as_conf = mocker.MagicMock()
    as_conf.platforms_data = {"local": {}}

    job, _ = _prepare_real_recovery(base, platform)
    db = JobsDbManager(schema="t000")
    _persist_unrecovered_job(db, job)
    job_data = _job_payload(job.name, job.id)

    platform._recover_single_log(job_data, db, as_conf)  # completes and persists
    assert db.load_job_by_name(job.name)["updated_log"] == 1

    platform._pending_recovery = {job.name: job_data}
    assert platform._filter_pending_needing_recovery() == {}


@pytest.mark.parametrize(
    "work_event, cleanup_event, recovery_queue_full, result",
    [
        (True, False, True, True),
        (True, False, False, True),
        (False, True, True, True),
        (False, True, False, True),
        (False, False, True, True),
        (False, False, False, False),
        (True, True, True, True),
    ],
    ids=[
        "w(T)|c(F)|rq(T)",
        "w(T)|c(F)|rq(F)",
        "w(F)|c(T)|rq(T)",
        "w(F)|c(T)|rq(F)",
        "w(F)|c(F)|rq(T)",
        "w(F)|c(F)|rq(F)",
        "w(T)|c(T)|rq(T)",
    ],
)
def test_wait_for_work(
    prepare_test,
    local,
    as_conf,
    mocker,
    cleanup_event,
    work_event,
    recovery_queue_full,
    result,
):
    mocker.patch(
        "autosubmit.platforms.platform.Platform.get_mp_context",
        return_value=mp.get_context("fork"),
    )
    local.config["LOG_RECOVERY_TIMEOUT"] = 2
    # This is needed, because in this occasion we are testing the function from the main process
    local.keep_alive_timeout = 2
    max_items = 1
    local.prepare_process()
    local.recovery_queue = CopyQueue(ctx=local.ctx)
    local.cleanup_event.set() if cleanup_event else local.cleanup_event.clear()
    local.work_event.set() if work_event else local.work_event.clear()
    if recovery_queue_full:
        for i in range(max_items):
            local.recovery_queue.put(Job("t000", f"000{i}", Status.COMPLETED, 0))
    process_log = local.wait_for_work()
    assert process_log == result


def test_unique_elements(local, mocker):
    mocker.patch(
        "autosubmit.platforms.platform.Platform.get_mp_context",
        return_value=mp.get_context("fork"),
    )
    max_items = 3
    local.prepare_process()
    local.recovery_queue = CopyQueue(ctx=local.ctx)
    for i in range(max_items):
        local.recovery_queue.put(Job(f"rng{i}", f"000{i}", Status.COMPLETED, 0))
    for i in range(max_items):
        local.recovery_queue.put(Job(f"rng2{i}", f"000{i}", Status.COMPLETED, 0))


@pytest.fixture()
def conf_dict(tmpdir_factory):
    temp = tmpdir_factory.mktemp("scheduler_tests")
    os.mkdir(temp.join("scratch"))
    os.mkdir(temp.join("scheduler_tmp_dir"))
    file_stat = os.stat(f"{temp.strpath}")
    file_owner_id = file_stat.st_uid
    file_owner = pwd.getpwuid(file_owner_id).pw_name
    temp.owner = file_owner
    return {
        "pytest-pjm": {
            "type": "pjm",
            "host": "127.0.0.1",
            "user": temp.owner,
            "project": "whatever",
            "scratch_dir": f"{temp}/scratch",
            "MAX_WALLCLOCK": "48:00",
            "TEMP_DIR": "",
            "MAX_PROCESSORS": 99999,
            "queue": "dummy",
            "DISABLE_RECOVERY_THREADS": True,
        },
        "pytest-slurm": {
            "type": "slurm",
            "host": "127.0.0.1",
            "user": temp.owner,
            "project": "whatever",
            "scratch_dir": f"{temp}/scratch",
            "QUEUE": "gp_debug",
            "ADD_PROJECT_TO_HOST": False,
            "MAX_WALLCLOCK": "48:00",
            "TEMP_DIR": "",
            "MAX_PROCESSORS": 99999,
            "PROCESSORS_PER_NODE": 123,
            "DISABLE_RECOVERY_THREADS": True,
        },
        "pytest-ecaccess": {
            "type": "ecaccess",
            "version": "slurm",
            "host": "127.0.0.1",
            "QUEUE": "nf",
            "EC_QUEUE": "hpc",
            "user": temp.owner,
            "project": "whatever",
            "scratch_dir": f"{temp}/scratch",
            "DISABLE_RECOVERY_THREADS": True,
        },
        "pytest-ps": {
            "type": "ps",
            "host": "127.0.0.1",
            "user": temp.owner,
            "project": "whatever",
            "scratch_dir": f"{temp}/scratch",
            "DISABLE_RECOVERY_THREADS": True,
        },
        "LOCAL_ROOT_DIR": f"{temp}/scratch",
        "LOCAL_TMP_DIR": f"{temp}/scratch",
        "LOCAL_ASLOG_DIR": f"{temp}/scratch",
    }


@pytest.fixture
def pjm(prepare_test, conf_dict):
    from autosubmit.platforms.pjmplatform import PJMPlatform

    pjm = PJMPlatform(expid="t000", name="pytest-pjm", config=conf_dict)
    return pjm


@pytest.fixture
def slurm(prepare_test, conf_dict):
    from autosubmit.platforms.slurmplatform import SlurmPlatform

    slurm = SlurmPlatform(expid="t000", name="pytest-slurm", config=conf_dict)
    return slurm


@pytest.fixture
def ecaccess(prepare_test, conf_dict):
    from autosubmit.platforms.ecplatform import EcPlatform

    ecaccess = EcPlatform(
        expid="t000", name="pytest-ecaccess", config=conf_dict, scheduler="slurm"
    )
    return ecaccess


def test_create_a_new_copy(local, pjm, slurm, ps_platform, ecaccess):
    assert local.create_a_new_copy().name == local.name
    assert pjm.create_a_new_copy().name == pjm.name
    assert slurm.create_a_new_copy().name == slurm.name
    assert ps_platform.create_a_new_copy().name == ps_platform.name
    assert ecaccess.create_a_new_copy().name == ecaccess.name


def test_worker_events_no_duplicates(local, mocker):
    """Verify that update_workers() does not add the same event twice."""
    mocker.patch(
        "autosubmit.platforms.platform.Platform.get_mp_context",
        return_value=mp.get_context("fork"),
    )
    Platform.worker_events.clear()
    local.prepare_process()
    # prepare_process adds one event; verify adding the same one again doesn't duplicate
    count_after_prepare = len(Platform.worker_events)
    assert count_after_prepare >= 1
    Platform.update_workers(local.work_event)
    assert len(Platform.worker_events) == count_after_prepare
    Platform.worker_events.clear()


def test_add_job_to_log_recover_failure_does_not_increment_updated_log(local, mocker):
    """Verify that when recovery_queue.put fails, updated_log is NOT incremented."""
    mocker.patch(
        "autosubmit.platforms.platform.Platform.get_mp_context",
        return_value=mp.get_context("fork"),
    )
    local.prepare_process()
    local.recovery_queue = CopyQueue(ctx=local.ctx)
    job = Job("t000", "0000", Status.COMPLETED, 0)
    job.name = "test_job"
    job.id = 123
    job.updated_log = 0
    local.log_recovery_process = mocker.MagicMock()
    local.log_recovery_process.is_alive.return_value = True

    # Patch put_raw to simulate a failure
    mocker.patch.object(
        local.recovery_queue, "put_raw", side_effect=Exception("Queue full")
    )
    local.add_job_to_log_recover(job)
    assert job.updated_log == 0


def test_add_job_to_log_recover_no_queue(local):
    """Verify graceful skip when recovery_queue is None."""
    local.recovery_queue = None
    job = Job("t000", "0000", Status.COMPLETED, 0)
    job.name = "test_job"
    job.id = 123
    job.updated_log = 0
    # Should not raise and should not increment updated_log
    local.add_job_to_log_recover(job)
    assert job.updated_log == 0


def test_clean_log_recovery_process_with_dead_child(local, as_conf, mocker):
    """Verify cleanup does not hang when child has crashed."""
    mocker.patch(
        "autosubmit.platforms.platform.Platform.get_mp_context",
        return_value=mp.get_context("fork"),
    )
    local.config["LOG_RECOVERY_TIMEOUT"] = 1
    local.spawn_log_retrieval_process(as_conf)
    assert local.log_recovery_process.is_alive()
    # Abruptly kill the child to simulate a crash
    local.log_recovery_process.kill()
    local.log_recovery_process.join(timeout=5)
    assert not local.log_recovery_process.is_alive()
    # Cleanup should return promptly without hanging
    local.clean_log_recovery_process()
    assert local.recovery_queue is None
    assert local.log_recovery_process is None


def test_add_job_to_log_recover_signals_work_event(local, mocker):
    """Verify work_event is set after adding a job to the recovery queue."""
    local.prepare_process()
    local.recovery_queue = CopyQueue(ctx=local.ctx)
    local.log_recovery_process = mocker.MagicMock()
    local.log_recovery_process.is_alive.return_value = True

    job = Job("t000", "0000", Status.COMPLETED, 0)
    job.name = "test_job"
    job.id = 123
    job.updated_log = 0
    job.updated_stats = 0

    local.add_job_to_log_recover(job)
    assert local.work_event.is_set()


def test_add_job_to_log_recover_respawns_dead_process(prepare_test, local, as_conf, mocker):
    """Issue #3342: a dead log recovery process must be respawned, not silently dropped."""
    mocker.patch(
        "autosubmit.platforms.platform.Platform.get_mp_context",
        return_value=mp.get_context("fork"),
    )
    local.config["LOG_RECOVERY_TIMEOUT"] = 10
    local.spawn_log_retrieval_process(as_conf)
    assert local.log_recovery_process.is_alive()

    # Simulate an unexpected crash of the recovery child.
    local.log_recovery_process.kill()
    local.log_recovery_process.join(timeout=5)
    assert not local.log_recovery_process.is_alive()
    dead_pid = local.log_recovery_process.pid

    job = Job("t000", "0000", Status.COMPLETED, 0)
    job.name = "test_job"
    job.id = 123
    job.updated_log = 0

    local.add_job_to_log_recover(job, as_conf)

    assert local.log_recovery_process is not None
    assert local.log_recovery_process.is_alive()
    assert local.log_recovery_process.pid != dead_pid

    local.cleanup_event.set()
    local.log_recovery_process.join(timeout=30)
    assert not local.log_recovery_process.is_alive()


def _job_payload(name: str, job_id: int = 1) -> dict:
    return {
        "id": job_id,
        "name": name,
        "fail_count": 0,
        "submit_time_timestamp": 0,
        "start_time_timestamp": 0,
        "finish_time_timestamp": 0,
        "wrapper_type": None,
    }


def _set_runtime_log_recovery(as_conf, **options) -> None:
    as_conf.experiment_data["RUNTIME"] = {"LOG_RECOVERY": options}


@pytest.fixture
def failing_spawn(local, mocker):
    """Patch ``spawn_log_retrieval_process`` so restarting the recovery process always fails."""

    def _failed_spawn(*_args, **_kwargs):
        local.log_recovery_process = None
        return False

    return mocker.patch.object(local, "spawn_log_retrieval_process", side_effect=_failed_spawn)


def test_respawn_requeues_pending_jobs_without_loss(prepare_test, local, as_conf, mocker):
    """Jobs pending when the process dies are re-queued into the new process (issue #3342)."""
    ctx = mp.get_context("fork")
    local.ctx = ctx
    local._log_recovery_started = True
    local._recovery_consecutive_failures = 0
    local.work_event = ctx.Event()
    local.cleanup_event = ctx.Event()
    local.recovery_queue = CopyQueue(ctx=ctx)
    # A job that was enqueued but never confirmed as recovered.
    local._pending_recovery = {"pending_job": _job_payload("pending_job", 42)}
    local.log_recovery_process = mocker.MagicMock()
    local.log_recovery_process.is_alive.return_value = False

    # Avoid hitting the database: keep every pending job.
    mocker.patch.object(
        local, "_filter_pending_needing_recovery",
        side_effect=lambda: dict(local._pending_recovery),
    )

    def _fake_spawn(*_args, **_kwargs):
        local.recovery_queue = CopyQueue(ctx=ctx)
        local.log_recovery_process = mocker.MagicMock()
        local.log_recovery_process.is_alive.return_value = True
        return True

    mocker.patch.object(local, "spawn_log_retrieval_process", side_effect=_fake_spawn)

    assert local._respawn_log_recovery_process(as_conf) is True

    assert "pending_job" in local._pending_recovery
    requeued = local.recovery_queue.get(timeout=5)
    assert requeued["name"] == "pending_job"


@pytest.mark.usefixtures("failing_spawn")
@pytest.mark.parametrize("inline_fallback", [True, False], ids=["inline_fallback_on", "inline_fallback_off"])
def test_consecutive_failures_enter_fallback(
    prepare_test, local, as_conf, mocker, inline_fallback,
):
    """Too many consecutive restart failures enter fallback; retry is scheduled only with inline fallback."""
    _set_runtime_log_recovery(
        as_conf, CONSECUTIVE_MAX_RESTARTS=1, INLINE_FALLBACK=inline_fallback, RETRY_DELAY=123,
    )
    local._log_recovery_started = True
    local._recovery_consecutive_failures = 1
    local.ctx = mp.get_context("fork")
    local.recovery_queue = CopyQueue(ctx=local.ctx)

    assert local._respawn_log_recovery_process(as_conf) is False
    assert local._log_recovery_disabled is True
    if inline_fallback:
        assert local._recovery_consecutive_failures == 0
        assert local._log_recovery_retry_at > time.time()
    else:
        assert local._log_recovery_retry_at == float("inf")


@pytest.mark.usefixtures("failing_spawn")
@pytest.mark.parametrize("inline_fallback", [True, False], ids=["inline_fallback_on", "inline_fallback_off"])
def test_add_job_falls_back_inline_when_disabled(
    prepare_test, local, as_conf, mocker, inline_fallback,
):
    """When restart fails, jobs are recovered inline only if INLINE_FALLBACK is enabled."""
    _set_runtime_log_recovery(
        as_conf, CONSECUTIVE_MAX_RESTARTS=1, INLINE_FALLBACK=inline_fallback,
    )
    local._log_recovery_started = True
    local.ctx = mp.get_context("fork")
    local.recovery_queue = CopyQueue(ctx=local.ctx)
    local.log_recovery_process = None

    job = Job("t000", "0000", Status.COMPLETED, 0)
    job.name = "test_job"
    job.id = 123

    assert local.add_job_to_log_recover(job, as_conf) is inline_fallback


def test_first_start_failure_is_fatal(prepare_test, local, as_conf, mocker):
    """The initial start of the recovery process must hard-crash when it cannot start."""
    mocker.patch.object(
        local, "create_new_process",
        side_effect=lambda *_args, **_kwargs: setattr(local, "log_recovery_process", None),
    )

    with pytest.raises(AutosubmitCritical):
        local.spawn_log_retrieval_process(as_conf)

