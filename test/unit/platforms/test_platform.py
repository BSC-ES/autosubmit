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

"""This file contains tests for the ``platform``."""

import multiprocessing as mp
import time
from pathlib import Path
from queue import Queue
from types import SimpleNamespace

import psutil
import pytest

from autosubmit.autosubmit import Autosubmit
from autosubmit.job.job import Job, RecoveryAttempt, RecoveryReport
from autosubmit.job.job_common import Status
from autosubmit.log.log import AutosubmitCritical, Log
from autosubmit.platforms.locplatform import LocalPlatform
from autosubmit.platforms.platform import CopyQueue, recover_platform_job_logs_wrapper
from test.unit.test_job import TestJob, FakeBasicConfig

_EXPID = 't000'


@pytest.mark.parametrize(
    'file_exists,count ',
    [
        [True, -1],
        [True, 0],
        [True, 1],
        [False, -1],
        [False, 0],
        [False, 1],
    ]
)
def test_get_stat_file(file_exists, count, tmp_path):
    """
    This test will test the local platform that uses the get_stat_file from the mother class.
    This test forces to execute create and delete files checking if the file was transferred from platform to local.
    """

    basic_config = FakeBasicConfig()
    basic_config.LOCAL_ROOT_DIR = str(tmp_path)
    basic_config.LOCAL_TMP_DIR = str(tmp_path)

    job = TestJob()
    job.stat_file = "test_file"
    job.name = "test_name"
    if count < 0:
        job.fail_count = 0
        filename = job.stat_file + "0"
    else:
        job.fail_count = count
        filename = job.name + f'_STAT_{str(count)}'

    if file_exists:
        with open(f"{basic_config.LOCAL_ROOT_DIR}/{filename}", "w", encoding="utf-8") as f:
            f.write("dummy content")
            f.flush()
        Path(f"{basic_config.LOCAL_ROOT_DIR}/LOG_t000/").mkdir()
        with open(f"{basic_config.LOCAL_ROOT_DIR}/LOG_t000/{filename}", "w", encoding="utf-8") as f:
            f.write("dummy content")
            f.flush()

    platform = LocalPlatform("t000", 'platform', basic_config.props())
    assert Path(f"{basic_config.LOCAL_ROOT_DIR}/{filename}").exists() == file_exists
    assert Path(f"{basic_config.LOCAL_ROOT_DIR}/LOG_t000/{filename}").exists() == file_exists
    assert platform.get_stat_file(job, count) == file_exists


def test_local_platform_read_file(tmp_path):
    basic_config = FakeBasicConfig()
    basic_config.LOCAL_ROOT_DIR = str(tmp_path)
    basic_config.LOCAL_TMP_DIR = str(tmp_path)

    platform = LocalPlatform("t001", "platform", basic_config.props())

    path_not_exists = Path(tmp_path).joinpath("foo", "bar")

    assert platform.get_file_size(path_not_exists) is None
    assert platform.read_file(path_not_exists) is None


def test_init_logs_log_process_no_root_dir(mocker, autosubmit_config):
    """This test ignores the platform, and only tests the log initialization when no root dir configured."""
    as_conf = autosubmit_config(_EXPID, experiment_data={
        'CONFIG': {
            'LOG_RECOVERY_CONSOLE_LEVEL': 'NO_LOG'
        }
    })

    platform = mocker.MagicMock()
    mocker.patch('autosubmit.platforms.platform._exit', return_value=0)
    recover_platform_job_logs_wrapper(
        platform, None, None, None, as_conf=as_conf, recovery_results=None)  # type: ignore

    assert Log.console_handler.level == Log.NO_LOG


def test_init_logs_log_process_with_root_dir(mocker, autosubmit_config):
    """This test ignores the platform, and only tests the log initialization when a root dir is provided."""
    as_conf = autosubmit_config(_EXPID, experiment_data={
        'CONFIG': {
            'LOG_RECOVERY_CONSOLE_LEVEL': 'NO_LOG'
        }
    })
    # TODO: We need to create it first to get a ``BasicConfig``, with Dani's improvement
    #       that may not be necessary in the future!
    as_conf.experiment_data['ROOTDIR'] = as_conf.basic_config.expid_dir(as_conf.expid)

    platform = mocker.MagicMock()
    platform.name = 'parrot'

    mocker.patch('autosubmit.platforms.platform._exit', return_value=0)

    current_number_of_handlers = len(Log.log.handlers)

    recover_platform_job_logs_wrapper(
        platform, None, None, None, as_conf=as_conf, recovery_results=None)  # type: ignore

    assert len(Log.log.handlers) == current_number_of_handlers + 2  # + out + err


def _recovery_platform(as_conf, *, alive=True):
    """Create a LocalPlatform wired for log recovery tests."""
    ctx = mp.get_context("fork")
    platform = LocalPlatform("t000", "test_platform", {})
    platform.recovery_queue = CopyQueue(ctx=ctx)
    platform.work_event = ctx.Event()
    platform.log_recovery_process = SimpleNamespace(is_alive=lambda: alive)
    as_conf.misc_data = {"AS_COMMAND": "run"}
    return platform


def test_add_job_to_log_recover_queues_and_tracks_pending(mocker, autosubmit_config):
    """add_job_to_log_recover queues the job, signals work and tracks it as pending."""
    as_conf = autosubmit_config("t000", {"PLATFORMS": {"test_platform": {}}})
    platform = _recovery_platform(as_conf)
    job = Job("test_job", 123, Status.COMPLETED, 0)

    try:
        assert platform.add_job_to_log_recover(job, as_conf) is True

        queued = platform.recovery_queue.get(timeout=5)
        assert queued["_name"] == "test_job"
        assert platform.work_event.is_set()
        assert platform._pending_recovery[job.name] == queued
    finally:
        platform.recovery_queue.cancel_join_thread()
        platform.recovery_queue.close()


def test_add_job_to_log_recover_falls_back_inline_when_unavailable(mocker, autosubmit_config):
    """When the recovery process cannot be started, the job is recovered inline."""
    as_conf = autosubmit_config("t000", {"PLATFORMS": {"test_platform": {}}})
    platform = _recovery_platform(as_conf, alive=False)
    job = Job("test_job", 123, Status.COMPLETED, 0)
    mocker.patch.object(platform, "clean_log_recovery_process")
    mocker.patch.object(platform, "spawn_log_retrieval_process", return_value=False)
    inline = mocker.patch.object(platform, "recover_log_inline", return_value=True)

    assert platform.add_job_to_log_recover(job, as_conf) is True
    inline.assert_called_once_with(job, as_conf)


def test_try_respawn_enters_backoff_and_recovers_inline(mocker, autosubmit_config):
    """After the consecutive restart limit, it backs off and recovers pending logs inline."""
    as_conf = autosubmit_config("t000", {
        "RUNTIME": {"LOG_RECOVERY": {"CONSECUTIVE_MAX_RESTARTS": 1, "RETRY_DELAY": 60, "INLINE_FALLBACK": True}},
        "PLATFORMS": {"test_platform": {}},
    })
    platform = _recovery_platform(as_conf, alive=False)
    mocker.patch.object(platform, "clean_log_recovery_process")
    spawn = mocker.patch.object(platform, "spawn_log_retrieval_process", return_value=False)
    pending_inline = mocker.patch.object(platform, "_recover_pending_inline")

    assert platform.try_respawn_log_recovery_process(as_conf) is False
    assert platform._log_recovery_in_backoff is True
    pending_inline.assert_called_once_with(as_conf)

    spawn.reset_mock()
    assert platform.try_respawn_log_recovery_process(as_conf) is False
    spawn.assert_not_called()


def test_try_respawn_requeues_pending_jobs(mocker, autosubmit_config):
    """A successful respawn re-queues the jobs that were pending."""
    as_conf = autosubmit_config("t000", {"PLATFORMS": {"test_platform": {}}})
    platform = _recovery_platform(as_conf, alive=False)
    job = Job("test_job", 123, Status.COMPLETED, 0)
    platform._pending_recovery = {job.name: job.__getstate__()}
    mocker.patch.object(platform, "clean_log_recovery_process")
    mocker.patch.object(platform, "spawn_log_retrieval_process", return_value=True)

    assert platform.try_respawn_log_recovery_process(as_conf) is True

    queued = platform.recovery_queue.get(timeout=5)
    assert queued["_name"] == "test_job"
    platform.recovery_queue.cancel_join_thread()
    platform.recovery_queue.close()


@pytest.mark.parametrize(
    "all_succeeded,errors,warning_expected,result_expected",
    [
        (True, [], False, False),
        (True, [None, None], False, True),
        (False, ["boom"], True, False),
        (False, ["Remote logs not found"], False, False),
    ],
    ids=["no-attempts", "all-succeeded", "failed-attempt", "remote-not-found"],
)
def test_recover_job_reports_attempts(mocker, all_succeeded, errors, warning_expected, result_expected):
    """_recover_job reports failed attempts and only logs recovered attempts when there are some."""
    platform = LocalPlatform("t000", "test_platform", {})
    attempts = [
        RecoveryAttempt(
            attempt=index,
            success=error is None,
            local_logs=("", ""),
            remote_logs=("", ""),
            error=error,
        )
        for index, error in enumerate(errors)
    ]
    report = RecoveryReport(job_name="test_job", attempts=attempts, all_succeeded=all_succeeded)
    job = SimpleNamespace(
        name="test_job",
        retrieve_logfiles=lambda: report,
        send_cpmip_notification=lambda as_conf: None,
    )
    warning = mocker.patch.object(Log, "warning")
    result = mocker.patch.object(Log, "result")

    platform._recover_job(job, object())

    if warning_expected:
        warning.assert_called_once()
    else:
        warning.assert_not_called()
    if result_expected:
        result.assert_called_once()
    else:
        result.assert_not_called()


def test_recover_job_log_continues_after_a_failed_job(mocker):
    """A failed job does not stop the recovery of the remaining queued jobs."""
    platform = LocalPlatform("t000", "test_platform", {})
    platform.recovery_queue = Queue()
    platform.recovery_queue.put(Job("job_a", 1, Status.COMPLETED, 0).__getstate__())
    platform.recovery_queue.put(Job("job_b", 2, Status.COMPLETED, 0).__getstate__())
    platform._as_conf = object()
    recover = mocker.patch.object(platform, "_recover_job", side_effect=[Exception("boom"), None])
    restore = mocker.patch.object(platform, "restore_connection")
    warning = mocker.patch.object(Log, "warning")
    mocker.patch.object(Log, "debug")

    platform.recover_job_log()

    assert recover.call_count == 2
    warning.assert_called_once()
    restore.assert_called_once_with(platform._as_conf, log_recovery_process=True)


def _restore_platforms_mock(mocker, *, connected=True, should_run=True, respawn_ok=False):
    platform = mocker.MagicMock()
    platform.name = "local"
    platform.host = "localhost"
    platform.connected = connected
    platform.test_connection.return_value = None
    platform.check_remote_permissions.return_value = True
    platform.should_run_log_recovery.return_value = should_run
    platform.try_respawn_log_recovery_process.return_value = respawn_ok
    return platform


@pytest.mark.parametrize(
    "fail_on_init_error,connected,should_run,respawn_ok,should_raise",
    [
        (True, True, True, False, True),
        (True, True, True, True, False),
        (False, True, True, False, False),
        (True, True, False, False, False),
        (True, False, True, False, False),
    ],
    ids=["init-error-missing", "init-error-running", "reconnect-missing", "recovery-disabled", "not-connected"],
)
def test_restore_platforms_enforces_log_recovery_on_init(
    mocker, fail_on_init_error, connected, should_run, respawn_ok, should_raise
):
    """Log recovery must be running after the initial connect when it is enabled."""
    as_conf = mocker.MagicMock()
    platform = _restore_platforms_mock(
        mocker, connected=connected, should_run=should_run, respawn_ok=respawn_ok
    )

    if should_raise:
        with pytest.raises(AutosubmitCritical) as exc_info:
            Autosubmit.restore_platforms([platform], as_conf=as_conf, fail_on_init_error=fail_on_init_error)
        assert exc_info.value.code == 6008
    else:
        Autosubmit.restore_platforms([platform], as_conf=as_conf, fail_on_init_error=fail_on_init_error)

    if fail_on_init_error and connected and should_run:
        platform.try_respawn_log_recovery_process.assert_called_once_with(as_conf)
    else:
        platform.try_respawn_log_recovery_process.assert_not_called()


def test_requeue_uses_pre_increment_snapshot(mocker, autosubmit_config):
    """A re-queued job keeps the updated_log value from enqueue time."""
    as_conf = autosubmit_config("t000", {"PLATFORMS": {"test_platform": {}}})
    platform = _recovery_platform(as_conf, alive=False)
    job = Job("test_job", 123, Status.COMPLETED, 0)
    job.fail_count = 1
    job.updated_log = 0
    mocker.patch.object(platform, "clean_log_recovery_process")
    mocker.patch.object(platform, "spawn_log_retrieval_process", return_value=True)

    try:
        assert platform.add_job_to_log_recover(job, as_conf) is True
        original = platform.recovery_queue.get(timeout=5)
        assert Job(loaded_data=original).updated_log == 0

        # The caller (_recover_log) marks the job after a successful queue.
        job.updated_log = job.fail_count + 1
        assert job.updated_log == 2

        # Simulate the recovery process dying and being respawned.
        platform.log_recovery_process = None
        assert platform.try_respawn_log_recovery_process(as_conf) is True

        queued = platform.recovery_queue.get(timeout=5)
        assert Job(loaded_data=queued).updated_log == 0
    finally:
        platform.recovery_queue.cancel_join_thread()
        platform.recovery_queue.close()


def test_ack_removes_pending_job(mocker, autosubmit_config):
    """The child acknowledges a processed job and the parent stops tracking it."""
    as_conf = autosubmit_config("t000", {"PLATFORMS": {"test_platform": {}}})
    platform = _recovery_platform(as_conf)
    platform.recovery_results = Queue()
    platform._pending_recovery = {"job": {"_name": "job"}}

    platform.recovery_results.put("job")
    platform._drain_recovery_results()

    assert "job" not in platform._pending_recovery


def test_missing_ack_keeps_pending_job(mocker, autosubmit_config):
    """A job that was not acknowledged stays pending so it can be re-queued."""
    as_conf = autosubmit_config("t000", {"PLATFORMS": {"test_platform": {}}})
    platform = _recovery_platform(as_conf)
    platform.recovery_results = Queue()
    platform._pending_recovery = {"job": {"_name": "job"}}

    platform._drain_recovery_results()

    assert "job" in platform._pending_recovery


def test_dead_process_counts_as_failure_and_backs_off(mocker, autosubmit_config):
    """A process that dies shortly after starting counts towards the restart limit."""
    as_conf = autosubmit_config("t000", {
        "RUNTIME": {"LOG_RECOVERY": {"CONSECUTIVE_MAX_RESTARTS": 1, "RETRY_DELAY": 60, "INLINE_FALLBACK": False}},
        "PLATFORMS": {"test_platform": {}},
    })
    platform = _recovery_platform(as_conf, alive=False)
    platform._log_recovery_started_at = time.time() - 1
    mocker.patch.object(platform, "clean_log_recovery_process")
    spawn = mocker.patch.object(platform, "spawn_log_retrieval_process", return_value=False)

    assert platform.try_respawn_log_recovery_process(as_conf) is False
    assert platform._log_recovery_in_backoff is True
    spawn.assert_not_called()


def test_long_lived_dead_process_does_not_back_off(mocker, autosubmit_config):
    """A process that died after living long enough is not treated as a crash loop."""
    as_conf = autosubmit_config("t000", {
        "RUNTIME": {"LOG_RECOVERY": {"CONSECUTIVE_MAX_RESTARTS": 1, "RETRY_DELAY": 60, "INLINE_FALLBACK": False}},
        "PLATFORMS": {"test_platform": {}},
    })
    platform = _recovery_platform(as_conf, alive=False)
    platform._recovery_consecutive_failures = 1
    platform._log_recovery_started_at = time.time() - 60
    mocker.patch.object(platform, "clean_log_recovery_process")
    spawn = mocker.patch.object(platform, "spawn_log_retrieval_process", return_value=True)

    assert platform.try_respawn_log_recovery_process(as_conf) is True
    assert platform._log_recovery_in_backoff is False
    spawn.assert_called_once()


def test_healthy_process_resets_consecutive_failures(mocker, autosubmit_config):
    """A process that survives the grace period resets the failure counter."""
    as_conf = autosubmit_config("t000", {"PLATFORMS": {"test_platform": {}}})
    platform = _recovery_platform(as_conf)
    platform._recovery_consecutive_failures = 3
    platform._log_recovery_started_at = time.time() - 60

    assert platform.try_respawn_log_recovery_process(as_conf) is True
    assert platform._recovery_consecutive_failures == 0


def test_backoff_expires_and_retries(mocker, autosubmit_config):
    """Once RETRY_DELAY passes, a new spawn is attempted."""
    as_conf = autosubmit_config("t000", {"PLATFORMS": {"test_platform": {}}})
    platform = _recovery_platform(as_conf, alive=False)
    platform._log_recovery_in_backoff = True
    platform._log_recovery_retry_at = time.time() - 1
    mocker.patch.object(platform, "clean_log_recovery_process")
    spawn = mocker.patch.object(platform, "spawn_log_retrieval_process", return_value=False)

    assert platform.try_respawn_log_recovery_process(as_conf) is False
    assert platform._log_recovery_in_backoff is False
    spawn.assert_called_once()


def test_zero_max_restarts_never_backs_off(mocker, autosubmit_config):
    """CONSECUTIVE_MAX_RESTARTS=0 disables the restart limit."""
    as_conf = autosubmit_config("t000", {
        "RUNTIME": {"LOG_RECOVERY": {"CONSECUTIVE_MAX_RESTARTS": 0}},
        "PLATFORMS": {"test_platform": {}},
    })
    platform = _recovery_platform(as_conf, alive=False)
    platform._log_recovery_started_at = time.time() - 60
    platform._recovery_consecutive_failures = 10
    mocker.patch.object(platform, "clean_log_recovery_process")
    mocker.patch.object(platform, "spawn_log_retrieval_process", return_value=False)

    assert platform.try_respawn_log_recovery_process(as_conf) is False
    assert platform._log_recovery_in_backoff is False


def test_should_run_log_recovery_requires_run_command(mocker, autosubmit_config):
    """Recovery threads are only expected for the ``run`` command."""
    as_conf = autosubmit_config("t000", {"PLATFORMS": {"test_platform": {}}})
    platform = _recovery_platform(as_conf)
    as_conf.misc_data = {"AS_COMMAND": "monitor"}

    assert platform.should_run_log_recovery(as_conf) is False


@pytest.mark.parametrize("job_id", [None, "", 0, "0", "not-a-number"], ids=["none", "empty", "zero", "zero-str", "non-numeric"])
def test_add_job_to_log_recover_rejects_invalid_ids(mocker, autosubmit_config, job_id):
    """Invalid job ids are skipped without raising."""
    as_conf = autosubmit_config("t000", {"PLATFORMS": {"test_platform": {}}})
    platform = _recovery_platform(as_conf)
    job = Job("test_job", job_id, Status.COMPLETED, 0)

    assert platform.add_job_to_log_recover(job, as_conf) is False
    assert job.updated_log == 1


def test_recover_log_inline_reports_failure(mocker):
    """Inline recovery reports failure when an attempt did not succeed."""
    platform = LocalPlatform("t000", "test_platform", {})
    report = RecoveryReport(
        job_name="job",
        attempts=[RecoveryAttempt(0, False, ("", ""), ("", ""), error="boom")],
        all_succeeded=False,
    )
    job = SimpleNamespace(
        name="job",
        retrieve_logfiles=lambda: report,
        send_cpmip_notification=lambda as_conf: None,
    )
    mocker.patch.object(Log, "warning")

    assert platform.recover_log_inline(job, object()) is False


def test_join_new_process_does_not_use_raw_waitpid(mocker):
    """Reaping goes through multiprocessing so is_alive()/exitcode stay accurate."""
    platform = LocalPlatform("t000", "test_platform", {})
    process = mocker.MagicMock()
    process.pid = 123
    process.name = "child"
    process.is_alive.return_value = False
    process.exitcode = 0
    platform.log_recovery_process = process
    waitpid = mocker.patch("autosubmit.platforms.platform.os.waitpid")

    platform.join_new_process()

    waitpid.assert_not_called()
    process.is_alive.assert_called_once()


def _noop():
    pass


def test_join_new_process_reaps_exited_child():
    """After join_new_process, an exited child is reaped and is_alive() is false."""
    try:
        ctx = mp.get_context("fork")
    except ValueError:
        pytest.skip("The fork context is not available on this platform")

    process = ctx.Process(target=_noop)
    process.start()
    platform = LocalPlatform("t000", "test_platform", {})
    platform.log_recovery_process = process
    try:
        deadline = time.time() + 5
        status = None
        while time.time() < deadline:
            try:
                status = psutil.Process(process.pid).status()
            except psutil.NoSuchProcess:
                break
            if status == psutil.STATUS_ZOMBIE:
                break
            time.sleep(0.05)
        if status != psutil.STATUS_ZOMBIE:
            pytest.skip("The child was not observed as a zombie on this platform")

        platform.join_new_process()

        assert process.is_alive() is False
        assert process.exitcode == 0
        with pytest.raises(psutil.NoSuchProcess):
            psutil.Process(process.pid).status()
    finally:
        if process.is_alive():
            process.kill()
        process.join(timeout=5)


@pytest.mark.parametrize(
    "min_alive_seconds,elapsed,expected_failures",
    [
        (1.0, 5.0, 0),
        (10.0, 5.0, 3),
    ],
    ids=["alive-enough", "not-alive-enough"],
)
def test_min_alive_seconds_is_configurable(
    mocker, autosubmit_config, min_alive_seconds, elapsed, expected_failures
):
    """The grace period comes from RUNTIME.LOG_RECOVERY.MIN_ALIVE_SECONDS."""
    as_conf = autosubmit_config("t000", {
        "RUNTIME": {"LOG_RECOVERY": {"MIN_ALIVE_SECONDS": min_alive_seconds}},
        "PLATFORMS": {"test_platform": {}},
    })
    platform = _recovery_platform(as_conf)
    platform._recovery_consecutive_failures = 3
    platform._log_recovery_started_at = 1000.0 - elapsed
    mocker.patch("autosubmit.platforms.platform.time.time", return_value=1000.0)

    assert platform.try_respawn_log_recovery_process(as_conf) is True
    assert platform._recovery_consecutive_failures == expected_failures


def test_recovery_queue_is_bounded_by_log_queue_size():
    """LOG_RECOVERY_QUEUE_SIZE is used as the recovery queue capacity."""
    platform = LocalPlatform("t000", "test_platform", {})
    platform.log_queue_size = 7
    platform.prepare_process()
    try:
        assert platform.recovery_queue._maxsize == 7
    finally:
        platform.clean_log_recovery_process()


def test_drain_multiple_acks_from_simple_queue(mocker, autosubmit_config):
    """The parent drains every acknowledgement from the real SimpleQueue."""
    as_conf = autosubmit_config("t000", {"PLATFORMS": {"test_platform": {}}})
    platform = _recovery_platform(as_conf)
    ctx = mp.get_context("fork")
    platform.recovery_results = ctx.SimpleQueue()
    platform._pending_recovery = {f"job{i}": {"_name": f"job{i}"} for i in range(3)}
    try:
        for index in range(3):
            platform.recovery_results.put(f"job{index}")
        platform._drain_recovery_results()
        assert platform._pending_recovery == {}
    finally:
        platform.recovery_results.close()


def test_recover_inline_keeps_pending_on_failure(mocker, autosubmit_config):
    """A failed inline recovery must keep the snapshot for a later retry."""
    as_conf = autosubmit_config("t000", {
        "RUNTIME": {"LOG_RECOVERY": {"INLINE_FALLBACK": True}},
        "PLATFORMS": {"test_platform": {}},
    })
    platform = _recovery_platform(as_conf)
    job = Job("test_job", 123, Status.COMPLETED, 0)
    platform._pending_recovery = {job.name: job.__getstate__()}
    mocker.patch.object(platform, "recover_log_inline", return_value=False)

    assert platform._recover_inline(job, as_conf) is False
    assert job.name in platform._pending_recovery


def test_invalid_id_marks_job_as_fully_attempted(mocker, autosubmit_config):
    """An invalid id marks every attempt as handled to avoid repeated warnings."""
    as_conf = autosubmit_config("t000", {"PLATFORMS": {"test_platform": {}}})
    platform = _recovery_platform(as_conf)
    job = Job("test_job", "not-a-number", Status.COMPLETED, 0)
    job.fail_count = 2

    assert platform.add_job_to_log_recover(job, as_conf) is False
    assert job.updated_log == 3
