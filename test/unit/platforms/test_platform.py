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
import queue
import time
from multiprocessing import connection
from pathlib import Path
from queue import Queue
from types import SimpleNamespace

import psutil
import pytest

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.job.job import Job, RecoveryAttempt, RecoveryReport
from autosubmit.job.job_common import Status
from autosubmit.log.log import AutosubmitCritical, Log
from autosubmit.platforms.locplatform import LocalPlatform
from autosubmit.platforms.manage import restore_platforms
from autosubmit.platforms.platform import CopyQueue, recover_platform_job_logs_wrapper
from test.unit.test_job import FakeBasicConfig, TestJob

_EXPID = 't000'


@pytest.mark.parametrize(
    'file_exists,count ',
    [
        [True, 0],
        [True, 1],
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
    basic_config.LOCAL_TMP_DIR = "tmp"

    job = TestJob()
    job.stat_file = "test_file"
    job.name = "test_name"
    job.fail_count = count
    filename = f"{job.stat_file}{count}"

    if file_exists:
        local_stat_path = Path(str(tmp_path), "t000", "tmp", filename)
        local_stat_path.parent.mkdir(parents=True, exist_ok=True)
        local_stat_path.write_text("dummy content")
        remote_stat_path = Path(str(tmp_path), "t000", "tmp", "LOG_t000", filename)
        remote_stat_path.parent.mkdir(parents=True, exist_ok=True)
        remote_stat_path.write_text("dummy content")

    platform = LocalPlatform("t000", 'platform', basic_config.props())
    assert platform.get_stat_file(job, count) == file_exists


def test_local_platform_read_file(tmp_path):
    basic_config = FakeBasicConfig()
    basic_config.LOCAL_ROOT_DIR = str(tmp_path)
    basic_config.LOCAL_TMP_DIR = str(tmp_path)

    platform = LocalPlatform("t001", "platform", basic_config.props())

    path_not_exists = Path(tmp_path).joinpath("foo", "bar")

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
    mocker.patch.object(Log, 'set_console_level')
    recover_platform_job_logs_wrapper(
        platform, None, None, None, None, as_conf=as_conf)  # type: ignore
    assert Log.set_console_level.call_count == 1


def test_init_logs_log_process_with_root_dir(mocker, autosubmit_config):
    """This test ignores the platform, and only tests the log initialization when a root dir is provided."""
    as_conf = autosubmit_config(_EXPID, experiment_data={
        'CONFIG': {
            'LOG_RECOVERY_CONSOLE_LEVEL': 'NO_LOG'
        }
    })
    # TODO: We need to create it first to get a ``BasicConfig``, with Dani's improvement
    #       that may not be necessary in the future!
    as_conf.experiment_data['ROOTDIR'] = BasicConfig.expid_dir(as_conf.expid)

    platform = mocker.MagicMock()
    platform.name = 'parrot'

    mocker.patch('autosubmit.platforms.platform._exit', return_value=0)

    current_number_of_handlers = len(Log.log.handlers)

    recover_platform_job_logs_wrapper(
        platform, None, None, None, None, as_conf=as_conf)  # type: ignore

    assert len(Log.log.handlers) == current_number_of_handlers + 2  # + out + err


@pytest.mark.parametrize(
    "work_event,queue_size,filter_called",
    [
        (True, 200, False),
        (False, 200, False),
        (True, 0, True),
    ],
    ids=["signals-work-event", "no-work-event", "bounds-pending"],
)
def test_add_job_to_log_recover_signals_work_event(
    autosubmit_config, mocker, work_event, queue_size, filter_called
):
    """add_job_to_log_recover queues the job, signals work, and bounds the pending map."""
    ctx = mp.get_context("fork")
    platform = LocalPlatform("t000", "test_platform", {})
    platform.recovery_queue = CopyQueue(ctx=ctx)
    platform.work_event = ctx.Event() if work_event else None
    platform.log_queue_size = queue_size
    platform.log_recovery_process = SimpleNamespace(is_alive=lambda: True)
    as_conf = autosubmit_config("t000", {"PLATFORMS": {"test_platform": {}}})
    as_conf.misc_data = {"AS_COMMAND": "run"}
    job = Job("test_job", 123, Status.COMPLETED, 0)
    pending_filter = mocker.patch.object(
        platform, "_pending_still_needing_recovery", return_value={}
    )

    try:
        assert platform.add_job_to_log_recover(job, as_conf) is True

        assert platform.recovery_queue.get(timeout=5) == CopyQueue.job_data(job)
        if work_event:
            assert platform.work_event.is_set()
        else:
            assert platform.work_event is None
        if filter_called:
            pending_filter.assert_called_once_with()
        else:
            pending_filter.assert_not_called()
    finally:
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
def test_recover_job_reports_attempts(
    mocker, all_succeeded, errors, warning_expected, result_expected
):
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
    report = RecoveryReport(
        job_name="test_job",
        attempts=attempts,
        all_succeeded=all_succeeded,
    )
    job = SimpleNamespace(
        name="test_job",
        retrieve_logfiles=lambda: report,
        send_cpmip_notification=lambda as_conf: None,
    )
    saved_jobs: list[object] = []
    jobs_db_manager = SimpleNamespace(save_job_log=saved_jobs.append)
    warning = mocker.patch.object(Log, "warning")
    result = mocker.patch.object(Log, "result")

    platform._recover_job(job, jobs_db_manager, object())

    if warning_expected:
        warning.assert_called_once()
    else:
        warning.assert_not_called()
    if result_expected:
        result.assert_called_once()
    else:
        result.assert_not_called()
    assert saved_jobs == [job]


def test_recover_job_reports_only_recovered_attempts(mocker):
    """The recovered attempt count ignores failed attempts."""
    platform = LocalPlatform("t000", "test_platform", {})
    report = RecoveryReport(
        job_name="test_job",
        attempts=[
            RecoveryAttempt(0, True, ("", ""), ("", "")),
            RecoveryAttempt(1, False, ("", ""), ("", ""), error="boom"),
            RecoveryAttempt(2, True, ("", ""), ("", "")),
        ],
        all_succeeded=False,
    )
    retrieved = []
    job = SimpleNamespace(
        name="test_job",
        retrieve_logfiles=lambda: retrieved.append(report) or report,
        send_cpmip_notification=lambda as_conf: None,
    )
    warning = mocker.patch.object(Log, "warning")
    result = mocker.patch.object(Log, "result")
    jobs_db_manager = mocker.MagicMock()

    platform._recover_job(job, jobs_db_manager, object())

    assert retrieved == [report]
    warning.assert_called_once()
    result.assert_called_once()
    assert "recovered 2 attempt" in result.call_args.args[0]
    jobs_db_manager.save_job_log.assert_called_once_with(job)


def test_recover_job_log_continues_after_a_failed_job(mocker):
    """A failed job does not stop the recovery of the remaining queued jobs."""
    platform = LocalPlatform("t000", "test_platform", {})
    platform.recovery_queue = Queue()
    platform.recovery_queue.put({"name": "job_a", "id": 1})
    platform.recovery_queue.put({"name": "job_b", "id": 2})
    platform._as_conf = object()
    recover = mocker.patch.object(
        platform, "_recover_single_log", side_effect=[Exception("boom"), None]
    )
    restore = mocker.patch.object(platform, "restore_connection")
    warning = mocker.patch.object(Log, "warning")
    mocker.patch.object(Log, "debug")

    platform.recover_job_log(mocker.MagicMock())

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
            restore_platforms([platform], as_conf=as_conf, fail_on_init_error=fail_on_init_error)
        assert exc_info.value.code == 6008
    else:
        restore_platforms([platform], as_conf=as_conf, fail_on_init_error=fail_on_init_error)

    if fail_on_init_error and connected and should_run:
        platform.try_respawn_log_recovery_process.assert_called_once_with(as_conf)
    else:
        platform.try_respawn_log_recovery_process.assert_not_called()


def _noop():
    pass


def test_join_new_process_does_not_use_raw_waitpid(mocker):
    """Reaping goes through multiprocessing so is_alive()/exitcode stay accurate."""
    platform = LocalPlatform("t000", "test_platform", {})
    is_alive_method = mocker.Mock(return_value=False)
    process = SimpleNamespace(is_alive=is_alive_method, pid=123, name="child", exitcode=0)
    platform.log_recovery_process = process
    waitpid = mocker.patch("os.waitpid")

    platform.join_new_process()

    waitpid.assert_not_called()
    is_alive_method.assert_called_once()


@pytest.mark.skipif(
    "fork" not in mp.get_all_start_methods(),
    reason="The fork start method is not available on this platform",
)
def test_join_new_process_reaps_exited_child():
    """join_new_process is the only call that reaps the exited child."""
    ctx = mp.get_context("fork")
    process = ctx.Process(target=_noop)
    process.start()
    platform = LocalPlatform("t000", "test_platform", {})
    platform.log_recovery_process = process
    try:
        ready = connection.wait([process.sentinel], timeout=5)
        assert process.sentinel in ready

        deadline = time.time() + 5
        status = None
        while time.time() < deadline:
            try:
                status = psutil.Process(process.pid).status()
            except psutil.NoSuchProcess:
                break
            if status == psutil.STATUS_ZOMBIE:
                break
            time.sleep(0.01)
        assert status == psutil.STATUS_ZOMBIE, f"Expected a zombie, got {status!r}"

        platform.join_new_process()

        with pytest.raises(psutil.NoSuchProcess):
            psutil.Process(process.pid).status()
        assert process.exitcode == 0
    finally:
        if process.is_alive():
            process.kill()
        process.join(timeout=5)


@pytest.mark.parametrize(
    "ready,expected_failures",
    [(False, 3), (True, 0)],
    ids=["not-ready", "ready"],
)
def test_alive_process_resets_only_when_ready(
    autosubmit_config, ready, expected_failures
):
    """A live child resets the streak only after it reports readiness."""
    as_conf = autosubmit_config("t000", {"PLATFORMS": {"test_platform": {}}})
    as_conf.misc_data = {"AS_COMMAND": "run"}
    platform = LocalPlatform("t000", "test_platform", {})
    platform.log_recovery_process = SimpleNamespace(is_alive=lambda: True)
    platform._log_recovery_ready = SimpleNamespace(is_set=lambda: ready)
    platform._recovery_consecutive_failures = 3

    assert platform.try_respawn_log_recovery_process(as_conf) is True
    assert platform._recovery_consecutive_failures == expected_failures


def test_spawn_resets_active_flag_when_process_is_dead(mocker, autosubmit_config):
    """A stale active flag must not prevent a new recovery process from starting."""
    as_conf = autosubmit_config("t000", {"PLATFORMS": {"test_platform": {}}})
    as_conf.misc_data = {"AS_COMMAND": "run"}
    platform = LocalPlatform("t000", "test_platform", {})
    platform.log_retrieval_process_active = True
    platform.log_recovery_process = SimpleNamespace(is_alive=lambda: False, pid=123, name="child")
    mocker.patch.object(platform, "prepare_process", return_value=None)
    mocker.patch.object(platform, "join_new_process")

    def _start_new_process(*_args, **_kwargs):
        platform.log_recovery_process = SimpleNamespace(is_alive=lambda: True, pid=456, name="child")

    mocker.patch.object(platform, "create_new_process", side_effect=_start_new_process)

    # The stale flag is cleared and this call cannot spawn.
    assert platform.spawn_log_retrieval_process(as_conf) is False
    assert platform.log_retrieval_process_active is False

    # A later call starts a new process.
    assert platform.spawn_log_retrieval_process(as_conf) is True
    assert platform.log_retrieval_process_active is True
    assert platform._log_recovery_started_at > 0


def test_spawn_does_not_leave_active_flag_when_prepare_process_raises(mocker, autosubmit_config):
    """A prepare_process failure must not leave the active flag set."""
    as_conf = autosubmit_config("t000", {"PLATFORMS": {"test_platform": {}}})
    as_conf.misc_data = {"AS_COMMAND": "run"}
    platform = LocalPlatform("t000", "test_platform", {})
    mocker.patch.object(platform, "prepare_process", side_effect=RuntimeError("boom"))

    with pytest.raises(RuntimeError, match="boom"):
        platform.spawn_log_retrieval_process(as_conf)

    assert platform.log_retrieval_process_active is False


@pytest.mark.parametrize(
    "ready,exitcode,cleanup,initial_failures,expected_failures",
    [
        (False, 0, False, 0, 1),
        (False, 1, False, 0, 1),
        (True, 1, False, 0, 1),
        (True, 0, False, 0, 0),
        (True, None, False, 0, 1),
        (True, 1, True, 3, 3),
        (True, 0, True, 3, 3),
    ],
    ids=[
        "never-ready-zero",
        "never-ready-one",
        "post-init-crash",
        "normal-exit",
        "unknown-exit",
        "cleanup-crash",
        "cleanup-normal",
    ],
)
def test_dead_process_classification(
    autosubmit_config,
    mocker,
    ready,
    exitcode,
    cleanup,
    initial_failures,
    expected_failures,
):
    """A dead child is classified by readiness, exit code, and cleanup state."""
    as_conf = autosubmit_config("t000", {"PLATFORMS": {"test_platform": {}}})
    as_conf.misc_data = {"AS_COMMAND": "run"}
    platform = LocalPlatform("t000", "test_platform", {})
    platform.log_recovery_process = SimpleNamespace(
        is_alive=lambda: False, exitcode=exitcode, pid=123, name="child"
    )
    platform._log_recovery_ready = SimpleNamespace(is_set=lambda: ready)
    platform.cleanup_event = SimpleNamespace(is_set=lambda: cleanup)
    platform._log_recovery_started_at = time.time() - 1
    platform._recovery_consecutive_failures = initial_failures
    mocker.patch.object(platform, "clean_log_recovery_process")
    mocker.patch.object(platform, "_pending_still_needing_recovery", return_value={})
    mocker.patch.object(platform, "spawn_log_retrieval_process", return_value=True)

    assert platform.try_respawn_log_recovery_process(as_conf) is True
    assert platform._recovery_consecutive_failures == expected_failures


def test_consecutive_startup_failures_enter_backoff(autosubmit_config, mocker):
    """A startup failure that reaches the limit enters backoff before respawning."""
    as_conf = autosubmit_config("t000", {
        "RUNTIME": {"LOG_RECOVERY": {
            "CONSECUTIVE_MAX_RESTARTS": 2,
            "RETRY_DELAY": 600,
            "INLINE_FALLBACK": False,
        }},
        "PLATFORMS": {"test_platform": {}},
    })
    as_conf.misc_data = {"AS_COMMAND": "run"}
    platform = LocalPlatform("t000", "test_platform", {})
    platform.log_recovery_process = SimpleNamespace(
        is_alive=lambda: False, exitcode=0, pid=123, name="child"
    )
    platform._log_recovery_ready = SimpleNamespace(is_set=lambda: False)
    platform.cleanup_event = SimpleNamespace(is_set=lambda: False)
    platform._log_recovery_started_at = time.time() - 1
    platform._recovery_consecutive_failures = 1
    spawn = mocker.patch.object(platform, "spawn_log_retrieval_process", return_value=True)

    assert platform.try_respawn_log_recovery_process(as_conf) is False
    assert platform._log_recovery_in_backoff is True
    assert platform._recovery_consecutive_failures == 0
    spawn.assert_not_called()


def test_prepare_process_clears_ready_event(mocker):
    """Each spawn starts from a cleared readiness event."""
    platform = LocalPlatform("t000", "test_platform", {})
    platform.ctx = mp.get_context("fork")
    platform._log_recovery_ready = platform.ctx.Event()
    platform._log_recovery_ready.set()
    mocker.patch.object(platform, "create_a_new_copy", return_value=platform)
    mocker.patch.object(platform, "load_process_info")
    mocker.patch("autosubmit.platforms.platform.atexit.register")
    mocker.patch("autosubmit.platforms.platform.Platform.update_workers")

    platform.prepare_process()

    assert platform._log_recovery_ready.is_set() is False


def _recovery_child_platform(as_conf, mocker):
    platform = LocalPlatform("t000", "test_platform", {})
    platform.ctx = mp.get_context("fork")
    platform.cleanup_event = platform.ctx.Event()
    platform.cleanup_event.set()
    platform.work_event = platform.ctx.Event()
    platform._log_recovery_ready = platform.ctx.Event()
    platform._as_conf = as_conf
    mocker.patch("setproctitle.setproctitle")
    mocker.patch("autosubmit.config.registry.load_config", return_value=as_conf)
    mocker.patch.object(as_conf, "check_conf_files")
    mocker.patch("autosubmit.platforms.platform.JobsDbManager")
    mocker.patch.object(platform, "recover_job_log")
    mocker.patch.object(platform, "close_connection")
    return platform


def test_recover_platform_job_logs_sets_ready_and_exits_zero(autosubmit_config, mocker):
    """The child reports readiness after connecting and exits zero on a clean stop."""
    as_conf = autosubmit_config("t000", {"PLATFORMS": {"test_platform": {}}})
    platform = _recovery_child_platform(as_conf, mocker)

    def _connect(*_args, **_kwargs):
        platform.connected = True

    mocker.patch.object(platform, "restore_connection", side_effect=_connect)
    exit_mock = mocker.patch("autosubmit.platforms.platform._exit")

    platform.recover_platform_job_logs()

    assert platform._log_recovery_ready.is_set() is True
    exit_mock.assert_called_once_with(0)


def test_recover_platform_job_logs_exits_non_zero_on_failure(autosubmit_config, mocker):
    """A child that cannot start must exit non-zero and must not report readiness."""
    as_conf = autosubmit_config("t000", {"PLATFORMS": {"test_platform": {}}})
    platform = _recovery_child_platform(as_conf, mocker)
    mocker.patch.object(platform, "restore_connection", side_effect=Exception("boom"))
    exit_mock = mocker.patch("autosubmit.platforms.platform._exit")

    platform.recover_platform_job_logs()

    assert platform._log_recovery_ready.is_set() is False
    exit_mock.assert_called_once_with(1)


def test_recover_platform_job_logs_exits_non_zero_when_not_connected(autosubmit_config, mocker):
    """A connection that returns without an error but leaves connected false is a failure."""
    as_conf = autosubmit_config("t000", {"PLATFORMS": {"test_platform": {}}})
    platform = _recovery_child_platform(as_conf, mocker)
    mocker.patch.object(platform, "restore_connection")
    exit_mock = mocker.patch("autosubmit.platforms.platform._exit")

    platform.recover_platform_job_logs()

    assert platform._log_recovery_ready.is_set() is False
    exit_mock.assert_called_once_with(1)


def test_recover_job_log_stops_when_queue_turns_empty(mocker):
    """A timeout while draining the queue must stop the loop without raising."""
    platform = LocalPlatform("t000", "test_platform", {})
    recovery_queue = mocker.MagicMock()
    recovery_queue.empty.return_value = False
    recovery_queue.get.side_effect = queue.Empty
    platform.recovery_queue = recovery_queue
    platform._as_conf = object()

    platform.recover_job_log(mocker.MagicMock())

    recovery_queue.get.assert_called_once()

