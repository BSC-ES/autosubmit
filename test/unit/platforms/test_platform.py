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
from pathlib import Path
from queue import Queue
from types import SimpleNamespace

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
        platform, None, None, None, as_conf=as_conf)  # type: ignore
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
        platform, None, None, None, as_conf=as_conf)  # type: ignore

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

