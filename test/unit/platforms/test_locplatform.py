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

"""Unit tests for the Local Platform."""
from multiprocessing.process import BaseProcess
from pathlib import Path
from unittest.mock import patch

import pytest

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.platforms.locplatform import LocalPlatform

_EXPID = 't001'


def test_local_platform_copy():
    local_platform = LocalPlatform(_EXPID, 'local', {}, auth_password=None)

    copied = local_platform.create_a_new_copy()

    assert local_platform.name == copied.name
    assert local_platform.expid == copied.expid


@pytest.mark.parametrize(
    'count,stats_file_exists,job_fail_count,remote_file_exists',
    [
        (-1, True, 0, True),
        (0, False, 0, False),
        (1, False, 1, True),
        (100, True, 100, True)
    ],
    ids=[
        'use fail_count, delete stats_file, remote file transferred',
        'use count, no stats_file, failed to transfer',
        'use count, no stats_file, remote file transferred',
        'use count, delete stats_file, remote file transferred',
    ]
)
def test_get_stat_file(count: int, stats_file_exists: bool, job_fail_count: int, remote_file_exists: bool,
                       autosubmit_config, mocker):
    """Test that ``get_stat_file`` uses the correct file name."""
    mocked_os_remove = mocker.patch('os.remove')

    as_conf = autosubmit_config(_EXPID, experiment_data={})
    exp_path = Path(as_conf.basic_config.LOCAL_ROOT_DIR) / _EXPID

    local = LocalPlatform(_EXPID, __name__, as_conf.experiment_data)

    job = Job('job', '1', Status.WAITING, None, None)
    job.fail_count = job_fail_count

    # TODO: this is from ``job.py``; we can probably find an easier way to fetch the file name,
    #       so we can re-use it in tests (e.g. move the logic to a small function/property/etc.).
    if count == -1:
        filename = f"{job.stat_file}{job.fail_count}"
    else:
        filename = f'{job.name}_STAT_{str(count)}'

    if remote_file_exists:
        # Create fake remote stat file transferred.
        Path(exp_path, as_conf.basic_config.LOCAL_TMP_DIR, f'LOG_{_EXPID}', filename).touch()

    if stats_file_exists:
        # Create fake local stat file, to be deleted before copying the remote file (created above).
        Path(exp_path, as_conf.basic_config.LOCAL_TMP_DIR, filename).touch()

    assert remote_file_exists == local.get_stat_file(job=job, count=count)
    assert mocked_os_remove.called == stats_file_exists


def test_get_job_names_cmd_contains_expected_jobs() -> None:
    """The command must use ps to list processes and filter by job name.

    LocalPlatform shares the same implementation as PsPlatform for detecting
    duplicated process names on a UNIX remote host.
    """
    local = LocalPlatform(_EXPID, 'local', {})
    cmd = local._get_job_names_cmd(["job_a", "job_b"])

    assert "job_a" in cmd
    assert "job_b" in cmd


def test_refresh_log_recovery_process(autosubmit, autosubmit_config, mocker):
    as_conf = autosubmit_config('t000', {}, reload=False, create=False)
    as_conf.misc_data["AS_COMMAND"] = 'run'

    local = LocalPlatform(expid='t000', name='local', config=as_conf.experiment_data)
    local.prepare_process()
    assert local.work_event is not None

    spy = mocker.spy(local, 'clean_log_recovery_process')
    spy2 = mocker.spy(local, 'spawn_log_retrieval_process')

    local.clean_log_recovery_process()
    local.log_recovery_process = BaseProcess()

    with patch('multiprocessing.process.BaseProcess.is_alive', return_value=True):
        autosubmit.refresh_log_recovery_process(platforms=[local], as_conf=as_conf)
        spy.assert_called()
        spy2.assert_not_called()
        assert local.work_event is None


def _make_simple_job(name: str, status: Status, fail_count: int = 0):
    """Return a lightweight job object for check_all_jobs tests."""
    job = type('Job', (), {})()
    job.name = name
    job.id = 1
    job.fail_count = fail_count
    job.status = status
    job.new_status = status
    job.finished_time = None
    job.start_time_timestamp = None
    job.wrapper_type = 'simple'
    return job


@pytest.mark.parametrize('stat_line,expected_final_status', [
    ('COMPLETED', Status.COMPLETED),
    ('FAILED', Status.FAILED),
    ('', Status.RUNNING),  # STAT not flushed yet → defer
])
def test_check_all_jobs_stat_confirmation(
        tmp_path: Path,
        stat_line: str,
        expected_final_status: Status,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """check_all_jobs confirms COMPLETED scheduler status via the STAT file.

    :param tmp_path: Temporary directory for remote log files.
    :param stat_line: Last line written to the STAT file (empty = absent).
    :param expected_final_status: Expected status after check_all_jobs.
    """
    platform = LocalPlatform(expid=_EXPID, name='local', config={})
    remote_log = tmp_path / f'LOG_{_EXPID}'
    remote_log.mkdir(parents=True)
    platform.remote_log_dir = str(remote_log)
    platform.connected = True

    job = _make_simple_job('t001_INI', status=Status.RUNNING)

    if stat_line:
        (remote_log / f'{job.name}_STAT_{job.fail_count}').write_text(
            f'1000\n1060\n{stat_line}\n'
        )

    as_conf = type('Conf', (), {'get_copy_remote_logs': lambda self: None})()

    platform.check_all_jobs([job], as_conf)

    assert job.new_status == expected_final_status


def test_check_all_jobs_save_flag_set_when_status_changes(
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """check_all_jobs updates job.new_status when the STAT file reports a new status.

    :param tmp_path: Temporary directory for remote log files.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    platform = LocalPlatform(expid=_EXPID, name='local', config={})
    remote_log = tmp_path / f'LOG_{_EXPID}'
    remote_log.mkdir(parents=True)
    platform.remote_log_dir = str(remote_log)
    platform.connected = True

    job = _make_simple_job('t001_INI', status=Status.RUNNING)
    (remote_log / f'{job.name}_STAT_{job.fail_count}').write_text('1000\n1060\nCOMPLETED\n')

    as_conf = type('Conf', (), {'get_copy_remote_logs': lambda self: None})()

    platform.check_all_jobs([job], as_conf)

    assert job.new_status == Status.COMPLETED


def test_check_all_jobs_no_change_returns_false(
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """check_all_jobs keeps job.new_status as RUNNING when no STAT file is present.

    :param tmp_path: Temporary directory for remote log files.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    platform = LocalPlatform(expid=_EXPID, name='local', config={})
    remote_log = tmp_path / f'LOG_{_EXPID}'
    remote_log.mkdir(parents=True)
    platform.remote_log_dir = str(remote_log)
    platform.connected = True

    job = _make_simple_job('t001_INI', status=Status.RUNNING)


    as_conf = type('Conf', (), {'get_copy_remote_logs': lambda self: None})()

    platform.check_all_jobs([job], as_conf)

    assert job.new_status == Status.RUNNING
