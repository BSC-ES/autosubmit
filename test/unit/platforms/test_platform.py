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

from pathlib import Path

import pytest

from autosubmit.log.log import Log
from autosubmit.platforms.locplatform import LocalPlatform
from autosubmit.platforms.platform import recover_platform_job_logs_wrapper
from autosubmit.job.job import Job
from test.unit.test_job import TestJob, FakeBasicConfig

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
    as_conf.experiment_data['ROOTDIR'] = as_conf.basic_config.expid_dir(as_conf.expid)

    platform = mocker.MagicMock()
    platform.name = 'parrot'

    mocker.patch('autosubmit.platforms.platform._exit', return_value=0)

    current_number_of_handlers = len(Log.log.handlers)

    recover_platform_job_logs_wrapper(
        platform, None, None, None, as_conf=as_conf)  # type: ignore

    assert len(Log.log.handlers) == current_number_of_handlers + 2  # + out + err


def test_add_job_to_log_recover_signals_work_event(mocker):
    """add_job_to_log_recover signals work_event after queuing the job."""
    platform = LocalPlatform("t000", "test_platform", {})
    platform.recovery_queue = mocker.MagicMock()
    platform.work_event = mocker.MagicMock()
    platform.log_recovery_process = mocker.MagicMock()
    platform.log_recovery_process.is_alive.return_value = True

    job = mocker.MagicMock(spec=Job)
    job.id = 42
    job.name = "test_job"
    job.fail_count = 1

    platform.add_job_to_log_recover(job)

    platform.recovery_queue.put.assert_called_once_with(job, timeout=30)
    platform.work_event.set.assert_called_once()
