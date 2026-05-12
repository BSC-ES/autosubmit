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
from autosubmit.platforms.platform import recover_platform_job_logs_wrapper, Platform
from test.unit.test_job import TestJob, FakeBasicConfig
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status

_EXPID = 't000'


@pytest.mark.parametrize(
    'file_exists,count',
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
    basic_config.LOCAL_TMP_DIR = str(tmp_path)

    job = TestJob()
    job.stat_file = "test_file"
    job.name = "test_name"
    job.fail_count = count
    filename = job.stat_file + str(count)

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
        platform, None, None, None, as_conf=as_conf)  # type: ignore

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
        platform, None, None, None, as_conf=as_conf)  # type: ignore

    assert len(Log.log.handlers) == current_number_of_handlers + 2  # + out + err

def test_io_safe_wait_class_constant_default():
    """Platform class must define IO_SAFE_WAIT defaulting to 0."""
    assert hasattr(Platform, 'IO_SAFE_WAIT')
    assert Platform.IO_SAFE_WAIT == 0


@pytest.mark.parametrize(
    'platform_config,expected',
    [
        ({"IO_SAFE_WAIT": 5}, 5),
        ({}, 0),
    ],
    ids=['configured', 'default'],
)
def test_io_safe_wait_from_platform_config(autosubmit_config, platform_config, expected):
    """IO_SAFE_WAIT must be read from platform config or default to 0."""
    as_conf = autosubmit_config(_EXPID, experiment_data={
        "PLATFORMS": {"TEST": platform_config}
    })
    platform = LocalPlatform(_EXPID, 'test', as_conf.experiment_data)
    assert platform.IO_SAFE_WAIT == expected


def test_get_stat_file_requires_explicit_count(local_platform):
    """get_stat_file must raise TypeError when count is omitted."""
    job = Job('job', '1', Status.WAITING, 0)
    with pytest.raises(TypeError):
        local_platform.get_stat_file(job)


def test_get_stat_file_uses_stat_file_plus_count(local_platform, mocker):
    """get_stat_file must build filename from job.stat_file + count."""
    mocker.patch.object(local_platform, 'check_file_exists', return_value=True)
    mocker.patch.object(local_platform, 'get_file', return_value=True)

    job = Job('job', '1', Status.WAITING, 0)
    job.stat_file = 'my_job_STAT_'
    job.fail_count = 7

    local_platform.get_stat_file(job, 3)
    local_platform.check_file_exists.assert_called_once_with('my_job_STAT_3')
