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

from autosubmit.platforms.locplatform import LocalPlatform
from test.unit.test_job import TestJob, FakeBasicConfig


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
