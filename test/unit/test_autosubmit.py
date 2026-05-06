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

"""Tests for ``AutosubmitGit``."""

from pathlib import Path
from textwrap import dedent

import pytest

from autosubmit.autosubmit import Autosubmit
from autosubmit.config.basicconfig import BasicConfig
from autosubmit.config.utils import copy_as_config
from autosubmit.log.log import AutosubmitCritical
from test.unit.conftest import AutosubmitConfigFactory


def test_copy_as_config(autosubmit_config: AutosubmitConfigFactory):
    """function to test copy_as_config from autosubmit.py

    :param autosubmit_config:
    :type autosubmit_config: AutosubmitConfigFactory
    """
    autosubmit_config('a000', {})
    BasicConfig.LOCAL_ROOT_DIR = f"{BasicConfig.LOCAL_ROOT_DIR}"

    ini_file = Path(f'{BasicConfig.LOCAL_ROOT_DIR}/a000/conf')
    new_file = Path(f'{BasicConfig.LOCAL_ROOT_DIR}/a001/conf')
    ini_file.mkdir(parents=True, exist_ok=True)
    new_file.mkdir(parents=True, exist_ok=True)
    ini_file = ini_file / 'jobs_a000.conf'
    new_file = new_file / 'jobs_a001.yml'

    with open(ini_file, 'w+', encoding="utf-8") as file:
        file.write(dedent('''\
                [LOCAL_SETUP]
                FILE = LOCAL_SETUP.sh
                PLATFORM = LOCAL
                '''))
        file.flush()

    copy_as_config('a001', 'a000')

    new_yaml_file = Path(new_file.parent, new_file.stem).with_suffix('.yml')

    assert new_yaml_file.exists()
    assert new_yaml_file.stat().st_size > 0

    new_yaml_file = Path(new_file.parent, new_file.stem).with_suffix('.conf_AS_v3_backup')

    assert new_yaml_file.exists()
    assert new_yaml_file.stat().st_size > 0


def test_pkl_fix_postgres(monkeypatch, autosubmit):
    """Test that trying to fix the pkl when using Postgres results in an error."""
    monkeypatch.setattr(BasicConfig, 'DATABASE_BACKEND', 'postgres')

    with pytest.raises(AutosubmitCritical):
        autosubmit.pkl_fix('a000')


def test_database_backup_postgres(monkeypatch, autosubmit, mocker):
    """Test that trying to back up a Postgres DB results in just a log message of WIP."""
    monkeypatch.setattr(BasicConfig, 'DATABASE_BACKEND', 'postgres')
    mocked_log = mocker.patch('autosubmit.autosubmit.Log')
    autosubmit.database_backup('a000')
    assert mocked_log.debug.called


@pytest.mark.parametrize(
    'completed,failed',
    [
        (0, 0),
        (1, 0),
        (2, 0),
        (1, 1),
        (1, 2)
    ]
)
def test_iteration_info(completed, failed, mocker):
    """Test that we return and print the iteration info.

    Autosubmit has a function that prints the information about the current main loop iteration.
    This includes total and failed jobs, and other information about the current loop step.
    """
    total_jobs = completed + failed
    job_list = mocker.MagicMock()
    job_list.get_job_list.return_value = list(range(total_jobs))
    job_list.get_completed.return_value = list(range(completed))
    job_list.get_failed.return_value = list(range(failed))

    expected_safety_time = 42
    expected_default_retries = 22
    expected_check_wrapper_time = 1984

    as_conf = mocker.MagicMock()
    as_conf.get_safetysleeptime.return_value = expected_safety_time
    as_conf.get_retrials.return_value = expected_default_retries
    as_conf.get_wrapper_check_time.return_value = expected_check_wrapper_time

    mocked_log = mocker.patch('autosubmit.autosubmit.Log')

    total, safety_time, default_retries, check_wrapper_time = Autosubmit.get_iteration_info(as_conf, job_list)

    assert expected_default_retries == default_retries
    assert expected_check_wrapper_time == check_wrapper_time
    assert expected_safety_time == safety_time

    log_info_called = mocked_log.info.call_count
    expected_info_called = 2 if failed > 0 else 1
    assert log_info_called == expected_info_called

    if failed > 0:
        failed_text = "job has" if failed == 1 else "jobs have"
        assert failed_text in mocked_log.info.call_args_list[1][0][0]

