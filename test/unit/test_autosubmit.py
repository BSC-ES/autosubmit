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

"""Tests for ``Autosubmit``."""

import signal
from pathlib import Path
from textwrap import dedent

import pytest

from autosubmit.autosubmit import Autosubmit
from autosubmit.config.basicconfig import BasicConfig
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
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

    Autosubmit.copy_as_config('a001', 'a000')

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


def test_install_creates_directories(monkeypatch, tmp_path, autosubmit, mocker):
    """install must create the Autosubmit directories (issue #2640)."""
    local_root = tmp_path / "experiments"

    monkeypatch.setattr(BasicConfig, "LOCAL_ROOT_DIR", str(local_root))
    monkeypatch.setattr(BasicConfig, "GLOBAL_LOG_DIR", str(local_root / "logs"))
    monkeypatch.setattr(BasicConfig, "STRUCTURES_DIR", str(local_root / "metadata/structures"))
    monkeypatch.setattr(BasicConfig, "JOBDATA_DIR", str(local_root / "metadata/data"))
    monkeypatch.setattr(BasicConfig, "HISTORICAL_LOG_DIR", str(local_root / "metadata/logs"))
    monkeypatch.setattr(BasicConfig, "DATABASE_BACKEND", "sqlite")
    monkeypatch.setattr(BasicConfig, "DB_PATH", str(tmp_path / "autosubmit.db"))
    mocker.patch("autosubmit.autosubmit.create_db", return_value=True)

    autosubmit.install()

    assert local_root.exists()
    assert (local_root / "logs").exists()
    assert (local_root / "metadata/structures").exists()
    assert (local_root / "metadata/data").exists()
    assert (local_root / "metadata/logs").exists()


def test_signal_handler_sets_exit_flag():
    """signal_handler must set ``Autosubmit.exit`` to ``True`` on SIGINT.

    :raises AssertionError: If ``Autosubmit.exit`` is not ``True`` after the handler runs.
    """
    from autosubmit.autosubmit import signal_handler
    original = Autosubmit.exit
    try:
        Autosubmit.exit = False
        signal_handler(signal.SIGINT, None)
        assert Autosubmit.exit is True
    finally:
        # Restore class-level flag so other tests are not affected.
        Autosubmit.exit = original


@pytest.mark.parametrize(
    'wrapper_status,expect_popped',
    [
        (Status.COMPLETED, True),
        (Status.FAILED, True),
        (Status.RUNNING, False),
        (Status.QUEUING, False),
        (Status.SUBMITTED, False),
    ],
    ids=['completed-popped', 'failed-popped', 'running-kept', 'queuing-kept', 'submitted-kept'],
)
def test_check_wrappers_pops_terminal_wrappers(
    fake_job_list, fake_platform, mocker, wrapper_status: Status, expect_popped: bool
) -> None:
    """check_wrappers must pop the wrapper from job_package_map only when its status is terminal.

    :param fake_job_list: Minimal JobList fixture.
    :param fake_platform: Minimal platform stub fixture.
    :param mocker: pytest-mock mocker fixture.
    :param wrapper_status: Status to assign to the wrapper.
    :type wrapper_status: Status
    :param expect_popped: Whether the wrapper should be removed from ``job_package_map``.
    :type expect_popped: bool
    """
    as_conf = mocker.MagicMock()

    inner_job = Job('a000_20000101_fc0_1_SIM', 100, wrapper_status, 0)
    wrapper_job = mocker.MagicMock()
    wrapper_job.job_list = [inner_job]
    wrapper_job.status = wrapper_status
    wrapper_job.new_status = wrapper_status
    wrapper_job.name = 'wrapper_1'
    wrapper_job.id = 100

    fake_job_list.job_package_map[100] = wrapper_job
    fake_job_list.packages_dict['wrapper_1'] = [inner_job]

    mocker.patch.object(Autosubmit, 'manage_wrapper_job', return_value=wrapper_job)
    mocker.patch.object(Autosubmit, 'wrapper_notify')

    Autosubmit.check_wrappers(as_conf, fake_job_list, 'a000')

    if expect_popped:
        assert 100 not in fake_job_list.job_package_map
    else:
        assert 100 in fake_job_list.job_package_map


def test_check_non_wrapped_jobs_calls_platform_and_updates_status(
    fake_job_list, fake_platform, mocker
) -> None:
    """check_non_wrapped_jobs must call check_all_jobs and update job status.

    Edge case vs. stashed version: the current signature is
    ``(platforms_to_test, job_list, as_conf, expid)`` — no ``jobs_to_check`` dict
    and no return value (it fires updates in place).

    :param fake_job_list: Minimal JobList fixture.
    :param fake_platform: Minimal platform stub.
    :param mocker: pytest-mock mocker fixture.
    """
    from autosubmit.job.job import Job as JobClass
    as_conf = mocker.MagicMock()

    job1 = Job('a000_20000101_fc0_1_SIM', 10, Status.RUNNING, 0)
    job1.platform = fake_platform
    fake_job_list._job_list.append(job1)

    def mock_update_status(self, conf):
        self.status = Status.COMPLETED
        return Status.COMPLETED

    mocker.patch.object(JobClass, 'update_status', mock_update_status)
    # Avoid hitting the filesystem – save() would try to pickle to a tmp path.
    mocker.patch.object(fake_job_list, 'save')
    # Simulate new_status differing so update_status is called.
    job1.new_status = Status.COMPLETED

    Autosubmit.check_non_wrapped_jobs([fake_platform], fake_job_list, as_conf, 'a000')

    fake_platform.check_all_jobs.assert_called_once()
    fake_job_list.save.assert_called_once()
