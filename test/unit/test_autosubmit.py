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

import datetime
import signal
from pathlib import Path
from textwrap import dedent


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


@pytest.mark.parametrize("status,elapsed,should_check", [
    (Status.WAITING, 0, True),    # non-RUNNING always checks
    (Status.RUNNING, 5, True),    # RUNNING and elapsed >= sleeptime
    (Status.RUNNING, 1, False),   # RUNNING but elapsed < sleeptime
])
def test_manage_wrapper_job_check_logic(mocker, status, elapsed, should_check):
    """manage_wrapper_job: check_wrapper logic based on status and elapsed time."""
    as_conf = mocker.MagicMock()
    as_conf.get_wrapper_check_time.return_value = 3
    as_conf.get_notifications.return_value = "false"

    wrapper_job = mocker.MagicMock()
    wrapper_job.status = status
    wrapper_job.checked_time = datetime.datetime.now() - datetime.timedelta(seconds=elapsed)
    wrapper_job.id = 99
    wrapper_job.job_list = []
    wrapper_job.check_and_update_status.return_value = False

    job_list = mocker.MagicMock()

    result = Autosubmit.manage_wrapper_job(as_conf, job_list, wrapper_job)

    assert result is wrapper_job
    if should_check:
        wrapper_job.check_and_update_status.assert_called_once_with(as_conf)
    else:
        wrapper_job.check_and_update_status.assert_not_called()


def test_manage_wrapper_job_saves_on_change(mocker):
    """manage_wrapper_job: saves job_list when check_and_update_status returns True."""
    as_conf = mocker.MagicMock()
    as_conf.get_wrapper_check_time.return_value = 0
    as_conf.get_notifications.return_value = "false"

    wrapper_job = mocker.MagicMock()
    wrapper_job.status = Status.WAITING
    wrapper_job.id = 99
    wrapper_job.job_list = []
    wrapper_job.check_and_update_status.return_value = True

    job_list = mocker.MagicMock()

    Autosubmit.manage_wrapper_job(as_conf, job_list, wrapper_job)

    job_list.save.assert_called_once()


def test_check_non_wrapped_jobs_empty_platform_jobs(mocker, fake_job_list, fake_platform):
    """check_non_wrapped_jobs: skips platform when no non-wrapped jobs exist."""
    as_conf = mocker.MagicMock()
    fake_job_list.job_package_map = {10: "wrapper"}
    job = Job("a000_20000101_fc0_1_SIM", 10, Status.RUNNING, 0)
    job.platform = fake_platform
    fake_job_list._job_list.append(job)
    # job.id (10) is in job_package_map, so platform_jobs will be empty

    Autosubmit.check_non_wrapped_jobs([fake_platform], fake_job_list, as_conf, "a000")

    fake_platform.check_all_jobs.assert_not_called()


def test_check_non_wrapped_jobs_notifies_on_change(mocker, fake_job_list, fake_platform):
    """check_non_wrapped_jobs: calls job_notify when prev_status differs from status."""
    as_conf = mocker.MagicMock()
    as_conf.get_notifications.return_value = "true"

    job = Job("a000_20000101_fc0_1_SIM", 10, Status.RUNNING, 0)
    job.platform = fake_platform
    job.new_status = Status.COMPLETED
    job.prev_status = Status.QUEUING  # differs from current status
    fake_job_list._job_list.append(job)

    def mock_update_status(self, conf):
        self.status = Status.COMPLETED
        return Status.COMPLETED

    mocker.patch.object(Job, "update_status", mock_update_status)
    mocker.patch.object(fake_job_list, "save")
    mock_notify = mocker.patch.object(Autosubmit, "job_notify")

    Autosubmit.check_non_wrapped_jobs([fake_platform], fake_job_list, as_conf, "a000")

    mock_notify.assert_called_once_with(as_conf, "a000", job)


def test_finish_current_experiment_run(mocker):
    """finish_current_experiment_run: delegates to ExperimentHistory."""
    mock_exp_hist = mocker.patch("autosubmit.autosubmit.ExperimentHistory")
    Autosubmit.finish_current_experiment_run("a000")
    mock_exp_hist.return_value.finish_current_experiment_run.assert_called_once()


def test_submit_ready_jobs_inspect_skips_check(mocker):
    """submit_ready_jobs: inspect=True skips jobs file existence check."""
    as_conf = mocker.MagicMock()
    job_list = mocker.MagicMock()
    job_list.get_ready.return_value = []
    platform = mocker.MagicMock()
    platform.prepare_submission.return_value = ({}, {})

    mocker.patch("autosubmit.autosubmit.check_jobs_file_exists")
    mocker.patch("autosubmit.autosubmit.JobPackager")

    Autosubmit.submit_ready_jobs(as_conf, job_list, [platform], inspect=True)

    from autosubmit.autosubmit import check_jobs_file_exists
    check_jobs_file_exists.assert_not_called()


def test_manage_wrapper_job_sets_prev_status_for_notifications(mocker):
    """manage_wrapper_job: sets prev_status on inner jobs when notifications enabled."""
    as_conf = mocker.MagicMock()
    as_conf.get_wrapper_check_time.return_value = 0
    as_conf.get_notifications.return_value = "true"

    inner = Job("inner", 2, Status.RUNNING, 0)
    wrapper_job = mocker.MagicMock()
    wrapper_job.status = Status.WAITING
    wrapper_job.id = 99
    wrapper_job.job_list = [inner]
    wrapper_job.check_and_update_status.return_value = False

    job_list = mocker.MagicMock()

    Autosubmit.manage_wrapper_job(as_conf, job_list, wrapper_job)

    assert inner.prev_status == Status.RUNNING


def test_check_non_wrapped_jobs_no_status_change(mocker, fake_job_list, fake_platform):
    """check_non_wrapped_jobs: skips update and save when new_status equals status."""
    as_conf = mocker.MagicMock()

    job = Job("a000_20000101_fc0_1_SIM", 10, Status.RUNNING, 0)
    job.platform = fake_platform
    job.new_status = Status.RUNNING  # same as status
    fake_job_list._job_list.append(job)

    mocker.patch.object(fake_job_list, "save")
    mock_notify = mocker.patch.object(Autosubmit, "job_notify")

    Autosubmit.check_non_wrapped_jobs([fake_platform], fake_job_list, as_conf, "a000")

    fake_job_list.save.assert_not_called()
    mock_notify.assert_not_called()


def test_submit_ready_jobs_checks_job_files(mocker):
    """submit_ready_jobs: inspect=False and only_wrappers=False triggers jobs file check."""
    as_conf = mocker.MagicMock()
    job = mocker.MagicMock()
    job.section = "SIM"
    job_list = mocker.MagicMock()
    job_list.get_ready.return_value = [job]
    platform = mocker.MagicMock()
    platform.prepare_submission.return_value = ({}, {})

    mock_check = mocker.patch("autosubmit.autosubmit.check_jobs_file_exists", return_value=False)
    mocker.patch("autosubmit.autosubmit.JobPackager")

    Autosubmit.submit_ready_jobs(as_conf, job_list, [platform], inspect=False, only_wrappers=False)

    mock_check.assert_called_once_with(as_conf, "SIM")


def test_wrapper_notify_calls_job_notify_for_each_inner(mocker):
    """wrapper_notify: calls job_notify for each inner job when notifications enabled."""
    as_conf = mocker.MagicMock()
    as_conf.get_notifications.return_value = "true"

    inner1 = Job("inner1", 1, Status.COMPLETED, 0)
    inner2 = Job("inner2", 2, Status.FAILED, 0)
    wrapper_job = mocker.MagicMock()
    wrapper_job.job_list = [inner1, inner2]

    mock_job_notify = mocker.patch.object(Autosubmit, "job_notify")
    Autosubmit.wrapper_notify(as_conf, "a000", wrapper_job)

    assert mock_job_notify.call_count == 2
    mock_job_notify.assert_any_call(as_conf, "a000", inner1)
    mock_job_notify.assert_any_call(as_conf, "a000", inner2)


def test_wrapper_notify_skips_when_disabled(mocker):
    """wrapper_notify: does nothing when notifications are disabled."""
    as_conf = mocker.MagicMock()
    as_conf.get_notifications.return_value = "false"

    wrapper_job = mocker.MagicMock()
    wrapper_job.job_list = [Job("inner", 1, Status.COMPLETED, 0)]

    mock_job_notify = mocker.patch.object(Autosubmit, "job_notify")
    Autosubmit.wrapper_notify(as_conf, "a000", wrapper_job)

    mock_job_notify.assert_not_called()


@pytest.mark.parametrize("statuses,expected,terminal", [
    ([Status.COMPLETED, Status.COMPLETED], Status.COMPLETED, True),
    ([Status.RUNNING, Status.WAITING], Status.RUNNING, False),
    ([Status.FAILED, Status.WAITING], Status.FAILED, True),
    ([Status.QUEUING, Status.WAITING], Status.QUEUING, False),
    ([Status.HELD, Status.WAITING], Status.HELD, False),
    ([Status.SUBMITTED, Status.WAITING], Status.SUBMITTED, False),
])
def test_check_wrapper_stored_status_sets_status(mocker, statuses, expected, terminal):
    """check_wrapper_stored_status: derives wrapper status from inner job statuses."""
    as_conf = mocker.MagicMock()
    job_list = mocker.MagicMock()
    job_list.packages_dict = {"wrapper_1": [
        Job("j1", 10, statuses[0], 0),
        Job("j2", 11, statuses[1], 0),
    ]}
    job_list.job_package_map = {}

    result = Autosubmit.check_wrapper_stored_status(as_conf, job_list, "01:00")

    if terminal:
        assert "wrapper_1" not in result.packages_dict
    else:
        assert 10 in result.job_package_map
        assert result.job_package_map[10].status == expected


def test_check_wrapper_stored_status_pops_terminal(mocker):
    """check_wrapper_stored_status: removes COMPLETED/FAILED wrappers from packages_dict."""
    as_conf = mocker.MagicMock()
    job_list = mocker.MagicMock()
    job_list.packages_dict = {"wrapper_1": [
        Job("j1", 10, Status.COMPLETED, 0),
    ]}
    job_list.job_package_map = {}

    result = Autosubmit.check_wrapper_stored_status(as_conf, job_list, "01:00")

    assert "wrapper_1" not in result.packages_dict
    assert 10 not in result.job_package_map


def test_check_wrapper_stored_status_no_packages_dict(mocker):
    """check_wrapper_stored_status: returns job_list unchanged when no packages_dict."""
    as_conf = mocker.MagicMock()
    job_list = mocker.MagicMock()
    # Remove packages_dict so hasattr returns False
    del job_list.packages_dict

    result = Autosubmit.check_wrapper_stored_status(as_conf, job_list, "01:00")
    assert result is job_list


def test_submit_ready_jobs_raises_on_missing_template(mocker):
    """submit_ready_jobs: raises AutosubmitCritical when job template is missing."""
    as_conf = mocker.MagicMock()
    job = mocker.MagicMock()
    job.section = "SIM"
    job_list = mocker.MagicMock()
    job_list.get_ready.return_value = [job]
    platform = mocker.MagicMock()

    mocker.patch("autosubmit.autosubmit.check_jobs_file_exists", return_value=True)
    mocker.patch("autosubmit.autosubmit.JobPackager")

    with pytest.raises(AutosubmitCritical) as exc_info:
        Autosubmit.submit_ready_jobs(as_conf, job_list, [platform], inspect=False, only_wrappers=False)

    assert "SIM" in str(exc_info.value)
