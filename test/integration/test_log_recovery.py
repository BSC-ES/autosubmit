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

import multiprocessing as mp
import os
import pwd
import time
from pathlib import Path

import pytest

from autosubmit.autosubmit import Autosubmit
from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.job.job import Job, RecoveryReport
from autosubmit.job.job_common import Status
from autosubmit.platforms.platform import CopyQueue, Platform


def _wait_for_ack(platform, timeout=5):
    """Wait for the child acknowledgement queue (a ``multiprocessing.SimpleQueue``)."""
    deadline = time.time() + timeout
    while platform.recovery_results.empty() and time.time() < deadline:
        time.sleep(0.05)
    assert not platform.recovery_results.empty(), "The recovery child did not acknowledge the job"
    return platform.recovery_results.get()


@pytest.fixture
def as_conf(prepare_test, mocker):
    mocker.patch('pathlib.Path.exists', return_value=True)
    as_conf = AutosubmitConfig("test")
    as_conf.experiment_data = as_conf.load_config_file(as_conf.experiment_data,
                                                       Path(prepare_test.join('platforms_t000.yml')))
    as_conf.misc_data = {"AS_COMMAND": "run"}
    return as_conf


def test_log_recovery_no_keep_alive(prepare_test, local, mocker, as_conf):
    mocker.patch('autosubmit.platforms.platform.Platform.get_mp_context', return_value=mp.get_context('fork'))
    local.config['LOG_RECOVERY_TIMEOUT'] = 1

    local.spawn_log_retrieval_process(as_conf)
    is_alive = local.log_recovery_process.is_alive()
    assert is_alive, 'Log recovery process is not alive'

    start = time.time()
    while local.log_recovery_process.is_alive() and time.time() - start < 10:
        time.sleep(1)

    assert not local.log_recovery_process.is_alive(), 'Log recovery process is still alive'
    local.cleanup_event.set()


def test_log_recovery_keep_alive(prepare_test, local, mocker, as_conf):
    mocker.patch('autosubmit.platforms.platform.Platform.get_mp_context', return_value=mp.get_context('fork'))
    local.config['LOG_RECOVERY_TIMEOUT'] = 1
    local.spawn_log_retrieval_process(as_conf)
    assert local.log_recovery_process.is_alive()
    local.work_event.set()
    assert local.log_recovery_process.is_alive()
    # there is new auto-fixture that doesn't allow sleeps higher than 1 while testing
    deadline = time.monotonic() + 1.2
    while time.monotonic() < deadline and local.log_recovery_process.is_alive():
        time.sleep(0.1)
    assert local.log_recovery_process.is_alive() is False


def test_log_recovery_keep_alive_cleanup(prepare_test, local, mocker, as_conf):
    mocker.patch('autosubmit.platforms.platform.Platform.get_mp_context', return_value=mp.get_context('fork'))
    local.config['LOG_RECOVERY_TIMEOUT'] = 0
    local.spawn_log_retrieval_process(as_conf)
    local.cleanup_event.set()
    deadline = time.time() + 10
    while local.log_recovery_process.is_alive() and time.time() < deadline:
        time.sleep(0.1)
    assert not local.log_recovery_process.is_alive()


def test_log_recovery_recover_log(local, mocker, as_conf):
    mocker.patch('autosubmit.platforms.platform.Platform.get_mp_context', return_value=mp.get_context('fork'))
    local.config['LOG_RECOVERY_TIMEOUT'] = 1
    mocker.patch('autosubmit.job.job.Job.write_stats')
    local.spawn_log_retrieval_process(as_conf)
    local.work_event.set()
    job = Job('t000', 123, Status.COMPLETED, 0)
    job.name = 'test_job'
    job.platform = local
    job.platform_name = 'local'
    job.local_logs = ("t000.cmd.out.moved", "t000.cmd.err.moved")
    local.work_event.set()
    assert local.add_job_to_log_recover(job, as_conf) is True
    # The child runs the real retrieval path and acknowledges the processed job.
    assert _wait_for_ack(local) == 'test_job'
    local.cleanup_event.set()
    local.log_recovery_process.join(30)  # should exit earlier.
    assert local.log_recovery_process.is_alive() is False


def test_log_recovery_child_acknowledges_processed_job(prepare_test, local, as_conf, mocker):
    """The recovery child acknowledges each processed job through the results queue."""
    mocker.patch('autosubmit.platforms.platform.Platform.get_mp_context', return_value=mp.get_context('fork'))
    local.config['LOG_RECOVERY_TIMEOUT'] = 1
    report = RecoveryReport(job_name='test_job', attempts=[], all_succeeded=True)
    mocker.patch('autosubmit.job.job.Job.retrieve_logfiles', return_value=report)
    mocker.patch('autosubmit.job.job.Job.send_cpmip_notification', return_value=None)
    local.spawn_log_retrieval_process(as_conf)
    job = Job('t000', 123, Status.COMPLETED, 0)
    job.name = 'test_job'
    job.platform = local
    job.platform_name = 'local'

    assert local.add_job_to_log_recover(job, as_conf) is True
    assert _wait_for_ack(local) == 'test_job'

    local.cleanup_event.set()
    local.log_recovery_process.join(30)


def test_refresh_log_recovery_process_restarts_dead_process(prepare_test, local, as_conf, mocker):
    mocker.patch('autosubmit.platforms.platform.Platform.get_mp_context', return_value=mp.get_context('fork'))
    local.config['LOG_RECOVERY_TIMEOUT'] = 1
    platforms = [local]
    local.spawn_log_retrieval_process(as_conf)
    Autosubmit.refresh_log_recovery_process(platforms, as_conf)
    assert local.log_recovery_process.is_alive()
    assert local.work_event.is_set()
    local.cleanup_event.set()
    local.log_recovery_process.join(30)
    assert local.log_recovery_process.is_alive() is False
    local.spawn_log_retrieval_process(as_conf)
    Autosubmit.refresh_log_recovery_process(platforms, as_conf)
    assert local.log_recovery_process.is_alive()
    local.send_cleanup_signal()  # this is called by atexit function
    assert local.log_recovery_process.is_alive() is False


@pytest.mark.parametrize("work_event, cleanup_event, recovery_queue_full, result", [
    (True, False, True, True),
    (True, False, False, True),
    (False, True, True, True),
    (False, True, False, True),
    (False, False, True, True),
    (False, False, False, False),
    (True, True, True, True),
], ids=[
    "w(T)|c(F)|rq(T)",
    "w(T)|c(F)|rq(F)",
    "w(F)|c(T)|rq(T)",
    "w(F)|c(T)|rq(F)",
    "w(F)|c(F)|rq(T)",
    "w(F)|c(F)|rq(F)",
    "w(T)|c(T)|rq(T)"
])
def test_wait_for_work(prepare_test, local, as_conf, mocker, cleanup_event, work_event, recovery_queue_full,
                       result):
    mocker.patch('autosubmit.platforms.platform.Platform.get_mp_context', return_value=mp.get_context('fork'))
    local.config['LOG_RECOVERY_TIMEOUT'] = 2
    # This is needed, because in this occasion we are testing the function from the main process
    local.keep_alive_timeout = 2
    max_items = 1
    local.prepare_process()
    local.recovery_queue = CopyQueue(ctx=local.ctx)
    local.cleanup_event.set() if cleanup_event else local.cleanup_event.clear()
    local.work_event.set() if work_event else local.work_event.clear()
    if recovery_queue_full:
        for i in range(max_items):
            local.recovery_queue.put(Job('t000', f'000{i}', Status.COMPLETED, 0))
    process_log = local.wait_for_work()
    assert process_log == result


@pytest.fixture()
def conf_dict(tmpdir_factory):
    temp = tmpdir_factory.mktemp('scheduler_tests')
    os.mkdir(temp.join('scratch'))
    os.mkdir(temp.join('scheduler_tmp_dir'))
    file_stat = os.stat(f"{temp.strpath}")
    file_owner_id = file_stat.st_uid
    file_owner = pwd.getpwuid(file_owner_id).pw_name
    temp.owner = file_owner
    return {
        "pytest-pjm": {
            "type": "pjm",
            "host": "127.0.0.1",
            "user": temp.owner,
            "project": "whatever",
            "scratch_dir": f"{temp}/scratch",
            "MAX_WALLCLOCK": "48:00",
            "TEMP_DIR": "",
            "MAX_PROCESSORS": 99999,
            "queue": "dummy",
            "DISABLE_RECOVERY_THREADS": True
        },
        "pytest-slurm": {
            "type": "slurm",
            "host": "127.0.0.1",
            "user": temp.owner,
            "project": "whatever",
            "scratch_dir": f"{temp}/scratch",
            "QUEUE": "gp_debug",
            "ADD_PROJECT_TO_HOST": False,
            "MAX_WALLCLOCK": "48:00",
            "TEMP_DIR": "",
            "MAX_PROCESSORS": 99999,
            "PROCESSORS_PER_NODE": 123,
            "DISABLE_RECOVERY_THREADS": True
        },
        "pytest-ecaccess": {
            "type": "ecaccess",
            "version": "slurm",
            "host": "127.0.0.1",
            "QUEUE": "nf",
            "EC_QUEUE": "hpc",
            "user": temp.owner,
            "project": "whatever",
            "scratch_dir": f"{temp}/scratch",
            "DISABLE_RECOVERY_THREADS": True
        },
        "pytest-ps": {
            "type": "ps",
            "host": "127.0.0.1",
            "user": temp.owner,
            "project": "whatever",
            "scratch_dir": f"{temp}/scratch",
            "DISABLE_RECOVERY_THREADS": True
        },
        'LOCAL_ROOT_DIR': f"{temp}/scratch",
        'LOCAL_TMP_DIR': f"{temp}/scratch",
        'LOCAL_ASLOG_DIR': f"{temp}/scratch",
    }


@pytest.fixture
def pjm(prepare_test, conf_dict):
    from autosubmit.platforms.pjmplatform import PJMPlatform
    pjm = PJMPlatform(expid='t000', name='pytest-pjm', config=conf_dict)
    return pjm


@pytest.fixture
def slurm(prepare_test, conf_dict):
    from autosubmit.platforms.slurmplatform import SlurmPlatform
    slurm = SlurmPlatform(expid='t000', name='pytest-slurm', config=conf_dict)
    return slurm


@pytest.fixture
def ecaccess(prepare_test, conf_dict):
    from autosubmit.platforms.ecplatform import EcPlatform
    ecaccess = EcPlatform(expid='t000', name='pytest-ecaccess', config=conf_dict, scheduler='slurm')
    return ecaccess


def test_create_a_new_copy(local, pjm, slurm, ps_platform, ecaccess):
    assert local.create_a_new_copy().name == local.name
    assert pjm.create_a_new_copy().name == pjm.name
    assert slurm.create_a_new_copy().name == slurm.name
    assert ps_platform.create_a_new_copy().name == ps_platform.name
    assert ecaccess.create_a_new_copy().name == ecaccess.name


def test_refresh_log_recovery_process(local, autosubmit, as_conf, mocker):
    as_conf.misc_data["AS_COMMAND"] = 'run'

    assert local.work_event is None

    local.clean_log_recovery_process()

    assert local.work_event is None

    p = local.work_event

    mocker.patch('multiprocessing.process.BaseProcess.is_alive', return_value=True)
    autosubmit.refresh_log_recovery_process(platforms=[local], as_conf=as_conf)
    assert p != local.work_event
    assert local.work_event.is_set()


def test_worker_events_no_duplicates(local, mocker):
    """Verify that update_workers() does not add the same event twice."""
    mocker.patch('autosubmit.platforms.platform.Platform.get_mp_context', return_value=mp.get_context('fork'))
    Platform.worker_events.clear()
    local.prepare_process()
    # prepare_process adds one event; verify adding the same one again doesn't duplicate
    count_after_prepare = len(Platform.worker_events)
    assert count_after_prepare >= 1
    Platform.update_workers(local.work_event)
    assert len(Platform.worker_events) == count_after_prepare
    Platform.worker_events.clear()


def test_add_job_to_log_recover_failure_does_not_increment_updated_log(local, as_conf, mocker):
    """Verify that when recovery_queue.put fails, updated_log is NOT incremented."""
    mocker.patch('autosubmit.platforms.platform.Platform.get_mp_context', return_value=mp.get_context('fork'))
    local.prepare_process()
    local.recovery_queue = CopyQueue(ctx=local.ctx)
    as_conf.experiment_data["RUNTIME"] = {"LOG_RECOVERY": {"INLINE_FALLBACK": False}}
    job = Job('t000', '0000', Status.COMPLETED, 0)
    job.name = 'test_job'
    job.id = 123
    job.updated_log = 0
    local.log_recovery_process = mocker.MagicMock()
    local.log_recovery_process.is_alive.return_value = True

    # Patch put to simulate a failure
    mocker.patch.object(local.recovery_queue, 'put_state', side_effect=Exception('Queue full'))
    assert local.add_job_to_log_recover(job, as_conf) is False
    assert job.updated_log == 0


def test_add_job_to_log_recover_no_queue(local, as_conf, mocker):
    """Verify graceful skip when recovery_queue is None."""
    local.recovery_queue = None
    as_conf.experiment_data["RUNTIME"] = {"LOG_RECOVERY": {"INLINE_FALLBACK": False}}
    mocker.patch.object(local, 'clean_log_recovery_process')
    mocker.patch.object(local, 'spawn_log_retrieval_process', return_value=False)
    job = Job('t000', '0000', Status.COMPLETED, 0)
    job.name = 'test_job'
    job.id = 123
    job.updated_log = 0
    # Should not raise and should not increment updated_log
    assert local.add_job_to_log_recover(job, as_conf) is False
    assert job.updated_log == 0


def test_clean_log_recovery_process_with_dead_child(local, as_conf, mocker):
    """Verify cleanup does not hang when child has crashed."""
    mocker.patch('autosubmit.platforms.platform.Platform.get_mp_context', return_value=mp.get_context('fork'))
    local.config['LOG_RECOVERY_TIMEOUT'] = 1
    local.spawn_log_retrieval_process(as_conf)
    assert local.log_recovery_process.is_alive()
    # Abruptly kill the child to simulate a crash
    local.log_recovery_process.kill()
    local.log_recovery_process.join(timeout=5)
    assert not local.log_recovery_process.is_alive()
    # Cleanup should return promptly without hanging
    local.clean_log_recovery_process()
    assert local.recovery_queue is None
    assert local.log_recovery_process is None
