import time
import pytest
from pathlib import Path
import os
import pwd

from autosubmit.autosubmit import Autosubmit
from autosubmit.job.job_common import Status
from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from autosubmit.job.job import Job


def _get_script_files_path() -> Path:
    return Path(__file__).resolve().parent / 'files'


@pytest.fixture
def as_conf(prepare_test, mocker):
    mocker.patch('pathlib.Path.exists', return_value=True)
    as_conf = AutosubmitConfig("test")
    as_conf.experiment_data = as_conf.load_config_file(as_conf.experiment_data,
                                                       Path(prepare_test.join('platforms_t000.yml')))
    as_conf.misc_data = {"AS_COMMAND": "run"}
    return as_conf


def test_log_recovery_no_keep_alive(prepare_test, local, mocker, as_conf):
    mocker.patch('autosubmit.platforms.platform.max', return_value=1)
    local.spawn_log_retrieval_process(as_conf)
    assert local.log_recovery_process.is_alive()
    time.sleep(2)
    assert local.log_recovery_process.is_alive() is False
    local.cleanup_event.set()


def test_log_recovery_keep_alive(prepare_test, local, mocker, as_conf):
    mocker.patch('autosubmit.platforms.platform.max', return_value=1)
    local.keep_alive_timeout = 0
    local.spawn_log_retrieval_process(as_conf)
    assert local.log_recovery_process.is_alive()
    local.work_event.set()
    time.sleep(1)
    assert local.log_recovery_process.is_alive()
    local.work_event.set()
    time.sleep(1)
    assert local.log_recovery_process.is_alive()
    time.sleep(1)
    assert local.log_recovery_process.is_alive() is False
    local.cleanup_event.set()


def test_log_recovery_keep_alive_cleanup(prepare_test, local, mocker, as_conf):
    mocker.patch('autosubmit.platforms.platform.max', return_value=1)
    local.keep_alive_timeout = 0
    local.spawn_log_retrieval_process(as_conf)
    assert local.log_recovery_process.is_alive()
    local.work_event.set()
    time.sleep(1)
    assert local.log_recovery_process.is_alive()
    local.work_event.set()
    local.cleanup_event.set()
    time.sleep(1)
    assert local.log_recovery_process.is_alive() is False
    local.cleanup_event.set()


def test_log_recovery_recover_log(prepare_test, local, mocker, as_conf):
    print(prepare_test.strpath)
    mocker.patch('autosubmit.platforms.platform.max', return_value=0)
    local.keep_alive_timeout = 20
    mocker.patch('autosubmit.job.job.Job.write_stats')  # Tested in test_run_command_intregation.py
    local.spawn_log_retrieval_process(as_conf)
    local.work_event.set()
    job = Job('t000', '0000', Status.COMPLETED, 0)
    job.name = 'test_job'
    job.platform = local
    job.platform_name = 'local'
    job.local_logs = ("t000.cmd.out.moved", "t000.cmd.err.moved")
    job._init_runtime_parameters()
    local.work_event.set()
    local.add_job_to_log_recover(job)
    local.cleanup_event.set()
    local.log_recovery_process.join(30)  # should exit earlier.
    assert local.log_recovery_process.is_alive() is False
    assert Path(f"{prepare_test.strpath}/scratch/LOG_t000/t000.cmd.out.moved").exists()
    assert Path(f"{prepare_test.strpath}/scratch/LOG_t000/t000.cmd.err.moved").exists()


def test_refresh_log_retry_process(prepare_test, local, as_conf, mocker):
    mocker.patch('autosubmit.platforms.platform.max', return_value=0)
    local.keep_alive_timeout = 20
    platforms = [local]
    Autosubmit.refresh_log_recovery_process(platforms, as_conf)
    assert local.log_recovery_process.is_alive()
    assert local.work_event.is_set()
    local.cleanup_event.set()
    local.log_recovery_process.join(30)
    assert local.log_recovery_process.is_alive() is False
    Autosubmit.refresh_log_recovery_process(platforms, as_conf)
    assert local.log_recovery_process.is_alive()
    local.send_cleanup_signal()  # this is called by atexit function
    assert local.log_recovery_process.is_alive() is False
