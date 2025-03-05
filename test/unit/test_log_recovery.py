import os
import pwd
import time
from distutils.command.config import config
from pathlib import Path
import pytest
import multiprocessing as mp


from autosubmit.autosubmit import Autosubmit
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.platforms.platform import CopyQueue
from autosubmitconfigparser.config.configcommon import AutosubmitConfig


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
    mocker.patch('autosubmit.platforms.platform.Platform.get_mp_context', return_value=mp.get_context('fork'))
    local.spawn_log_retrieval_process(as_conf)
    assert local.log_recovery_process.is_alive()
    time.sleep(2)
    assert local.log_recovery_process.is_alive() is False
    local.cleanup_event.set()


def test_log_recovery_keep_alive(prepare_test, local, mocker, as_conf):
    mocker.patch('autosubmit.platforms.platform.max', return_value=1)
    mocker.patch('autosubmit.platforms.platform.Platform.get_mp_context', return_value=mp.get_context('fork'))
    local.keep_alive_timeout = 0
    local.spawn_log_retrieval_process(as_conf)
    assert local.log_recovery_process.is_alive()
    local.work_event.set()
    time.sleep(0.9)
    assert local.log_recovery_process.is_alive()
    local.work_event.set()
    time.sleep(0.9)
    assert local.log_recovery_process.is_alive()
    time.sleep(1.1)  # added .1 because the code could take a bit more time to exit
    assert local.log_recovery_process.is_alive() is False
    local.cleanup_event.set()


def test_log_recovery_keep_alive_cleanup(prepare_test, local, mocker, as_conf):
    mocker.patch('autosubmit.platforms.platform.max', return_value=1)
    mocker.patch('autosubmit.platforms.platform.Platform.get_mp_context', return_value=mp.get_context('fork'))
    local.keep_alive_timeout = 0
    local.spawn_log_retrieval_process(as_conf)
    assert local.log_recovery_process.is_alive()
    local.work_event.set()
    time.sleep(0.9)
    assert local.log_recovery_process.is_alive()
    local.work_event.set()
    local.cleanup_event.set()
    time.sleep(1.1)  # added .1 because the code could take a bit more time to exit
    assert local.log_recovery_process.is_alive() is False
    local.cleanup_event.set()


def test_log_recovery_recover_log(prepare_test, local, mocker, as_conf):
    print(prepare_test.strpath)
    mocker.patch('autosubmit.platforms.platform.max', return_value=0)
    mocker.patch('autosubmit.platforms.platform.Platform.get_mp_context', return_value=mp.get_context('fork'))
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
    mocker.patch('autosubmit.platforms.platform.Platform.get_mp_context', return_value=mp.get_context('fork'))
    local.keep_alive_timeout = 20
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
], ids=["w(T)|c(F)|rq(T)", "w(T)|c(F)|rq(F)", "w(F)|c(T)|rq(T)", "w(F)|c(T)|rq(F)", "w(F)|c(F)|rq(T)",
        "w(F)|c(F)|rq(F)", "w(T)|c(T)|rq(T)"])
def test_wait_until_timeout(prepare_test, local, as_conf, mocker, cleanup_event, work_event, recovery_queue_full,
                            result):
    mocker.patch('autosubmit.platforms.platform.max', return_value=2)
    mocker.patch('autosubmit.platforms.platform.Platform.get_mp_context', return_value=mp.get_context('fork'))
    local.keep_alive_timeout = 2
    max_items = 1
    ctx = local.get_mp_context()
    local.prepare_process(ctx)
    local.recovery_queue = CopyQueue(ctx=ctx)
    local.cleanup_event.set() if cleanup_event else local.cleanup_event.clear()
    local.work_event.set() if work_event else local.work_event.clear()
    if recovery_queue_full:
        for i in range(max_items):
            local.recovery_queue.put(Job('t000', f'000{i}', Status.COMPLETED, 0))
    process_log = local.wait_until_timeout(2)
    assert process_log == result


@pytest.mark.parametrize("work_event, cleanup_event, recovery_queue_full, result", [
    (True, False, True, True),
    (True, False, False, True),
    (False, True, True, True),
    (False, True, False, True),
    (False, False, True, True),
    (False, False, False, False),
    (True, True, True, True),
], ids=["w(T)|c(F)|rq(T)", "w(T)|c(F)|rq(F)", "w(F)|c(T)|rq(T)", "w(F)|c(T)|rq(F)", "w(F)|c(F)|rq(T)",
        "w(F)|c(F)|rq(F)", "w(T)|c(T)|rq(T)"])
def test_wait_for_work(prepare_test, local, as_conf, mocker, cleanup_event, work_event, recovery_queue_full,
                       result):
    mocker.patch('autosubmit.platforms.platform.max', return_value=2)
    mocker.patch('autosubmit.platforms.platform.Platform.get_mp_context', return_value=mp.get_context('fork'))
    local.keep_alive_timeout = 2
    max_items = 1
    ctx = local.get_mp_context()
    local.prepare_process(ctx)
    local.recovery_queue = CopyQueue(ctx=ctx)
    local.cleanup_event.set() if cleanup_event else local.cleanup_event.clear()
    local.work_event.set() if work_event else local.work_event.clear()
    if recovery_queue_full:
        for i in range(max_items):
            local.recovery_queue.put(Job('t000', f'000{i}', Status.COMPLETED, 0))
    process_log = local.wait_for_work(2)
    assert process_log == result


@pytest.mark.parametrize("work_event, cleanup_event, recovery_queue_full, result", [
    (True, False, True, True),
    (True, False, False, True),
    (False, True, True, True),
    (False, True, False, True),
    (False, False, True, True),
    (False, False, False, False),
    (True, True, True, True),
], ids=["w(T)|c(F)|rq(T)", "w(T)|c(F)|rq(F)", "w(F)|c(T)|rq(T)", "w(F)|c(T)|rq(F)", "w(F)|c(F)|rq(T)",
        "w(F)|c(F)|rq(F)", "w(T)|c(T)|rq(T)"])
def test_wait_mandatory_time(prepare_test, local, as_conf, mocker, cleanup_event, work_event, recovery_queue_full,
                             result):
    mocker.patch('autosubmit.platforms.platform.max', return_value=2)
    mocker.patch('autosubmit.platforms.platform.Platform.get_mp_context', return_value=mp.get_context('fork'))
    local.keep_alive_timeout = 2
    max_items = 1
    ctx = local.get_mp_context()
    local.prepare_process(ctx)
    local.recovery_queue = CopyQueue(ctx=ctx)
    local.cleanup_event.set() if cleanup_event else local.cleanup_event.clear()
    local.work_event.set() if work_event else local.work_event.clear()
    if recovery_queue_full:
        for i in range(max_items):
            local.recovery_queue.put(Job('rng', f'000{i}', Status.COMPLETED, 0))
    process_log = local.wait_mandatory_time(2)
    assert process_log == result


def test_unique_elements(local, mocker):
    mocker.patch('autosubmit.platforms.platform.Platform.get_mp_context', return_value=mp.get_context('fork'))
    max_items = 3
    ctx = local.get_mp_context()
    local.prepare_process(ctx)
    local.recovery_queue = CopyQueue(ctx=ctx)
    for i in range(max_items):
        local.recovery_queue.put(Job(f'rng{i}', f'000{i}', Status.COMPLETED, 0))
    for i in range(max_items):
        local.recovery_queue.put(Job(f'rng2{i}', f'000{i}', Status.COMPLETED, 0))


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


@pytest.fixture
def ps(prepare_test, conf_dict):
    from autosubmit.platforms.psplatform import PsPlatform
    ps = PsPlatform(expid='t000', name='pytest-ps', config=conf_dict)
    return ps


def test_create_a_new_copy(local, pjm, slurm, ps, ecaccess):
    assert local.create_a_new_copy().name == local.name
    assert pjm.create_a_new_copy().name == pjm.name
    assert slurm.create_a_new_copy().name == slurm.name
    assert ps.create_a_new_copy().name == ps.name
    assert ecaccess.create_a_new_copy().name == ecaccess.name
