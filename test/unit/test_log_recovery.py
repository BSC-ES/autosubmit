import os
import pwd
import time
from pathlib import Path

import pytest
import multiprocessing as mp

from autosubmit.autosubmit import Autosubmit
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.platforms.platform import UniqueQueue
from autosubmitconfigparser.config.configcommon import AutosubmitConfig


def _get_script_files_path() -> Path:
    return Path(__file__).resolve().parent / 'files'


@pytest.fixture
def current_tmpdir(tmpdir_factory):
    folder = tmpdir_factory.mktemp(f'tests')
    os.mkdir(folder.join('scratch'))
    file_stat = os.stat(f"{folder.strpath}")
    file_owner_id = file_stat.st_uid
    file_owner = pwd.getpwuid(file_owner_id).pw_name
    folder.owner = file_owner
    return folder


@pytest.fixture
def prepare_test(current_tmpdir):
    # touch as_misc
    platforms_path = Path(f"{current_tmpdir.strpath}/platforms_t000.yml")
    jobs_path = Path(f"{current_tmpdir.strpath}/jobs_t000.yml")
    project = "whatever"
    scratch_dir = f"{current_tmpdir.strpath}/scratch"
    Path(f"{scratch_dir}/{project}/{current_tmpdir.owner}").mkdir(parents=True, exist_ok=True)
    Path(f"{scratch_dir}/LOG_t000").mkdir(parents=True, exist_ok=True)
    Path(f"{scratch_dir}/LOG_t000/t000.cmd.out.0").touch()
    Path(f"{scratch_dir}/LOG_t000/t000.cmd.err.0").touch()

    # Add each platform to test
    with platforms_path.open('w') as f:
        f.write(f"""
PLATFORMS:
    pytest-ps:
        type: ps
        host: 127.0.0.1
        user: {current_tmpdir.owner}
        project: {project}
        scratch_dir: {scratch_dir}
        """)
    # add a job of each platform type
    with jobs_path.open('w') as f:
        f.write(f"""
JOBS:
    base:
        SCRIPT: |
            echo "Hello World"
            echo sleep 5
        QUEUE: hpc
        PLATFORM: pytest-ps
        RUNNING: once
        wallclock: 00:01
EXPERIMENT:
    # List of start dates
    DATELIST: '20000101'
    # List of members.
    MEMBERS: fc0
    # Unit of the chunk size. Can be hour, day, month, or year.
    CHUNKSIZEUNIT: month
    # Size of each chunk.
    CHUNKSIZE: '4'
    # Number of chunks of the experiment.
    NUMCHUNKS: '2'
    CHUNKINI: ''
    # Calendar used for the experiment. Can be standard or noleap.
    CALENDAR: standard
  """)
    return current_tmpdir


@pytest.fixture
def local(prepare_test):
    # Init Local platform
    from autosubmit.platforms.locplatform import LocalPlatform
    config = {
        'LOCAL_ROOT_DIR': f"{prepare_test}/scratch",
        'LOCAL_TMP_DIR': f"{prepare_test}/scratch",
    }
    local = LocalPlatform(expid='t000', name='local', config=config)
    return local


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
    local.recovery_queue = UniqueQueue(max_items=max_items, ctx=ctx)
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
    local.recovery_queue = UniqueQueue(max_items=max_items, ctx=ctx)
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
    local.recovery_queue = UniqueQueue(max_items=max_items, ctx=ctx)
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
    local.recovery_queue = UniqueQueue(max_items=max_items, ctx=ctx)
    for i in range(max_items):
        local.recovery_queue.put(Job(f'rng{i}', f'000{i}', Status.COMPLETED, 0))
    assert len(local.recovery_queue.all_items) == max_items
    for i in range(max_items):
        local.recovery_queue.put(Job(f'rng2{i}', f'000{i}', Status.COMPLETED, 0))
    assert len(local.recovery_queue.all_items) == max_items
