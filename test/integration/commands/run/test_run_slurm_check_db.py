from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING

import pytest
from time import sleep

from ruamel.yaml import YAML
from autosubmit.config.basicconfig import BasicConfig
from test.integration.commands.run.conftest import _check_db_fields, _assert_exit_code, _EXPID, _check_files_recovered, _assert_db_fields, _assert_files_recovered, run_in_thread

if TYPE_CHECKING:
    from testcontainers.core.container import DockerContainer


# -- Tests

@pytest.mark.slurm
@pytest.mark.parametrize("jobs_data,expected_db_entries,final_status,wrapper_type", [
    # Success
    (dedent("""\
    
    EXPERIMENT:
        NUMCHUNKS: '3'
    JOBS:
        job:
            SCRIPT: |
                echo "Hello World with id=Success"
                sleep 1
            PLATFORM: TEST_SLURM
            RUNNING: chunk
            wallclock: 00:01
    """), 3, "COMPLETED", "simple"),  # No wrappers, simple type

    # Success wrapper
    (dedent("""\
    EXPERIMENT:
        NUMCHUNKS: '2'
    JOBS:
        job:
            SCRIPT: |
                echo "Hello World with id=Success + wrappers"
                sleep 1
            DEPENDENCIES: job-1
            PLATFORM: TEST_SLURM
            RUNNING: chunk
            wallclock: 00:01

        job2:
            SCRIPT: |
                echo "Hello World with id=Success + wrappers"
                sleep 1
            DEPENDENCIES: job2-1
            PLATFORM: TEST_SLURM
            RUNNING: chunk
            wallclock: 00:01

    wrappers:
        wrapper:
            JOBS_IN_WRAPPER: job
            TYPE: vertical
            policy: flexible
    
        wrapper2:
            JOBS_IN_WRAPPER: job2
            TYPE: vertical
            policy: flexible

    """), 4, "COMPLETED", "vertical"),  # Wrappers present, vertical type

    # Failure
    (dedent("""\
    EXPERIMENT:
        NUMCHUNKS: '2'
    JOBS:
        job:
            SCRIPT: |
                sleep 2
                d_echo "Hello World with id=FAILED"
            PLATFORM: TEST_SLURM
            RUNNING: chunk
            wallclock: 00:01
            retrials: 2  

    """), (2 + 1) * 2, "FAILED", "simple"),  # No wrappers, simple type

    # Failure wrappers
    (dedent("""\
    JOBS:
        job:
            SCRIPT: |
                sleep 2
                d_echo "Hello World with id=FAILED + wrappers"
            PLATFORM: TEST_SLURM
            DEPENDENCIES: job-1
            RUNNING: chunk
            wallclock: 00:10
            retrials: 2
    wrappers:
        wrapper:
            JOBS_IN_WRAPPER: job
            TYPE: vertical
            policy: flexible

    """), (2 + 1) * 1, "FAILED", "vertical"),  # Wrappers present, vertical type

    # wrappers H-V
    (dedent("""\
EXPERIMENT:
    NUMCHUNKS: '2'
    MEMBERS: fc0 fc1
JOBS:
    job:
        SCRIPT: |
            sleep 2
            echo "Hello World"
        PLATFORM: TEST_SLURM
        DEPENDENCIES: job-1
        RUNNING: chunk
        wallclock: 00:10
        PROCESSORS: 1
wrappers:
    wrapper:
        JOBS_IN_WRAPPER: job
        TYPE: horizontal-vertical
        policy: flexible

"""), 4 * 1, "COMPLETED", "horizontal-vertical"),

    # wrappers H-V
    (dedent("""\
EXPERIMENT:
    NUMCHUNKS: '2'
    MEMBERS: fc0 fc1
JOBS:
    job:
        SCRIPT: |
            sleep 2
            f_echo "Hello World with id=FAILED + wrappers"
        PLATFORM: TEST_SLURM
        DEPENDENCIES: job-1
        RUNNING: chunk
        wallclock: 00:10
        retrials: 2
        PROCESSORS: 1
wrappers:
    wrapper:
        JOBS_IN_WRAPPER: job
        TYPE: horizontal-vertical
        policy: flexible

"""), 2 * 3, "FAILED", "horizontal-vertical"),

    # wrappers V-H
    (dedent("""\
EXPERIMENT:
    NUMCHUNKS: '2'
    MEMBERS: fc0 fc1
JOBS:
    job:
        SCRIPT: |
            sleep 2
            echo "Hello World"
        PLATFORM: TEST_SLURM
        DEPENDENCIES: job-1
        RUNNING: chunk
        wallclock: 00:10
        PROCESSORS: 1
wrappers:
    wrapper:
        JOBS_IN_WRAPPER: job
        TYPE: vertical-horizontal
        policy: flexible

"""), 4 * 1, "COMPLETED", "vertical-horizontal"),

    # wrappers V-H
    (dedent("""\
    
EXPERIMENT:
    NUMCHUNKS: '2'
    MEMBERS: fc0 fc1
JOBS:
    job:
        SCRIPT: |
            sleep 2
            d_echo "Hello World"
        PLATFORM: TEST_SLURM
        DEPENDENCIES: job-1
        RUNNING: chunk
        wallclock: 00:10
        retrials: 2
        PROCESSORS: 1
wrappers:
    wrapper:
        JOBS_IN_WRAPPER: job
        TYPE: vertical-horizontal
        policy: flexible

"""), 2 * 3, "FAILED", "vertical-horizontal"),

    # wrappers H
    (dedent("""\
EXPERIMENT:
    NUMCHUNKS: '1'
    MEMBERS: fc0 fc1
JOBS:
    job:
        SCRIPT: |
            sleep 2
            echo "Hello World"
        PLATFORM: TEST_SLURM
        DEPENDENCIES: job-1
        RUNNING: chunk
        wallclock: 00:10
        PROCESSORS: 1
wrappers:
    wrapper:
        JOBS_IN_WRAPPER: job
        TYPE: horizontal
        policy: flexible
"""), 2, "COMPLETED", "horizontal"),

    # wrappers H
    (dedent("""\
EXPERIMENT:
    NUMCHUNKS: '1'
    MEMBERS: fc0 fc1
JOBS:
    job:
        SCRIPT: |
            sleep 2
            f_echo "Hello World"
        PROCESSORS: 1
        PLATFORM: TEST_SLURM
        DEPENDENCIES: job-1
        RUNNING: chunk
        wallclock: 00:10
        retrials: 2
wrappers:
    wrapper:
        JOBS_IN_WRAPPER: job
        TYPE: horizontal
        POLICY: FLEXIBLE

"""), 2 * 3, "FAILED", "horizontal"),
], ids=["Success", "Success with wrapper (vertical_wrapper)", "Failure", "Failure with wrapper (vertical_wrapper)",
        "Success with wrapper (horizontal-vertical)", "Failure with wrapper (horizontal-vertical)",
        "Success with wrapper (vertical-horizontal)", "Failure with wrapper (vertical-horizontal)",
        "Success with wrapper (horizontal)", "Failure with wrapper (horizontal)"])
def test_run_uninterrupted(
        autosubmit_exp,
        jobs_data: str,
        expected_db_entries,
        final_status,
        wrapper_type,
        slurm_server: 'DockerContainer',
        prepare_scratch,
        common_conf,
):
    yaml = YAML(typ='rt')
    as_exp = autosubmit_exp(experiment_data=common_conf | yaml.load(jobs_data), include_jobs=False, create=True)
    as_conf = as_exp.as_conf
    exp_path = Path(BasicConfig.LOCAL_ROOT_DIR, as_exp.expid)
    tmp_path = Path(exp_path, BasicConfig.LOCAL_TMP_DIR)
    log_dir = tmp_path / f"LOG_{as_exp.expid}"
    aslogs_path = Path(tmp_path, BasicConfig.LOCAL_ASLOG_DIR)
    as_conf.set_last_as_command('run')

    as_exp.autosubmit._setup_log_files(
        command="run",
        expids=None,
        expid=as_exp.expid,
        owner=True,
        tmp_path=tmp_path,
        aslogs_path=aslogs_path,
        exp_path=exp_path,
        log_level="DEBUG",
        console_level="DEBUG"
    )
    # Run the experiment
    exit_code = as_exp.autosubmit.run_experiment(expid=_EXPID)
    _assert_exit_code(final_status, exit_code)

    # Check and display results
    run_tmpdir = Path(as_conf.basic_config.LOCAL_ROOT_DIR)

    db_check_list = _check_db_fields(run_tmpdir, expected_db_entries, final_status)
    e_msg = f"Current folder: {str(run_tmpdir)}\n"
    files_check_list = _check_files_recovered(as_conf, log_dir, expected_files=expected_db_entries * 2)
    for check, value in db_check_list.items():
        if not value:
            e_msg += f"{check}: {value}\n"
        elif isinstance(value, dict):
            for job_name in value:
                for job_counter in value[job_name]:
                    for check_name, value_ in value[job_name][job_counter].items():
                        if not value_:
                            if check_name != "empty_fields":
                                e_msg += f"{job_name}_run_number_{job_counter} field: {check_name}: {value_}\n"

    for check, value in files_check_list.items():
        if not value:
            e_msg += f"{check}: {value}\n"
    try:
        _assert_db_fields(db_check_list)
        _assert_files_recovered(files_check_list)
    except AssertionError:
        pytest.fail(e_msg)


@pytest.mark.slurm
@pytest.mark.parametrize("jobs_data,expected_db_entries,final_status,wrapper_type", [
    # Success
    (dedent("""\
    EXPERIMENT:
        NUMCHUNKS: '3'
    JOBS:
        job:
            SCRIPT: |
                echo "Hello World with id=Success"
                sleep 1
            PLATFORM: TEST_SLURM
            RUNNING: chunk
            wallclock: 00:01
    """), 3, "COMPLETED", "simple"),  # No wrappers, simple type

    # Success wrapper
    (dedent("""\
    EXPERIMENT:
        NUMCHUNKS: '2'
    JOBS:
        job:
            SCRIPT: |
                echo "Hello World with id=Success + wrappers"
                sleep 1
            DEPENDENCIES: job-1
            PLATFORM: TEST_SLURM
            RUNNING: chunk
            wallclock: 00:01

        job2:
            SCRIPT: |
                echo "Hello World with id=Success + wrappers"
                sleep 1
            DEPENDENCIES: job2-1
            PLATFORM: TEST_SLURM
            RUNNING: chunk
            wallclock: 00:01

    wrappers:
        wrapper:
            JOBS_IN_WRAPPER: job
            TYPE: vertical
            policy: flexible

        wrapper2:
            JOBS_IN_WRAPPER: job2
            TYPE: vertical
            policy: flexible

    """), 4, "COMPLETED", "vertical"),  # Wrappers present, vertical type

    # Failure
    (dedent("""\
    EXPERIMENT:
        NUMCHUNKS: '2'
    JOBS:
        job:
            SCRIPT: |
                sleep 2
                d_echo "Hello World with id=FAILED"
            PLATFORM: TEST_SLURM
            RUNNING: chunk
            wallclock: 00:01
            retrials: 2  # In local, it started to fail at 18 retrials.

    """), (2 + 1) * 2, "FAILED", "simple"),  # No wrappers, simple type

    # Failure wrappers
    (dedent("""\
    JOBS:
        job:
            SCRIPT: |
                sleep 2
                d_echo "Hello World with id=FAILED + wrappers"
            PLATFORM: TEST_SLURM
            DEPENDENCIES: job-1
            RUNNING: chunk
            wallclock: 00:10
            retrials: 2
    wrappers:
        wrapper:
            JOBS_IN_WRAPPER: job
            TYPE: vertical
            policy: flexible

    """), (2 + 1) * 1, "FAILED", "vertical"),  # Wrappers present, vertical type

    # wrappers H-V
    (dedent("""\
EXPERIMENT:
    NUMCHUNKS: '2'
    MEMBERS: fc0 fc1
JOBS:
    job:
        SCRIPT: |
            sleep 2
            echo "Hello World"
        PLATFORM: TEST_SLURM
        DEPENDENCIES: job-1
        RUNNING: chunk
        wallclock: 00:10
        PROCESSORS: 1
wrappers:
    wrapper:
        JOBS_IN_WRAPPER: job
        TYPE: horizontal-vertical
        policy: flexible

"""), 4 * 1, "COMPLETED", "horizontal-vertical"),

    # wrappers H-V
    (dedent("""\
EXPERIMENT:
    NUMCHUNKS: '2'
    MEMBERS: fc0 fc1
JOBS:
    job:
        SCRIPT: |
            sleep 2
            f_echo "Hello World with id=FAILED + wrappers"
        PLATFORM: TEST_SLURM
        DEPENDENCIES: job-1
        RUNNING: chunk
        wallclock: 00:10
        retrials: 2
        PROCESSORS: 1
wrappers:
    wrapper:
        JOBS_IN_WRAPPER: job
        TYPE: horizontal-vertical
        policy: flexible

"""), 2 * 3, "FAILED", "horizontal-vertical"),

    # wrappers V-H
    (dedent("""\
EXPERIMENT:
    NUMCHUNKS: '2'
    MEMBERS: fc0 fc1
JOBS:
    job:
        SCRIPT: |
            sleep 2
            echo "Hello World"
        PLATFORM: TEST_SLURM
        DEPENDENCIES: job-1
        RUNNING: chunk
        wallclock: 00:10
        PROCESSORS: 1
wrappers:
    wrapper:
        JOBS_IN_WRAPPER: job
        TYPE: vertical-horizontal
        policy: flexible

"""), 4 * 1, "COMPLETED", "vertical-horizontal"),

    # wrappers V-H
    (dedent("""\
EXPERIMENT:
    NUMCHUNKS: '2'
    MEMBERS: fc0 fc1
JOBS:
    job:
        SCRIPT: |
            sleep 2
            d_echo "Hello World"
        PLATFORM: TEST_SLURM
        DEPENDENCIES: job-1
        RUNNING: chunk
        wallclock: 00:10
        retrials: 2
        PROCESSORS: 1
wrappers:
    wrapper:
        JOBS_IN_WRAPPER: job
        TYPE: vertical-horizontal
        policy: flexible

"""), 2 * 3, "FAILED", "vertical-horizontal"),

    # wrappers H
    (dedent("""\
EXPERIMENT:
    NUMCHUNKS: '1'
    MEMBERS: fc0 fc1
JOBS:
    job:
        SCRIPT: |
            sleep 2
            echo "Hello World"
        PLATFORM: TEST_SLURM
        DEPENDENCIES: job-1
        RUNNING: chunk
        wallclock: 00:10
        PROCESSORS: 1
wrappers:
    wrapper:
        JOBS_IN_WRAPPER: job
        TYPE: horizontal
        policy: flexible
"""), 2, "COMPLETED", "horizontal"),

    # wrappers H
    (dedent("""\
EXPERIMENT:
    NUMCHUNKS: '1'
    MEMBERS: fc0 fc1
JOBS:
    job:
        SCRIPT: |
            sleep 2
            f_echo "Hello World"
        PROCESSORS: 1
        PLATFORM: TEST_SLURM
        DEPENDENCIES: job-1
        RUNNING: chunk
        wallclock: 00:10
        retrials: 2
wrappers:
    wrapper:
        JOBS_IN_WRAPPER: job
        TYPE: horizontal
        policy: flexible

"""), 2 * 3, "FAILED", "horizontal"),
], ids=["Success", "Success_simple_v", "Failed", "Failed_simple_v",
        "Success_hybrid_hv", "Failed_hybrid_hv",
        "Success_hybrid_vh", "Failed_hybrid_vh",
        "Success_simple_h", "Failed_simple_h"])
def test_run_interrupted(
        autosubmit_exp,
        jobs_data: str,
        expected_db_entries,
        final_status,
        wrapper_type,
        slurm_server: 'DockerContainer',
        prepare_scratch,
        common_conf,
):
    yaml = YAML(typ='rt')
    as_exp = autosubmit_exp(experiment_data=common_conf | yaml.load(jobs_data), include_jobs=False, create=True)
    as_conf = as_exp.as_conf
    exp_path = Path(BasicConfig.LOCAL_ROOT_DIR, as_exp.expid)
    tmp_path = Path(exp_path, BasicConfig.LOCAL_TMP_DIR)
    log_dir = tmp_path / f"LOG_{as_exp.expid}"
    as_conf.set_last_as_command('run')

    # Run the experiment
    # This was not being interrupted, so we run it in a thread to simulate the interruption and then stop it.
    run_in_thread(as_exp.autosubmit.run_experiment, expid=_EXPID)
    sleep(2)
    current_statuses = 'SUBMITTED, QUEUING, RUNNING'
    as_exp.autosubmit.stop(
        all_expids=False,
        cancel=False,
        current_status=current_statuses,
        arg_expids=_EXPID,
        force=True,
        force_all=True,
        status='FAILED')

    exit_code = as_exp.autosubmit.run_experiment(expid=_EXPID)

    # Check and display results
    run_tmpdir = Path(as_conf.basic_config.LOCAL_ROOT_DIR)

    db_check_list = _check_db_fields(run_tmpdir, expected_db_entries, final_status)
    _assert_db_fields(db_check_list)

    files_check_list = _check_files_recovered(as_conf, log_dir, expected_files=expected_db_entries * 2)
    _assert_files_recovered(files_check_list)

    _assert_exit_code(final_status, exit_code)


@pytest.mark.parametrize("jobs_data, expected_db_entries, final_status, wrapper_type", [

    # Failure
    (dedent("""\
    CONFIG:
        SAFETYSLEEPTIME: 0
    EXPERIMENT:
        NUMCHUNKS: '2'
    JOBS:
        job:
            SCRIPT: |
                d_echo "Hello World with id=FAILED"
            PLATFORM: local
            RUNNING: chunk
            wallclock: 00:01
            retrials: 1  
    """), (2 + 1) * 2, "FAILED", "simple"),  # No wrappers, simple type
], ids=["Force Failure -> Correct it -> Completed"])
def test_run_failed_set_to_ready_on_new_run(
        autosubmit_exp,
        common_conf,
        jobs_data,
        expected_db_entries,
        final_status,
        wrapper_type):
    yaml = YAML(typ='rt')
    jobs_data_yaml = yaml.load(jobs_data)
    as_exp = autosubmit_exp(experiment_data=common_conf | jobs_data_yaml, include_jobs=False, create=True)
    as_conf = as_exp.as_conf
    as_conf.set_last_as_command('run')

    exit_code = as_exp.autosubmit.run_experiment(as_exp.expid)
    _assert_exit_code(final_status, exit_code)

    jobs_data_yaml['JOBS']['job']['SCRIPT'] = """\
                echo "Hello World with id=READY"
    """
    as_exp = autosubmit_exp(as_exp.expid, experiment_data=common_conf | jobs_data_yaml, include_jobs=False, create=True)
    as_conf.set_last_as_command('run')

    exit_code = as_exp.autosubmit.run_experiment(as_exp.expid)

    _assert_exit_code("SUCCESS", exit_code)
