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
            PLATFORM: TEST_PS
            RUNNING: chunk
            wallclock: 00:01
    """), 3, "COMPLETED", "simple"),  # No wrappers, simple type

    # Failure
    (dedent("""\
    EXPERIMENT:
        NUMCHUNKS: '2'
    JOBS:
        job:
            SCRIPT: |
                sleep 2
                d_echo "Hello World with id=FAILED"
            PLATFORM: TEST_PS
            RUNNING: chunk
            wallclock: 00:01
            retrials: 2  

    """), (2 + 1) * 2, "FAILED", "simple"),  # No wrappers, simple type
], ids=["Success", "Failure"])
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
                PLATFORM: LOCAL
                RUNNING: chunk
                wallclock: 00:01
        """), 3, "COMPLETED", "simple"),  # No wrappers, simple type

    # Failure
    (dedent("""\
        EXPERIMENT:
            NUMCHUNKS: '2'
        JOBS:
            job:
                SCRIPT: |
                    sleep 2
                    d_echo "Hello World with id=FAILED"
                PLATFORM: TEST_PS
                RUNNING: chunk
                wallclock: 00:01
                retrials: 2  

        """), (2 + 1) * 2, "FAILED", "simple"),  # No wrappers, simple type
], ids=["Success", "Failure"])
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
