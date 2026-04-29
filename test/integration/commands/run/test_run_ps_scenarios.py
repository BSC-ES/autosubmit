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

"""Tests that run diverse scenarios for Autosubmit run with a ``ps`` platform
and checks the database for expected results."""
import time
from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING

import pytest
from ruamel.yaml import YAML

from autosubmit.config.basicconfig import BasicConfig
from test.integration.commands.run.conftest import _check_db_fields, _assert_exit_code, _check_files_recovered, \
    _assert_db_fields, _assert_files_recovered, run_in_thread
from test.integration.test_utils.misc import wait_locker

if TYPE_CHECKING:
    from docker.models.containers import Container


# -- Tests

@pytest.mark.xdist_group("slurm")
@pytest.mark.docker
@pytest.mark.ssh
@pytest.mark.parametrize(
    "jobs_data,expected_db_entries,final_status,wrapper_type", [
        # Success
        (
                dedent("""\
                EXPERIMENT:
                    NUMCHUNKS: '3'
                JOBS:
                    JOB:
                        SCRIPT: |
                            echo "Hello World with id=Success"
                            sleep 1
                        PLATFORM: TEST_PS
                        RUNNING: chunk
                        WALLCLOCK: 00:01
                        RETRIALS: 0
                """), 3, "COMPLETED", "simple"
        ),  # No wrappers, simple type
        # Failure
        (
                dedent("""\
                EXPERIMENT:
                    NUMCHUNKS: '2'
                JOBS:
                    JOB:
                        SCRIPT: |
                            sleep 2
                            d_echo "Hello World with id=FAILED"
                        PLATFORM: TEST_PS
                        RUNNING: chunk
                        WALLCLOCK: 00:01
                        RETRIALS: 2
                """), (2 + 1) * 2, "FAILED", "simple"
        ),  # No wrappers, simple type
    ],
    ids=["Success", "Failure"])
def test_run_uninterrupted(
        autosubmit_exp,
        jobs_data: str,
        expected_db_entries,
        final_status,
        wrapper_type,
        ssh_server: 'Container',
        prepare_scratch,
        general_data,
):
    yaml = YAML(typ='rt')
    as_exp = autosubmit_exp(experiment_data=general_data | yaml.load(jobs_data), include_jobs=False, create=True)
    prepare_scratch(expid=as_exp.expid)
    as_conf = as_exp.as_conf
    exp_path = Path(BasicConfig.LOCAL_ROOT_DIR, as_exp.expid)
    tmp_path = Path(exp_path, BasicConfig.LOCAL_TMP_DIR)
    log_dir = tmp_path / f"LOG_{as_exp.expid}"
    as_conf.set_last_as_command('run')

    # Run the experiment
    exit_code = as_exp.autosubmit.run_experiment(expid=as_exp.expid)
    _assert_exit_code(final_status, exit_code)

    # Check and display results
    run_tmpdir = Path(as_conf.basic_config.LOCAL_ROOT_DIR)

    db_check_list = _check_db_fields(run_tmpdir, expected_db_entries, final_status, as_exp.expid)
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


@pytest.mark.xdist_group("slurm")
@pytest.mark.docker
@pytest.mark.ssh
@pytest.mark.parametrize("jobs_data,expected_db_entries,final_status,wrapper_type", [
    # Success
    (dedent("""\
        EXPERIMENT:
            NUMCHUNKS: '3'
        JOBS:
            JOB:
                SCRIPT: |
                    echo "Hello World with id=Success"
                PLATFORM: TEST_PS
                RUNNING: chunk
                WALLCLOCK: 00:01
                RETRIALS: 0
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
                WALLCLOCK: 00:01
                RETRIALS: 2
        """), (2 + 1) * 2, "FAILED", "simple"),  # No wrappers, simple type
], ids=["Success", "Failure"])
def test_run_interrupted(
        autosubmit_exp,
        jobs_data: str,
        expected_db_entries,
        final_status,
        wrapper_type,
        ssh_server: 'Container',
        prepare_scratch,
        general_data,
):
    yaml = YAML(typ='rt')
    as_exp = autosubmit_exp(experiment_data=general_data | yaml.load(jobs_data), include_jobs=False, create=True)
    prepare_scratch(expid=as_exp.expid)
    as_conf = as_exp.as_conf
    exp_path = Path(BasicConfig.LOCAL_ROOT_DIR, as_exp.expid)
    tmp_path = Path(exp_path, BasicConfig.LOCAL_TMP_DIR)
    log_dir = tmp_path / f"LOG_{as_exp.expid}"
    as_conf.set_last_as_command('run')

    # Run the experiment
    as_thread, result, stop_event = run_in_thread(
        as_exp.autosubmit.run_experiment,
        expid=as_exp.expid
    )

    time.sleep(2)

    if as_thread.is_alive():
        stop_event.set()  # signal "terminate"
        as_thread.join(timeout=2)

    assert not as_thread.is_alive(), "Autosubmit thread did not stop as expected."

    exit_code = as_exp.autosubmit.run_experiment(expid=as_exp.expid)

    # Check and display results
    run_tmpdir = Path(as_conf.basic_config.LOCAL_ROOT_DIR)

    db_check_list = _check_db_fields(run_tmpdir, expected_db_entries, final_status, as_exp.expid)
    _assert_db_fields(db_check_list)

    files_check_list = _check_files_recovered(as_conf, log_dir, expected_files=expected_db_entries * 2)
    _assert_files_recovered(files_check_list)

    _assert_exit_code(final_status, exit_code)
