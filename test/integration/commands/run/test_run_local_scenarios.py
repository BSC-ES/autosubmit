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
import sys

from multiprocessing import Process
from pathlib import Path
from textwrap import dedent

import pytest
from ruamel.yaml import YAML

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.log.log import AutosubmitCritical
from test.integration.commands.run.conftest import (
    _assert_db_fields,
    _assert_exit_code,
    _assert_files_recovered,
    _check_db_fields,
    _check_files_recovered,
)
from test.integration.test_utils.misc import wait_locker


# -- Tests

@pytest.mark.parametrize("jobs_data,expected_db_entries,final_status,run_type", [
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
            PLATFORM: LOCAL
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
        run_type,
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
                PLATFORM: LOCAL
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

    # Run the experiment. This was not being interrupted, so we run it in a
    # child process and then stop it to simulate the interruption.
    process = Process(target=as_exp.autosubmit.run_experiment, args=(as_exp.expid,))
    process.start()

    max_waiting_time_seconds = 30

    # Wait until the process starts (we wait until the file lock is locked).
    lock_file = tmp_path / 'autosubmit.lock'
    wait_locker(lock_file, expect_locked=True, timeout=max_waiting_time_seconds)

    current_statuses = 'SUBMITTED, QUEUING, RUNNING'
    as_exp.autosubmit.stop(
        all_expids=False,
        cancel=False,
        current_status=current_statuses,
        expids=as_exp.expid,
        force=True,
        force_all=True,
        status='FAILED')

    # Ensure the AS run process is done
    process.join(timeout=max_waiting_time_seconds)
    if process.is_alive():
        process.terminate()
        process.join()
    # Wait until the process stops (we wait until the file lock is unlocked).
    wait_locker(lock_file, expect_locked=False, timeout=max_waiting_time_seconds)

    exit_code = as_exp.autosubmit.run_experiment(expid=as_exp.expid)

    # Check and display results
    run_tmpdir = Path(as_conf.basic_config.LOCAL_ROOT_DIR)

    db_check_list = _check_db_fields(run_tmpdir, expected_db_entries, final_status, as_exp.expid)
    _assert_db_fields(db_check_list)

    files_check_list = _check_files_recovered(as_conf, log_dir, expected_files=expected_db_entries * 2)
    _assert_files_recovered(files_check_list)

    _assert_exit_code(final_status, exit_code)


@pytest.mark.parametrize("jobs_data, must_success", [
    # Python: inline script success
    (dedent("""\
        JOBS:
            job:
                SCRIPT: |
                    print("Hello!")
                validate: True
                PLATFORM: LOCAL
                RUNNING: once
                wallclock: 00:01
                type: Python
        """), True),
    # Python: file-based success
    (dedent("""\
        PROJECT:
            PROJECT_TYPE: local
            project_destination: "test"
        JOBS:
            job:
                FILE: test.py
                validate: True
                PLATFORM: LOCAL
                RUNNING: once
                wallclock: 00:01
                type: Python
        """), True),
    # Python: inline script syntax error
    (dedent("""\
        JOBS:
            job:
                SCRIPT: |
                    print("Hello!")syntaxerror
                validate: True
                PLATFORM: LOCAL
                RUNNING: once
                wallclock: 00:01
                type: Python
        """), False),
    # Python: file-based syntax error
    (dedent("""\
        PROJECT:
            PROJECT_TYPE: local
            project_destination: "test"
        JOBS:
            job:
                FILE: test.py
                validate: True
                PLATFORM: LOCAL
                RUNNING: once
                wallclock: 00:01
                type: Python
        """), False),
    # Bash: inline script success
    (dedent("""\
        JOBS:
            job:
                SCRIPT: |
                    echo "Hello from Bash!"
                validate: True
                PLATFORM: LOCAL
                RUNNING: once
                wallclock: 00:01
                type: Bash
        """), True),
    # Bash: file-based success
    (dedent("""\
        PROJECT:
            PROJECT_TYPE: local
            project_destination: "test"
        JOBS:
            job:
                FILE: test.sh
                validate: True
                PLATFORM: LOCAL
                RUNNING: once
                wallclock: 00:01
                type: Bash
        """), True),
    # Bash: inline script syntax error
    (dedent("""\
        JOBS:
            job:
                SCRIPT: |
                    echo "Hello!" $(()invalid
                validate: True
                PLATFORM: LOCAL
                RUNNING: once
                wallclock: 00:01
                type: Bash
        """), False),
    # Bash: file-based syntax error
    (dedent("""\
        PROJECT:
            PROJECT_TYPE: local
            project_destination: "test"
        JOBS:
            job:
                FILE: test.sh
                validate: True
                PLATFORM: LOCAL
                RUNNING: once
                wallclock: 00:01
                type: Bash
        """), False),
    # R-script: inline script success
    (dedent("""\
    JOBS:
        job:
            SCRIPT: |
                print("Hello from R!")
            validate: True
            PLATFORM: LOCAL
            RUNNING: once
            wallclock: 00:01
            type: R
    """), True),
    # R-script: file-based success
    (dedent("""\
    PROJECT:
        PROJECT_TYPE: local
        project_destination: "test"
    JOBS:
        job:
            FILE: test.R
            validate: True
            PLATFORM: LOCAL
            RUNNING: once
            wallclock: 00:01
            type: R
    """), True),
    # R-script: inline script syntax error
    (dedent("""\
    JOBS:
        job:
            SCRIPT: |
                print("Hello from R!")syntaxerror
            validate: True
            PLATFORM: LOCAL
            RUNNING: once
            wallclock: 00:01
            type: R
    """), False),
    # R-script: file-based syntax error
    (dedent("""\
    PROJECT:
        PROJECT_TYPE: local
        project_destination: "test"
    JOBS:
        job:
            FILE: test.R
            validate: True
            PLATFORM: LOCAL
            RUNNING: once
            wallclock: 00:01
            type: R
    """), False),

], ids=[
    "Python-Script",
    "Python-File",
    "Python-Script-syntax-error",
    "Python-File-syntax-error",
    "Bash-Script",
    "Bash-File",
    "Bash-Script-syntax-error",
    "Bash-File-syntax-error",
    "R-Script",
    "R-File",
    "R-Script-syntax-error",
    "R-File-syntax-error",
])
def test_run_debug(
        autosubmit_exp,
        jobs_data: str,
        must_success: bool,
        general_data: dict,
        tmp_path: Path,
):
    """Test debug mode execution for Python and Bash job types.

    Covers inline scripts and file-based jobs, verifying both successful
    execution and proper failure on syntax errors.

    :param autosubmit_exp: Fixture providing an Autosubmit experiment instance.
    :param jobs_data: YAML string defining the job configuration.
    :param must_success: Whether the experiment run is expected to succeed.
    :param general_data: Fixture providing general experiment configuration data.
    :param tmp_path: Pytest-provided temporary directory for project files.
    """
    project_files = tmp_path / "project_files"
    general_data["LOCAL"] = {"PROJECT_PATH": str(project_files)}
    project_files.mkdir(parents=True, exist_ok=True)

    valid_python = 'print("Hello from test.py")'
    invalid_python = 'print("Hello from test.py")syntaxerror'
    valid_bash = '#!/usr/bin/env bash\necho "Hello from test.sh"'
    invalid_bash = '#!/usr/bin/env bash\necho "Hello!" $(()invalid'
    valid_r = 'print("Hello World!")'
    invalid_r = 'print("Hello from test.R")syntaxerror'

    (project_files / "test.py").write_text(valid_python if must_success else invalid_python)
    (project_files / "test.sh").write_text(valid_bash if must_success else invalid_bash)
    (project_files / "test.R").write_text(valid_r if must_success else invalid_r)

    for script_file in project_files.iterdir():
        script_file.chmod(0o755)

    yaml = YAML(typ='rt')
    as_exp = autosubmit_exp(experiment_data=general_data | yaml.load(jobs_data), include_jobs=False, create=True)
    as_conf = as_exp.as_conf
    as_conf.set_last_as_command('run')

    if must_success:
        exit_code = as_exp.autosubmit.run_experiment(expid=as_exp.expid)
        assert exit_code == 0
    else:
        with pytest.raises(AutosubmitCritical) as exc_info:
            as_exp.autosubmit.run_experiment(expid=as_exp.expid)
        assert "Syntax error" in exc_info.value.message
        assert "Generated script" in exc_info.value.message
        assert exc_info.value.code == 7014


def test_run_with_chunk_ini_greater_than_one(
        autosubmit_exp,
        general_data,
        prepare_scratch,
):
    yaml = YAML(typ='rt')
    jobs_data = dedent("""\
        EXPERIMENT:
            DATELIST: "200001[01-03]"
            MEMBERS: "fc[00-02]"
            NUMCHUNKS: '3'
            CHUNKINI: '2'
        JOBS:
            job:
                SCRIPT: |
                    echo "Hello World with id=Success"
                DEPENDENCIES:
                    SIM-1:
                SPLITS: 3
                PLATFORM: LOCAL
                RUNNING: chunk
                wallclock: 00:01
    """)
    as_exp = autosubmit_exp(experiment_data=general_data | yaml.load(jobs_data), include_jobs=False, create=True)
    prepare_scratch(expid=as_exp.expid)
    as_exp.as_conf.set_last_as_command('run')

    exit_code = as_exp.autosubmit.run_experiment(expid=as_exp.expid)

    assert exit_code == 0


@pytest.mark.parametrize(
    "jobs_data, expected_db_entries, final_status, get_call_option",
    [
        (
            dedent("""\
            EXPERIMENT:
                NUMCHUNKS: '1'
            JOBS:
                job:
                    SCRIPT: |
                        echo "Hello from default bash"
                    PLATFORM: LOCAL
                    RUNNING: chunk
                    WALLCLOCK: 00:01
            """),
            1,
            "COMPLETED",
            "default_bash",
        ),
        (
            dedent("""\
            EXPERIMENT:
                NUMCHUNKS: '1'
            JOBS:
                job:
                    SCRIPT: |
                        print("Hello from Python")
                    PLATFORM: LOCAL
                    RUNNING: chunk
                    WALLCLOCK: 00:01
                    TYPE: python
            """),
            1,
            "COMPLETED",
            "type_python",
        ),
        (
            dedent("""\
            EXPERIMENT:
                NUMCHUNKS: '1'
            JOBS:
                job:
                    SCRIPT: |
                        print("Hello from R")
                    PLATFORM: LOCAL
                    RUNNING: chunk
                    WALLCLOCK: 00:01
                    TYPE: r
            """),
            1,
            "COMPLETED",
            "type_r",
        ),
        (
            dedent("""\
            EXPERIMENT:
                NUMCHUNKS: '1'
            JOBS:
                job:
                    SCRIPT: |
                        echo "Hello with explicit /bin/bash executable"
                    PLATFORM: LOCAL
                    RUNNING: chunk
                    WALLCLOCK: 00:01
                    EXECUTABLE: /bin/bash
            """),
            1,
            "COMPLETED",
            "executable_bash",
        ),
        (
            dedent(f"""\
            EXPERIMENT:
                NUMCHUNKS: '1'
            JOBS:
                job:
                    SCRIPT: |
                        print("Hello with explicit python3 executable")
                    PLATFORM: LOCAL
                    RUNNING: chunk
                    WALLCLOCK: 00:01
                    TYPE: python
                    EXECUTABLE: {sys.executable}
            """),
            1,
            "COMPLETED",
            "executable_python3",
        ),
        (
            dedent("""\
            EXPERIMENT:
                NUMCHUNKS: '1'
            JOBS:
                job:
                    SCRIPT: |
                        %CURRENT_EXPORT%
                        echo "AS_INTEGRATION_VAR=${AS_INTEGRATION_VAR}"
                        test "${AS_INTEGRATION_VAR}" = "hello_from_export"
                    PLATFORM: LOCAL
                    RUNNING: chunk
                    WALLCLOCK: 00:01
                    EXPORT: "export AS_INTEGRATION_VAR=hello_from_export"
            """),
            1,
            "COMPLETED",
            "export_placeholder",
        ),
        (
            dedent("""\
            EXPERIMENT:
                NUMCHUNKS: '1'
            JOBS:
                job:
                    SCRIPT: |
                        echo "Hello with X11 explicitly disabled"
                    PLATFORM: LOCAL
                    RUNNING: chunk
                    WALLCLOCK: 00:01
                    X11: False
            """),
            1,
            "COMPLETED",
            "x11_false",
        ),
        (
            dedent("""\
            EXPERIMENT:
                NUMCHUNKS: '1'
            JOBS:
                job:
                    SCRIPT: |
                        echo "Hello with X11 enabled but no x11 options"
                    PLATFORM: LOCAL
                    RUNNING: chunk
                    WALLCLOCK: 00:01
                    X11: True
            """),
            1,
            "COMPLETED",
            "x11_true_no_options",
        ),
        (
            dedent("""\
            EXPERIMENT:
                NUMCHUNKS: '1'
            JOBS:
                job:
                    SCRIPT: |
                        echo "Hello with custom wallclock"
                        sleep 1
                    PLATFORM: LOCAL
                    RUNNING: chunk
                    WALLCLOCK: 00:05
            """),
            1,
            "COMPLETED",
            "wallclock",
        ),
    ],
    ids=[
        "default_bash",
        "type_python",
        "type_r",
        "executable_bash",
        "executable_python3",
        "export_placeholder",
        "x11_false",
        "x11_true_no_options",
        "wallclock",
    ],
)
def test_run_uninterrupted_get_call_options(
    autosubmit_exp,
    jobs_data: str,
    expected_db_entries: int,
    final_status: str,
    get_call_option: str,
    prepare_scratch,
    general_data: dict,
) -> None:
    """Test that all JOBS.job YAML keys that feed get_call work end-to-end.

    Each parametrized case sets one or more of the following options in the
    job YAML configuration:

    - ``EXECUTABLE`` — explicit interpreter placed in the shebang and as a
      command prefix in the get_call execution command.
    - ``TYPE``       — selects the script language (bash / python / r) and
      therefore the default interpreter used when EXECUTABLE is not set.
    - ``EXPORT``     — value exposed as ``%CURRENT_EXPORT%`` placeholder so
      the script body can source or export environment variables.
    - ``X11``        — enables or disables X11 forwarding; with no
      ``X11_OPTIONS`` the submission still uses the standard nohup path.
    - ``WALLCLOCK``  — controls the ``timeout`` prefix injected by get_call.

    Every case runs to COMPLETED and the usual DB / log-file assertions are
    applied to confirm a correct end-to-end execution.

    :param autosubmit_exp: Fixture that creates and manages an Autosubmit experiment.
    :param jobs_data: YAML string with the JOBS section for the experiment.
    :param expected_db_entries: Expected number of rows in the job_data table.
    :param final_status: Expected final job status (``COMPLETED`` or ``FAILED``).
    :param get_call_option: Label identifying the get_call option under test.
    :param prepare_scratch: Fixture that sets up the remote scratch directory.
    :param general_data: Fixture providing base experiment configuration.
    """
    yaml = YAML(typ="rt")
    as_exp = autosubmit_exp(
        experiment_data=general_data | yaml.load(jobs_data), include_jobs=False, create=True
    )
    prepare_scratch(expid=as_exp.expid)
    as_conf = as_exp.as_conf
    exp_path = Path(BasicConfig.LOCAL_ROOT_DIR, as_exp.expid)
    tmp_path = Path(exp_path, BasicConfig.LOCAL_TMP_DIR)
    log_dir = tmp_path / f"LOG_{as_exp.expid}"
    as_conf.set_last_as_command("run")

    exit_code = as_exp.autosubmit.run_experiment(expid=as_exp.expid)
    _assert_exit_code(final_status, exit_code)

    run_tmpdir = Path(as_conf.basic_config.LOCAL_ROOT_DIR)
    db_check_list = _check_db_fields(run_tmpdir, expected_db_entries, final_status, as_exp.expid)
    files_check_list = _check_files_recovered(
        as_conf, log_dir, expected_files=expected_db_entries * 2
    )

    e_msg = f"get_call_option={get_call_option!r}, experiment folder: {run_tmpdir}\n"

    try:
        _assert_db_fields(db_check_list)
        _assert_files_recovered(files_check_list)
    except AssertionError as e:
        pytest.fail(e_msg + str(e))
