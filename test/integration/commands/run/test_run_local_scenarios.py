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
import sqlite3
import sys
from datetime import datetime, timedelta
from multiprocessing import Process
from pathlib import Path
from textwrap import dedent

import pytest
from ruamel.yaml import YAML

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.helpers.utils import build_and_connect_platform
from autosubmit.log.log import AutosubmitCritical
from autosubmit.platforms.locplatform import LocalPlatform
from autosubmit.platforms.platform_type import PlatformType
from test.integration.commands.run.conftest import (
    _assert_db_fields,
    _assert_exit_code,
    _assert_files_recovered,
    _check_db_fields,
    _check_files_recovered,
    run_in_thread,
)
from test.integration.test_utils.misc import wait_locker


def _job_data_db(expid: str) -> Path:
    """Return the path to an experiment historical database file."""
    return Path(BasicConfig.LOCAL_ROOT_DIR) / 'metadata/data' / f'job_data_{expid}.db'


def _get_last_run_row(expid: str) -> sqlite3.Row:
    """Return the latest ``experiment_run`` row of an experiment historical database."""
    with sqlite3.connect(_job_data_db(expid)) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute("SELECT * FROM experiment_run ORDER BY run_id DESC LIMIT 1").fetchone()


def _count_job_data_entries(expid: str) -> int:
    """Return the number of rows in the ``job_data`` table of an experiment."""
    with sqlite3.connect(_job_data_db(expid)) as conn:
        return conn.execute("SELECT COUNT(*) FROM job_data").fetchone()[0]


def _bounded_poll_sleep(max_polls: int = 100):
    """Return a ``sleep`` replacement that raises ``TimeoutError`` after ``max_polls`` calls.

    Used to bound the ``handle_start_after`` poll loop in the tests: if the
    monitored experiment never satisfies the completion condition, the poll
    stops after ``max_polls`` iterations and the running thread finishes.
    """
    state = {"polls": 0}

    def _sleep(_):
        state["polls"] += 1
        if state["polls"] > max_polls:
            raise TimeoutError("start_after monitor never triggered")

    return _sleep

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

    # Test Splits
    (dedent("""\
    EXPERIMENT:
        NUMCHUNKS: '1'
    JOBS:
        job:
            SCRIPT: |
                echo "Hello World with id=TestSplits"
                sleep 1
            PLATFORM: LOCAL
            RUNNING: chunk
            SPLITS: '2'
            wallclock: 00:01
    """), 2, "COMPLETED", "split"),
    # Test splits: auto
    (dedent("""\
    EXPERIMENT:
        NUMCHUNKS: '1'
        CHUNKSIZE: '1'
        CHUNKUNIT: 'month'
        DATELIST: "20000101"

    JOBS:
        job:
            SCRIPT: |
                echo "Hello World with id=TestSplitsAuto"
                sleep 1
            PLATFORM: LOCAL
            RUNNING: chunk
            SPLITS: auto
            wallclock: 00:01
    """), 31, "COMPLETED", "split"),
], ids=["Success", "Failure", "Test Splits", "Test splits: auto"])
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

    db_check_list = _check_db_fields(run_tmpdir, expected_db_entries, as_exp.expid,
                                     run_type="split" if run_type == "split" else "simple")
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

    max_waiting_time_seconds = 60
    # # Wait until the process starts (we wait until the file lock is locked).
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

    db_check_list = _check_db_fields(run_tmpdir, expected_db_entries, as_exp.expid)
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
    db_check_list = _check_db_fields(run_tmpdir, expected_db_entries, as_exp.expid)
    files_check_list = _check_files_recovered(
        as_conf, log_dir, expected_files=expected_db_entries * 2
    )

    e_msg = f"get_call_option={get_call_option!r}, experiment folder: {run_tmpdir}\n"

    try:
        _assert_db_fields(db_check_list)
        _assert_files_recovered(files_check_list)
    except AssertionError as e:
        pytest.fail(e_msg + str(e))


@pytest.mark.timeout(35)
@pytest.mark.parametrize("run_mode, members, num_chunks, expected_db_entries", [
    ("start_time", None, 1, 1),
    ("start_time", None, 3, 3),
    ("start_after", None, 1, 1),
    ("start_after", None, 3, 3),
    ("run_only_members", "fc0 fc1", 1, 1),
    ("run_only_members", "fc0 fc1", 3, 3),
], ids=[
    "start_time-1chunk",
    "start_time-3chunks",
    "start_after-1chunk",
    "start_after-3chunks",
    "run_only_members-1chunk",
    "run_only_members-3chunks",
])
def test_run_with_run_modes(
        autosubmit_exp,
        general_data,
        prepare_scratch,
        run_mode: str,
        members: str | None,
        num_chunks: int,
        expected_db_entries: int,
        monkeypatch,
):
    """Test the different ``autosubmit run`` trigger/filter flags.

    - ``-st`` / ``--start_time``: the run waits until the given time.
    - ``-sa`` / ``--start_after``: the run starts when the given experiment completes.
    - ``-rom`` / ``--run_only_members``: only the given members are submitted.

    Each mode is exercised with 1- and 3-chunk workflows so the run-totals and
    member-filtering logic is covered for different job counts.
    """
    yaml = YAML(typ='rt')
    jobs_data = dedent(f"""\
    EXPERIMENT:
        NUMCHUNKS: '{num_chunks}'
    JOBS:
        job:
            SCRIPT: |
                echo "Hello World"
                sleep 1
            PLATFORM: LOCAL
            RUNNING: chunk
            wallclock: 00:01
    """)
    experiment_data = yaml.load(jobs_data)
    if members:
        experiment_data["EXPERIMENT"]["MEMBERS"] = members

    as_exp = autosubmit_exp(experiment_data=general_data | experiment_data, include_jobs=False, create=True)
    prepare_scratch(expid=as_exp.expid)
    as_exp.as_conf.set_last_as_command('run')

    if run_mode == "start_time":
        monkeypatch.setattr("autosubmit.helpers.autosubmit_helper.sleep", lambda _: None)
        start_time = (datetime.now() + timedelta(seconds=3)).strftime("%Y-%m-%d %H:%M:%S")
        exit_code = as_exp.autosubmit.run_experiment(expid=as_exp.expid, start_time=start_time)
        _assert_exit_code("COMPLETED", exit_code)
    elif run_mode == "run_only_members":
        exit_code = as_exp.autosubmit.run_experiment(expid=as_exp.expid, run_only_members="fc0")
        _assert_exit_code("COMPLETED", exit_code)
    elif run_mode == "start_after":
        # Experiment A finishes first; experiment B is launched waiting for A's
        # completion via `start_after=A`.
        as_exp_a = autosubmit_exp(
            experiment_data=general_data | yaml.load(jobs_data), include_jobs=False, create=True)
        prepare_scratch(expid=as_exp_a.expid)
        as_exp_a.as_conf.set_last_as_command('run')
        exit_code_a = as_exp_a.autosubmit.run_experiment(expid=as_exp_a.expid)
        _assert_exit_code("COMPLETED", exit_code_a)
        # Speed up the `handle_start_after` poll (sleeps 60s per iteration) and
        # bound it so the thread does not leak if B never starts.
        monkeypatch.setattr("autosubmit.helpers.autosubmit_helper.sleep", _bounded_poll_sleep())
        # Avoid hanging the test if B can't start after A finishes
        thread, result, _ = run_in_thread(
            as_exp.autosubmit.run_experiment, expid=as_exp.expid, start_after=as_exp_a.expid)
        thread.join(timeout=15)
        assert not thread.is_alive(), "Experiment B never started after experiment A finished"
        assert result["exception"] is None
        exit_code = result["exit_code"]
        _assert_exit_code("COMPLETED", exit_code)
        last_run_a = _get_last_run_row(as_exp_a.expid)
        assert last_run_a is not None
        assert last_run_a["finish"] > 0
        assert last_run_a["total"] > 0
        assert last_run_a["total"] == last_run_a["completed"]
    else:
        raise AssertionError(f"Unknown run_mode: {run_mode}")

    # Check and display results
    run_tmpdir = Path(as_exp.as_conf.basic_config.LOCAL_ROOT_DIR)
    db_check_list = _check_db_fields(run_tmpdir, expected_db_entries, as_exp.expid)
    _assert_db_fields(db_check_list)


@pytest.mark.parametrize("scenario", ["failed_job", "not_completed"])
def test_start_after_does_not_start(
        autosubmit_exp,
        general_data,
        prepare_scratch,
        scenario: str,
        monkeypatch,
):
    """B must NOT start when experiment A did not complete all its jobs.

    ``handle_start_after`` only triggers once A's run is finished
    (``finish > 0``) and all its jobs reached a terminal state
    (``total == completed + suspended``). If A has a failed job, or it was
    interrupted before completing, B must keep waiting.
    """
    yaml = YAML(typ='rt')
    if scenario == "failed_job":
        jobs_data = dedent("""\
        EXPERIMENT:
            NUMCHUNKS: '1'
        JOBS:
            job:
                SCRIPT: |
                    d_echo "Hello World with id=FAILED"
                PLATFORM: LOCAL
                RUNNING: chunk
                wallclock: 00:01
                retrials: 1
        """)
    else:  # not_completed
        # job2 depends on job1. job1 sleeps long enough so that job2 never runs
        # before the run is interrupted.
        jobs_data = dedent("""\
        EXPERIMENT:
            NUMCHUNKS: '1'
        JOBS:
            job:
                SCRIPT: |
                    sleep 60
                PLATFORM: LOCAL
                RUNNING: chunk
                wallclock: 00:05
            job2:
                SCRIPT: |
                    echo "Hello World with id=NOT_RUN"
                DEPENDENCIES:
                    job:
                PLATFORM: LOCAL
                RUNNING: chunk
                wallclock: 00:01
        """)

    as_exp_a = autosubmit_exp(experiment_data=general_data | yaml.load(jobs_data), include_jobs=False, create=True)
    prepare_scratch(expid=as_exp_a.expid)
    as_exp_a.as_conf.set_last_as_command('run')

    if scenario == "failed_job":
        exit_code_a = as_exp_a.autosubmit.run_experiment(expid=as_exp_a.expid)
        _assert_exit_code("FAILED", exit_code_a)
    else:  # not_completed
        # Run A in a child process and interrupt it while job1 is still running,
        # so job2 (dependent on job1) never runs. `Autosubmit.stop` cannot be
        # used here: it matches processes by their `autosubmit run <expid>`
        # command line, which does not apply to a Python-call child process.
        exp_path = Path(BasicConfig.LOCAL_ROOT_DIR, as_exp_a.expid)
        lock_file = exp_path / BasicConfig.LOCAL_TMP_DIR / 'autosubmit.lock'
        process = Process(target=as_exp_a.autosubmit.run_experiment, args=(as_exp_a.expid,))
        process.start()
        wait_locker(lock_file, expect_locked=True, timeout=60)
        process.terminate()
        process.join(timeout=30)
        if process.is_alive():
            process.kill()
            process.join()
        wait_locker(lock_file, expect_locked=False, timeout=60)

    # A's run must not be finalized: `finish` is only set on a successful run,
    # which is why B can never satisfy the start_after condition.
    last_run_a = _get_last_run_row(as_exp_a.expid)
    assert last_run_a is not None
    assert last_run_a["finish"] == 0
    if scenario == "not_completed":
        assert last_run_a["completed"] < last_run_a["total"]

    # B waits for A and must never start.
    as_exp_b = autosubmit_exp(experiment_data=general_data | yaml.load(jobs_data), include_jobs=False, create=True)
    prepare_scratch(expid=as_exp_b.expid)
    as_exp_b.as_conf.set_last_as_command('run')
    # Speed up and bound the `handle_start_after` poll so the thread finishes.
    monkeypatch.setattr("autosubmit.helpers.autosubmit_helper.sleep", _bounded_poll_sleep())
    thread, result, _ = run_in_thread(
        as_exp_b.autosubmit.run_experiment, expid=as_exp_b.expid, start_after=as_exp_a.expid)
    thread.join(timeout=15)
    # B never started: its run did not complete and no job was submitted.
    assert result["exception"] is not None, "B started even though A did not complete all its jobs"
    assert _count_job_data_entries(as_exp_b.expid) == 0


def test_run_only_members_invalid_member(autosubmit_exp, general_data, prepare_scratch):
    """An invalid member in ``-rom`` must fail before the run starts."""
    yaml = YAML(typ='rt')
    jobs_data = dedent("""\
    EXPERIMENT:
        NUMCHUNKS: '1'
        MEMBERS: 'fc0 fc1'
    JOBS:
        job:
            SCRIPT: |
                echo "Hello World"
            PLATFORM: LOCAL
            RUNNING: chunk
            wallclock: 00:01
    """)
    as_exp = autosubmit_exp(experiment_data=general_data | yaml.load(jobs_data), include_jobs=False, create=True)
    prepare_scratch(expid=as_exp.expid)
    as_exp.as_conf.set_last_as_command('run')

    with pytest.raises(AutosubmitCritical, match="do not exist"):
        as_exp.autosubmit.run_experiment(expid=as_exp.expid, run_only_members="nonexistent")


def test_start_after_inexistent_experiment(autosubmit_exp, general_data, prepare_scratch):
    """``start_after`` pointing to a non-existent experiment must not block the run."""
    yaml = YAML(typ='rt')
    jobs_data = dedent("""\
    EXPERIMENT:
        NUMCHUNKS: '1'
    JOBS:
        job:
            SCRIPT: |
                echo "Hello World"
            PLATFORM: LOCAL
            RUNNING: chunk
            wallclock: 00:01
    """)
    as_exp = autosubmit_exp(experiment_data=general_data | yaml.load(jobs_data), include_jobs=False, create=True)
    prepare_scratch(expid=as_exp.expid)
    as_exp.as_conf.set_last_as_command('run')

    exit_code = as_exp.autosubmit.run_experiment(expid=as_exp.expid, start_after="a000")

    _assert_exit_code("COMPLETED", exit_code)


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
        general_data,
        jobs_data,
        expected_db_entries,
        final_status,
        wrapper_type,
):
    yaml = YAML(typ='rt')
    jobs_data_yaml = yaml.load(jobs_data)
    as_exp = autosubmit_exp(experiment_data=general_data | jobs_data_yaml, include_jobs=False, create=True)
    as_conf = as_exp.as_conf
    as_conf.set_last_as_command('run')

    exit_code = as_exp.autosubmit.run_experiment(as_exp.expid)
    _assert_exit_code(final_status, exit_code)

    # The experiment must have failed above with a final status.
    # But the job script has d_echo, so here we replace it, and
    # run it again. It should succeed now.
    yaml_with_jobs = Path(as_exp.exp_path, 'conf/additional_data.yml')
    with open(yaml_with_jobs, 'r') as f:
        data = yaml.load(f)
    data["JOBS"]["job"]["SCRIPT"] = 'echo "Hello World with id=READY"'
    with yaml_with_jobs.open("w") as f:
        yaml.dump(data, f)

    as_conf.set_last_as_command('create')
    assert 0 == as_exp.autosubmit.create(as_exp.expid, noplot=True, hide=False, force=True, check_wrappers=False)

    as_conf.set_last_as_command('run')
    exit_code = as_exp.autosubmit.run_experiment(as_exp.expid)

    _assert_exit_code("SUCCESS", exit_code)


def test_build_and_connect_platform_local(autosubmit_exp, general_data):
    """build_and_connect_platform creates a connected LocalPlatform from real config."""
    experiment_data = general_data | {
        "EXPERIMENT": {"MEMBERS": "fc0", "NUMCHUNKS": "1"},
    }
    as_exp = autosubmit_exp(experiment_data=experiment_data, include_jobs=False, create=True)
    plat = build_and_connect_platform(PlatformType.LOCAL.value, as_exp.as_conf, as_exp.expid)
    assert isinstance(plat, LocalPlatform)
    assert plat.TYPE == PlatformType.LOCAL
    assert plat.connected
