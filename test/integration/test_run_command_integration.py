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

import os
import pwd
import sqlite3
from contextlib import suppress
from pathlib import Path
from textwrap import dedent
from typing import Any, Callable

import pytest
from psutil import ZombieProcess

from autosubmitconfigparser.config.configcommon import AutosubmitConfig

_EXPID = 't000'
"""The experiment ID used throughout the test."""


# TODO expand the tests to test Slurm, PSPlatform, Ecplatform whenever possible

# --- Fixtures.

@pytest.fixture
def as_conf(autosubmit_config: Callable) -> AutosubmitConfig:
    return autosubmit_config(_EXPID, {}, touch_db=False)


@pytest.fixture(autouse=True)
def prepare_run(as_conf: AutosubmitConfig) -> AutosubmitConfig:
    """Prepare the integration test run. This fixture is executed for every test."""
    run_tmpdir = Path(as_conf.basic_config.LOCAL_ROOT_DIR)

    # touch as_misc
    # remove files under _EXPID/conf
    platforms_path = Path(run_tmpdir, f"{_EXPID}/conf/platforms_{_EXPID}.yml")
    main_path = Path(run_tmpdir, f"{_EXPID}/conf/AAA_main.yml")

    # File with platforms.
    with platforms_path.open('w') as f:
        f.write(
            dedent(f"""\
                PLATFORMS:
                    dummy:
                        type: dummy
                """)
        )

    # File with experiment, config, default, project, as.
    with main_path.open('w') as f:
        f.write(
            dedent(f"""\
                EXPERIMENT:
                    DATELIST: '20000101'
                    MEMBERS: fc0
                    CHUNKSIZEUNIT: month
                    CHUNKSIZE: '1'
                    NUMCHUNKS: '1'  
                    CHUNKINI: ''
                    CALENDAR: standard
                
                CONFIG:
                    AUTOSUBMIT_VERSION: ""
                    TOTALJOBS: 3
                    MAXWAITINGJOBS: 3
                    SAFETYSLEEPTIME: 0
                DEFAULT:
                    EXPID: "{_EXPID}"
                    HPCARCH: "local"
                project:
                    PROJECT_TYPE: None
                    PROJECT_DESTINATION: local_project
                """)
        )

    dummy_dir = Path(run_tmpdir, f"scratch/whatever/{run_tmpdir.owner()}/{_EXPID}/dummy_dir")
    real_data = Path(run_tmpdir, f"scratch/whatever/{run_tmpdir.owner()}/{_EXPID}/real_data")
    # We write some dummy data inside the scratch_dir
    dummy_dir.mkdir(parents=True)
    real_data.mkdir(parents=True)

    with open(dummy_dir / 'dummy_file', 'w') as f:
        f.write('dummy data')

    # create some dummy absolute symlinks in expid_dir to test migrate function
    Path(real_data / 'dummy_symlink').symlink_to(dummy_dir / 'dummy_file')

    as_conf.reload(force_load=True)

    return as_conf


# --- Internal utility functions.

def _check_db_fields(run_tmpdir: Path, expected_entries, final_status) -> dict[str, (bool, str)]:
    """
    Check that the database contains the expected number of entries, and that all fields contain data after a completed run.
    """
    # Test database exists.
    job_data_db = run_tmpdir / f'metadata/data/job_data_{_EXPID}.db'
    autosubmit_db = Path(run_tmpdir, "tests.db")
    db_check_list = {
        "JOB_DATA_EXIST": (job_data_db.exists(), f"DB {str(job_data_db)} missing"),
        "AUTOSUBMIT_DB_EXIST": (autosubmit_db.exists(), f"DB {str(autosubmit_db)} missing"),
        "JOB_DATA_FIELDS": {}
    }

    # Check job_data info
    with sqlite3.connect(job_data_db) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("SELECT * FROM job_data")
        rows = c.fetchall()
        db_check_list["JOB_DATA_ENTRIES"] = len(rows) == expected_entries, \
            f"Expected {expected_entries} entries, found {len(rows)}"
        # Convert rows to a list of dictionaries
        rows_as_dicts: list[dict[str, Any]] = [dict(row) for row in rows]
        # Tune the print, so it is more readable, so it is easier to debug in case of failure
        counter_by_name = {}

        excluded_keys = ["status", "finish", "submit", "start", "extra_data", "children", "platform_output"]

        for row_dict in rows_as_dicts:
            # Check that all fields contain data, except extra_data, children, and platform_output
            # Check that submit, start and finish are > 0

            job_name = row_dict["job_name"]

            if job_name not in counter_by_name:
                counter_by_name[job_name] = 0

            if job_name not in db_check_list["JOB_DATA_FIELDS"]:
                db_check_list["JOB_DATA_FIELDS"][job_name] = {}

            check_job_submit = row_dict["submit"] > 0 and row_dict["submit"] != 1970010101
            check_job_start = row_dict["start"] > 0 and row_dict["start"] != 1970010101
            check_job_finish = row_dict["finish"] > 0 and row_dict["finish"] != 1970010101
            check_job_status = row_dict["status"] == final_status
            # TODO: Now that we run the real workflow with less mocking, we cannot get the
            #       debug mock workflow commit, as in reality the temporary project will
            #       simply return an empty commit. We could modify the test to actually create
            #       a project in the future, but this test will verify just that the job data
            #       contains the workflow commit column. For the content we can verify it
            #       later with a more complete functional test using Git.
            check_workflow_commit = "workflow_commit" in row_dict

            db_check_job = db_check_list["JOB_DATA_FIELDS"][job_name]

            job_counter_by_name = str(counter_by_name[job_name])
            db_check_job[job_counter_by_name] = {
                "submit": check_job_submit,
                "start": check_job_start,
                "finish": check_job_finish,
                "status": check_job_status,
                "workflow_commit": check_workflow_commit
            }

            db_check_job[job_counter_by_name]["empty_fields"] = " ".join(
                {
                    str(k): v
                    for k, v in row_dict.items()
                    if k not in excluded_keys and v == ""
                }.keys()
            )

            counter_by_name[job_name] += 1

    return db_check_list


def _assert_db_fields(db_check_list: dict[str, (bool, str)]) -> None:
    """Run assertions against database values, checking for possible issues."""
    assert db_check_list["JOB_DATA_EXIST"][0], db_check_list["JOB_DATA_EXIST"][1]
    assert db_check_list["AUTOSUBMIT_DB_EXIST"][0], db_check_list["AUTOSUBMIT_DB_EXIST"][1]
    assert db_check_list["JOB_DATA_ENTRIES"][0], db_check_list["JOB_DATA_ENTRIES"][1]

    for job_name in db_check_list["JOB_DATA_FIELDS"]:
        db_check_job = db_check_list["JOB_DATA_FIELDS"][job_name]

        for job_counter in db_check_job:
            db_check_job_counter = db_check_job[job_counter]

            for field in db_check_job_counter:
                db_check_job_field = db_check_job_counter[field]

                if field == "empty_fields":
                    assert len(db_check_job_field) == 0
                else:
                    assert db_check_job_field, f"Field {field} missing"


def _assert_exit_code(final_status: str, exit_code: int) -> None:
    """Check that the exit code is correct."""
    if final_status == "FAILED":
        assert exit_code > 0
    else:
        assert exit_code == 0


def _check_files_recovered(as_conf, log_dir, expected_files) -> dict:
    """Check that all files are recovered after a run."""
    retrials = as_conf.experiment_data['JOBS']['JOB'].get('RETRIALS', 0)
    files_check_list = {}
    for f in log_dir.glob('*'):
        files_check_list[f.name] = not any(
            str(f).endswith(f".{i}.err") or str(f).endswith(f".{i}.out") for i in range(retrials + 1))
    stat_files = [str(f).split("_")[-1] for f in log_dir.glob('*') if "STAT" in str(f)]
    for i in range(retrials + 1):
        files_check_list[f"STAT_{i}"] = str(i) in stat_files

    print("\nFiles check results:")
    all_ok = True
    for file in files_check_list:
        if not files_check_list[file]:
            all_ok = False
            print(f"{file} does not exists: {files_check_list[file]}")
    if all_ok:
        print("All log files downloaded are renamed correctly.")
    else:
        print("Some log files are not renamed correctly.")
    files_err_out_found = [
        f for f in log_dir.glob('*')
        if (
                   str(f).endswith(".err") or
                   str(f).endswith(".out") or
                   "retrial" in str(f).lower()
           ) and "ASThread" not in str(f)
    ]
    files_check_list["EXPECTED_FILES"] = len(files_err_out_found) == expected_files
    if not files_check_list["EXPECTED_FILES"]:
        print(f"Expected number of log files: {expected_files}. Found: {len(files_err_out_found)}")
        files_err_out_found_str = ", ".join([f.name for f in files_err_out_found])
        print(f"Log files found: {files_err_out_found_str}")
        print("Log files content:")
        for f in files_err_out_found:
            print(f"File: {f.name}\n{f.read_text()}")
        print("All files, permissions and owner:")
        for f in log_dir.glob('*'):
            file_stat = os.stat(f)
            file_owner_id = file_stat.st_uid
            file_owner = pwd.getpwuid(file_owner_id).pw_name
            print(f"File: {f.name} owner: {file_owner} permissions: {oct(file_stat.st_mode)}")
    else:
        print(f"All log files are gathered: {expected_files}")
    return files_check_list


def _assert_files_recovered(files_check_list):
    """Assert that the files are recovered correctly."""
    for check_name in files_check_list:
        assert files_check_list[check_name]


def _init_run(as_conf, jobs_data):
    """Initialize the run, writing the jobs.yml file and creating the experiment."""
    # write jobs_data
    run_tmpdir = Path(as_conf.basic_config.LOCAL_ROOT_DIR)
    exp_path = run_tmpdir / _EXPID

    jobs_path = exp_path / f"conf/jobs_{_EXPID}.yml"
    log_dir = exp_path / f"tmp/LOG_{_EXPID}"
    with jobs_path.open('w') as f:
        f.write(jobs_data)

    # This is set in _init_log which is not called
    as_misc = exp_path / "conf/as_misc.yml"
    with as_misc.open('w') as f:
        f.write(
            dedent("""\
            AS_MISC: True
            ASMISC:
                COMMAND: run
            AS_COMMAND: run
            """)
        )

    # We have to reload as we changed the jobs.
    as_conf.reload(force_load=True)

    return log_dir


# -- Tests

@pytest.mark.parametrize("jobs_data, expected_db_entries, final_status", [
    # Success
    (dedent("""\
    EXPERIMENT:
        NUMCHUNKS: '3'
    JOBS:
        job:
            SCRIPT: |
                echo "Hello World with id=Success"
            PLATFORM: local
            RUNNING: chunk
            wallclock: 00:01
    """), 3, "COMPLETED"),  # Number of jobs
    # Success wrapper
    (dedent("""\
    EXPERIMENT:
        NUMCHUNKS: '2'
    JOBS:
        job:
            SCRIPT: |
                echo "Hello World with id=Success + wrappers"
            DEPENDENCIES: job-1
            PLATFORM: local
            RUNNING: chunk
            wallclock: 00:01
        job2:
            SCRIPT: |
                echo "Hello World with id=Success + wrappers"
            DEPENDENCIES: job2-1
            PLATFORM: local
            RUNNING: chunk
            wallclock: 00:01
    wrappers:
        wrapper:
            JOBS_IN_WRAPPER: job
            TYPE: vertical
        wrapper2:
            JOBS_IN_WRAPPER: job2
            TYPE: vertical
    """), 4, "COMPLETED"),  # Number of jobs
    # Failure
    (dedent("""\
    EXPERIMENT:
        NUMCHUNKS: '2'
    JOBS:
        job:
            SCRIPT: |
                d_echo "Hello World with id=FAILED"
            PLATFORM: local
            RUNNING: chunk
            wallclock: 00:01
            retrials: 2  # In local, it started to fail at 18 retrials.
    """), (2 + 1) * 2, "FAILED"),  # Retries set (N + 1) * number of jobs to run
    # Failure wrappers
    (dedent("""\
    JOBS:
        job:
            SCRIPT: |
                d_echo "Hello World with id=FAILED + wrappers"
            PLATFORM: local
            DEPENDENCIES: job-1
            RUNNING: chunk
            wallclock: 00:10
            retrials: 2
    wrappers:
        wrapper:
            JOBS_IN_WRAPPER: job
            TYPE: vertical
    """), (2 + 1) * 1, "FAILED"),  # Retries set (N + 1) * job chunk 1 ( the rest shouldn't run )
], ids=["Success", "Success with wrapper", "Failure", "Failure with wrapper"])
def test_run_uninterrupted(
        autosubmit,
        as_conf,
        jobs_data,
        expected_db_entries,
        final_status):
    log_dir = _init_run(as_conf, jobs_data)

    # Run the experiment
    exit_code = autosubmit.run_experiment(expid=_EXPID)
    _assert_exit_code(final_status, exit_code)

    # Check and display results
    run_tmpdir = Path(as_conf.basic_config.LOCAL_ROOT_DIR)

    db_check_list = _check_db_fields(run_tmpdir, expected_db_entries, final_status)
    _assert_db_fields(db_check_list)

    files_check_list = _check_files_recovered(as_conf, log_dir, expected_files=expected_db_entries * 2)
    _assert_files_recovered(files_check_list)


@pytest.mark.parametrize("jobs_data, expected_db_entries, final_status", [
    # Success
    ("""
    EXPERIMENT:
        NUMCHUNKS: '3'
    JOBS:
        job:
            SCRIPT: |
                echo "Hello World with id=Success"
            PLATFORM: local
            RUNNING: chunk
            wallclock: 00:01
    """, 3, "COMPLETED"),  # Number of jobs
    # Success wrapper
    ("""
    EXPERIMENT:
        NUMCHUNKS: '2'
    JOBS:
        job:
            SCRIPT: |
                echo "Hello World with id=Success + wrappers"
            DEPENDENCIES: job-1
            PLATFORM: local
            RUNNING: chunk
            wallclock: 00:01
        job2:
            SCRIPT: |
                echo "Hello World with id=Success + wrappers"
            DEPENDENCIES: job2-1
            PLATFORM: local
            RUNNING: chunk
            wallclock: 00:01
    wrappers:
        wrapper:
            JOBS_IN_WRAPPER: job
            TYPE: vertical
        wrapper2:
            JOBS_IN_WRAPPER: job2
            TYPE: vertical
    """, 4, "COMPLETED"),  # Number of jobs
    # Failure
    ("""
    EXPERIMENT:
        NUMCHUNKS: '2'
    JOBS:
        job:
            SCRIPT: |
                d_echo "Hello World with id=FAILED"
            PLATFORM: local
            RUNNING: chunk
            wallclock: 00:01
            retrials: 2  # In local, it started to fail at 18 retrials.
    """, (2 + 1) * 2, "FAILED"),  # Retries set (N + 1) * number of jobs to run
    # Failure wrappers
    ("""
    EXPERIMENT:
        NUMCHUNKS: '2'
    JOBS:
        job:
            SCRIPT: |
                d_echo "Hello World with id=FAILED + wrappers"
            PLATFORM: local
            DEPENDENCIES: job-1
            RUNNING: chunk
            wallclock: 00:10
            retrials: 2
    wrappers:
        wrapper:
            JOBS_IN_WRAPPER: job
            TYPE: vertical
    """, (2 + 1) * 1, "FAILED"),  # Retries set (N + 1) * job chunk 1 ( the rest shouldn't run )
], ids=["Success", "Success with wrapper", "Failure", "Failure with wrapper"])
def test_run_interrupted(
        autosubmit,
        as_conf,
        jobs_data,
        expected_db_entries,
        final_status,
        mocker):

    log_dir = _init_run(as_conf, jobs_data)

    # Run the experiment
    exit_code = autosubmit.run_experiment(expid=_EXPID)
    _assert_exit_code(final_status, exit_code)

    current_statuses = 'SUBMITTED, QUEUING, RUNNING'
    # When you call ``.cmdline`` in a ``psutil.Process`` during an iteration,
    # if it is a zombie it might raise a ``ZombieProcess`` error.
    with suppress(ZombieProcess):
        autosubmit.stop(
            all_expids=False,
            cancel=False,
            current_status=current_statuses,
            expids=_EXPID,
            force=True,
            force_all=True,
            status='FAILED')

    exit_code = autosubmit.run_experiment(expid=_EXPID)
    _assert_exit_code(final_status, exit_code)

    # Check and display results
    run_tmpdir = Path(as_conf.basic_config.LOCAL_ROOT_DIR)

    db_check_list = _check_db_fields(run_tmpdir, expected_db_entries, final_status)
    _assert_db_fields(db_check_list)

    files_check_list = _check_files_recovered(as_conf, log_dir, expected_files=expected_db_entries * 2)
    _assert_files_recovered(files_check_list)
