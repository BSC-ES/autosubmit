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

"""This integration test runs jobs with the Slurm platform and verifies
if the database values were created correctly. Previously, this file was
called ``test_run_command_integration.py`` and it used the ``LOCAL``
platform with wrappers -- the ``LOCAL`` platform had wrappers support
removed in #2550 after we introduced Docker containers to run integration
tests (and because the implementation of wrappers for local platforms was
an extra and not very well-tested)."""

import os
import pwd
import sqlite3
from pathlib import Path
from typing import Any, Callable

import pytest
from threading import Thread

# TODO: This should come from a fixture to ensure that the expid is unique per test.
_EXPID = 't100'
"""The experiment ID used throughout the test."""


# TODO expand the tests (Ecplatform, PJM) whenever possible
# TODO The db check could be improved to check everything not only the job_data table

# --- Fixtures.
@pytest.fixture(autouse=True, scope="function")
def set_debug_log_level() -> None:
    """
    Set the console log level to DEBUG for all tests in the session.
    """
    from autosubmit.log.log import Log
    Log.set_console_level("DEBUG")


@pytest.fixture
def prepare_scratch(tmp_path: Path) -> Any:
    """
    Create an isolated experiment using a temporary directory for each test.

    :param tmp_path: Temporary directory unique to the test.
    :type tmp_path: Path
    :return: Configured experiment object.
    :rtype: Any
    """
    run_tmpdir = tmp_path

    dummy_dir = run_tmpdir / f"scratch/whatever/{run_tmpdir.owner()}/{_EXPID}/dummy_dir"
    real_data = run_tmpdir / f"scratch/whatever/{run_tmpdir.owner()}/{_EXPID}/real_data"
    dummy_dir.mkdir(parents=True)
    real_data.mkdir(parents=True)

    with open(dummy_dir / 'dummy_file', 'w') as f:
        f.write('dummy data')

    (real_data / 'dummy_symlink').symlink_to(dummy_dir / 'dummy_file')


# --- Internal utility functions.
def _print_db_results(db_check_list, rows_as_dicts, run_tmpdir):
    """
    Print the database check results.
    """
    column_names = rows_as_dicts[0].keys() if rows_as_dicts else []
    column_widths = [max(len(str(row[col])) for row in rows_as_dicts + [dict(zip(column_names, column_names))]) for col
                     in column_names]
    print(f"Experiment folder: {run_tmpdir}")
    header = " | ".join(f"{name:<{width}}" for name, width in zip(column_names, column_widths))
    print(f"\n{header}")
    print("-" * len(header))
    # Print the rows
    for row_dict in rows_as_dicts:  # always print, for debug proposes
        print(" | ".join(f"{str(row_dict[col]):<{width}}" for col, width in zip(column_names, column_widths)))
    # Print the results
    print("\nDatabase check results:")
    print(f"JOB_DATA_EXIST: {db_check_list['JOB_DATA_EXIST']}")
    print(f"AUTOSUBMIT_DB_EXIST: {db_check_list['AUTOSUBMIT_DB_EXIST']}")
    print(f"JOB_DATA_ENTRIES_ARE_CORRECT: {db_check_list['JOB_DATA_ENTRIES']}")

    for job_name in db_check_list["JOB_DATA_FIELDS"]:
        for job_counter in db_check_list["JOB_DATA_FIELDS"][job_name]:
            all_ok = True
            for field in db_check_list["JOB_DATA_FIELDS"][job_name][job_counter]:
                if field == "empty_fields":
                    if len(db_check_list['JOB_DATA_FIELDS'][job_name][job_counter][field]) > 0:
                        all_ok = False
                        print(f"{field} assert FAILED")
                else:
                    if not db_check_list['JOB_DATA_FIELDS'][job_name][job_counter][field]:
                        all_ok = False
                        print(f"{field} assert FAILED")
            if int(job_counter) > 0:
                print(f"Job entry: {job_name} retrial: {job_counter} assert {str(all_ok).upper()}")
            else:
                print(f"Job entry: {job_name} assert {str(all_ok).upper()}")


def run_in_thread(target: Callable[..., Any], *args, **kwargs) -> Thread:
    """
    Run the given target function in a separate thread.

    :param target: The function to execute in the thread.
    :type target: Callable[..., Any]
    :param args: Positional arguments for the target function.
    :type args: Any
    :param kwargs: Keyword arguments for the target function.
    :type kwargs: Any
    :return: The started Thread object.
    :rtype: Thread
    """
    thread = Thread(target=target, args=args, kwargs=kwargs)
    thread.start()
    return thread


def _check_db_fields(run_tmpdir: Path, expected_entries, final_status, expid) -> dict[str, Any]:
    """Check that the database contains the expected number of entries,
    and that all fields contain data after a completed run."""
    # Test database exists.
    job_data_db = run_tmpdir / f'metadata/data/job_data_{expid}.db'
    autosubmit_db = Path(run_tmpdir, "tests.db")
    db_check_list: dict = {
        "JOB_DATA_EXIST": (job_data_db.exists(), f"DB {str(job_data_db)} missing"),
        "AUTOSUBMIT_DB_EXIST": (autosubmit_db.exists(), f"DB {str(autosubmit_db)} missing"),
        "JOB_DATA_FIELDS": {}
    }

    # Check job_data info
    with sqlite3.connect(job_data_db) as conn:
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("SELECT * FROM job_data ORDER BY job_name, counter")
        rows = c.fetchall()
        db_check_list["JOB_DATA_ENTRIES"] = len(rows) == expected_entries, \
            f"Expected {expected_entries} entries, found {len(rows)}"
        # Convert rows to a list of dictionaries
        rows_as_dicts: list[dict[str, Any]] = [dict(row) for row in rows]
        # Tune the print, so it is more readable, so it is easier to debug in case of failure
        counter_by_name = {}
        group_by_job_name = {
            job_name: sorted(
                [row for row in rows_as_dicts if row["job_name"] == job_name],
                key=lambda x: x["job_id"]
            )
            for job_name in {row["job_name"] for row in rows_as_dicts}
        }
        excluded_keys = ["status", "finish", "submit", "start", "extra_data", "children", "platform_output"]

        for job_name, grouped_rows in group_by_job_name.items():
            # Check that all fields contain data, except extra_data, children, and platform_output
            # Check that submit, start and finish are > 0
            counter_by_name[job_name] = len(grouped_rows)
            if job_name not in db_check_list["JOB_DATA_FIELDS"]:
                db_check_list["JOB_DATA_FIELDS"][job_name] = {}

            previous_retry_row: dict = {}
            for i, row_dict in enumerate(grouped_rows):
                check_job_submit = row_dict["submit"] > 0 and row_dict["submit"] != 1970010101
                check_job_start = row_dict["start"] > 0 and row_dict["start"] != 1970010101
                check_job_finish = row_dict["finish"] > 0 and row_dict["finish"] != 1970010101
                check_job_start_submit = int(row_dict["start"]) >= int(row_dict["submit"])
                check_job_finish_start = int(row_dict["finish"]) >= int(row_dict["start"])
                check_job_finish_submit = int(row_dict["finish"]) >= int(row_dict["submit"])
                check_job_status = row_dict["status"] == final_status
                # TODO: Now that we run the real workflow with less mocking, we cannot get the
                #       debug mock workflow commit, as in reality the temporary project will
                #       simply return an empty commit. We could modify the test to actually create
                #       a project in the future, but this test will verify just that the job data
                #       contains the workflow commit column. For the content we can verify it
                #       later with a more complete functional test using Git.
                check_workflow_commit = "workflow_commit" in row_dict

                if previous_retry_row:
                    check_submit_previous_submit_retry = row_dict["submit"] >= previous_retry_row["submit"]
                    check_submit_previous_finish_retry = row_dict["submit"] >= previous_retry_row["finish"]
                    check_submit_previous_start_retry = row_dict["submit"] >= previous_retry_row["start"]

                    check_start_previous_start_retry = row_dict["start"] >= previous_retry_row["start"]
                    check_start_previous_finish_retry = row_dict["start"] >= previous_retry_row["finish"]
                    check_start_previous_submit_retry = row_dict["start"] >= previous_retry_row["submit"]

                    check_finish_previous_finish_retry = row_dict["finish"] >= previous_retry_row["finish"]
                    check_finish_previous_start_retry = row_dict["finish"] >= previous_retry_row["start"]
                    check_finish_previous_submit_retry = row_dict["finish"] >= previous_retry_row["submit"]
                else:
                    check_submit_previous_submit_retry = True
                    check_submit_previous_finish_retry = True
                    check_submit_previous_start_retry = True

                    check_start_previous_start_retry = True
                    check_start_previous_finish_retry = True
                    check_start_previous_submit_retry = True

                    check_finish_previous_finish_retry = True
                    check_finish_previous_start_retry = True
                    check_finish_previous_submit_retry = True

                db_check_job = db_check_list["JOB_DATA_FIELDS"][job_name]
                db_check_job[i] = {
                    "submit": check_job_submit,
                    "start": check_job_start,
                    "start>submit": check_job_start_submit,
                    "finish": check_job_finish,
                    "finish>start": check_job_finish_start,
                    "finish>submit": check_job_finish_submit,
                    "status": check_job_status,
                    "workflow_commit": check_workflow_commit,
                    "submit>=previous_submit": check_submit_previous_submit_retry,
                    "submit>=previous_finish": check_submit_previous_finish_retry,
                    "submit>=previous_start": check_submit_previous_start_retry,
                    "start>=previous_start": check_start_previous_start_retry,
                    "start>=previous_finish": check_start_previous_finish_retry,
                    "start>=previous_submit": check_start_previous_submit_retry,
                    "finish>=previous_finish": check_finish_previous_finish_retry,
                    "finish>=previous_start": check_finish_previous_start_retry,
                    "finish>=previous_submit": check_finish_previous_submit_retry,
                }

                db_check_job[i]["empty_fields"] = "".join(
                    {
                        str(k): v
                        for k, v in row_dict.items()
                        if k not in excluded_keys and v == ""
                    }.keys()
                )
                previous_retry_row = row_dict
    _print_db_results(db_check_list, rows_as_dicts, run_tmpdir)
    return db_check_list


def _assert_db_fields(db_check_list: dict[str, Any]) -> None:
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
    stat_files = [str(f).split("_")[-1] for f in log_dir.parent.glob('*') if "STAT" in str(f)]
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
