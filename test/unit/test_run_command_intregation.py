import shutil
import pytest
from pathlib import Path
from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from autosubmit.autosubmit import Autosubmit
import os
import pwd
from test.unit.utils.common import create_database, init_expid
import sqlite3


def _get_script_files_path() -> Path:
    return Path(__file__).resolve().parent / 'files'


# TODO expand the tests to test Slurm, PSPlatform, Ecplatform whenever possible

@pytest.fixture
def run_tmpdir(tmpdir_factory):
    folder = tmpdir_factory.mktemp('run_tests')
    os.mkdir(folder.join('scratch'))
    os.mkdir(folder.join('run_tmp_dir'))
    file_stat = os.stat(f"{folder.strpath}")
    file_owner_id = file_stat.st_uid
    file_owner = pwd.getpwuid(file_owner_id).pw_name
    folder.owner = file_owner

    # Write an autosubmitrc file in the temporary directory
    autosubmitrc = folder.join('autosubmitrc')
    autosubmitrc.write(f'''
[database]
path = {folder}
filename = tests.db

[local]
path = {folder}

[globallogs]
path = {folder}

[structures]
path = {folder}

[historicdb]
path = {folder}

[historiclog]
path = {folder}

[defaultstats]
path = {folder}

''')
    os.environ['AUTOSUBMIT_CONFIGURATION'] = str(folder.join('autosubmitrc'))
    create_database(str(folder.join('autosubmitrc')))
    assert "tests.db" in [Path(f).name for f in folder.listdir()]
    init_expid(str(folder.join('autosubmitrc')), platform='local', create=False, test_type='test')
    assert "t000" in [Path(f).name for f in folder.listdir()]
    return folder


@pytest.fixture
def prepare_run(run_tmpdir):
    # touch as_misc
    # remove files under t000/conf
    conf_folder = Path(f"{run_tmpdir.strpath}/t000/conf")
    shutil.rmtree(conf_folder)
    os.makedirs(conf_folder)
    platforms_path = Path(f"{run_tmpdir.strpath}/t000/conf/platforms.yml")
    main_path = Path(f"{run_tmpdir.strpath}/t000/conf/AAAmain.yml")
    # Add each platform to test
    with platforms_path.open('w') as f:
        f.write(f"""
PLATFORMS:
    dummy:
        type: dummy
        """)

    with main_path.open('w') as f:
        f.write("""
EXPERIMENT:
    # List of start dates
    DATELIST: '20000101'
    # List of members.
    MEMBERS: fc0
    # Unit of the chunk size. Can be hour, day, month, or year.
    CHUNKSIZEUNIT: month
    # Size of each chunk.
    CHUNKSIZE: '2'
    # Number of chunks of the experiment.
    NUMCHUNKS: '3'  
    CHUNKINI: ''
    # Calendar used for the experiment. Can be standard or noleap.
    CALENDAR: standard

CONFIG:
    # Current version of Autosubmit.
    AUTOSUBMIT_VERSION: ""
    # Total number of jobs in the workflow.
    TOTALJOBS: 3
    # Maximum number of jobs permitted in the waiting status.
    MAXWAITINGJOBS: 3
    SAFETYSLEEPTIME: 0
DEFAULT:
    # Job experiment ID.
    EXPID: "t000"
    # Default HPC platform name.
    HPCARCH: "local"
    #hint: use %PROJDIR% to point to the project folder (where the project is cloned)
    # Custom configuration location.
project:
    # Type of the project.
    PROJECT_TYPE: None
    # Folder to hold the project sources.
    PROJECT_DESTINATION: local_project
AUTOSUBMIT:
    WORKFLOW_COMMIT: "debug-commit-hash"
""")
    expid_dir = Path(f"{run_tmpdir.strpath}/scratch/whatever/{run_tmpdir.owner}/t000")
    dummy_dir = Path(f"{run_tmpdir.strpath}/scratch/whatever/{run_tmpdir.owner}/t000/dummy_dir")
    real_data = Path(f"{run_tmpdir.strpath}/scratch/whatever/{run_tmpdir.owner}/t000/real_data")
    # We write some dummy data inside the scratch_dir
    os.makedirs(expid_dir, exist_ok=True)
    os.makedirs(dummy_dir, exist_ok=True)
    os.makedirs(real_data, exist_ok=True)

    with open(dummy_dir.joinpath('dummy_file'), 'w') as f:
        f.write('dummy data')
    # create some dummy absolute symlinks in expid_dir to test migrate function
    (real_data / 'dummy_symlink').symlink_to(dummy_dir / 'dummy_file')
    return run_tmpdir


def check_db_fields(run_tmpdir, expected_entries, final_status) -> dict:
    """
    Check that the database contains the expected number of entries, and that all fields contain data after a completed run.
    """
    db_check_list = {}
    # Test database exists.
    job_data = Path(f"{run_tmpdir.strpath}/job_data_t000.db")
    autosubmit_db = Path(f"{run_tmpdir.strpath}/tests.db")
    db_check_list["JOB_DATA_EXIST"] = job_data.exists()
    db_check_list["AUTOSUBMIT_DB_EXIST"] = autosubmit_db.exists()

    # Check job_data info
    conn = sqlite3.connect(job_data)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM job_data")
    rows = c.fetchall()
    db_check_list["JOB_DATA_ENTRIES"] = len(rows) == expected_entries
    # Convert rows to a list of dictionaries
    rows_as_dicts = [dict(row) for row in rows]
    # Tune the print so it is more readable, so it is easier to debug in case of failure
    db_check_list["JOB_DATA_FIELDS"] = {}
    counter_by_name = {}
    for row_dict in rows_as_dicts:
        # Check that all fields contain data, except extra_data, children, and platform_output
        # Check that submit, start and finish are > 0
        if row_dict["job_name"] not in counter_by_name:
            counter_by_name[row_dict["job_name"]] = 0
        if row_dict["job_name"] not in db_check_list["JOB_DATA_FIELDS"]:
            db_check_list["JOB_DATA_FIELDS"][row_dict["job_name"]] = {}
        db_check_list["JOB_DATA_FIELDS"][row_dict["job_name"]][str(counter_by_name[row_dict["job_name"]])] = {}
        db_check_list["JOB_DATA_FIELDS"][row_dict["job_name"]][str(counter_by_name[row_dict["job_name"]])]["submit"] = \
            row_dict["submit"] > 0 and row_dict["submit"] != 1970010101
        db_check_list["JOB_DATA_FIELDS"][row_dict["job_name"]][str(counter_by_name[row_dict["job_name"]])]["start"] = \
            row_dict["start"] > 0 and row_dict["start"] != 1970010101
        db_check_list["JOB_DATA_FIELDS"][row_dict["job_name"]][str(counter_by_name[row_dict["job_name"]])]["finish"] = \
            row_dict["finish"] > 0 and row_dict["finish"] != 1970010101
        db_check_list["JOB_DATA_FIELDS"][row_dict["job_name"]][str(counter_by_name[row_dict["job_name"]])]["status"] = \
            row_dict["status"] == final_status
        db_check_list["JOB_DATA_FIELDS"][row_dict["job_name"]][str(counter_by_name[row_dict["job_name"]])][
            "workflow_commit"] = row_dict["workflow_commit"] == "debug-commit-hash"
        empty_fields = []
        for key in [key for key in row_dict.keys() if
                    key not in ["status", "finish", "submit", "start", "extra_data", "children", "platform_output"]]:
            if str(row_dict[key]) == str(""):
                empty_fields.append(key)
        db_check_list["JOB_DATA_FIELDS"][row_dict["job_name"]][str(counter_by_name[row_dict["job_name"]])][
            "empty_fields"] = " ".join(empty_fields)
        counter_by_name[row_dict["job_name"]] += 1
    print_db_results(db_check_list, rows_as_dicts, run_tmpdir)
    c.close()
    conn.close()
    return db_check_list


def print_db_results(db_check_list, rows_as_dicts, run_tmpdir):
    """
    Print the database check results.
    """
    column_names = rows_as_dicts[0].keys() if rows_as_dicts else []
    column_widths = [max(len(str(row[col])) for row in rows_as_dicts + [dict(zip(column_names, column_names))]) for col
                     in column_names]
    print(f"Experiment folder: {run_tmpdir.strpath}")
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


def assert_db_fields(db_check_list):
    """
    Assert that the database fields are correct.
    """
    assert db_check_list["JOB_DATA_EXIST"]
    assert db_check_list["AUTOSUBMIT_DB_EXIST"]
    assert db_check_list["JOB_DATA_ENTRIES"]
    for job_name in db_check_list["JOB_DATA_FIELDS"]:
        for job_counter in db_check_list["JOB_DATA_FIELDS"][job_name]:
            for field in db_check_list["JOB_DATA_FIELDS"][job_name][job_counter]:
                if field == "empty_fields":
                    assert len(db_check_list['JOB_DATA_FIELDS'][job_name][job_counter][field]) == 0
                else:
                    assert db_check_list['JOB_DATA_FIELDS'][job_name][job_counter][field]


def assert_exit_code(final_status, exit_code):
    """
    Check that the exit code is correct.
    """
    if final_status == "FAILED":
        assert exit_code > 0
    else:
        assert exit_code == 0


def check_files_recovered(run_tmpdir, log_dir, expected_files) -> dict:
    """
    Check that all files are recovered after a run.
    """
    # Check logs recovered and all stat files exists.
    as_conf = AutosubmitConfig("t000")
    as_conf.reload()
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
    files_err_out_found = [f for f in log_dir.glob('*') if (
            str(f).endswith(".err") or str(f).endswith(".out") or "retrial" in str(
        f).lower()) and "ASThread" not in str(f)]
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


def assert_files_recovered(files_check_list):
    """
    Assert that the files are recovered correctly.
    """
    for check_name in files_check_list:
        assert files_check_list[check_name]


def init_run(run_tmpdir, jobs_data):
    """
    Initialize the run, writing the jobs.yml file and creating the experiment.
    """
    # write jobs_data
    jobs_path = Path(f"{run_tmpdir.strpath}/t000/conf/jobs.yml")
    log_dir = Path(f"{run_tmpdir.strpath}/t000/tmp/LOG_t000")
    with jobs_path.open('w') as f:
        f.write(jobs_data)

    # Create
    init_expid(os.environ["AUTOSUBMIT_CONFIGURATION"], platform='local', expid='t000', create=True, test_type='test')

    # This is set in _init_log which is not called
    as_misc = Path(f"{run_tmpdir.strpath}/t000/conf/as_misc.yml")
    with as_misc.open('w') as f:
        f.write("""
    AS_MISC: True
    ASMISC:
        COMMAND: run
    AS_COMMAND: run
            """)
    return log_dir


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
    JOBS:
        job:
            SCRIPT: |
                decho "Hello World with id=FAILED"
            PLATFORM: local
            RUNNING: chunk
            wallclock: 00:01
            retrials: 2  # In local, it started to fail at 18 retrials.
    """, (2 + 1) * 3, "FAILED"),  # Retries set (N + 1) * number of jobs to run
    # Failure wrappers
    ("""
    JOBS:
        job:
            SCRIPT: |
                decho "Hello World with id=FAILED + wrappers"
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
def test_run_uninterrupted(run_tmpdir, prepare_run, jobs_data, expected_db_entries, final_status):
    log_dir = init_run(run_tmpdir, jobs_data)
    # Run the experiment
    exit_code = Autosubmit.run_experiment(expid='t000')

    # Check and display results
    db_check_list = check_db_fields(run_tmpdir, expected_db_entries, final_status)
    files_check_list = check_files_recovered(run_tmpdir, log_dir, expected_files=expected_db_entries * 2)

    # Assert
    assert_db_fields(db_check_list)
    assert_files_recovered(files_check_list)
    # TODO: GITLAB pipeline is not returning 0 or 1 for check_exit_code(final_status, exit_code)
    # assert_exit_code(final_status, exit_code)


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
    JOBS:
        job:
            SCRIPT: |
                decho "Hello World with id=FAILED"
            PLATFORM: local
            RUNNING: chunk
            wallclock: 00:01
            retrials: 2  # In local, it started to fail at 18 retrials.
    """, (2 + 1) * 3, "FAILED"),  # Retries set (N + 1) * number of jobs to run
    # Failure wrappers
    ("""
    JOBS:
        job:
            SCRIPT: |
                decho "Hello World with id=FAILED + wrappers"
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
def test_run_interrupted(run_tmpdir, prepare_run, jobs_data, expected_db_entries, final_status, mocker):
    mocked_input = mocker.patch('autosubmit.autosubmit.input')
    mocked_input.side_effect = ['yes']

    from time import sleep
    log_dir = init_run(run_tmpdir, jobs_data)
    # Run the experiment
    exit_code = Autosubmit.run_experiment(expid='t000')
    assert exit_code == 0 if final_status != 'FAILED' else 1
    sleep(2)
    Autosubmit.stop(all_expids=False, cancel=False, current_status='SUBMITTED, QUEUING, RUNNING', expids='t000',
                    force=True, force_all=False, status='FAILED')
    Autosubmit.run_experiment(expid='t000')
    # Check and display results
    db_check_list = check_db_fields(run_tmpdir, expected_db_entries, final_status)
    files_check_list = check_files_recovered(run_tmpdir, log_dir, expected_files=expected_db_entries * 2)

    # Assert
    assert_db_fields(db_check_list)
    assert_files_recovered(files_check_list)
    # TODO: GITLAB pipeline is not returning 0 or 1 for check_exit_code(final_status, exit_code)
    # assert_exit_code(final_status, exit_code)
