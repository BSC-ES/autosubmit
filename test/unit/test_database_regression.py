import shutil

import pytest
from pathlib import Path
from autosubmit.autosubmit import Autosubmit
from log.log import Log
import os
import pwd
from autosubmit.platforms.locplatform import LocalPlatform

from test.unit.utils.common import create_database, init_expid
import sqlite3


def _get_script_files_path() -> Path:
    return Path(__file__).resolve().parent / 'files'


# Maybe this should be a regression test

@pytest.fixture
def db_tmpdir(tmpdir_factory):
    folder = tmpdir_factory.mktemp(f'db_tests')
    os.mkdir(folder.join('scratch'))
    os.mkdir(folder.join('db_tmp_dir'))
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
def prepare_db(db_tmpdir):
    # touch as_misc
    # remove files under t000/conf
    conf_folder = Path(f"{db_tmpdir.strpath}/t000/conf")
    shutil.rmtree(conf_folder)
    os.makedirs(conf_folder)
    platforms_path = Path(f"{db_tmpdir.strpath}/t000/conf/platforms.yml")
    main_path = Path(f"{db_tmpdir.strpath}/t000/conf/main.yml")
    # Add each platform to test
    with platforms_path.open('w') as f:
        f.write(f"""
PLATFORMS:
    dummy:
        type: dummy
        """)

    with main_path.open('w') as f:
        f.write(f"""
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
    NUMCHUNKS: '3'  
    CHUNKINI: ''
    # Calendar used for the experiment. Can be standard or noleap.
    CALENDAR: standard

CONFIG:
    # Current version of Autosubmit.
    AUTOSUBMIT_VERSION: ""
    # Total number of jobs in the workflow.
    TOTALJOBS: 20
    # Maximum number of jobs permitted in the waiting status.
    MAXWAITINGJOBS: 20
    SAFETYSLEEPTIME: 1
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
""")
    expid_dir = Path(f"{db_tmpdir.strpath}/scratch/whatever/{db_tmpdir.owner}/t000")
    dummy_dir = Path(f"{db_tmpdir.strpath}/scratch/whatever/{db_tmpdir.owner}/t000/dummy_dir")
    real_data = Path(f"{db_tmpdir.strpath}/scratch/whatever/{db_tmpdir.owner}/t000/real_data")
    # We write some dummy data inside the scratch_dir
    os.makedirs(expid_dir, exist_ok=True)
    os.makedirs(dummy_dir, exist_ok=True)
    os.makedirs(real_data, exist_ok=True)

    with open(dummy_dir.joinpath('dummy_file'), 'w') as f:
        f.write('dummy data')
    # create some dummy absolute symlinks in expid_dir to test migrate function
    (real_data / 'dummy_symlink').symlink_to(dummy_dir / 'dummy_file')
    return db_tmpdir


@pytest.mark.parametrize("jobs_data, expected_count, final_status", [
    # Success
    ("""
    JOBS:
        job:
            SCRIPT: |
                echo "Hello World"
                sleep 1
            PLATFORM: local
            DEPENDENCIES: job-1
            RUNNING: chunk
            wallclock: 00:01
            retrials: 2
    """, 1, "COMPLETED"),
    # Success wrapper
    ("""
    JOBS:
        job:
            SCRIPT: |
                echo "Hello World"
                sleep 1
            DEPENDENCIES: job-1
            PLATFORM: local
            RUNNING: chunk
            wallclock: 00:01
            retrials: 2
    wrappers:
        wrapper:
            JOBS_IN_WRAPPER: job
            TYPE: vertical
    """, 1, "COMPLETED"),
    # Failure
    ("""
    JOBS:
        job:
            SCRIPT: |
                echo "Hello World"
                sleep 1
                exit 1
            DEPENDENCIES: job-1
            PLATFORM: local
            RUNNING: chunk
            wallclock: 00:01
            retrials: 2
    """, 3, "FAILED"),
    # Failure wrappers
    ("""
    JOBS:
        job:
            SCRIPT: |
                echo "Hello World"
                sleep 1
                exit 1
            PLATFORM: local
            DEPENDENCIES: job-1
            RUNNING: chunk
            wallclock: 00:10
            retrials: 2
    wrappers:
        wrapper:
            JOBS_IN_WRAPPER: job
            TYPE: vertical
    """, 3, "FAILED"),
], ids=["Success", "Success with wrapper", "Failure", "Failure with wrapper"])
def test_db(db_tmpdir, prepare_db, jobs_data, expected_count, final_status, mocker):
    # write jobs_data
    jobs_path = Path(f"{db_tmpdir.strpath}/t000/conf/jobs.yml")
    with jobs_path.open('w') as f:
        f.write(jobs_data)

    # Create
    init_expid(os.environ["AUTOSUBMIT_CONFIGURATION"], platform='local', expid='t000', create=True, test_type='test')

    # This is set in _init_log which is not called
    as_misc = Path(f"{db_tmpdir.strpath}/t000/conf/as_misc.yml")
    with as_misc.open('w') as f:
        f.write(f"""
    AS_MISC: True
    ASMISC:
        COMMAND: run
    AS_COMMAND: run
            """)

    # Run the experiment
    with mocker.patch('autosubmit.platforms.platform.max', return_value=20):
        Autosubmit.run_experiment(expid='t000')

    # Test database exists.
    job_data = Path(f"{db_tmpdir.strpath}/job_data_t000.db")
    autosubmit_db = Path(f"{db_tmpdir.strpath}/tests.db")
    assert job_data.exists()
    assert autosubmit_db.exists()

    # Check job_data info
    conn = sqlite3.connect(job_data)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM job_data")
    rows = c.fetchall()
    # Convert rows to a list of dictionaries
    rows_as_dicts = [dict(row) for row in rows]
    # Tune the print so it is more readable, so it is easier to debug in case of failure
    column_names = rows_as_dicts[0].keys() if rows_as_dicts else []
    column_widths = [max(len(str(row[col])) for row in rows_as_dicts + [dict(zip(column_names, column_names))]) for col
                     in column_names]
    print(f"Experiment folder: {db_tmpdir.strpath}")
    header = " | ".join(f"{name:<{width}}" for name, width in zip(column_names, column_widths))
    print(f"\n{header}")
    print("-" * len(header))
    # Print the rows
    for row_dict in rows_as_dicts:  # always print, for debug proposes
        print(" | ".join(f"{str(row_dict[col]):<{width}}" for col, width in zip(column_names, column_widths)))
    for row_dict in rows_as_dicts:
        # Check that all fields contain data, except extra_data, children, and platform_output
        # Check that submit, start and finish are > 0
        assert int(row_dict["submit"]) > 0 and int(row_dict["finish"]) != 1970010101
        assert int(row_dict["start"]) > 0 and int(row_dict["finish"]) != 1970010101
        assert int(row_dict["finish"]) > 0 and int(row_dict["finish"]) != 1970010101
        assert row_dict["status"] == final_status
        for key in [key for key in row_dict.keys() if
                    key not in ["status", "finish", "submit", "start", "extra_data", "children", "platform_output"]]:
            assert str(row_dict[key]) != str("")
    # Check that the job_data table has the expected number of entries
    c.execute("SELECT job_name, COUNT(*) as count FROM job_data GROUP BY job_name")
    count_rows = c.fetchall()
    for row in count_rows:
        assert int(row["count"]) == expected_count
    # Close the cursor and connection
    c.close()
    conn.close()
