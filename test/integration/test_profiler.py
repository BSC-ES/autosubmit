# Copyright 2015-2020 Earth Sciences Department, BSC-CNS

# This file is part of Autosubmit.

# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.
"""
File to create a test for the profiling
"""
from test.unit.utils.common import create_database, init_expid
from pathlib import Path

import os
import pwd
import pytest

from _pytest._py.path import LocalPath
from _pytest.legacypath import TempdirFactory

from autosubmit.autosubmit import Autosubmit

@pytest.fixture
def run_tmpdir(tmpdir_factory: TempdirFactory) -> LocalPath:
    """
    factory creating path and directories for test execution
    :param tmpdir_factory: mktemp
    :return: LocalPath
    """
    folder = tmpdir_factory.mktemp('run_tests')
    os.mkdir(folder.join(Path('scratch')))
    os.mkdir(folder.join(Path('run_tmp_dir')))
    file_stat = os.stat(f"{folder.strpath}")
    file_owner_id = file_stat.st_uid
    file_owner = pwd.getpwuid(file_owner_id).pw_name
    folder.owner = file_owner

    # Write an autosubmitrc file in the temporary directory
    autosubmitrc = folder.join(Path('autosubmitrc'))
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
    os.environ['AUTOSUBMIT_CONFIGURATION'] = str(folder.join(Path('autosubmitrc')))
    create_database(str(folder.join(Path('autosubmitrc'))))
    assert "tests.db" in [Path(f).name for f in folder.listdir()]
    init_expid(str(folder.join(Path('autosubmitrc'))),
               platform='local', create=False, test_type='test')
    assert "t000" in [Path(f).name for f in folder.listdir()]
    return folder

def init_run(run_tmpdir, jobs_data):
    """
    Initialize the run, writing the jobs.yml file and creating the experiment.
    """
    # write jobs_data
    jobs_path = Path(f"{run_tmpdir.strpath}/t000/conf/jobs.yml")
    log_dir = Path(f"{run_tmpdir.strpath}/t000/tmp/LOG_t000")
    with jobs_path.open('w', encoding="utf-8") as f:
        f.write(jobs_data)

    # Create
    init_expid(os.environ["AUTOSUBMIT_CONFIGURATION"], platform='local', expid='t000',
               create=True, test_type='test')

    # This is set in _init_log which is not called
    as_misc = Path(f"{run_tmpdir.strpath}/t000/conf/as_misc.yml")
    with as_misc.open('w', encoding="utf-8") as f:
        f.write("""
            AS_MISC: True
            ASMISC:
                COMMAND: run
            AS_COMMAND: run
        """)
    return log_dir


def check_profile(run_tmpdir) -> bool:
    """
    Initialize the run, writing the jobs.yml file and creating the experiment.
    """
    # write jobs_data
    profile_path = Path(f"{run_tmpdir.strpath}/t000/tmp/profile/")
    if profile_path.exists():
        return True
    return False


@pytest.mark.parametrize("jobs_data, profiler", [
        ("""
            CONFIG:
                SAFETYSLEEPTIME: 0
            EXPERIMENT:
                NUMCHUNKS: '1'
            JOBS:
                job:
                    SCRIPT: |
                        echo "Hello World with id=Success"
            """,
            True
        ),
    ], ids=['profile experiment']
 )
def test_run_profile(run_tmpdir, jobs_data, profiler):
    """
    tester function of the run_profile function
    """
    init_run(run_tmpdir, jobs_data)
    # Run the experiment
    try:
        Autosubmit.run_experiment(expid='t000', profile=profiler)
        assert check_profile(run_tmpdir)
    except Exception as exc:
        assert False, f"test_run_uninterrupted_profile raised an exception: {exc}"
