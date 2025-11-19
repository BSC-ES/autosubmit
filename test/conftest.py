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

"""Fixtures available to all test files must be created in this file."""

import os
import pwd
from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING

import pytest

from autosubmit.autosubmit import Autosubmit
from autosubmit.config.basicconfig import generate_dirs, BasicConfig

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from py._path.local import LocalPath  # type: ignore
    from pytest import FixtureRequest


@pytest.fixture(scope='module')
def autosubmit() -> Autosubmit:
    """Create an instance of ``Autosubmit``.

    Useful when you need ``Autosubmit`` but do not need any experiments."""
    return Autosubmit()


@pytest.fixture
def current_tmpdir(tmpdir_factory):
    folder = tmpdir_factory.mktemp('tests')
    os.mkdir(folder.join('scratch'))
    file_stat = os.stat(f"{folder.strpath}")
    file_owner_id = file_stat.st_uid
    file_owner = pwd.getpwuid(file_owner_id).pw_name
    folder.owner = file_owner
    return folder


@pytest.fixture
def prepare_test(current_tmpdir):
    """Prepares test data, creating the platform configuration and a few
    basic files for tests.
    """
    # touch as_misc
    platforms_path = Path(f"{current_tmpdir.strpath}/platforms_t000.yml")
    jobs_path = Path(f"{current_tmpdir.strpath}/jobs_t000.yml")
    project = "whatever"
    scratch_dir = f"{current_tmpdir.strpath}/scratch"
    Path(f"{scratch_dir}/{project}/{current_tmpdir.owner}").mkdir(parents=True, exist_ok=True)
    Path(f"{scratch_dir}/LOG_t000").mkdir(parents=True, exist_ok=True)
    Path(f"{scratch_dir}/LOG_t000/t000.cmd.out.0").touch()
    Path(f"{scratch_dir}/LOG_t000/t000.cmd.err.0").touch()

    # Add each platform to the test
    with platforms_path.open('w') as f:
        f.write(f"""
PLATFORMS:
    pytest-ps:
        type: ps
        host: 127.0.0.1
        user: {current_tmpdir.owner}
        project: {project}
        scratch_dir: {scratch_dir}
        """)
    # add a job of each platform type
    with jobs_path.open('w') as f:
        f.write("""
JOBS:
    base:
        SCRIPT: |
            echo "Hello World"
            echo sleep 5
        QUEUE: hpc
        PLATFORM: pytest-ps
        RUNNING: once
        wallclock: 00:01
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
    NUMCHUNKS: '2'
    CHUNKINI: ''
    # Calendar used for the experiment. Can be standard or noleap.
    CALENDAR: standard
  """)
    return current_tmpdir


@pytest.fixture
def local(prepare_test):
    # Init Local platform
    from autosubmit.platforms.locplatform import LocalPlatform
    config = {
        'LOCAL_ROOT_DIR': f"{prepare_test}/scratch",
        'LOCAL_TMP_DIR': f"{prepare_test}/scratch",
    }
    local = LocalPlatform(expid='t000', name='local', config=config)
    return local


@pytest.fixture(scope='function', autouse=True)
def initialize_autosubmitrc(tmp_path: 'LocalPath', request: 'FixtureRequest', autosubmit: Autosubmit) -> None:
    """Initialize the ``autosubmit.rc`` file for each test, automatically.

    This function should populate enough information so ``BasicConfig.read()``
    works without the need of any mocking.

    The Autosubmit database file used is called ``tests.db``.

    This function can be called multiple times.

    By default, the database backend is SQLite. If you need Postgres, you
    must use the ``as_db`` integration tests fixture, and that fixture will
    modify the INI settings appropriately and create one database per test.
    """
    autosubmitrc = tmp_path / 'autosubmitrc'
    autosubmitrc.write_text(
        dedent(f'''\
                [local]
                path = {tmp_path}

                [globallogs]
                path = {tmp_path / "logs"}

                [structures]
                path = {tmp_path / "metadata/structures"}

                [historicdb]
                path = {tmp_path / "metadata/data"}

                [historiclog]
                path = {tmp_path / "metadata/logs"}

                [defaultstats]
                path = {tmp_path / "as_output/stats"}

                [database]
                backend = sqlite
                path = {tmp_path}
                filename = tests.db
                ''')
    )

    os.environ['AUTOSUBMIT_CONFIGURATION'] = str(autosubmitrc)

    BasicConfig.read()
    generate_dirs()


@pytest.fixture
def test_tmp_path(tmp_path: 'LocalPath', request: 'FixtureRequest') -> Path:
    """Create a ``tmp_path`` subdirectory for the current test.

    This prevents the reuse of ``tmp_path`` across test + fixtures.
    """
    test_name = request.node.name
    test_path = tmp_path / test_name
    test_path.mkdir()
    return Path(test_path)
