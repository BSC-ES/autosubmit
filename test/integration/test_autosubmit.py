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

"""Integration tests for ``autosubmit run`` command."""

from contextlib import nullcontext as does_not_raise
from os import R_OK, W_OK
from pathlib import Path
from shutil import copy
from typing import TYPE_CHECKING

import pytest

from autosubmit.autosubmit import Autosubmit
from autosubmit.config.basicconfig import BasicConfig
from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.config.yamlparser import YAMLParserFactory
from autosubmit.database.db_common import get_experiment_description
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_list import JobList
from autosubmit.log.log import AutosubmitCritical
from autosubmit.platforms.platform import Platform
from autosubmit.scripts.autosubmit import main

if TYPE_CHECKING:
    from test.integration.conftest import AutosubmitExperimentFixture
    from contextlib import AbstractContextManager


def test__init_logs_config_file_not_found(autosubmit, autosubmit_exp, mocker, monkeypatch):
    """Test that an error is raised when the ``BasicConfig.CONFIG_FILE_FOUND`` returns ``False``."""
    autosubmit_exp()

    args = mocker.MagicMock()
    args.logconsole = 'DEBUG'
    args.logfile = 'DEBUG'
    args.command = 'describe'

    monkeypatch.setattr(BasicConfig, 'CONFIG_FILE_FOUND', False)

    with pytest.raises(AutosubmitCritical) as cm:
        autosubmit.run_command(args)

    assert 'No configuration file' in str(cm.value.message)


def test__init_logs_sqlite_db_path_not_found(autosubmit, autosubmit_exp, mocker, monkeypatch, tmp_path):
    """Test that an error is raised when the SQLite file cannot be located."""
    exp = autosubmit_exp()

    args = mocker.MagicMock()
    args.expid = exp.expid
    args.logconsole = 'DEBUG'
    args.logfile = 'DEBUG'
    args.command = 'describe'

    monkeypatch.setattr(BasicConfig, 'DATABASE_BACKEND', 'sqlite')
    monkeypatch.setattr(BasicConfig, 'DB_PATH', str(tmp_path / 'you-cannot-find-me.xz'))

    with pytest.raises(AutosubmitCritical) as cm:
        autosubmit.run_command(args)

    assert 'Experiments database not found in this filesystem' in str(cm.value.message)


def test__init_logs_sqlite_db_not_readable(autosubmit, autosubmit_exp, mocker, monkeypatch):
    """Test that an error is raised when the SQLite file is not readable."""
    exp = autosubmit_exp()

    args = mocker.MagicMock()
    args.expid = exp.expid
    args.logconsole = 'DEBUG'
    args.logfile = 'DEBUG'
    args.command = 'describe'

    monkeypatch.setattr(BasicConfig, 'DATABASE_BACKEND', 'sqlite')

    def path_exists(_, perm):
        return perm != R_OK

    mocker.patch('os.access', side_effect=path_exists)

    with pytest.raises(AutosubmitCritical) as cm:
        autosubmit.run_command(args)

    assert 'not readable' in str(cm.value.message)


def test__init_logs_sqlite_db_not_writable(autosubmit, autosubmit_exp, mocker, monkeypatch):
    """Test that an error is raised when the SQLite file is not writable."""
    exp = autosubmit_exp()

    args = mocker.MagicMock()
    args.expid = exp.expid
    args.logconsole = 'DEBUG'
    args.logfile = 'DEBUG'
    args.command = 'describe'

    monkeypatch.setattr(BasicConfig, 'DATABASE_BACKEND', 'sqlite')

    def path_exists(_, perm):
        return perm != W_OK

    mocker.patch('os.access', side_effect=path_exists)

    with pytest.raises(AutosubmitCritical) as cm:
        autosubmit.run_command(args)

    assert 'not writable' in str(cm.value.message)


def test__init_logs_sqlite_exp_path_does_not_exist(autosubmit, autosubmit_exp, mocker, monkeypatch):
    """Test that an error is raised when the experiment path does not exist and SQLite is used."""
    autosubmit_exp()

    args = mocker.MagicMock()
    args.expid = '0000'
    args.logconsole = 'DEBUG'
    args.logfile = 'DEBUG'
    args.command = 'setstatus'

    monkeypatch.setattr(BasicConfig, 'DATABASE_BACKEND', 'sqlite')

    with pytest.raises(AutosubmitCritical) as cm:
        autosubmit.run_command(args)

    assert 'Experiment does not exist' == str(cm.value.message)


def test__init_logs_postgres_exp_path_does_not_exist_no_yaml_data(autosubmit, autosubmit_exp, mocker, monkeypatch):
    """Test that a new experiment is created for Postgres when the directory is empty,
    but an error is raised when the experiment data is empty."""
    autosubmit_exp()

    args = mocker.MagicMock()
    args.expid = '0000'
    args.logconsole = 'DEBUG'
    args.logfile = 'DEBUG'
    args.command = 'setstatus'

    monkeypatch.setattr(BasicConfig, 'DATABASE_BACKEND', 'postgres')
    mocker.patch('autosubmit.config.configcommon.AutosubmitConfig.reload')

    with pytest.raises(AutosubmitCritical) as cm:
        autosubmit.run_command(args)

    assert 'has no yml data' in str(cm.value.message)


def test__init_logs_sqlite_mismatch_as_version_upgrade_it(autosubmit, autosubmit_exp, mocker):
    """Test that setting an invalid AS version but passing the arg to update version results in the command
    being called correctly."""
    exp = autosubmit_exp(experiment_data={
        'CONFIG': {
            'AUTOSUBMIT_VERSION': 'bright-opera'
        }
    })

    args = mocker.MagicMock()
    args.expid = exp.expid
    args.logconsole = 'DEBUG'
    args.logfile = 'DEBUG'
    args.command = 'setstatus'
    args.update_version = True
    args.__contains__ = lambda x, y: True

    mocked_set_status = mocker.patch('autosubmit.autosubmit.Autosubmit.set_status')

    autosubmit.run_command(args)

    assert mocked_set_status.called


def test__init_logs_sqlite_mismatch_as_version(autosubmit, autosubmit_exp, mocker):
    """Test that an Autosubmit command ran with the wrong AS version results in an error."""
    exp = autosubmit_exp(experiment_data={
        'CONFIG': {
            'AUTOSUBMIT_VERSION': 'bright-opera'
        }
    })

    args = mocker.MagicMock()
    args.expid = exp.expid
    args.logconsole = 'DEBUG'
    args.logfile = 'DEBUG'
    args.command = 'setstatus'

    with pytest.raises(AutosubmitCritical) as cm:
        autosubmit.run_command(args)

    assert 'update the experiment version' in str(cm.value.message)

@pytest.mark.parametrize('autosubmit_missing,as_times_missing', [(True, False), (False, True), (True, True)])
def test_install_sqlite_already_exists(monkeypatch, tmp_path, autosubmit, mocker, autosubmit_missing, as_times_missing):
    """Test that a log message is displayed when autosubmit.db or as_times.db already exist when installing with SQLite."""
    monkeypatch.setattr(BasicConfig, 'DATABASE_BACKEND', 'sqlite')
    db_file = tmp_path / 'test.db'
    monkeypatch.setattr(BasicConfig, 'DB_PATH', str(db_file))
    monkeypatch.setattr(BasicConfig, 'DB_DIR', str(tmp_path))

    # Create the files if they are not supposed to be missing
    if not autosubmit_missing:
        db_file.touch()
    
    if not as_times_missing:
        as_times_file = tmp_path / BasicConfig.AS_TIMES_DB
        as_times_file.touch()

    autosubmit_db_path = Path(BasicConfig.DB_PATH)
    as_times_path = Path(BasicConfig.DB_DIR) / BasicConfig.AS_TIMES_DB

    # Mock the log to check the messages
    mocked_log = mocker.patch('autosubmit.autosubmit.Log')

    # Call
    autosubmit.install()

    # Assert
    if autosubmit_missing:
        mocked_log.info.assert_any_call("Creating autosubmit database...")
    else:        
        mocked_log.info.assert_any_call(f"Database {autosubmit_db_path} already exists.")
    
    if as_times_missing:
        mocked_log.info.assert_any_call("Creating as_times database...")
    else:
        mocked_log.info.assert_any_call(f"Database {as_times_path} already exists.")


def test_install_sqlite_create_db_fails(monkeypatch, tmp_path, autosubmit, mocker):
    monkeypatch.setattr(BasicConfig, 'DATABASE_BACKEND', 'sqlite')
    db_file = tmp_path / 'test.db'
    monkeypatch.setattr(BasicConfig, 'DB_PATH', str(db_file))
    mocker.patch('autosubmit.autosubmit.create_db', return_value=False)

    with pytest.raises(AutosubmitCritical) as cm:
        autosubmit.install()

    assert 'Can not write database file' == str(cm.value.message)


def test_install_sqlite_create_new_db(monkeypatch, tmp_path, autosubmit):
    monkeypatch.setattr(BasicConfig, 'DATABASE_BACKEND', 'sqlite')
    db_file = tmp_path / 'test.db'
    monkeypatch.setattr(BasicConfig, 'DB_PATH', str(db_file))

    autosubmit.install()

    assert db_file.exists()


def test_install_postgres_create_db_fails(monkeypatch, autosubmit, mocker):
    monkeypatch.setattr(BasicConfig, 'DATABASE_BACKEND', 'postgres')
    mocker.patch('autosubmit.autosubmit.create_db', return_value=False)

    with pytest.raises(AutosubmitCritical) as cm:
        autosubmit.install()

    assert 'Failed to create Postgres database' == str(cm.value.message)


def test_install_postgres_initializes_as_times(monkeypatch, autosubmit, mocker):
    monkeypatch.setattr(BasicConfig, 'DATABASE_BACKEND', 'postgres')
    mocker.patch('autosubmit.autosubmit.create_db', return_value=True)
    mock_status_manager = mocker.patch(
        'autosubmit.history.database_managers.experiment_status_db_manager.create_experiment_status_db_manager'
    )

    autosubmit.install()

    mock_status_manager.assert_called_once_with('postgres')


@pytest.mark.docker
@pytest.mark.postgres
def test_update_version(as_db: str, autosubmit, autosubmit_exp, mocker):
    wrong_version = 'bright-opera'
    exp = autosubmit_exp(experiment_data={
        'CONFIG': {
            'AUTOSUBMIT_VERSION': wrong_version
        }
    })

    args = mocker.MagicMock()
    args.expid = exp.expid
    args.logconsole = 'DEBUG'
    args.logfile = 'DEBUG'
    args.command = 'setstatus'

    assert autosubmit.update_version(exp.expid)

    as_conf = AutosubmitConfig(exp.expid, BasicConfig, YAMLParserFactory())
    as_conf.reload(force_load=True)

    assert as_conf.get_version() != wrong_version


@pytest.mark.docker
@pytest.mark.postgres
def test_update_description(as_db: str, autosubmit, autosubmit_exp, mocker):
    wrong_version = 'bright-opera'
    exp = autosubmit_exp(experiment_data={
        'CONFIG': {
            'AUTOSUBMIT_VERSION': wrong_version
        }
    })

    args = mocker.MagicMock()
    args.expid = exp.expid
    args.logconsole = 'DEBUG'
    args.logfile = 'DEBUG'
    args.command = 'setstatus'

    new_description = 'a new description arrived'
    assert autosubmit.update_description(exp.expid, new_description)

    assert new_description == get_experiment_description(exp.expid)[0][0]


def test_autosubmit_pklfix_no_backup(autosubmit_exp, mocker, tmp_path):
    exp = autosubmit_exp()
    mocker.patch('sys.argv', ['autosubmit', 'pklfix', exp.expid])

    mocked_log = mocker.patch('autosubmit.autosubmit.Log')

    assert 0 == main()

    assert mocked_log.info.called
    assert mocked_log.info.call_args[0][0].startswith('Backup file not found')


def test_autosubmit_pklfix_restores_backup(autosubmit_exp, mocker):
    exp = autosubmit_exp(include_jobs=True)

    pkl_path = Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR, exp.expid, 'pkl')
    current = pkl_path / f'job_list_{exp.expid}.pkl'
    backup = pkl_path / f'job_list_{exp.expid}_backup.pkl'

    copy(current, backup)

    mocker.patch('sys.argv', ['autosubmit', 'pklfix', exp.expid])

    mocked_log = mocker.patch('autosubmit.autosubmit.Log')

    mocker.patch('autosubmit.autosubmit.user_yes_no_query', return_value=True)

    assert 0 == main()

    assert mocked_log.result.called
    assert mocked_log.result.call_args[0][0].startswith('Pkl restored')


@pytest.mark.parametrize('experiment_data,context_mgr', [
    ({
         'JOBS': {
             'DQC': {
                 'FOR': {
                     'NAME': [
                         'BASIC',
                         'FULL',
                     ],
                     'WALLCLOCK': "00:40",
                 },
             },
         },
     }, pytest.raises(IndexError)),
    ({
         'JOBS': {
             'DQC': {
                 'FOR': {
                     'NAME': [
                         'BASIC',
                         'FULL',
                     ],
                 },
                 'WALLCLOCK': "00:40",
             },
         },
     }, does_not_raise()),
], ids=[
    'Missing WALLCLOCK in FOR',
    'Correct FOR',
])
def test_parse_data_loops(autosubmit_exp: 'AutosubmitExperimentFixture', experiment_data: dict, context_mgr: 'AbstractContextManager'):
    with context_mgr:
        autosubmit_exp('t000', experiment_data, create=False, include_jobs=False)


@pytest.mark.parametrize(
    '_exit,job_previous_status,expected_jobs_to_check',
    [
        (
            True,
            Status.FAILED,
            0
        ),
        (
            True,
            Status.RUNNING,
            0
        ),
        (
            False,
            Status.FAILED,
            0
        ),
        (
            False,
            Status.RUNNING,
            1
        ),
    ],
    ids=[
        "If exiting, no jobs are checked",
        "If exiting, no jobs are checked",
        "If not exiting, ignore failed jobs",
        "If not exiting, do NOT ignore running jobs"
    ]
)
def test_check_wrappers_and_as_exit(
        _exit, job_previous_status, expected_jobs_to_check, autosubmit_exp, autosubmit, mocker):
    """Test the function ``check_wrappers`` in ``Autosubmit``.

    We almost had a regression in 4.1.16, due to https://github.com/BSC-ES/autosubmit/pull/2474.

    The logic changed had a bug, and was only identified upon manual testing.

    This unit test is intended to cover only the parts related to where ``Autosubmit.exit``
    is used.

    This function ``check_wrappers`` should probably be moved to another place in the future,
    to simplify the 6K+ lines ``autosubmit.py``.
    """
    exp = autosubmit_exp(experiment_data={})
    as_conf: AutosubmitConfig = exp.as_conf

    job_list: JobList = mocker.MagicMock(spec=JobList)
    job_list.get_in_queue_grouped_id.return_value = {
        '1': [
            Job('1', '1', job_previous_status)
        ]
    }
    job_list.job_package_map = {}

    platform = mocker.MagicMock(spec=Platform)
    platform.name = 'test_platform'
    platforms_to_test: set[Platform] = {platform}

    Autosubmit.exit = _exit

    t: tuple[dict[str, list[list[Job]]], dict[str, tuple[Status, Status]]] = \
        autosubmit.check_wrappers(as_conf, job_list, platforms_to_test, exp.expid)
    jobs_to_check, _ = t

    assert len(jobs_to_check) == expected_jobs_to_check
