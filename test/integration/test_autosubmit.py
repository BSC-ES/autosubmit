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
import time

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


def test_install_sqlite_already_exists(monkeypatch, tmp_path, autosubmit):
    monkeypatch.setattr(BasicConfig, 'DATABASE_BACKEND', 'sqlite')
    db_file = tmp_path / 'test.db'
    db_file.touch()
    monkeypatch.setattr(BasicConfig, 'DB_PATH', str(db_file))

    with pytest.raises(AutosubmitCritical) as cm:
        autosubmit.install()

    assert 'Database already exists.' == str(cm.value.message)


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
def test_parse_data_loops(autosubmit_exp: 'AutosubmitExperimentFixture', experiment_data: dict,
                          context_mgr: 'AbstractContextManager'):
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

    job = Job('1', '1', job_previous_status)
    job_list: JobList = mocker.MagicMock(spec=JobList)
    job_list.get_job_list.return_value = [job]
    job_list.job_package_map = {}

    platform = mocker.MagicMock(spec=Platform)
    platform.name = 'test_platform'

    Autosubmit.exit = _exit

    autosubmit.check_wrappers(as_conf, job_list, exp.expid)


def test_create_txt_output_writes_status_file(autosubmit_exp):
    """Test that -o txt and -d both write a txt file to the status folder."""
    exp = autosubmit_exp(include_jobs=True)

    # status/ should be empty before test
    assert not list(exp.status_dir.glob('*.txt')), "status/ should be empty before test"

    # test -o txt creates a file in status/
    exp.autosubmit.create(exp.expid, noplot=False, hide=True, output='txt', force=True)
    txt_files_after_txt = list(exp.status_dir.glob('*.txt'))
    assert len(txt_files_after_txt) == 1, "Expected exactly one txt file in status/ for -o txt"

    # wait to ensure a different timestamp for the second file
    time.sleep(1)

    # test -d creates another file in status/
    exp.autosubmit.create(exp.expid, noplot=True, hide=True, output=None, detail=True, force=True)
    txt_files_after_detail = list(exp.status_dir.glob('*.txt'))
    assert len(txt_files_after_detail) == 2, "Expected a second txt file in status/ for -d"


def test_prepare_run_returns_tuple(autosubmit_exp):
    """prepare_run: returns the expected 7-tuple with recover=False."""
    exp = autosubmit_exp(include_jobs=True)
    result = Autosubmit.prepare_run(exp.expid, check_scripts=False)
    assert len(result) == 7
    job_list, submitter, exp_history, host, as_conf, platforms_to_test, recover = result
    assert job_list is not None
    assert submitter is not None
    assert exp_history is not None
    assert recover is False


def test_prepare_run_returns_tuple_with_recover(autosubmit_exp):
    """prepare_run: returns the expected 7-tuple with recover=True."""
    exp = autosubmit_exp(include_jobs=True)
    result = Autosubmit.prepare_run(exp.expid, check_scripts=False, recover=True)
    assert len(result) == 7
    job_list, submitter, exp_history, host, as_conf, platforms_to_test, recover = result
    assert job_list is not None
    assert submitter is not None
    assert exp_history is None
    assert recover is True


def test_stop_sets_exit_flag(autosubmit_exp, mocker):
    """stop: sets Autosubmit.exit to True for the given experiment."""
    exp = autosubmit_exp()
    mocker.patch('builtins.input', return_value='y')
    mocker.patch('autosubmit.helpers.processes.process_id', return_value=0)
    original = Autosubmit.exit
    try:
        Autosubmit.exit = False
        result = Autosubmit.stop(exp.expid, force_yes=True)
        assert result is True
    finally:
        Autosubmit.exit = original


def test_monitor_with_check_wrapper(autosubmit_exp):
    """monitor: check_wrapper=True loads wrapper packages."""
    exp = autosubmit_exp(include_jobs=True)
    # Create a minimal job_list pickle so load_job_list works
    as_conf = AutosubmitConfig(exp.expid, BasicConfig, YAMLParserFactory())
    as_conf.check_conf_files(True)
    job_list = Autosubmit.load_job_list(exp.expid, as_conf, monitor=True, new=False)
    job_list.save()

    result = Autosubmit.monitor(exp.expid, file_format='pdf', lst='', filter_chunks='',
                                filter_status='', filter_section='', hide=True,
                                check_wrapper=True)
    assert result is True


def test_set_status_with_detail(autosubmit_exp):
    """set_status: detail=True prints job list after status change."""
    exp = autosubmit_exp(include_jobs=True)
    # Create a job list with a job
    as_conf = AutosubmitConfig(exp.expid, BasicConfig, YAMLParserFactory())
    as_conf.check_conf_files(True)
    job_list = Autosubmit.load_job_list(exp.expid, as_conf, monitor=True, new=False)
    job_name = job_list.get_job_list()[0].name
    job_list.save()

    result = Autosubmit.set_status(
        exp.expid, noplot=True, save=True, final='WAITING',
        filter_list=job_name, filter_chunks='', filter_status='',
        filter_section='', filter_type_chunk='', filter_type_chunk_split='',
        hide=True, detail=True
    )
    assert result is True
