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
from mock import Mock, patch

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.config.yamlparser import YAMLParserFactory
from autosubmit.database.db_common import get_experiment_description
from autosubmit.job.job import Job
from autosubmit.job.job_list import JobList
from autosubmit.job.job_list_persistence import JobListPersistencePkl
from autosubmit.job.job_packages import JobPackageBase
from autosubmit.log.log import AutosubmitCritical
from autosubmit.platforms.platform import Platform
from autosubmit.scripts.autosubmit import main

if TYPE_CHECKING:
    from test.integration.conftest import AutosubmitExperimentFixture
    from contextlib import AbstractContextManager

_EXPID = 't000'


def test__init_logs_config_file_not_found(autosubmit, autosubmit_exp, mocker, monkeypatch):
    """Test that an error is raised when the ``BasicConfig.CONFIG_FILE_FOUND`` returns ``False``."""
    autosubmit_exp(_EXPID)

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
    exp = autosubmit_exp(_EXPID)

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
    exp = autosubmit_exp(_EXPID)

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
    exp = autosubmit_exp(_EXPID)

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
    autosubmit_exp(_EXPID)

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
    autosubmit_exp(_EXPID)

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
    exp = autosubmit_exp(_EXPID, experiment_data={
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
    exp = autosubmit_exp(_EXPID, experiment_data={
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
    exp = autosubmit_exp(_EXPID, experiment_data={
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
    exp = autosubmit_exp(_EXPID, experiment_data={
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
    exp = autosubmit_exp(_EXPID)
    mocker.patch('sys.argv', ['autosubmit', 'pklfix', exp.expid])

    mocked_log = mocker.patch('autosubmit.autosubmit.Log')

    assert 0 == main()

    assert mocked_log.info.called
    assert mocked_log.info.call_args[0][0].startswith('Backup file not found')


def test_autosubmit_pklfix_restores_backup(autosubmit_exp, mocker):
    exp = autosubmit_exp(_EXPID, include_jobs=True)

    pkl_path = Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR, exp.expid, 'pkl')
    current = pkl_path / f'job_list_{exp.expid}.pkl'
    backup = pkl_path / f'job_list_{exp.expid}_backup.pkl'

    copy(current, backup)

    mocker.patch('sys.argv', ['autosubmit', 'pklfix', exp.expid])

    mocked_log = mocker.patch('autosubmit.autosubmit.Log')

    mocker.patch('autosubmit.autosubmit.Autosubmit._user_yes_no_query', return_value=True)

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


def test_submit_ready_jobs(autosubmit_exp, mocker):

    exp = autosubmit_exp('a000', experiment_data={})

    platform_config = {
        "LOCAL_ROOT_DIR": exp.as_conf.basic_config.LOCAL_ROOT_DIR,
        "LOCAL_TMP_DIR": str(exp.as_conf.basic_config.LOCAL_ROOT_DIR+'exp_tmp_dir'),
        "LOCAL_ASLOG_DIR": str(exp.as_conf.basic_config.LOCAL_ROOT_DIR+'aslogs_dir')
    }
    platform = Platform('a000', "Platform", platform_config)

    job_list = JobList('a000', exp.as_conf, YAMLParserFactory(), JobListPersistencePkl())

    for i in range(3):
        job = Job(f"job{i}", i, 2, 0)
        job.section = f"SECTION{i}"
        job.platform = platform
        job_list._job_list.append(job)
    packages_to_submit = JobPackageBase(job_list.get_job_list())
    packages_to_submit.name = "test"
    packages_to_submit.x11 = "false"

    with patch("autosubmit.job.job_utils.JobPackagePersistence") as mock_persistence:
        job_persistence = mock_persistence.return_value.load.return_value = [
            ['dummy/expid', '0005_job_packages', 'dummy/expid']
        ]

    mocker.patch('autosubmit.platforms.platform.Platform.generate_submit_script', Mock())
    mocker.patch('autosubmit.job.job_packages.JobPackageBase.submit', Mock())
    save, failed_packages, error_message, valid_packages_to_submit, any_job_submitted = platform.submit_ready_jobs(
        exp.as_conf, job_list, job_persistence, [packages_to_submit])
    assert save
    assert len(failed_packages) == 0
    assert error_message == ''
    assert len(valid_packages_to_submit) == 1
    assert any_job_submitted
