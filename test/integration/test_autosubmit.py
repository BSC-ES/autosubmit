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

from os import R_OK, W_OK
from pathlib import Path
from shutil import copy

import pytest
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory

from autosubmit.database.db_common import get_experiment_description
from autosubmit.scripts.autosubmit import main
from log.log import AutosubmitCritical

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
    mocker.patch('autosubmitconfigparser.config.configcommon.AutosubmitConfig.reload')

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


@pytest.mark.parametrize(
    "db_engine",
    [
        # postgres
        pytest.param("postgres", marks=[pytest.mark.postgres]),
        # sqlite
        pytest.param("sqlite")
    ],
)
def test_update_version(db_engine: str, autosubmit, autosubmit_exp, mocker, request):
    request.getfixturevalue(f"as_db_{db_engine}")
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


@pytest.mark.parametrize(
    "db_engine",
    [
        # postgres
        pytest.param("postgres", marks=[pytest.mark.postgres]),
        # sqlite
        pytest.param("sqlite")
    ],
)
def test_update_description(db_engine: str, autosubmit, autosubmit_exp, mocker, request):
    request.getfixturevalue(f"as_db_{db_engine}")
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
    exp = autosubmit_exp(_EXPID)

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
