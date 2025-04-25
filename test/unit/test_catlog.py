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

import io
import sys
from contextlib import redirect_stdout
from pathlib import Path

import pytest

from autosubmit.autosubmit import AutosubmitCritical

_EXPID = 'a000'


# NOTE: this fixture is marked to be auto-used, so that all the tests here
#       use the mocked configuration directories.
@pytest.fixture(autouse=True)
def as_conf(autosubmit_config):
    return autosubmit_config(_EXPID, {})


@pytest.fixture
def exp_path(as_conf) -> Path:
    return Path(as_conf.basic_config.LOCAL_ROOT_DIR) / _EXPID


@pytest.fixture
def exp_logs_dir(exp_path, as_conf):
    exp_tmp_dir = exp_path / as_conf.basic_config.LOCAL_TMP_DIR
    exp_logs_dir = exp_tmp_dir / f'LOG_{_EXPID}'
    return exp_logs_dir


@pytest.fixture
def aslogs_dir(exp_path, as_conf):
    exp_tmp_dir = exp_path / as_conf.basic_config.LOCAL_TMP_DIR
    aslogs_dir = exp_tmp_dir / as_conf.basic_config.LOCAL_ASLOG_DIR
    return aslogs_dir


@pytest.fixture
def status_path(exp_path, as_conf):
    status_path = exp_path / 'status'
    status_path.mkdir(exist_ok=True)
    return status_path


def test_invalid_file(autosubmit):
    def _fn():
        autosubmit.cat_log(None, '8', None)  # type: ignore

    pytest.raises(AutosubmitCritical, _fn)


def test_invalid_mode(autosubmit):
    def _fn():
        autosubmit.cat_log(None, 'o', '8')  # type: ignore

    pytest.raises(AutosubmitCritical, _fn)


# -- workflow


def test_is_workflow_invalid_file(autosubmit):
    def _fn():
        autosubmit.cat_log(_EXPID, 'j', None)

    pytest.raises(AutosubmitCritical, _fn)


def test_is_workflow_not_found(mocker, autosubmit):
    mocked_log = mocker.patch('autosubmit.autosubmit.Log')
    autosubmit.cat_log(_EXPID, 'o', 'c')
    assert mocked_log.info.called
    assert mocked_log.info.call_args[0][0] == 'No logs found.'


def test_is_workflow_log_is_dir(autosubmit, aslogs_dir):
    log_file_actually_dir = aslogs_dir / 'log_run.log'
    log_file_actually_dir.mkdir()

    def _fn():
        autosubmit.cat_log(_EXPID, 'o', 'c')

    pytest.raises(AutosubmitCritical, _fn)


def test_is_workflow_out_cat(mocker, autosubmit, aslogs_dir):
    popen = mocker.patch('subprocess.Popen')
    log_file = Path(aslogs_dir, 'log_run.log')
    if log_file.is_dir():  # dir is created in previous test
        log_file.rmdir()
    with open(log_file, 'w') as f:
        f.write('as test')
        f.flush()
        autosubmit.cat_log(_EXPID, file=None, mode='c')
        assert popen.called
        args = popen.call_args[0][0]
        assert args[0] == 'cat'
        assert args[1] == str(log_file)


def test_is_workflow_status_tail(mocker, autosubmit, status_path):
    popen = mocker.patch('subprocess.Popen')
    log_file = status_path / f'{_EXPID}_anything.txt'
    with open(log_file, 'w') as f:
        f.write('as test')
        f.flush()
        autosubmit.cat_log(_EXPID, file='s', mode='t')
        assert popen.called
        args = popen.call_args[0][0]
        assert args[0] == 'tail'
        assert str(args[-1]) == str(log_file)


# --- jobs


def test_is_jobs_not_found(mocker, autosubmit):
    mocked_log = mocker.patch('autosubmit.autosubmit.Log')
    for file in ['j', 's', 'o']:
        autosubmit.cat_log(f'{_EXPID}_INI', file=file, mode='c')
        assert mocked_log.info.called
        assert mocked_log.info.call_args[0][0] == 'No logs found.'


def test_is_jobs_log_is_dir(autosubmit, exp_logs_dir):
    log_file_actually_dir = exp_logs_dir / f'{_EXPID}_INI.20000101.out'
    log_file_actually_dir.mkdir()

    def _fn():
        autosubmit.cat_log(f'{_EXPID}_INI', 'o', 'c')

    pytest.raises(AutosubmitCritical, _fn)


def test_is_jobs_out_tail(mocker, autosubmit, exp_logs_dir):
    popen = mocker.patch('subprocess.Popen')
    log_file = Path(exp_logs_dir, f'{_EXPID}_INI.20200101.out')
    if log_file.is_dir():  # dir is created in previous test
        log_file.rmdir()
    with open(log_file, 'w') as f:
        f.write('as test')
        f.flush()
        autosubmit.cat_log(f'{_EXPID}_INI', file=None, mode='t')
        assert popen.called
        args = popen.call_args[0][0]
        assert args[0] == 'tail'
        assert str(args[-1]) == str(log_file)

        # --- command-line


def test_command_line_help(mocker, autosubmit):
    args = ['autosubmit', 'cat-log', '--help']
    mocker.patch.object(sys, 'argv', args)
    with io.StringIO() as buf, redirect_stdout(buf):
        assert autosubmit.parse_args()
        assert buf
        assert 'View workflow and job logs.' in buf.getvalue()  # type: ignore
