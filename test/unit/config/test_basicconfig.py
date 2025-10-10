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

import os
from pathlib import Path

import pytest

from autosubmit.config.basicconfig import BasicConfig


def test_read_file_config(tmp_path):
    config_content = f"""
    [database]
    path = {tmp_path}
    backend = True
    connection_url = 127.0.0.1
    filename = test.db
    [local]
    path = {tmp_path}
    [conf]
    platforms = local
    custom_platforms = test_platform
    jobs = 0
    [mail]
    smtp_server = 127.0.0.1
    mail_from = test@bsc.es
    [hosts]
    authorized = [[[1, 3]], 4]
    forbidden = [[1, 3], 4]
    [structures]
    path = {tmp_path}
    [globallogs]
    path = {tmp_path}
    [defaultstats]
    path = {tmp_path}
    [historicdb]
    path = {tmp_path}
    [historiclog]
    path = {tmp_path}
    [autosubmitapi]
    path = {tmp_path}
    [config]
    log_recovery_timeout = 45
    """
    config_file = tmp_path / "autosubmitrc"
    config_file.write_text(config_content)
    assert BasicConfig.LOG_RECOVERY_TIMEOUT == 60
    os.environ = {'AUTOSUBMIT_CONFIGURATION': str(config_file)}
    BasicConfig.read()
    assert BasicConfig.ALLOWED_HOSTS == {'': ['3]]', ''], '1': ['3]]', '']}
    assert BasicConfig.AS_TIMES_DB == 'as_times.db'
    assert BasicConfig.AUTOSUBMIT_API_URL.startswith('http://192.168.11.91:8081')
    assert BasicConfig.CONFIG_FILE_FOUND
    assert BasicConfig.CUSTOM_PLATFORMS_PATH == 'test_platform'
    assert BasicConfig.DATABASE_BACKEND == 'True'
    assert BasicConfig.DATABASE_CONN_URL == '127.0.0.1'
    assert BasicConfig.DB_DIR == f'{tmp_path}'
    assert BasicConfig.DB_FILE == 'test.db'
    assert BasicConfig.DB_PATH == f'{tmp_path}/test.db'
    assert BasicConfig.DEFAULT_JOBS_CONF == '0'
    assert BasicConfig.DEFAULT_OUTPUT_DIR == f'{tmp_path}'
    assert BasicConfig.DEFAULT_PLATFORMS_CONF == 'local'
    assert BasicConfig.DENIED_HOSTS == {'': ['3]', ''], '1': ['3]', '']}
    assert BasicConfig.GLOBAL_LOG_DIR == f'{tmp_path}'
    assert BasicConfig.HISTORICAL_LOG_DIR == f'{tmp_path}'
    assert BasicConfig.JOBDATA_DIR == f'{tmp_path}'
    assert BasicConfig.LOCAL_ASLOG_DIR == 'ASLOGS'
    assert BasicConfig.LOCAL_PROJ_DIR == 'proj'
    assert BasicConfig.LOCAL_ROOT_DIR == f'{tmp_path}'
    assert BasicConfig.LOCAL_TMP_DIR == 'tmp'
    assert BasicConfig.LOG_RECOVERY_TIMEOUT == 45
    assert BasicConfig.MAIL_FROM == 'test@bsc.es'
    assert BasicConfig.SMTP_SERVER == '127.0.0.1'
    assert BasicConfig.STRUCTURES_DIR == f'{tmp_path}'


def test_invalid_expid_path():
    invalid_expids = ["", "12345", "123/", 1234]  # empty, more than 4 char, contains folder separator, not string

    with pytest.raises(Exception):
        for expid in invalid_expids:
            BasicConfig.expid_dir(expid)


functions_expid = [BasicConfig.expid_dir,
                   BasicConfig.expid_tmp_dir,
                   BasicConfig.expid_log_dir,
                   BasicConfig.expid_aslog_dir]
root_dirs = [
    lambda root_path, exp_id: Path(root_path, exp_id),
    lambda root_path, exp_id: Path(root_path, exp_id, "tmp"),
    lambda root_path, exp_id: Path(root_path, exp_id, "tmp", f"LOG_{exp_id}"),
    lambda root_path, exp_id: Path(root_path, exp_id, "tmp", "ASLOGS")
]


@pytest.mark.parametrize("foo, dir_func", zip(functions_expid, root_dirs))
def test_expid_dir_structure(foo, dir_func, autosubmit_config):
    exp_id = 'a000'
    root_path = autosubmit_config(expid=exp_id, experiment_data={}).basic_config.LOCAL_ROOT_DIR
    expected_path = dir_func(root_path, exp_id)
    result = foo(exp_id)
    assert result == expected_path
