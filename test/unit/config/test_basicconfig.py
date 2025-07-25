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
    config_content = """
    [config]
    log_recovery_timeout = 45
    """
    config_file = tmp_path / "autosubmitrc"
    config_file.write_text(config_content)
    assert BasicConfig.LOG_RECOVERY_TIMEOUT == 60
    os.environ = {'AUTOSUBMIT_CONFIGURATION': str(config_file)}
    BasicConfig.read()
    assert BasicConfig.LOG_RECOVERY_TIMEOUT == 45


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
