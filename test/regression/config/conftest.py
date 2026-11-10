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
import pwd
from pathlib import Path

import pytest
from ruamel.yaml import YAML


@pytest.fixture
def temp_folder(tmpdir_factory):
    folder = tmpdir_factory.mktemp('tests')
    os.mkdir(folder.join('scratch'))
    file_stat = os.stat(f"{folder.strpath}")
    file_owner_id = file_stat.st_uid
    file_owner = pwd.getpwuid(file_owner_id).pw_name
    folder.owner = file_owner
    return folder


def prepare_yaml_files(yaml_file_content, temp_folder):
    # create the folder
    yaml_file_path = Path(f"{temp_folder.strpath}/test_exp_data.yml")
    # Add each platform to test
    if isinstance(yaml_file_content, dict):
        yaml = YAML()
        with yaml_file_path.open("w") as f:
            yaml.dump(yaml_file_content, f)
    else:
        with yaml_file_path.open("w") as f:
            f.write(str(yaml_file_content))
    return yaml_file_content
