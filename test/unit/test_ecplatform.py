#!/usr/bin/env python3

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
from pathlib import Path

import pytest
from _pytest._py.path import LocalPath

from autosubmit.log.log import AutosubmitError
from autosubmit.platforms.ecplatform import EcPlatform

@pytest.fixture
def ec_platform(tmp_path: 'LocalPath'):
    config = {"LOCAL_ROOT_DIR": str(tmp_path), "LOCAL_TMP_DIR": "tmp"}
    from autosubmit.platforms.ecplatform import EcPlatform
    yield EcPlatform(expid='t000', name='pytest-slurm', config=config, scheduler='slurm')


def test_file_read_size_and_send(ec_platform: EcPlatform, mocker):
    path = ec_platform.config.get("LOCAL_ROOT_DIR")
    assert isinstance(path, str)
    random_file = Path(path) / "random_file"
    assert isinstance(random_file, Path)
    with open(random_file, "w") as f:
        f.write("a" * 100)

    with pytest.raises(AutosubmitError):
        assert ec_platform.send_file(str(random_file))

    mocked_check_call = mocker.patch('autosubmit.platforms.ecplatform.subprocess')
    mocked_check_call.check_call.return_value = True
    assert ec_platform.send_file(str(random_file))
