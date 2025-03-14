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

""" Test file for autosubmit/autosubmit.py """
from typing import Callable

import pytest
from mock.mock import MagicMock, patch

from autosubmit.autosubmit import Autosubmit


@pytest.mark.parametrize("fake_dir, real_dir", [
    pytest.param("a000", "a000", marks=pytest.mark.xfail(reason="Meant to fail since it can't "
                                "create a folder if one already exists")), # test meant to FAIL
    ("","a000"), # test meant to PASS
    ("",""), # test meant to PASS with a generated expid
    ], ids=('FAIL','PASS','PASS with a generated expid'))
def test_expid(autosubmit_config: Callable, tmp_path, fake_dir, real_dir)\
        -> None:
    """
    Function to test if the autosubmit().expid generates the paths and expid properly

    ::fake_dir -> if fake dir exists test will fail since it won't be able to generate folder
    ::real_dir -> folder it'll try to create and experiment id
    """
    with patch('autosubmit.autosubmit.BasicConfig', MagicMock()) as fake_basic_config:
        # act
        fake_basic_config.STRUCTURES_DIR = fake_basic_config.LOCAL_ROOT_DIR = str(tmp_path)
        fake_basic_config.JOBDATA_DIR = str(tmp_path)
        fake_basic_config.read()

        if fake_dir != "":
            path = tmp_path / fake_dir
            path.mkdir()

        if real_dir != "":
            autosubmit_config(real_dir, {})

        expid = Autosubmit.expid("Test", real_dir)

        if real_dir == "":
            autosubmit_config(expid, {})

        experiment = Autosubmit.describe(expid)

        path = tmp_path / expid

        assert path.exists()
        assert experiment is not None
        assert isinstance(expid, str) and len(expid) == 4
