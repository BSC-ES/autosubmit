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

"""Tests for ``AutosubmitGit``."""

from pathlib import Path
from textwrap import dedent

import pytest

from autosubmit.autosubmit import Autosubmit
from autosubmit.config.basicconfig import BasicConfig
from autosubmit.log.log import AutosubmitCritical
from test.unit.conftest import AutosubmitConfigFactory


def test_copy_as_config(autosubmit_config: AutosubmitConfigFactory):
    """function to test copy_as_config from autosubmit.py

    :param autosubmit_config:
    :type autosubmit_config: AutosubmitConfigFactory
    """
    autosubmit_config('a000', {})
    BasicConfig.LOCAL_ROOT_DIR = f"{BasicConfig.LOCAL_ROOT_DIR}"

    ini_file = Path(f'{BasicConfig.LOCAL_ROOT_DIR}/a000/conf')
    new_file = Path(f'{BasicConfig.LOCAL_ROOT_DIR}/a001/conf')
    ini_file.mkdir(parents=True, exist_ok=True)
    new_file.mkdir(parents=True, exist_ok=True)
    ini_file = ini_file / 'jobs_a000.conf'
    new_file = new_file / 'jobs_a001.yml'

    with open(ini_file, 'w+', encoding="utf-8") as file:
        file.write(dedent('''\
                [LOCAL_SETUP]
                FILE = LOCAL_SETUP.sh
                PLATFORM = LOCAL
                '''))
        file.flush()

    Autosubmit.copy_as_config('a001', 'a000')

    new_yaml_file = Path(new_file.parent, new_file.stem).with_suffix('.yml')

    assert new_yaml_file.exists()
    assert new_yaml_file.stat().st_size > 0

    new_yaml_file = Path(new_file.parent, new_file.stem).with_suffix('.conf_AS_v3_backup')

    assert new_yaml_file.exists()
    assert new_yaml_file.stat().st_size > 0


def test_pkl_fix_postgres(monkeypatch, autosubmit):
    """Test that trying to fix the pkl when using Postgres results in an error."""
    monkeypatch.setattr(BasicConfig, 'DATABASE_BACKEND', 'postgres')

    with pytest.raises(AutosubmitCritical):
        autosubmit.pkl_fix('a000')


def test_database_backup_postgres(monkeypatch, autosubmit, mocker):
    """Test that trying to back up a Postgres DB results in just a log message of WIP."""
    monkeypatch.setattr(BasicConfig, 'DATABASE_BACKEND', 'postgres')
    mocked_log = mocker.patch('autosubmit.autosubmit.Log')
    autosubmit.database_backup('a000')
    assert mocked_log.debug.called
