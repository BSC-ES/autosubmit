# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
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

"""Tests for ``upgrade_scripts.py`` module."""

import locale
import shutil
from pathlib import Path

import pytest
from typing import TYPE_CHECKING
from autosubmit.config.utils import copy_as_config
from autosubmit.config.upgrade_scripts import upgrade_scripts, ini_to_yaml
from textwrap import dedent
from ruamel.yaml import YAML

if TYPE_CHECKING:
    from py._path.local import LocalPath  # type: ignore


def test_jobs_ini_to_yaml(tmp_path: 'LocalPath'):
    ini_file = tmp_path / 'jobs_monarch.ini'
    ini_file.touch()
    ini_file.write_text(dedent('''\
    [LOCAL_SETUP]
    FILE = templates/local_setup.sh
    PLATFORM = LOCAL
    RUNNING = once
    NOTIFY_ON = FAILED COMPLETED
    '''))

    yaml_file = ini_to_yaml(tmp_path, ini_file)
    assert yaml_file

    yaml_dict = YAML().load(yaml_file)

    assert yaml_dict['JOBS']['LOCAL_SETUP']['FILE'] == 'templates/local_setup.sh'


def test_platforms_ini_to_yaml(tmp_path: 'LocalPath'):
    ini_file = tmp_path / 'platforms.ini'
    ini_file.touch()
    ini_file.write_text(dedent('''\
    [marenostrum4]
    TYPE = slurm
    HOST = mn1.bsc.es
    PROJECT = bsc32
    USER = bsc32xxx
    SCRATCH_DIR = /gpfs/scratch
    ADD_PROJECT_TO_HOST = False
    TEST_SUITE = False
    MAX_WALLCLOCK = 48:00
    MAX_PROCESSORS = 2400
    PROCESSORS_PER_NODE = 48
    '''))

    yaml_file = ini_to_yaml(tmp_path, ini_file)
    assert yaml_file

    yaml_dict = YAML().load(yaml_file)

    assert yaml_dict['PLATFORMS']['marenostrum4']['TYPE'] == 'slurm'


def test_ini_to_yaml(tmp_path: 'LocalPath'):
    ini_file = tmp_path / 'any_config.ini'
    ini_file.touch()
    ini_file.write_text(dedent('''\
    [marenostrum4]
    TYPE = slurm
    HOST = mn1.bsc.es
    PROJECT = bsc32
    USER = bsc32xxx
    SCRATCH_DIR = /gpfs/scratch
    ADD_PROJECT_TO_HOST = False
    TEST_SUITE = False
    MAX_WALLCLOCK = 48:00
    MAX_PROCESSORS = 2400
    PROCESSORS_PER_NODE = 48
    '''))

    yaml_file = ini_to_yaml(tmp_path, ini_file)
    assert yaml_file

    yaml_dict = YAML().load(yaml_file)

    assert 'PLATFORMS' not in yaml_dict
    assert yaml_dict['marenostrum4']['TYPE'] == 'slurm'


def test_ini_to_yaml_backup_exists(tmp_path: 'LocalPath'):
    backup_file = tmp_path / 'backup.ini_AS_v3_backup'
    backup_file.touch()
    backup_file.write_text('autosubmit')
    backup_file_size = backup_file.stat().st_size

    ini_file = tmp_path / 'any_config.ini'
    ini_file.touch()
    ini_file.write_text(dedent('''\
    [marenostrum4]
    TYPE = slurm
    '''))

    _ = ini_to_yaml(tmp_path, ini_file)
    assert backup_file_size == backup_file.stat().st_size, 'ini_to_yaml replaced the existing backup file'


def test_ini_to_yaml_lists(tmp_path: 'LocalPath'):
    ini_file = tmp_path / 'any_config.ini'
    ini_file.touch()
    ini_file.write_text(dedent('''\
    [marenostrum4]
    HOST = [mn1.bsc.es, mn2.bsc.s]
    '''))

    yaml_file = ini_to_yaml(tmp_path, ini_file)
    yaml = YAML().load(yaml_file)
    assert yaml['marenostrum4']['HOST'] == '"[mn1.bsc.es, mn2.bsc.s]"'


def test_upgrade_scripts(autosubmit_exp):
    as_exp = autosubmit_exp(experiment_data={})

    exp_dir = Path(as_exp.as_conf.basic_config.LOCAL_ROOT_DIR, as_exp.expid)

    as3_jobs_file = exp_dir / f'jobs_{as_exp.expid}.conf'
    as3_jobs_file.touch()
    as3_jobs_file.write_text(dedent('''\
    [LOCAL_SETUP]
    FILE = templates/local_setup.sh
    PLATFORM = marenostrum4
    RUNNING = once
    NOTIFY_ON = FAILED COMPLETED
    '''))

    as3_platforms_file = exp_dir / 'platforms.conf'
    as3_platforms_file.touch()
    as3_platforms_file.write_text(dedent('''\
    [marenostrum4]
    TYPE = slurm
    HOST = mn1.bsc.es
    PROJECT = bsc32
    USER = bsc32xxx
    SCRATCH_DIR = /gpfs/scratch
    ADD_PROJECT_TO_HOST = False
    TEST_SUITE = False
    MAX_WALLCLOCK = 48:00
    MAX_PROCESSORS = 2400
    PROCESSORS_PER_NODE = 48
    '''))

    assert upgrade_scripts(as_exp.expid, files="*.conf")
