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

from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING

from configobj import ConfigObj
from ruamel.yaml import YAML

from autosubmit.config.upgrade_scripts import _config_obj_to_nested_dict
from autosubmit.config.upgrade_scripts import upgrade_scripts, ini_to_yaml

if TYPE_CHECKING:
    # noinspection PyProtectedMember
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

    yaml_file = ini_to_yaml(ini_file)
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

    yaml_file = ini_to_yaml(ini_file)
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

    yaml_file = ini_to_yaml(ini_file)
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

    _ = ini_to_yaml(ini_file)
    assert backup_file_size == backup_file.stat().st_size, 'ini_to_yaml replaced the existing backup file'


def test_ini_to_yaml_lists(tmp_path: 'LocalPath'):
    """Test that lists are converted from '= [a, b]' to '= "a, b"'."""
    ini_file = tmp_path / 'any_config.ini'
    ini_file.touch()
    ini_file.write_text(dedent('''\
    [marenostrum4]
    HOST = [mn1.bsc.es, mn2.bsc.s]
    '''))

    yaml_file = ini_to_yaml(ini_file)
    yaml = YAML().load(yaml_file)
    assert yaml['marenostrum4']['HOST'] == '"mn1.bsc.es, mn2.bsc.s"'


def test_upgrade_scripts(autosubmit_exp, tmp_path: 'LocalPath'):
    temp_project = tmp_path / 'temp_project'
    as_exp = autosubmit_exp(experiment_data={
        'PROJECT': {
            'PROJECT_TYPE': 'LOCAL',
            'PROJECT_DESTINATION': 'local_project'
        },
        'LOCAL': {
            'PROJECT_PATH': str(temp_project)
        }
    }, create=False)

    exp_dir = Path(as_exp.as_conf.basic_config.LOCAL_ROOT_DIR, as_exp.expid)

    as3_jobs_file = exp_dir / f'conf/jobs_{as_exp.expid}.conf'
    as3_jobs_file.parent.mkdir(exist_ok=True)
    as3_jobs_file.touch()
    as3_jobs_file.write_text(dedent('''\
    [LOCAL_SETUP]
    FILE = templates/local_setup.sh
    PLATFORM = marenostrum4-test
    RUNNING = once
    NOTIFY_ON = FAILED COMPLETED
    '''))

    as3_platforms_file = exp_dir / 'conf/platforms.conf'
    as3_platforms_file.touch()
    as3_platforms_file.write_text(dedent('''\
    [marenostrum4-test]
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

    as3_script_template = temp_project / 'templates/local_setup.sh'
    as3_script_template.parent.mkdir(exist_ok=True, parents=True)
    as3_script_template.touch()
    as3_script_template.write_text(dedent('''\
    #!/bin/bash
    
    echo "The job name is %JOBNAME%"
    '''))

    assert as_exp.autosubmit.create(as_exp.expid, force=True, noplot=True, hide=True) == 0

    assert upgrade_scripts(as_exp.expid, files=[])

    assert as_exp.autosubmit.create(as_exp.expid, force=True, noplot=True, hide=True) == 0
    assert as_exp.autosubmit.inspect(as_exp.expid, lst='', filter_chunks='', filter_status='', filter_section='')

    inspect_generated_script = exp_dir / f'tmp/{as_exp.expid}_LOCAL_SETUP.cmd'
    assert inspect_generated_script.exists(), f"inspect executed but {inspect_generated_script} does not exist"

    assert f'The job name is {as_exp.expid}_LOCAL_SETUP' in inspect_generated_script.read_text()


def test_single_level_keys():
    """Test if the function correctly processes single-level keys."""
    config_obj = ConfigObj({"key1": "value1", "key2": "value2"})
    result = _config_obj_to_nested_dict(config_obj)
    expected = {"key1": "value1", "key2": "value2"}
    assert expected == result


def test_multi_level_keys():
    """Test if the function correctly processes multi-level keys."""
    config_obj = ConfigObj({"a.b.c": "value1", "a.b.d": "value2"})
    result = _config_obj_to_nested_dict(config_obj)
    expected = {"a": {"b": {"c": "value1", "d": "value2"}}}
    assert expected == result


def test_mix_of_single_and_multi_level_keys():
    """Test if the function handles a mix of single and multi-level keys."""
    config_obj = ConfigObj({"x": "value1", "a.b.c": "value2", "a.b.d": "value3"})
    result = _config_obj_to_nested_dict(config_obj)
    expected = {"x": "value1", "a": {"b": {"c": "value2", "d": "value3"}}}
    assert expected == result


def test_nested_dictionaries():
    """Test if the function processes nested dictionaries as values."""
    config_obj = ConfigObj({
        "top.level": {
            "nested": "value1",
            "another": "value2"
        }
    })
    result = _config_obj_to_nested_dict(config_obj)
    expected = {"top": {"level": {"nested": "value1", "another": "value2"}}}
    assert expected == result


def test_empty_config_obj():
    """Test if the function handles an empty ConfigObj."""
    config_obj = ConfigObj({})
    result = _config_obj_to_nested_dict(config_obj)
    expected = {}
    assert expected == result


def test_overwrite_existing_key():
    """Test if the function overwrites values correctly in case of conflict."""
    config_obj = ConfigObj({"a.b": {"c": "old_value"}, "a.b.c": "new_value"})
    result = _config_obj_to_nested_dict(config_obj)
    expected = {"a": {"b": {"c": "old_value"}}}  # old value stays
    assert result == expected
