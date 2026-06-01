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
import shutil
from pathlib import Path
from textwrap import dedent
from typing import Any, TYPE_CHECKING

import pytest
from ruamel.yaml import YAML

from autosubmit.config.upgrade_scripts import upgrade_scripts, ini_to_yaml

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from py._path.local import LocalPath  # type: ignore


@pytest.fixture
def as3_ini_files(tmp_path: 'LocalPath'):
    """Create AS3 ini files."""
    jobs_ini = tmp_path / 'jobs_monarch.conf'
    jobs_ini.write_text(dedent('''\
    
        [LOCAL_SETUP]
        FILE = templates/local_setup.sh
        PLATFORM = marenostrum4-test
        RUNNING = once
        NOTIFY_ON = FAILED COMPLETED
        '''))

    platforms_ini = tmp_path / 'platform_monarch.conf'
    platforms_ini.write_text(dedent('''\
        [marenostrum4-test]
        TYPE = slurm
        HOST = mn1.bsc.es
        PROJECT = bsc32
        USER = %user%
        SCRATCH_DIR = /gpfs/scratch
        ADD_PROJECT_TO_HOST = False
        TEST_SUITE = False
        MAX_WALLCLOCK = 48:00
        MAX_PROCESSORS = 2400
        PROCESSORS_PER_NODE = 48
        '''))

    autosubmit_ini = tmp_path / 'autosubmit.conf'
    autosubmit_ini.write_text(dedent('''\
        [config]
        EXPID = a000
        AUTOSUBMIT_VERSION = v3.15.0
        # a comment
        MAXWAITINGJOBS = 3
        TOTALJOBS = 6
        HPCARCH = CESGA
        '''))

    return [jobs_ini, platforms_ini, autosubmit_ini]


def _get_yaml_dict(tmp_path: 'LocalPath'):
    yaml_dict = {}
    y = YAML()
    for f in tmp_path.glob('*.yml'):
        yaml_dict.update(y.load(f))
    return yaml_dict


def _convert_all(as3_ini_files: list[Path]):
    for ini_file in as3_ini_files:
        ini_to_yaml(ini_file)


def test_jobs_ini_to_yaml(tmp_path: 'LocalPath', as3_ini_files: list[Path]):
    _convert_all(as3_ini_files)
    yaml_dict = _get_yaml_dict(tmp_path)
    assert yaml_dict['JOBS']['LOCAL_SETUP']['FILE'] == 'templates/local_setup.sh'


def test_platforms_ini_to_yaml(tmp_path: 'LocalPath', as3_ini_files: list[Path]):
    _convert_all(as3_ini_files)
    yaml_dict = _get_yaml_dict(tmp_path)
    assert yaml_dict['PLATFORMS']['marenostrum4-test']['TYPE'] == 'slurm'


def test_ini_to_yaml_already_exists(tmp_path: 'LocalPath'):
    """Test that the conversion is not executed if the file already exists."""
    ini_file = tmp_path / 'any_config.conf'
    yaml_file = Path(tmp_path / 'any_config.yml')
    with yaml_file.open('w') as f:
        f.write('test')

    ini_to_yaml(ini_file)
    assert yaml_file.read_text() == 'test'


def test_ini_to_yaml(tmp_path: 'LocalPath'):
    """Test that the Autosubmit 3 configuration model is followed.

    In AS3 platforms are only loaded from a ``platform_*.conf`` file.
    """
    ini_file = tmp_path / 'any_config.conf'
    ini_file.touch()
    ini_file.write_text(dedent('''\
    [marenostrum4]
    TYPE = slurm
    HOST = mn1.bsc.es
    PROJECT = bsc32
    USER = %user%
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


def test_ini_to_yaml_backup_exists(tmp_path: 'LocalPath', mocker):
    """Test that a backup file is not overwritten if it already exists."""
    backup_file = tmp_path / 'any_config.conf_as_v3_backup'
    backup_file.write_text('autosubmit')
    backup_file_size = backup_file.stat().st_size

    ini_file = tmp_path / 'any_config.conf'
    ini_file.write_text(dedent('''\
    [marenostrum4]
    TYPE = slurm
    '''))

    mocked_log = mocker.patch('autosubmit.config.upgrade_scripts.Log')
    _ = ini_to_yaml(ini_file)
    assert backup_file_size == backup_file.stat().st_size, 'ini_to_yaml replaced the existing backup file'

    assert mocked_log.info.called
    assert 'Backup already exists at' in mocked_log.info.call_args[0][0]


def test_ini_to_yaml_lists(tmp_path: 'LocalPath'):
    """Test that lists are converted from '= [a, b]' to '= "a, b"'."""
    ini_file = tmp_path / 'any_config.conf'
    ini_file.touch()
    ini_file.write_text(dedent('''\
    [marenostrum4]
    HOST = [mn1.bsc.es, mn2.bsc.es]
    '''))

    yaml_file = ini_to_yaml(ini_file)
    yaml = YAML().load(yaml_file)
    assert yaml['marenostrum4']['HOST'] == '"mn1.bsc.es, mn2.bsc.es"'


def test_upgrade_scripts_as_3_conf_causes_error(
        autosubmit_exp,
        tmp_path: 'LocalPath',
        as3_ini_files: list[Path],
        mocker
):
    """Test that the upgrade script fails if the AS3 configuration file upgrade fails."""
    as_exp = autosubmit_exp(experiment_data={}, create=False)

    exp_dir = Path(as_exp.as_conf.basic_config.LOCAL_ROOT_DIR, as_exp.expid)
    # Copy fixture INI files into the experiment path
    for ini_file in as3_ini_files:
        shutil.copy(ini_file, exp_dir / 'conf')

    mocker.patch('autosubmit.config.upgrade_scripts.ini_to_yaml', side_effect=ValueError('test'))
    mocked_log = mocker.patch('autosubmit.config.upgrade_scripts.Log')
    upgrade_scripts(as_exp.expid, files=[])
    assert mocked_log.warning.called
    assert 'Failed to upgrade AS3 conf file' in mocked_log.warning.call_args[0][0]


@pytest.mark.parametrize(
    'side_effect_index,expected_error',
    [
        (0, 'Failed to fix placeholders in the new AS4 YAML file'),
        (3, 'Failed to fix placeholders in template file')
    ],
    ids=[
        'First YAML file fails',
        'First template file fails'
    ]
)
def test_upgrade_scripts_as_3_yaml_placeholders_cause_error(
        side_effect_index: int,
        expected_error: str,
        autosubmit_exp,
        tmp_path: 'LocalPath',
        as3_ini_files: list[Path],
        mocker
):
    """Test that the upgrade script fails if the AS4 YAML or template placeholder fix fails.

    Sorry to any fellow developer who finds this test a bit confusing. That is because the logic
    is as follows:

    * We have 3 test conf files in a fixture (as_ini_files)
    * Each file will produce a YAML
    * Each YAML has a template
    * The function being tested is called once per YAML or template
    * We mock, so the first YAML (index 0) raises a value error, with the message that the YAML failed
    * We mock, so the first template (index 3) raises a value error, with the message that the template failed
    """
    as_exp = autosubmit_exp(experiment_data={}, create=False)

    exp_dir = Path(as_exp.as_conf.basic_config.LOCAL_ROOT_DIR, as_exp.expid)
    # Copy fixture INI files into the experiment path
    for ini_file in as3_ini_files:
        shutil.copy(ini_file, exp_dir / 'conf')

    # We have one call per conf file converted to YAML; each file should create a new entry for its job.
    side_effect: list[Any] = [([], []) for _ in as3_ini_files]
    side_effect.extend([([], []) for _ in as3_ini_files])
    # The first call results in an error
    side_effect[side_effect_index] = ValueError('test')

    mocker.patch('autosubmit.config.upgrade_scripts._fix_placeholders', side_effect=side_effect)
    mocked_log = mocker.patch('autosubmit.config.upgrade_scripts.Log')
    upgrade_scripts(as_exp.expid, files=[])
    assert mocked_log.printlog.called
    assert expected_error in mocked_log.printlog.call_args_list[0][0][0]


def test_upgrade_scripts(autosubmit_exp, tmp_path: 'LocalPath', as3_ini_files: list[Path]):
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
    # Copy fixture INI files into the experiment path
    for ini_file in as3_ini_files:
        shutil.copy(ini_file, exp_dir / 'conf')

    as3_script_template = temp_project / 'templates/local_setup.sh'
    as3_script_template.parent.mkdir(exist_ok=True, parents=True)
    as3_script_template.write_text(dedent('''\
    #!/bin/bash
    
    echo "The job name is %JOBNAME% on %HPCARCH%"
    '''))

    assert as_exp.autosubmit.create(as_exp.expid, force=True, noplot=True, hide=True) == 0

    assert upgrade_scripts(as_exp.expid, files=[])

    assert as_exp.autosubmit.create(as_exp.expid, force=True, noplot=True, hide=True) == 0
    assert as_exp.autosubmit.inspect(as_exp.expid, lst='', filter_chunks='', filter_status='', filter_section='')

    inspect_generated_script = exp_dir / f'tmp/{as_exp.expid}_LOCAL_SETUP.cmd'
    assert inspect_generated_script.exists(), f"inspect executed but {inspect_generated_script} does not exist"

    assert f'The job name is {as_exp.expid}_LOCAL_SETUP' in inspect_generated_script.read_text()

    assert as_exp.as_conf.experiment_data['PLATFORMS']['MARENOSTRUM4-TEST']['USER'] == ['%USER%'], "Not uppercase!"
