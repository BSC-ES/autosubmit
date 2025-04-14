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

import argparse
import os
import pwd
import sqlite3
import tempfile
from itertools import permutations, product
from pathlib import Path
from textwrap import dedent
from ruamel.yaml import YAML

import pytest
from pytest_mock import MockerFixture
from mock import Mock, patch

from autosubmit.autosubmit import Autosubmit
from autosubmit.git.autosubmit_git import AutosubmitGit
from autosubmit.experiment.experiment_common import new_experiment, copy_experiment
from autosubmitconfigparser.config.basicconfig import BasicConfig
from log.log import AutosubmitCritical, AutosubmitError
from test.unit.utils.common import create_database, init_expid

"""Tests for experiment IDs."""

_DESCRIPTION = "for testing"
_VERSION = "test-version"


@patch('autosubmit.experiment.experiment_common.db_common')
def test_create_new_experiment(db_common_mock):
    current_experiment_id = "empty"
    _build_db_mock(current_experiment_id, db_common_mock)
    experiment_id = new_experiment(_DESCRIPTION, _VERSION)
    assert "a000" == experiment_id


@patch('autosubmit.experiment.experiment_common.db_common')
def test_create_new_test_experiment(db_common_mock):
    current_experiment_id = "empty"
    _build_db_mock(current_experiment_id, db_common_mock)
    experiment_id = new_experiment(_DESCRIPTION, _VERSION, True)
    assert "t000" == experiment_id


@patch('autosubmit.experiment.experiment_common.db_common')
def test_create_new_operational_experiment(db_common_mock):
    current_experiment_id = "empty"
    _build_db_mock(current_experiment_id, db_common_mock)
    experiment_id = new_experiment(_DESCRIPTION, _VERSION, False, True)
    assert "o000" == experiment_id


@patch('autosubmit.experiment.experiment_common.db_common')
def test_create_new_evaluation_experiment(db_common_mock):
    current_experiment_id = "empty"
    _build_db_mock(current_experiment_id, db_common_mock)
    experiment_id = new_experiment(_DESCRIPTION, _VERSION, False, False, True)
    assert "e000" == experiment_id


@patch('autosubmit.experiment.experiment_common.db_common')
def test_create_new_experiment_with_previous_one(db_common_mock):
    current_experiment_id = "a007"
    _build_db_mock(current_experiment_id, db_common_mock)
    experiment_id = new_experiment(_DESCRIPTION, _VERSION)
    assert "a007" == experiment_id


@patch('autosubmit.experiment.experiment_common.db_common')
def test_create_new_test_experiment_with_previous_one(db_common_mock):
    current_experiment_id = "t0ac"
    _build_db_mock(current_experiment_id, db_common_mock)
    experiment_id = new_experiment(_DESCRIPTION, _VERSION, True)
    assert "t0ac" == experiment_id


@patch('autosubmit.experiment.experiment_common.db_common')
def test_create_new_operational_experiment_with_previous_one(db_common_mock):
    current_experiment_id = "o113"
    _build_db_mock(current_experiment_id, db_common_mock)
    experiment_id = new_experiment(_DESCRIPTION, _VERSION, False, True)
    assert "o113" == experiment_id


@patch('autosubmit.experiment.experiment_common.db_common')
def test_create_new_evaluation_experiment_with_previous_one(db_common_mock):
    current_experiment_id = "e113"
    _build_db_mock(current_experiment_id, db_common_mock)
    experiment_id = new_experiment(_DESCRIPTION, _VERSION, False, False, True)
    assert "e113" == experiment_id


@patch('autosubmit.experiment.experiment_common.db_common')
def test_copy_experiment_new(db_common_mock):
    current_experiment_id = "empty"
    _build_db_mock(current_experiment_id, db_common_mock)
    experiment_id = copy_experiment(current_experiment_id, _DESCRIPTION, _VERSION, False, False, True)
    assert "" == experiment_id


@patch('autosubmit.experiment.experiment_common.db_common')
def test_create_new_evaluation_experiment_with_empty_current(db_common_mock):
    current_experiment_id = ""
    _build_db_mock(current_experiment_id, db_common_mock)
    experiment_id = new_experiment(_DESCRIPTION, _VERSION, False, False, True)
    assert "" == experiment_id


def _build_db_mock(current_experiment_id, mock_db_common):
    mock_db_common.last_name_used = Mock(return_value=current_experiment_id)
    mock_db_common.check_experiment_exists = Mock(return_value=False)


@patch('autosubmit.autosubmit.read_files')
def test_autosubmit_generate_config(read_files_mock):
    expid = 'ff99'
    original_local_root_dir = BasicConfig.LOCAL_ROOT_DIR

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_file_path = os.path.join(temp_dir, 'files')
        os.makedirs(temp_file_path)

        with tempfile.NamedTemporaryFile(dir=temp_file_path, suffix='.yaml', mode='w') as source_yaml:
            # Our processed and commented YAML output file must be written here
            Path(temp_dir, expid, 'conf').mkdir(parents=True)
            BasicConfig.LOCAL_ROOT_DIR = temp_dir

            source_yaml.write(
                dedent('''
                    JOB:
                        JOBNAME: SIM
                        PLATFORM: local
                    CONFIG:
                        TEST: The answer?
                        ROOT: No'''))
            source_yaml.flush()
            read_files_mock.return_value = Path(temp_dir)

            parameters = {
                'JOB': {
                    'JOBNAME': 'sim'
                },
                'CONFIG': {
                    'CONFIG.TEST': '42'
                }
            }
            Autosubmit.generate_as_config(exp_id=expid, parameters=parameters)

            source_text = Path(source_yaml.name).read_text()
            source_name = Path(source_yaml.name)
            output_text = Path(temp_dir, expid, 'conf', f'{source_name.stem}_{expid}.yml').read_text()

            assert source_text != output_text
            assert not '# sim' in source_text
            assert '# sim' in output_text
            assert not '# 42' in source_text
            print(output_text)
            assert '# 42' in output_text

    # Reset the local root dir.
    BasicConfig.LOCAL_ROOT_DIR = original_local_root_dir


@patch('autosubmit.autosubmit.YAML.dump')
@patch('autosubmit.autosubmit.read_files')
def test_autosubmit_generate_config_resource_listdir_order(
        read_files_mock,
        yaml_mock
) -> None:
    """
    In https://earth.bsc.es/gitlab/es/autosubmit/-/issues/1063 we had a bug
    where we relied on the order of returned entries of ``pkg_resources.resource_listdir``
    (which is actually undefined per https://importlib-resources.readthedocs.io/en/latest/migration.html).

    We use the arrays below to test that defining a git minimal, we process only
    the expected files. We permute and then product the arrays to get as many test
    cases as possible.

    For every test case, we know that for dummy and minimal we get just one configuration
    template file used. But for other cases we get as many files as we have that are not
    ``*minimal.yml`` nor ``*dummy.yml``. In our test cases here, when not dummy and not minimal
    we must get 2 files, since we have ``include_me_please.yml`` and ``me_too.yml``.

    :param read_files_mock: mocked function to read files
    :param yaml_mock: mocked YAML dump function
    :return: None
    """

    # unique lists of resources, no repetitions
    resources = permutations(
        ['dummy.yml', 'local-minimal.yml', 'git-minimal.yml', 'include_me_please.yml', 'me_too.yml'])
    dummy = [True, False]
    local = [True, False]
    minimal_configuration = [True, False]
    test_cases = product(resources, dummy, local, minimal_configuration)
    keys = ['resources', 'dummy', 'local', 'minimal_configuration']

    for test_case in test_cases:
        test = dict(zip(keys, test_case))
        expid = 'ff99'
        original_local_root_dir = BasicConfig.LOCAL_ROOT_DIR

        with tempfile.TemporaryDirectory() as temp_dir:
            Path(temp_dir, expid, 'conf').mkdir(parents=True)
            temp_file_path = os.path.join(temp_dir, 'files')
            os.makedirs(temp_file_path)

            BasicConfig.LOCAL_ROOT_DIR = temp_dir

            resources_return = []
            filenames_return = []

            for file_name in test['resources']:
                input_path = Path(temp_file_path, file_name)
                with open(input_path, 'w+') as source_yaml:
                    source_yaml.write('TEST: YES')
                    source_yaml.flush()

                    resources_return.append(input_path.name)  # path
                    filenames_return.append(source_yaml.name)  # textiowrapper

            read_files_mock.return_value = Path(temp_dir)

            Autosubmit.generate_as_config(
                exp_id=expid,
                dummy=test['dummy'],
                minimal_configuration=test['minimal_configuration'],
                local=test['local'])

            msg = f'Incorrect call count for resources={",".join(resources_return)}, dummy={test["dummy"]}, minimal_configuration={test["minimal_configuration"]}, local={test["local"]}'
            expected = 2 if (not test['dummy'] and not test['minimal_configuration']) else 1
            assert yaml_mock.call_count == expected, msg
            yaml_mock.reset_mock()

        # Reset the local root dir.
        BasicConfig.LOCAL_ROOT_DIR = original_local_root_dir


def _get_script_files_path() -> Path:
    return Path(__file__).resolve().parent / 'files'


@pytest.fixture
def create_autosubmit_tmpdir(tmpdir_factory):
    folder = tmpdir_factory.mktemp('autosubmit_tests')
    Path(folder).joinpath('scratch').mkdir()
    file_stat = os.stat(f"{folder.strpath}")
    file_owner_id = file_stat.st_uid
    file_owner = pwd.getpwuid(file_owner_id).pw_name
    folder.owner = file_owner

    # Write an autosubmitrc file in the temporary directory
    autosubmitrc = folder.join('autosubmitrc')
    autosubmitrc.write(f'''
[database]
path = {folder}
filename = tests.db

[local]
path = {folder}

[globallogs]
path = {folder}/globallogs

[structures]
path = {folder}/metadata/structures

[historicdb]
path = {folder}/metadata/database

[historiclog]
path = {folder}/metadata/logs

[defaultstats]
path = {folder}

''')
    os.environ['AUTOSUBMIT_CONFIGURATION'] = str(folder.join('autosubmitrc'))
    create_database(str(folder.join('autosubmitrc')))
    Path(folder).joinpath('metadata').mkdir()
    Path(folder).joinpath('metadata/structures').mkdir()
    Path(folder).joinpath('metadata/database').mkdir()
    Path(folder).joinpath('metadata/logs').mkdir()
    assert "tests.db" in [Path(f).name for f in folder.listdir()]
    return folder


@pytest.fixture
def generate_new_experiment(create_autosubmit_tmpdir, request):
    test_type = request.param
    # Setup code that depends on the expid parameter
    expid = init_expid(os.environ["AUTOSUBMIT_CONFIGURATION"], platform='local', expid=None, create=True,
                       test_type=test_type)
    Path(f"{BasicConfig.STRUCTURES_DIR}/structure_{expid}.db").touch()

    yield expid


@pytest.fixture
def setup_experiment_yamlfiles(generate_new_experiment, create_autosubmit_tmpdir):
    expid = generate_new_experiment
    # touch as_misc
    platforms_path = Path(f"{create_autosubmit_tmpdir.strpath}/{expid}/conf/platforms_{expid}.yml")
    jobs_path = Path(f"{create_autosubmit_tmpdir.strpath}/{expid}/conf/jobs_{expid}.yml")
    # Add each platform to test
    with platforms_path.open('w') as f:
        f.write(f"""
PLATFORMS:
    pytest-ps:
        type: ps
        host: 127.0.0.1
        user: {create_autosubmit_tmpdir.owner}
        project: whatever
        scratch_dir: {create_autosubmit_tmpdir}/scratch
        DISABLE_RECOVERY_THREADS: True
        """)
    # add a job of each platform type
    with jobs_path.open('w') as f:
        f.write("""
JOBS:
    debug:
        script: echo "Hello world"
        running: once
EXPERIMENT:
    DATELIST: '20000101'
    MEMBERS: fc0
    CHUNKSIZEUNIT: month
    CHUNKSIZE: '1'
    NUMCHUNKS: '1'
    CHUNKINI: ''
    CALENDAR: standard
  """)

    expid_dir = Path(f"{create_autosubmit_tmpdir.strpath}/scratch/whatever/{create_autosubmit_tmpdir.owner}/{expid}")
    dummy_dir = Path(
        f"{create_autosubmit_tmpdir.strpath}/scratch/whatever/{create_autosubmit_tmpdir.owner}/{expid}/dummy_dir")
    real_data = Path(
        f"{create_autosubmit_tmpdir.strpath}/scratch/whatever/{create_autosubmit_tmpdir.owner}/{expid}/real_data")
    # write some dummy data inside scratch dir
    expid_dir.mkdir(parents=True, exist_ok=True)
    dummy_dir.mkdir(parents=True, exist_ok=True)
    real_data.mkdir(parents=True, exist_ok=True)

    with open(dummy_dir.joinpath('dummy_file'), 'w') as f:
        f.write('dummy data')
    real_data.joinpath('dummy_symlink').symlink_to(dummy_dir / 'dummy_file')
    yield expid


@pytest.mark.parametrize("generate_new_experiment", ['test', 'normal', 'operational', 'evaluation'], indirect=True)
def test_expid_generated_correctly(create_autosubmit_tmpdir, generate_new_experiment, setup_experiment_yamlfiles):
    expid = generate_new_experiment
    print(f"Running test for {expid}")
    Autosubmit.inspect(expid=f'{expid}', check_wrapper=True, force=True, lst=None, filter_chunks=None,
                       filter_status=None, filter_section=None)
    assert expid in ['t000', 'a000', 'o000', 'e000']
    assert f"{expid}_DEBUG.cmd" in [Path(f).name for f in
                                    Path(f"{create_autosubmit_tmpdir.strpath}/{expid}/tmp").iterdir()]
    # Consult if the expid is in the database
    db_path = Path(f"{create_autosubmit_tmpdir.strpath}/tests.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT name FROM experiment WHERE name='{expid}'")
    assert cursor.fetchone() is not None
    cursor.close()


@pytest.mark.parametrize("generate_new_experiment", ['test', 'normal', 'operational', 'evaluation'], indirect=True)
def test_delete_experiment(create_autosubmit_tmpdir, generate_new_experiment, setup_experiment_yamlfiles):
    expid = generate_new_experiment
    print(f"Running test for {expid}")
    Autosubmit.delete(expid=f'{expid}', force=True)
    assert all(expid not in Path(f).name for f in Path(f"{create_autosubmit_tmpdir.strpath}").iterdir())
    assert all(
        expid not in Path(f).name for f in Path(f"{create_autosubmit_tmpdir.strpath}/metadata/database").iterdir())
    assert all(expid not in Path(f).name for f in Path(f"{create_autosubmit_tmpdir.strpath}/metadata/logs").iterdir())
    assert all(
        expid not in Path(f).name for f in Path(f"{create_autosubmit_tmpdir.strpath}/metadata/structures").iterdir())
    # Consult if the expid is not in the database
    db_path = Path(f"{create_autosubmit_tmpdir.strpath}/tests.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT name FROM experiment WHERE name='{expid}'")
    assert cursor.fetchone() is None
    cursor.close()
    # Test doesn't exist
    with pytest.raises(AutosubmitCritical):
        Autosubmit.delete(expid=f'{expid}', force=True)


@pytest.mark.parametrize("generate_new_experiment", ['test', 'normal', 'operational', 'evaluation'], indirect=True)
def test_delete_experiment_not_owner(create_autosubmit_tmpdir, generate_new_experiment, setup_experiment_yamlfiles,
                                     mocker):
    expid = generate_new_experiment
    print(f"Running test for {expid}")
    mocker.patch('autosubmit.autosubmit.Autosubmit._user_yes_no_query', return_value=True)
    mocker.patch('pwd.getpwuid', side_effect=TypeError)
    _, _, current_owner = Autosubmit._check_ownership(expid)
    assert current_owner is None
    # test not owner not eadmin
    mocker.patch("autosubmit.autosubmit.Autosubmit._check_ownership",
                 return_value=(False, False, create_autosubmit_tmpdir.owner))
    with pytest.raises(AutosubmitCritical):
        Autosubmit.delete(expid=f'{expid}', force=True)
    # test eadmin
    mocker.patch("autosubmit.autosubmit.Autosubmit._check_ownership",
                 return_value=(False, True, create_autosubmit_tmpdir.owner))
    with pytest.raises(AutosubmitCritical):
        Autosubmit.delete(expid=f'{expid}', force=False)
    # test eadmin force
    Autosubmit.delete(expid=f'{expid}', force=True)
    assert all(expid not in Path(f).name for f in Path(f"{create_autosubmit_tmpdir.strpath}").iterdir())
    assert all(
        expid not in Path(f).name for f in Path(f"{create_autosubmit_tmpdir.strpath}/metadata/database").iterdir())
    assert all(expid not in Path(f).name for f in Path(f"{create_autosubmit_tmpdir.strpath}/metadata/logs").iterdir())
    assert all(
        expid not in Path(f).name for f in Path(f"{create_autosubmit_tmpdir.strpath}/metadata/structures").iterdir())
    # Consult if the expid is not in the database
    db_path = Path(f"{create_autosubmit_tmpdir.strpath}/tests.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT name FROM experiment WHERE name='{expid}'")
    assert cursor.fetchone() is None
    cursor.close()


@pytest.mark.parametrize("generate_new_experiment", ['normal'], indirect=True)
def test_delete_expid(create_autosubmit_tmpdir, generate_new_experiment, setup_experiment_yamlfiles, mocker):
    expid = generate_new_experiment
    mocker.patch("autosubmit.autosubmit.Autosubmit._check_ownership",
                 return_value=(True, True, create_autosubmit_tmpdir.owner))
    mocker.patch('autosubmit.autosubmit.Autosubmit._perform_deletion', return_value="error")
    with pytest.raises(AutosubmitError):
        Autosubmit._delete_expid(expid, force=True)
    mocker.stopall()
    mocker.patch("autosubmit.autosubmit.Autosubmit._check_ownership",
                 return_value=(True, True, create_autosubmit_tmpdir.owner))
    Autosubmit._delete_expid(expid, force=True)
    assert not Autosubmit._delete_expid(expid, force=True)


@pytest.mark.parametrize("generate_new_experiment", ['normal'], indirect=True)
def test_perform_deletion(create_autosubmit_tmpdir, generate_new_experiment, setup_experiment_yamlfiles, mocker):
    expid = generate_new_experiment
    mocker.patch("shutil.rmtree", side_effect=FileNotFoundError)
    mocker.patch("os.remove", side_effect=FileNotFoundError)
    basic_config = BasicConfig()
    basic_config.read()
    experiment_path = Path(f"{basic_config.LOCAL_ROOT_DIR}/{expid}")
    structure_db_path = Path(f"{basic_config.STRUCTURES_DIR}/structure_{expid}.db")
    job_data_db_path = Path(f"{basic_config.JOBDATA_DIR}/job_data_{expid}")
    if all("tmp" not in path for path in [str(experiment_path), str(structure_db_path), str(job_data_db_path)]):
        raise AutosubmitCritical("tmp not in path")
    mocker.patch("autosubmit.autosubmit.delete_experiment", side_effect=FileNotFoundError)
    err_message = Autosubmit._perform_deletion(experiment_path, structure_db_path, job_data_db_path, expid)
    assert all(x in err_message for x in
               ["Cannot delete experiment entry", "Cannot delete directory", "Cannot delete structure",
                "Cannot delete job_data"])


@pytest.mark.parametrize(
    "project_type, generate_new_experiment",
    [
        ('git', 'operational'),
        ('git', 'normal'),
        ('git', 'test'),
        ('git', 'evaluation'),
    ],
    indirect=["generate_new_experiment"]
)
def test_remote_repo_operational(generate_new_experiment: str, create_autosubmit_tmpdir: str, project_type: str, mocker: MockerFixture) -> None:
    '''
    Tests the check_unpushed_changed function from AutosubmitGit, which ensures no operational test with unpushed changes in their Git repository is run.
    '''
    expid = generate_new_experiment
    temp_path = Path(create_autosubmit_tmpdir) / expid / "conf" / f"expdef_{expid}.yml"
    
    with open(temp_path, 'r') as f: 
        yaml = YAML(typ='rt')
        data = yaml.load(f)
    data["PROJECT"]["PROJECT_TYPE"] = project_type
    with open(temp_path, 'w') as f:
        yaml.dump(data, f)

    mocker.patch('subprocess.check_output', return_value = b'M\n')
    if expid[0] != 'o':
        AutosubmitGit.check_unpushed_changes(expid)
    else:
        with pytest.raises(AutosubmitCritical):
            AutosubmitGit.check_unpushed_changes(expid) 
            
