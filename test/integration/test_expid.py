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

"""Tests for ``autosubmit expid``."""

import sqlite3
import tempfile
from collections.abc import Callable
from getpass import getuser
from itertools import permutations, product
from pathlib import Path
from textwrap import dedent

import pytest
from autosubmitconfigparser.config.basicconfig import BasicConfig

from autosubmit.autosubmit import Autosubmit
from autosubmit.experiment.experiment_common import new_experiment, copy_experiment
from log.log import AutosubmitCritical, AutosubmitError

_EXPID = 't000'
_DESCRIPTION = "for testing"
_VERSION = "test-version"


@pytest.mark.parametrize(
    'type_flag',
    [
        'op',
        'ev'
    ]
)
def test_copy_experiment(type_flag: str, autosubmit_exp: Callable, autosubmit: Autosubmit) -> None:
    """Test that we can copy experiment using flags for operational and evaluation experiment types.

    :param type_flag: Variable to check which kind of flag it is.
    :type type_flag: bool
    :param autosubmit_exp: Autosubmit interface that instantiate with an experiment.
    :type autosubmit_exp: Callable
    :param autosubmit: Autosubmit interface that instantiate with no experiment.
    :type autosubmit: Autosubmit

    :return: None
    """
    base_experiment = autosubmit_exp('t000', experiment_data={})

    is_operational = type_flag == 'op'
    is_evaluation = type_flag == 'ev'

    expid = autosubmit.expid(
        'test',
        hpc='local',
        copy_id=base_experiment.expid,
        operational=is_operational,
        evaluation=is_evaluation
    )

    assert expid


@pytest.mark.parametrize(
    'type_flag',
    [
        'op',
        'ev'
    ]
)
def test_expid_mutually_exclusive_arguments(type_flag: str, autosubmit: Autosubmit) -> None:
    """Test for issue 2280, where mutually exclusive arguments like op/ev flags and min were not working.

    :param type_flag: Variable to check which kind of flag it is.
    :type type_flag: bool
    :param autosubmit: Autosubmit interface that instantiate with no experiment.
    :type autosubmit: Autosubmit

    :return: None
    """
    is_operational = type_flag == 'op'
    is_evaluation = type_flag == 'ev'

    expid = autosubmit.expid(
        'test',
        hpc='local',
        operational=is_operational,
        evaluation=is_evaluation,
        use_local_minimal=True
    )

    assert expid


@pytest.mark.parametrize(
    'has_min_yaml',
    [
        True,
        False
    ]
)
def test_copy_minimal(has_min_yaml: bool, autosubmit: Autosubmit) -> None:
    """
    Test for issue 2280, ensure autosubmit expid -min --copy expid_id cannot be used if the experiment
    does not have an expid_id/conf/minimal.yml file

    :param has_min_yaml: Variable to simulate if experiment has minimal or not.
    :type has_min_yaml: bool
    :param autosubmit: Autosubmit interface that instantiate with no experiment.
    :type autosubmit: Autosubmit

    :return: None
    """

    expid = autosubmit.expid(
        'test',
        hpc='local',
        minimal_configuration=True
    )
    minimal_file = Path(BasicConfig.LOCAL_ROOT_DIR) / expid / "conf" / "minimal.yml"
    if has_min_yaml:
        minimal_file.write_text("test")
        expid2 = autosubmit.expid(
            minimal_configuration=True,
            copy_id=expid,
            description="Pytest experiment")
        assert expid2
        minimal2 = Path(BasicConfig.LOCAL_ROOT_DIR) / expid2 / "conf" / "minimal.yml"
        content = minimal2.read_text()
        assert content == "test", f"Unexpected content: {content}"
    else:
        minimal_file.unlink()
        with pytest.raises(AutosubmitCritical) as exc_info:
            autosubmit.expid(
                minimal_configuration=True,
                copy_id=expid,
                description="Pytest experiment")
            assert exc_info.value.code == 7011
            assert "minimal.yml" in str(exc_info.value)


def test_create_expid_default_hpc(autosubmit: Autosubmit) -> None:
    """Create expid with the default hcp value (no -H flag defined).

    code-block:: console

        autosubmit expid -d "test descript"

    :param autosubmit: Autosubmit interface that instantiate with no experiment.
    :type autosubmit: Autosubmit

    :return: None
    """
    # create default expid
    experiment_id = autosubmit.expid(
        'experiment_id',
        "",
        minimal_configuration=True
    )

    # capture the platform using the "describe"
    describe = autosubmit.describe(experiment_id)
    hpc_result = describe[4].lower()

    assert hpc_result == "local"


@pytest.mark.parametrize("fake_hpc,expected_hpc", [
    ("mn5", "mn5"),
    ("", "local"), ])
def test_create_expid_flag_hpc(fake_hpc: str, expected_hpc: str, autosubmit: Autosubmit) -> None:
    """Create expid using the flag -H. Defining a value for the flag and not defining any value for that flag.

    code-block:: console

        autosubmit expid -H ithaca -d "experiment"
        autosubmit expid -H "" -d "experiment"

    :param fake_hpc: The value for the -H flag (hpc value).
    :type fake_hpc: str
    :param expected_hpc: The value it is expected for the variable hpc.
    :type expected_hpc: str
    :param autosubmit: Autosubmit interface that instantiate with no experiment.
    :type autosubmit: Autosubmit

    :return: None
    """
    # create default expid with know hpc

    experiment_id = autosubmit.expid(
        'experiment',
        fake_hpc,
        minimal_configuration=True
    )

    # capture the platform using the "describe"
    describe_experiment = autosubmit.describe(experiment_id)
    hpc_result_experiment = describe_experiment[4].lower()

    assert hpc_result_experiment == expected_hpc


@pytest.mark.parametrize("fake_hpc,expected_hpc", [
    ("mn5", "mn5"),
    ("", "local"),
])
def test_copy_expid(fake_hpc: str, expected_hpc: str, autosubmit: Autosubmit) -> None:
    """Copy an experiment without indicating which is the new HPC platform

    code-block:: console

        autosubmit expid -d "original" -H "<PLATFORM>"
        autosubmit expid -y a000 -d ""

    :param fake_hpc: The value for the -H flag (hpc value).
    :type fake_hpc: str
    :param expected_hpc: The value it is expected for the variable hpc.
    :type expected_hpc: str
    :param autosubmit: Autosubmit interface that instantiate with no experiment.
    :type autosubmit: Autosubmit

    :return: None
    """
    # create default expid with know hpc

    original_id = autosubmit.expid(
        'original',
        fake_hpc,
        minimal_configuration=True
    )

    copy_id = autosubmit.expid('copy', "", original_id)

    # capture the platform using the "describe"
    describe_copy = autosubmit.describe(copy_id)
    hpc_result_copy = describe_copy[4].lower()

    assert hpc_result_copy == expected_hpc


def test_copy_expid_no(autosubmit: Autosubmit) -> None:
    """Copying expid, but choosing another HPC value must create a new experiment with the chosen HPC value

    code-block:: console

        autosubmit expid -y a000 -h local -d "experiment is about..."

    :param autosubmit: Autosubmit interface that instantiates with no experiment.
    :type autosubmit: Autosubmit

    :return: None
    """
    # create default expid with know hpc
    fake_hpc = "mn5"
    new_hpc = "local"

    experiment_id = autosubmit.expid('original', fake_hpc)
    copy_experiment_id = autosubmit.expid("copy experiment", new_hpc, experiment_id)
    # capture the platform using the "describe"
    describe = autosubmit.describe(copy_experiment_id)
    hpc_result = describe[4].lower()

    assert hpc_result != new_hpc


def _get_experiment_data(tmp_path):
    _user = getuser()

    return {
        'PLATFORMS': {
            'pytest-ps': {
                'type': 'ps',
                'host': '127.0.0.1',
                'user': _user,
                'project': 'whatever',
                'scratch': str(tmp_path / 'scratch'),
                'DISABLE_RECOVERY_THREADS': 'True'
            }
        },
        'JOBS': {
            'debug': {
                'SCRIPT': 'echo "Hello world"',
                'RUNNING': 'once'
            },
        }
    }


@pytest.mark.parametrize('test,operational,evaluation,expected,new_exp', [
    (False, False, False, "a000", False),
    (True, False, False, "t000", False),
    (False, True, False, "o000", False),
    (False, False, True, "e000", False),
    (False, False, False, "a007", True),
    (True, False, False, "t0ac", True),
    (False, True, False, "o113", True),
    (False, False, True, "e113", True),
    (False, False, True, "", True)
], ids=["Experiment", "Test Experiment", "Operational Experiment", "Evaluation Experiment", "New Experiment",
        "New Test Experiment", "New Operational Experiment", "New Evaluation Experiment",
        "New Evaluation Experiment With Empty Current"])
def test_create_new_experiment(mocker, test, operational, evaluation, expected, new_exp):
    db_common_mock = mocker.patch('autosubmit.experiment.experiment_common.db_common')
    if new_exp:
        current_experiment_id = expected
    else:
        current_experiment_id = "empty"
    _build_db_mock(current_experiment_id, db_common_mock, mocker)
    experiment_id = new_experiment(_DESCRIPTION, _VERSION, test, operational, evaluation)
    assert expected == experiment_id


def test_copy_experiment_new(mocker):
    db_common_mock = mocker.patch('autosubmit.experiment.experiment_common.db_common')
    current_experiment_id = "empty"
    _build_db_mock(current_experiment_id, db_common_mock, mocker)
    experiment_id = copy_experiment(current_experiment_id, _DESCRIPTION, _VERSION, False, False, True)
    assert "" == experiment_id


def _build_db_mock(current_experiment_id, mock_db_common, mocker):
    mock_db_common.last_name_used = mocker.Mock(return_value=current_experiment_id)
    mock_db_common.check_experiment_exists = mocker.Mock(return_value=False)


def test_autosubmit_generate_config(mocker, autosubmit, tmp_path):
    expid = 'ff99'
    original_local_root_dir = BasicConfig.LOCAL_ROOT_DIR
    read_files_mock = mocker.patch('autosubmit.autosubmit.read_files')

    temp_file_path = Path(tmp_path, 'files')
    temp_file_path.mkdir(exist_ok=True)

    with tempfile.NamedTemporaryFile(dir=temp_file_path, suffix='.yaml', mode='w') as source_yaml:
        # Our processed and commented YAML output file must be written here
        Path(tmp_path, expid, 'conf').mkdir(parents=True)
        BasicConfig.LOCAL_ROOT_DIR = tmp_path

        source_yaml.write(
            dedent('''
                    JOB:
                        JOBNAME: SIM
                        PLATFORM: local
                    CONFIG:
                        TEST: The answer?
                        ROOT: No'''))
        source_yaml.flush()
        read_files_mock.return_value = Path(tmp_path)

        parameters = {
            'JOB': {
                'JOBNAME': 'sim'
            },
            'CONFIG': {
                'CONFIG.TEST': '42'
            }
        }
        autosubmit.generate_as_config(exp_id=expid, parameters=parameters)

        source_text = Path(source_yaml.name).read_text()
        source_name = Path(source_yaml.name)
        output_text = Path(tmp_path, expid, 'conf', f'{source_name.stem}_{expid}.yml').read_text()

        assert source_text != output_text
        assert '# sim' not in source_text
        assert '# sim' in output_text
        assert '# 42' not in source_text
        assert '# 42' in output_text

    # Reset the local root dir.
    BasicConfig.LOCAL_ROOT_DIR = original_local_root_dir


def test_autosubmit_generate_config_resource_listdir_order(
        autosubmit,
        mocker
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

    :return: None
    """

    # unique lists of resources, no repetitions

    yaml_mock = mocker.patch('autosubmit.autosubmit.YAML.dump', return_value=None)
    read_files_mock = mocker.patch('autosubmit.autosubmit.read_files', return_value=None)
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
            temp_file_path = Path(temp_dir, 'files')
            Path(temp_file_path).mkdir()

            BasicConfig.LOCAL_ROOT_DIR = temp_dir

            resources_return = []
            filenames_return = []

            for file_name in test['resources']:
                input_path = Path(temp_file_path, file_name)
                with open(input_path, 'w+') as source_yaml:
                    source_yaml.write('TEST: YES')
                    source_yaml.flush()

                    resources_return.append(input_path.name)  # path
                    filenames_return.append(source_yaml.name)  # text IO wrapper

            read_files_mock.return_value = Path(temp_dir)

            autosubmit.generate_as_config(
                exp_id=expid,
                dummy=test['dummy'],
                minimal_configuration=test['minimal_configuration'],
                local=test['local'])

            msg = (f'Incorrect call count for resources={",".join(resources_return)}, dummy={test["dummy"]},'
                   f' minimal_configuration={test["minimal_configuration"]}, local={test["local"]}')
            expected = 2 if (not test['dummy'] and not test['minimal_configuration']) else 1
            assert yaml_mock.call_count == expected, msg
            yaml_mock.reset_mock()

        # Reset the local root dir.
        BasicConfig.LOCAL_ROOT_DIR = original_local_root_dir


def test_expid_generated_correctly(tmp_path, autosubmit_exp, autosubmit):
    as_exp = autosubmit_exp(_EXPID, _get_experiment_data(tmp_path))
    run_dir = as_exp.as_conf.basic_config.LOCAL_ROOT_DIR
    autosubmit.inspect(expid=f'{_EXPID}', check_wrapper=True, force=True, lst=None, filter_chunks=None,
                       filter_status=None, filter_section=None)
    assert f"{_EXPID}_DEBUG.cmd" in [Path(f).name for f in
                                     Path(f"{run_dir}/{_EXPID}/tmp").iterdir()]
    # Consult if the expid is in the database
    db_path = Path(f"{run_dir}/tests.db")
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM experiment WHERE name='{_EXPID}'")
        assert cursor.fetchone() is not None
        cursor.close()


def test_delete_experiment(mocker, tmp_path, autosubmit_exp, autosubmit):
    as_exp = autosubmit_exp(_EXPID, _get_experiment_data(tmp_path))
    run_dir = as_exp.as_conf.basic_config.LOCAL_ROOT_DIR
    mocker.patch("autosubmit.autosubmit.process_id", return_value=None)
    autosubmit.delete(expid=f'{_EXPID}', force=True)
    assert all(_EXPID not in Path(f).name for f in Path(f"{run_dir}").iterdir())
    assert all(_EXPID not in Path(f).name for f in Path(f"{run_dir}/metadata/data").iterdir())
    assert all(_EXPID not in Path(f).name for f in Path(f"{run_dir}/metadata/logs").iterdir())
    assert all(_EXPID not in Path(f).name for f in Path(f"{run_dir}/metadata/structures").iterdir())
    # Consult if the expid is not in the database
    db_path = Path(f"{run_dir}/tests.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT name FROM experiment WHERE name='{_EXPID}'")
    assert cursor.fetchone() is None
    cursor.close()
    # Test doesn't exist
    with pytest.raises(AutosubmitCritical):
        autosubmit.delete(expid=f'{_EXPID}', force=True)


def test_delete_experiment_not_owner(mocker, tmp_path, autosubmit_exp, autosubmit):
    as_exp = autosubmit_exp(_EXPID, _get_experiment_data(tmp_path))
    run_dir = as_exp.as_conf.basic_config.LOCAL_ROOT_DIR
    mocker.patch('autosubmit.autosubmit.Autosubmit._user_yes_no_query', return_value=True)
    mocker.patch('pwd.getpwuid', side_effect=TypeError)
    mocker.patch("autosubmit.autosubmit.process_id", return_value=None)
    _, _, current_owner = autosubmit._check_ownership(_EXPID)
    assert current_owner is None
    # test not owner not eadmin
    _user = getuser()
    mocker.patch("autosubmit.autosubmit.Autosubmit._check_ownership",
                 return_value=(False, False, _user))
    with pytest.raises(AutosubmitCritical):
        autosubmit.delete(expid=f'{_EXPID}', force=True)
    # test eadmin
    mocker.patch("autosubmit.autosubmit.Autosubmit._check_ownership",
                 return_value=(False, True, _user))
    with pytest.raises(AutosubmitCritical):
        autosubmit.delete(expid=f'{_EXPID}', force=False)
    # test eadmin force
    autosubmit.delete(expid=f'{_EXPID}', force=True)
    assert all(_EXPID not in Path(f).name for f in Path(f"{run_dir}").iterdir())
    assert all(_EXPID not in Path(f).name for f in Path(f"{run_dir}/metadata/data").iterdir())
    assert all(_EXPID not in Path(f).name for f in Path(f"{run_dir}/metadata/logs").iterdir())
    assert all(_EXPID not in Path(f).name for f in Path(f"{run_dir}/metadata/structures").iterdir())
    # Consult if the expid is not in the database
    db_path = Path(f"{run_dir}/tests.db")
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM experiment WHERE name='{_EXPID}'")
        assert cursor.fetchone() is None
        cursor.close()


def test_delete_expid(mocker, tmp_path, autosubmit_exp, autosubmit):
    as_exp = autosubmit_exp(_EXPID, _get_experiment_data(tmp_path))
    mocker.patch('autosubmit.autosubmit.Autosubmit._perform_deletion', return_value="error")
    with pytest.raises(AutosubmitError):
        autosubmit._delete_expid(as_exp.expid, force=True)
    mocker.stopall()
    autosubmit._delete_expid(as_exp.expid, force=True)
    assert not autosubmit._delete_expid(as_exp.expid, force=True)


def test_perform_deletion(mocker, tmp_path, autosubmit_exp, autosubmit):
    as_exp = autosubmit_exp(_EXPID, _get_experiment_data(tmp_path))
    mocker.patch("shutil.rmtree", side_effect=FileNotFoundError)
    mocker.patch("os.remove", side_effect=FileNotFoundError)
    basic_config = as_exp.as_conf.basic_config
    experiment_path = Path(f"{basic_config.LOCAL_ROOT_DIR}/{_EXPID}")
    structure_db_path = Path(f"{basic_config.STRUCTURES_DIR}/structure_{_EXPID}.db")
    job_data_db_path = Path(f"{basic_config.JOBDATA_DIR}/job_data_{_EXPID}")
    if all("tmp" not in path for path in [str(experiment_path), str(structure_db_path), str(job_data_db_path)]):
        raise AutosubmitCritical("tmp not in path")
    mocker.patch("autosubmit.autosubmit.delete_experiment", side_effect=FileNotFoundError)
    err_message = autosubmit._perform_deletion(experiment_path, structure_db_path, job_data_db_path, _EXPID)
    assert all(x in err_message for x in
               ["Cannot delete experiment entry", "Cannot delete directory", "Cannot delete structure",
                "Cannot delete job_data"])
