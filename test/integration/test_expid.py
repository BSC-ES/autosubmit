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
from contextlib import nullcontext as does_not_raise
from getpass import getuser
from itertools import permutations, product
from pathlib import Path
from textwrap import dedent
from typing import cast, Any, Iterable

import pytest
from ruamel.yaml import YAML

from autosubmit.autosubmit import Autosubmit
from autosubmit.config.basicconfig import BasicConfig
# noinspection PyProtectedMember
from autosubmit.experiment.experiment_common import (
    check_ownership, copy_experiment, delete_experiment,
    _delete_expid, new_experiment, _perform_deletion
)
from autosubmit.log.log import AutosubmitCritical, AutosubmitError
from autosubmit.utils import as_conf_default_values

_DESCRIPTION = "for testing"
_VERSION = "test-version"


def test_copying_experiment_with_hpc_in_command(autosubmit_exp: Callable, autosubmit: Autosubmit, tmp_path):
    """Test to validate if values are being properly copied/erased from one project to another.
    It checks if the HPC copied to the new experiment was the one passed by the command"""

    yaml = YAML(typ='rt')
    exp = autosubmit_exp(experiment_data={
        'DEFAULT': {
            'CUSTOM_CONFIG': 'test'
        },
        'MAIL': {
            'NOTIFICATIONS': 'True',
            'TO': 'uhu@uhu.com'
        },
        'JOBS': {
            'LOCAL_SEND_INITIAL': {
                'CHUNKS_FROM': {
                    '1': {
                        'CHUNKS_TO': '1'
                    }
                }
            }
        },
        'LOCAL': {
            'PROJECT_PATH': 'path'
        },
        'GIT': {
            'PROJECT_ORIGIN': 'origin_test',
            'PROJECT_BRANCH': 'branch_test'
        }
    })

    exp_id = autosubmit.expid(
        'test',
        hpc='test1',
        copy_id=exp.expid
    )
    assert autosubmit.create(exp_id, noplot=True, hide=True) == 0

    yaml_data = yaml.load(open(tmp_path / f"{exp_id}/conf/metadata/experiment_data.yml"))
    assert yaml_data['DEFAULT']['EXPID'] == exp_id
    assert yaml_data['DEFAULT']['HPCARCH'] == "TEST1"
    assert not yaml_data['MAIL']['NOTIFICATIONS']
    assert yaml_data['MAIL']['TO'] == ""
    assert yaml_data['JOBS']['LOCAL_SEND_INITIAL']['CHUNKS_FROM']['1']['CHUNKS_TO'] == '1'
    assert yaml_data['LOCAL']['PROJECT_PATH'] == ""
    assert yaml_data['GIT']['PROJECT_ORIGIN'] == "origin_test"
    assert yaml_data['GIT']['PROJECT_BRANCH'] == "branch_test"


def test_copying_experiment_without_hpc_in_command(autosubmit_exp: Callable, autosubmit: Autosubmit, tmp_path):
    """Test to validate if values are being properly copied/erased from one project to another.
    It checks if the HPC copied to the new experiment was local since both file and command were empty"""

    yaml = YAML(typ='rt')
    exp = autosubmit_exp(experiment_data={
        'DEFAULT': {
            'HPCARCH': '',
            'CUSTOM_CONFIG': 'test',
        },
        'JOBS': {
            'LOCAL_SEND_INITIAL': {
                'CHUNKS_FROM': {
                    1: {
                        'CHUNKS_TO': 1
                    }
                }
            }
        },
    })

    exp_id = autosubmit.expid(
        'test',
        copy_id=exp.expid,
    )

    assert autosubmit.create(exp_id, noplot=True, hide=True) == 0
    yaml_data = yaml.load(open(tmp_path / f"{exp_id}/conf/metadata/experiment_data.yml"))
    assert yaml_data['DEFAULT']['HPCARCH'] == "LOCAL"


def test_copy_expid_with_main_yaml(tmp_path: Path, autosubmit_exp):
    """Test copying an experiment that contains a non-referenced ``main.yaml`` (as in DestinE)."""
    exp = autosubmit_exp(experiment_data={
        'JOBS': {
            'A': {
                'SCRIPT': 'true',
                'PLATFORM': 'LOCAL'
            }
        }
    }, create=False)
    exp_path = Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR, exp.expid)
    main_yaml_path = exp_path / 'conf/main.yaml'
    with open(main_yaml_path, 'w+') as f:
        f.write(dedent('''\
        JOBS:
            B:
                SCRIPT: 'true'
                PLATFORM: LOCAL
        '''))

    assert exp.autosubmit.create(exp.expid, noplot=True, hide=True) == 0
    yaml_data = YAML(typ='rt').load(open(tmp_path / f"{exp.expid}/conf/metadata/experiment_data.yml"))
    assert len(yaml_data['JOBS']) == 2
    assert {'A', 'B'} <= set(yaml_data['JOBS'].keys())

    new_expid = exp.autosubmit.expid(
        'test',
        hpc='local',
        copy_id=exp.expid
    )
    new_exp_path = Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR, new_expid)
    new_main_yaml_path = new_exp_path / 'conf/main.yaml'

    assert new_main_yaml_path.exists() and new_main_yaml_path.is_file()


def test_copying_experiment_with_hpc_in_file(autosubmit_exp: Callable, autosubmit: Autosubmit, tmp_path):
    """Test to validate if values are being properly copied/erased from one project to another.
    It checks if the HPC copied to the new experiment was the one passed by the file"""

    yaml = YAML(typ='rt')

    exp = autosubmit_exp(experiment_data={
        'DEFAULT': {
            'HPCARCH': 'MN5',
            'CUSTOM_CONFIG': 'test',
        },
        'JOBS': {
            'LOCAL_SEND_INITIAL': {
                'CHUNKS_FROM': {
                    1: {
                        'CHUNKS_TO': 1
                    }
                }
            }
        },
    })

    exp_id = autosubmit.expid(
        'test',
        copy_id=exp.expid
    )

    assert autosubmit.create(exp_id, noplot=True, hide=True) == 0
    yaml_data = yaml.load(open(tmp_path / f"{exp_id}/conf/metadata/experiment_data.yml"))
    assert yaml_data['DEFAULT']['HPCARCH'] == "MN5"

@pytest.mark.parametrize(
    'git_command,git_session',
    [
        (['', ''], ['', '']),
        (['test_1', 'test_2'], ['', '']),
        (['', 'test_2'], ['', '']),
        (['test_1', ''], ['', '']),
        (['', ''], ['test_3', 'test_4']),
        (['test_1', 'test_2'], ['test_3', 'test_4']),
        (['', 'test_2'], ['test_3', 'test_4']),
        (['test_1', ''], ['test_3', 'test_4']),
    ],
)
def test_as_conf_default_values(git_command, git_session, autosubmit_exp: Callable, autosubmit: Autosubmit, tmp_path):
    """Test that the ``check_jobs_file_exists`` function ignores a non-existent section."""
    exp = autosubmit_exp(experiment_data={
        'JOBS': {
            'LOCAL_SEND_INITIAL': {
                'CHUNKS_FROM': {
                    1: {
                        'CHUNKS_TO': 1
                    }
                }
            }
        },
        'GIT': {
            'PROJECT_ORIGIN': f'{git_session[0]}',
            'PROJECT_BRANCH': f'{git_session[1]}'
        },
    })
    as_conf_default_values(autosubmit.autosubmit_version, exp.expid, 'MN5', True, git_command[0], git_command[1], 'test_3')

    yaml = YAML(typ='rt')
    assert autosubmit.create(exp.expid, noplot=True, hide=True) == 0
    yaml_data = yaml.load(open(tmp_path / f"{exp.expid}/conf/metadata/experiment_data.yml"))
    assert yaml_data['DEFAULT']['HPCARCH'] == "MN5"
    assert yaml_data['DEFAULT']['EXPID'] == exp.expid
    assert yaml_data['DEFAULT']['CUSTOM_CONFIG'] == f'"{tmp_path}/{exp.expid}/proj/test_3"'
    assert yaml_data['LOCAL']['PROJECT_PATH'] == ""

    if git_command[0] != '':
        assert yaml_data['GIT']['PROJECT_ORIGIN'] == git_command[0]
    else:
        assert yaml_data['GIT']['PROJECT_ORIGIN'] == git_session[0]

    if git_command[1] != '':
        assert yaml_data['GIT']['PROJECT_BRANCH'] == git_command[1]
    else:
        assert yaml_data['GIT']['PROJECT_BRANCH'] == git_session[1]

    if git_session[0] != '' and git_command[0] == '':
        assert yaml_data['GIT']['PROJECT_ORIGIN'] == git_session[0]
    else:
        assert yaml_data['GIT']['PROJECT_ORIGIN'] == git_command[0]

    if git_session[1] != '' and git_command[1] == '':
        assert yaml_data['GIT']['PROJECT_BRANCH'] == git_session[1]
    else:
        assert yaml_data['GIT']['PROJECT_BRANCH'] == git_command[1]


@pytest.mark.parametrize(
    'type_flag',
    [
        'op',
        'ev'
    ]
)
def test_copy_experiment(type_flag: str, autosubmit_exp: Callable, autosubmit: Autosubmit):
    """Test that we can copy the experiment using flags for operational and evaluation experiment types.

    :param type_flag: Variable to check which kind of flag it is.
    :type type_flag: bool
    :param autosubmit_exp: Autosubmit interface that instantiates with an experiment.
    :type autosubmit_exp: Callable
    :param autosubmit: Autosubmit interface that instantiates with no experiment.
    :type autosubmit: Autosubmit

    :return: None
    """
    autosubmit.install()
    base_experiment = autosubmit_exp('t000', experiment_data={}, include_jobs=True)

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
def test_expid_mutually_exclusive_arguments(type_flag: str, autosubmit: Autosubmit):
    """Test for issue 2280, where mutually exclusive arguments like op/ev flags and min were not working.

    :param type_flag: Variable to check which kind of flag it is.
    :param autosubmit: Autosubmit interface that instantiates with no experiment.
    """
    autosubmit.install()
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
def test_copy_minimal(has_min_yaml: bool, autosubmit: Autosubmit):
    """Test for issue 2280, ensure autosubmit expid -min --copy expid_id cannot be used if the experiment
    does not have an expid_id/conf/minimal.yml file

    :param has_min_yaml: Variable to simulate if the experiment has minimal or not.
    :param autosubmit: Autosubmit interface that instantiates with no experiment.
    """
    autosubmit.install()
    expid = autosubmit.expid(
        'test',
        hpc='local',
        minimal_configuration=True
    )
    minimal_file = Path(BasicConfig.LOCAL_ROOT_DIR) / expid / "conf" / "minimal.yml"
    if has_min_yaml:
        minimal_file.write_text("test: test")
        expid2 = autosubmit.expid(
            minimal_configuration=True,
            copy_id=expid,
            description="Pytest experiment")
        assert expid2
        minimal2 = Path(BasicConfig.LOCAL_ROOT_DIR) / expid2 / "conf" / "minimal.yml"
        content = minimal2.read_text()
        assert content == "test: test\n", f"Unexpected content: {content}"
    else:
        minimal_file.unlink()
        with pytest.raises(AutosubmitCritical) as exc_info:
            autosubmit.expid(
                minimal_configuration=True,
                copy_id=expid,
                description="Pytest experiment")
            assert exc_info.value.code == 7011
            assert "minimal.yml" in str(exc_info.value)


def test_create_expid_default_hpc(autosubmit: Autosubmit):
    """Create expid with the default hcp value (no -H flag defined).

    code-block:: console

        autosubmit expid -d "test descript"

    :param autosubmit: Autosubmit interface that instantiates with no experiment.
    """
    autosubmit.install()

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
def test_create_expid_flag_hpc(fake_hpc: str, expected_hpc: str, autosubmit: Autosubmit):
    """Create expid using the flag -H. Defining a value for the flag and not defining any value for that flag.

    code-block:: console

        autosubmit expid -H ithaca -d "experiment"
        autosubmit expid -H "" -d "experiment"

    :param fake_hpc: The value for the -H flag (hpc value).
    :param expected_hpc: The value it is expected for the variable hpc.
    :param autosubmit: Autosubmit interface that instantiates with no experiment.
    """
    # create default expid with know hpc
    autosubmit.install()

    experiment_id = autosubmit.expid(
        'experiment',
        fake_hpc,
        minimal_configuration=True
    )

    # capture the platform using the "describe"
    describe_experiment = autosubmit.describe(experiment_id)
    hpc_result_experiment = describe_experiment[4].lower()

    assert hpc_result_experiment == expected_hpc


@pytest.mark.parametrize("experiment_hpc,expected_hpc", [
    ("local", "mn5"),
    ("mn5", "marenostrum"), ])
def test_copy_expid_with_flag_hpc(tmp_path: Path, autosubmit: Autosubmit, experiment_hpc, expected_hpc):
    """Create expid using the flag -H. Defining a value for the flag and not defining any value for that flag.

    code-block:: console

        autosubmit expid -H ithaca -d "experiment"
        autosubmit expid -H "" -d "experiment"

    :param expected_hpc: The value it is expected for the variable hpc.
    :param autosubmit: Autosubmit interface that instantiate with no experiment.
    """
    autosubmit.install()

    exp_id = autosubmit.expid('experiment', hpc=experiment_hpc, minimal_configuration=True)
    copy_exp = autosubmit.expid('copy', hpc=expected_hpc, copy_id=exp_id, minimal_configuration=True)

    describe = autosubmit.describe(exp_id)
    hpc_experiment = describe[4].lower()

    describe_copy = autosubmit.describe(copy_exp)
    hpc_experiment_copy = describe_copy[4].lower()

    assert hpc_experiment != hpc_experiment_copy
    assert hpc_experiment_copy == expected_hpc


@pytest.mark.parametrize("fake_hpc,expected_hpc", [
    ("mn5", "mn5"),
    ("", "local"),
])
def test_copy_expid(fake_hpc: str, expected_hpc: str, autosubmit: Autosubmit):
    """Copy an experiment without indicating which is the new HPC platform

    code-block:: console

        autosubmit expid -d "original" -H "<PLATFORM>"
        autosubmit expid -y a000 -d ""

    :param fake_hpc: The value for the -H flag (hpc value).
    :param expected_hpc: The value it is expected for the variable hpc.
    :param autosubmit: Autosubmit interface that instantiate with no experiment.
    """
    autosubmit.install()
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


def test_copy_expid_no(autosubmit: Autosubmit):
    """Copying expid, but choosing another HPC value must create a new experiment with the chosen HPC value

    code-block:: console

        autosubmit expid -y a000 -h local -d "experiment is about..."

    :param autosubmit: Autosubmit interface that instantiates with no experiment.
    """
    autosubmit.install()
    # create default expid with know hpc
    fake_hpc = "mn5"
    new_hpc = "local"

    experiment_id = autosubmit.expid('original', fake_hpc)
    copy_experiment_id = autosubmit.expid("copy experiment", new_hpc, experiment_id)
    # capture the platform using the "describe"
    describe = autosubmit.describe(copy_experiment_id)
    hpc_result = describe[4].lower()

    assert hpc_result == new_hpc


def _get_experiment_data(tmp_path):
    _user = getuser()

    return {
        'PLATFORMS': {
            'PYTEST-PS': {
                'TYPE': 'ps',
                'HOST': '127.0.0.1',
                'USER': _user,
                'PROJECT': 'whatever',
                'SCRATCH': str(tmp_path / 'scratch'),
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


def test_autosubmit_generate_config(mocker, autosubmit: Autosubmit, tmp_path, get_next_expid: Callable[[], str]):
    expid = get_next_expid()
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
        autosubmit.generate_as_config(exp_id=expid, parameters=parameters)  # type: ignore

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


def test_autosubmit_generate_config_resource_listdir_order(autosubmit, mocker):
    """In https://earth.bsc.es/gitlab/es/autosubmit/-/issues/1063 we had a bug
    where we relied on the order of returned entries of ``pkg_resources.resource_listdir``
    (which is actually undefined per https://importlib-resources.readthedocs.io/en/latest/migration.html).

    We use the arrays below to test that defining a git minimal, we process only
    the expected files. We permute and then product the arrays to get as many test
    cases as possible.

    For every test case, we know that for dummy and minimal we get just one configuration
    template file used. But for other cases we get as many files as we have that are not
    ``*minimal.yml`` nor ``*dummy.yml``. In our test cases here, when not dummy and not minimal
    we must get 2 files, since we have ``include_me_please.yml`` and ``me_too.yml``.
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
        test: dict[str, Iterable[Any]] = cast(dict[str, Iterable], dict(zip(keys, test_case)))
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
                dummy=cast(bool, test['dummy']),
                minimal_configuration=cast(bool, test['minimal_configuration']),
                local=cast(bool, test['local']))

            msg = (f'Incorrect call count for resources={",".join(resources_return)}, dummy={test["dummy"]},'
                   f' minimal_configuration={test["minimal_configuration"]}, local={test["local"]}')
            expected = 2 if (not test['dummy'] and not test['minimal_configuration']) else 1
            assert yaml_mock.call_count == expected, msg
            yaml_mock.reset_mock()

        # Reset the local root dir.
        BasicConfig.LOCAL_ROOT_DIR = original_local_root_dir


def test_expid_generated_correctly(tmp_path, autosubmit_exp, autosubmit):
    autosubmit.install()
    as_exp = autosubmit_exp(experiment_data=_get_experiment_data(tmp_path))
    run_dir = as_exp.as_conf.basic_config.LOCAL_ROOT_DIR
    autosubmit.inspect(expid=f'{as_exp.expid}', check_wrapper=True, force=True, lst=None,  # type: ignore
                       filter_chunks=None, filter_status=None, filter_section=None)  # type: ignore
    assert f"{as_exp.expid}_DEBUG.cmd" in [
        Path(f).name for f in Path(f"{run_dir}/{as_exp.expid}/tmp").iterdir()
    ]
    # Consult if the expid is in the database
    db_path = Path(f"{run_dir}/tests.db")
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM experiment WHERE name='{as_exp.expid}'")
        assert cursor.fetchone() is not None
        cursor.close()


def test_delete_experiment(mocker, tmp_path, autosubmit_exp, autosubmit: Autosubmit):
    autosubmit.install()
    as_exp = autosubmit_exp(experiment_data=_get_experiment_data(tmp_path))
    run_dir = as_exp.as_conf.basic_config.LOCAL_ROOT_DIR
    mocker.patch("autosubmit.experiment.experiment_common.process_id", return_value=None)
    mocked_log = mocker.patch("autosubmit.experiment.experiment_common.Log")

    assert delete_experiment(expids=f'{as_exp.expid}', force=True)
    assert mocked_log.error.call_count == 0

    assert all(as_exp.expid not in Path(f).name for f in Path(f"{run_dir}").iterdir())
    assert all(as_exp.expid not in Path(f).name for f in Path(f"{run_dir}/metadata/data").iterdir())
    assert all(as_exp.expid not in Path(f).name for f in Path(f"{run_dir}/metadata/logs").iterdir())
    assert all(as_exp.expid not in Path(f).name for f in Path(f"{run_dir}/metadata/structures").iterdir())
    # Consult if the expid is not in the database
    db_path = Path(f"{run_dir}/tests.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT name FROM experiment WHERE name='{as_exp.expid}'")
    assert cursor.fetchone() is None
    cursor.close()
    # Test doesn't exist

    delete_experiment(expids=f'{as_exp.expid}', force=True)
    assert mocked_log.error.call_count > 0
    assert 'Experiment does not exist' in mocked_log.error.call_args_list[0][0][0]


def test_delete_experiment_not_owner(mocker, tmp_path, autosubmit_exp, autosubmit: Autosubmit):
    autosubmit.install()
    as_exp = autosubmit_exp(experiment_data=_get_experiment_data(tmp_path))
    run_dir = as_exp.as_conf.basic_config.LOCAL_ROOT_DIR
    mocker.patch('autosubmit.experiment.experiment_common.user_yes_no_query', return_value=True)
    mocker.patch('pwd.getpwuid', side_effect=TypeError)
    mocker.patch("autosubmit.experiment.experiment_common.process_id", return_value=None)
    mocked_log = mocker.patch("autosubmit.experiment.experiment_common.Log")
    _, _, current_owner = check_ownership(as_exp.expid)
    assert current_owner is None
    # test not owner not eadmin
    _user = getuser()
    mocker.patch("autosubmit.experiment.experiment_common.check_ownership",
                 return_value=(False, False, _user))

    delete_experiment(expids=f'{as_exp.expid}', force=True)
    assert mocked_log.error.call_count > 0
    assert 'Failed to delete experiment' in mocked_log.error.call_args_list[0][0][0]

    # test eadmin
    mocker.patch("autosubmit.experiment.experiment_common.check_ownership",
                 return_value=(False, True, _user))
    delete_experiment(expids=f'{as_exp.expid}', force=False)
    assert mocked_log.error.call_count > 0
    assert 'Failed to delete experiment' in mocked_log.error.call_args_list[0][0][0]

    # test eadmin force
    delete_experiment(expids=f'{as_exp.expid}', force=True)
    assert all(as_exp.expid not in Path(f).name for f in Path(f"{run_dir}").iterdir())
    assert all(as_exp.expid not in Path(f).name for f in Path(f"{run_dir}/metadata/data").iterdir())
    assert all(as_exp.expid not in Path(f).name for f in Path(f"{run_dir}/metadata/logs").iterdir())
    assert all(as_exp.expid not in Path(f).name for f in Path(f"{run_dir}/metadata/structures").iterdir())
    # Consult if the expid is not in the database
    db_path = Path(f"{run_dir}/tests.db")
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM experiment WHERE name='{as_exp.expid}'")
        assert cursor.fetchone() is None
        cursor.close()


@pytest.mark.parametrize(
    "expid_value",
    [
        pytest.param("..", id="parent_dir"),
        pytest.param("", id="empty_string"),
        pytest.param(".", id="current_dir"),
        pytest.param("as_exp.expid", id="valid_expid"),
    ]
)
def test_delete_expid(mocker, tmp_path, autosubmit_exp, autosubmit, expid_value):
    as_exp = autosubmit_exp(experiment_data=_get_experiment_data(tmp_path))
    mocker.patch('autosubmit.experiment.experiment_common._perform_deletion', return_value="error")
    expid_value = as_exp.expid if expid_value == "as_exp.expid" else expid_value
    if expid_value in ["..", "", "."]:
        with pytest.raises(AutosubmitCritical) as exc_info:
            _delete_expid(expid_value, force=True)
            assert exc_info.value.code == 7001
    else:
        with pytest.raises(AutosubmitError):
            _delete_expid(as_exp.expid, force=True)
    mocker.stopall()
    _delete_expid(as_exp.expid, force=True)
    with does_not_raise():
        _delete_expid(as_exp.expid, force=True)


def test_perform_deletion(mocker, tmp_path, autosubmit_exp, autosubmit):
    as_exp = autosubmit_exp(experiment_data=_get_experiment_data(tmp_path))
    mocker.patch("autosubmit.experiment.experiment_common.rmtree", side_effect=FileNotFoundError)
    basic_config = as_exp.as_conf.basic_config
    experiment_path = Path(basic_config.LOCAL_ROOT_DIR, as_exp.expid)
    structure_db_path = Path(basic_config.STRUCTURES_DIR, f'structure_{as_exp.expid}.db')
    job_data_db_path = Path(basic_config.JOBDATA_DIR, f'job_data_{as_exp.expid}')
    if all("tmp" not in path for path in [str(experiment_path), str(structure_db_path), str(job_data_db_path)]):
        raise AutosubmitCritical("tmp not in path")
    mocker.patch("autosubmit.database.db_common.delete_experiment", side_effect=FileNotFoundError)
    err_message = _perform_deletion(experiment_path, structure_db_path, job_data_db_path, as_exp.expid)
    assert all(x in err_message for x in ["Cannot delete experiment entry", "Cannot delete directory"])
