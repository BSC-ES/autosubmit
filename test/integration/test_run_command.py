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
from pathlib import Path
from typing import Callable, TYPE_CHECKING

import pytest
from ruamel.yaml import YAML

from autosubmit.log.log import AutosubmitCritical

if TYPE_CHECKING:
    from pytest_mock import MockerFixture
    from test.integration.conftest import AutosubmitExperiment, AutosubmitExperimentFixture


def set_up_test(
        expid: str,
        command: list[str],
        autosubmit_exp: 'AutosubmitExperimentFixture',
        mocker
) -> tuple['AutosubmitExperiment', argparse.Namespace, list[str]]:
    test_files_path = Path(__file__).resolve().parents[1]
    fake_jobs: dict = YAML().load(test_files_path / "files/fake-jobs.yml")
    fake_platforms: dict = YAML().load(test_files_path / "files/fake-platforms.yml")
    exp = autosubmit_exp(
        expid=expid,
        experiment_data={
            'DEFAULT': {
                'HPCARCH': 'TEST_SLURM'
            },
            **fake_jobs,
            **fake_platforms
        }
    )

    if 'delete' in command:
        mocker.patch('autosubmit.autosubmit.Autosubmit._user_yes_no_query', return_value=True)

    command = [c.format(expid=expid) for c in command]

    mocker.patch('sys.argv', command)
    _, args = exp.autosubmit.parse_args()
    return exp, args, command


@pytest.mark.parametrize(
    'command',
    [
        ['autosubmit', 'configure'],
        ['autosubmit', 'expid', '-dm', '-H', 'local', '-d', 'Tutorial'],
        ['autosubmit', 'delete', '{expid}'],
        ['autosubmit', 'monitor', '{expid}', '--hide', '--notransitive'],  # TODO
        ['autosubmit', 'stats', '{expid}'],  # TODO
        ['autosubmit', 'clean', '{expid}'],
        # ['autosubmit', 'check', '{expid}', '--notransitive'],
        ['autosubmit', 'inspect', '{expid}', '--notransitive'],  # TODO
        ['autosubmit', 'report', '{expid}'],  # TODO
        ['autosubmit', 'describe', '{expid}'],
        # ['autosubmit', 'migrate', '-fs', 'Any', '{expid}'],
        ['autosubmit', 'create', '{expid}', '--hide'],
        ['autosubmit', 'setstatus', '{expid}', '-t', 'READY', '-fs', 'WAITING', '--hide'],  # TODO
        ['autosubmit', 'testcase', '-dm', '-H', 'local', '-d', 'Tutorial', '-c', '1', '-m', 'fc0', '-s', '19651101'],
        # TODO
        ['autosubmit', 'refresh', '{expid}'],  # TODO
        ['autosubmit', 'updateversion', '{expid}'],  # TODO
        ['autosubmit', 'upgrade', '{expid}'],  # TODO
        ['autosubmit', 'archive', '{expid}'],  # TODO
        ['autosubmit', 'readme'],  # TODO
        ['autosubmit', 'changelog'],  # TODO
        ['autosubmit', 'dbfix', '{expid}'],  # TODO
        ['autosubmit', 'pklfix', '{expid}'],
        ['autosubmit', 'updatedescrip', '{expid}', 'description'],
        ['autosubmit', 'cat-log', '{expid}'],
        ['autosubmit', 'stop', '-a'],
        ['autosubmit', 'testcase', '-y', '{expid}', '-H', 'Marenostrum5', '-d', 'Tsuite MAIN - IFS-FESOM tco79, historical, version fesom_ensemble_gen, sdate 19900101, 2 monthly chunks with DQC, AQUA, TRANSFER, WIPE, SYNC_LRA (MN5)',],
    ],
    ids=[
        'configure',
        'expid',
        'delete',
        'monitor',
        'stats',
        'clean',
        'inspect',
        'report',
        'describe',
        # 'migrate',
        'create',
        'setstatus',
        'testcase',
        'refresh',
        'updateversion',
        'upgrade',
        'archive',
        'readme',
        'changelog',
        'dbfix',
        'pklfix',
        'updatedescrip',
        'cat-log',
        'stop',
        'testcase copy',
    ]
)
def test_run_command(
        command: list[str],
        autosubmit_exp: 'AutosubmitExperimentFixture',
        mocker: 'MockerFixture',
        get_next_expid: Callable[[], str]):
    """Test the is simply used to check if commands are not broken on runtime, it doesn't check behaviour or output

    TODO: improve quality of the test in order to validate each scenario and its outputs
    TODO: commands that have a TODO at its side needs behaviour tests
    """
    exp, args, command = set_up_test(get_next_expid(), command, autosubmit_exp, mocker)
    if 'create' in command or 'pklfix' in command:
        assert exp.autosubmit.run_command(args=args) == 0
    else:
        assert exp.autosubmit.run_command(args=args)


@pytest.mark.parametrize(
    'command',
    [
        ['autosubmit', 'install'],
        ['autosubmit', '-lc', 'ERROR', '-lf', 'WARNING', 'run', '{expid}'],
        ['autosubmit', 'recovery', '{expid}', '--hide'],
        ['autosubmit', 'provenance', '{expid}', '--rocrate'],
    ],
    ids=['install', 'run', 'recovery', 'provenance']
)
def test_run_command_raises_autosubmit(
        command: list[str],
        autosubmit_exp: 'AutosubmitExperimentFixture',
        mocker: 'MockerFixture',
        get_next_expid: Callable[[], str]):
    """Test the is simply used to check if commands are not broken on runtime.

    It doesn't check behaviour or output.
    """
    exp, args, command = set_up_test(get_next_expid(), command, autosubmit_exp, mocker)
    if 'run' in command:
        with pytest.raises(AutosubmitCritical) as error:
            exp.autosubmit.run_command(args=args)
        assert str(error.value.code) == '7010'
    elif 'install' in command:
        with pytest.raises(AutosubmitCritical) as error:
            exp.autosubmit.run_command(args=args)
        assert str(error.value.code) == '7004'
    elif 'recovery' in command:
        with pytest.raises(AutosubmitCritical) as error:
            exp.autosubmit.run_command(args=args)
        # Can't establish a connection to a platform.
        assert str(error.value.code) == '7050'
    elif 'provenance' in command:
        with pytest.raises(AutosubmitCritical) as error:
            exp.autosubmit.run_command(args=args)
        # RO-Crate key is missing
        assert str(error.value.code) == '7012'
