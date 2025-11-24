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

from pathlib import Path

import pytest
from ruamel.yaml import YAML

from autosubmit.autosubmit import Autosubmit
from autosubmit.log.log import AutosubmitCritical
from test.integration.conftest import AutosubmitExperimentFixture

_EXPIDS = 'z000'

def set_up_test(autosubmit_exp, autosubmit, mocker, command: str):
    fake_jobs: dict = YAML().load(Path(__file__).resolve().parents[1] / "files/fake-jobs.yml")
    fake_platforms: dict = YAML().load(Path(__file__).resolve().parents[1] / "files/fake-platforms.yml")
    autosubmit_exp(
        _EXPIDS,
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

    mocker.patch('sys.argv', command)
    _, args = autosubmit.parse_args()
    return args


@pytest.mark.parametrize(
    'command',
    [
        ['autosubmit', 'configure'],
        ['autosubmit', 'expid', '-dm', '-H', 'local', '-d', 'Tutorial'],
        ['autosubmit', 'delete', _EXPIDS],
        ['autosubmit', 'monitor', _EXPIDS, '--hide', '--notransitive'],  # TODO
        ['autosubmit', 'stats', _EXPIDS],  # TODO
        ['autosubmit', 'clean', _EXPIDS],
        # ['autosubmit', 'check', _EXPIDS, '--notransitive'],
        ['autosubmit', 'inspect', _EXPIDS, '--notransitive'],  # TODO
        ['autosubmit', 'report', _EXPIDS],  # TODO
        ['autosubmit', 'describe', _EXPIDS],
        ['autosubmit', 'migrate', '-fs', 'Any', _EXPIDS],
        ['autosubmit', 'create', _EXPIDS, '--hide'],
        ['autosubmit', 'setstatus', _EXPIDS, '-t', 'READY', '-fs', 'WAITING', '--hide'], # TODO
        ['autosubmit', 'testcase', '-dm', '-H', 'local', '-d', 'Tutorial', '-c', '1', '-m', 'fc0', '-s', '19651101'],
        # TODO
        ['autosubmit', 'refresh', _EXPIDS],  # TODO
        ['autosubmit', 'updateversion', _EXPIDS],  # TODO
        ['autosubmit', 'upgrade', _EXPIDS],  # TODO
        ['autosubmit', 'archive', _EXPIDS],  # TODO
        ['autosubmit', 'readme'],  # TODO
        ['autosubmit', 'changelog'],  # TODO
        ['autosubmit', 'dbfix', _EXPIDS],  # TODO
        ['autosubmit', 'pklfix', _EXPIDS],
        ['autosubmit', 'updatedescrip', _EXPIDS, 'description'],
        ['autosubmit', 'cat-log', _EXPIDS],
        ['autosubmit', 'stop', '-a']
    ],
    ids=['configure', 'expid', 'delete', 'monitor', 'stats', 'clean', 'inspect', 'report', 'describe', 'migrate', 'create',
         'setstatus', 'testcase', 'refresh', 'updateversion', 'upgrade', 'archive', 'readme', 'changelog', 'dbfix', 'pklfix',
         'updatedescrip', 'cat-log', 'stop']
)  # TODO: improve quality of the test in order to validate each scenario and its outputs  #noqa
def test_run_command(autosubmit_exp: AutosubmitExperimentFixture, autosubmit: Autosubmit, mocker, command: str):
    """Test the is simply used to check if commands are not broken on runtime, it doesn't check behaviour or output
    TODO: commands that have a TODO at its side needs behaviour tests
    """
    args = set_up_test(autosubmit_exp, autosubmit, mocker, command)
    if 'create' in command or 'pklfix' in command:
        assert autosubmit.run_command(args=args) == 0
    else:
        assert autosubmit.run_command(args=args)


@pytest.mark.parametrize(
    'command',
    [
        ['autosubmit', 'install'],
        ['autosubmit', '-lc', 'ERROR', '-lf', 'WARNING', 'run', _EXPIDS],
        ['autosubmit', 'recovery', _EXPIDS, '--hide'],
        ['autosubmit', 'provenance', _EXPIDS, '--rocrate'],
    ],
    ids=['install', 'run', 'recovery', 'provenance']
)
def test_run_command_raises_autosubmit(autosubmit_exp: AutosubmitExperimentFixture, autosubmit: Autosubmit, mocker, command: str):
    """Test the is simply used to check if commands are not broken on runtime, it doesn't check behaviour or output
    """
    args = set_up_test(autosubmit_exp, autosubmit, mocker, command)
    if 'run' in command:
        with pytest.raises(AutosubmitCritical) as error:
            autosubmit.run_command(args=args)
        assert str(error.value.code) == '7010'
    elif 'install' in command:
        with pytest.raises(AutosubmitCritical) as error:
            autosubmit.run_command(args=args)
        assert str(error.value.code) == '7004'
    elif 'recovery' in command:
        with pytest.raises(AutosubmitCritical) as error:
            autosubmit.run_command(args=args)
        # Can't establish a connection to a platform.
        assert str(error.value.code) == '7050'
    elif 'provenance' in command:
        with pytest.raises(AutosubmitCritical) as error:
            autosubmit.run_command(args=args)
        # RO-Crate key is missing
        assert str(error.value.code) == '7012'
