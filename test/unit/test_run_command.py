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
from typing import Callable

import pytest
from ruamel.yaml import YAML

from autosubmit.autosubmit import Autosubmit

_EXPIDS = 'z000'


@pytest.mark.parametrize(
    'command',
    [
        # ['autosubmit', 'configure', ''],
        # ['autosubmit', 'install', ''],
        # ['autosubmit', '-lc', 'ERROR', '-lf', 'WARNING', 'run', _EXPIDS],
        ['autosubmit', 'expid', '-dm', '-H', 'local', '-d', 'Tutorial'],
        ['autosubmit', 'delete', _EXPIDS],
        ['autosubmit', 'monitor', _EXPIDS, '--hide', '--notransitive'],
        ['autosubmit', 'stats', _EXPIDS],
        ['autosubmit', 'clean', _EXPIDS],
        # ['autosubmit', 'recovery', _EXPIDS],
        # ['autosubmit', 'check', _EXPIDS, '--notransitive'],
        ['autosubmit', 'inspect', _EXPIDS, '--notransitive'],
        ['autosubmit', 'report', _EXPIDS],
        # ['autosubmit', 'describe', _EXPIDS],
        ['autosubmit', 'migrate', '-fs', 'Any', _EXPIDS],
        ['autosubmit', 'create', _EXPIDS, '--hide'],
        # ['autosubmit', 'configure', _EXPIDS],
        # ['autosubmit', 'install', _EXPIDS],
        # ['autosubmit', 'setstatus', _EXPIDS],
        ['autosubmit', 'testcase', '-dm', '-H', 'local', '-d', 'Tutorial', '-c', '1', '-m', 'fc0', '-s', '19651101'],
        ['autosubmit', 'refresh', _EXPIDS],
        ['autosubmit', 'updateversion', _EXPIDS],
        ['autosubmit', 'upgrade', _EXPIDS],
        # ['autosubmit', 'provenance', _EXPIDS, '--rocrate'],
        ['autosubmit', 'archive', _EXPIDS],
        ['autosubmit', 'readme'],
        ['autosubmit', 'changelog'],
        ['autosubmit', 'dbfix', _EXPIDS],
        ['autosubmit', 'pklfix', _EXPIDS],
        ['autosubmit', 'updatedescrip', _EXPIDS, 'description'],
        ['autosubmit', 'cat-log', _EXPIDS],
        # ['autosubmit', 'stop', _EXPIDS]
    ],
    ids=['expid', 'delete', 'monitor', 'stats', 'clean', 'inspect', 'report', 'migrate', 'create',
         'testcase', 'refresh', 'updateversion', 'upgrade', 'archive', 'readme', 'changelog', 'dbfix', 'pklfix',
         'updatedescrip', 'cat-log']
)  # TODO: improve quality of the test in order to validate each scenario and its outputs  #noqa
def test_run_command(autosubmit_exp: Callable, autosubmit: Autosubmit, mocker, command: str):
    """Test the is simply used to check if functions are not broken in a running way, it doesn't check behaviour or output
    """
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
    autosubmit.run_command(args=args)
