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

from getpass import getuser
from pathlib import Path
from typing import Callable

import pytest
from pytest_mock import MockerFixture
from ruamel.yaml import YAML

from autosubmit.autosubmit import Autosubmit

_EXPIDS = ['z000', 'z001']


@pytest.mark.parametrize(
    'input_experiment_list,get_from_user,not_described',
    [
        (' '.join(_EXPIDS), '', False),  # It accepts expids separated by spaces,
        (','.join(_EXPIDS), '', False),  # or by commas,
        (_EXPIDS[0], '', False),  # or a single experiment ID.
        ('zzzz', '', True),  # An expid that does not exist.
        ('', '', True),  # If nothing is provided.
    ]
)
def test_describe(
        input_experiment_list,
        get_from_user,
        not_described,
        autosubmit_exp: Callable,
        mocker: MockerFixture) -> None:
    mocked_log = mocker.patch('autosubmit.autosubmit.Log')

    expids = filter(lambda e: e in _EXPIDS, input_experiment_list.replace(',', ' ').split(' '))

    exps = []

    fake_jobs: dict = YAML().load(Path(__file__).resolve().parents[1] / "files/fake-jobs.yml")
    fake_platforms: dict = YAML().load(Path(__file__).resolve().parents[1] / "files/fake-platforms.yml")

    for expid in expids:
        exp = autosubmit_exp(
            expid,
            experiment_data={
                'DEFAULT': {
                    'HPCARCH': 'ARM'
                },
                **fake_jobs,
                **fake_platforms
            }
        )
        exps.append(exp)

    Autosubmit.describe(
        input_experiment_list=input_experiment_list,
        get_from_user=get_from_user
    )

    # Log.printlog is only called when an experiment is not described
    # TODO: We could re-design the class to make this behaviour clearer.
    assert mocked_log.printlog.call_count == (1 if not_described else 0)

    if exps and not not_described:
        location_lines = [
            line_tuple.args[0].format(line_tuple.args[1])
            for line_tuple in mocked_log.result.mock_calls
            if line_tuple[1][0].startswith('Location: ')
        ]

        assert len(location_lines) == len(exps)

        for exp in exps:
            assert f'Location: {exp.exp_path}' in location_lines


def test_run_command_describe(autosubmit_exp: Callable, autosubmit, mocker):
    """Test the ``describe`` command calling it from the ``Autosubmit.run_command``.

    ``sys.argv`` is mocked to return what ``argparse`` would parse for a command
    such as ``autosubmit -lc ERROR -lf WARNING describe z000``.
    
    This triggers the log initialization, and verifies that log levels work too,
    as documented.

    `Ref <https://github.com/BSC-ES/autosubmit/issues/2412>`_.
    """
    fake_jobs: dict = YAML().load(Path(__file__).resolve().parents[1] / "files/fake-jobs.yml")
    fake_platforms: dict = YAML().load(Path(__file__).resolve().parents[1] / "files/fake-platforms.yml")
    exp = autosubmit_exp(
        _EXPIDS[0],
        experiment_data={
            'DEFAULT': {
                'HPCARCH': 'ARM'
            },
            **fake_jobs,
            **fake_platforms
        }
    )

    mocker.patch('sys.argv', ['autosubmit', '-lc', 'ERROR', '-lf', 'WARNING', 'describe', exp.expid])
    _, args = autosubmit.parse_args()
    output = autosubmit.run_command(args=args)

    assert getuser() == output[0]
