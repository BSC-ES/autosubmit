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
from shutil import rmtree
from typing import Callable, Optional

import pytest
from pytest_mock import MockerFixture
from ruamel.yaml import YAML

from autosubmit.autosubmit import Autosubmit


def _experiment_data(hpcarch: str = 'ARM') -> dict:
    """Build experiment data from the shared fake jobs/platforms fixtures."""
    files_dir = Path(__file__).resolve().parents[1] / "files"
    return {
        'DEFAULT': {'HPCARCH': hpcarch},
        **YAML().load(files_dir / "fake-jobs.yml"),
        **YAML().load(files_dir / "fake-platforms.yml"),
    }


def _location_lines(mocked_log) -> list:
    """The ``Location: ...`` lines emitted via ``Log.result``."""
    return [
        call.args[0]
        for call in mocked_log.result.mock_calls
        if call.args and call.args[0].startswith('Location: ')
    ]


@pytest.mark.parametrize(
    'expid_count,spaces,unknown',
    [
        (2, True, False),   # Valid expids, space-separated.
        (2, False, False),  # Valid expids, comma-separated.
        (1, True, False),   # A single expid.
        (None, True, True), # An expid not in the database.
        (0, True, True),    # Empty input.
    ]
)
def test_describe(
        expid_count: Optional[int],
        spaces: bool,
        unknown: bool,
        autosubmit_exp: Callable,
        mocker: MockerFixture,
        get_next_expid: Callable[[], str]) -> None:
    """``describe`` enumerates experiments from the database; expids not
    found there are reported via ``Log.warning`` and not described.
    """
    # describe reads the database; autosubmit_exp creates it on first call.
    autosubmit_exp(experiment_data=_experiment_data())

    input_list = ''
    exps = []
    if expid_count is None:
        input_list = 'zzzz'  # Valid format, not in the database.
    elif expid_count > 0:
        expids = [get_next_expid() for _ in range(expid_count)]
        exps = [autosubmit_exp(e, experiment_data=_experiment_data()) for e in expids]
        input_list = (' ' if spaces else ',').join(expids)

    mocked_log = mocker.patch('autosubmit.autosubmit.Log')
    Autosubmit.describe(input_experiment_list=input_list, get_from_user='')

    if unknown:
        assert not _location_lines(mocked_log)
    else:
        locations = _location_lines(mocked_log)
        for exp in exps:
            assert f'Location: {exp.exp_path}' in locations


def test_describe_unknown_expid_warns(
        autosubmit_exp: Callable, mocker: MockerFixture) -> None:
    """An expid not in the database is warned about and skipped (#1110)."""
    autosubmit_exp(experiment_data=_experiment_data())

    mocked_log = mocker.patch('autosubmit.autosubmit.Log')
    Autosubmit.describe(input_experiment_list='zzzz', get_from_user='')

    assert mocked_log.warning.called
    assert not _location_lines(mocked_log)


def test_describe_archived_experiment(
        autosubmit_exp: Callable,
        mocker: MockerFixture,
        get_next_expid: Callable[[], str]) -> None:
    """``describe`` falls back to the database snapshot when an
    experiment's files are missing, e.g. archived (#2717).
    """
    expid = get_next_expid()
    exp = autosubmit_exp(expid, experiment_data=_experiment_data())

    # NOTE: the snapshot fallback reads the `details` table. If
    # autosubmit_exp does not populate it, create the snapshot first:
    #   from autosubmit.experiment.detail_updater import ExperimentDetails
    #   ExperimentDetails(expid).save_update_details()

    rmtree(exp.exp_path)  # Simulate archiving: remove files, keep the DB row.

    mocked_log = mocker.patch('autosubmit.autosubmit.Log')
    Autosubmit.describe(input_experiment_list=expid, get_from_user='')

    assert mocked_log.info.called
    described = [
        call.args[0]
        for call in mocked_log.result.mock_calls
        if call.args and call.args[0].startswith('Describing ')
    ]
    assert f'Describing {expid}' in described


def test_run_command_describe(autosubmit_exp: Callable, autosubmit, mocker):
    """Run ``describe`` through ``Autosubmit.run_command`` to also exercise
    log initialization and log levels.

    `Ref <https://github.com/BSC-ES/autosubmit/issues/2412>`_.
    """
    exp = autosubmit_exp(experiment_data=_experiment_data(hpcarch='TEST_SLURM'))

    mocker.patch('sys.argv', ['autosubmit', '-lc', 'ERROR', '-lf', 'WARNING', 'describe', exp.expid])
    _, args = autosubmit.parse_args()
    output = autosubmit.run_command(args=args)

    assert getuser() == output[0]