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
        input_list = 'zzzz'  # An expid not registered in the database.
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


def test_describe_unknown_expids_emit_single_warning(
        autosubmit_exp: Callable,
        mocker: MockerFixture,
        get_next_expid: Callable[[], str]) -> None:
    """Multiple unknown expids are reported in one batched warning."""
    expid = get_next_expid()
    autosubmit_exp(expid, experiment_data=_experiment_data())

    mocked_log = mocker.patch('autosubmit.autosubmit.Log')
    Autosubmit.describe(input_experiment_list=f'zzzz,yyyy,{expid}', get_from_user='')

    assert mocked_log.warning.call_count == 1
    warning_msg = mocked_log.warning.call_args[0][0]
    assert 'zzzz' in warning_msg and 'yyyy' in warning_msg


def test_describe_archived_experiment(
        autosubmit_exp: Callable,
        mocker: MockerFixture,
        get_next_expid: Callable[[], str]) -> None:
    """``describe`` falls back to the database snapshot when an
    experiment's files are missing, e.g. archived (#2717).
    """
    expid = get_next_expid()
    exp = autosubmit_exp(expid, experiment_data=_experiment_data())
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
    assert args
    output = autosubmit.run_command(args=args)

    assert getuser() == output[0]


@pytest.mark.parametrize("user", ["", "*"])
def test_describe_current_user(
    user,
    autosubmit_exp,
    mocker,
):
    """Current user aliases resolve correctly."""
    exp = autosubmit_exp(experiment_data=_experiment_data())

    mocked_log = mocker.patch("autosubmit.autosubmit.Log")

    Autosubmit.describe(input_experiment_list=exp.expid, get_from_user=user)

    assert mocked_log.result.called
    assert f"Location: {exp.exp_path}" in _location_lines(mocked_log)


def test_describe_skip_other_user(autosubmit_exp, get_next_expid, mocker):
    """Experiments owned by another user are skipped."""
    exp = autosubmit_exp(experiment_data=_experiment_data())

    mocked_log = mocker.patch("autosubmit.autosubmit.Log")
    fake_owner = mocker.Mock()
    fake_owner.pw_name = "someone_else"
    mocker.patch("autosubmit.autosubmit.pwd.getpwuid", return_value=fake_owner)

    Autosubmit.describe(input_experiment_list=exp.expid, get_from_user="current_user")

    assert not _location_lines(mocked_log)


def test_describe_uid_without_user(autosubmit_exp, mocker):
    """UID without passwd entry falls back to numeric id."""
    exp = autosubmit_exp(experiment_data=_experiment_data())

    mocked_log = mocker.patch("autosubmit.autosubmit.Log")

    owner = mocker.Mock()
    owner.pw_name = "current"

    def fake(uid):
        if fake.calls == 0:
            fake.calls += 1
            return owner
        raise KeyError

    fake.calls = 0
    mocker.patch("autosubmit.autosubmit.pwd.getpwuid", side_effect=fake)

    Autosubmit.describe(input_experiment_list=exp.expid, get_from_user="current")
    mocked_log.warning.assert_any_call("The user does not exist anymore in the system, using id instead")


def test_describe_archived_without_snapshot(autosubmit_exp, mocker):
    """Archived experiments without a snapshot cannot be described."""
    exp = autosubmit_exp(experiment_data=_experiment_data())

    rmtree(exp.exp_path)

    details = mocker.patch("autosubmit.autosubmit.ExperimentDetails")
    details.return_value.get_details.return_value = None

    mocked_log = mocker.patch("autosubmit.autosubmit.Log")

    Autosubmit.describe(exp.expid)

    assert mocked_log.printlog.call_count == 1

    msg = mocked_log.printlog.call_args.args[0]

    assert "Could not describe the following experiments" in msg
    assert exp.expid in msg

    assert any(
        call.args[0].startswith(f"Failed to describe experiment {exp.expid}")
        for call in mocked_log.warning.mock_calls
    )


@pytest.mark.parametrize(
    "exception",
    [
        KeyError,
        TypeError,
    ],
)
def test_describe_ignore_owner_lookup_errors(
    exception,
    autosubmit_exp,
    mocker,
    monkeypatch,
):
    """Owner lookup failures are ignored."""
    exp = autosubmit_exp(experiment_data=_experiment_data())

    mocked_log = mocker.patch("autosubmit.autosubmit.Log")
    monkeypatch.setattr(
        "autosubmit.autosubmit.pwd.getpwuid",
        lambda *_: (_ for _ in ()).throw(exception),
    )

    Autosubmit.describe(input_experiment_list=exp.expid, get_from_user="some-user")
    assert f"Location: {exp.exp_path}" in _location_lines(mocked_log)


def test_describe_ignore_owner_stat_errors(autosubmit_exp, mocker, monkeypatch):
    """Missing folder ownership information does not prevent describing."""
    exp = autosubmit_exp(experiment_data=_experiment_data())
    mocked_log = mocker.patch("autosubmit.autosubmit.Log")
    monkeypatch.setattr(Path, "is_dir", lambda *_: True)

    original_stat = Path.stat

    def failing_stat(path: Path, *args, **kwargs):
        if path == exp.exp_path:
            raise OSError("Cannot stat experiment folder")
        return original_stat(path, *args, **kwargs)  # type: ignore

    monkeypatch.setattr(Path, "stat", failing_stat)

    Autosubmit.describe(
        input_experiment_list=exp.expid,
        get_from_user="some-user",
    )

    assert f"Location: {exp.exp_path}" in _location_lines(mocked_log)
    assert f"Location: {exp.exp_path}" in _location_lines(mocked_log)
