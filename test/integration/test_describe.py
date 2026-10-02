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

from collections.abc import Callable
from pathlib import Path
from shutil import rmtree

import pytest
from pytest_mock import MockerFixture
from ruamel.yaml import YAML

from autosubmit.experiment.manage import describe
from autosubmit.scripts.describe import main


def _experiment_data(hpcarch: str = "ARM") -> dict:
    """Build experiment data from the shared fake jobs/platforms fixtures."""
    files_dir = Path(__file__).resolve().parents[1] / "files"
    return {
        "DEFAULT": {"HPCARCH": hpcarch},
        **YAML().load(files_dir / "fake-jobs.yml"),
        **YAML().load(files_dir / "fake-platforms.yml"),
    }


def _location_lines(mocked_log) -> list[str]:
    return [
        call.args[0].split(":", 1)[1].strip()
        for call in mocked_log.info.mock_calls
        if call.args and call.args[0].lstrip().startswith("Location:")
    ]


@pytest.mark.parametrize(
    "expid_count,spaces,unknown",
    [
        (2, True, False),  # Valid expids, space-separated.
        (2, False, False),  # Valid expids, comma-separated.
        (1, True, False),  # A single expid.
        (None, True, True),  # An expid not in the database.
        (0, True, True),  # Empty input.
    ],
)
def test_describe(
    expid_count: int | None,
    spaces: bool,
    unknown: bool,
    autosubmit_exp: Callable,
    mocker: MockerFixture,
    get_next_expid: Callable[[], str],
) -> None:
    """``describe`` enumerates experiments from the database; expids not
    found there are reported via ``Log.warning`` and not described.
    """
    # describe reads the database; autosubmit_exp creates it on first call.
    autosubmit_exp(experiment_data=_experiment_data())

    input_list = ""
    exps = []
    if expid_count is None:
        input_list = "zzzz"  # An expid not registered in the database.
    elif expid_count > 0:
        expids = [get_next_expid() for _ in range(expid_count)]
        exps = [autosubmit_exp(e, experiment_data=_experiment_data()) for e in expids]
        input_list = (" " if spaces else ",").join(expids)

    manage_log = mocker.patch("autosubmit.experiment.manage.Log")
    describe_log = mocker.patch("autosubmit.experiment.describe.Log")

    describe(input_experiment_list=input_list, get_from_user="")

    if unknown:
        assert not _location_lines(manage_log)
    else:
        locations = _location_lines(describe_log)
        for exp in exps:
            assert f"{str(exp.exp_path)}" in locations


def test_describe_unknown_expid_warns(
    autosubmit_exp: Callable, mocker: MockerFixture
) -> None:
    """An expid not in the database is warned about and skipped (#1110)."""
    autosubmit_exp(experiment_data=_experiment_data())

    mocked_log = mocker.patch("autosubmit.experiment.manage.Log")
    describe(input_experiment_list="zzzz", get_from_user="")

    assert mocked_log.warning.called
    assert not _location_lines(mocked_log)


def test_describe_unknown_expids_emit_single_warning(
    autosubmit_exp: Callable, mocker: MockerFixture, get_next_expid: Callable[[], str]
) -> None:
    """Multiple unknown expids are reported in one batched warning."""
    expid = get_next_expid()
    autosubmit_exp(expid, experiment_data=_experiment_data())

    mocked_log = mocker.patch("autosubmit.experiment.manage.Log")
    describe(input_experiment_list=f"zzzz,yyyy,{expid}", get_from_user="")

    assert mocked_log.warning.call_count == 1
    warning_msg = mocked_log.warning.call_args[0][0]
    assert "zzzz" in warning_msg
    assert "yyyy" in warning_msg


def test_describe_archived_experiment(autosubmit_exp: Callable) -> None:
    """``describe`` falls back to the database snapshot when an
    experiment's files are missing, e.g. archived (#2717).
    """
    exp = autosubmit_exp(experiment_data=_experiment_data())
    rmtree(exp.exp_path)  # Simulate archiving: remove files, keep the DB row.

    assert describe(input_experiment_list=exp.expid, get_from_user="")


def test_run_command_describe(autosubmit_exp: Callable, mocker):
    """Run ``describe`` through ``Autosubmit.run_command`` to also exercise
    log initialisation and log levels.

    `Ref <https://github.com/BSC-ES/autosubmit/issues/2412>`_.
    """
    exp = autosubmit_exp(experiment_data=_experiment_data(hpcarch="TEST_SLURM"))

    mocked_describe = mocker.patch(
        "autosubmit.experiment.manage.describe",
        return_value=object(),
    )

    result = main(exp.expid, "--user", "kinow")  # type: ignore

    assert result == 0
    mocked_describe.assert_called_once_with(exp.expid, "kinow")


@pytest.mark.parametrize("user", ["", "*"])
def test_describe_current_user(user, autosubmit_exp):
    """Current user aliases resolve correctly."""
    exp = autosubmit_exp(experiment_data=_experiment_data())

    assert describe(input_experiment_list=exp.expid, get_from_user=user)


def test_describe_skip_other_user(autosubmit_exp, get_next_expid, mocker):
    """Experiments owned by another user are skipped."""
    exp = autosubmit_exp(experiment_data=_experiment_data())

    mocked_log = mocker.patch("autosubmit.experiment.describe.Log")
    fake_owner = mocker.Mock()
    fake_owner.pw_name = "someone_else"
    mocker.patch("autosubmit.experiment.utils.pwd.getpwuid", return_value=fake_owner)

    describe(input_experiment_list=exp.expid, get_from_user="current_user")

    assert not _location_lines(mocked_log)


def test_describe_uid_without_user(autosubmit_exp, mocker, tmp_path):
    """UID without passwd entry falls back to numeric id."""
    exp = autosubmit_exp(experiment_data=_experiment_data())

    mocked_log = mocker.patch("autosubmit.experiment.utils.Log")
    mocker.patch(
        "autosubmit.experiment.describe.get_experiment_ids",
        return_value=([exp.expid], []),
    )
    mock_conf = mocker.MagicMock()
    mocker.patch(
        "autosubmit.experiment.describe.AutosubmitConfig",
        return_value=mock_conf,
    )
    mock_conf.conf_folder_yaml = str(tmp_path)
    owner_uid = exp.exp_path.stat().st_uid

    mocker.patch(
        "autosubmit.experiment.utils.pwd.getpwuid",
        side_effect=KeyError,
    )

    result = describe(
        input_experiment_list=exp.expid,
        get_from_user="current",
    )

    assert result[0].user == str(owner_uid)
    mocked_log.warning.assert_any_call(
        f"Current owner of experiment {exp.expid} could not be retrieved. "
        "The owner is no longer in the system database."
    )


def test_describe_archived_without_snapshot(autosubmit_exp, mocker):
    """Archived experiments without a snapshot cannot be described."""
    exp = autosubmit_exp(experiment_data=_experiment_data())

    rmtree(exp.exp_path)

    details = mocker.patch("autosubmit.experiment.describe.ExperimentDetails")
    details.return_value.get_details.return_value = None

    mocked_log = mocker.patch("autosubmit.experiment.manage.Log")

    describe(exp.expid)

    assert mocked_log.warning.call_count == 1

    msg = mocked_log.warning.call_args.args[0]

    assert exp.expid in msg

    assert any(
        call.args[0].startswith(f"Failed to describe experiment {exp.expid}")
        for call in mocked_log.warning.mock_calls
    )
