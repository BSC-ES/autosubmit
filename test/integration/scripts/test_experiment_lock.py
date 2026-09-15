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

"""Integration tests for the experiment lock held by ``@cli_function(lock=True)``."""

from pathlib import Path

from autosubmit.experiment.lock import experiment_lock
from autosubmit.log.log import AutosubmitCritical
from autosubmit.scripts.create import main as create_main


def _lock_file(exp) -> Path:
    return Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR) / exp.expid / "tmp" / "autosubmit.lock"


def test_create_removes_lock_file(autosubmit_exp):
    """Reproduce #3033: ``autosubmit create`` left ``tmp/autosubmit.lock`` behind."""
    exp = autosubmit_exp()

    assert create_main(exp.expid) == 0
    assert not _lock_file(exp).exists()


def test_lock_held_while_command_runs(autosubmit_exp, mocker):
    exp = autosubmit_exp()
    seen = []
    mocker.patch(
        "autosubmit.experiment.manage.create",
        side_effect=lambda *_, **__: seen.append(_lock_file(exp).exists()),
    )

    create_main(exp.expid)

    assert seen == [True]
    assert not _lock_file(exp).exists()


def test_lock_removed_when_command_raises(autosubmit_exp, mocker):
    exp = autosubmit_exp()
    mocker.patch(
        "autosubmit.experiment.manage.create",
        side_effect=AutosubmitCritical("boom", 7000),
    )

    assert create_main(exp.expid) != 0
    assert not _lock_file(exp).exists()


def test_command_blocked_while_lock_held(autosubmit_exp, mocker):
    exp = autosubmit_exp()
    create = mocker.patch("autosubmit.experiment.manage.create")

    with experiment_lock(exp.expid):
        assert create_main(exp.expid) == 1
        assert _lock_file(exp).exists()

    create.assert_not_called()
    assert not _lock_file(exp).exists()
