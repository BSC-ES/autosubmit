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

"""Unit tests for the ``create`` subcommand."""

import pytest

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.log.log import AutosubmitCritical
from autosubmit.experiment.manage import create

_EXPID = "t000"
"""Test experiment ID."""


def test_create_git_clone_disables_remote_git_on_platform_error(mocker, tmp_path):
    """Test that when an ``AutosubmitCritical`` happens when copying git code, it uses ``LOCAL`` platform."""
    mocker.patch.object(BasicConfig, "LOCAL_ROOT_DIR", str(tmp_path))
    mocker.patch("autosubmit.scripts._validation.check_ownership")

    lock = mocker.patch("autosubmit.experiment.manage.Lock")
    lock.return_value.__enter__.return_value.flush = mocker.Mock()
    lock.return_value.__enter__.return_value.fileno.return_value = 1

    as_conf = mocker.Mock()
    as_conf.get_project_type.return_value = "git"
    as_conf.get_project_destination.return_value = "dest"
    as_conf.get_platform.return_value = "platform"

    as_conf.check_conf_files.side_effect = AutosubmitCritical("stop test", 7014)

    mocker.patch("autosubmit.experiment.manage.AutosubmitConfig", return_value=as_conf)
    as_conf.reload.return_value = None

    mocker.patch(
        "autosubmit.experiment.manage.ParamikoSubmitter",
        side_effect=AutosubmitCritical("platform error", 6000),
    )
    clone = mocker.patch("autosubmit.experiment.manage.clone_repository", return_value=True)

    with pytest.raises(AutosubmitCritical, match="stop test"):
        create(_EXPID, noplot=True, hide=True)

    clone.assert_called_once_with(as_conf, False)


def test_create_git_clone_disables_remote_git_on_missing_platform(mocker, tmp_path):
    """Test that when a ``KeyError`` happens when copying git code, it uses ``LOCAL`` platform."""
    mocker.patch.object(BasicConfig, "LOCAL_ROOT_DIR", str(tmp_path))
    mocker.patch("autosubmit.scripts._validation.check_ownership")

    lock = mocker.patch("autosubmit.experiment.manage.Lock")
    lock.return_value.__enter__.return_value.flush = mocker.Mock()
    lock.return_value.__enter__.return_value.fileno.return_value = 1

    as_conf = mocker.Mock()
    as_conf.get_project_type.return_value = "git"
    as_conf.get_project_destination.return_value = "dest"
    as_conf.get_platform.return_value = "platform"

    as_conf.check_conf_files.side_effect = AutosubmitCritical("stop test", 7014)

    mocker.patch("autosubmit.experiment.manage.AutosubmitConfig", return_value=as_conf)

    as_conf.reload.return_value = None

    submitter = mocker.Mock()
    submitter.platforms = {}

    mocker.patch("autosubmit.experiment.manage.ParamikoSubmitter", return_value=submitter)
    clone = mocker.patch("autosubmit.experiment.manage.clone_repository", return_value=True)

    with pytest.raises(AutosubmitCritical, match="stop test"):
        create(_EXPID, noplot=True, hide=True)

    clone.assert_called_once_with(as_conf, False)
