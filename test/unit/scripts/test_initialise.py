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

"""Unit tests for the ``autosubmit.scripts._initialise`` module."""

import locale

import pytest

# noinspection PyProtectedMember
from autosubmit.scripts._initialise import (
    initialise_command,
)


@pytest.mark.parametrize("command", ["describe", "unarchive"])
def test_initialise_command_skips_initialisation_commands(mocker, command):
    """Test that commands excluded from initialisation return."""
    set_locale = mocker.patch("autosubmit.scripts._initialise._set_locale")
    parse_expids = mocker.patch("autosubmit.scripts._initialise.parse_expids")

    opts = mocker.Mock(expid="a000")

    initialise_command(command, opts)

    set_locale.assert_called_once_with()
    parse_expids.assert_not_called()


@pytest.mark.parametrize("expid", [None, "", False])
def test_initialise_command_without_expid_returns(mocker, expid):
    """Test that initialisation stops when no experiment ID is provided."""
    set_locale = mocker.patch("autosubmit.scripts._initialise._set_locale")
    parse_expids = mocker.patch("autosubmit.scripts._initialise.parse_expids")

    opts = mocker.Mock()
    opts.expid = expid

    initialise_command("run", opts)

    set_locale.assert_called_once_with()
    parse_expids.assert_not_called()


def test_initialise_command(mocker):
    """Test normal command initialisation."""
    mocker.patch("autosubmit.scripts._initialise._set_locale")
    mocker.patch(
        "autosubmit.scripts._initialise.parse_expids",
        return_value=["a000"],
    )
    mocker.patch(
        "autosubmit.scripts._initialise.get_version",
        return_value="4.0.0",
    )

    config = mocker.Mock()
    config.experiment_data = {"config": "data"}
    config.get_version.return_value = "4.0.0"

    autosubmit_config = mocker.patch(
        "autosubmit.config.configcommon.AutosubmitConfig",
        return_value=config,
    )

    opts = mocker.Mock()
    opts.expid = "a000"
    opts.update_version = False

    initialise_command("run", opts)

    autosubmit_config.assert_called_once_with("a000")
    config.reload.assert_called_once_with(force_load=True)
    config.set_last_as_command.assert_called_once_with("run")


def test_initialise_command_multiple_expids(mocker):
    """Test that every experiment is initialised."""
    mocker.patch("autosubmit.scripts._initialise._set_locale")
    mocker.patch(
        "autosubmit.scripts._initialise.parse_expids",
        return_value=["a000", "a001"],
    )
    mocker.patch(
        "autosubmit.scripts._initialise.get_version",
        return_value="4.0.0",
    )

    config_a000 = mocker.Mock()
    config_a000.experiment_data = {"config": "data"}
    config_a000.get_version.return_value = "4.0.0"

    config_a001 = mocker.Mock()
    config_a001.experiment_data = {"config": "data"}
    config_a001.get_version.return_value = "4.0.0"

    autosubmit_config = mocker.patch(
        "autosubmit.config.configcommon.AutosubmitConfig",
        side_effect=[config_a000, config_a001],
    )

    opts = mocker.Mock(expid="a000,a001", update_version=False)

    initialise_command("run", opts)

    assert autosubmit_config.call_args_list == [
        mocker.call("a000"),
        mocker.call("a001"),
    ]
    config_a000.reload.assert_called_once_with(force_load=True)
    config_a001.reload.assert_called_once_with(force_load=True)
    config_a000.set_last_as_command.assert_called_once_with("run")
    config_a001.set_last_as_command.assert_called_once_with("run")


@pytest.mark.parametrize("command", ["expid", "upgrade"])
def test_initialise_command_allows_missing_yaml_for_special_commands(mocker, command):
    """Test that expid and upgrade do not require YAML data."""
    mocker.patch("autosubmit.scripts._initialise._set_locale")
    mocker.patch(
        "autosubmit.scripts._initialise.parse_expids",
        return_value=["a000"],
    )
    mocker.patch(
        "autosubmit.scripts._initialise.get_version",
        return_value="4.0.0",
    )

    config = mocker.Mock()
    config.experiment_data = None
    config.get_version.return_value = "4.0.0"

    autosubmit_config = mocker.patch(
        "autosubmit.config.configcommon.AutosubmitConfig",
        return_value=config,
    )

    opts = mocker.Mock(expid="a000", update_version=False)

    initialise_command(command, opts)

    autosubmit_config.assert_called_once_with("a000")
    config.set_last_as_command.assert_called_once_with(command)


def test_initialise_command_missing_yaml_exits(mocker):
    """Test that commands requiring YAML fail when configuration is empty."""
    mocker.patch("autosubmit.scripts._initialise._set_locale")
    mocker.patch(
        "autosubmit.scripts._initialise.parse_expids",
        return_value=["a000"],
    )
    mocker.patch(
        "autosubmit.scripts._initialise.get_version",
        return_value="4.0.0",
    )
    log_error = mocker.patch("autosubmit.scripts._initialise.Log.error")

    config = mocker.Mock()
    config.experiment_data = None

    mocker.patch(
        "autosubmit.config.configcommon.AutosubmitConfig",
        return_value=config,
    )

    opts = mocker.Mock(expid="a000", update_version=False)

    with pytest.raises(SystemExit) as exc_info:
        initialise_command("run", opts)

    assert exc_info.value.code == 1
    log_error.assert_called_once_with(
        "Experiment 'a000' contains no YAML configuration.\n"
        'Please upgrade it with: "autosubmit upgrade a000"'
    )

    config.set_last_as_command.assert_not_called()


def test_initialise_command_updates_experiment_version(mocker):
    """Test updating an experiment version when requested."""
    mocker.patch("autosubmit.scripts._initialise._set_locale")
    mocker.patch(
        "autosubmit.scripts._initialise.parse_expids",
        return_value=["a000"],
    )
    mocker.patch(
        "autosubmit.scripts._initialise.get_version",
        return_value="4.0.0",
    )

    config = mocker.Mock()
    config.experiment_data = {"config": "data"}
    config.get_version.return_value = "3.0.0"

    mocker.patch(
        "autosubmit.config.configcommon.AutosubmitConfig",
        return_value=config,
    )

    update_description = mocker.patch(
        "autosubmit.database.db_common.update_experiment_description_version"
    )
    log_info = mocker.patch("autosubmit.scripts._initialise.Log.info")

    opts = mocker.Mock(expid="a000", update_version=True)

    initialise_command("run", opts)

    config.set_version.assert_called_once_with("4.0.0")
    update_description.assert_called_once_with(
        "a000",
        version="4.0.0",
    )
    config.set_last_as_command.assert_called_once_with("run")

    assert any(
        "a000" in call.args[0] and "4.0.0" in call.args[0]
        for call in log_info.call_args_list
    )


def test_initialise_command_does_not_update_matching_version(mocker):
    """Test that an already matching version is not updated."""
    mocker.patch("autosubmit.scripts._initialise._set_locale")
    mocker.patch(
        "autosubmit.scripts._initialise.parse_expids",
        return_value=["a000"],
    )
    mocker.patch(
        "autosubmit.scripts._initialise.get_version",
        return_value="4.0.0",
    )

    config = mocker.Mock()
    config.experiment_data = {"config": "data"}
    config.get_version.return_value = "4.0.0"

    mocker.patch(
        "autosubmit.config.configcommon.AutosubmitConfig",
        return_value=config,
    )

    update_description = mocker.patch(
        "autosubmit.database.db_common.update_experiment_description_version"
    )

    opts = mocker.Mock(expid="a000", update_version=True)

    initialise_command("run", opts)

    config.set_version.assert_not_called()
    update_description.assert_not_called()
    config.set_last_as_command.assert_called_once_with("run")


def test_initialise_command_version_mismatch_exits(mocker):
    """Test that a version mismatch stops the command."""
    mocker.patch("autosubmit.scripts._initialise._set_locale")
    mocker.patch(
        "autosubmit.scripts._initialise.parse_expids",
        return_value=["a000"],
    )
    mocker.patch(
        "autosubmit.scripts._initialise.get_version",
        return_value="4.0.0",
    )
    log_error = mocker.patch("autosubmit.scripts._initialise.Log.error")

    config = mocker.Mock()
    config.experiment_data = {"config": "data"}
    config.get_version.return_value = "3.0.0"

    mocker.patch(
        "autosubmit.config.configcommon.AutosubmitConfig",
        return_value=config,
    )

    opts = mocker.Mock(expid="a000", update_version=False)

    with pytest.raises(SystemExit) as exc_info:
        initialise_command("run", opts)

    assert exc_info.value.code == 1

    message = log_error.call_args.args[0]
    assert "3.0.0" in message
    assert "4.0.0" in message
    assert "autosubmit updateversion a000" in message
    assert "autosubmit run a000 -v" in message

    config.set_last_as_command.assert_not_called()


def test_initialise_command_allows_missing_experiment_version(mocker):
    """Test that an experiment without a stored version is accepted."""
    mocker.patch("autosubmit.scripts._initialise._set_locale")
    mocker.patch(
        "autosubmit.scripts._initialise.parse_expids",
        return_value=["a000"],
    )
    mocker.patch(
        "autosubmit.scripts._initialise.get_version",
        return_value="4.0.0",
    )

    config = mocker.Mock()
    config.experiment_data = {"config": "data"}
    config.get_version.return_value = None

    mocker.patch(
        "autosubmit.config.configcommon.AutosubmitConfig",
        return_value=config,
    )

    opts = mocker.Mock(expid="a000", update_version=False)

    initialise_command("run", opts)

    config.set_last_as_command.assert_called_once_with("run")


@pytest.mark.parametrize(
    "command",
    ["archive", "delete", "upgrade", "updateversion"],
)
def test_initialise_command_does_not_update_non_updatable_commands(mocker, command):
    """Test that non-updatable commands skip version handling."""
    mocker.patch("autosubmit.scripts._initialise._set_locale")
    mocker.patch(
        "autosubmit.scripts._initialise.parse_expids",
        return_value=["a000"],
    )
    mocker.patch(
        "autosubmit.scripts._initialise.get_version",
        return_value="4.0.0",
    )

    config = mocker.Mock()
    config.experiment_data = {"config": "data"}
    config.get_version.return_value = "3.0.0"

    mocker.patch(
        "autosubmit.config.configcommon.AutosubmitConfig",
        return_value=config,
    )

    opts = mocker.Mock(expid="a000", update_version=True)

    initialise_command(command, opts)

    config.get_version.assert_not_called()
    config.set_version.assert_not_called()
    config.set_last_as_command.assert_called_once_with(command)


def test_initialise_command_sets_locale(mocker):
    """Test that initialisation sets the first available UTF-8 locale."""
    setlocale = mocker.patch(
        "autosubmit.scripts._initialise.locale.setlocale",
        return_value="C.UTF-8",
    )

    opts = mocker.Mock(expid=None)

    initialise_command("run", opts)

    assert setlocale.call_count == 1
    setlocale.assert_called_once_with(
        mocker.ANY,
        "C.UTF-8",
    )


def test_initialise_command_tries_multiple_locales(mocker):
    """Test that unavailable locales are skipped."""
    setlocale = mocker.patch(
        "autosubmit.scripts._initialise.locale.setlocale",
        side_effect=[
            locale.Error,
            locale.Error,
            "en_GB",
        ],
    )

    opts = mocker.Mock(expid=None)

    initialise_command("run", opts)

    assert setlocale.call_args_list == [
        mocker.call(mocker.ANY, "C.UTF-8"),
        mocker.call(mocker.ANY, "C.utf8"),
        mocker.call(mocker.ANY, "en_GB"),
    ]


def test_initialise_command_falls_back_to_c_locale(mocker):
    """Test fallback to the C locale when no UTF-8 locale is available."""
    setlocale = mocker.patch(
        "autosubmit.scripts._initialise.locale.setlocale",
        side_effect=[
            locale.Error,
            locale.Error,
            locale.Error,
            locale.Error,
            "C",
        ],
    )
    log_info = mocker.patch("autosubmit.scripts._initialise.Log.info")

    opts = mocker.Mock(expid=None)

    initialise_command("run", opts)

    assert setlocale.call_args_list == [
        mocker.call(mocker.ANY, "C.UTF-8"),
        mocker.call(mocker.ANY, "C.utf8"),
        mocker.call(mocker.ANY, "en_GB"),
        mocker.call(mocker.ANY, "es_ES"),
        mocker.call(mocker.ANY, "C"),
    ]

    log_info.assert_called_once_with("UTF-8 locale not found, using 'C' as fallback.")
