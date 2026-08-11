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


"""Unit tests for the ``autosubmit.scripts._validation`` module."""

import os
from pathlib import Path

import pytest

# noinspection PyProtectedMember
import autosubmit.scripts._validation as validators
from autosubmit.log.log import AutosubmitCritical

# noinspection PyProtectedMember
from autosubmit.scripts._args import DefaultOptions


def test_get_host_restrictions():
    """Test _get_host_restrictions."""
    assert validators._get_host_restrictions("", "clean") == []
    assert validators._get_host_restrictions({}, "clean") == []
    assert validators._get_host_restrictions(
        {"clean": ["truck", "all"]},
        "clean",
    ) == ["truck", "all"]
    assert (
        validators._get_host_restrictions(
            {"run": ["truck"]},
            "clean",
        )
        == []
    )


@pytest.mark.parametrize(
    "restrictions,command,expected",
    [
        ("", "clean", []),
        ({}, "clean", []),
        ({"clean": ["truck", "all"]}, "clean", ["truck", "all"]),
        ({"run": ["truck"]}, "clean", []),
        ({"clean": []}, "clean", []),
    ],
    ids=[
        "legacy-empty-string",
        "empty-dict",
        "matching-command",
        "different-command",
        "empty-restrictions",
    ],
)
def test_get_host_restrictions_parametrized(
    restrictions,
    command,
    expected,
):
    """Test _get_host_restrictions with different configurations."""
    assert (
        validators._get_host_restrictions(
            restrictions,
            command,
        )
        == expected
    )


def test_validate_host_prohibited_commands_allowed(mocker):
    """Test a command allowed on the current host."""
    mocker.patch(
        "autosubmit.scripts._validation.platform.node",
        return_value="truck.bsc.es",
    )
    mocker.patch.object(
        validators.BasicConfig,
        "DENIED_HOSTS",
        {},
    )
    mocker.patch.object(
        validators.BasicConfig,
        "ALLOWED_HOSTS",
        {},
    )
    log_error = mocker.patch(
        "autosubmit.scripts._validation.Log.error",
    )

    validators.validate_host_prohibited_commands(
        "clean",
        mocker.Mock(),
    )

    log_error.assert_not_called()


def test_validate_host_prohibited_commands_denied_host(mocker):
    """Test a command explicitly denied on a host."""
    mocker.patch(
        "autosubmit.scripts._validation.platform.node",
        return_value="truck.bsc.es",
    )
    mocker.patch.object(
        validators.BasicConfig,
        "DENIED_HOSTS",
        {"clean": ["truck"]},
    )
    mocker.patch.object(
        validators.BasicConfig,
        "ALLOWED_HOSTS",
        {},
    )
    log_error = mocker.patch(
        "autosubmit.scripts._validation.Log.error",
    )

    with pytest.raises(SystemExit) as error:
        validators.validate_host_prohibited_commands(
            "clean",
            mocker.Mock(),
        )

    assert error.value.code == 1
    log_error.assert_called_once_with(
        "Command 'clean' is not allowed on host 'truck'.\n"
        "The command is explicitly denied on this host.",
    )


def test_validate_host_prohibited_commands_denied_fqdn(mocker):
    """Test a command denied using the FQDN."""
    mocker.patch(
        "autosubmit.scripts._validation.platform.node",
        return_value="truck.bsc.es",
    )
    mocker.patch.object(
        validators.BasicConfig,
        "DENIED_HOSTS",
        {"clean": ["truck.bsc.es"]},
    )
    mocker.patch.object(
        validators.BasicConfig,
        "ALLOWED_HOSTS",
        {},
    )

    with pytest.raises(SystemExit) as error:
        validators.validate_host_prohibited_commands(
            "clean",
            mocker.Mock(),
        )

    assert error.value.code == 1


def test_validate_host_prohibited_commands_denied_all(mocker):
    """Test a command denied using the 'all' restriction."""
    mocker.patch(
        "autosubmit.scripts._validation.platform.node",
        return_value="truck.bsc.es",
    )
    mocker.patch.object(
        validators.BasicConfig,
        "DENIED_HOSTS",
        {"clean": ["all"]},
    )
    mocker.patch.object(
        validators.BasicConfig,
        "ALLOWED_HOSTS",
        {},
    )

    with pytest.raises(SystemExit) as error:
        validators.validate_host_prohibited_commands(
            "clean",
            mocker.Mock(),
        )

    assert error.value.code == 1


def test_validate_host_prohibited_commands_not_allowed(mocker):
    """Test a command whose host is not in the allowed hosts."""
    mocker.patch(
        "autosubmit.scripts._validation.platform.node",
        return_value="truck.bsc.es",
    )
    mocker.patch.object(
        validators.BasicConfig,
        "DENIED_HOSTS",
        {},
    )
    mocker.patch.object(
        validators.BasicConfig,
        "ALLOWED_HOSTS",
        {"clean": ["other-host"]},
    )
    log_error = mocker.patch(
        "autosubmit.scripts._validation.Log.error",
    )

    with pytest.raises(SystemExit) as error:
        validators.validate_host_prohibited_commands(
            "clean",
            mocker.Mock(),
        )

    assert error.value.code == 1
    log_error.assert_called_once_with(
        "Command 'clean' is not allowed on host 'truck'.\nAllowed hosts: other-host.",
    )


def test_validate_host_prohibited_commands_allowed_all(mocker):
    """Test that 'all' in allowed hosts allows the command."""
    mocker.patch(
        "autosubmit.scripts._validation.platform.node",
        return_value="truck.bsc.es",
    )
    mocker.patch.object(
        validators.BasicConfig,
        "DENIED_HOSTS",
        {},
    )
    mocker.patch.object(
        validators.BasicConfig,
        "ALLOWED_HOSTS",
        {"clean": ["all"]},
    )
    log_error = mocker.patch(
        "autosubmit.scripts._validation.Log.error",
    )

    validators.validate_host_prohibited_commands(
        "clean",
        mocker.Mock(),
    )

    log_error.assert_not_called()


def test_validate_host_prohibited_commands_warning(mocker):
    """Test host restrictions emit a deprecation warning."""
    mocker.patch(
        "autosubmit.scripts._validation.platform.node",
        return_value="truck.bsc.es",
    )
    mocker.patch.object(
        validators.BasicConfig,
        "DENIED_HOSTS",
        {"clean": ["truck"]},
    )
    mocker.patch.object(
        validators.BasicConfig,
        "ALLOWED_HOSTS",
        {},
    )

    with pytest.warns(
        FutureWarning,
        match="Host-based command restrictions",
    ):
        with pytest.raises(SystemExit):
            validators.validate_host_prohibited_commands(
                "clean",
                mocker.Mock(),
            )


@pytest.mark.parametrize(
    "command,expid",
    [
        ("stop", None),
        ("describe", "*"),
    ],
    ids=[
        "stop-without-expid",
        "describe-all-experiments",
    ],
)
def test_validate_expid_special_cases(mocker, command, expid):
    """Test commands that don't require a normal experiment ID."""
    opts = mocker.Mock()
    opts.expid = expid

    validators.validate_expid(command, opts)


def test_validate_expid_multiple_not_accepted(mocker):
    """Test multiple experiment IDs when the command does not accept them."""
    opts = mocker.Mock()
    opts.expid = "a001 b001"
    opts.accepts_multiple_expids = False

    log_error = mocker.patch(
        "autosubmit.scripts._validation.Log.error",
    )

    with pytest.raises(SystemExit) as error:
        validators.validate_expid("run", opts)

    assert error.value.code == 1
    log_error.assert_called_once_with(
        "The command 'run' does not accept multiple experiment IDs.",
    )


def test_validate_expid_invalid_expid(mocker):
    """Test an invalid experiment ID."""
    opts = mocker.Mock()
    opts.expid = "abc"
    opts.accepts_multiple_expids = False

    mocker.patch(
        "autosubmit.scripts._validation.is_valid_experiment_id",
        return_value=False,
    )
    log_error = mocker.patch(
        "autosubmit.scripts._validation.Log.error",
    )

    with pytest.raises(SystemExit) as error:
        validators.validate_expid("run", opts)

    assert error.value.code == 1
    log_error.assert_called_once_with(
        "Invalid experiment ID: 'abc'",
    )


def test_validate_expid_not_found_sqlite(mocker):
    """Test a missing experiment using SQLite."""
    opts = mocker.Mock()
    opts.expid = "a001"
    opts.accepts_multiple_expids = False

    mocker.patch(
        "autosubmit.scripts._validation.is_valid_experiment_id",
        return_value=True,
    )
    mocker.patch(
        "autosubmit.scripts._validation.experiment_exists",
        return_value=False,
    )
    mocker.patch.object(
        validators.BasicConfig,
        "DATABASE_BACKEND",
        "sqlite",
    )

    create_folders = mocker.patch(
        "autosubmit.scripts._validation.create_required_folders",
    )
    log_error = mocker.patch(
        "autosubmit.scripts._validation.Log.error",
    )

    with pytest.raises(SystemExit) as error:
        validators.validate_expid("run", opts)

    assert error.value.code == 1
    create_folders.assert_not_called()
    log_error.assert_called_once_with(
        "Experiment 'a001' was not found.",
    )


def test_validate_expid_not_found_non_sqlite(mocker):
    """Test a missing experiment using a non-SQLite backend."""
    opts = mocker.Mock()
    opts.expid = "a001"
    opts.accepts_multiple_expids = False

    mocker.patch(
        "autosubmit.scripts._validation.is_valid_experiment_id",
        return_value=True,
    )
    mocker.patch(
        "autosubmit.scripts._validation.experiment_exists",
        return_value=False,
    )
    mocker.patch.object(
        validators.BasicConfig,
        "DATABASE_BACKEND",
        "postgresql",
    )
    mocker.patch.object(
        validators.BasicConfig,
        "LOCAL_ROOT_DIR",
        "/tmp/autosubmit",
    )

    create_folders = mocker.patch(
        "autosubmit.scripts._validation.create_required_folders",
    )

    with pytest.raises(SystemExit) as error:
        validators.validate_expid("run", opts)

    assert error.value.code == 1
    create_folders.assert_called_once_with(
        "a001",
        Path("/tmp/autosubmit", "a001"),
    )


def test_validate_expid_different_owner(mocker):
    """Test an experiment owned by a different user."""
    opts = mocker.Mock()
    opts.expid = "a001"
    opts.accepts_multiple_expids = False
    opts.accepts_other_users = False

    mocker.patch(
        "autosubmit.scripts._validation.is_valid_experiment_id",
        return_value=True,
    )
    mocker.patch(
        "autosubmit.scripts._validation.experiment_exists",
        return_value=True,
    )
    mocker.patch(
        "autosubmit.scripts._validation.check_ownership",
        side_effect=AutosubmitCritical("not owner"),
    )
    log_error = mocker.patch(
        "autosubmit.scripts._validation.Log.error",
    )

    with pytest.raises(SystemExit) as error:
        validators.validate_expid("run", opts)

    assert error.value.code == 1
    log_error.assert_called_once_with(
        "Experiment 'a001' is owned by a different user: not owner\nError code: 7000",
    )


def test_validate_expid_valid(mocker):
    """Test a valid experiment ID owned by the current user.

    :param mocker: Pytest mocker fixture.
    """
    opts = mocker.Mock()
    opts.expid = "a001"
    opts.accepts_multiple_expids = False
    opts.accepts_other_users = False

    mocker.patch(
        "autosubmit.scripts._validation.is_valid_experiment_id",
        return_value=True,
    )
    mocker.patch(
        "autosubmit.scripts._validation.experiment_exists",
        return_value=True,
    )
    check_ownership = mocker.patch(
        "autosubmit.scripts._validation.check_ownership",
    )
    log_debug = mocker.patch(
        "autosubmit.scripts._validation.Log.debug",
    )

    validators.validate_expid("run", opts)

    check_ownership.assert_called_once_with("a001")
    log_debug.assert_not_called()


def test_validate_expid_multiple_valid(mocker):
    """Test multiple valid experiment IDs."""
    opts = mocker.Mock()
    opts.expid = "a001 b001"
    opts.accepts_multiple_expids = True
    opts.accepts_other_users = False

    mocker.patch(
        "autosubmit.scripts._validation.is_valid_experiment_id",
        return_value=True,
    )
    mocker.patch(
        "autosubmit.scripts._validation.experiment_exists",
        return_value=True,
    )
    check_ownership = mocker.patch(
        "autosubmit.scripts._validation.check_ownership",
    )

    validators.validate_expid("run", opts)

    assert check_ownership.call_count == 2
    check_ownership.assert_any_call("a001")
    check_ownership.assert_any_call("b001")


@pytest.mark.parametrize(
    "description,hpc,error_message",
    [
        (
            None,
            "Marenostrum",
            "You must provide an experiment description (-d/--description)",
        ),
        (
            "",
            "Marenostrum",
            "You must provide an experiment description (-d/--description)",
        ),
        (
            "   ",
            "Marenostrum",
            "You must provide an experiment description (-d/--description)",
        ),
        (
            "My experiment",
            None,
            "You must provide an HPC (-H/--HPC)",
        ),
        (
            "My experiment",
            "",
            "You must provide an HPC (-H/--HPC)",
        ),
        (
            "My experiment",
            "   ",
            "You must provide an HPC (-H/--HPC)",
        ),
    ],
    ids=[
        "missing-description",
        "empty-description",
        "blank-description",
        "missing-hpc",
        "empty-hpc",
        "blank-hpc",
    ],
)
def test_validate_expid_required_args_missing(
    mocker,
    description,
    hpc,
    error_message,
):
    """Test missing experiment creation arguments."""
    opts = mocker.Mock()
    opts.description = description
    opts.HPC = hpc

    log_error = mocker.patch(
        "autosubmit.scripts._validation.Log.error",
    )

    with pytest.raises(SystemExit) as error:
        validators.validate_expid_required_args("create", opts)

    assert error.value.code == 1
    log_error.assert_called_once_with(error_message)


def test_validate_expid_required_args_valid(mocker):
    """Test valid experiment creation arguments."""
    opts = mocker.Mock()
    opts.description = "My experiment"
    opts.HPC = "Marenostrum"

    log_error = mocker.patch(
        "autosubmit.scripts._validation.Log.error",
    )

    validators.validate_expid_required_args("create", opts)

    log_error.assert_not_called()


def test_validate_required_files_configure():
    """Test configure does not require the autosubmitrc file."""
    validators.validate_required_files("configure", object())  # type: ignore


def test_validate_required_files_missing_config(mocker):
    """Test missing autosubmitrc."""
    mocker.patch.object(
        validators.BasicConfig,
        "CONFIG_FILE_FOUND",
        False,
    )
    log_error = mocker.patch(
        "autosubmit.scripts._validation.Log.error",
    )

    with pytest.raises(SystemExit) as error:
        validators.validate_required_files("run", mocker.Mock())

    assert error.value.code == 1
    log_error.assert_called_once_with(
        'Autosubmit configuration file "autosubmitrc" not found. '
        'Please run "autosubmit configure" to create it.',
    )


def test_validate_required_files_install(mocker):
    """Test ``install`` does not require an existing database."""
    mocker.patch.object(
        validators.BasicConfig,
        "CONFIG_FILE_FOUND",
        True,
    )

    validators.validate_required_files("install", mocker.Mock())


def test_validate_required_files_non_sqlite(mocker):
    """Test non-SQLite backends do not require a local database."""
    mocker.patch.object(
        validators.BasicConfig,
        "CONFIG_FILE_FOUND",
        True,
    )
    mocker.patch.object(
        validators.BasicConfig,
        "DATABASE_BACKEND",
        "postgresql",
    )

    validators.validate_required_files("run", mocker.Mock())


def test_validate_required_files_missing_database(mocker, tmp_path):
    """Test a missing SQLite database."""
    mocker.patch.object(
        validators.BasicConfig,
        "CONFIG_FILE_FOUND",
        True,
    )
    mocker.patch.object(
        validators.BasicConfig,
        "DATABASE_BACKEND",
        "sqlite",
    )
    mocker.patch.object(
        validators.BasicConfig,
        "DB_PATH",
        tmp_path / "missing.db",
    )
    log_error = mocker.patch(
        "autosubmit.scripts._validation.Log.error",
    )

    with pytest.raises(SystemExit) as error:
        validators.validate_required_files("run", mocker.Mock())

    assert error.value.code == 1
    log_error.assert_called_once_with(
        'Experiments database not found. Please run "autosubmit install" to create it.',
    )


def test_validate_required_files_database_readable_and_writable(
    mocker,
    tmp_path,
):
    """Test a readable and writable SQLite database."""
    db_path = tmp_path / "autosubmit.db"
    db_path.touch()

    mocker.patch.object(
        validators.BasicConfig,
        "CONFIG_FILE_FOUND",
        True,
    )
    mocker.patch.object(
        validators.BasicConfig,
        "DATABASE_BACKEND",
        "sqlite",
    )
    mocker.patch.object(
        validators.BasicConfig,
        "DB_PATH",
        db_path,
    )
    mocker.patch(
        "autosubmit.scripts._validation.os.access",
        return_value=True,
    )

    validators.validate_required_files("run", mocker.Mock())


def test_validate_required_files_database_not_readable(mocker, tmp_path):
    """Test a SQLite database that is not readable."""
    db_path = tmp_path / "autosubmit.db"
    db_path.touch()

    mocker.patch.object(
        validators.BasicConfig,
        "CONFIG_FILE_FOUND",
        True,
    )
    mocker.patch.object(
        validators.BasicConfig,
        "DATABASE_BACKEND",
        "sqlite",
    )
    mocker.patch.object(
        validators.BasicConfig,
        "DB_PATH",
        db_path,
    )
    mocker.patch(
        "autosubmit.scripts._validation.os.access",
        side_effect=lambda path, mode: mode != os.R_OK,
    )

    log_error = mocker.patch(
        "autosubmit.scripts._validation.Log.error",
    )

    with pytest.raises(SystemExit) as error:
        validators.validate_required_files("run", mocker.Mock())

    assert error.value.code == 1
    log_error.assert_called_once_with(
        f'Experiments database "{db_path}" is not readable. '
        "Please check the file permissions.",
    )


def test_validate_required_files_database_not_writable(mocker, tmp_path):
    """Test a SQLite database that is not writable."""
    db_path = tmp_path / "autosubmit.db"
    db_path.touch()

    mocker.patch.object(
        validators.BasicConfig,
        "CONFIG_FILE_FOUND",
        True,
    )
    mocker.patch.object(
        validators.BasicConfig,
        "DATABASE_BACKEND",
        "sqlite",
    )
    mocker.patch.object(
        validators.BasicConfig,
        "DB_PATH",
        db_path,
    )
    mocker.patch(
        "autosubmit.scripts._validation.os.access",
        side_effect=lambda path, mode: mode != os.W_OK,
    )

    log_error = mocker.patch(
        "autosubmit.scripts._validation.Log.error",
    )

    with pytest.raises(SystemExit) as error:
        validators.validate_required_files("run", mocker.Mock())

    assert error.value.code == 1
    log_error.assert_called_once_with(
        f'Experiments database "{db_path}" is not writable. '
        "Please check the file permissions.",
    )


@pytest.mark.parametrize(
    "accepts_other_users",
    [True, False],
    ids=[
        "accepts-other-users",
        "does-not-accept-other-users",
    ],
)
def test_validate_expid_ownership_check_depends_on_user_permissions(
    mocker,
    accepts_other_users,
):
    """Test that ownership is checked only when other users are not accepted.

    :param mocker: Pytest mocker fixture.
    :param accepts_other_users: Whether the command accepts experiments owned
        by other users.
    """
    opts = mocker.Mock()
    opts.expid = "a001"
    opts.accepts_multiple_expids = False
    opts.accepts_other_users = accepts_other_users

    mocker.patch(
        "autosubmit.scripts._validation.is_valid_experiment_id",
        return_value=True,
    )
    mocker.patch(
        "autosubmit.scripts._validation.experiment_exists",
        return_value=True,
    )
    check_ownership = mocker.patch(
        "autosubmit.scripts._validation.check_ownership",
    )
    log_debug = mocker.patch(
        "autosubmit.scripts._validation.Log.debug",
    )

    validators.validate_expid("run", opts)

    if accepts_other_users:
        check_ownership.assert_not_called()
        log_debug.assert_called_once_with(
            "Skipping ownership check for experiment 'a001'.",
        )
    else:
        check_ownership.assert_called_once_with("a001")
        log_debug.assert_not_called()


def test_validate_expid_other_user_allowed(mocker):
    """Test that an experiment owned by another user is accepted when enabled.

    :param mocker: Pytest mocker fixture.
    """
    opts = mocker.Mock()
    opts.expid = "a001"
    opts.accepts_multiple_expids = False
    opts.accepts_other_users = True

    mocker.patch(
        "autosubmit.scripts._validation.is_valid_experiment_id",
        return_value=True,
    )
    mocker.patch(
        "autosubmit.scripts._validation.experiment_exists",
        return_value=True,
    )
    check_ownership = mocker.patch(
        "autosubmit.scripts._validation.check_ownership",
        side_effect=AutosubmitCritical("not owner"),
    )
    log_debug = mocker.patch(
        "autosubmit.scripts._validation.Log.debug",
    )
    log_error = mocker.patch(
        "autosubmit.scripts._validation.Log.error",
    )

    validators.validate_expid("run", opts)

    check_ownership.assert_not_called()
    log_debug.assert_called_once_with(
        "Skipping ownership check for experiment 'a001'.",
    )
    log_error.assert_not_called()


def test_default_options_do_not_accept_other_users():
    """Test that commands reject experiments owned by other users by default.

    :return: None.
    """
    assert DefaultOptions.accepts_other_users is False
