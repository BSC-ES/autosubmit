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

"""Unit tests for for the experiment status DB managers."""

from typing import cast, TYPE_CHECKING

import pytest

from autosubmit.history.database_managers.experiment_status_db_manager import (
    ExperimentStatusDbManager,
    create_experiment_status_db_manager
)

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from _pytest._py.path import LocalPath


def test_create_experiment_status_db_manager_invalid_value():
    """Test that providing an invalid database type (diff from 'sqlite' and 'postgres') raises an error."""
    with pytest.raises(ValueError):
        create_experiment_status_db_manager(None)  # type: ignore


@pytest.mark.parametrize(
    "mock_path,status_row_return",
    [
        (
            "get_experiment_status_row_by_expid",
            ValueError("missing experiment status row"),
        ),
        ("get_experiment_row_by_expid", ValueError("missing experiment row")),
    ],
    ids=["status_row_lookup_fails", "experiment_row_lookup_fails"],
)
def test_set_exp_status_logs_warning_on_lookup_failures(
    tmp_path: "LocalPath", mocker, mock_path, status_row_return
):
    """Test that set_exp_status() logs a warning when experiment lookups fail."""
    db_dir = tmp_path / "db"
    db_dir.mkdir()
    local_root_dir = tmp_path / "local"
    local_root_dir.mkdir()

    database_manager = ExperimentStatusDbManager(
        expid="a000",
        db_dir_path=str(db_dir),
        main_db_name="test.db",
        local_root_dir_path=str(local_root_dir),
    )

    warning_mock = mocker.patch(
        "autosubmit.history.database_managers.experiment_status_db_manager.Log.warning"
    )
    mocker.patch.object(
        database_manager,
        mock_path,
        side_effect=status_row_return,
    )
    create_status_mock = mocker.patch.object(database_manager, "create_exp_status")

    # Act
    database_manager.set_exp_status("a000", "RUNNING")

    # Assert
    warning_mock.assert_called_once()
    assert (
        "Experiment a000 not found when trying to set status"
        in warning_mock.call_args[0][0]
    )
    create_status_mock.assert_not_called()
