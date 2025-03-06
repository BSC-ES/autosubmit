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

"""Integration tests for the experiment status DB managers."""

from pathlib import Path
from typing import cast, Union, TYPE_CHECKING

import pytest
from sqlalchemy import inspect

from autosubmit.database.tables import ExperimentStatusTable
from autosubmit.history.database_managers.database_models import ExperimentRow, ExperimentStatusRow
from autosubmit.history.database_managers.experiment_status_db_manager import (
    ExperimentStatusDbManager,
    SqlAlchemyExperimentStatusDbManager,
    create_experiment_status_db_manager,
)
from autosubmit.job.job_common import Status

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from _pytest._py.path import LocalPath

_EXPID = 't099'


def test_create_experiment_status_db_manager_invalid_value():
    with pytest.raises(ValueError):
        create_experiment_status_db_manager(None)  # type: ignore


@pytest.mark.parametrize(
    "db_engine,options,clazz",
    [
        # postgres
        pytest.param(
            "postgres",
            {"expid": _EXPID},
            SqlAlchemyExperimentStatusDbManager,
            marks=[pytest.mark.postgres],
        ),
        # sqlite
        pytest.param(
            "sqlite",
            {"expid": _EXPID, "main_db_name": "tests.db"},
            ExperimentStatusDbManager,
        )
    ],
)
def test_experiment_status_db_manager(
    tmp_path: 'LocalPath',
    db_engine: str,
    options: dict,
    clazz: Union[type[SqlAlchemyExperimentStatusDbManager], type[ExperimentStatusDbManager]],
    request: pytest.FixtureRequest,
):
    """Test status database manager using the old (SQLite) and new (SQLAlchemy) implementations."""
    request.getfixturevalue(f"as_db_{db_engine}")

    tmp_test_dir = tmp_path / "test_status"
    tmp_test_dir.mkdir()

    is_sqlalchemy = db_engine == "sqlite"
    if is_sqlalchemy:
        options["db_dir_path"] = tmp_test_dir
        options["local_root_dir_path"] = tmp_test_dir

    # Assert type of database manager
    database_manager = create_experiment_status_db_manager(db_engine, **options)  # type: clazz
    assert isinstance(database_manager, clazz)

    # Test initialization of the table (is possible is created by some previous test)
    if is_sqlalchemy:
        assert Path(cast(ExperimentStatusDbManager, database_manager)._as_times_file_path).exists()
    else:
        inspector = inspect(database_manager.engine)
        assert inspector.has_table(ExperimentStatusTable.name, schema="public")

    # Test methods
    # Create as RUNNING
    experiment = ExperimentRow(id=1, name=options["expid"], autosubmit_version="4.1.10", description="test")
    database_manager.create_experiment_status_as_running(experiment)

    exp_status: ExperimentStatusRow = (database_manager.get_experiment_status_row_by_exp_id(exp_id=experiment.id))
    assert exp_status.status == "RUNNING"

    # Update status
    database_manager.update_exp_status(experiment.name, "READY")
    exp_status: ExperimentStatusRow = (database_manager.get_experiment_status_row_by_exp_id(exp_id=experiment.id))
    assert exp_status.status == "READY"

    # Set back to RUNNING
    database_manager.set_existing_experiment_status_as_running(exp_status.name)
    exp_status: ExperimentStatusRow = (database_manager.get_experiment_status_row_by_exp_id(exp_id=experiment.id))
    assert exp_status.status == "RUNNING"


@pytest.mark.parametrize(
    "db_engine,options,clazz",
    [
        # postgres
        pytest.param(
            "postgres",
            {"expid": _EXPID},
            SqlAlchemyExperimentStatusDbManager,
            marks=[pytest.mark.postgres],
        ),
        # sqlite
        pytest.param(
            "sqlite",
            {"expid": _EXPID, "main_db_name": "tests.db"},
            ExperimentStatusDbManager,
        )
    ],
)
def test_get_experiment_status_row_by_expid(
    tmp_path: 'LocalPath',
    db_engine: str,
    options: dict,
    autosubmit_exp,
    clazz: Union[type[SqlAlchemyExperimentStatusDbManager], type[ExperimentStatusDbManager]],
    request: pytest.FixtureRequest,
):
    request.getfixturevalue(f"as_db_{db_engine}")

    tmp_test_dir = tmp_path

    is_sqlalchemy = db_engine == "sqlite"
    if is_sqlalchemy:
        options["db_dir_path"] = tmp_test_dir
        options["local_root_dir_path"] = tmp_test_dir

    database_manager = create_experiment_status_db_manager(db_engine, **options)

    # An error as there is no such experiment ID in the database
    with pytest.raises(ValueError):
        database_manager.get_experiment_status_row_by_expid(_EXPID)

    # Create the experiment, but it still will not have any experiment status
    exp = autosubmit_exp(_EXPID)
    experiment_status_row = database_manager.get_experiment_status_row_by_expid(exp.expid)
    assert experiment_status_row is None

    last_row_id = database_manager.create_exp_status(1, exp.expid, Status.SUBMITTED)
    assert last_row_id > 0

    experiment_status_row = database_manager.get_experiment_status_row_by_expid(exp.expid)
    assert experiment_status_row
