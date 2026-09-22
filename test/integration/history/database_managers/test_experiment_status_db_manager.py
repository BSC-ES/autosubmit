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

import pytest
from sqlalchemy import inspect

from autosubmit.database.tables import ExperimentStatusTable
from autosubmit.history.database_managers.database_models import (
    ExperimentRow,
    ExperimentStatusRow,
)
from autosubmit.history.database_managers.experiment_status_db_manager import (
    SqlAlchemyExperimentStatusDbManager,
)
from autosubmit.job.job_common import Status


@pytest.mark.docker
@pytest.mark.postgres
def test_experiment_status_db_manager(as_db: str, get_next_expid):
    """The SQLAlchemy status manager creates and updates the experiment status."""
    expid = get_next_expid()
    database_manager = SqlAlchemyExperimentStatusDbManager()

    inspector = inspect(database_manager.status_engine)
    schema = None if as_db == "sqlite" else "public"
    assert inspector.has_table(ExperimentStatusTable.name, schema=schema)

    # Create as RUNNING
    experiment = ExperimentRow(id=1, name=expid, autosubmit_version="4.1.10", description="test")
    database_manager.create_experiment_status_as_running(experiment)

    exp_status: ExperimentStatusRow = database_manager.get_experiment_status_row_by_exp_id(exp_id=experiment.id)
    assert exp_status.status == "RUNNING"

    # Update status
    database_manager.update_exp_status(experiment.name, "READY")
    exp_status = database_manager.get_experiment_status_row_by_exp_id(exp_id=experiment.id)
    assert exp_status.status == "READY"

    # Set back to RUNNING
    database_manager.set_existing_experiment_status_as_running(exp_status.name)
    exp_status = database_manager.get_experiment_status_row_by_exp_id(exp_id=experiment.id)
    assert exp_status.status == "RUNNING"


@pytest.mark.docker
@pytest.mark.postgres
def test_get_experiment_status_row_by_expid(as_db: str, autosubmit_exp, get_next_expid):
    expid = get_next_expid()
    database_manager = SqlAlchemyExperimentStatusDbManager()

    # An error as there is no such experiment ID in the database
    with pytest.raises(ValueError):
        database_manager.get_experiment_status_row_by_expid(expid)

    # Create the experiment, but it still will not have any experiment status
    exp = autosubmit_exp(expid=expid, include_jobs=True)
    experiment_status_row = database_manager.get_experiment_status_row_by_expid(exp.expid)
    assert experiment_status_row is None

    # Get the experiment row
    experiment_row = database_manager.get_experiment_row_by_expid(exp.expid)
    experiment_db_id = experiment_row.id if experiment_row else None
    assert experiment_db_id is not None

    # Create the experiment status row and assert it is created
    last_row_id = database_manager.create_exp_status(experiment_db_id, exp.expid, Status.SUBMITTED)
    assert last_row_id > 0

    experiment_status_row = database_manager.get_experiment_status_row_by_expid(exp.expid)
    assert experiment_status_row
