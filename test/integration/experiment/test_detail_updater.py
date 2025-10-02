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

"""Integration tests for detail updater."""

import pytest
from sqlalchemy.schema import CreateTable

from autosubmit.database import session
from autosubmit.database.db_common import get_connection_url
from autosubmit.database.tables import DetailsTable
from autosubmit.experiment.detail_updater import (
    ExperimentDetails,
    create_experiment_details_repository,
)

_EXPID = 't000'


def test_create_experiment_details_repository_invalid_db_engine():
    with pytest.raises(ValueError):
        create_experiment_details_repository('csv')


def test_details_properties(mocker):
    # TODO: mocked create_experiment_details_repository as it fails intermittently with
    #       sqlite3.OperationalError: unable to open database file
    mocker.patch('autosubmit.experiment.detail_updater.create_experiment_details_repository')
    exp_details = ExperimentDetails(_EXPID, init_reload=False)

    exp_details.exp_id = 0

    mock_as_conf = mocker.MagicMock()
    mock_as_conf.get_project_type.return_value = "git"
    mock_as_conf.get_git_project_origin.return_value = "my_git_origin"
    mock_as_conf.get_git_project_branch.return_value = "my_git_branch"
    mock_as_conf.get_platform.return_value = "my_platform"

    exp_details.as_conf = mock_as_conf

    assert exp_details.hpc == "my_platform"

    assert exp_details.model == "my_git_origin"
    assert exp_details.branch == "my_git_branch"


@pytest.mark.docker
@pytest.mark.postgres
def test_details_repository(tmpdir, as_db: str):
    connection_url = get_connection_url(tmpdir / 'details.db')
    with session.create_engine(connection_url=connection_url).connect() as conn:
        conn.execute(CreateTable(DetailsTable, if_not_exists=True))
        conn.commit()

    details_repo = create_experiment_details_repository(as_db)

    new_data = {
        "exp_id": 10,
        "user": "foo",
        "created": "2024-04-11T13:34:41+02:00",
        "model": "my_model",
        "branch": "NA",
        "hpc": "MN5",
    }

    # Insert data
    details_repo.upsert_details(
        exp_id=new_data["exp_id"],
        user=new_data["user"],
        created=new_data["created"],
        model=new_data["model"],
        branch=new_data["branch"],
        hpc=new_data["hpc"],
    )
    result = details_repo.get_details(new_data["exp_id"])
    assert result == new_data

    # Update data
    updated_data = {
        "exp_id": 10,
        "user": "bar",
        "created": "2024-04-11T13:34:41+02:00",
        "model": "my_model",
        "branch": "NA",
        "hpc": "MN5",
    }
    details_repo.upsert_details(
        exp_id=updated_data["exp_id"],
        user=updated_data["user"],
        created=updated_data["created"],
        model=updated_data["model"],
        branch=updated_data["branch"],
        hpc=updated_data["hpc"],
    )
    result = details_repo.get_details(updated_data["exp_id"])
    assert result == updated_data

    # Delete data
    details_repo.delete_details(updated_data["exp_id"])
    result = details_repo.get_details(updated_data["exp_id"])
    assert result is None
