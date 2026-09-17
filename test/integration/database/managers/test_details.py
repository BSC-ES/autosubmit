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

"""Integration tests for the experiment details repository."""

import pytest

from autosubmit.database.managers.details import ExperimentDetailsRepository


@pytest.mark.docker
@pytest.mark.postgres
def test_details_repository(as_db: str):
    details_repo = ExperimentDetailsRepository()

    exp_id = 10
    created = "2024-04-11T13:34:41+02:00"

    # Insert data
    details_repo.upsert_details(
        exp_id=exp_id, user="foo", created=created, model="my_model", branch="NA", hpc="MN5"
    )
    assert details_repo.get_details(exp_id) == {
        "exp_id": exp_id,
        "user": "foo",
        "created": created,
        "model": "my_model",
        "branch": "NA",
        "hpc": "MN5",
    }

    # Update data
    details_repo.upsert_details(
        exp_id=exp_id, user="bar", created=created, model="my_model", branch="NA", hpc="MN5"
    )
    updated = details_repo.get_details(exp_id)
    assert updated is not None
    assert updated["user"] == "bar"

    # Delete data
    details_repo.delete_details(exp_id)
    assert details_repo.get_details(exp_id) is None
