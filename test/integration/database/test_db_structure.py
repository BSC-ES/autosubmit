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

from pathlib import Path

import networkx as nx
import pytest

from autosubmit.database import db_structure


@pytest.mark.parametrize(
    'db_engine,options',
    [
        # postgres
        pytest.param('postgres', {'schema': 'test_schema'}, marks=[pytest.mark.postgres]),
        # sqlite
        ('sqlite', {'db_name': 'test_db_manager.db', 'db_version': 999})
    ])
def test_db_structure(
        tmp_path: Path,
        db_engine: str,
        options: dict,
        request: pytest.FixtureRequest
):
    # Load dynamically the fixture,
    # ref: https://stackoverflow.com/a/64348247.
    request.getfixturevalue(f'as_db_{db_engine}')

    graph = nx.DiGraph([("a", "b"), ("b", "c"), ("a", "d")])
    graph.add_node("z")

    # Creates a new SQLite db file
    expid = "ut01"

    # Table not exists
    assert db_structure.get_structure(expid, str(tmp_path)) == {}

    # Save table
    db_structure.save_structure(graph, expid, str(tmp_path))

    # Get correct data
    structure_data = db_structure.get_structure(expid, str(tmp_path))
    assert sorted(structure_data) == sorted({
        "a": ["b", "d"],
        "b": ["c"],
        "c": [],
        "d": [],
        "z": ["z"],
    })
