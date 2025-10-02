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


@pytest.mark.docker
@pytest.mark.postgres
def test_db_structure(tmp_path: Path, as_db: str):
    graph = nx.DiGraph([("a", "b"), ("b", "c"), ("a", "d")])
    graph.add_node("z")

    # Creates a new SQLite db file
    expid = "ut01"

    # Table does not exist
    assert db_structure.get_structure(expid, tmp_path) == {}

    # Save table
    db_structure.save_structure(graph, expid, tmp_path)

    # Get correct data
    structure_data = db_structure.get_structure(expid, tmp_path)
    assert sorted(structure_data) == sorted({
        "a": ["b", "d"],
        "b": ["c"],
        "c": [],
        "d": [],
        "z": ["z"],
    })


@pytest.mark.docker
@pytest.mark.postgres
def test_db_structure_db_already_exists(tmp_path: Path, as_db: str):
    """Different from the test above, this one saves it first, and then checks that
    data is retrieved correctly.
    """
    graph = nx.DiGraph([("a", "b"), ("b", "c"), ("a", "d")])
    graph.add_node("z")

    # Creates a new SQLite db file
    expid = "ut01"

    # Save table
    db_structure.save_structure(graph, expid, tmp_path)

    # Table does not exist
    structure_data = db_structure.get_structure(expid, tmp_path)
    assert sorted(structure_data) == sorted({
        "a": ["b", "d"],
        "b": ["c"],
        "c": [],
        "d": [],
        "z": ["z"],
    })
