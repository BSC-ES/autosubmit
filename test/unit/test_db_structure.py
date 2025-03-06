import networkx as nx
import pytest
from pathlib import Path
from typing import Type

from autosubmit.database import db_structure
from autosubmit.database.db_manager import DbManager, SqlAlchemyDbManager


@pytest.mark.parametrize(
    'db_engine,options,clazz',
    [
        # postgres
        pytest.param('postgres', {'schema': 'test_schema'}, SqlAlchemyDbManager, marks=[pytest.mark.postgres]),
        # sqlite
        ('sqlite', {'db_name': 'test_db_manager.db', 'db_version': 999}, DbManager)
    ])
def test_db_structure(
        tmp_path: Path,
        db_engine: str,
        options: dict,
        clazz: Type,
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
