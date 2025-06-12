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

from autosubmit.database.db_common import check_db_path, get_connection_url
from autosubmit.database import db_manager_job_list
from autosubmit.database.db_manager_job_list import JobsDbManager

raw_job_list = [
    {'chunk': None, 'current_checkpoint_step': 0, 'date': None, 'date_split': None, 'finish_time_timestamp': None,
     'frequency': None, 'id': 0, 'local_logs_err': None, 'local_logs_out': None, 'max_checkpoint_step': 0,
     'name': 'a01f_REMOTE_SETUP', 'packed': False, 'platform_name': None, 'priority': 1, 'ready_date': None,
     'remote_logs_err': None, 'remote_logs_out': None, 'script_name': 'a01f_REMOTE_SETUP.cmd',
     'section': 'REMOTE_SETUP', 'split': -1, 'splits': -1, 'start_time': None, 'start_time_timestamp': None,
     'status': 'WAITING', 'submit_time_timestamp': None, 'synchronize': None, 'updated_log': False},
    {'chunk': None, 'current_checkpoint_step': 0, 'date': None, 'date_split': None, 'finish_time_timestamp': None,
     'frequency': None, 'id': 0, 'local_logs_err': None, 'local_logs_out': None, 'max_checkpoint_step': 0,
     'name': 'a01f_LOCAL_SETUP', 'packed': False, 'platform_name': None, 'priority': 0, 'ready_date': None,
     'remote_logs_err': None, 'remote_logs_out': None, 'script_name': 'a01f_LOCAL_SETUP.cmd', 'section': 'LOCAL_SETUP',
     'split': -1, 'splits': -1, 'start_time': None, 'start_time_timestamp': None, 'status': 'READY',
     'submit_time_timestamp': None, 'synchronize': None, 'updated_log': False}
]

raw_graph_edges = [
    {'completed': 'WAITING', 'e_from': 'a01f_REMOTE_SETUP', 'e_to': 'a01f_20000101_fc0_INI', 'from_step': 0,
     'optional': True, 'status': 'COMPLETED'},
    {'completed': 'WAITING', 'e_from': 'a01f_LOCAL_SETUP', 'e_to': 'a01f_REMOTE_SETUP', 'from_step': 0,
     'optional': True, 'status': 'COMPLETED'}
]

def _create_db_manager(db_path: Path = None, scheme: str = None) -> JobsDbManager:
    if db_path:
        connection_url = get_connection_url(db_path=db_path)
    else:
        connection_url = None
    return JobsDbManager(connection_url=connection_url, schema=scheme)

@pytest.mark.parametrize(
    'db_engine,options',
    [
        # postgres
        pytest.param('postgres', {'schema': 'test_schema'},
                     marks=[pytest.mark.postgres, pytest.mark.docker]),
        # sqlite
        pytest.param('sqlite', {'db_name': 'test_db_manager.db', 'db_version': 999})
    ])
def test_db_job_list_edges(
        tmp_path: Path,
        db_engine: str,
        options: dict,
        request: pytest.FixtureRequest
):
    # Load dynamically the fixture,
    # ref: https://stackoverflow.com/a/64348247.
    request.getfixturevalue(f"as_db_{db_engine}")

    if db_engine == 'sqlite':
        db_manager_job_list = _create_db_manager(db_path=tmp_path / options['db_name'])
    else:
        db_manager_job_list = _create_db_manager(scheme=options['schema'])

    # Table is empty ( created by load_edges or any method )
    result = db_manager_job_list.load_edges(raw_job_list, full_load=True)

    assert result == []
    # Save edges with the raw_graph_edges
    db_manager_job_list.save_edges(raw_graph_edges)

    # Get correct data
    loaded_edges = db_manager_job_list.load_edges(raw_job_list, full_load=True)

    assert len(loaded_edges) == len(raw_graph_edges)
    for i, edge in enumerate(loaded_edges):
        # Check that the edge is a dict
        assert isinstance(edge, dict)
        # Check that the edge has the expected keys
        assert set(edge.keys()) == {'e_from', 'e_to', 'from_step', 'status', 'completed', 'optional'}
        assert edge['e_from'] == raw_graph_edges[i]['e_from']
        assert edge['e_to'] == raw_graph_edges[i]['e_to']
        assert edge['from_step'] == raw_graph_edges[i]['from_step']
        assert edge['status'] == raw_graph_edges[i]['status']



@pytest.mark.parametrize(
    'db_engine,options',
    [
        # postgres
        pytest.param('postgres', {'schema': 'test_schema'},
                     marks=[pytest.mark.postgres, pytest.mark.docker]),
        # sqlite
        pytest.param('sqlite', {'db_name': 'test_db_manager.db', 'db_version': 999})
    ])
def test_db_structure_db_already_exists(
        tmp_path: Path,
        db_engine: str,
        options: dict,
        request: pytest.FixtureRequest
):
    """Different from the test above, this one saves it first, and then checks that
    data is retrieved correctly.
    """
    # Load dynamically the fixture,
    # ref: https://stackoverflow.com/a/64348247.
    request.getfixturevalue(f"as_db_{db_engine}")

    graph = nx.DiGraph([("a", "b"), ("b", "c"), ("a", "d")])
    graph.add_node("z")

    # Creates a new SQLite db file
    expid = "ut01"

    # Save table
    db_structure.save_structure(graph, expid, tmp_path)

    # Table not exists
    structure_data = db_structure.get_structure(expid, tmp_path)
    assert sorted(structure_data) == sorted({
        "a": ["b", "d"],
        "b": ["c"],
        "c": [],
        "d": [],
        "z": ["z"],
    })
