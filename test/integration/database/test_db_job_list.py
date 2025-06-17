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
from collections.abc import Callable
from pathlib import Path

import networkx as nx
import pytest
from typing import List, Dict, Any

from autosubmit.database.db_common import check_db_path, get_connection_url
from autosubmit.database import db_manager_job_list
from autosubmit.database.db_manager_job_list import JobsDbManager
from autosubmit.job.job_list import JobList


@pytest.fixture
def create_job_list(mocker) -> Callable[[List[Dict[str, Any]]], JobList]:
    """Create a mocked job list for the job_utils tests."""

    def _fn(jobs_data: List[Dict[str, Any]]):
        job_list = mocker.patch('autosubmit.job.job_list.JobList', autospec=True)
        return job_list

    return _fn


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
     'submit_time_timestamp': None, 'synchronize': None, 'updated_log': False},
    {'chunk': None, 'current_checkpoint_step': 0, 'date': None, 'date_split': None, 'finish_time_timestamp': None,
     'frequency': None, 'id': 0, 'local_logs_err': None, 'local_logs_out': None, 'max_checkpoint_step': 0,
     'name': 'a01f_20000101_fc0_INI', 'packed': False, 'platform_name': None, 'priority': 0, 'ready_date': None,
     'remote_logs_err': None, 'remote_logs_out': None, 'script_name': 'a01f_20000101_fc0_INI.cmd', 'section': 'INI',
     'split': -1, 'splits': -1, 'start_time': None, 'start_time_timestamp': None, 'status': 'WAITING',
     'submit_time_timestamp': None, 'synchronize': None, 'updated_log': False}
]

raw_graph_edges = [
    {'completed': 'WAITING', 'e_from': 'a01f_REMOTE_SETUP', 'e_to': 'a01f_20000101_fc0_INI', 'from_step': 0,
     'optional': True, 'status': 'COMPLETED'},
    {'completed': 'WAITING', 'e_from': 'a01f_LOCAL_SETUP', 'e_to': 'a01f_REMOTE_SETUP', 'from_step': 0,
     'optional': True, 'status': 'COMPLETED'},
    {'completed': 'WAITING', 'e_from': 'a01f_20000101_fc0_INI', 'e_to': 'a01f_SIM', 'from_step': 0,
     'optional': True, 'status': 'COMPLETED'},
]


def _create_db_manager(db_path: Path = None, scheme: str = None) -> JobsDbManager:
    connection_url = get_connection_url(db_path=db_path)
    return JobsDbManager(connection_url=connection_url, schema=scheme)


@pytest.mark.parametrize(
    'db_engine,options,full_load',
    [
        # postgres
        pytest.param('postgres', {'schema': 'test_schema'}, True,
                     marks=[pytest.mark.postgres, pytest.mark.docker]),
        pytest.param('postgres', {'schema': 'test_schema'}, False),
        # sqlite
        pytest.param('sqlite', {'db_name': 'test_db_manager.db', 'db_version': 999}, True),
        pytest.param('sqlite', {'db_name': 'test_db_manager.db', 'db_version': 999}, False)
    ]
)
def test_db_job_list_edges(
        tmp_path: Path,
        db_engine: str,
        options: dict,
        full_load: bool,
        request: pytest.FixtureRequest
):
    # Load dynamically the fixture,
    # ref: https://stackoverflow.com/a/64348247.
    request.getfixturevalue(f"as_db_{db_engine}")
    raw_graph_edges_local = raw_graph_edges
    if db_engine == 'sqlite':
        db_manager = _create_db_manager(db_path=tmp_path / options['db_name'])
    else:
        db_manager = _create_db_manager(scheme=options['schema'])

    # Table is empty ( created by load_edges or any method )
    result = db_manager.load_edges(raw_job_list, full_load=full_load)

    assert result == []
    # Save edges with the raw_graph_edges
    db_manager.save_edges(raw_graph_edges_local)

    # Get correct data
    loaded_edges = db_manager.load_edges(raw_job_list, full_load=full_load)

    assert len(loaded_edges) == len(raw_graph_edges_local)

    # order it
    loaded_edges = sorted(loaded_edges, key=lambda x: (x['e_from'], x['e_to']))
    raw_graph_edges_local = sorted(raw_graph_edges_local, key=lambda x: (x['e_from'], x['e_to']))

    for i, edge in enumerate(loaded_edges):
        # Check that the edge is a dict
        assert isinstance(edge, dict)
        # Check that the edge has the expected keys
        assert set(edge.keys()) == {'e_from', 'e_to', 'from_step', 'status', 'completed', 'optional'}
        assert edge['e_from'] == raw_graph_edges_local[i]['e_from']
        assert edge['e_to'] == raw_graph_edges_local[i]['e_to']
        assert edge['from_step'] == raw_graph_edges_local[i]['from_step']
        assert edge['status'] == raw_graph_edges_local[i]['status']


@pytest.mark.parametrize(
    'db_engine,options,full_load',
    [
        # postgres
        pytest.param('postgres', {'schema': 'test_schema'}, True,
                     marks=[pytest.mark.postgres, pytest.mark.docker]),
        pytest.param('postgres', {'schema': 'test_schema'}, False),
        # sqlite
        pytest.param('sqlite', {'db_name': 'test_db_manager.db', 'db_version': 999}, True),
        pytest.param('sqlite', {'db_name': 'test_db_manager.db', 'db_version': 999}, False)
    ]
)
def test_db_job_list_save_and_load_jobs(tmp_path: Path, db_engine: str, options: dict, request, full_load: bool):
    request.getfixturevalue(f"as_db_{db_engine}")
    if db_engine == 'sqlite':
        db_manager = _create_db_manager(db_path=tmp_path / options['db_name'])
    else:
        db_manager = _create_db_manager(scheme=options['schema'])

    job_list = JobList(expid, as_conf, YAMLParserFactory(), run_mode=True)

    # Save jobs
    db_manager.save_jobs(job_list)

    # Load jobs active jobs
    loaded_jobs = db_manager.load_jobs(full_load=full_load)

    if full_load:
        assert len(loaded_jobs) == len(raw_job_list)
    else:
        # If not full load, we expect only the active jobs and children jobs
        assert len(loaded_jobs) < len(raw_job_list)

    for job in loaded_jobs:
        # Check that the job is a dict
        assert isinstance(job, dict)
        # Check that the job has the expected keys
        assert set(job.keys()) == {
            'chunk', 'current_checkpoint_step', 'date', 'date_split', 'finish_time_timestamp', 'frequency',
            'id', 'local_logs_err', 'local_logs_out', 'max_checkpoint_step', 'name', 'packed', 'platform_name',
            'priority', 'ready_date', 'remote_logs_err', 'remote_logs_out', 'script_name', 'section',
            'split', 'splits', 'start_time', 'start_time_timestamp', 'status', 'submit_time_timestamp',
            'synchronize', 'updated_log'
        }


@pytest.mark.parametrize(
    'db_engine,options',
    [
        pytest.param('sqlite', {'db_name': 'test_db_manager.db', 'db_version': 999}),
        pytest.param('postgres', {'schema': 'test_schema'}, marks=[pytest.mark.postgres, pytest.mark.docker]),
    ]
)
def test_db_job_list_size(tmp_path: Path, db_engine: str, options: dict, request):
    request.getfixturevalue(f"as_db_{db_engine}")
    if db_engine == 'sqlite':
        db_manager = JobsDbManager(get_connection_url(db_path=tmp_path / options['db_name']))
    else:
        db_manager = JobsDbManager(get_connection_url(scheme=options['schema']), schema=options['schema'])

    db_manager.save_jobs(raw_job_list)
    total, completed, failed = db_manager.get_job_list_size()
    assert total == len(raw_job_list)
    assert completed == 0
    assert failed == 0


@pytest.mark.parametrize(
    'db_engine,options',
    [
        pytest.param('sqlite', {'db_name': 'test_db_manager.db', 'db_version': 999}),
        pytest.param('postgres', {'schema': 'test_schema'}, marks=[pytest.mark.postgres, pytest.mark.docker]),
    ]
)
def test_db_job_list_select_active_jobs(tmp_path: Path, db_engine: str, options: dict, request):
    request.getfixturevalue(f"as_db_{db_engine}")
    if db_engine == 'sqlite':
        db_manager = JobsDbManager(get_connection_url(db_path=tmp_path / options['db_name']))
    else:
        db_manager = JobsDbManager(get_connection_url(scheme=options['schema']), schema=options['schema'])

    db_manager.save_jobs(raw_job_list)
    active_jobs = db_manager.select_active_jobs()
    for job in active_jobs:
        assert job['status'] in db_manager._ACTIVE_STATUSES
