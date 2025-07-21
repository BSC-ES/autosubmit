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
from autosubmit.job.job import Job
from autosubmit.job.job_list import JobList
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory

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
     'submit_time_timestamp': None, 'synchronize': None, 'updated_log': False},
    # {'chunk': None, 'current_checkpoint_step': 0, 'date': None, 'date_split': None, 'finish_time_timestamp': None,
    #  'frequency': None, 'id': 0, 'local_logs_err': None, 'local_logs_out': None, 'max_checkpoint_step': 0,
    #  'name': 'a01f_SIM', 'packed': False, 'platform_name': None, 'priority': 0, 'ready_date': None,
    #  'remote_logs_err': None, 'remote_logs_out': None, 'script_name': 'a01f_SIM.cmd', 'section': 'SIM',
    #  'split': -1, 'splits': -1, 'start_time': None, 'start_time_timestamp': None, 'status': 'WAITING',
    #  'submit_time_timestamp': None, 'synchronize': None, 'updated_log': False}
]

raw_graph_edges = [
    {'completed': 'WAITING', 'e_from': 'a01f_REMOTE_SETUP', 'e_to': 'a01f_20000101_fc0_INI', 'from_step': 0,
     'optional': True, 'status': 'COMPLETED'},
    {'completed': 'WAITING', 'e_from': 'a01f_LOCAL_SETUP', 'e_to': 'a01f_REMOTE_SETUP', 'from_step': 0,
     'optional': True, 'status': 'COMPLETED'},
    {'completed': 'WAITING', 'e_from': 'a01f_20000101_fc0_INI', 'e_to': 'a01f_SIM', 'from_step': 0,
     'optional': True, 'status': 'COMPLETED'},
]


def generate_job_list(autosubmit_config) -> JobList:
    """Generate a JobList with the raw_job_list data."""
    as_conf = autosubmit_config("dummy-expid", {})

    job_list = JobList("dummy-expid", as_conf, YAMLParserFactory(), run_mode=True)
    for job_dict in raw_job_list:
        job = Job(loaded_data=job_dict)
        job_list.add_job(job)

    for edge in raw_graph_edges:
        if edge['e_from'] in job_list.graph and edge['e_to'] in job_list.graph:
            job_list.graph.add_edge(edge['e_from'], edge['e_to'], from_step=edge['from_step'], status=edge['status'],
                                    completed=edge['completed'], optional=edge['optional'])
    return job_list


def _create_db_manager(db_path: Path = None, scheme: str = None) -> JobsDbManager:
    connection_url = get_connection_url(db_path=db_path)
    return JobsDbManager(connection_url=connection_url, schema=scheme)


@pytest.mark.parametrize(
    'db_engine,options,full_load',
    [
        # postgres
        pytest.param('postgres', {'schema': 'test_schema'}, True,
                     marks=[pytest.mark.postgres, pytest.mark.docker]),
        pytest.param('postgres', {'schema': 'test_schema'}, False,
                     marks=[pytest.mark.postgres, pytest.mark.docker]),
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
        pytest.param('postgres', {'schema': 'test_schema'}, False,
                     marks=[pytest.mark.postgres, pytest.mark.docker]),
        # sqlite
        pytest.param('sqlite', {'db_name': 'test_db_manager.db', 'db_version': 999}, True),
        pytest.param('sqlite', {'db_name': 'test_db_manager.db', 'db_version': 999}, False)
    ]
)
def test_db_job_list_jobs(tmp_path: Path, db_engine: str, options: dict, request, full_load: bool,
                          autosubmit_config):
    request.getfixturevalue(f"as_db_{db_engine}")
    if db_engine == 'sqlite':
        db_manager = _create_db_manager(db_path=tmp_path / options['db_name'])
    else:
        db_manager = _create_db_manager(scheme=options['schema'])

    job_list = generate_job_list(autosubmit_config)
    job_list.dbmanager = db_manager
    job_list.save_jobs()

    # Load jobs active jobs
    loaded_jobs = job_list.dbmanager.load_jobs(full_load=full_load)

    if full_load:
        assert len(loaded_jobs) == len(raw_job_list)
    else:
        # If not full load, we expect only the active jobs (edges is empty)
        assert len(loaded_jobs) < len(raw_job_list)

    for job in loaded_jobs:
        # Check that the job is a dict
        assert isinstance(job, dict)
        # Check that the job has the expected keys
        assert set(job.keys()) == {
            'chunk', 'member', 'current_checkpoint_step', 'date', 'date_split', 'finish_time_timestamp', 'frequency',
            'id', 'local_logs_err', 'local_logs_out', 'max_checkpoint_step', 'name', 'packed', 'platform_name',
            'priority', 'ready_date', 'remote_logs_err', 'remote_logs_out', 'script_name', 'section',
            'split', 'splits', 'start_time', 'start_time_timestamp', 'status', 'submit_time_timestamp',
            'synchronize', 'updated_log'
        }


@pytest.mark.parametrize(
    'db_engine,options,full_load',
    [
        # postgres
        pytest.param('postgres', {'schema': 'test_schema'}, True,
                     marks=[pytest.mark.postgres, pytest.mark.docker]),
        pytest.param('postgres', {'schema': 'test_schema'}, False,
                     marks=[pytest.mark.postgres, pytest.mark.docker]),
        # sqlite
        pytest.param('sqlite', {'db_name': 'test_db_manager.db', 'db_version': 999}, True),
        pytest.param('sqlite', {'db_name': 'test_db_manager.db', 'db_version': 999}, False)
    ]
)
def test_db_job_list_jobs_and_edges_together(
        tmp_path: Path,
        db_engine: str,
        options: dict,
        full_load: bool,
        request: pytest.FixtureRequest,
        autosubmit_config: Callable
):
    """
    Test loading and saving both jobs and edges together with different full_load options.

    This test verifies that JobList's database manager can correctly save and load
    both jobs and graph edges in a coordinated way.

    :param tmp_path: Temporary directory path for SQLite database files
    :type tmp_path: Path
    :param db_engine: Database engine to use ('sqlite' or 'postgres')
    :type db_engine: str
    :param options: Database connection options
    :type options: dict
    :param full_load: Whether to use full_load when loading data
    :type full_load: bool
    :param request: Pytest request fixture for accessing other fixtures
    :type request: pytest.FixtureRequest
    :param autosubmit_config: Fixture to create a test configuration
    :type autosubmit_config: Callable
    """
    # Load database fixture
    request.getfixturevalue(f"as_db_{db_engine}")

    # Create database manager
    if db_engine == 'sqlite':
        db_manager = _create_db_manager(db_path=tmp_path / options['db_name'])
    else:
        db_manager = _create_db_manager(scheme=options['schema'])

    # Create and save original job list with jobs and edges
    job_list = generate_job_list(autosubmit_config)
    job_list.dbmanager = db_manager

    # Save jobs and edges to database
    job_list.save_jobs()
    db_manager.save_edges(raw_graph_edges)

    # Load jobs and edges with the specified full_load parameter
    loaded_jobs = db_manager.load_jobs(full_load=full_load)
    loaded_edges = db_manager.load_edges(loaded_jobs, full_load=full_load)

    if full_load:
        assert len(loaded_jobs) == len(raw_job_list)
        assert len(loaded_edges) == len(raw_graph_edges)
    else:
        # If not full load, we expect only the active jobs and children jobs
        assert 0 < len(loaded_jobs) < len(raw_job_list)
        assert 0 < len(loaded_edges) < len(raw_graph_edges)

    for job in loaded_jobs:
        # Check that the job is a dict
        assert isinstance(job, dict)
        # Check that the job has the expected keys
        assert set(job.keys()) == {
            'chunk', 'member', 'current_checkpoint_step', 'date', 'date_split', 'finish_time_timestamp', 'frequency',
            'id', 'local_logs_err', 'local_logs_out', 'max_checkpoint_step', 'name', 'packed', 'platform_name',
            'priority', 'ready_date', 'remote_logs_err', 'remote_logs_out', 'script_name', 'section',
            'split', 'splits', 'start_time', 'start_time_timestamp', 'status', 'submit_time_timestamp',
            'synchronize', 'updated_log'
        }

    for edge in loaded_edges:
        # Check that the edge is a dict
        assert isinstance(edge, dict)
        # Check that the edge has the expected keys
        assert set(edge.keys()) == {'e_from', 'e_to', 'from_step', 'status', 'completed', 'optional'}
        # Check that the edge matches the saved edges
