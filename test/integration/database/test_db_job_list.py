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
from typing import Any, Dict, List

import pytest
from ruamel.yaml import YAML

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.config.yamlparser import YAMLParserFactory
from autosubmit.database.db_common import get_connection_url
from autosubmit.database.db_manager_job_list import JobsDbManager
from autosubmit.database.tables import WrapperJobsTable, get_table_from_name, JobsTable, ExperimentStructureTable
from autosubmit.job.job import Job
from autosubmit.job.job_list import JobList

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
    {'completion_status': 'WAITING', 'e_from': 'a01f_REMOTE_SETUP', 'e_to': 'a01f_20000101_fc0_INI', 'from_step': 0,
     'fail_ok': True, 'min_trigger_status': 'COMPLETED'},
    {'completion_status': 'WAITING', 'e_from': 'a01f_LOCAL_SETUP', 'e_to': 'a01f_REMOTE_SETUP', 'from_step': 0,
     'fail_ok': True, 'min_trigger_status': 'COMPLETED'},
    {'completion_status': 'WAITING', 'e_from': 'a01f_20000101_fc0_INI', 'e_to': 'a01f_SIM', 'from_step': 0,
     'fail_ok': True, 'min_trigger_status': 'COMPLETED'},
]


# --- Fixtures.

@pytest.fixture(scope='function')
def _expid(as_exp):
    """
    Creates a new expid and returns the EXPID
    """
    return as_exp.expid


@pytest.fixture(scope='function')
def as_exp(autosubmit_exp):
    """
    Creates a new expid and returns the EXPID
    """
    as_exp = autosubmit_exp(include_jobs=False)
    return as_exp


def _modify_data(expid, conf_dir, data) -> Path:
    data_path = conf_dir / f"{expid}.yml"
    yaml = YAML()
    with data_path.open('w') as f:
        yaml.dump(data, f)
    return data_path


def _init_test(as_exp, conf_dir, data) -> Path:
    for f in conf_dir.glob('*.yml'):
        f.unlink()

    for f in (Path(BasicConfig.LOCAL_ROOT_DIR) / as_exp.expid / "db").glob("*.db"):
        f.unlink()

    _modify_data(as_exp.expid, conf_dir, data)


def _assert_exit_code(final_status: str, exit_code: int) -> None:
    """Check that the exit code is correct."""
    if final_status == "FAILED":
        assert exit_code > 0
    else:
        assert exit_code == 0


def _get_expected_job_names(
    expid: str,
    unified_data: Dict[str, Any],
    once_sections: List[str],
    member_sections: List[str],
    date_sections: List[str],
    chunk_sections: List[str]
) -> List[str]:
    """
    Generate expected job names based on experiment configuration and section types.

    :param expid: Experiment ID.
    :type expid: str
    :param unified_data: Unified experiment configuration data.
    :type unified_data: Dict[str, Any]
    :param once_sections: Sections that run only once per experiment.
    :type once_sections: List[str]
    :param member_sections: Sections that run per member.
    :type member_sections: List[str]
    :param date_sections: Sections that run per date.
    :type date_sections: List[str]
    :param chunk_sections: Sections that run per chunk (date, member, chunk, split).
    :type chunk_sections: List[str]
    :return: List of expected job names.
    :rtype: List[str]
    """
    dates = unified_data['EXPERIMENT']['DATELIST']
    members = unified_data['EXPERIMENT']['MEMBERS']
    chunks = int(unified_data['EXPERIMENT']['NUMCHUNKS'])
    section_names = [section for section in unified_data.get('JOBS', {}).keys()]
    job_names = []
    for section in section_names:
        splits = unified_data['JOBS'][section].get('splits', None)
        if section in chunk_sections:
            if splits is not None:
                splits = int(splits)
            for date in dates.split():
                for member in members.split():
                    for chunk in range(1, chunks + 1):
                        if splits:
                            for split in range(1, splits + 1):
                                job_names.append(f"{expid}_{str(date)}_{str(member)}_{str(chunk)}_{str(split)}_{section}".upper())
                        else:
                            job_names.append(f"{expid}_{str(date)}_{str(member)}_{str(chunk)}_{section}".upper())
        elif section in once_sections:
            if splits is not None:
                splits = int(splits)
                for split in range(1, splits + 1) if splits else [None]:
                    job_names.append(f"{expid}_{str(split)}_{section}".upper())
            else:
                job_names.append(f"{expid}_{section}".upper())
        elif section in member_sections:
            if splits:
                splits = int(splits)
                for split in range(1, splits + 1) if splits else [None]:
                    for date in dates.split():
                        for member in members.split():
                            job_names.append(f"{expid}_{str(date)}_{str(member)}_{str(split)}_{section}".upper())
            else:
                for date in dates.split():
                    for member in members.split():
                        job_names.append(f"{expid}_{str(date)}_{str(member)}_{section}".upper())
        elif section in date_sections:
            if splits is not None:
                splits = int(splits)
                for split in range(1, splits + 1) if splits else [None]:
                    for date in dates.split():
                        job_names.append(f"{expid}_{str(date)}_{str(split)}_{section}".upper())
            else:
                for date in dates.split():
                    job_names.append(f"{expid}_{str(date)}_{section}".upper())

    return job_names


def generate_job_list(as_conf, db_manager) -> JobList:
    """Generate a JobList with the raw_job_list data."""
    job_list = JobList("dummy-expid", as_conf, YAMLParserFactory(), run_mode=True)
    job_list.dbmanager = db_manager

    for job_dict in raw_job_list:
        job = Job(loaded_data=job_dict)
        job_list.add_job(job)

    for edge in raw_graph_edges:
        if edge['e_from'] in job_list.graph and edge['e_to'] in job_list.graph:
            job_list.graph.add_edge(edge['e_from'], edge['e_to'], from_step=edge['from_step'],
                                    min_trigger_status=edge['min_trigger_status'],
                                    completion_status=edge['completion_status'], fail_ok=edge['fail_ok'])
    return job_list


def _create_db_manager(db_path: Path = None, schema: str = None) -> JobsDbManager:
    connection_url = get_connection_url(db_path=db_path)
    return JobsDbManager(connection_url=connection_url, schema=schema)


@pytest.mark.parametrize(
    'full_load',
    [True, False]
)
def test_db_job_list_edges(
        tmp_path: Path,
        full_load: bool,
        as_db: str,
        _expid: str
):
    db_path = tmp_path
    if as_db == 'sqlite':
        db_manager = _create_db_manager(Path(db_path, 'tests.db'))
    else:
        db_manager = _create_db_manager(schema=_expid)

    raw_graph_edges_local = raw_graph_edges

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
        assert set(edge.keys()) == {'e_from', 'e_to', 'from_step', 'min_trigger_status', 'completion_status',
                                    'fail_ok'}
        assert edge['e_from'] == raw_graph_edges_local[i]['e_from']
        assert edge['e_to'] == raw_graph_edges_local[i]['e_to']
        assert edge['from_step'] == raw_graph_edges_local[i]['from_step']
        assert edge['min_trigger_status'] == raw_graph_edges_local[i]['min_trigger_status']


@pytest.mark.parametrize(
    'full_load',
    [True, False]
)
def test_db_job_list_jobs(tmp_path: Path, full_load: bool, as_db: str, autosubmit_exp, _expid: str):
    as_exp = autosubmit_exp(_expid)
    db_path = tmp_path
    if as_db == 'sqlite':
        db_manager = _create_db_manager(Path(db_path, 'tests.db'))
    else:
        db_manager = _create_db_manager(schema=f"{_expid}")
    job_list = generate_job_list(as_exp.as_conf, db_manager)
    job_list.save_jobs()
    job_list.save_edges()

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
            'synchronize', 'updated_log', 'created', 'modified'
        }


@pytest.mark.parametrize(
    'full_load',
    [True, False]
)
def test_db_job_list_jobs_and_edges_together(
        tmp_path: Path,
        full_load: bool,
        as_db: str,
        as_exp: Any,
        _expid: str
):
    """
    Test loading and saving both jobs and edges together with different full_load options.

    This test verifies that JobList's database manager can correctly save and load
    both jobs and graph edges in a coordinated way.
    :param tmp_path: Temporary directory path
    :type tmp_path: Path
    :param full_load: Whether to perform a full load of jobs and edges
    :type full_load: bool
    :param _expid: Experiment ID fixture
    :type _expid: str
    """
    exp_path = Path(BasicConfig.LOCAL_ROOT_DIR) / _expid
    db_path = exp_path / "db"
    if as_db == 'sqlite':
        db_manager = _create_db_manager(Path(db_path, 'tests.db'))
    else:
        db_manager = _create_db_manager(schema=_expid)

    # Create and save original job list with jobs and edges
    job_list = generate_job_list(as_exp.as_conf, db_manager)
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
        assert isinstance(job, dict)
        assert set(job.keys()) == {
            'chunk', 'member', 'current_checkpoint_step', 'date', 'date_split', 'finish_time_timestamp', 'frequency',
            'id', 'local_logs_err', 'local_logs_out', 'max_checkpoint_step', 'name', 'packed', 'platform_name',
            'priority', 'ready_date', 'remote_logs_err', 'remote_logs_out', 'script_name', 'section',
            'split', 'splits', 'start_time', 'start_time_timestamp', 'status', 'submit_time_timestamp',
            'synchronize', 'updated_log', 'created', 'modified'
        }

    for edge in loaded_edges:
        assert isinstance(edge, dict)
        assert set(edge.keys()) == {'e_from', 'e_to', 'from_step', 'min_trigger_status', 'completion_status', 'fail_ok'}


@pytest.mark.parametrize(
    'full_load',
    [True, False]
)
def test_select_latest_inner_jobs(
        tmp_path: Path,
        as_db: str,
        full_load: bool,
        _expid: str,
        as_exp: Any,
):
    exp_path = Path(BasicConfig.LOCAL_ROOT_DIR) / _expid
    db_path = exp_path / "db"
    if as_db == 'sqlite':
        db_manager = _create_db_manager(Path(db_path, 'tests.db'))
    else:
        db_manager = _create_db_manager(schema=_expid)

    job_list = generate_job_list(as_exp.as_conf, db_manager)
    job_list.save_jobs()

    # prepare wrapper
    package_name = "test_package"
    wrapper_info_dict = [{
        "name": package_name,
        "id": 1,
        "script_name": "wrapper_script.sh",
        "status": 1,
        "local_logs_out": None,
        "local_logs_err": None,
        "remote_logs_out": None,
        "remote_logs_err": None,
        "updated_log": True,
        "platform_name": "test_platform",
        "wallclock": "01:00",
        "num_processors": "4",
        "type": 0,
        "sections": None,
        "method": None,
    }]

    inner_jobs_data = []
    for job in job_list.job_list[:3]:
        inner_jobs_data.append({
            "package_id": 1,
            "package_name": package_name,
            "job_name": job.name,
            "timestamp": "2023-01-01T00:00:00"
        })

    wrappers = [(wrapper_info_dict, inner_jobs_data)]

    db_manager.save_wrappers(wrappers, preview=False)

    # Try to insert again the same inner jobs, should warns  ( TODO: check the log.warning somehow)
    db_manager.save_wrappers(wrappers, preview=False)

    newer_timestamp = "2023-01-02T00:00:00"
    updated_job = {
        "package_id": 2,
        "package_name": f"{package_name}_2",
        "job_name": job_list.job_list[0].name,
        "timestamp": newer_timestamp
    }
    db_manager.insert(WrapperJobsTable.name, updated_job)

    innerjobs_table = get_table_from_name(schema=db_manager.schema, table_name=WrapperJobsTable.name)
    latest_wrapped_jobs = db_manager.select_latest_inner_jobs(innerjobs_table)

    assert len(latest_wrapped_jobs) == 3

    # Test now with filtering by job name
    filtered_jobs = db_manager.select_latest_inner_jobs(
        innerjobs_table,
        [job_list.job_list[0].name]
    )
    assert len(filtered_jobs) == 1
    assert filtered_jobs[0]["job_name"] == job_list.job_list[0].name
    assert filtered_jobs[0]["timestamp"] == newer_timestamp

    no_results = db_manager.select_latest_inner_jobs(innerjobs_table, ["null"])
    assert len(no_results) == 0


@pytest.mark.parametrize(
    'full_load',
    [True, False]
)
def test_load_job_by_name(
        tmp_path: Path,
        full_load: bool,
        as_db: str,
        _expid: str,
        as_exp: Any,
):
    exp_path = Path(BasicConfig.LOCAL_ROOT_DIR) / _expid
    db_path = exp_path / "db"
    if as_db == 'sqlite':
        db_manager = _create_db_manager(Path(db_path, 'tests.db'))
    else:
        db_manager = _create_db_manager(schema=_expid)

    # Generate and save some jobs
    job_list = generate_job_list(as_exp.as_conf, db_manager)
    job_list.save_jobs()

    # this returns the raw tuple, convert to dict for easier testing
    job_selected = dict(db_manager.select_job_by_name("a01f_LOCAL_SETUP"))
    assert job_selected is not None
    assert job_selected['name'] == "a01f_LOCAL_SETUP"
    assert job_selected['section'] == "LOCAL_SETUP"
    assert job_selected['status'] == "READY"
    assert job_selected['script_name'] == "a01f_LOCAL_SETUP.cmd"
    assert job_selected['priority'] == 0

    job_load = db_manager.load_job_by_name("a01f_LOCAL_SETUP")
    assert job_load is not None
    assert job_load['name'] == "a01f_LOCAL_SETUP"
    assert job_load['section'] == "LOCAL_SETUP"
    assert job_load['status'] == "READY"
    assert job_load['script_name'] == "a01f_LOCAL_SETUP.cmd"
    assert job_load['priority'] == 0


@pytest.mark.parametrize(
    'preview',
    [True, False]
)
def test_load_wrapper(
        tmp_path: Path,
        preview: bool,
        as_db: str,
        _expid: str,
        as_exp: Any,
):
    exp_path = Path(BasicConfig.LOCAL_ROOT_DIR) / _expid
    db_path = exp_path / "db"
    if as_db == 'sqlite':
        db_manager = _create_db_manager(Path(db_path, 'tests.db'))
    else:
        db_manager = _create_db_manager(schema=_expid)

    # Generate and save some jobs
    job_list = generate_job_list(as_exp.as_conf, db_manager)
    for job in job_list.job_list:
        job.status = 2
    job_list.save_jobs()

    # prepare wrapper
    package_name = "test_package"
    wrapper_info_dict = [{
        "name": package_name,
        "id": 1,
        "script_name": "wrapper_script.sh",
        "status": 1,
        "local_logs_out": None,
        "local_logs_err": None,
        "remote_logs_out": None,
        "remote_logs_err": None,
        "updated_log": True,
        "platform_name": "test_platform",
        "wallclock": "01:00",
        "num_processors": "4",
        "type": 0,
        "sections": None,
        "method": None,
    }]

    inner_jobs_data = []
    for job in job_list.job_list[:3]:
        inner_jobs_data.append({
            "package_id": 1,
            "package_name": package_name,
            "job_name": job.name,
            "timestamp": "2023-01-01T00:00:00"
        })

    wrappers = [(wrapper_info_dict, inner_jobs_data)]

    db_manager.save_wrappers(wrappers, preview=preview)

    un_mapped_wrapper_info, un_mapped_inner_jobs = db_manager.load_wrappers(preview=preview, job_list=job_list.job_list)
    assert len(un_mapped_wrapper_info) == 1
    assert len(un_mapped_inner_jobs) == 3
    for wrapper in un_mapped_wrapper_info:
        wrapper = dict(wrapper)
        assert wrapper['name'] == package_name
        assert wrapper['script_name'] == "wrapper_script.sh"
        assert wrapper['status'] == 1
        assert wrapper['platform_name'] == "test_platform"
        assert wrapper['wallclock'] == "01:00"
        assert wrapper['num_processors'] == 4
        assert wrapper['type'] == '0'
        assert wrapper['updated_log'] is True
        assert wrapper['sections'] is None
        assert wrapper['method'] is None
    for inner_job in un_mapped_inner_jobs:
        inner_job = dict(inner_job)
        assert inner_job['package_name'] == package_name
        assert inner_job['timestamp'] == "2023-01-01T00:00:00"
        assert inner_job['job_name'] in [job.name for job in job_list.job_list[:3]]


def test_clear_unused_nodes(
        tmp_path: Path,
        as_db: str,
        as_exp: Any,
        _expid: str
):
    """
    Test the clear_unused_nodes method which removes jobs from the database based on configuration differences.

    :param tmp_path: Temporary directory path
    :type tmp_path: Path
    :param as_exp.as_conf: Fixture to create a test configuration
    :type as_exp.as_conf: Callable
    """
    exp_path = Path(BasicConfig.LOCAL_ROOT_DIR) / _expid
    db_path = exp_path / "db"
    if as_db == 'sqlite':
        db_manager = _create_db_manager(Path(db_path, 'tests.db'))
    else:
        db_manager = _create_db_manager(schema=_expid)

    job_list = generate_job_list(as_exp.as_conf, db_manager)

    test_jobs = [
        Job(loaded_data={
            'chunk': None, 'current_checkpoint_step': 0, 'date': None, 'date_split': None,
            'frequency': None, 'id': 0, 'name': 'removed_section_job', 'section': 'REMOVED_SECTION',
            'script_name': 'test.cmd', 'split': -1, 'splits': -1, 'status': 'WAITING',
            'finish_time_timestamp': None, 'local_logs_err': None, 'local_logs_out': None,
            'max_checkpoint_step': 0, 'packed': False, 'platform_name': None, 'priority': 0,
            'ready_date': None, 'remote_logs_err': None, 'remote_logs_out': None,
            'start_time': None, 'start_time_timestamp': None, 'submit_time_timestamp': None,
            'synchronize': None, 'updated_log': False, 'member': None
        }),
        Job(loaded_data={
            'chunk': 10, 'current_checkpoint_step': 0, 'date': "2000-01-01", 'date_split': None,
            'frequency': None, 'id': 0, 'name': 'chunk_limit_job', 'section': 'MODIFIED_SECTION',
            'script_name': 'test.cmd', 'split': -1, 'splits': -1, 'status': 'WAITING',
            'finish_time_timestamp': None, 'local_logs_err': None, 'local_logs_out': None,
            'max_checkpoint_step': 0, 'packed': False, 'platform_name': None, 'priority': 0,
            'ready_date': None, 'remote_logs_err': None, 'remote_logs_out': None,
            'start_time': None, 'start_time_timestamp': None, 'submit_time_timestamp': None,
            'synchronize': None, 'updated_log': False, 'member': 'fc0'
        }),
        Job(loaded_data={
            'chunk': 1, 'current_checkpoint_step': 0, 'date': '2000-01-01', 'date_split': None,
            'frequency': None, 'id': 0, 'name': 'split_limit_job', 'section': 'MODIFIED_SECTION',
            'script_name': 'test.cmd', 'split': 5, 'splits': 5, 'status': 'WAITING',
            'finish_time_timestamp': None, 'local_logs_err': None, 'local_logs_out': None,
            'max_checkpoint_step': 0, 'packed': False, 'platform_name': None, 'priority': 0,
            'ready_date': None, 'remote_logs_err': None, 'remote_logs_out': None,
            'start_time': None, 'start_time_timestamp': None, 'submit_time_timestamp': None,
            'synchronize': None, 'updated_log': False, 'member': 'fc0'
        }),
        Job(loaded_data={
            'chunk': 1, 'current_checkpoint_step': 0, 'date': '2000-12-31', 'date_split': None,
            'frequency': None, 'id': 0, 'name': 'date_removed_job', 'section': 'MODIFIED_SECTION',
            'script_name': 'test.cmd', 'split': 1, 'splits': 1, 'status': 'WAITING',
            'finish_time_timestamp': None, 'local_logs_err': None, 'local_logs_out': None,
            'max_checkpoint_step': 0, 'packed': False, 'platform_name': None, 'priority': 0,
            'ready_date': None, 'remote_logs_err': None, 'remote_logs_out': None,
            'start_time': None, 'start_time_timestamp': None, 'submit_time_timestamp': None,
            'synchronize': None, 'updated_log': False, 'member': 'fc0'
        }),
        Job(loaded_data={
            'chunk': 1, 'current_checkpoint_step': 0, 'date': '2000-01-01', 'date_split': None,
            'frequency': None, 'id': 0, 'name': 'member_removed_job', 'section': 'MODIFIED_SECTION',
            'script_name': 'test.cmd', 'split': 1, 'splits': 1, 'status': 'WAITING',
            'finish_time_timestamp': None, 'local_logs_err': None, 'local_logs_out': None,
            'max_checkpoint_step': 0, 'packed': False, 'platform_name': None, 'priority': 0,
            'ready_date': None, 'remote_logs_err': None, 'remote_logs_out': None,
            'start_time': None, 'start_time_timestamp': None, 'submit_time_timestamp': None,
            'synchronize': None, 'updated_log': False, 'member': 'fc2'
        }),
        Job(loaded_data={
            'chunk': 1, 'current_checkpoint_step': 0, 'date': '2000-01-01', 'date_split': None,
            'frequency': None, 'id': 0, 'name': 'safe_job', 'section': 'MODIFIED_SECTION',
            'script_name': 'test.cmd', 'split': 1, 'splits': 1, 'status': 'WAITING',
            'finish_time_timestamp': None, 'local_logs_err': None, 'local_logs_out': None,
            'max_checkpoint_step': 0, 'packed': False, 'platform_name': None, 'priority': 0,
            'ready_date': None, 'remote_logs_err': None, 'remote_logs_out': None,
            'start_time': None, 'start_time_timestamp': None, 'submit_time_timestamp': None,
            'synchronize': None, 'updated_log': False, 'member': 'fc0'
        })
    ]

    for job in test_jobs:
        job_list.add_job(job)
    job_list.save_jobs()

    # Verify all jobs are saved
    for job in test_jobs:
        loaded_job = db_manager.load_job_by_name(job.name)
        assert loaded_job['name'] == job.name

    differences = {
        'REMOVED_SECTION': {
            'status': 'removed'
        },
        'MODIFIED_SECTION': {
            'status': 'modified',
            'numchunks': 5,  # Jobs with chunk > 5 should be removed
            'splits': 3,  # Jobs with split > 3 should be removed
            'datelist': '20000101 20000201',  # Only these dates are allowed
            'members': 'fc0 fc1'  # Only these members are allowed
        }
    }

    db_manager.clear_unused_nodes(differences)

    jobs_should_be_deleted = [
        'removed_section_job',  # Section removed
        'chunk_limit_job',  # Chunk > 5
        'split_limit_job',  # Split > 3
        'date_removed_job',  # Date not in datelist
        'member_removed_job',  # Member not in members list
    ]

    jobs_should_remain = [
        'safe_job',
    ]

    for job_name in jobs_should_be_deleted:
        loaded_job = db_manager.load_job_by_name(job_name)
        assert loaded_job is None, f"Job {job_name} should have been deleted"

    for job_name in jobs_should_remain:
        loaded_job = db_manager.load_job_by_name(job_name)
        assert loaded_job is not None, f"Job {job_name} should not have been deleted"
        assert loaded_job['name'] == job_name


def test_backup_and_restore(monkeypatch, tmp_path, _expid, as_db, as_exp: Any):
    """" Test backup of database and restore it afterwards. """

    exp_path = Path(BasicConfig.LOCAL_ROOT_DIR) / _expid
    db_path = exp_path / "db"
    if as_db == 'sqlite':
        db_manager = _create_db_manager(Path(db_path, 'tests.db'))
    else:
        db_manager = _create_db_manager(schema=_expid)
        # TODO: not implemented
        return 0
    # Create tables
    jobs_table = get_table_from_name(schema=db_manager.schema, table_name=JobsTable.name)
    edges_table = get_table_from_name(schema=db_manager.schema, table_name=ExperimentStructureTable.name)
    db_manager.create_table(jobs_table.name)
    db_manager.create_table(edges_table.name)
    # Insert some data
    sample_job = {
        'chunk': None, 'current_checkpoint_step': 0, 'date': None, 'date_split': None,
        'finish_time_timestamp': None, 'frequency': None, 'id': 0,
        'name': f'{_expid}_LOCAL_SETUP', 'section': 'LOCAL_SETUP',
        'script_name': f'{_expid}_LOCAL_SETUP', 'split': -1, 'splits': -1,
        'status': 'READY', 'local_logs_err': None, 'local_logs_out': None,
        'max_checkpoint_step': 0, 'packed': False, 'platform_name': None,
        'priority': 0, 'ready_date': None, 'remote_logs_err': None,
        'remote_logs_out': None, 'start_time': None,
        'start_time_timestamp': None, 'submit_time_timestamp': None,
        'synchronize': None, 'updated_log': False, 'member': None
    }
    db_manager.insert(jobs_table.name, sample_job)
    sample_edge = {
        'e_from': f'{_expid}_LOCAL_SETUP', 'e_to': f'{_expid}_REMOTE_SETUP', 'from_step': 0,
        'min_trigger_status': 'COMPLETED', 'completion_status': 'WAITING', 'fail_ok': True
    }
    db_manager.insert(edges_table.name, sample_edge)

    # Backup

    db_manager.backup()

    # Clear tables
    db_manager.drop_table(jobs_table.name)
    db_manager.drop_table(edges_table.name)

    # Restore
    assert 0 == db_manager.restore()

    loaded_job = db_manager.load_job_by_name(f'{_expid}_LOCAL_SETUP')
    assert loaded_job is not None
    assert loaded_job['name'] == f'{_expid}_LOCAL_SETUP'


# -- Tests for detecting changes in dependencies -- #


DEPENDENCIES_CHANGED_DATA = [
    {
        "EXPERIMENT": {
            "DATELIST": "20000101 20010101",
            "MEMBERS": "fc0 fc1",
            "CHUNKSIZEUNIT": "month",
            "CHUNKSIZE": "4",
            "NUMCHUNKS": "1",
            "CHUNKINI": "",
            "CALENDAR": "standard",
        }
    },
    {
        "EXPERIMENT": {
            "DATELIST": "20000101 20010101",
            "MEMBERS": "fc0 fc1",
            "CHUNKSIZEUNIT": "month",
            "CHUNKSIZE": "4",
            "NUMCHUNKS": "3",
            "CHUNKINI": "",
            "CALENDAR": "standard",
        }
    },
    {
        "EXPERIMENT": {
            "DATELIST": "20000101 20010101 20020101",
            "MEMBERS": "fc0 fc1",
            "CHUNKSIZEUNIT": "month",
            "CHUNKSIZE": "4",
            "NUMCHUNKS": "2",
            "CHUNKINI": "",
            "CALENDAR": "standard",
        }
    },
    {
        "EXPERIMENT": {
            "DATELIST": "20000101",
            "MEMBERS": "fc0 fc1",
            "CHUNKSIZEUNIT": "month",
            "CHUNKSIZE": "4",
            "NUMCHUNKS": "2",
            "CHUNKINI": "",
            "CALENDAR": "standard",
        }
    },
    {
        "EXPERIMENT": {
            "DATELIST": "19990101 19990131",
            "MEMBERS": "fc0 fc1",
            "CHUNKSIZEUNIT": "month",
            "CHUNKSIZE": "4",
            "NUMCHUNKS": "2",
            "CHUNKINI": "",
            "CALENDAR": "standard",
        }
    },
    {
        "EXPERIMENT": {
            "DATELIST": "20000101 20010101",
            "MEMBERS": "fc0",
            "CHUNKSIZEUNIT": "month",
            "CHUNKSIZE": "4",
            "NUMCHUNKS": "2",
            "CHUNKINI": "",
            "CALENDAR": "standard",
        }
    },
    {
        "EXPERIMENT": {
            "DATELIST": "20000101 20010101",
            "MEMBERS": "fc0 fc1 fc2",
            "CHUNKSIZEUNIT": "month",
            "CHUNKSIZE": "4",
            "NUMCHUNKS": "2",
            "CHUNKINI": "",
            "CALENDAR": "standard",
        }
    },
    {
        "EXPERIMENT": {
            "DATELIST": "20000101 20010101",
            "MEMBERS": "fcA fcB",
            "CHUNKSIZEUNIT": "month",
            "CHUNKSIZE": "4",
            "NUMCHUNKS": "2",
            "CHUNKINI": "",
            "CALENDAR": "standard",
        }
    },
    {
        "JOBS": {
            "vjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "vjob-1",
                "wallclock": "00:01",
                "splits": 3,
            },
            "hjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "hjob-1",
                "wallclock": "00:01",
                "splits": 3,
            },
            "vhjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "vhjob-1",
                "wallclock": "00:01",
                "splits": 3,
            },
            "hvjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "hvjob-1",
                "wallclock": "00:01",
                "splits": 3,
            },
            "job": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "job-1",
                "wallclock": "00:01",
                "splits": 3,
            }
        }
    },
    {
        "JOBS": {
            "vjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "vjob-1",
                "wallclock": "00:01",
                "splits": 3,
            },
            "hjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "hjob-1",
                "wallclock": "00:01",
                "splits": 3,
            },
            "vhjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "vhjob-1",
                "wallclock": "00:01",
                "splits": 3,
            },
            "hvjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "hvjob-1",
                "wallclock": "00:01",
                "splits": 3,
            },
            "job": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "job-1",
                "wallclock": "00:01",
                "splits": 3,
            }
        }
    },
    {
        "JOBS": {
            "vjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "vjob-1",
                "wallclock": "00:01",
                "splits": 3,
            },
            "hjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "hjob-1",
                "wallclock": "00:01",
                "splits": 3,
            },
            "vhjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "vhjob-1",
                "wallclock": "00:01",
                "splits": 3,
            },
            "hvjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "hvjob-1",
                "wallclock": "00:01",
                "splits": 3,
            },
            "job": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "job-1",
                "wallclock": "00:01",
                "splits": 3,
            }
        }
    },
    {
        "JOBS": {
            "hjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "hjob-1",
                "wallclock": "00:01",
                "splits": 2,
            },
            "vhjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "vhjob-1",
                "wallclock": "00:01",
                "splits": 2,
            },
            "vjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "vjob-1",
                "wallclock": "00:01",
                "splits": 2,
            },
            "hvjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "hvjob-1",
                "wallclock": "00:01",
                "splits": 2,
            },
        }
    },
    {

        "JOBS": {
            "hjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "hjob-1",
                "wallclock": "00:01",
                "splits": 2,
            },
            "vhjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "vhjob-1",
                "wallclock": "00:01",
                "splits": 2,
            },
            "hvjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "hvjob-1",
                "wallclock": "00:01",
                "splits": 2,
            },
            "vjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "vjob-1",
                "wallclock": "00:01",
                "splits": 2,
            },
            "newjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "newjob-1",
                "wallclock": "00:01",
                "splits": 2,
            },
            "job": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "job-1",
                "wallclock": "00:01",
                "splits": 2,
            }
        }
    },
    {
        "JOBS": {
            "hjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "hjob-1",
                "wallclock": "00:01",
                "splits": 2,
            },
            "vhjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "vhjob-1",
                "wallclock": "00:01",
                "splits": 2,
            },
            "vjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "vjob-1",
                "wallclock": "00:01",
                "splits": 2,
            },
            "hvjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "hvjob-1",
                "wallclock": "00:01",
                "splits": 2,
            },
            "job": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "vjob",
                "wallclock": "00:01",
                "splits": 2,
            }
        }
    },
    {
        "JOBS": {
            "hjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "hjob-1",
                "wallclock": "00:01",
                "splits": 2,
            },
            "vhjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "vhjob-1",
                "wallclock": "00:01",
                "splits": 2,
            },
            "vjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "vjob-1",
                "wallclock": "00:01",
                "splits": 2,
            },
            "hvjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "hvjob-1",
                "wallclock": "00:01",
                "splits": 2,
            },
            "job": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "date",
                "DEPENDENCIES": "job-1",
                "wallclock": "00:01",
                "splits": 2,
            }
        }
    },
    {
        "JOBS": {
            "hjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "hjob-1",
                "wallclock": "00:01",
                "splits": 2,
            },
            "vhjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "vhjob-1",
                "wallclock": "00:01",
                "splits": 2,
            },
            "vjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "vjob-1",
                "wallclock": "00:01",
                "splits": 2,
            },
            "hvjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "hvjob-1",
                "wallclock": "00:01",
                "splits": 2,
            },
            "job": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "member",
                "DEPENDENCIES": "job-1",
                "wallclock": "00:01",
                "splits": 2,
            }
        }
    },
    {
        "JOBS": {
            "hjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "hjob-1",
                "wallclock": "00:01",
                "splits": 2,
            },
            "vhjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "vhjob-1",
                "wallclock": "00:01",
                "splits": 2,
            },
            "vjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "vjob-1",
                "wallclock": "00:01",
                "splits": 2,
            },
            "hvjob": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "chunk",
                "DEPENDENCIES": "hvjob-1",
                "wallclock": "00:01",
                "splits": 2,
            },
            "job": {
                "SCRIPT": "echo 'Hello World'",
                "RUNNING": "once",
                "DEPENDENCIES": "job-1",
                "wallclock": "00:01",
                "splits": 2,
            }
        }
    }
]

DEPENDENCIES_IDS = [
    "fewer_chunks",
    "more_chunks",
    "more_datelist",
    "fewer_datelist",
    "change_datelist",
    "fewer_members",
    "more_members",
    "change_member_names",
    "more_splits",
    "fewer_splits",
    "remove_splits",
    "remove_job",
    "add_new_job",
    "change_dependencies",
    "running_type_change_date",
    "running_type_change_member",
    "running_type_change_once",

]


@pytest.mark.parametrize(
    "changed_data",
    [
        pytest.param(
            DEPENDENCIES_CHANGED_DATA[i],
            id=DEPENDENCIES_IDS[i]
        )
        for i in range(len(DEPENDENCIES_CHANGED_DATA))
    ]
)
def test_with_createcw_command_differences(
        as_exp: Any,
        changed_data: Dict[str, Any],
        tmp_path: Path,
        as_db: str,
        _expid: str,
) -> None:
    """
    Integration test to verify database updates.
    The experiment is created with an initial configuration, and then recreated with modifications to test
    So we're testing the Autosubmit ability to detect and handle changes.
    :param as_exp: new expid
    :param changed_data: Dictionary containing configuration changes to be applied
    :param tmp_path: Temporary directory path
    :param as_db: Fixture to set up the database

    """
    fixed_data = dict(
        CONFIG={
            'AUTOSUBMIT_VERSION': 4.2,
            'MAXWAITINGJOBS': 100,
            'TOTALJOBS': 100,
            'SAFETYSLEEPTIME': 0,
            'RETRIALS': 0,
        },
        MAIL={
            'NOTIFICATIONS': False,
            'TO': '',
        },
        STORAGE={
            'TYPE': f"{as_db}",
            'COPY_REMOTE_LOGS': True,
        },
        DEFAULT={
            'EXPID': _expid,
            'HPCARCH': 'TEST_SLURM',
        },
        PROJECT={
            'PROJECT_TYPE': 'none',
            'PROJECT_DESTINATION': '',
        },
        GIT={
            'PROJECT_ORIGIN': '',
            'PROJECT_BRANCH': '',
            'PROJECT_COMMIT': '',
            'PROJECT_SUBMODULES': '',
            'FETCH_SINGLE_BRANCH': True,
        },
        SVN={
            'PROJECT_URL': '',
            'PROJECT_REVISION': '',
        },
        LOCAL={
            'PROJECT_PATH': '',
        },
        PROJECT_FILES={
            'FILE_PROJECT_CONF': '',
            'FILE_JOBS_CONF': '',
            'JOB_SCRIPTS_TYPE': '',
        },
        RERUN={
            'RERUN': False,
            'RERUN_JOBLIST': '',
        },
        PLATFORMS={
            'TEST_SLURM': {
                'TYPE': 'slurm',
                'ADD_PROJECT_TO_HOST': 'False',
                'HOST': '127.0.0.1',
                'MAX_WALLCLOCK': '48:00',
                'PROJECT': 'group',
                'QUEUE': 'gp_debug',
                'SCRATCH_DIR': '/tmp/scratch',
                'TEMP_DIR': '',
                'USER': 'root',
                'PROCESSORS': '1',
                'MAX_PROCESSORS': '128',
                'PROCESSORS_PER_NODE': '128',
            }
        },
        WRAPPERS={
            'WRAPPERV': {
                'TYPE': 'vertical',
                'JOBS_IN_WRAPPER': "vjob"
            },
            'WRAPPERH': {
                'TYPE': 'horizontal',
                'JOBS_IN_WRAPPER': "hjob"
            },
            'WRAPPERVH': {
                'TYPE': 'vertical-horizontal',
                'JOBS_IN_WRAPPER': "vhjob"
            },
            'WRAPPERHV': {
                'TYPE': 'horizontal-vertical',
                'JOBS_IN_WRAPPER': "hvjob"
            },
        }
    )
    mutable_experiment_wrappers = dict(
        EXPERIMENT={
            'DATELIST': '20000101 20010101',
            'MEMBERS': 'fc0 fc1',
            'CHUNKSIZEUNIT': 'month',
            'CHUNKSIZE': '4',
            'NUMCHUNKS': '2',
            'CHUNKINI': '',
            'CALENDAR': 'standard',
        },
    )
    mutable_jobs = dict(
        JOBS={
            'job': {
                'SCRIPT': 'echo "Hello World"',
                'DEPENDENCIES': 'job-1',
                'RUNNING': 'chunk',
                'wallclock': '00:01',
                'splits': 2,
            },
            'vjob': {
                'SCRIPT': 'echo "Hello World"',
                'DEPENDENCIES': 'vjob-1',
                'RUNNING': 'chunk',
                'wallclock': '00:01',
                'splits': 2,
            },
            'hjob': {
                'SCRIPT': 'echo "Hello World"',
                'DEPENDENCIES': 'hjob-1',
                'RUNNING': 'chunk',
                'wallclock': '00:01',
                'splits': 2,
            },
            'vhjob': {
                'SCRIPT': 'echo "Hello World"',
                'DEPENDENCIES': 'vhjob-1',
                'RUNNING': 'chunk',
                'wallclock': '00:01',
                'splits': 2,
            },
            'hvjob': {
                'SCRIPT': 'echo "Hello World"',
                'DEPENDENCIES': 'hvjob-1',
                'RUNNING': 'chunk',
                'wallclock': '00:01',
                'splits': 2,
            },

        }
    )
    exp_path = Path(BasicConfig.LOCAL_ROOT_DIR) / _expid
    db_path = exp_path / "db"
    conf_dir = exp_path / 'conf'
    unified_data: Dict[str, Dict] = fixed_data | mutable_experiment_wrappers | mutable_jobs
    _init_test(as_exp, conf_dir, unified_data)
    if as_db == 'sqlite':
        db_manager = _create_db_manager(Path(db_path, 'job_list.db'))
    else:
        db_manager = _create_db_manager(schema=_expid)

    exit_code = as_exp.autosubmit.create(_expid, noplot=True, hide=False, force=True, check_wrappers=True)
    _assert_exit_code("SUCCESS", exit_code)
    once_sections = []
    member_sections = []
    date_sections = []
    chunk_sections = []
    for section in unified_data.get('JOBS', {}):
        if unified_data['JOBS'][section].get('RUNNING', '').lower() == 'once':
            once_sections.append(section)
        elif unified_data['JOBS'][section].get('RUNNING', '').lower() == 'member':
            member_sections.append(section)
        elif unified_data['JOBS'][section].get('RUNNING', '').lower() == 'date':
            date_sections.append(section)
        else:
            chunk_sections.append(section)
    expected_job_names = _get_expected_job_names(as_exp.expid, unified_data, once_sections, member_sections, date_sections, chunk_sections)
    db_jobs = db_manager.load_jobs(full_load=True)
    db_jobs = {job['name'].upper(): job for job in db_jobs}

    db_jobs_names = set(db_jobs.keys())
    expected_job_names_set = set(expected_job_names)
    missing_in_db = expected_job_names_set - db_jobs_names
    unexpected_in_db = db_jobs_names - expected_job_names_set

    assert len(missing_in_db) == 0, f"Missing jobs in DB: {missing_in_db}"
    assert len(unexpected_in_db) == 0, f"Unexpected jobs in DB: {unexpected_in_db}"

    new_data: Dict[str, Dict] = unified_data | changed_data
    _modify_data(as_exp.expid, conf_dir, new_data)

    exit_code = as_exp.autosubmit.create(_expid, noplot=True, hide=False, force=False, check_wrappers=True)
    _assert_exit_code("SUCCESS", exit_code)
    once_sections = []
    member_sections = []
    date_sections = []
    chunk_sections = []

    for section in new_data.get('JOBS', {}):
        if new_data['JOBS'][section].get('RUNNING', '').lower() == 'once':
            once_sections.append(section)
        elif new_data['JOBS'][section].get('RUNNING', '').lower() == 'member':
            member_sections.append(section)
        elif new_data['JOBS'][section].get('RUNNING', '').lower() == 'date':
            date_sections.append(section)
        else:
            chunk_sections.append(section)
    expected_job_names = _get_expected_job_names(as_exp.expid, new_data, once_sections, member_sections, date_sections, chunk_sections)
    db_jobs = db_manager.load_jobs(full_load=True)
    db_jobs = {job['name'].upper(): job for job in db_jobs}

    db_jobs_names = set(db_jobs.keys())
    expected_job_names_set = set(expected_job_names)
    missing_in_db = expected_job_names_set - db_jobs_names
    unexpected_in_db = db_jobs_names - expected_job_names_set
    assert len(missing_in_db) == 0, f"Missing jobs in DB: {missing_in_db}"
    assert len(unexpected_in_db) == 0, f"Unexpected jobs in DB: {unexpected_in_db}"
