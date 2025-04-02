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

import shutil
from copy import copy
from pathlib import Path
from random import randrange

import networkx
import pytest
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory
from networkx import DiGraph  # type: ignore

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_common import Type
from autosubmit.job.job_dict import DicJobs
from autosubmit.job.job_list import JobList
from autosubmit.job.job_list_persistence import JobListPersistencePkl

"""Tests for the ``JobList`` class."""

_EXPID = 'a000'


@pytest.fixture
def as_conf(autosubmit_config):
    return autosubmit_config(_EXPID, experiment_data={
        'JOBS': {},
        'PLATFORMS': {}
    })


@pytest.fixture(scope='function')
def setup_job_list(as_conf, tmpdir, mocker):
    job_list = JobList(_EXPID, as_conf, YAMLParserFactory(), JobListPersistencePkl())
    dummy_serial_platform = mocker.MagicMock()
    dummy_serial_platform.name = 'serial'
    dummy_platform = mocker.MagicMock()
    dummy_platform.serial_platform = dummy_serial_platform
    dummy_platform.name = 'dummy_platform'

    job_list._platforms = [dummy_platform]
    # add some jobs to the job list
    job = Job("job1", "1", Status.COMPLETED, 0)
    job.section = "SECTION1"
    job_list._job_list.append(job)
    job = Job("job2", "2", Status.WAITING, 0)
    job.section = "SECTION1"
    job_list._job_list.append(job)
    job = Job("job3", "3", Status.COMPLETED, 0)
    job.section = "SECTION2"
    job_list._job_list.append(job)
    return job_list


@pytest.fixture
def jobs_as_dict():
    return {
        Status.COMPLETED: [
            _create_dummy_job_with_status(Status.COMPLETED),
            _create_dummy_job_with_status(Status.COMPLETED),
            _create_dummy_job_with_status(Status.COMPLETED),
            _create_dummy_job_with_status(Status.COMPLETED)
        ],
        Status.SUBMITTED: [
            _create_dummy_job_with_status(Status.SUBMITTED),
            _create_dummy_job_with_status(Status.SUBMITTED),
            _create_dummy_job_with_status(Status.SUBMITTED)
        ],
        Status.RUNNING: [
            _create_dummy_job_with_status(Status.RUNNING),
            _create_dummy_job_with_status(Status.RUNNING)
        ],
        Status.QUEUING: [
            _create_dummy_job_with_status(Status.QUEUING)
        ],
        Status.FAILED: [
            _create_dummy_job_with_status(Status.FAILED),
            _create_dummy_job_with_status(Status.FAILED),
            _create_dummy_job_with_status(Status.FAILED),
            _create_dummy_job_with_status(Status.FAILED)
        ],
        Status.READY: [
            _create_dummy_job_with_status(Status.READY),
            _create_dummy_job_with_status(Status.READY),
            _create_dummy_job_with_status(Status.READY)
        ],
        Status.WAITING: [
            _create_dummy_job_with_status(Status.WAITING),
            _create_dummy_job_with_status(Status.WAITING)
        ],
        Status.UNKNOWN: [
            _create_dummy_job_with_status(Status.UNKNOWN)
        ]
    }


@pytest.fixture
def job_list(as_conf, mocker, jobs_as_dict):
    parameters = {'fake-key': 'fake-value',
                  'fake-key2': 'fake-value2'}
    as_conf.load_parameters = mocker.Mock(return_value=parameters)
    as_conf.default_parameters = {}
    joblist_persistence = JobListPersistencePkl()
    job_list = JobList(_EXPID, as_conf, YAMLParserFactory(), joblist_persistence)

    for status, jobs in jobs_as_dict.items():
        job_list._job_list.extend(jobs)
    return job_list


def _create_dummy_job_with_status(status):
    job_name = str(randrange(999999, 999999999))
    job_id = randrange(1, 999)
    job = Job(job_name, job_id, status, 0)
    job.type = randrange(0, 2)
    return job


@pytest.fixture
def empty_job_list(tmp_path, as_conf, mocker):
    def fn():
        parameters = {'fake-key': 'fake-value',
                      'fake-key2': 'fake-value2'}
        as_conf.load_parameters = mocker.Mock(return_value=parameters)
        as_conf.default_parameters = {}
        job_list = JobList(_EXPID, as_conf, as_conf.parser_factory, JobListPersistencePkl())
        pickle_directory = Path(as_conf.basic_config.LOCAL_ROOT_DIR, _EXPID, 'pkl')
        pickle_directory.mkdir(parents=True, exist_ok=True)
        job_list._persistence_path = str(pickle_directory)

        return job_list

    return fn


def test_load(mocker, as_conf, empty_job_list):
    date_list = ['fake-date1', 'fake-date2']
    member_list = ['fake-member1', 'fake-member2']
    num_chunks = 999
    parameters = {'fake-key': 'fake-value',
                  'fake-key2': 'fake-value2'}
    job_list = empty_job_list()
    job_list.changes = mocker.Mock(return_value=['random_section', 'random_section'])
    as_conf.detailed_deep_diff = mocker.Mock(return_value={})
    # act
    job_list.generate(
        as_conf=as_conf,
        date_list=date_list,
        member_list=member_list,
        num_chunks=num_chunks,
        chunk_ini=1,
        parameters=parameters,
        date_format='H',
        default_retrials=9999,
        default_job_type=Type.BASH,
        wrapper_jobs={},
        new=True,
        create=True,
    )
    job_list.save()
    # Test load
    job_list_to_load = empty_job_list()
    # chmod
    job_list_to_load.load(False)
    assert job_list_to_load._job_list == job_list._job_list
    job_list_to_load.load(True)
    assert job_list_to_load._job_list == job_list._job_list
    temp_dir = as_conf.basic_config.LOCAL_ROOT_DIR
    pickle_file = Path(temp_dir) / _EXPID / 'pkl' / f'job_list_{_EXPID}.pkl'
    pickle_file.chmod(0o000)
    # Works with pytest doesn't work in pipeline TODO enable this test
    # with assertRaises(AutosubmitCritical):
    #     job_list_to_load.load(False)
    job_list_to_load.load(True)
    assert job_list_to_load._job_list == job_list._job_list
    pickle_file.chmod(0o777)
    shutil.copy(str(pickle_file), f'{temp_dir}/{_EXPID}/pkl/job_list_{_EXPID}_backup.pkl')
    pickle_file.unlink()
    job_list_to_load.load(False)
    assert job_list_to_load._job_list == job_list._job_list
    job_list_to_load.load(True)
    assert job_list_to_load._job_list == job_list._job_list


def test_get_job_list_returns_the_right_list(job_list):
    other_job_list = job_list.get_job_list()
    assert job_list._job_list == other_job_list


@pytest.mark.parametrize(
    'status',
    [
        Status.COMPLETED,
        Status.SUBMITTED,
        Status.RUNNING,
        Status.QUEUING,
        Status.FAILED,
        Status.READY,
        Status.WAITING,
        Status.UNKNOWN
    ]
)
def test_get_function_returns(job_list, jobs_as_dict, status):
    """Test that the ``JobList`` object provided contains the correct jobs for each ``Status``."""
    status_text = Status.VALUE_TO_KEY[status].lower()
    # Here, ``Status.COMPLETED`` is ``int(5)``, so above we convert it to the text ``'COMPLETED'``.
    # Finally, we call the function ``job_list.get_completed()`` to retrieve completed jobs.
    # As the test is parametrized, we repeat for each state parameter.
    jobs_in_state = getattr(job_list, f'get_{status_text}')()
    expected_jobs_in_state = jobs_as_dict[status]

    assert len(jobs_in_state) == len(expected_jobs_in_state)
    for expected_job in expected_jobs_in_state:
        assert expected_job in jobs_in_state


def test_get_completed_returns_only_the_completed(job_list, jobs_as_dict):
    completed = job_list.get_completed()

    expected_completed = jobs_as_dict[Status.COMPLETED]

    assert 4 == len(expected_completed)
    for expected_job in expected_completed:
        assert expected_job in completed


def test_get_in_queue_returns_only_which_are_queuing_submitted_and_running(job_list, jobs_as_dict):
    in_queue = job_list.get_in_queue()

    queuing_job = jobs_as_dict[Status.QUEUING][0]
    running_job = jobs_as_dict[Status.RUNNING][0]
    running_job2 = jobs_as_dict[Status.RUNNING][1]
    submitted_job = jobs_as_dict[Status.SUBMITTED][0]
    submitted_job2 = jobs_as_dict[Status.SUBMITTED][1]
    submitted_job3 = jobs_as_dict[Status.SUBMITTED][2]
    unknown_job = jobs_as_dict[Status.UNKNOWN][0]

    assert 7 == len(in_queue)
    assert queuing_job in in_queue
    assert running_job in in_queue
    assert running_job2 in in_queue
    assert submitted_job in in_queue
    assert submitted_job2 in in_queue
    assert submitted_job3 in in_queue
    assert unknown_job in in_queue


def test_get_active_returns_only_which_are_in_queue_ready_and_unknown(job_list, jobs_as_dict):
    active = job_list.get_active()

    queuing_job = jobs_as_dict[Status.QUEUING][0]
    running_job = jobs_as_dict[Status.RUNNING][0]
    running_job2 = jobs_as_dict[Status.RUNNING][1]
    submitted_job = jobs_as_dict[Status.SUBMITTED][0]
    submitted_job2 = jobs_as_dict[Status.SUBMITTED][1]
    submitted_job3 = jobs_as_dict[Status.SUBMITTED][2]
    ready_job = jobs_as_dict[Status.READY][0]
    ready_job2 = jobs_as_dict[Status.READY][1]
    ready_job3 = jobs_as_dict[Status.READY][2]
    unknown_job = jobs_as_dict[Status.UNKNOWN][0]

    assert 10 == len(active)
    assert queuing_job in active
    assert running_job in active
    assert running_job2 in active
    assert submitted_job in active
    assert submitted_job2 in active
    assert submitted_job3 in active
    assert ready_job in active
    assert ready_job2 in active
    assert ready_job3 in active
    assert unknown_job in active


def test_get_job_by_name_returns_the_expected_job(job_list, jobs_as_dict):
    completed_jobs = jobs_as_dict[Status.COMPLETED]
    completed_job = completed_jobs[0]
    job = job_list.get_job_by_name(completed_job.name)

    assert completed_job == job


def test_sort_by_name_returns_the_list_of_jobs_well_sorted(job_list):
    sorted_by_name = job_list.sort_by_name()

    for i in range(len(sorted_by_name) - 1):
        assert sorted_by_name[i].name <= sorted_by_name[i + 1].name


def test_sort_by_id_returns_the_list_of_jobs_well_sorted(job_list):
    sorted_by_id = job_list.sort_by_id()

    for i in range(len(sorted_by_id) - 1):
        assert sorted_by_id[i].id <= sorted_by_id[i + 1].id


def test_sort_by_type_returns_the_list_of_jobs_well_sorted(job_list):
    sorted_by_type = job_list.sort_by_type()

    for i in range(len(sorted_by_type) - 1):
        assert sorted_by_type[i].type <= sorted_by_type[i + 1].type


def test_sort_by_status_returns_the_list_of_jobs_well_sorted(job_list):
    sorted_by_status = job_list.sort_by_status()

    for i in range(len(sorted_by_status) - 1):
        assert sorted_by_status[i].status <= sorted_by_status[i + 1].status


def test_that_create_method_makes_the_correct_calls(mocker, empty_job_list, as_conf):
    job_list = empty_job_list()
    job_list._create_jobs = mocker.Mock()
    job_list._add_dependencies = mocker.Mock()
    job_list.update_genealogy = mocker.Mock()
    job_list._job_list = [Job('random-name', 9999, Status.WAITING, 0),
                          Job('random-name2', 99999, Status.WAITING, 0)]
    date_list = ['fake-date1', 'fake-date2']
    member_list = ['fake-member1', 'fake-member2']
    num_chunks = 999
    chunk_list = list(range(1, num_chunks + 1))
    parameters = {'fake-key': 'fake-value',
                  'fake-key2': 'fake-value2'}
    graph = networkx.DiGraph()
    job_list.graph = graph

    as_conf.experiment_data = {
        'PLATFORMS': {
            'fake-platform': {
                'TYPE': 'ps',
                'NAME': 'fake-name',
                'USERNAME': 'fake-user'
            }
        }
    }
    as_conf.get_platform = mocker.Mock(return_value="fake-platform")
    # act
    mocker.patch('autosubmit.job.job.Job.update_parameters', return_value={})
    job_list.generate(
        as_conf=as_conf,
        date_list=date_list,
        member_list=member_list,
        num_chunks=num_chunks,
        chunk_ini=1,
        parameters=parameters,
        date_format='H',
        default_retrials=9999,
        default_job_type=Type.BASH,
        wrapper_jobs={},
        new=True,
        create=True,
    )

    # assert
    assert job_list.parameters == parameters
    assert job_list._date_list == date_list
    assert job_list._member_list == member_list
    assert job_list._chunk_list == list(range(1, num_chunks + 1))

    cj_args, cj_kwargs = job_list._create_jobs.call_args  # type: ignore
    assert 0 == cj_args[2]

    # _add_dependencies(date_list, member_list, chunk_list, dic_jobs, option="DEPENDENCIES"):

    job_list._add_dependencies.assert_called_once_with(date_list, member_list, chunk_list, cj_args[0])  # type: ignore
    # Adding flag update structure
    job_list.update_genealogy.assert_called_once_with()  # type: ignore

    # job doesn't have job.parameters anymore TODO
    # for job in job_list._job_list:
    #     assertEqual(parameters, job.parameters)


def test_that_create_job_method_calls_dic_jobs_method_with_increasing_priority(mocker):
    # arrange
    dic_mock = mocker.Mock()
    dic_mock.read_section = mocker.Mock()
    dic_mock.experiment_data = dict()
    dic_mock.experiment_data["JOBS"] = {'fake-section-1': {}, 'fake-section-2': {}}
    # act
    JobList._create_jobs(dic_mock, 0, Type.BASH)

    # arrange
    dic_mock.read_section.assert_any_call('fake-section-1', 0, Type.BASH)
    dic_mock.read_section.assert_any_call('fake-section-2', 1, Type.BASH)


def test_run_member(job_list, mocker, as_conf, empty_job_list):
    as_conf.experiment_data = {
        'PLATFORMS': {
            'fake-platform': {
                'TYPE': 'ps',
                'NAME': 'fake-name',
                'USER': 'fake-user'
            }
        }
    }
    as_conf.get_platform = mocker.MagicMock(return_value="fake-platform")
    job_list = empty_job_list()
    job_list._create_jobs = mocker.Mock()
    job_list._add_dependencies = mocker.Mock()
    job_list.update_genealogy = mocker.Mock()
    job_list._job_list = [
        Job('random-name', 9999, Status.WAITING, 0),
        Job('random-name2', 99999, Status.WAITING, 0)
    ]
    date_list = ['fake-date1', 'fake-date2']
    member_list = ['fake-member1', 'fake-member2']
    num_chunks = 2
    parameters = {
        'fake-key': 'fake-value',
        'fake-key2': 'fake-value2'
    }
    graph = networkx.DiGraph()
    as_conf.get_platform = mocker.Mock(return_value="fake-platform")
    job_list.graph = graph
    # act
    mocker.patch('autosubmit.job.job.Job.update_parameters', return_value={})
    job_list.generate(
        as_conf=as_conf,
        date_list=date_list,
        member_list=member_list,
        num_chunks=num_chunks,
        chunk_ini=1,
        parameters=parameters,
        date_format='H',
        default_retrials=1,
        default_job_type=Type.BASH,
        wrapper_jobs={},
        new=True,
        create=True,
    )
    job_list._job_list[0].member = "fake-member1"
    job_list._job_list[1].member = "fake-member2"
    job_list_aux = copy(job_list)
    job_list_aux.run_members = "fake-member1"
    # assert len of job_list_aux._job_list match only fake-member1 jobs
    assert len(job_list_aux._job_list) == 1
    job_list_aux = copy(job_list)
    job_list_aux.run_members = "not_exists"
    assert len(job_list_aux._job_list) == 0


def test_create_dictionary(job_list, mocker, as_conf, empty_job_list):
    parameters = {'fake-key': 'fake-value',
                  'fake-key2': 'fake-value2'}
    as_conf.experiment_data = {
        'JOBS': {
            'fake-section': parameters,
            'fake-section-2': parameters}
    }
    job_list = empty_job_list()
    job_list._create_jobs = mocker.Mock()
    job_list._add_dependencies = mocker.Mock()
    job_list.update_genealogy = mocker.Mock()
    job_list._job_list = [
        Job('random-name_fake-date1_fake-member1', 9999, Status.WAITING, 0),
        Job('random-name2_fake_date2_fake-member2', 99999, Status.WAITING, 0)
    ]
    for job in job_list._job_list:
        job.section = "fake-section"
    date_list = ['fake-date1', 'fake-date2']
    member_list = ['fake-member1', 'fake-member2']
    num_chunks = 2
    graph = networkx.DiGraph()
    job_list.graph = graph
    # act

    mock_get_submitter = mocker.patch('autosubmit.job.job_list._get_submitter', autospec=True)
    mock_submitter = mock_get_submitter.return_value
    mock_submitter.load_platforms = mocker.MagicMock()
    mock_submitter.load_platforms.return_value = ["fake-platform"]
    mock_submitter.platforms = None
    job_list.generate(
        as_conf=as_conf,
        date_list=date_list,
        member_list=member_list,
        num_chunks=num_chunks,
        chunk_ini=1,
        parameters=as_conf.load_parameters(),
        date_format='H',
        default_retrials=1,
        default_job_type=Type.BASH,
        wrapper_jobs={},
        new=True,
        create=True
    )
    job_list._job_list[0].section = "fake-section"
    job_list._job_list[0].date = "fake-date1"
    job_list._job_list[0].member = "fake-member1"
    job_list._job_list[0].chunk = 1
    wrapper_jobs = {"WRAPPER_FAKE_SECTION": 'fake-section'}
    num_chunks = 2
    chunk_ini = 1
    date_format = "day"
    default_retrials = 1
    job_list._get_date = mocker.Mock(return_value="fake-date1")

    # act
    job_list.create_dictionary(date_list, member_list, num_chunks, chunk_ini, date_format, default_retrials,
                               wrapper_jobs, as_conf)
    # assert
    assert len(job_list._ordered_jobs_by_date_member["WRAPPER_FAKE_SECTION"]["fake-date1"]["fake-member1"]) == 1


def test_generate_job_list_from_monitor_run(as_conf, mocker, empty_job_list):
    as_conf.experiment_data = {
        'DEFAULT': {
            'EXPID': _EXPID,
            'HPCARCH': 'ARM'
        },
        'JOBS': {
            'fake-section': {
                'file': 'fake-file',
                'running': 'once'
            },
            'fake-section2': {
                'file': 'fake-file2',
                'running': 'once'
            }
        },
        'PLATFORMS': {
            'fake-platform': {
                'type': 'fake-type',
                'name': 'fake-name',
                'user': 'fake-user'
            }
        }
    }

    date_list = ['fake-date1', 'fake-date2']
    member_list = ['fake-member1', 'fake-member2']
    num_chunks = 999
    parameters = {'fake-key': 'fake-value',
                  'fake-key2': 'fake-value2'}
    job_list = empty_job_list()
    job_list.changes = mocker.Mock(return_value=['random_section', 'random_section'])
    as_conf.detailed_deep_diff = mocker.Mock(return_value={})
    mocker.patch('autosubmit.job.job.Job.update_parameters', return_value={})
    # act
    job_list.generate(
        as_conf=as_conf,
        date_list=date_list,
        member_list=member_list,
        num_chunks=num_chunks,
        chunk_ini=1,
        parameters=parameters,
        date_format='H',
        default_retrials=9999,
        default_job_type=Type.BASH,
        wrapper_jobs={},
        new=True,
        create=True,
    )
    job_list.save()
    job_list2 = empty_job_list()
    # act
    job_list2.generate(
        as_conf=as_conf,
        date_list=date_list,
        member_list=member_list,
        num_chunks=num_chunks,
        chunk_ini=1,
        parameters=parameters,
        date_format='H',
        default_retrials=9999,
        default_job_type=Type.BASH,
        wrapper_jobs={},
        new=False,
        create=True,
    )

    # return False
    job_list2.update_from_file = mocker.Mock()
    job_list2.update_from_file.return_value = False
    job_list2.update_list(as_conf, False)

    # check that name is the same
    for index, job in enumerate(job_list._job_list):
        assert job_list2._job_list[index].name == job.name
    # check that status is the same
    for index, job in enumerate(job_list._job_list):
        assert job_list2._job_list[index].status == job.status
    assert job_list2._date_list == job_list._date_list
    assert job_list2._member_list == job_list._member_list
    assert job_list2._chunk_list == job_list._chunk_list
    assert job_list2.parameters == job_list.parameters
    job_list3 = empty_job_list()
    job_list3.generate(
        as_conf=as_conf,
        date_list=date_list,
        member_list=member_list,
        num_chunks=num_chunks,
        chunk_ini=1,
        parameters=parameters,
        date_format='H',
        default_retrials=9999,
        default_job_type=Type.BASH,
        wrapper_jobs={},
        new=False,
    )
    job_list3.update_from_file = mocker.Mock()
    job_list3.update_from_file.return_value = False
    job_list3.update_list(as_conf, False)
    # assert
    # check that name is the same
    for index, job in enumerate(job_list._job_list):
        assert job_list3._job_list[index].name == job.name
    # check that status is the same
    for index, job in enumerate(job_list._job_list):
        assert job_list3._job_list[index].status == job.status
    assert job_list3._date_list == job_list._date_list
    assert job_list3._member_list == job_list._member_list
    assert job_list3._chunk_list == job_list._chunk_list
    assert job_list3.parameters == job_list.parameters
    # DELETE WHEN EDGELESS TEST
    job_list3._job_list[0].dependencies = {"not_exist": None}
    job_list3._delete_edgeless_jobs()
    assert len(job_list3._job_list) == 1
    # Update Mayor Version test ( 4.0 -> 4.1)
    job_list3.graph = DiGraph()
    job_list3.save()
    job_list3 = empty_job_list()
    job_list3.update_genealogy = mocker.Mock(wraps=job_list3.update_genealogy)
    job_list3.generate(
        as_conf=as_conf,
        date_list=date_list,
        member_list=member_list,
        num_chunks=num_chunks,
        chunk_ini=1,
        parameters=parameters,
        date_format='H',
        default_retrials=9999,
        default_job_type=Type.BASH,
        wrapper_jobs={},
        new=False,
        create=True,
    )
    # assert update_genealogy called with right values
    # When using an 4.0 experiment, the pkl has to be recreated and act as a new one.
    job_list3.update_genealogy.assert_called_once_with()  # type: ignore

    # Test when the graph previous run has more jobs than the current run
    job_list3.graph.add_node("fake-node", job=job_list3._job_list[0])
    job_list3.save()
    job_list3.generate(
        as_conf=as_conf,
        date_list=date_list,
        member_list=member_list,
        num_chunks=num_chunks,
        chunk_ini=1,
        parameters=parameters,
        date_format='H',
        default_retrials=9999,
        default_job_type=Type.BASH,
        wrapper_jobs={},
        new=False,
    )
    assert len(job_list3.graph.nodes) == len(job_list3._job_list)
    # Test when the graph previous run has fewer jobs than the current run
    as_conf.experiment_data["JOBS"]["fake-section3"] = dict()
    as_conf.experiment_data["JOBS"]["fake-section3"]["file"] = "fake-file3"
    as_conf.experiment_data["JOBS"]["fake-section3"]["running"] = "once"
    job_list3.generate(
        as_conf=as_conf,
        date_list=date_list,
        member_list=member_list,
        num_chunks=num_chunks,
        chunk_ini=1,
        parameters=parameters,
        date_format='H',
        default_retrials=9999,
        default_job_type=Type.BASH,
        wrapper_jobs={},
        new=False,
    )
    assert len(job_list3.graph.nodes) == len(job_list3._job_list)
    for node in job_list3.graph.nodes:
        # if name is in the job_list
        if node in [job.name for job in job_list3._job_list]:
            assert job_list3.graph.nodes[node]["job"] in job_list3._job_list


def test_find_and_delete_redundant_relations(job_list, mocker):
    problematic_jobs = {'SECTION': {'CHILD': ['parents_names', 'parents_names1', 'parents_names2'],
                                    'CHILD2': ['parents_names3', 'parents_names4']}}
    mock_job_list = mocker.patch('autosubmit.job.job_list.DiGraph.has_successor')
    # TODO: looks like a we have one assert here that's not called, either the last one in try, or the one in except
    try:
        mock_job_list.return_value = True
        assert job_list.find_and_delete_redundant_relations(problematic_jobs) is None
        mock_job_list.return_value = False
        assert job_list.find_and_delete_redundant_relations(problematic_jobs) is None
    except Exception as e:
        assert (f'Find and delete redundant relations ran into an '
                f'Error deleting the relationship between parent and child: {e}')


def test_normalize_to_filters(job_list):
    """
    validating behaviour of _normalize_to_filters
    """
    dict_filter = [
        {"DATES_TO": ""},
        {"DATES_TO": "all"},
        {"DATES_TO": "20020205,[20020207:20020208],"},
        {"DATES_TO": ",20020205,[20020207:20020208]"}
        # ,{"DATES_TO": 123} # Error Case
    ]
    filter_type = "DATES_TO"

    for filter_to in dict_filter:
        try:
            job_list._normalize_to_filters(filter_to, filter_type)
        except Exception as e:
            print(f'Unexpected exception raised: {e}')
            assert not bool(e)


def test_manage_dependencies(as_conf, empty_job_list):
    """testing function _manage_dependencies from job_list."""
    dependencies_keys = {
        'dummy=1': {'test', 'test2'},
        'dummy-2': {'test', 'test2'},
        'dummy+3': "",
        'dummy*4': "",
        'dummy?5': ""
    }

    job_list = empty_job_list()

    job = {
        'dummy':
            {
                'dummy': 'SIM.sh',
                'RUNNING': 'once'
            },
        'RUNNING': 'once',
        'dummy*4': {}
    }

    dic_jobs_fake = DicJobs(
        ['fake-date1', 'fake-date2'],
        ['fake-member1', 'fake-member2'],
        list(range(2, 10 + 1)),
        'H',
        1,
        as_conf)
    dic_jobs_fake.experiment_data["JOBS"] = job
    dependency = job_list._manage_dependencies(dependencies_keys, dic_jobs_fake)
    assert len(dependency) == 3
    for job in dependency:
        assert job in dependencies_keys


@pytest.mark.parametrize(
    "section_list, banned_jobs, get_only_non_completed, expected_length, expected_section",
    [
        (["SECTION1"], [], False, 2, "SECTION1"),
        (["SECTION2"], [], False, 1, "SECTION2"),
        (["SECTION1"], [], True, 1, "SECTION1"),
        (["SECTION2"], [], True, 0, "SECTION2"),
        (["SECTION1"], ["job1"], True, 1, "SECTION1"),
    ],
    ids=[
        "all_jobs_in_section1",
        "all_jobs_in_section2",
        "non_completed_jobs_in_section1",
        "non_completed_jobs_in_section2",
        "ban_job1"
    ]
)
def test_get_jobs_by_section(setup_job_list, section_list, banned_jobs, get_only_non_completed, expected_length,
                             expected_section):
    result = setup_job_list.get_jobs_by_section(section_list, banned_jobs, get_only_non_completed)
    assert len(result) == expected_length
    assert all(job.section == expected_section for job in result)


@pytest.mark.parametrize(
    'make_exception,seconds',
    [
        (True, True),
        (False, True),
        (True, False),
        (False, False)
    ]
)
def test_retrieve_times(job_list, jobs_as_dict, tmp_path, make_exception, seconds):
    """testing function retrieve_times from job_list."""

    for completed_jobs in jobs_as_dict.values():
        for job in completed_jobs:
            job = job_list.get_job_by_name(job.name)
            retrieve_data = job_list.retrieve_times(job.status, job.name, job._tmp_path, make_exception=make_exception,
                                                    job_times=None, seconds=seconds, job_data_collection=None)
            assert retrieve_data.name == job.name
            assert retrieve_data.status == Status.VALUE_TO_KEY[job.status]
