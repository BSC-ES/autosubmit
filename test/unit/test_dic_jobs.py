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

import math
import pytest
from datetime import datetime
from mock import Mock

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_common import Type
from autosubmit.job.job_dict import DicJobs
from autosubmit.job.job_list import JobList
from autosubmit.job.job_list_persistence import JobListPersistenceDb
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory

_EXPID = 't001'
_DATE_LIST = ['fake-date1', 'fake-date2']
_MEMBER_LIST = ['fake-member1', 'fake-member2']
_NUM_CHUNKS = 99
_CHUNK_LIST = list(range(1, _NUM_CHUNKS + 1))
_DATE_FORMAT = 'H'
_DEFAULT_RETRIES = 999


@pytest.fixture
def as_conf(autosubmit_config):
    return autosubmit_config(_EXPID)


@pytest.fixture
def dictionary(as_conf):
    dictionary = DicJobs(_DATE_LIST, _MEMBER_LIST, _CHUNK_LIST, _DATE_FORMAT,
                         default_retrials=_DEFAULT_RETRIES, as_conf=as_conf)
    dictionary.changes = {}
    return dictionary


@pytest.fixture
def joblist(tmp_path, as_conf):
    job_list_persistence = JobListPersistenceDb(str(tmp_path))
    return JobList(_EXPID, as_conf, YAMLParserFactory(), job_list_persistence)


def test_read_section_running_once_create_jobs_once(mocker, dictionary):
    mock_date2str = mocker.patch('autosubmit.job.job_dict.date2str')
    # arrange
    mock_date2str.side_effect = lambda x, y: str(x)
    dictionary.compare_section = mocker.Mock()

    section = 'fake-section'
    priority = 999
    frequency = 123
    splits = -1
    running = "once"
    options = {
        'FREQUENCY': frequency,
        'PRIORITY': priority,
        'SPLITS': splits,
        'EXCLUDED_LIST_C': [],
        'EXCLUDED_LIST_M': [],
        'RUNNING': running
    }

    dictionary.experiment_data = dict()
    dictionary.experiment_data["JOBS"] = {}
    dictionary.experiment_data["JOBS"][section] = options

    dictionary._create_jobs_once = mocker.Mock()
    dictionary._create_jobs_startdate = mocker.Mock()
    dictionary._create_jobs_member = mocker.Mock()
    dictionary._create_jobs_chunk = mocker.Mock()
    dictionary.compare_section = mocker.Mock()

    # act
    dictionary.read_section(section, priority, Type.BASH)

    # assert
    dictionary._create_jobs_once.assert_called_once_with(  # type: ignore
        section, priority, Type.BASH, splits)
    dictionary._create_jobs_startdate.assert_not_called()  # type: ignore
    dictionary._create_jobs_member.assert_not_called()  # type: ignore
    dictionary._create_jobs_chunk.assert_not_called()  # type: ignore


def test_read_section_running_date_create_jobs_startdate(mocker, dictionary):
    mock_date2str = mocker.patch('autosubmit.job.job_dict.date2str')
    # arrange
    mock_date2str.side_effect = lambda x, y: str(x)
    dictionary.compare_section = mocker.Mock()

    section = 'fake-section'
    priority = 999
    frequency = 123
    splits = -1
    running = "date"
    synchronize = "date"
    options = {
        'FREQUENCY': frequency,
        'PRIORITY': priority,
        'SYNCHRONIZE': synchronize,
        'SPLITS': splits,
        'EXCLUDED_LIST_C': [],
        'EXCLUDED_LIST_M': [],
        'RUNNING': running
    }
    dictionary.experiment_data = dict()
    dictionary.experiment_data["JOBS"] = {}
    dictionary.experiment_data["JOBS"][section] = options
    dictionary._create_jobs_once = mocker.Mock()
    dictionary._create_jobs_startdate = mocker.Mock()
    dictionary._create_jobs_member = mocker.Mock()
    dictionary._create_jobs_chunk = mocker.Mock()

    # act
    dictionary.read_section(section, priority, Type.BASH)

    # assert
    dictionary._create_jobs_once.assert_not_called()  # type: ignore
    dictionary._create_jobs_startdate.assert_called_once_with(  # type: ignore
        section, priority, frequency, Type.BASH, splits)
    dictionary._create_jobs_member.assert_not_called()  # type: ignore
    dictionary._create_jobs_chunk.assert_not_called()  # type: ignore


def test_read_section_running_member_create_jobs_member(mocker, dictionary):
    mock_date2str = mocker.patch('autosubmit.job.job_dict.date2str')
    mock_date2str.side_effect = lambda x, y: str(x)
    dictionary.compare_section = mocker.Mock()

    # arrange
    section = 'fake-section'
    priority = 999
    frequency = 123
    splits = 0
    running = "member"
    options = {
        'FREQUENCY': frequency,
        'PRIORITY': priority,
        'SPLITS': splits,
        'EXCLUDED_LIST_C': [],
        'EXCLUDED_LIST_M': [],
        'RUNNING': running
    }

    dictionary.experiment_data = dict()
    dictionary.experiment_data["JOBS"] = {}
    dictionary.experiment_data["JOBS"][section] = options

    dictionary._create_jobs_once = mocker.Mock()
    dictionary._create_jobs_startdate = mocker.Mock()
    dictionary._create_jobs_member = mocker.Mock()
    dictionary._create_jobs_chunk = mocker.Mock()

    # act
    dictionary.read_section(section, priority, Type.BASH)

    # assert
    dictionary._create_jobs_once.assert_not_called()  # type: ignore
    dictionary._create_jobs_startdate.assert_not_called()  # type: ignore
    dictionary._create_jobs_member.assert_called_once_with(  # type: ignore
        section, priority, frequency, Type.BASH, splits)
    dictionary._create_jobs_chunk.assert_not_called()  # type: ignore


def test_read_section_running_chunk_create_jobs_chunk(mocker, dictionary):
    mock_date2str = mocker.patch('autosubmit.job.job_dict.date2str')
    # arrange
    mock_date2str.side_effect = lambda x, y: str(x)

    section = 'fake-section'
    options = {
        'FREQUENCY': 123,
        'PRIORITY': 999,
        'DELAY': -1,
        'SYNCHRONIZE': 'date',
        'SPLITS': 0,
        'EXCLUDED_LIST_C': [],
        'EXCLUDED_LIST_M': [],
        'RUNNING': "chunk"
    }

    dictionary.experiment_data = dict()
    dictionary.experiment_data["JOBS"] = {}
    dictionary.experiment_data["JOBS"][section] = options
    dictionary._create_jobs_once = mocker.Mock()
    dictionary._create_jobs_startdate = mocker.Mock()
    dictionary._create_jobs_member = mocker.Mock()
    dictionary._create_jobs_chunk = mocker.Mock()
    dictionary.compare_section = mocker.Mock()
    # act
    dictionary.read_section(section, options["PRIORITY"], Type.BASH)

    # assert
    dictionary._create_jobs_once.assert_not_called()  # type: ignore
    dictionary._create_jobs_startdate.assert_not_called()  # type: ignore
    dictionary._create_jobs_member.assert_not_called()  # type: ignore
    dictionary._create_jobs_chunk.assert_called_once_with(  # type: ignore
        section, options["PRIORITY"], options["FREQUENCY"],
        Type.BASH, options["SYNCHRONIZE"], options["DELAY"],
        options["SPLITS"])


def test_build_job_with_existent_job_list_status(mocker, dictionary):
    mock_date2str = mocker.patch('autosubmit.job.job_dict.date2str')
    mock_date2str.side_effect = lambda x, y: str(x)

    priority = 0
    date = "fake-date"
    member = 'fc0'
    chunk = 2
    # act
    section_data = []

    # arrange
    job_list = [Job(f"{_EXPID}_fake-date_fc0_2_fake-section1", 1, Status.READY, 0),
                Job(f"{_EXPID}_fake-date_fc0_2_fake-section2", 2, Status.RUNNING, 0)]

    dictionary.job_list = {}
    for job in job_list:
        dictionary.job_list[job.name] = job.__getstate__()

    dictionary.build_job('fake-section1', priority, date, member, chunk, Type.BASH, section_data, splits=1)
    dictionary.build_job('fake-section2', priority, date, member, chunk, Type.BASH, section_data, splits=1)

    # assert
    assert Status.WAITING == section_data[0].status
    assert Status.RUNNING == section_data[1].status


def test_dic_creates_right_jobs_by_startdate(mocker, dictionary):
    mock_date2str = mocker.patch('autosubmit.job.job_dict.date2str')
    # arrange
    mock_date2str.side_effect = lambda x, y: str(x)

    mock_section = mocker.Mock()
    mock_section.name = 'fake-section'
    priority = 999
    frequency = 1
    dictionary.build_job = Mock(wraps=dictionary.build_job)
    # act
    dictionary._create_jobs_startdate(mock_section.name, priority, frequency, Type.BASH)

    # assert
    assert len(_DATE_LIST) == dictionary.build_job.call_count
    assert len(dictionary._dic[mock_section.name]) == len(_DATE_LIST)
    for date in _DATE_LIST:
        assert dictionary._dic[mock_section.name][date][0].name == f'{_EXPID}_{date}_{mock_section.name}'


def test_dic_creates_right_jobs_by_member(mocker, dictionary):
    mock_date2str = mocker.patch('autosubmit.job.job_dict.date2str')
    # arrange
    mock_section = mocker.Mock()
    mock_date2str.side_effect = lambda x, y: str(x)
    mock_section.name = 'fake-section'
    priority = 999
    frequency = 1
    dictionary.build_job = Mock(wraps=dictionary.build_job)

    # act
    dictionary._create_jobs_member(mock_section.name, priority, frequency, Type.BASH)

    # assert
    assert len(_DATE_LIST) * len(_MEMBER_LIST) == dictionary.build_job.call_count
    assert len(dictionary._dic[mock_section.name]) == len(_DATE_LIST)
    for date in _DATE_LIST:
        for member in _MEMBER_LIST:
            assert dictionary._dic[mock_section.name][date][member][0].name == \
                   f'{_EXPID}_{date}_{member}_{mock_section.name}'


def test_dic_creates_right_jobs_by_chunk(mocker, dictionary):
    # arrange
    mock_section = mocker.Mock()
    mock_section.name = 'fake-section'
    dictionary.build_job = Mock(return_value=mock_section)


def test_dic_creates_right_jobs_by_chunk_with_frequency_3(mocker, dictionary):
    # arrange
    mock_section = mocker.Mock()
    mock_section.name = 'fake-section'
    priority = 999
    frequency = 3
    dictionary.build_job = Mock(return_value=mock_section)

    # act
    dictionary._create_jobs_chunk(mock_section.name, priority, frequency, Type.BASH)

    # assert
    assert len(_DATE_LIST) * len(_MEMBER_LIST) * (len(_CHUNK_LIST) / frequency) == \
           dictionary.build_job.call_count
    assert len(dictionary._dic[mock_section.name]) == len(_DATE_LIST)


def test_dic_creates_right_jobs_by_chunk_with_frequency_4(mocker, dictionary):
    # arrange
    mock_section = mocker.Mock()
    mock_section.name = 'fake-section'
    priority = 999
    frequency = 4
    dictionary.build_job = Mock(return_value=mock_section)

    # act
    dictionary._create_jobs_chunk(mock_section.name, priority, frequency, Type.BASH)

    # assert
    # you have to multiply to the round upwards (ceil) of the next division
    assert len(_DATE_LIST) * len(_MEMBER_LIST) * math.ceil(len(_CHUNK_LIST) / float(frequency)) == \
           dictionary.build_job.call_count
    assert len(dictionary._dic[mock_section.name]) == len(_DATE_LIST)


def test_dic_creates_right_jobs_by_chunk_with_date_synchronize(mocker, dictionary):
    # arrange
    mock_section = mocker.Mock()
    mock_section.name = 'fake-section'
    priority = 999
    frequency = 1
    dictionary.build_job = Mock(wraps=dictionary.build_job)

    # act
    dictionary._create_jobs_chunk(mock_section.name, priority, frequency, Type.BASH, 'date')

    # assert
    assert len(_CHUNK_LIST) == dictionary.build_job.call_count
    assert len(dictionary._dic[mock_section.name]) == len(_DATE_LIST)
    for date in _DATE_LIST:
        for member in _MEMBER_LIST:
            for chunk in _CHUNK_LIST:
                assert dictionary._dic[mock_section.name][date][member][chunk][0].name == \
                       f'{_EXPID}_{chunk}_{mock_section.name}'


def test_dic_creates_right_jobs_by_chunk_with_date_synchronize_and_frequency_4(mocker, dictionary):
    # arrange
    mock_section = mocker.Mock()
    mock_section.name = 'fake-section'
    priority = 999
    frequency = 4
    dictionary.build_job = Mock(return_value=mock_section)

    # act
    dictionary._create_jobs_chunk(mock_section.name, priority, frequency, Type.BASH, 'date')

    # assert
    assert math.ceil(len(_CHUNK_LIST) / float(frequency)) == \
           dictionary.build_job.call_count
    assert len(dictionary._dic[mock_section.name]) == len(_DATE_LIST)


def test_dic_creates_right_jobs_by_chunk_with_member_synchronize(mocker, dictionary):
    mock_date2str = mocker.patch('autosubmit.job.job_dict.date2str')
    # patch date2str
    mock_date2str.side_effect = lambda x, y: str(x)
    # arrange
    mock_section = mocker.Mock()
    mock_section.name = 'fake-section'
    priority = 999
    frequency = 1
    dictionary.build_job = Mock(wraps=dictionary.build_job)

    # act
    dictionary._create_jobs_chunk(mock_section.name, priority, frequency, Type.BASH, 'member')

    # assert
    assert len(_DATE_LIST) * len(_CHUNK_LIST) == \
           dictionary.build_job.call_count
    assert len(dictionary._dic[mock_section.name]) == len(_DATE_LIST)
    for date in _DATE_LIST:
        for member in _MEMBER_LIST:
            for chunk in _CHUNK_LIST:
                assert dictionary._dic[mock_section.name][date][member][chunk][0].name == \
                       f'{_EXPID}_{date}_{chunk}_{mock_section.name}'


def test_dic_creates_right_jobs_by_chunk_with_member_synchronize_and_frequency_4(mocker, dictionary):
    # arrange
    mock_section = mocker.Mock()
    mock_section.name = 'fake-section'
    priority = 999
    frequency = 4
    dictionary.build_job = Mock(return_value=mock_section)

    # act
    dictionary._create_jobs_chunk(mock_section.name, priority, frequency, Type.BASH, 'member')

    # assert
    assert len(_DATE_LIST) * math.ceil(len(_CHUNK_LIST) / float(frequency)) == \
           dictionary.build_job.call_count
    assert len(dictionary._dic[mock_section.name]) == len(_DATE_LIST)


def test_create_job_creates_a_job_with_right_parameters(mocker, dictionary):
    section = 'test'
    priority = 99
    date = datetime(2016, 1, 1)
    member = 'fc0'
    chunk = 'ch0'
    # arrange

    dictionary.experiment_data = dict()
    dictionary.experiment_data["DEFAULT"] = dict()
    dictionary.experiment_data["DEFAULT"]["EXPID"] = "random-id"
    dictionary.experiment_data["JOBS"] = {}
    dictionary.experiment_data["PLATFORMS"] = {}
    dictionary.experiment_data["CONFIG"] = {}
    dictionary.experiment_data["PLATFORMS"]["FAKE-PLATFORM"] = {}
    job_list_mock = mocker.Mock()
    job_list_mock.append = mocker.Mock()

    # act
    section_data = []
    dictionary.build_job(section, priority, date, member, chunk, 'bash', section_data)
    created_job = section_data[0]
    # assert
    assert 'random-id_2016010100_fc0_ch0_test' == created_job.name
    assert Status.WAITING == created_job.status
    assert priority == created_job.priority
    assert section == created_job.section
    assert date == created_job.date
    assert member == created_job.member
    assert chunk == created_job.chunk
    assert _DATE_FORMAT == created_job.date_format
    assert Type.BASH == created_job.type
    assert None is created_job.executable
    assert created_job.check
    assert 0 == created_job.retrials


def test_get_member_returns_the_jobs_if_no_member(dictionary):
    # arrange
    jobs = 'fake-jobs'
    dic = {'any-key': 'any-value'}

    # act
    returned_jobs = dictionary._get_member(jobs, dic, 'fake-member', None)  # expected jobs

    # arrange
    assert jobs == returned_jobs


def test_get_member_returns_the_jobs_with_the_member(dictionary):
    # arrange
    jobs = ['fake-job']
    dic = {'fake-job2': 'any-value'}
    member = 'fake-job2'

    # act
    returned_jobs = dictionary._get_member(jobs, dic, member, None)

    # arrange
    assert ['fake-job'] + ['any-value'] == returned_jobs  # expected jobs + member


def test_get_member_returns_the_jobs_with_the_given_chunk_of_the_member(dictionary):
    # arrange
    jobs = ['fake-job']
    dic = {'fake-job2': {'fake-job3': 'fake'}}
    member = 'fake-job2'

    # act
    returned_jobs = dictionary._get_member(jobs, dic, member, 'fake-job3')

    # arrange
    assert ['fake-job'] + ['fake'] == returned_jobs  # expected jobs + chunk


def test_get_member_returns_the_jobs_with_all_the_chunks_of_the_member(dictionary):
    # arrange
    jobs = ['fake-job']
    dic = {'fake-job2': {5: 'fake5', 8: 'fake8', 9: 'fake9'}}
    member = 'fake-job2'

    # act
    returned_jobs = dictionary._get_member(jobs, dic, member, None)

    # arrange
    assert ['fake-job'] + ['fake5', 'fake8', 'fake9'] == returned_jobs  # expected jobs + all chunks


def test_get_date_returns_the_jobs_if_no_date(dictionary):
    # arrange
    jobs = 'fake-jobs'
    dic = {'any-key': 'any-value'}

    # act
    returned_jobs = dictionary._get_date(jobs, dic, 'whatever', None, None)

    # arrange
    assert 'fake-jobs' == returned_jobs  # expected jobs


def test_get_date_returns_the_jobs_with_the_date(dictionary):
    # arrange
    jobs = ['fake-job']
    dic = {'fake-job2': 'any-value'}
    date = 'fake-job2'

    # act
    returned_jobs = dictionary._get_date(jobs, dic, date, None, None)

    # arrange
    assert ['fake-job'] + ['any-value'] == returned_jobs  # expected jobs + date


def test_get_date_returns_the_jobs_and_calls_get_member_once_with_the_given_member(mocker, dictionary):
    # arrange
    jobs = ['fake-job']
    date_dic = {'fake-job3': 'fake'}
    dic = {'fake-job2': date_dic}
    date = 'fake-job2'
    member = 'fake-member'
    chunk = 'fake-chunk'
    dictionary._get_member = mocker.Mock()

    # act
    returned_jobs = dictionary._get_date(jobs, dic, date, member, chunk)

    # arrange
    assert ['fake-job'] == returned_jobs
    dictionary._get_member.assert_called_once_with(jobs, date_dic, member, chunk)  # type: ignore


def test_get_date_returns_the_jobs_and_calls_get_member_for_all_its_members(mocker, dictionary):
    # arrange
    jobs = ['fake-job']
    date_dic = {'fake-job3': 'fake'}
    dic = {'fake-job2': date_dic}
    date = 'fake-job2'
    chunk = 'fake-chunk'
    dictionary._get_member = mocker.Mock()

    # act
    returned_jobs = dictionary._get_date(jobs, dic, date, None, chunk)

    # arrange
    assert ['fake-job'] == returned_jobs
    assert len(dictionary._member_list) == dictionary._get_member.call_count  # type: ignore
    for member in dictionary._member_list:
        dictionary._get_member.assert_any_call(jobs, date_dic, member, chunk)  # type: ignore


def test_get_jobs_returns_the_job_of_the_section(dictionary):
    # arrange
    section = 'fake-section'
    dictionary._dic = {'fake-section': 'fake-job'}

    # act
    returned_jobs = dictionary.get_jobs(section)

    # arrange
    assert ['fake-job'] == returned_jobs


def test_get_jobs_calls_get_date_with_given_date(mocker, dictionary):
    # arrange
    section = 'fake-section'
    dic = {'fake-job3': 'fake'}
    date = 'fake-date'
    member = 'fake-member'
    chunk = 111
    dictionary._dic = {'fake-section': dic}
    dictionary._get_date = mocker.Mock()

    # act
    returned_jobs = dictionary.get_jobs(section, date, member, chunk)

    # arrange
    assert list() == returned_jobs
    dictionary._get_date.assert_called_once_with(list(), dic, date, member, chunk)  # type: ignore


def test_get_jobs_calls_get_date_for_all_its_dates(mocker, dictionary):
    # arrange
    section = 'fake-section'
    dic = {'fake-job3': 'fake'}
    member = 'fake-member'
    chunk = 111
    dictionary._dic = {'fake-section': dic}
    dictionary._get_date = mocker.Mock()

    # act
    returned_jobs = dictionary.get_jobs(section, member=member, chunk=chunk)

    # arrange
    assert list() == returned_jobs
    assert len(dictionary._date_list) == dictionary._get_date.call_count  # type: ignore
    for date in dictionary._date_list:
        dictionary._get_date.assert_any_call(list(), dic, date, member, chunk)  # type: ignore


def test_job_list_returns_the_job_list_by_name(dictionary):
    # act
    job_list = [Job("child", 1, Status.WAITING, 0), Job("child2", 1, Status.WAITING, 0)]
    dictionary.job_list = job_list
    # arrange
    assert {'child': job_list[0], 'child2': job_list[1]} == dictionary.job_list


def test_create_jobs_split(mocker, dictionary):
    mock_date2str = mocker.patch('autosubmit.job.job_dict.date2str')
    mock_date2str.side_effect = lambda x, y: str(x)
    section_data = []
    dictionary._create_jobs_split(5, 'fake-section', 'fake-date', 'fake-member', 'fake-chunk', 0, Type.BASH,
                                  section_data)
    assert 5 == len(section_data)
