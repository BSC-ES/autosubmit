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
from datetime import datetime

import pytest
from mock import Mock

from autosubmit.config.yamlparser import YAMLParserFactory
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_dict import DicJobs
from autosubmit.job.job_list import JobList
from autosubmit.job.template import Language

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
    return JobList(_EXPID, as_conf, YAMLParserFactory())


@pytest.mark.parametrize("running,expected_call,splits,extra_opts", [
    ("once", "_create_jobs_once", -1, {}),
    ("date", "_create_jobs_startdate", -1, {"SYNCHRONIZE": "date"}),
    ("member", "_create_jobs_member", 0, {}),
    ("chunk", "_create_jobs_chunk", 0, {"SYNCHRONIZE": "date", "DELAY": -1}),
])
def test_read_section_routes_to_correct_create_method(mocker, dictionary, running, expected_call, splits, extra_opts):
    mock_date2str = mocker.patch('autosubmit.job.job_dict.date2str')
    mock_date2str.side_effect = lambda x, y: str(x)
    dictionary.compare_section = mocker.Mock()

    section = 'fake-section'
    priority = 999
    frequency = 123
    options = {
        'FREQUENCY': frequency,
        'PRIORITY': priority,
        'SPLITS': splits,
        'EXCLUDED_LIST_C': [],
        'EXCLUDED_LIST_M': [],
        'RUNNING': running,
        **extra_opts,
    }

    dictionary.experiment_data = {"JOBS": {section: options}}
    dictionary._create_jobs_once = mocker.Mock()
    dictionary._create_jobs_startdate = mocker.Mock()
    dictionary._create_jobs_member = mocker.Mock()
    dictionary._create_jobs_chunk = mocker.Mock()

    dictionary.read_section(section, priority, Language.BASH)

    expected_args = {
        "_create_jobs_once": (section, priority, Language.BASH, splits),
        "_create_jobs_startdate": (section, priority, frequency, Language.BASH, splits),
        "_create_jobs_member": (section, priority, frequency, Language.BASH, splits),
        "_create_jobs_chunk": (section, priority, frequency, Language.BASH, options.get("SYNCHRONIZE"), options.get("DELAY"), splits),
    }
    for method in ["_create_jobs_once", "_create_jobs_startdate", "_create_jobs_member", "_create_jobs_chunk"]:
        mock = getattr(dictionary, method)
        if method == expected_call:
            mock.assert_called_once_with(*expected_args[method])
        else:
            mock.assert_not_called()


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

    dictionary.build_job('fake-section1', priority, date, member, chunk, Language.BASH, section_data, splits=1)
    dictionary.build_job('fake-section2', priority, date, member, chunk, Language.BASH, section_data, splits=1)

    # assert
    assert Status.READY == section_data[0].status
    assert Status.RUNNING == section_data[1].status


@pytest.mark.parametrize("create_method,frequency,synchronize,expected_count,verify_name", [
    ("_create_jobs_startdate", 1, None, lambda: len(_DATE_LIST), True),
    ("_create_jobs_member", 1, None, lambda: len(_DATE_LIST) * len(_MEMBER_LIST), True),
    ("_create_jobs_chunk", 1, None, lambda: len(_DATE_LIST) * len(_MEMBER_LIST) * len(_CHUNK_LIST), False),
    ("_create_jobs_chunk", 3, None, lambda: len(_DATE_LIST) * len(_MEMBER_LIST) * (len(_CHUNK_LIST) / 3), False),
    ("_create_jobs_chunk", 4, None, lambda: len(_DATE_LIST) * len(_MEMBER_LIST) * math.ceil(len(_CHUNK_LIST) / 4.0), False),
    ("_create_jobs_chunk", 1, 'date', lambda: len(_CHUNK_LIST), True),
    ("_create_jobs_chunk", 4, 'date', lambda: math.ceil(len(_CHUNK_LIST) / 4.0), False),
    ("_create_jobs_chunk", 1, 'member', lambda: len(_DATE_LIST) * len(_CHUNK_LIST), True),
    ("_create_jobs_chunk", 4, 'member', lambda: len(_DATE_LIST) * math.ceil(len(_CHUNK_LIST) / 4.0), False),
])
def test_dic_creates_right_jobs(mocker, dictionary, create_method, frequency, synchronize, expected_count, verify_name):
    mock_date2str = mocker.patch('autosubmit.job.job_dict.date2str')
    mock_date2str.side_effect = lambda x, y: str(x)
    mock_section = mocker.Mock()
    mock_section.name = 'fake-section'
    priority = 999
    use_wraps = synchronize is not None or create_method != "_create_jobs_chunk"
    dictionary.build_job = Mock(wraps=dictionary.build_job) if use_wraps else Mock(return_value=mock_section)

    args = [mock_section.name, priority, frequency, Language.BASH]
    if synchronize and create_method == "_create_jobs_chunk":
        args.append(synchronize)

    getattr(dictionary, create_method)(*args)

    assert expected_count() == dictionary.build_job.call_count
    assert len(dictionary._dic[mock_section.name]) == len(_DATE_LIST)

    if verify_name:
        for date in _DATE_LIST:
            for member in _MEMBER_LIST:
                for chunk in _CHUNK_LIST:
                    entry = dictionary._dic.get(mock_section.name, {})
                    if synchronize == 'date':
                        item = entry.get(date, {}).get(member, {}).get(chunk, None)
                        if item:
                            assert item[0].name == f'{_EXPID}_{chunk}_{mock_section.name}'
                    elif synchronize == 'member':
                        item = entry.get(date, {}).get(member, {}).get(chunk, None)
                        if item:
                            assert item[0].name == f'{_EXPID}_{date}_{chunk}_{mock_section.name}'
                    elif create_method == "_create_jobs_member":
                        item = entry.get(date, {}).get(member, None)
                        if item:
                            assert item[0].name == f'{_EXPID}_{date}_{member}_{mock_section.name}'
                    elif create_method == "_create_jobs_startdate":
                        items = entry.get(date, [None])
                        if items[0]:
                            assert items[0].name == f'{_EXPID}_{date}_{mock_section.name}'


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
    assert Language.BASH == created_job.type
    assert None is created_job.executable
    assert created_job.check
    assert 0 == created_job.retrials


@pytest.mark.parametrize("jobs,dic,member,chunk,expected", [
    ('fake-jobs', {'any-key': 'any-value'}, 'fake-member', None, 'fake-jobs'),
    (['fake-job'], {'fake-job2': 'any-value'}, 'fake-job2', None, ['fake-job', 'any-value']),
    (['fake-job'], {'fake-job2': {'fake-job3': 'fake'}}, 'fake-job2', 'fake-job3', ['fake-job', 'fake']),
    (['fake-job'], {'fake-job2': {5: 'fake5', 8: 'fake8', 9: 'fake9'}}, 'fake-job2', None, ['fake-job', 'fake5', 'fake8', 'fake9']),
])
def test_get_member_returns_correct_jobs(dictionary, jobs, dic, member, chunk, expected):
    result = dictionary._get_member(jobs, dic, member, chunk)
    assert result == expected


@ pytest.mark.parametrize("jobs,dic,date,member,chunk,expected,expected_member_call", [
    ('fake-jobs', {'any-key': 'any-value'}, 'whatever', None, None, 'fake-jobs', None),
    (['fake-job'], {'fake-job2': 'any-value'}, 'fake-job2', None, None, ['fake-job', 'any-value'], None),
    (['fake-job'], {'fake-job2': {'fake-job3': 'fake'}}, 'fake-job2', 'fake-member', 'fake-chunk', ['fake-job'], ('fake-member', 'fake-chunk')),
])
def test_get_date_returns_correct_jobs(mocker, dictionary, jobs, dic, date, member, chunk, expected, expected_member_call):
    date_dic = dic.get(date, {})
    if expected_member_call:
        dictionary._get_member = mocker.Mock()
        dictionary._get_member.return_value = expected

    result = dictionary._get_date(jobs, dic, date, member, chunk)

    if expected_member_call:
        assert result == expected
        dictionary._get_member.assert_called_once_with(jobs, date_dic, expected_member_call[0], expected_member_call[1])
    else:
        assert result == expected


def test_get_date_calls_get_member_for_all_members(mocker, dictionary):
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
    assert len(dictionary._member_list) == dictionary._get_member.call_count
    for member in dictionary._member_list:
        dictionary._get_member.assert_any_call(jobs, date_dic, member, chunk)


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
    dictionary._get_date.assert_called_once_with(list(), dic, date, member, chunk)


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
    assert len(dictionary._date_list) == dictionary._get_date.call_count
    for date in dictionary._date_list:
        dictionary._get_date.assert_any_call(list(), dic, date, member, chunk)


def test_create_jobs_split(mocker, dictionary):
    mock_date2str = mocker.patch('autosubmit.job.job_dict.date2str')
    mock_date2str.side_effect = lambda x, y: str(x)
    section_data = []
    dictionary._create_jobs_split(5, 'fake-section', 'fake-date', 'fake-member', 'fake-chunk', 0, Language.BASH,
                                  section_data)
    assert 5 == len(section_data)
