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

import pytest
from bscearth.utils.date import parse_date, date2str
from random import randrange

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_grouping import JobGrouping
from autosubmit.job.job_list import JobList
from autosubmit.job.job_list_persistence import JobListPersistenceDb
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory


def _create_dummy_job(name, status, date=None, member=None, chunk=None, split=None):
    job_id = randrange(1, 999)
    job = Job(name, job_id, status, 0)
    job.type = randrange(0, 2)

    job.date = parse_date(date)
    job.member = member
    job.chunk = chunk
    job.split = split

    return job


@pytest.fixture
def job_list(autosubmit_config, tmp_path):
    as_conf = autosubmit_config('a000', {
        'JOBS': {},
        'PLATFORMS': {}
    })
    job_list_persistence = JobListPersistenceDb('a000')
    job_list = JobList(as_conf.expid, as_conf, YAMLParserFactory(), job_list_persistence)

    # Basic workflow with SETUP, INI, SIM, POST, CLEAN
    _create_dummy_job('expid_SETUP', Status.READY)

    for date in ['19000101', '19000202']:
        for member in ['m1', 'm2']:
            job = _create_dummy_job('expid_' + date + '_' + member + '_' + 'INI', Status.WAITING, date, member)
            job_list.get_job_list().append(job)

    sections = ['SIM', 'POST', 'CLEAN']
    for section in sections:
        for date in ['19000101', '19000202']:
            for member in ['m1', 'm2']:
                for chunk in [1, 2]:
                    job = _create_dummy_job(
                        f'expid_{date}_{member}_{str(chunk)}_{section}',
                        Status.WAITING,
                        date,
                        member,
                        chunk)
                    job_list.get_job_list().append(job)
    return job_list


def test_group_by_date(job_list, mocker):
    groups_dict = dict()

    groups_dict['status'] = {'19000101': Status.WAITING, '19000202': Status.WAITING}
    groups_dict['jobs'] = {
        'expid_19000101_m1_INI': ['19000101'], 'expid_19000101_m2_INI': ['19000101'],
        'expid_19000202_m1_INI': ['19000202'], 'expid_19000202_m2_INI': ['19000202'],

        'expid_19000101_m1_1_SIM': ['19000101'], 'expid_19000101_m1_2_SIM': ['19000101'],
        'expid_19000101_m2_1_SIM': ['19000101'], 'expid_19000101_m2_2_SIM': ['19000101'],
        'expid_19000202_m1_1_SIM': ['19000202'], 'expid_19000202_m1_2_SIM': ['19000202'],
        'expid_19000202_m2_1_SIM': ['19000202'], 'expid_19000202_m2_2_SIM': ['19000202'],

        'expid_19000101_m1_1_POST': ['19000101'], 'expid_19000101_m1_2_POST': ['19000101'],
        'expid_19000101_m2_1_POST': ['19000101'], 'expid_19000101_m2_2_POST': ['19000101'],
        'expid_19000202_m1_1_POST': ['19000202'], 'expid_19000202_m1_2_POST': ['19000202'],
        'expid_19000202_m2_1_POST': ['19000202'], 'expid_19000202_m2_2_POST': ['19000202'],

        'expid_19000101_m1_1_CLEAN': ['19000101'], 'expid_19000101_m1_2_CLEAN': ['19000101'],
        'expid_19000101_m2_1_CLEAN': ['19000101'], 'expid_19000101_m2_2_CLEAN': ['19000101'],
        'expid_19000202_m1_1_CLEAN': ['19000202'], 'expid_19000202_m1_2_CLEAN': ['19000202'],
        'expid_19000202_m2_1_CLEAN': ['19000202'], 'expid_19000202_m2_2_CLEAN': ['19000202']
    }

    job_list.get_date_list = mocker.Mock(return_value=['19000101', '19000202'])
    job_list.get_member_list = mocker.Mock(return_value=['m1', 'm2'])
    job_list.get_chunk_list = mocker.Mock(return_value=[1, 2])
    job_list.get_date_format = mocker.Mock(return_value='')

    side_effect = []
    for job in reversed(job_list.get_job_list()):
        if job.date is not None:
            side_effect.append(date2str(job.date, ''))

    mocker.patch('autosubmit.job.job_grouping.date2str', side_effect=side_effect)
    job_grouping = JobGrouping('date', job_list.get_job_list(), job_list)
    assert job_grouping.group_jobs() == groups_dict


def test_group_by_member(job_list, mocker):
    groups_dict = dict()

    groups_dict['status'] = {'19000101_m1': Status.WAITING, '19000101_m2': Status.WAITING,
                             '19000202_m1': Status.WAITING, '19000202_m2': Status.WAITING}
    groups_dict['jobs'] = {
        'expid_19000101_m1_INI': ['19000101_m1'], 'expid_19000101_m2_INI': ['19000101_m2'],
        'expid_19000202_m1_INI': ['19000202_m1'], 'expid_19000202_m2_INI': ['19000202_m2'],

        'expid_19000101_m1_1_SIM': ['19000101_m1'], 'expid_19000101_m1_2_SIM': ['19000101_m1'],
        'expid_19000101_m2_1_SIM': ['19000101_m2'],
        'expid_19000101_m2_2_SIM': ['19000101_m2'],
        'expid_19000202_m1_1_SIM': ['19000202_m1'], 'expid_19000202_m1_2_SIM': ['19000202_m1'],
        'expid_19000202_m2_1_SIM': ['19000202_m2'],
        'expid_19000202_m2_2_SIM': ['19000202_m2'],

        'expid_19000101_m1_1_POST': ['19000101_m1'], 'expid_19000101_m1_2_POST': ['19000101_m1'],
        'expid_19000101_m2_1_POST': ['19000101_m2'],
        'expid_19000101_m2_2_POST': ['19000101_m2'],
        'expid_19000202_m1_1_POST': ['19000202_m1'], 'expid_19000202_m1_2_POST': ['19000202_m1'],
        'expid_19000202_m2_1_POST': ['19000202_m2'],
        'expid_19000202_m2_2_POST': ['19000202_m2'],

        'expid_19000101_m1_1_CLEAN': ['19000101_m1'], 'expid_19000101_m1_2_CLEAN': ['19000101_m1'],
        'expid_19000101_m2_1_CLEAN': ['19000101_m2'],
        'expid_19000101_m2_2_CLEAN': ['19000101_m2'],
        'expid_19000202_m1_1_CLEAN': ['19000202_m1'], 'expid_19000202_m1_2_CLEAN': ['19000202_m1'],
        'expid_19000202_m2_1_CLEAN': ['19000202_m2'],
        'expid_19000202_m2_2_CLEAN': ['19000202_m2']
    }

    job_list.get_date_list = mocker.Mock(return_value=['19000101', '19000202'])
    job_list.get_member_list = mocker.Mock(return_value=['m1', 'm2'])
    job_list.get_chunk_list = mocker.Mock(return_value=[1, 2])
    job_list.get_date_format = mocker.Mock(return_value='')

    side_effect = []
    for job in reversed(job_list.get_job_list()):
        if job.member is not None:
            side_effect.append(date2str(job.date, ''))

    mocker.patch('autosubmit.job.job_grouping.date2str', side_effect=side_effect)
    job_grouping = JobGrouping('member', job_list.get_job_list(), job_list)
    assert job_grouping.group_jobs() == groups_dict


def test_group_by_chunk(job_list, mocker):
    groups_dict = dict()

    groups_dict['status'] = {'19000101_m1_1': Status.WAITING, '19000101_m1_2': Status.WAITING,
                             '19000101_m2_1': Status.WAITING, '19000101_m2_2': Status.WAITING,
                             '19000202_m1_1': Status.WAITING, '19000202_m1_2': Status.WAITING,
                             '19000202_m2_1': Status.WAITING, '19000202_m2_2': Status.WAITING}
    groups_dict['jobs'] = {
        'expid_19000101_m1_1_SIM': ['19000101_m1_1'], 'expid_19000101_m1_2_SIM': ['19000101_m1_2'],
        'expid_19000101_m2_1_SIM': ['19000101_m2_1'],
        'expid_19000101_m2_2_SIM': ['19000101_m2_2'],
        'expid_19000202_m1_1_SIM': ['19000202_m1_1'], 'expid_19000202_m1_2_SIM': ['19000202_m1_2'],
        'expid_19000202_m2_1_SIM': ['19000202_m2_1'],
        'expid_19000202_m2_2_SIM': ['19000202_m2_2'],

        'expid_19000101_m1_1_POST': ['19000101_m1_1'], 'expid_19000101_m1_2_POST': ['19000101_m1_2'],
        'expid_19000101_m2_1_POST': ['19000101_m2_1'],
        'expid_19000101_m2_2_POST': ['19000101_m2_2'],
        'expid_19000202_m1_1_POST': ['19000202_m1_1'], 'expid_19000202_m1_2_POST': ['19000202_m1_2'],
        'expid_19000202_m2_1_POST': ['19000202_m2_1'],
        'expid_19000202_m2_2_POST': ['19000202_m2_2'],

        'expid_19000101_m1_1_CLEAN': ['19000101_m1_1'], 'expid_19000101_m1_2_CLEAN': ['19000101_m1_2'],
        'expid_19000101_m2_1_CLEAN': ['19000101_m2_1'],
        'expid_19000101_m2_2_CLEAN': ['19000101_m2_2'],
        'expid_19000202_m1_1_CLEAN': ['19000202_m1_1'], 'expid_19000202_m1_2_CLEAN': ['19000202_m1_2'],
        'expid_19000202_m2_1_CLEAN': ['19000202_m2_1'],
        'expid_19000202_m2_2_CLEAN': ['19000202_m2_2']
    }

    job_list.get_date_list = mocker.Mock(return_value=['19000101', '19000202'])
    job_list.get_member_list = mocker.Mock(return_value=['m1', 'm2'])
    job_list.get_chunk_list = mocker.Mock(return_value=[1, 2])
    job_list.get_date_format = mocker.Mock(return_value='')

    side_effect = []
    for job in reversed(job_list.get_job_list()):
        if job.chunk is not None:
            side_effect.append(date2str(job.date, ''))

    mocker.patch('autosubmit.job.job_grouping.date2str', side_effect=side_effect)
    job_grouping = JobGrouping('chunk', job_list.get_job_list(), job_list)
    assert job_grouping.group_jobs() == groups_dict


def test_group_by_split(job_list, mocker):
    for date in ['19000101', '19000202']:
        for member in ['m1', 'm2']:
            for chunk in [1, 2]:
                for split in [1, 2]:
                    job = _create_dummy_job(
                        'expid_' + date + '_' + member + '_' + str(chunk) + '_' + str(split) + '_CMORATM',
                        Status.WAITING, date, member, chunk, split)
                    job_list.get_job_list().append(job)

    groups_dict = dict()

    groups_dict['status'] = {
        'expid_19000101_m1_1_CMORATM': Status.WAITING,
        'expid_19000101_m1_2_CMORATM': Status.WAITING,
        'expid_19000101_m2_1_CMORATM': Status.WAITING,
        'expid_19000101_m2_2_CMORATM': Status.WAITING,
        'expid_19000202_m1_1_CMORATM': Status.WAITING,
        'expid_19000202_m1_2_CMORATM': Status.WAITING,
        'expid_19000202_m2_1_CMORATM': Status.WAITING,
        'expid_19000202_m2_2_CMORATM': Status.WAITING,
    }

    groups_dict['jobs'] = {
        'expid_19000101_m1_1_1_CMORATM': ['expid_19000101_m1_1_CMORATM'],
        'expid_19000101_m1_1_2_CMORATM': ['expid_19000101_m1_1_CMORATM'],
        'expid_19000101_m1_2_1_CMORATM': ['expid_19000101_m1_2_CMORATM'],
        'expid_19000101_m1_2_2_CMORATM': ['expid_19000101_m1_2_CMORATM'],
        'expid_19000101_m2_1_1_CMORATM': ['expid_19000101_m2_1_CMORATM'],
        'expid_19000101_m2_1_2_CMORATM': ['expid_19000101_m2_1_CMORATM'],
        'expid_19000101_m2_2_1_CMORATM': ['expid_19000101_m2_2_CMORATM'],
        'expid_19000101_m2_2_2_CMORATM': ['expid_19000101_m2_2_CMORATM'],
        'expid_19000202_m1_1_1_CMORATM': ['expid_19000202_m1_1_CMORATM'],
        'expid_19000202_m1_1_2_CMORATM': ['expid_19000202_m1_1_CMORATM'],
        'expid_19000202_m1_2_1_CMORATM': ['expid_19000202_m1_2_CMORATM'],
        'expid_19000202_m1_2_2_CMORATM': ['expid_19000202_m1_2_CMORATM'],
        'expid_19000202_m2_1_1_CMORATM': ['expid_19000202_m2_1_CMORATM'],
        'expid_19000202_m2_1_2_CMORATM': ['expid_19000202_m2_1_CMORATM'],
        'expid_19000202_m2_2_1_CMORATM': ['expid_19000202_m2_2_CMORATM'],
        'expid_19000202_m2_2_2_CMORATM': ['expid_19000202_m2_2_CMORATM']
    }

    job_list.get_date_list = mocker.Mock(return_value=['19000101', '19000202'])
    job_list.get_member_list = mocker.Mock(return_value=['m1', 'm2'])
    job_list.get_chunk_list = mocker.Mock(return_value=[1, 2])
    job_list.get_date_format = mocker.Mock(return_value='')

    job_grouping = JobGrouping('split', job_list.get_job_list(), job_list)
    assert job_grouping.group_jobs() == groups_dict


def test_automatic_grouping_all(job_list, mocker):
    groups_dict = dict()

    groups_dict['status'] = {'19000101': Status.WAITING, '19000202': Status.WAITING}
    groups_dict['jobs'] = {
        'expid_19000101_m1_INI': ['19000101'], 'expid_19000101_m2_INI': ['19000101'],
        'expid_19000202_m1_INI': ['19000202'], 'expid_19000202_m2_INI': ['19000202'],

        'expid_19000101_m1_1_SIM': ['19000101'], 'expid_19000101_m1_2_SIM': ['19000101'],
        'expid_19000101_m2_1_SIM': ['19000101'],
        'expid_19000101_m2_2_SIM': ['19000101'],
        'expid_19000202_m1_1_SIM': ['19000202'], 'expid_19000202_m1_2_SIM': ['19000202'],
        'expid_19000202_m2_1_SIM': ['19000202'],
        'expid_19000202_m2_2_SIM': ['19000202'],

        'expid_19000101_m1_1_POST': ['19000101'], 'expid_19000101_m1_2_POST': ['19000101'],
        'expid_19000101_m2_1_POST': ['19000101'],
        'expid_19000101_m2_2_POST': ['19000101'],
        'expid_19000202_m1_1_POST': ['19000202'], 'expid_19000202_m1_2_POST': ['19000202'],
        'expid_19000202_m2_1_POST': ['19000202'],
        'expid_19000202_m2_2_POST': ['19000202'],

        'expid_19000101_m1_1_CLEAN': ['19000101'], 'expid_19000101_m1_2_CLEAN': ['19000101'],
        'expid_19000101_m2_1_CLEAN': ['19000101'],
        'expid_19000101_m2_2_CLEAN': ['19000101'],
        'expid_19000202_m1_1_CLEAN': ['19000202'], 'expid_19000202_m1_2_CLEAN': ['19000202'],
        'expid_19000202_m2_1_CLEAN': ['19000202'],
        'expid_19000202_m2_2_CLEAN': ['19000202']
    }

    job_list.get_date_list = mocker.Mock(return_value=['19000101', '19000202'])
    job_list.get_member_list = mocker.Mock(return_value=['m1', 'm2'])
    job_list.get_chunk_list = mocker.Mock(return_value=[1, 2])
    job_list.get_date_format = mocker.Mock(return_value='')

    '''side_effect = []
    for job in reversed(job_list.get_job_list()):
        if job.date is not None:
            side_effect.append(date2str(job.date, ''))

    with mocker.patch('autosubmit.job.job_grouping.date2str', side_effect=side_effect):'''
    job_grouping = JobGrouping('automatic', job_list.get_job_list(), job_list)
    grouped = job_grouping.group_jobs()
    assert grouped["status"] == groups_dict["status"]
    assert grouped["jobs"] == groups_dict["jobs"]


def test_automatic_grouping_not_ini(job_list, mocker):
    job_list.get_job_by_name('expid_19000101_m1_INI').status = Status.READY
    job_list.get_job_by_name('expid_19000101_m2_INI').status = Status.READY
    job_list.get_job_by_name('expid_19000202_m1_INI').status = Status.READY
    job_list.get_job_by_name('expid_19000202_m2_INI').status = Status.READY

    groups_dict = dict()

    groups_dict['status'] = {'19000101': Status.WAITING, '19000202': Status.WAITING}
    groups_dict['jobs'] = {
        'expid_19000101_m1_1_SIM': ['19000101'], 'expid_19000101_m1_2_SIM': ['19000101'],
        'expid_19000101_m2_1_SIM': ['19000101'],
        'expid_19000101_m2_2_SIM': ['19000101'],
        'expid_19000202_m1_1_SIM': ['19000202'], 'expid_19000202_m1_2_SIM': ['19000202'],
        'expid_19000202_m2_1_SIM': ['19000202'],
        'expid_19000202_m2_2_SIM': ['19000202'],

        'expid_19000101_m1_1_POST': ['19000101'], 'expid_19000101_m1_2_POST': ['19000101'],
        'expid_19000101_m2_1_POST': ['19000101'],
        'expid_19000101_m2_2_POST': ['19000101'],
        'expid_19000202_m1_1_POST': ['19000202'], 'expid_19000202_m1_2_POST': ['19000202'],
        'expid_19000202_m2_1_POST': ['19000202'],
        'expid_19000202_m2_2_POST': ['19000202'],

        'expid_19000101_m1_1_CLEAN': ['19000101'], 'expid_19000101_m1_2_CLEAN': ['19000101'],
        'expid_19000101_m2_1_CLEAN': ['19000101'],
        'expid_19000101_m2_2_CLEAN': ['19000101'],
        'expid_19000202_m1_1_CLEAN': ['19000202'], 'expid_19000202_m1_2_CLEAN': ['19000202'],
        'expid_19000202_m2_1_CLEAN': ['19000202'],
        'expid_19000202_m2_2_CLEAN': ['19000202']
    }

    job_list.get_date_list = mocker.Mock(return_value=['19000101', '19000202'])
    job_list.get_member_list = mocker.Mock(return_value=['m1', 'm2'])
    job_list.get_chunk_list = mocker.Mock(return_value=[1, 2])
    job_list.get_date_format = mocker.Mock(return_value='')

    side_effect = []
    for job in reversed(job_list.get_job_list()):
        if job.date is not None:
            side_effect.append(date2str(job.date, ''))

    mocker.patch('autosubmit.job.job_grouping.date2str', side_effect=side_effect)
    job_grouping = JobGrouping('automatic', job_list.get_job_list(), job_list)
    assert job_grouping.group_jobs() == groups_dict


def test_automatic_grouping_splits(job_list, mocker):
    for date in ['19000101', '19000202']:
        for member in ['m1', 'm2']:
            for chunk in [1, 2]:
                for split in [1, 2]:
                    job = _create_dummy_job(
                        'expid_' + date + '_' + member + '_' + str(chunk) + '_' + str(split) + '_CMORATM',
                        Status.WAITING, date, member, chunk, split)
                    job_list.get_job_list().append(job)

    groups_dict = dict()

    groups_dict['status'] = {'19000101': Status.WAITING, '19000202': Status.WAITING}
    groups_dict['jobs'] = {
        'expid_19000101_m1_INI': ['19000101'], 'expid_19000101_m2_INI': ['19000101'],
        'expid_19000202_m1_INI': ['19000202'], 'expid_19000202_m2_INI': ['19000202'],

        'expid_19000101_m1_1_SIM': ['19000101'], 'expid_19000101_m1_2_SIM': ['19000101'],
        'expid_19000101_m2_1_SIM': ['19000101'],
        'expid_19000101_m2_2_SIM': ['19000101'],
        'expid_19000202_m1_1_SIM': ['19000202'], 'expid_19000202_m1_2_SIM': ['19000202'],
        'expid_19000202_m2_1_SIM': ['19000202'],
        'expid_19000202_m2_2_SIM': ['19000202'],

        'expid_19000101_m1_1_POST': ['19000101'], 'expid_19000101_m1_2_POST': ['19000101'],
        'expid_19000101_m2_1_POST': ['19000101'],
        'expid_19000101_m2_2_POST': ['19000101'],
        'expid_19000202_m1_1_POST': ['19000202'], 'expid_19000202_m1_2_POST': ['19000202'],
        'expid_19000202_m2_1_POST': ['19000202'],
        'expid_19000202_m2_2_POST': ['19000202'],

        'expid_19000101_m1_1_CLEAN': ['19000101'], 'expid_19000101_m1_2_CLEAN': ['19000101'],
        'expid_19000101_m2_1_CLEAN': ['19000101'],
        'expid_19000101_m2_2_CLEAN': ['19000101'],
        'expid_19000202_m1_1_CLEAN': ['19000202'], 'expid_19000202_m1_2_CLEAN': ['19000202'],
        'expid_19000202_m2_1_CLEAN': ['19000202'],
        'expid_19000202_m2_2_CLEAN': ['19000202'],

        'expid_19000101_m1_1_1_CMORATM': ['19000101'],
        'expid_19000101_m1_1_2_CMORATM': ['19000101'],
        'expid_19000101_m1_2_1_CMORATM': ['19000101'],
        'expid_19000101_m1_2_2_CMORATM': ['19000101'],
        'expid_19000101_m2_1_1_CMORATM': ['19000101'],
        'expid_19000101_m2_1_2_CMORATM': ['19000101'],
        'expid_19000101_m2_2_1_CMORATM': ['19000101'],
        'expid_19000101_m2_2_2_CMORATM': ['19000101'],
        'expid_19000202_m1_1_1_CMORATM': ['19000202'],
        'expid_19000202_m1_1_2_CMORATM': ['19000202'],
        'expid_19000202_m1_2_1_CMORATM': ['19000202'],
        'expid_19000202_m1_2_2_CMORATM': ['19000202'],
        'expid_19000202_m2_1_1_CMORATM': ['19000202'],
        'expid_19000202_m2_1_2_CMORATM': ['19000202'],
        'expid_19000202_m2_2_1_CMORATM': ['19000202'],
        'expid_19000202_m2_2_2_CMORATM': ['19000202']
    }

    job_list.get_date_list = mocker.Mock(return_value=['19000101', '19000202'])
    job_list.get_member_list = mocker.Mock(return_value=['m1', 'm2'])
    job_list.get_chunk_list = mocker.Mock(return_value=[1, 2])
    job_list.get_date_format = mocker.Mock(return_value='')

    side_effect = []
    for job in reversed(job_list.get_job_list()):
        if job.date is not None:
            side_effect.append(date2str(job.date, ''))

    mocker.patch('autosubmit.job.job_grouping.date2str', side_effect=side_effect)
    job_grouping = JobGrouping('automatic', job_list.get_job_list(), job_list)
    assert job_grouping.group_jobs() == groups_dict


def test_automatic_grouping_different_status_member(job_list, mocker):
    job_list.get_job_by_name('expid_19000101_m1_INI').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m2_INI').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000202_m1_INI').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000202_m2_INI').status = Status.COMPLETED

    job_list.get_job_by_name('expid_19000101_m1_1_SIM').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m1_2_SIM').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m1_1_POST').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m1_2_POST').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m1_1_CLEAN').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m1_2_CLEAN').status = Status.COMPLETED

    job_list.get_job_by_name('expid_19000101_m2_1_SIM').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m2_1_POST').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m2_1_CLEAN').status = Status.RUNNING

    job_list.get_job_by_name('expid_19000101_m2_2_SIM').status = Status.READY

    groups_dict = dict()

    groups_dict['status'] = {'19000101_m1': Status.COMPLETED, '19000202': Status.WAITING}
    groups_dict['jobs'] = {
        'expid_19000101_m1_INI': ['19000101_m1'],

        'expid_19000101_m1_1_SIM': ['19000101_m1'], 'expid_19000101_m1_2_SIM': ['19000101_m1'],
        'expid_19000202_m1_1_SIM': ['19000202'], 'expid_19000202_m1_2_SIM': ['19000202'],
        'expid_19000202_m2_1_SIM': ['19000202'],
        'expid_19000202_m2_2_SIM': ['19000202'],

        'expid_19000101_m1_1_POST': ['19000101_m1'], 'expid_19000101_m1_2_POST': ['19000101_m1'],
        'expid_19000202_m1_1_POST': ['19000202'], 'expid_19000202_m1_2_POST': ['19000202'],
        'expid_19000202_m2_1_POST': ['19000202'],
        'expid_19000202_m2_2_POST': ['19000202'],

        'expid_19000101_m1_1_CLEAN': ['19000101_m1'], 'expid_19000101_m1_2_CLEAN': ['19000101_m1'],
        'expid_19000202_m1_1_CLEAN': ['19000202'], 'expid_19000202_m1_2_CLEAN': ['19000202'],
        'expid_19000202_m2_1_CLEAN': ['19000202'],
        'expid_19000202_m2_2_CLEAN': ['19000202']
    }

    job_list.get_date_list = mocker.Mock(return_value=['19000101', '19000202'])
    job_list.get_member_list = mocker.Mock(return_value=['m1', 'm2'])
    job_list.get_chunk_list = mocker.Mock(return_value=[1, 2])
    job_list.get_date_format = mocker.Mock(return_value='')

    side_effect = []
    for job in reversed(job_list.get_job_list()):
        if job.date is not None:
            side_effect.append(date2str(job.date, ''))

    mocker.patch('autosubmit.job.job_grouping.date2str', side_effect=side_effect)
    job_grouping = JobGrouping('automatic', job_list.get_job_list(), job_list)
    assert job_grouping.group_jobs() == groups_dict


def test_automatic_grouping_different_status_chunk(job_list, mocker):
    job_list.get_job_by_name('expid_19000101_m1_INI').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m2_INI').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000202_m1_INI').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000202_m2_INI').status = Status.COMPLETED

    job_list.get_job_by_name('expid_19000101_m1_1_SIM').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m1_1_POST').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m1_1_CLEAN').status = Status.COMPLETED

    job_list.get_job_by_name('expid_19000101_m1_2_SIM').status = Status.READY

    job_list.get_job_by_name('expid_19000101_m2_1_SIM').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m2_1_POST').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m2_1_CLEAN').status = Status.RUNNING

    job_list.get_job_by_name('expid_19000101_m2_2_SIM').status = Status.READY

    groups_dict = dict()

    groups_dict['status'] = {'19000101_m1_1': Status.COMPLETED, '19000202': Status.WAITING}
    groups_dict['jobs'] = {
        'expid_19000101_m1_1_SIM': ['19000101_m1_1'],
        'expid_19000202_m1_1_SIM': ['19000202'], 'expid_19000202_m1_2_SIM': ['19000202'],
        'expid_19000202_m2_1_SIM': ['19000202'],
        'expid_19000202_m2_2_SIM': ['19000202'],

        'expid_19000101_m1_1_POST': ['19000101_m1_1'],
        'expid_19000202_m1_1_POST': ['19000202'], 'expid_19000202_m1_2_POST': ['19000202'],
        'expid_19000202_m2_1_POST': ['19000202'],
        'expid_19000202_m2_2_POST': ['19000202'],

        'expid_19000101_m1_1_CLEAN': ['19000101_m1_1'],
        'expid_19000202_m1_1_CLEAN': ['19000202'], 'expid_19000202_m1_2_CLEAN': ['19000202'],
        'expid_19000202_m2_1_CLEAN': ['19000202'],
        'expid_19000202_m2_2_CLEAN': ['19000202']
    }

    job_list.get_date_list = mocker.Mock(return_value=['19000101', '19000202'])
    job_list.get_member_list = mocker.Mock(return_value=['m1', 'm2'])
    job_list.get_chunk_list = mocker.Mock(return_value=[1, 2])
    job_list.get_date_format = mocker.Mock(return_value='')

    side_effect = []
    for job in reversed(job_list.get_job_list()):
        if job.date is not None:
            side_effect.append(date2str(job.date, ''))

    mocker.patch('autosubmit.job.job_grouping.date2str', side_effect=side_effect)
    job_grouping = JobGrouping('automatic', job_list.get_job_list(), job_list)
    assert job_grouping.group_jobs() == groups_dict


def test_group_by_member_expand_running(job_list, mocker):
    job_list.get_job_by_name('expid_19000101_m1_INI').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m2_INI').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000202_m1_INI').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000202_m2_INI').status = Status.COMPLETED

    job_list.get_job_by_name('expid_19000101_m1_1_SIM').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m1_1_POST').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m1_1_CLEAN').status = Status.COMPLETED

    job_list.get_job_by_name('expid_19000101_m1_2_SIM').status = Status.READY

    job_list.get_job_by_name('expid_19000101_m2_1_SIM').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m2_1_POST').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m2_1_CLEAN').status = Status.RUNNING

    job_list.get_job_by_name('expid_19000101_m2_2_SIM').status = Status.READY

    groups_dict = dict()

    groups_dict['status'] = {'19000101_m1': Status.READY, '19000202_m1': Status.WAITING,
                             '19000202_m2': Status.WAITING}
    groups_dict['jobs'] = {
        'expid_19000101_m1_INI': ['19000101_m1'], 'expid_19000202_m1_INI': ['19000202_m1'],
        'expid_19000202_m2_INI': ['19000202_m2'],

        'expid_19000101_m1_1_SIM': ['19000101_m1'], 'expid_19000101_m1_2_SIM': ['19000101_m1'],
        'expid_19000202_m1_1_SIM': ['19000202_m1'], 'expid_19000202_m1_2_SIM': ['19000202_m1'],
        'expid_19000202_m2_1_SIM': ['19000202_m2'],
        'expid_19000202_m2_2_SIM': ['19000202_m2'],

        'expid_19000101_m1_1_POST': ['19000101_m1'], 'expid_19000101_m1_2_POST': ['19000101_m1'],
        'expid_19000202_m1_1_POST': ['19000202_m1'], 'expid_19000202_m1_2_POST': ['19000202_m1'],
        'expid_19000202_m2_1_POST': ['19000202_m2'],
        'expid_19000202_m2_2_POST': ['19000202_m2'],

        'expid_19000101_m1_1_CLEAN': ['19000101_m1'], 'expid_19000101_m1_2_CLEAN': ['19000101_m1'],
        'expid_19000202_m1_1_CLEAN': ['19000202_m1'], 'expid_19000202_m1_2_CLEAN': ['19000202_m1'],
        'expid_19000202_m2_1_CLEAN': ['19000202_m2'],
        'expid_19000202_m2_2_CLEAN': ['19000202_m2']
    }

    job_list.get_date_list = mocker.Mock(return_value=['19000101', '19000202'])
    job_list.get_member_list = mocker.Mock(return_value=['m1', 'm2'])
    job_list.get_chunk_list = mocker.Mock(return_value=[1, 2])
    job_list.get_date_format = mocker.Mock(return_value='')

    side_effect = []
    for job in reversed(job_list.get_job_list()):
        if job.member is not None:
            side_effect.append(date2str(job.date, ''))

    mocker.patch('autosubmit.job.job_grouping.date2str', side_effect=side_effect)
    job_grouping = JobGrouping('member', job_list.get_job_list(), job_list, expanded_status=[Status.RUNNING])
    assert job_grouping.group_jobs() == groups_dict


def test_group_by_chunk_expand_failed_running(job_list, mocker):
    job_list.get_job_by_name('expid_19000101_m1_INI').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m2_INI').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000202_m1_INI').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000202_m2_INI').status = Status.COMPLETED

    job_list.get_job_by_name('expid_19000101_m1_1_SIM').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m1_1_POST').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m1_1_CLEAN').status = Status.FAILED

    job_list.get_job_by_name('expid_19000101_m1_2_SIM').status = Status.READY

    job_list.get_job_by_name('expid_19000101_m2_1_SIM').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m2_1_POST').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m2_1_CLEAN').status = Status.RUNNING

    job_list.get_job_by_name('expid_19000101_m2_2_SIM').status = Status.READY

    groups_dict = dict()

    groups_dict['status'] = {'19000101_m1_2': Status.READY, '19000101_m2_2': Status.READY,
                             '19000202_m1_1': Status.WAITING, '19000202_m1_2': Status.WAITING,
                             '19000202_m2_1': Status.WAITING, '19000202_m2_2': Status.WAITING}
    groups_dict['jobs'] = {
        'expid_19000101_m1_2_SIM': ['19000101_m1_2'],
        'expid_19000101_m2_2_SIM': ['19000101_m2_2'],
        'expid_19000202_m1_1_SIM': ['19000202_m1_1'], 'expid_19000202_m1_2_SIM': ['19000202_m1_2'],
        'expid_19000202_m2_1_SIM': ['19000202_m2_1'],
        'expid_19000202_m2_2_SIM': ['19000202_m2_2'],

        'expid_19000101_m1_2_POST': ['19000101_m1_2'],
        'expid_19000101_m2_2_POST': ['19000101_m2_2'],
        'expid_19000202_m1_1_POST': ['19000202_m1_1'], 'expid_19000202_m1_2_POST': ['19000202_m1_2'],
        'expid_19000202_m2_1_POST': ['19000202_m2_1'],
        'expid_19000202_m2_2_POST': ['19000202_m2_2'],

        'expid_19000101_m1_2_CLEAN': ['19000101_m1_2'],
        'expid_19000101_m2_2_CLEAN': ['19000101_m2_2'],
        'expid_19000202_m1_1_CLEAN': ['19000202_m1_1'], 'expid_19000202_m1_2_CLEAN': ['19000202_m1_2'],
        'expid_19000202_m2_1_CLEAN': ['19000202_m2_1'],
        'expid_19000202_m2_2_CLEAN': ['19000202_m2_2']
    }

    job_list.get_date_list = mocker.Mock(return_value=['19000101', '19000202'])
    job_list.get_member_list = mocker.Mock(return_value=['m1', 'm2'])
    job_list.get_chunk_list = mocker.Mock(return_value=[1, 2])
    job_list.get_date_format = mocker.Mock(return_value='')

    side_effect = []
    for job in reversed(job_list.get_job_list()):
        if job.chunk is not None:
            side_effect.append(date2str(job.date, ''))

    mocker.patch('autosubmit.job.job_grouping.date2str', side_effect=side_effect)
    expanded_status = [Status.RUNNING, Status.FAILED]
    job_grouping = JobGrouping('chunk', job_list.get_job_list(), job_list, expanded_status=expanded_status)
    assert job_grouping.group_jobs() == groups_dict


def test_group_by_member_expand(job_list, mocker):
    job_list.get_job_by_name('expid_19000101_m1_INI').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m2_INI').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000202_m1_INI').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000202_m2_INI').status = Status.COMPLETED

    job_list.get_job_by_name('expid_19000101_m1_1_SIM').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m1_1_POST').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m1_1_CLEAN').status = Status.COMPLETED

    job_list.get_job_by_name('expid_19000101_m1_2_SIM').status = Status.READY

    job_list.get_job_by_name('expid_19000101_m2_1_SIM').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m2_1_POST').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m2_1_CLEAN').status = Status.RUNNING

    job_list.get_job_by_name('expid_19000101_m2_2_SIM').status = Status.READY

    groups_dict = dict()

    groups_dict['status'] = {'19000101_m1': Status.READY}
    groups_dict['jobs'] = {
        'expid_19000101_m1_INI': ['19000101_m1'],

        'expid_19000101_m1_1_SIM': ['19000101_m1'], 'expid_19000101_m1_2_SIM': ['19000101_m1'],

        'expid_19000101_m1_1_POST': ['19000101_m1'], 'expid_19000101_m1_2_POST': ['19000101_m1'],

        'expid_19000101_m1_1_CLEAN': ['19000101_m1'], 'expid_19000101_m1_2_CLEAN': ['19000101_m1'],
    }

    job_list.get_date_list = mocker.Mock(return_value=['19000101', '19000202'])
    job_list.get_member_list = mocker.Mock(return_value=['m1', 'm2'])
    job_list.get_chunk_list = mocker.Mock(return_value=[1, 2])
    job_list.get_date_format = mocker.Mock(return_value='')

    side_effect = []
    for job in reversed(job_list.get_job_list()):
        if job.member is not None:
            side_effect.append(date2str(job.date, ''))

    mocker.patch('autosubmit.job.job_grouping.date2str', side_effect=side_effect)
    expand_list = "[ 19000101 [m2] 19000202 [m1 m2] ]"
    job_grouping = JobGrouping('member', job_list.get_job_list(), job_list, expand_list=expand_list)
    assert job_grouping.group_jobs() == groups_dict


def test_group_by_member_expand_and_running(job_list, mocker):
    job_list.get_job_by_name('expid_19000101_m1_INI').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m2_INI').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000202_m1_INI').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000202_m2_INI').status = Status.COMPLETED

    job_list.get_job_by_name('expid_19000101_m1_1_SIM').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m1_1_POST').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m1_1_CLEAN').status = Status.COMPLETED

    job_list.get_job_by_name('expid_19000101_m1_2_SIM').status = Status.READY

    job_list.get_job_by_name('expid_19000101_m2_1_SIM').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m2_1_POST').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m2_1_CLEAN').status = Status.RUNNING

    job_list.get_job_by_name('expid_19000101_m2_2_SIM').status = Status.READY

    job_list.get_job_by_name('expid_19000202_m1_1_SIM').status = Status.RUNNING

    groups_dict = dict()

    groups_dict['status'] = {'19000101_m1': Status.READY}
    groups_dict['jobs'] = {
        'expid_19000101_m1_INI': ['19000101_m1'],

        'expid_19000101_m1_1_SIM': ['19000101_m1'], 'expid_19000101_m1_2_SIM': ['19000101_m1'],

        'expid_19000101_m1_1_POST': ['19000101_m1'], 'expid_19000101_m1_2_POST': ['19000101_m1'],

        'expid_19000101_m1_1_CLEAN': ['19000101_m1'], 'expid_19000101_m1_2_CLEAN': ['19000101_m1'],
    }

    job_list.get_date_list = mocker.Mock(return_value=['19000101', '19000202'])
    job_list.get_member_list = mocker.Mock(return_value=['m1', 'm2'])
    job_list.get_chunk_list = mocker.Mock(return_value=[1, 2])
    job_list.get_date_format = mocker.Mock(return_value='')

    side_effect = []
    for job in reversed(job_list.get_job_list()):
        if job.member is not None:
            side_effect.append(date2str(job.date, ''))

    mocker.patch('autosubmit.job.job_grouping.date2str', side_effect=side_effect)
    job_grouping = JobGrouping(
        'member',
        job_list.get_job_list(),
        job_list,
        expand_list="[ 19000101 [m2] 19000202 [m2] ]",
        expanded_status=[Status.RUNNING]
    )
    assert job_grouping.group_jobs() == groups_dict


def test_group_by_chunk_expand(job_list, mocker):
    job_list.get_job_by_name('expid_19000101_m1_INI').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m2_INI').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000202_m1_INI').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000202_m2_INI').status = Status.COMPLETED

    job_list.get_job_by_name('expid_19000101_m1_1_SIM').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m1_1_POST').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m1_1_CLEAN').status = Status.FAILED

    job_list.get_job_by_name('expid_19000101_m1_2_SIM').status = Status.READY

    job_list.get_job_by_name('expid_19000101_m2_1_SIM').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m2_1_POST').status = Status.COMPLETED
    job_list.get_job_by_name('expid_19000101_m2_1_CLEAN').status = Status.RUNNING

    groups_dict = dict()

    groups_dict['status'] = {'19000101_m1_1': Status.FAILED, '19000101_m1_2': Status.READY,
                             '19000101_m2_1': Status.RUNNING, '19000202_m2_2': Status.WAITING}
    groups_dict['jobs'] = {
        'expid_19000101_m1_1_SIM': ['19000101_m1_1'], 'expid_19000101_m1_2_SIM': ['19000101_m1_2'],
        'expid_19000101_m2_1_SIM': ['19000101_m2_1'],
        'expid_19000202_m2_2_SIM': ['19000202_m2_2'],

        'expid_19000101_m1_1_POST': ['19000101_m1_1'], 'expid_19000101_m1_2_POST': ['19000101_m1_2'],
        'expid_19000101_m2_1_POST': ['19000101_m2_1'],
        'expid_19000202_m2_2_POST': ['19000202_m2_2'],

        'expid_19000101_m1_1_CLEAN': ['19000101_m1_1'], 'expid_19000101_m1_2_CLEAN': ['19000101_m1_2'],
        'expid_19000101_m2_1_CLEAN': ['19000101_m2_1'],
        'expid_19000202_m2_2_CLEAN': ['19000202_m2_2']
    }

    job_list.get_date_list = mocker.Mock(return_value=['19000101', '19000202'])
    job_list.get_member_list = mocker.Mock(return_value=['m1', 'm2'])
    job_list.get_chunk_list = mocker.Mock(return_value=[1, 2])
    job_list.get_date_format = mocker.Mock(return_value='')

    job_grouping = JobGrouping('chunk', job_list.get_job_list(), job_list,
                               expand_list="[ 19000101 [m2 [2] ] 19000202 [m1 [1 2] m2 [1] ] ]")
    assert job_grouping.group_jobs() == groups_dict


def test_synchronize_member_group_member(job_list, mocker):
    for date in ['19000101', '19000202']:
        for chunk in [1, 2]:
            job = _create_dummy_job('expid_' + date + '_' + str(chunk) + '_ASIM',
                                    Status.WAITING, date, None, chunk)
            for member in ['m1', 'm2']:
                job.add_parent(
                    job_list.get_job_by_name('expid_' + date + '_' + member + '_' + str(chunk) + '_SIM'))

            job_list.get_job_list().append(job)

    groups_dict = dict()
    groups_dict['status'] = {'19000101_m1': Status.WAITING,
                             '19000101_m2': Status.WAITING,
                             '19000202_m1': Status.WAITING,
                             '19000202_m2': Status.WAITING}
    groups_dict['jobs'] = {
        'expid_19000101_m1_INI': ['19000101_m1'], 'expid_19000101_m2_INI': ['19000101_m2'],
        'expid_19000202_m1_INI': ['19000202_m1'],
        'expid_19000202_m2_INI': ['19000202_m2'],
        'expid_19000101_m1_1_SIM': ['19000101_m1'], 'expid_19000101_m1_2_SIM': ['19000101_m1'],
        'expid_19000101_m2_1_SIM': ['19000101_m2'],
        'expid_19000101_m2_2_SIM': ['19000101_m2'],
        'expid_19000202_m1_1_SIM': ['19000202_m1'], 'expid_19000202_m1_2_SIM': ['19000202_m1'],
        'expid_19000202_m2_1_SIM': ['19000202_m2'],
        'expid_19000202_m2_2_SIM': ['19000202_m2'],

        'expid_19000101_m1_1_POST': ['19000101_m1'], 'expid_19000101_m1_2_POST': ['19000101_m1'],
        'expid_19000101_m2_1_POST': ['19000101_m2'],
        'expid_19000101_m2_2_POST': ['19000101_m2'],
        'expid_19000202_m1_1_POST': ['19000202_m1'], 'expid_19000202_m1_2_POST': ['19000202_m1'],
        'expid_19000202_m2_1_POST': ['19000202_m2'],
        'expid_19000202_m2_2_POST': ['19000202_m2'],

        'expid_19000101_m1_1_CLEAN': ['19000101_m1'], 'expid_19000101_m1_2_CLEAN': ['19000101_m1'],
        'expid_19000101_m2_1_CLEAN': ['19000101_m2'],
        'expid_19000101_m2_2_CLEAN': ['19000101_m2'],
        'expid_19000202_m1_1_CLEAN': ['19000202_m1'], 'expid_19000202_m1_2_CLEAN': ['19000202_m1'],
        'expid_19000202_m2_1_CLEAN': ['19000202_m2'],
        'expid_19000202_m2_2_CLEAN': ['19000202_m2'],

        'expid_19000101_1_ASIM': ['19000101_m1', '19000101_m2'],
        'expid_19000101_2_ASIM': ['19000101_m1', '19000101_m2'],
        'expid_19000202_1_ASIM': ['19000202_m1', '19000202_m2'],
        'expid_19000202_2_ASIM': ['19000202_m1', '19000202_m2']
    }

    job_list.get_date_list = mocker.Mock(return_value=['19000101', '19000202'])
    job_list.get_member_list = mocker.Mock(return_value=['m1', 'm2'])
    job_list.get_chunk_list = mocker.Mock(return_value=[1, 2])
    job_list.get_date_format = mocker.Mock(return_value='')

    job_grouping = JobGrouping('member', job_list.get_job_list(), job_list)
    assert job_grouping.group_jobs() == groups_dict


def test_synchronize_member_group_chunk(job_list, mocker):
    for date in ['19000101', '19000202']:
        for chunk in [1, 2]:
            job = _create_dummy_job('expid_' + date + '_' + str(chunk) + '_ASIM',
                                    Status.WAITING, date, None, chunk)
            for member in ['m1', 'm2']:
                job.add_parent(
                    job_list.get_job_by_name('expid_' + date + '_' + member + '_' + str(chunk) + '_SIM'))

            job_list.get_job_list().append(job)

    groups_dict = dict()
    groups_dict['status'] = {'19000101_m1_1': Status.WAITING, '19000101_m1_2': Status.WAITING,
                             '19000101_m2_1': Status.WAITING, '19000101_m2_2': Status.WAITING,
                             '19000202_m1_1': Status.WAITING, '19000202_m1_2': Status.WAITING,
                             '19000202_m2_1': Status.WAITING, '19000202_m2_2': Status.WAITING}
    groups_dict['jobs'] = {
        'expid_19000101_m1_1_SIM': ['19000101_m1_1'], 'expid_19000101_m1_2_SIM': ['19000101_m1_2'],
        'expid_19000101_m2_1_SIM': ['19000101_m2_1'],
        'expid_19000101_m2_2_SIM': ['19000101_m2_2'],
        'expid_19000202_m1_1_SIM': ['19000202_m1_1'], 'expid_19000202_m1_2_SIM': ['19000202_m1_2'],
        'expid_19000202_m2_1_SIM': ['19000202_m2_1'],
        'expid_19000202_m2_2_SIM': ['19000202_m2_2'],

        'expid_19000101_m1_1_POST': ['19000101_m1_1'], 'expid_19000101_m1_2_POST': ['19000101_m1_2'],
        'expid_19000101_m2_1_POST': ['19000101_m2_1'],
        'expid_19000101_m2_2_POST': ['19000101_m2_2'],
        'expid_19000202_m1_1_POST': ['19000202_m1_1'], 'expid_19000202_m1_2_POST': ['19000202_m1_2'],
        'expid_19000202_m2_1_POST': ['19000202_m2_1'],
        'expid_19000202_m2_2_POST': ['19000202_m2_2'],

        'expid_19000101_m1_1_CLEAN': ['19000101_m1_1'], 'expid_19000101_m1_2_CLEAN': ['19000101_m1_2'],
        'expid_19000101_m2_1_CLEAN': ['19000101_m2_1'],
        'expid_19000101_m2_2_CLEAN': ['19000101_m2_2'],
        'expid_19000202_m1_1_CLEAN': ['19000202_m1_1'], 'expid_19000202_m1_2_CLEAN': ['19000202_m1_2'],
        'expid_19000202_m2_1_CLEAN': ['19000202_m2_1'],
        'expid_19000202_m2_2_CLEAN': ['19000202_m2_2'],

        'expid_19000101_1_ASIM': ['19000101_m1_1', '19000101_m2_1'],
        'expid_19000101_2_ASIM': ['19000101_m1_2', '19000101_m2_2'],
        'expid_19000202_1_ASIM': ['19000202_m1_1', '19000202_m2_1'],
        'expid_19000202_2_ASIM': ['19000202_m1_2', '19000202_m2_2']
    }

    job_list.get_date_list = mocker.Mock(return_value=['19000101', '19000202'])
    job_list.get_member_list = mocker.Mock(return_value=['m1', 'm2'])
    job_list.get_chunk_list = mocker.Mock(return_value=[1, 2])
    job_list.get_date_format = mocker.Mock(return_value='')

    job_grouping = JobGrouping('chunk', job_list.get_job_list(), job_list)
    assert job_grouping.group_jobs() == groups_dict


def test_synchronize_member_group_date(job_list):
    for date in ['19000101', '19000202']:
        for chunk in [1, 2]:
            job = _create_dummy_job('expid_' + date + '_' + str(chunk) + '_ASIM',
                                    Status.WAITING, date, None, chunk)
            for member in ['m1', 'm2']:
                job.add_parent(
                    job_list.get_job_by_name('expid_' + date + '_' + member + '_' + str(chunk) + '_SIM'))

            job_list.get_job_list().append(job)

    groups_dict = dict()
    groups_dict['status'] = {'19000101': Status.WAITING,
                             '19000202': Status.WAITING}
    groups_dict['jobs'] = {
        'expid_19000101_m1_INI': ['19000101'], 'expid_19000101_m2_INI': ['19000101'],
        'expid_19000202_m1_INI': ['19000202'],
        'expid_19000202_m2_INI': ['19000202'],
        'expid_19000101_m1_1_SIM': ['19000101'], 'expid_19000101_m1_2_SIM': ['19000101'],
        'expid_19000101_m2_1_SIM': ['19000101'],
        'expid_19000101_m2_2_SIM': ['19000101'],
        'expid_19000202_m1_1_SIM': ['19000202'], 'expid_19000202_m1_2_SIM': ['19000202'],
        'expid_19000202_m2_1_SIM': ['19000202'],
        'expid_19000202_m2_2_SIM': ['19000202'],

        'expid_19000101_m1_1_POST': ['19000101'], 'expid_19000101_m1_2_POST': ['19000101'],
        'expid_19000101_m2_1_POST': ['19000101'],
        'expid_19000101_m2_2_POST': ['19000101'],
        'expid_19000202_m1_1_POST': ['19000202'], 'expid_19000202_m1_2_POST': ['19000202'],
        'expid_19000202_m2_1_POST': ['19000202'],
        'expid_19000202_m2_2_POST': ['19000202'],

        'expid_19000101_m1_1_CLEAN': ['19000101'], 'expid_19000101_m1_2_CLEAN': ['19000101'],
        'expid_19000101_m2_1_CLEAN': ['19000101'],
        'expid_19000101_m2_2_CLEAN': ['19000101'],
        'expid_19000202_m1_1_CLEAN': ['19000202'], 'expid_19000202_m1_2_CLEAN': ['19000202'],
        'expid_19000202_m2_1_CLEAN': ['19000202'],
        'expid_19000202_m2_2_CLEAN': ['19000202'],

        'expid_19000101_1_ASIM': ['19000101'], 'expid_19000101_2_ASIM': ['19000101'],
        'expid_19000202_1_ASIM': ['19000202'], 'expid_19000202_2_ASIM': ['19000202']
    }

    job_grouping = JobGrouping('date', job_list.get_job_list(), job_list)
    assert job_grouping.group_jobs() == groups_dict


def test_synchronize_date_group_member(job_list, mocker):
    for chunk in [1, 2]:
        job = _create_dummy_job('expid_' + str(chunk) + '_ASIM',
                                Status.WAITING, None, None, chunk)
        for date in ['19000101', '19000202']:
            for member in ['m1', 'm2']:
                job.add_parent(
                    job_list.get_job_by_name('expid_' + date + '_' + member + '_' + str(chunk) + '_SIM'))

        job_list.get_job_list().append(job)

    groups_dict = dict()
    groups_dict['status'] = {'19000101_m1': Status.WAITING,
                             '19000101_m2': Status.WAITING,
                             '19000202_m1': Status.WAITING,
                             '19000202_m2': Status.WAITING}
    groups_dict['jobs'] = {
        'expid_19000101_m1_INI': ['19000101_m1'], 'expid_19000101_m2_INI': ['19000101_m2'],
        'expid_19000202_m1_INI': ['19000202_m1'],
        'expid_19000202_m2_INI': ['19000202_m2'],
        'expid_19000101_m1_1_SIM': ['19000101_m1'], 'expid_19000101_m1_2_SIM': ['19000101_m1'],
        'expid_19000101_m2_1_SIM': ['19000101_m2'],
        'expid_19000101_m2_2_SIM': ['19000101_m2'],
        'expid_19000202_m1_1_SIM': ['19000202_m1'], 'expid_19000202_m1_2_SIM': ['19000202_m1'],
        'expid_19000202_m2_1_SIM': ['19000202_m2'],
        'expid_19000202_m2_2_SIM': ['19000202_m2'],

        'expid_19000101_m1_1_POST': ['19000101_m1'], 'expid_19000101_m1_2_POST': ['19000101_m1'],
        'expid_19000101_m2_1_POST': ['19000101_m2'],
        'expid_19000101_m2_2_POST': ['19000101_m2'],
        'expid_19000202_m1_1_POST': ['19000202_m1'], 'expid_19000202_m1_2_POST': ['19000202_m1'],
        'expid_19000202_m2_1_POST': ['19000202_m2'],
        'expid_19000202_m2_2_POST': ['19000202_m2'],

        'expid_19000101_m1_1_CLEAN': ['19000101_m1'], 'expid_19000101_m1_2_CLEAN': ['19000101_m1'],
        'expid_19000101_m2_1_CLEAN': ['19000101_m2'],
        'expid_19000101_m2_2_CLEAN': ['19000101_m2'],
        'expid_19000202_m1_1_CLEAN': ['19000202_m1'], 'expid_19000202_m1_2_CLEAN': ['19000202_m1'],
        'expid_19000202_m2_1_CLEAN': ['19000202_m2'],
        'expid_19000202_m2_2_CLEAN': ['19000202_m2'],

        'expid_1_ASIM': ['19000101_m1', '19000101_m2', '19000202_m1', '19000202_m2'],
        'expid_2_ASIM': ['19000101_m1', '19000101_m2', '19000202_m1', '19000202_m2']
    }

    job_list.get_date_list = mocker.Mock(return_value=['19000101', '19000202'])
    job_list.get_member_list = mocker.Mock(return_value=['m1', 'm2'])
    job_list.get_chunk_list = mocker.Mock(return_value=[1, 2])
    job_list.get_date_format = mocker.Mock(return_value='')

    side_effect = []
    for job in reversed(job_list.get_job_list()):
        if job.date is None and job.chunk is not None:
            side_effect.append('19000101')
            side_effect.append('19000101')
            side_effect.append('19000101')
            side_effect.append('19000202')
            side_effect.append('19000202')
            side_effect.append('19000202')
        else:
            side_effect.append(date2str(job.date, ''))

    mocker.patch('autosubmit.job.job_grouping.date2str', side_effect=side_effect)
    job_grouping = JobGrouping('member', job_list.get_job_list(), job_list)
    assert job_grouping.group_jobs() == groups_dict


def test_synchronize_date_group_chunk(job_list, mocker):
    for chunk in [1, 2]:
        job = _create_dummy_job('expid_' + str(chunk) + '_ASIM',
                                Status.WAITING, None, None, chunk)
        for date in ['19000101', '19000202']:
            for member in ['m1', 'm2']:
                job.add_parent(
                    job_list.get_job_by_name('expid_' + date + '_' + member + '_' + str(chunk) + '_SIM'))

        job_list.get_job_list().append(job)

    groups_dict = dict()
    groups_dict['status'] = {'19000101_m1_1': Status.WAITING, '19000101_m1_2': Status.WAITING,
                             '19000101_m2_1': Status.WAITING, '19000101_m2_2': Status.WAITING,
                             '19000202_m1_1': Status.WAITING, '19000202_m1_2': Status.WAITING,
                             '19000202_m2_1': Status.WAITING, '19000202_m2_2': Status.WAITING}
    groups_dict['jobs'] = {
        'expid_19000101_m1_1_SIM': ['19000101_m1_1'], 'expid_19000101_m1_2_SIM': ['19000101_m1_2'],
        'expid_19000101_m2_1_SIM': ['19000101_m2_1'],
        'expid_19000101_m2_2_SIM': ['19000101_m2_2'],
        'expid_19000202_m1_1_SIM': ['19000202_m1_1'], 'expid_19000202_m1_2_SIM': ['19000202_m1_2'],
        'expid_19000202_m2_1_SIM': ['19000202_m2_1'],
        'expid_19000202_m2_2_SIM': ['19000202_m2_2'],

        'expid_19000101_m1_1_POST': ['19000101_m1_1'], 'expid_19000101_m1_2_POST': ['19000101_m1_2'],
        'expid_19000101_m2_1_POST': ['19000101_m2_1'],
        'expid_19000101_m2_2_POST': ['19000101_m2_2'],
        'expid_19000202_m1_1_POST': ['19000202_m1_1'], 'expid_19000202_m1_2_POST': ['19000202_m1_2'],
        'expid_19000202_m2_1_POST': ['19000202_m2_1'],
        'expid_19000202_m2_2_POST': ['19000202_m2_2'],

        'expid_19000101_m1_1_CLEAN': ['19000101_m1_1'], 'expid_19000101_m1_2_CLEAN': ['19000101_m1_2'],
        'expid_19000101_m2_1_CLEAN': ['19000101_m2_1'],
        'expid_19000101_m2_2_CLEAN': ['19000101_m2_2'],
        'expid_19000202_m1_1_CLEAN': ['19000202_m1_1'], 'expid_19000202_m1_2_CLEAN': ['19000202_m1_2'],
        'expid_19000202_m2_1_CLEAN': ['19000202_m2_1'],
        'expid_19000202_m2_2_CLEAN': ['19000202_m2_2'],

        'expid_1_ASIM': ['19000101_m1_1', '19000101_m2_1', '19000202_m1_1', '19000202_m2_1'],
        'expid_2_ASIM': ['19000101_m1_2', '19000101_m2_2', '19000202_m1_2', '19000202_m2_2'],
    }

    job_list.get_date_list = mocker.Mock(return_value=['19000101', '19000202'])
    job_list.get_member_list = mocker.Mock(return_value=['m1', 'm2'])
    job_list.get_chunk_list = mocker.Mock(return_value=[1, 2])
    job_list.get_date_format = mocker.Mock(return_value='')

    side_effect = []
    for job in reversed(job_list.get_job_list()):
        if job.chunk is not None:
            if job.date is None:
                side_effect.append('19000101')
                side_effect.append('19000101')
                side_effect.append('19000101')
                side_effect.append('19000202')
                side_effect.append('19000202')
                side_effect.append('19000202')
            else:
                side_effect.append(date2str(job.date, ''))

    mocker.patch('autosubmit.job.job_grouping.date2str', side_effect=side_effect)
    job_grouping = JobGrouping('chunk', job_list.get_job_list(), job_list)
    assert job_grouping.group_jobs() == groups_dict


def test_synchronize_date_group_date(job_list, mocker):
    for chunk in [1, 2]:
        job = _create_dummy_job('expid_' + str(chunk) + '_ASIM',
                                Status.WAITING, None, None, chunk)
        for date in ['19000101', '19000202']:
            for member in ['m1', 'm2']:
                job.add_parent(
                    job_list.get_job_by_name('expid_' + date + '_' + member + '_' + str(chunk) + '_SIM'))

        job_list.get_job_list().append(job)

    groups_dict = dict()
    groups_dict['status'] = {'19000101': Status.WAITING,
                             '19000202': Status.WAITING}
    groups_dict['jobs'] = {
        'expid_19000101_m1_INI': ['19000101'], 'expid_19000101_m2_INI': ['19000101'],
        'expid_19000202_m1_INI': ['19000202'],
        'expid_19000202_m2_INI': ['19000202'],
        'expid_19000101_m1_1_SIM': ['19000101'], 'expid_19000101_m1_2_SIM': ['19000101'],
        'expid_19000101_m2_1_SIM': ['19000101'],
        'expid_19000101_m2_2_SIM': ['19000101'],
        'expid_19000202_m1_1_SIM': ['19000202'], 'expid_19000202_m1_2_SIM': ['19000202'],
        'expid_19000202_m2_1_SIM': ['19000202'],
        'expid_19000202_m2_2_SIM': ['19000202'],

        'expid_19000101_m1_1_POST': ['19000101'], 'expid_19000101_m1_2_POST': ['19000101'],
        'expid_19000101_m2_1_POST': ['19000101'],
        'expid_19000101_m2_2_POST': ['19000101'],
        'expid_19000202_m1_1_POST': ['19000202'], 'expid_19000202_m1_2_POST': ['19000202'],
        'expid_19000202_m2_1_POST': ['19000202'],
        'expid_19000202_m2_2_POST': ['19000202'],

        'expid_19000101_m1_1_CLEAN': ['19000101'], 'expid_19000101_m1_2_CLEAN': ['19000101'],
        'expid_19000101_m2_1_CLEAN': ['19000101'],
        'expid_19000101_m2_2_CLEAN': ['19000101'],
        'expid_19000202_m1_1_CLEAN': ['19000202'], 'expid_19000202_m1_2_CLEAN': ['19000202'],
        'expid_19000202_m2_1_CLEAN': ['19000202'],
        'expid_19000202_m2_2_CLEAN': ['19000202'],

        'expid_1_ASIM': ['19000101', '19000202'], 'expid_2_ASIM': ['19000101', '19000202']
    }

    job_list.get_date_list = mocker.Mock(return_value=['19000101', '19000202'])
    job_list.get_member_list = mocker.Mock(return_value=['m1', 'm2'])
    job_list.get_chunk_list = mocker.Mock(return_value=[1, 2])
    job_list.get_date_format = mocker.Mock(return_value='')

    side_effect = []
    for job in reversed(job_list.get_job_list()):
        if job.date is not None:
            side_effect.append(date2str(job.date))
        elif job.chunk is not None:
            side_effect.append('19000101')
            side_effect.append('19000202')

    mocker.patch('autosubmit.job.job_grouping.date2str', side_effect=side_effect)
    job_grouping = JobGrouping('date', job_list.get_job_list(), job_list)
    assert job_grouping.group_jobs() == groups_dict
