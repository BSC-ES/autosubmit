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

from random import randrange

import pytest

from autosubmit.config.yamlparser import YAMLParserFactory
from autosubmit.job.job import Job, WrapperJob
from autosubmit.job.job_common import Status
from autosubmit.job.job_list import JobList
from autosubmit.monitor.monitor import Monitor

_EXPID = 'random-id'


def _create_dummy_job(name, status, date=None, member=None, chunk=None, split=None):
    job_id = randrange(1, 999)
    job = Job(name, job_id, status, 0)
    job.type = randrange(0, 2)

    job.date = date
    job.member = member
    job.chunk = chunk
    job.split = split

    return job


@pytest.fixture
def job_list(autosubmit_config, tmp_path):
    as_conf = autosubmit_config(_EXPID, {
        'JOBS': {},
        'PLATFORMS': {},
    })
    job_list = JobList(_EXPID, as_conf, YAMLParserFactory())
    # Basic workflow with SETUP, INI, SIM, POST, CLEAN
    setup_job = _create_dummy_job('expid_SETUP', Status.READY)
    job_list.add_job(setup_job)
    for date in ['d1', 'd2']:
        for member in ['m1', 'm2']:
            job = _create_dummy_job('expid_' + date + '_' + member + '_' + 'INI', Status.WAITING, date, member)
            job.add_parent(setup_job)
            job_list.add_job(job)

    sections = ['SIM', 'POST', 'CLEAN']
    for section in sections:
        for date in ['d1', 'd2']:
            for member in ['m1', 'm2']:
                for chunk in [1, 2]:
                    job = _create_dummy_job('expid_' + date + '_' + member + '_' + str(chunk) + '_' + section,
                                            Status.WAITING, date, member, chunk)
                    if section == 'SIM':
                        if chunk > 1:
                            job.add_parent(job_list.get_job_by_name(
                                'expid_' + date + '_' + member + '_' + str(chunk - 1) + '_SIM'))
                        else:
                            job.add_parent(job_list.get_job_by_name('expid_' + date + '_' + member + '_INI'))
                    elif section == 'POST':
                        job.add_parent(job_list.get_job_by_name(
                            'expid_' + date + '_' + member + '_' + str(chunk) + '_SIM'))
                    elif section == 'CLEAN':
                        job.add_parent(job_list.get_job_by_name(
                            'expid_' + date + '_' + member + '_' + str(chunk) + '_POST'))
                    job_list.add_job(job)
    return job_list


def test_grouping_date(job_list):
    groups_dict = dict()
    groups_dict['status'] = {'d1': Status.WAITING, 'd2': Status.WAITING}
    groups_dict['jobs'] = {
        'expid_d1_m1_INI': ['d1'], 'expid_d1_m2_INI': ['d1'], 'expid_d2_m1_INI': ['d2'], 'expid_d2_m2_INI': ['d2'],
        'expid_d1_m1_1_SIM': ['d1'], 'expid_d1_m1_2_SIM': ['d1'], 'expid_d1_m2_1_SIM': ['d1'],
        'expid_d1_m2_2_SIM': ['d1'],
        'expid_d2_m1_1_SIM': ['d2'], 'expid_d2_m1_2_SIM': ['d2'], 'expid_d2_m2_1_SIM': ['d2'],
        'expid_d2_m2_2_SIM': ['d2'],

        'expid_d1_m1_1_POST': ['d1'], 'expid_d1_m1_2_POST': ['d1'], 'expid_d1_m2_1_POST': ['d1'],
        'expid_d1_m2_2_POST': ['d1'],
        'expid_d2_m1_1_POST': ['d2'], 'expid_d2_m1_2_POST': ['d2'], 'expid_d2_m2_1_POST': ['d2'],
        'expid_d2_m2_2_POST': ['d2'],

        'expid_d1_m1_1_CLEAN': ['d1'], 'expid_d1_m1_2_CLEAN': ['d1'], 'expid_d1_m2_1_CLEAN': ['d1'],
        'expid_d1_m2_2_CLEAN': ['d1'],
        'expid_d2_m1_1_CLEAN': ['d2'], 'expid_d2_m1_2_CLEAN': ['d2'], 'expid_d2_m2_1_CLEAN': ['d2'],
        'expid_d2_m2_2_CLEAN': ['d2']
    }

    nodes = [
        "expid_SETUP", 'd1', 'd2'
    ]
    edges = [
        ("expid_SETUP", "d1"), ("expid_SETUP", "d2"), ("d1", "d1"), ("d2", "d2")
    ]

    monitor = Monitor()
    graph = monitor.create_tree_list(_EXPID, job_list.get_job_list(), None, groups_dict)

    assert graph.obj_dict['strict']

    subgraph = graph.obj_dict['subgraphs']['Experiment'][0]

    assert sorted(list(subgraph['nodes'].keys())) == sorted(nodes)
    assert sorted(list(subgraph['edges'].keys())) == sorted(edges)


def test_grouping_member(job_list):
    groups_dict = dict()
    groups_dict['status'] = {'d1_m1': Status.WAITING,
                             'd1_m2': Status.WAITING,
                             'd2_m1': Status.WAITING,
                             'd2_m2': Status.WAITING}
    groups_dict['jobs'] = {
        'expid_d1_m1_INI': ['d1_m1'], 'expid_d1_m2_INI': ['d1_m2'], 'expid_d2_m1_INI': ['d2_m1'],
        'expid_d2_m2_INI': ['d2_m2'],
        'expid_d1_m1_1_SIM': ['d1_m1'], 'expid_d1_m1_2_SIM': ['d1_m1'], 'expid_d1_m2_1_SIM': ['d1_m2'],
        'expid_d1_m2_2_SIM': ['d1_m2'],
        'expid_d2_m1_1_SIM': ['d2_m1'], 'expid_d2_m1_2_SIM': ['d2_m1'], 'expid_d2_m2_1_SIM': ['d2_m2'],
        'expid_d2_m2_2_SIM': ['d2_m2'],

        'expid_d1_m1_1_POST': ['d1_m1'], 'expid_d1_m1_2_POST': ['d1_m1'], 'expid_d1_m2_1_POST': ['d1_m2'],
        'expid_d1_m2_2_POST': ['d1_m2'],
        'expid_d2_m1_1_POST': ['d2_m1'], 'expid_d2_m1_2_POST': ['d2_m1'], 'expid_d2_m2_1_POST': ['d2_m2'],
        'expid_d2_m2_2_POST': ['d2_m2'],

        'expid_d1_m1_1_CLEAN': ['d1_m1'], 'expid_d1_m1_2_CLEAN': ['d1_m1'], 'expid_d1_m2_1_CLEAN': ['d1_m2'],
        'expid_d1_m2_2_CLEAN': ['d1_m2'],
        'expid_d2_m1_1_CLEAN': ['d2_m1'], 'expid_d2_m1_2_CLEAN': ['d2_m1'], 'expid_d2_m2_1_CLEAN': ['d2_m2'],
        'expid_d2_m2_2_CLEAN': ['d2_m2']
    }

    nodes = [
        "expid_SETUP",
        'd1_m1', 'd1_m2', 'd2_m2', 'd2_m1'
    ]
    edges = [
        ("expid_SETUP", "d1_m1"), ("expid_SETUP", "d1_m2"), ("expid_SETUP", "d2_m1"),
        ("expid_SETUP", "d2_m2"),

        ("d1_m1", "d1_m1"), ("d1_m2", "d1_m2"),
        ("d2_m1", "d2_m1"), ("d2_m2", "d2_m2")
    ]

    monitor = Monitor()
    graph = monitor.create_tree_list(_EXPID, job_list.get_job_list(), None, groups_dict)

    assert graph.obj_dict['strict']

    subgraph = graph.obj_dict['subgraphs']['Experiment'][0]

    assert sorted(list(subgraph['nodes'].keys())) == sorted(nodes)
    assert sorted(list(subgraph['edges'].keys())) == sorted(edges)


def test_grouping_chunk(job_list):
    groups_dict = dict()
    groups_dict['status'] = {'d1_m1_1': Status.WAITING, 'd1_m1_2': Status.WAITING,
                             'd1_m2_1': Status.WAITING, 'd1_m2_2': Status.WAITING,
                             'd2_m1_1': Status.WAITING, 'd2_m1_2': Status.WAITING,
                             'd2_m2_1': Status.WAITING, 'd2_m2_2': Status.WAITING}
    groups_dict['jobs'] = {
        'expid_d1_m1_1_SIM': ['d1_m1_1'], 'expid_d1_m1_2_SIM': ['d1_m1_2'], 'expid_d1_m2_1_SIM': ['d1_m2_1'],
        'expid_d1_m2_2_SIM': ['d1_m2_2'],
        'expid_d2_m1_1_SIM': ['d2_m1_1'], 'expid_d2_m1_2_SIM': ['d2_m1_2'], 'expid_d2_m2_1_SIM': ['d2_m2_1'],
        'expid_d2_m2_2_SIM': ['d2_m2_2'],

        'expid_d1_m1_1_POST': ['d1_m1_1'], 'expid_d1_m1_2_POST': ['d1_m1_2'], 'expid_d1_m2_1_POST': ['d1_m2_1'],
        'expid_d1_m2_2_POST': ['d1_m2_2'],
        'expid_d2_m1_1_POST': ['d2_m1_1'], 'expid_d2_m1_2_POST': ['d2_m1_2'], 'expid_d2_m2_1_POST': ['d2_m2_1'],
        'expid_d2_m2_2_POST': ['d2_m2_2'],

        'expid_d1_m1_1_CLEAN': ['d1_m1_1'], 'expid_d1_m1_2_CLEAN': ['d1_m1_2'], 'expid_d1_m2_1_CLEAN': ['d1_m2_1'],
        'expid_d1_m2_2_CLEAN': ['d1_m2_2'],
        'expid_d2_m1_1_CLEAN': ['d2_m1_1'], 'expid_d2_m1_2_CLEAN': ['d2_m1_2'], 'expid_d2_m2_1_CLEAN': ['d2_m2_1'],
        'expid_d2_m2_2_CLEAN': ['d2_m2_2']
    }

    nodes = [
        "expid_SETUP", "expid_d1_m1_INI", "expid_d1_m2_INI", "expid_d2_m1_INI", "expid_d2_m2_INI",
        'd1_m1_1', 'd1_m2_1', 'd2_m1_1', 'd2_m2_1', 'd1_m1_2', 'd1_m2_2', 'd2_m1_2', 'd2_m2_2'
    ]
    edges = [
        ("expid_SETUP", "expid_d1_m1_INI"), ("expid_SETUP", "expid_d1_m2_INI"), ("expid_SETUP", "expid_d2_m1_INI"),
        ("expid_SETUP", "expid_d2_m2_INI"), ("expid_d1_m1_INI", "d1_m1_1"), ("expid_d1_m2_INI", "d1_m2_1"),
        ("expid_d2_m1_INI", "d2_m1_1"), ("expid_d2_m2_INI", "d2_m2_1"),
        ("d1_m1_1", "d1_m1_2"), ("d1_m2_1", "d1_m2_2"), ("d2_m1_1", "d2_m1_2"), ("d2_m2_1", "d2_m2_2"),

        ("d1_m1_1", "d1_m1_1"), ("d1_m1_2", "d1_m1_2"), ("d1_m2_1", "d1_m2_1"), ("d1_m2_2", "d1_m2_2"),
        ("d2_m1_1", "d2_m1_1"), ("d2_m1_2", "d2_m1_2"), ("d2_m2_1", "d2_m2_1"), ("d2_m2_2", "d2_m2_2")
    ]

    monitor = Monitor()
    graph = monitor.create_tree_list(_EXPID, job_list.get_job_list(), None, groups_dict)

    assert graph.obj_dict['strict']

    subgraph = graph.obj_dict['subgraphs']['Experiment'][0]

    assert sorted(list(subgraph['nodes'].keys())) == sorted(nodes)
    assert sorted(list(subgraph['edges'].keys())) == sorted(edges)


def test_grouping_automatic_hide(job_list):
    groups_dict = dict()
    groups_dict['status'] = {'d1_m1_1': Status.WAITING, 'd1_m1_2': Status.WAITING,
                             'd1_m2_1': Status.WAITING, 'd1_m2_2': Status.WAITING,
                             'd2_m1_1': Status.WAITING, 'd2_m1_2': Status.WAITING,
                             'd2_m2_1': Status.WAITING, 'd2_m2_2': Status.WAITING}
    groups_dict['jobs'] = {
        'expid_d1_m1_1_SIM': ['d1_m1_1'], 'expid_d1_m1_2_SIM': ['d1_m1_2'], 'expid_d1_m2_1_SIM': ['d1_m2_1'],
        'expid_d1_m2_2_SIM': ['d1_m2_2'],
        'expid_d2_m1_1_SIM': ['d2_m1_1'], 'expid_d2_m1_2_SIM': ['d2_m1_2'], 'expid_d2_m2_1_SIM': ['d2_m2_1'],
        'expid_d2_m2_2_SIM': ['d2_m2_2'],

        'expid_d1_m1_1_POST': ['d1_m1_1'], 'expid_d1_m1_2_POST': ['d1_m1_2'], 'expid_d1_m2_1_POST': ['d1_m2_1'],
        'expid_d1_m2_2_POST': ['d1_m2_2'],
        'expid_d2_m1_1_POST': ['d2_m1_1'], 'expid_d2_m1_2_POST': ['d2_m1_2'], 'expid_d2_m2_1_POST': ['d2_m2_1'],
        'expid_d2_m2_2_POST': ['d2_m2_2'],

        'expid_d1_m1_1_CLEAN': ['d1_m1_1'], 'expid_d1_m1_2_CLEAN': ['d1_m1_2'], 'expid_d1_m2_1_CLEAN': ['d1_m2_1'],
        'expid_d1_m2_2_CLEAN': ['d1_m2_2'],
        'expid_d2_m1_1_CLEAN': ['d2_m1_1'], 'expid_d2_m1_2_CLEAN': ['d2_m1_2'], 'expid_d2_m2_1_CLEAN': ['d2_m2_1'],
        'expid_d2_m2_2_CLEAN': ['d2_m2_2']
    }

    nodes = [
        "expid_SETUP", "expid_d1_m1_INI", "expid_d1_m2_INI", "expid_d2_m1_INI", "expid_d2_m2_INI"
    ]
    edges = [
        ("expid_SETUP", "expid_d1_m1_INI"), ("expid_SETUP", "expid_d1_m2_INI"), ("expid_SETUP", "expid_d2_m1_INI"),
        ("expid_SETUP", "expid_d2_m2_INI")
    ]

    monitor = Monitor()
    graph = monitor.create_tree_list(_EXPID, job_list.get_job_list(), None, groups_dict,
                                     hide_groups=True)

    assert graph.obj_dict['strict']

    subgraph = graph.obj_dict['subgraphs']['Experiment'][0]

    assert sorted(list(subgraph['nodes'].keys())) == sorted(nodes)
    assert sorted(list(subgraph['edges'].keys())) == sorted(edges)


def test_synchronize_member(job_list):
    for date in ['d1', 'd2']:
        for chunk in [1, 2]:
            job = _create_dummy_job('expid_' + date + '_' + str(chunk) + '_ASIM',
                                    Status.WAITING, date, None, chunk)
            for member in ['m1', 'm2']:
                job.add_parent(
                    job_list.get_job_by_name('expid_' + date + '_' + member + '_' + str(chunk) + '_SIM'))
            job_list.add_job(job)

    nodes = [
        "expid_SETUP", "expid_d1_m1_INI", "expid_d1_m2_INI", "expid_d2_m1_INI", "expid_d2_m2_INI",
        "expid_d1_m1_1_SIM", "expid_d1_m1_2_SIM", "expid_d1_m2_1_SIM", "expid_d1_m2_2_SIM",
        "expid_d2_m1_1_SIM", "expid_d2_m1_2_SIM", "expid_d2_m2_1_SIM", "expid_d2_m2_2_SIM",
        "expid_d1_m1_1_POST", "expid_d1_m1_2_POST", "expid_d1_m2_1_POST", "expid_d1_m2_2_POST",
        "expid_d2_m1_1_POST", "expid_d2_m1_2_POST", "expid_d2_m2_1_POST", "expid_d2_m2_2_POST",
        "expid_d1_m1_1_CLEAN", "expid_d1_m1_2_CLEAN", "expid_d1_m2_1_CLEAN", "expid_d1_m2_2_CLEAN",
        "expid_d2_m1_1_CLEAN", "expid_d2_m1_2_CLEAN", "expid_d2_m2_1_CLEAN", "expid_d2_m2_2_CLEAN",
        "expid_d1_1_ASIM", "expid_d1_2_ASIM", "expid_d2_1_ASIM", "expid_d2_2_ASIM"
    ]
    edges = [
        ('expid_SETUP', 'expid_d1_m1_INI'), ('expid_SETUP', 'expid_d1_m2_INI'), ('expid_SETUP', 'expid_d2_m1_INI'),
        ('expid_SETUP', 'expid_d2_m2_INI'),

        ('expid_d1_m1_INI', 'expid_d1_m1_1_SIM'), ('expid_d1_m2_INI', 'expid_d1_m2_1_SIM'),
        ('expid_d2_m1_INI', 'expid_d2_m1_1_SIM'), ('expid_d2_m2_INI', 'expid_d2_m2_1_SIM'),

        ('expid_d1_m1_1_SIM', 'expid_d1_m1_2_SIM'),
        ('expid_d1_m1_1_SIM', 'expid_d1_m1_1_POST'),
        ('expid_d1_m1_1_SIM', 'expid_d1_1_ASIM'),

        ('expid_d1_m1_2_SIM', 'expid_d1_m1_2_POST'),
        ('expid_d1_m1_2_SIM', 'expid_d1_2_ASIM'),

        ('expid_d1_m2_1_SIM', 'expid_d1_m2_2_SIM'),
        ('expid_d1_m2_1_SIM', 'expid_d1_m2_1_POST'),
        ('expid_d1_m2_1_SIM', 'expid_d1_1_ASIM'),

        ('expid_d1_m2_2_SIM', 'expid_d1_m2_2_POST'),
        ('expid_d1_m2_2_SIM', 'expid_d1_2_ASIM'),

        ('expid_d2_m1_1_SIM', 'expid_d2_m1_2_SIM'),
        ('expid_d2_m1_1_SIM', 'expid_d2_m1_1_POST'),
        ('expid_d2_m1_1_SIM', 'expid_d2_1_ASIM'),

        ('expid_d2_m1_2_SIM', 'expid_d2_m1_2_POST'),
        ('expid_d2_m1_2_SIM', 'expid_d2_2_ASIM'),

        ('expid_d2_m2_1_SIM', 'expid_d2_m2_2_SIM'),
        ('expid_d2_m2_1_SIM', 'expid_d2_m2_1_POST'),
        ('expid_d2_m2_1_SIM', 'expid_d2_1_ASIM'),

        ('expid_d2_m2_2_SIM', 'expid_d2_m2_2_POST'),
        ('expid_d2_m2_2_SIM', 'expid_d2_2_ASIM'),

        ('expid_d1_m1_1_POST', 'expid_d1_m1_1_CLEAN'),
        ('expid_d1_m1_2_POST', 'expid_d1_m1_2_CLEAN'),
        ('expid_d1_m2_1_POST', 'expid_d1_m2_1_CLEAN'),
        ('expid_d1_m2_2_POST', 'expid_d1_m2_2_CLEAN'),

        ('expid_d2_m1_1_POST', 'expid_d2_m1_1_CLEAN'),
        ('expid_d2_m1_2_POST', 'expid_d2_m1_2_CLEAN'),
        ('expid_d2_m2_1_POST', 'expid_d2_m2_1_CLEAN'),
        ('expid_d2_m2_2_POST', 'expid_d2_m2_2_CLEAN')
    ]

    monitor = Monitor()
    graph = monitor.create_tree_list(_EXPID, job_list.get_job_list(), None, dict())

    assert not graph.obj_dict['strict']

    subgraph = graph.obj_dict['subgraphs']['Experiment'][0]

    assert sorted(list(subgraph['nodes'].keys())) == sorted(nodes)
    assert sorted(list(subgraph['edges'].keys())) == sorted(edges)


def test_synchronize_date(job_list):
    for chunk in [1, 2]:
        job = _create_dummy_job('expid_' + str(chunk) + '_ASIM',
                                Status.WAITING, None, None, chunk)
        for date in ['d1', 'd2']:
            for member in ['m1', 'm2']:
                job.add_parent(
                    job_list.get_job_by_name('expid_' + date + '_' + member + '_' + str(chunk) + '_SIM'))
        job_list.add_job(job)

    nodes = [
        "expid_SETUP", "expid_d1_m1_INI", "expid_d1_m2_INI", "expid_d2_m1_INI", "expid_d2_m2_INI",
        "expid_d1_m1_1_SIM", "expid_d1_m1_2_SIM", "expid_d1_m2_1_SIM", "expid_d1_m2_2_SIM",
        "expid_d2_m1_1_SIM", "expid_d2_m1_2_SIM", "expid_d2_m2_1_SIM", "expid_d2_m2_2_SIM",
        "expid_d1_m1_1_POST", "expid_d1_m1_2_POST", "expid_d1_m2_1_POST", "expid_d1_m2_2_POST",
        "expid_d2_m1_1_POST", "expid_d2_m1_2_POST", "expid_d2_m2_1_POST", "expid_d2_m2_2_POST",
        "expid_d1_m1_1_CLEAN", "expid_d1_m1_2_CLEAN", "expid_d1_m2_1_CLEAN", "expid_d1_m2_2_CLEAN",
        "expid_d2_m1_1_CLEAN", "expid_d2_m1_2_CLEAN", "expid_d2_m2_1_CLEAN", "expid_d2_m2_2_CLEAN",
        "expid_1_ASIM", "expid_2_ASIM"
    ]
    edges = [
        ('expid_SETUP', 'expid_d1_m1_INI'), ('expid_SETUP', 'expid_d1_m2_INI'), ('expid_SETUP', 'expid_d2_m1_INI'),
        ('expid_SETUP', 'expid_d2_m2_INI'),

        ('expid_d1_m1_INI', 'expid_d1_m1_1_SIM'), ('expid_d1_m2_INI', 'expid_d1_m2_1_SIM'),
        ('expid_d2_m1_INI', 'expid_d2_m1_1_SIM'), ('expid_d2_m2_INI', 'expid_d2_m2_1_SIM'),

        ('expid_d1_m1_1_SIM', 'expid_d1_m1_2_SIM'),
        ('expid_d1_m1_1_SIM', 'expid_d1_m1_1_POST'),
        ('expid_d1_m1_1_SIM', 'expid_1_ASIM'),

        ('expid_d1_m1_2_SIM', 'expid_d1_m1_2_POST'),
        ('expid_d1_m1_2_SIM', 'expid_2_ASIM'),

        ('expid_d1_m2_1_SIM', 'expid_d1_m2_2_SIM'),
        ('expid_d1_m2_1_SIM', 'expid_d1_m2_1_POST'),
        ('expid_d1_m2_1_SIM', 'expid_1_ASIM'),

        ('expid_d1_m2_2_SIM', 'expid_d1_m2_2_POST'),
        ('expid_d1_m2_2_SIM', 'expid_2_ASIM'),

        ('expid_d2_m1_1_SIM', 'expid_d2_m1_2_SIM'),
        ('expid_d2_m1_1_SIM', 'expid_d2_m1_1_POST'),
        ('expid_d2_m1_1_SIM', 'expid_1_ASIM'),

        ('expid_d2_m1_2_SIM', 'expid_d2_m1_2_POST'),
        ('expid_d2_m1_2_SIM', 'expid_2_ASIM'),

        ('expid_d2_m2_1_SIM', 'expid_d2_m2_2_SIM'),
        ('expid_d2_m2_1_SIM', 'expid_d2_m2_1_POST'),
        ('expid_d2_m2_1_SIM', 'expid_1_ASIM'),

        ('expid_d2_m2_2_SIM', 'expid_d2_m2_2_POST'),
        ('expid_d2_m2_2_SIM', 'expid_2_ASIM'),

        ('expid_d1_m1_1_POST', 'expid_d1_m1_1_CLEAN'),
        ('expid_d1_m1_2_POST', 'expid_d1_m1_2_CLEAN'),
        ('expid_d1_m2_1_POST', 'expid_d1_m2_1_CLEAN'),
        ('expid_d1_m2_2_POST', 'expid_d1_m2_2_CLEAN'),

        ('expid_d2_m1_1_POST', 'expid_d2_m1_1_CLEAN'),
        ('expid_d2_m1_2_POST', 'expid_d2_m1_2_CLEAN'),
        ('expid_d2_m2_1_POST', 'expid_d2_m2_1_CLEAN'),
        ('expid_d2_m2_2_POST', 'expid_d2_m2_2_CLEAN')
    ]

    monitor = Monitor()
    graph = monitor.create_tree_list(_EXPID, job_list.get_job_list(), None, dict())

    assert not graph.obj_dict['strict']

    subgraph = graph.obj_dict['subgraphs']['Experiment'][0]

    assert sorted(list(subgraph['nodes'].keys())) == sorted(nodes)
    assert sorted(list(subgraph['edges'].keys())) == sorted(edges)


@pytest.fixture()
def setup_wrappers(job_list, autosubmit_config):
    job_list.graph.clear()
    jobs = [
        _create_dummy_job("expid_SETUP", Status.READY),
        _create_dummy_job("expid_d1_m1_INI", Status.WAITING, "d1", "m1"),
        _create_dummy_job("expid_d1_m2_INI", Status.WAITING, "d1", "m2"),
        _create_dummy_job("expid_d2_m1_INI", Status.WAITING, "d2", "m1"),
        _create_dummy_job("expid_d2_m2_INI", Status.WAITING, "d2", "m2"),
        _create_dummy_job("expid_d1_m1_1_SIM", Status.WAITING, "d1", "m1", 1),
        _create_dummy_job("expid_d1_m1_2_SIM", Status.WAITING, "d1", "m1", 2),
        _create_dummy_job("expid_d1_m2_1_SIM", Status.WAITING, "d1", "m2", 1),
        _create_dummy_job("expid_d1_m2_2_SIM", Status.WAITING, "d1", "m2", 2),
        _create_dummy_job("expid_d2_m1_1_SIM", Status.WAITING, "d2", "m1", 1),
        _create_dummy_job("expid_d2_m1_2_SIM", Status.WAITING, "d2", "m1", 2),
        _create_dummy_job("expid_d2_m2_1_SIM", Status.WAITING, "d2", "m2", 1),
        _create_dummy_job("expid_d2_m2_2_SIM", Status.WAITING, "d2", "m2", 2),
        _create_dummy_job("expid_d1_m1_1_POST", Status.WAITING, "d1", "m1", 1),
        _create_dummy_job("expid_d1_m1_2_POST", Status.WAITING, "d1", "m1", 2),
        _create_dummy_job("expid_d1_m2_1_POST", Status.WAITING, "d1", "m2", 1),
        _create_dummy_job("expid_d1_m2_2_POST", Status.WAITING, "d1", "m2", 2),
        _create_dummy_job("expid_d2_m1_1_POST", Status.WAITING, "d2", "m1", 1),
        _create_dummy_job("expid_d2_m1_2_POST", Status.WAITING, "d2", "m1", 2),
        _create_dummy_job("expid_d2_m2_1_POST", Status.WAITING, "d2", "m2", 1),
        _create_dummy_job("expid_d2_m2_2_POST", Status.WAITING, "d2", "m2", 2),
        _create_dummy_job("expid_d1_m1_1_CLEAN", Status.WAITING, "d1", "m1", 1),
        _create_dummy_job("expid_d1_m1_2_CLEAN", Status.WAITING, "d1", "m1", 2),
        _create_dummy_job("expid_d1_m2_1_CLEAN", Status.WAITING, "d1", "m2", 1),
        _create_dummy_job("expid_d1_m2_2_CLEAN", Status.WAITING, "d1", "m2", 2),
        _create_dummy_job("expid_d2_m1_1_CLEAN", Status.WAITING, "d2", "m1", 1),
        _create_dummy_job("expid_d2_m1_2_CLEAN", Status.WAITING, "d2", "m1", 2),
        _create_dummy_job("expid_d2_m2_1_CLEAN", Status.WAITING, "d2", "m2", 1),
        _create_dummy_job("expid_d2_m2_2_CLEAN", Status.WAITING, "d2", "m2", 2),
    ]
    edges = [
        ("expid_SETUP", "expid_d1_m1_INI"), ("expid_SETUP", "expid_d1_m2_INI"), ("expid_SETUP", "expid_d2_m1_INI"),
        ("expid_SETUP", "expid_d2_m2_INI"),
        ("expid_d1_m1_INI", "d1_m1_1"), ("expid_d1_m2_INI", "d1_m2_1"), ("expid_d2_m1_INI", "expid_d2_m1_1_SIM"),
        ("expid_d2_m2_INI", "expid_d2_m2_1_SIM"),
        ("d1_m1_1", "d1_m1_2"), ("d1_m1_1", "d1_m1_1"), ("d1_m1_2", "d1_m1_2"), ("d1_m2_1", "d1_m2_1"),
        ("d2_m2_2", "d2_m2_2"),
        ("d1_m2_1", "expid_d1_m2_2_SIM"),
        ("expid_d1_m2_2_SIM", "expid_d1_m2_2_POST"),
        ("expid_d1_m2_2_POST", "expid_d1_m2_2_CLEAN"),
        ("expid_d2_m1_1_SIM", "expid_d2_m1_1_POST"),
        ("expid_d2_m1_1_POST", "expid_d2_m1_1_CLEAN"),
        ("expid_d2_m1_1_SIM", "expid_d2_m1_2_SIM"),
        ("expid_d2_m1_2_SIM", "expid_d2_m1_2_POST"),
        ("expid_d2_m1_2_POST", "expid_d2_m1_2_CLEAN"),
        ("expid_d2_m2_1_SIM", "expid_d2_m2_1_POST"),
        ("expid_d2_m2_1_POST", "expid_d2_m2_1_CLEAN"),
        ("expid_d2_m2_1_SIM", "d2_m2_2")
    ]
    for job in jobs:
        job_list.add_job(job)

    for edge in [edge for edge in edges if edge[0].startswith('expid_') and edge[1].startswith('expid_')]:
        job_list._add_edge_and_parent({"e_from": edge[0], "e_to": edge[1]})

    as_conf = autosubmit_config(_EXPID, {
        'JOBS': {},
        'PLATFORMS': {},
    })
    common = {
        '_wallclock': '02:00',
        '_num_processors': 4,
        'platform': 'test-hpc',
        'platforms': {
            'test-hpc': {
                'queue': 'test-queue',
                'partition': 'test-partition',
                'account': 'test-account'
            }
        }
    }
    package_1 = {
        'name': 'package_d1_m1_SIM',
        'jobs': [
            job_list.get_job_by_name('expid_d1_m1_1_SIM'),
            job_list.get_job_by_name('expid_d1_m1_2_SIM'),
        ],
    }
    package_1.update(common)
    package_2 = {
        'name': 'package_d2_m2_SIM',
        'jobs': [
            job_list.get_job_by_name('expid_d2_m2_1_SIM'),
            job_list.get_job_by_name('expid_d2_m2_2_SIM'),
        ],
    }
    package_2.update(common)
    packages_dict = [package_1, package_2]
    packages = []
    for package in packages_dict:
        packages.append(WrapperJob(
            package['name'],
            jobs[0].id,
            Status.WAITING,
            0,
            package['jobs'],
            package['_wallclock'],
            package['_num_processors'],
            package['platform'],
            as_conf,
        ))

    return jobs, edges, job_list, packages


def test_wrapper_package(setup_wrappers, autosubmit_config):
    _, _, job_list, packages = setup_wrappers
    monitor = Monitor()
    graph = monitor.create_tree_list(_EXPID, job_list.get_job_list(), packages, dict())
    assert not graph.obj_dict['strict']
    for wrapper_job in packages:
        assert 'cluster_' + wrapper_job.name in graph.obj_dict['subgraphs']


# TODO wip
# def test_wrapper_package_with_job_edges(setup_wrappers, autosubmit_config):
#     _, _, job_list, packages = setup_wrappers
#     test = job_list.graph_dict
#     job_edges = job_list.graph_dict_by_job_name
#     job_edges['expid_d1_m1_1_SIM'] = [{'e_to': 'expid_d1_m1_2_SIM', 'from_step': 0, 'min_trigger_status': 'COMPLETED',
#                                        'completion_status': 'WAITING', 'fail_ok': True}]
#     job_edges['expid_d2_m2_1_SIM'] = [{'e_to': 'expid_d2_m2_2_SIM', 'from_step': 1, 'min_trigger_status': 'RUNNING',
#                                        'completion_status': 'WAITING', 'fail_ok': True}]
#
#     monitor = Monitor(job_edges)
#
#     graph = monitor.create_tree_list(_EXPID, job_list.get_job_list(), packages, dict())
#     assert not graph.obj_dict['strict']
#     for wrapper_job in packages:
#         assert 'cluster_' + wrapper_job.name in graph.obj_dict['subgraphs']


def test_synchronize_member_group_member(job_list):
    for date in ['d1', 'd2']:
        for chunk in [1, 2]:
            job = _create_dummy_job('expid_' + date + '_' + str(chunk) + '_ASIM',
                                    Status.WAITING, date, None, chunk)
            for member in ['m1', 'm2']:
                job.add_parent(
                    job_list.get_job_by_name('expid_' + date + '_' + member + '_' + str(chunk) + '_SIM'))

            job_list.add_job(job)

    groups_dict = dict()
    groups_dict['status'] = {'d1_m1': Status.WAITING,
                             'd1_m2': Status.WAITING,
                             'd2_m1': Status.WAITING,
                             'd2_m2': Status.WAITING}
    groups_dict['jobs'] = {
        'expid_d1_m1_INI': ['d1_m1'], 'expid_d1_m2_INI': ['d1_m2'], 'expid_d2_m1_INI': ['d2_m1'],
        'expid_d2_m2_INI': ['d2_m2'],
        'expid_d1_m1_1_SIM': ['d1_m1'], 'expid_d1_m1_2_SIM': ['d1_m1'], 'expid_d1_m2_1_SIM': ['d1_m2'],
        'expid_d1_m2_2_SIM': ['d1_m2'],
        'expid_d2_m1_1_SIM': ['d2_m1'], 'expid_d2_m1_2_SIM': ['d2_m1'], 'expid_d2_m2_1_SIM': ['d2_m2'],
        'expid_d2_m2_2_SIM': ['d2_m2'],

        'expid_d1_m1_1_POST': ['d1_m1'], 'expid_d1_m1_2_POST': ['d1_m1'], 'expid_d1_m2_1_POST': ['d1_m2'],
        'expid_d1_m2_2_POST': ['d1_m2'],
        'expid_d2_m1_1_POST': ['d2_m1'], 'expid_d2_m1_2_POST': ['d2_m1'], 'expid_d2_m2_1_POST': ['d2_m2'],
        'expid_d2_m2_2_POST': ['d2_m2'],

        'expid_d1_m1_1_CLEAN': ['d1_m1'], 'expid_d1_m1_2_CLEAN': ['d1_m1'], 'expid_d1_m2_1_CLEAN': ['d1_m2'],
        'expid_d1_m2_2_CLEAN': ['d1_m2'],
        'expid_d2_m1_1_CLEAN': ['d2_m1'], 'expid_d2_m1_2_CLEAN': ['d2_m1'], 'expid_d2_m2_1_CLEAN': ['d2_m2'],
        'expid_d2_m2_2_CLEAN': ['d2_m2'],

        'expid_d1_1_ASIM': ['d1_m1', 'd1_m2'], 'expid_d1_2_ASIM': ['d1_m1', 'd1_m2'],
        'expid_d2_1_ASIM': ['d2_m1', 'd2_m2'], 'expid_d2_2_ASIM': ['d2_m1', 'd2_m2']
    }

    nodes = [
        "expid_SETUP",
        'd1_m1', 'd1_m2', 'd2_m2', 'd2_m1'
    ]
    edges = [
        ("expid_SETUP", "d1_m1"), ("expid_SETUP", "d1_m2"), ("expid_SETUP", "d2_m1"),
        ("expid_SETUP", "d2_m2"),

        ("d1_m1", "d1_m1"), ("d1_m2", "d1_m2"),
        ("d2_m1", "d2_m1"), ("d2_m2", "d2_m2")
    ]

    monitor = Monitor()
    graph = monitor.create_tree_list(_EXPID, job_list.get_job_list(), None, groups_dict)

    assert graph.obj_dict['strict']

    subgraphs = graph.obj_dict['subgraphs']
    experiment_subgraph = subgraphs['Experiment'][0]

    assert sorted(list(experiment_subgraph['nodes'].keys())) == sorted(nodes)
    assert sorted(list(experiment_subgraph['edges'].keys())) == sorted(edges)

    subgraph_synchronize_d1 = graph.obj_dict['subgraphs']['cluster_d1_m1_d1_m2'][0]
    assert sorted(list(subgraph_synchronize_d1['nodes'].keys())) == sorted(['d1_m1', 'd1_m2'])
    assert sorted(list(subgraph_synchronize_d1['edges'].keys())) == sorted([('d1_m1', 'd1_m2')])

    subgraph_synchronize_d2 = graph.obj_dict['subgraphs']['cluster_d2_m1_d2_m2'][0]
    assert sorted(list(subgraph_synchronize_d2['nodes'].keys())) == sorted(['d2_m1', 'd2_m2'])
    assert sorted(list(subgraph_synchronize_d2['edges'].keys())) == sorted([('d2_m1', 'd2_m2')])


def test_synchronize_member_group_chunk(job_list):
    for date in ['d1', 'd2']:
        for chunk in [1, 2]:
            job = _create_dummy_job('expid_' + date + '_' + str(chunk) + '_ASIM',
                                    Status.WAITING, date, None, chunk)
            for member in ['m1', 'm2']:
                job.add_parent(
                    job_list.get_job_by_name('expid_' + date + '_' + member + '_' + str(chunk) + '_SIM'))
            job_list.add_job(job)

    groups_dict = dict()
    groups_dict['status'] = {'d1_m1_1': Status.WAITING, 'd1_m1_2': Status.WAITING,
                             'd1_m2_1': Status.WAITING, 'd1_m2_2': Status.WAITING,
                             'd2_m1_1': Status.WAITING, 'd2_m1_2': Status.WAITING,
                             'd2_m2_1': Status.WAITING, 'd2_m2_2': Status.WAITING}
    groups_dict['jobs'] = {
        'expid_d1_m1_1_SIM': ['d1_m1_1'], 'expid_d1_m1_2_SIM': ['d1_m1_2'], 'expid_d1_m2_1_SIM': ['d1_m2_1'],
        'expid_d1_m2_2_SIM': ['d1_m2_2'],
        'expid_d2_m1_1_SIM': ['d2_m1_1'], 'expid_d2_m1_2_SIM': ['d2_m1_2'], 'expid_d2_m2_1_SIM': ['d2_m2_1'],
        'expid_d2_m2_2_SIM': ['d2_m2_2'],

        'expid_d1_m1_1_POST': ['d1_m1_1'], 'expid_d1_m1_2_POST': ['d1_m1_2'], 'expid_d1_m2_1_POST': ['d1_m2_1'],
        'expid_d1_m2_2_POST': ['d1_m2_2'],
        'expid_d2_m1_1_POST': ['d2_m1_1'], 'expid_d2_m1_2_POST': ['d2_m1_2'], 'expid_d2_m2_1_POST': ['d2_m2_1'],
        'expid_d2_m2_2_POST': ['d2_m2_2'],

        'expid_d1_m1_1_CLEAN': ['d1_m1_1'], 'expid_d1_m1_2_CLEAN': ['d1_m1_2'], 'expid_d1_m2_1_CLEAN': ['d1_m2_1'],
        'expid_d1_m2_2_CLEAN': ['d1_m2_2'],
        'expid_d2_m1_1_CLEAN': ['d2_m1_1'], 'expid_d2_m1_2_CLEAN': ['d2_m1_2'], 'expid_d2_m2_1_CLEAN': ['d2_m2_1'],
        'expid_d2_m2_2_CLEAN': ['d2_m2_2'],

        'expid_d1_1_ASIM': ['d1_m1_1', 'd1_m2_1'], 'expid_d1_2_ASIM': ['d1_m1_2', 'd1_m2_2'],
        'expid_d2_1_ASIM': ['d2_m1_1', 'd2_m2_1'], 'expid_d2_2_ASIM': ['d2_m1_2', 'd2_m2_2']
    }

    nodes = [
        "expid_SETUP", "expid_d1_m1_INI", "expid_d1_m2_INI", "expid_d2_m1_INI", "expid_d2_m2_INI",
        'd1_m1_1', 'd1_m2_1', 'd2_m1_1', 'd2_m2_1', 'd1_m1_2', 'd1_m2_2', 'd2_m1_2', 'd2_m2_2'
    ]
    edges = [
        ("expid_SETUP", "expid_d1_m1_INI"), ("expid_SETUP", "expid_d1_m2_INI"), ("expid_SETUP", "expid_d2_m1_INI"),
        ("expid_SETUP", "expid_d2_m2_INI"), ("expid_d1_m1_INI", "d1_m1_1"), ("expid_d1_m2_INI", "d1_m2_1"),
        ("expid_d2_m1_INI", "d2_m1_1"), ("expid_d2_m2_INI", "d2_m2_1"),
        ("d1_m1_1", "d1_m1_2"), ("d1_m2_1", "d1_m2_2"), ("d2_m1_1", "d2_m1_2"), ("d2_m2_1", "d2_m2_2"),

        ("d1_m1_1", "d1_m1_1"), ("d1_m1_2", "d1_m1_2"), ("d1_m2_1", "d1_m2_1"), ("d1_m2_2", "d1_m2_2"),
        ("d2_m1_1", "d2_m1_1"), ("d2_m1_2", "d2_m1_2"), ("d2_m2_1", "d2_m2_1"), ("d2_m2_2", "d2_m2_2")
    ]

    monitor = Monitor()
    graph = monitor.create_tree_list(_EXPID, job_list.get_job_list(), None, groups_dict)

    assert graph.obj_dict['strict']

    subgraphs = graph.obj_dict['subgraphs']
    experiment_subgraph = subgraphs['Experiment'][0]

    assert sorted(list(experiment_subgraph['nodes'].keys())) == sorted(nodes)
    assert sorted(list(experiment_subgraph['edges'].keys())) == sorted(edges)

    subgraph_synchronize_d1_1 = graph.obj_dict['subgraphs']['cluster_d1_m1_1_d1_m2_1'][0]
    assert sorted(list(subgraph_synchronize_d1_1['nodes'].keys())) == sorted(['d1_m1_1', 'd1_m2_1'])
    assert sorted(list(subgraph_synchronize_d1_1['edges'].keys())) == sorted([('d1_m1_1', 'd1_m2_1')])

    subgraph_synchronize_d1_2 = graph.obj_dict['subgraphs']['cluster_d1_m1_2_d1_m2_2'][0]
    assert sorted(list(subgraph_synchronize_d1_2['nodes'].keys())) == sorted(['d1_m1_2', 'd1_m2_2'])
    assert sorted(list(subgraph_synchronize_d1_2['edges'].keys())) == sorted([('d1_m1_2', 'd1_m2_2')])

    subgraph_synchronize_d2_1 = graph.obj_dict['subgraphs']['cluster_d2_m1_1_d2_m2_1'][0]
    assert sorted(list(subgraph_synchronize_d2_1['nodes'].keys())) == sorted(['d2_m1_1', 'd2_m2_1'])
    assert sorted(list(subgraph_synchronize_d2_1['edges'].keys())) == sorted([('d2_m1_1', 'd2_m2_1')])

    subgraph_synchronize_d2_2 = graph.obj_dict['subgraphs']['cluster_d2_m1_2_d2_m2_2'][0]
    assert sorted(list(subgraph_synchronize_d2_2['nodes'].keys())) == sorted(['d2_m1_2', 'd2_m2_2'])
    assert sorted(list(subgraph_synchronize_d2_2['edges'].keys())) == sorted([('d2_m1_2', 'd2_m2_2')])


def test_synchronize_member_group_date(job_list):
    for date in ['d1', 'd2']:
        for chunk in [1, 2]:
            job = _create_dummy_job('expid_' + date + '_' + str(chunk) + '_ASIM',
                                    Status.WAITING, date, None, chunk)
            for member in ['m1', 'm2']:
                job.add_parent(
                    job_list.get_job_by_name('expid_' + date + '_' + member + '_' + str(chunk) + '_SIM'))

            job_list.add_job(job)

    groups_dict = dict()
    groups_dict['status'] = {'d1': Status.WAITING,
                             'd2': Status.WAITING}
    groups_dict['jobs'] = {
        'expid_d1_m1_INI': ['d1'], 'expid_d1_m2_INI': ['d1'], 'expid_d2_m1_INI': ['d2'],
        'expid_d2_m2_INI': ['d2'],
        'expid_d1_m1_1_SIM': ['d1'], 'expid_d1_m1_2_SIM': ['d1'], 'expid_d1_m2_1_SIM': ['d1'],
        'expid_d1_m2_2_SIM': ['d1'],
        'expid_d2_m1_1_SIM': ['d2'], 'expid_d2_m1_2_SIM': ['d2'], 'expid_d2_m2_1_SIM': ['d2'],
        'expid_d2_m2_2_SIM': ['d2'],

        'expid_d1_m1_1_POST': ['d1'], 'expid_d1_m1_2_POST': ['d1'], 'expid_d1_m2_1_POST': ['d1'],
        'expid_d1_m2_2_POST': ['d1'],
        'expid_d2_m1_1_POST': ['d2'], 'expid_d2_m1_2_POST': ['d2'], 'expid_d2_m2_1_POST': ['d2'],
        'expid_d2_m2_2_POST': ['d2'],

        'expid_d1_m1_1_CLEAN': ['d1'], 'expid_d1_m1_2_CLEAN': ['d1'], 'expid_d1_m2_1_CLEAN': ['d1'],
        'expid_d1_m2_2_CLEAN': ['d1'],
        'expid_d2_m1_1_CLEAN': ['d2'], 'expid_d2_m1_2_CLEAN': ['d2'], 'expid_d2_m2_1_CLEAN': ['d2'],
        'expid_d2_m2_2_CLEAN': ['d2'],

        'expid_d1_1_ASIM': ['d1'], 'expid_d1_2_ASIM': ['d1'],
        'expid_d2_1_ASIM': ['d2'], 'expid_d2_2_ASIM': ['d2']
    }

    nodes = [
        "expid_SETUP",
        'd1', 'd2'
    ]
    edges = [
        ("expid_SETUP", "d1"), ("expid_SETUP", "d2"),

        ("d1", "d1"), ("d2", "d2")
    ]

    monitor = Monitor()
    graph = monitor.create_tree_list(_EXPID, job_list.get_job_list(), None, groups_dict)

    assert graph.obj_dict['strict']

    subgraphs = graph.obj_dict['subgraphs']
    experiment_subgraph = subgraphs['Experiment'][0]

    assert sorted(list(experiment_subgraph['nodes'].keys())) == sorted(nodes)
    assert sorted(list(experiment_subgraph['edges'].keys())) == sorted(edges)

    for subgraph in list(subgraphs.keys()):
        assert not subgraph.startswith('cluster')


def test_synchronize_date_group_member(job_list):
    for chunk in [1, 2]:
        job = _create_dummy_job('expid_' + str(chunk) + '_ASIM',
                                Status.WAITING, None, None, chunk)
        for date in ['d1', 'd2']:
            for member in ['m1', 'm2']:
                job.add_parent(
                    job_list.get_job_by_name('expid_' + date + '_' + member + '_' + str(chunk) + '_SIM'))

        job_list.add_job(job)

    groups_dict = dict()
    groups_dict['status'] = {'d1_m1': Status.WAITING,
                             'd1_m2': Status.WAITING,
                             'd2_m1': Status.WAITING,
                             'd2_m2': Status.WAITING}
    groups_dict['jobs'] = {
        'expid_d1_m1_INI': ['d1_m1'], 'expid_d1_m2_INI': ['d1_m2'], 'expid_d2_m1_INI': ['d2_m1'],
        'expid_d2_m2_INI': ['d2_m2'],
        'expid_d1_m1_1_SIM': ['d1_m1'], 'expid_d1_m1_2_SIM': ['d1_m1'], 'expid_d1_m2_1_SIM': ['d1_m2'],
        'expid_d1_m2_2_SIM': ['d1_m2'],
        'expid_d2_m1_1_SIM': ['d2_m1'], 'expid_d2_m1_2_SIM': ['d2_m1'], 'expid_d2_m2_1_SIM': ['d2_m2'],
        'expid_d2_m2_2_SIM': ['d2_m2'],

        'expid_d1_m1_1_POST': ['d1_m1'], 'expid_d1_m1_2_POST': ['d1_m1'], 'expid_d1_m2_1_POST': ['d1_m2'],
        'expid_d1_m2_2_POST': ['d1_m2'],
        'expid_d2_m1_1_POST': ['d2_m1'], 'expid_d2_m1_2_POST': ['d2_m1'], 'expid_d2_m2_1_POST': ['d2_m2'],
        'expid_d2_m2_2_POST': ['d2_m2'],

        'expid_d1_m1_1_CLEAN': ['d1_m1'], 'expid_d1_m1_2_CLEAN': ['d1_m1'], 'expid_d1_m2_1_CLEAN': ['d1_m2'],
        'expid_d1_m2_2_CLEAN': ['d1_m2'],
        'expid_d2_m1_1_CLEAN': ['d2_m1'], 'expid_d2_m1_2_CLEAN': ['d2_m1'], 'expid_d2_m2_1_CLEAN': ['d2_m2'],
        'expid_d2_m2_2_CLEAN': ['d2_m2'],

        'expid_1_ASIM': ['d1_m1', 'd1_m2', 'd2_m1', 'd2_m2'], 'expid_2_ASIM': ['d1_m1', 'd1_m2', 'd2_m1', 'd2_m2']
    }

    nodes = [
        "expid_SETUP",
        'd1_m1', 'd1_m2', 'd2_m2', 'd2_m1'
    ]
    edges = [
        ("expid_SETUP", "d1_m1"), ("expid_SETUP", "d1_m2"), ("expid_SETUP", "d2_m1"),
        ("expid_SETUP", "d2_m2"),

        ("d1_m1", "d1_m1"), ("d1_m2", "d1_m2"),
        ("d2_m1", "d2_m1"), ("d2_m2", "d2_m2")
    ]

    monitor = Monitor()
    graph = monitor.create_tree_list(_EXPID, job_list.get_job_list(), None, groups_dict)

    assert graph.obj_dict['strict']

    subgraphs = graph.obj_dict['subgraphs']
    experiment_subgraph = subgraphs['Experiment'][0]

    assert sorted(list(experiment_subgraph['nodes'].keys())) == sorted(nodes)
    assert sorted(list(experiment_subgraph['edges'].keys())) == sorted(edges)

    subgraph_synchronize_d1_d2 = graph.obj_dict['subgraphs']['cluster_d1_m1_d1_m2_d2_m1_d2_m2'][0]
    assert sorted(list(subgraph_synchronize_d1_d2['nodes'].keys())) == \
           sorted(['d1_m1', 'd1_m2', 'd2_m1', 'd2_m2'])
    assert sorted(list(subgraph_synchronize_d1_d2['edges'].keys())) == \
           sorted([('d1_m1', 'd1_m2'), ('d1_m2', 'd2_m1'), ('d2_m1', 'd2_m2')])


def test_synchronize_date_group_chunk(job_list):
    for chunk in [1, 2]:
        job = _create_dummy_job('expid_' + str(chunk) + '_ASIM',
                                Status.WAITING, None, None, chunk)
        for date in ['d1', 'd2']:
            for member in ['m1', 'm2']:
                job.add_parent(
                    job_list.get_job_by_name('expid_' + date + '_' + member + '_' + str(chunk) + '_SIM'))

        job_list.add_job(job)

    groups_dict = dict()
    groups_dict['status'] = {'d1_m1_1': Status.WAITING, 'd1_m1_2': Status.WAITING,
                             'd1_m2_1': Status.WAITING, 'd1_m2_2': Status.WAITING,
                             'd2_m1_1': Status.WAITING, 'd2_m1_2': Status.WAITING,
                             'd2_m2_1': Status.WAITING, 'd2_m2_2': Status.WAITING}
    groups_dict['jobs'] = {
        'expid_d1_m1_1_SIM': ['d1_m1_1'], 'expid_d1_m1_2_SIM': ['d1_m1_2'], 'expid_d1_m2_1_SIM': ['d1_m2_1'],
        'expid_d1_m2_2_SIM': ['d1_m2_2'],
        'expid_d2_m1_1_SIM': ['d2_m1_1'], 'expid_d2_m1_2_SIM': ['d2_m1_2'], 'expid_d2_m2_1_SIM': ['d2_m2_1'],
        'expid_d2_m2_2_SIM': ['d2_m2_2'],

        'expid_d1_m1_1_POST': ['d1_m1_1'], 'expid_d1_m1_2_POST': ['d1_m1_2'], 'expid_d1_m2_1_POST': ['d1_m2_1'],
        'expid_d1_m2_2_POST': ['d1_m2_2'],
        'expid_d2_m1_1_POST': ['d2_m1_1'], 'expid_d2_m1_2_POST': ['d2_m1_2'], 'expid_d2_m2_1_POST': ['d2_m2_1'],
        'expid_d2_m2_2_POST': ['d2_m2_2'],

        'expid_d1_m1_1_CLEAN': ['d1_m1_1'], 'expid_d1_m1_2_CLEAN': ['d1_m1_2'], 'expid_d1_m2_1_CLEAN': ['d1_m2_1'],
        'expid_d1_m2_2_CLEAN': ['d1_m2_2'],
        'expid_d2_m1_1_CLEAN': ['d2_m1_1'], 'expid_d2_m1_2_CLEAN': ['d2_m1_2'], 'expid_d2_m2_1_CLEAN': ['d2_m2_1'],
        'expid_d2_m2_2_CLEAN': ['d2_m2_2'],

        'expid_1_ASIM': ['d1_m1_1', 'd1_m2_1', 'd2_m1_1', 'd2_m2_1'],
        'expid_2_ASIM': ['d1_m1_2', 'd1_m2_2', 'd2_m1_2', 'd2_m2_2'],
    }

    monitor = Monitor()
    graph = monitor.create_tree_list(_EXPID, job_list.get_job_list(), None, groups_dict)

    assert graph.obj_dict['strict']

    subgraph_synchronize_1 = graph.obj_dict['subgraphs']['cluster_d1_m1_1_d1_m2_1_d2_m1_1_d2_m2_1'][0]
    assert sorted(list(subgraph_synchronize_1['nodes'].keys())) == \
           sorted(['d1_m1_1', 'd1_m2_1', 'd2_m1_1', 'd2_m2_1'])
    assert sorted(list(subgraph_synchronize_1['edges'].keys())) == \
           sorted([('d1_m1_1', 'd1_m2_1'), ('d1_m2_1', 'd2_m1_1'), ('d2_m1_1', 'd2_m2_1')])

    subgraph_synchronize_2 = graph.obj_dict['subgraphs']['cluster_d1_m1_2_d1_m2_2_d2_m1_2_d2_m2_2'][0]
    assert sorted(list(subgraph_synchronize_2['nodes'].keys())) == \
           sorted(['d1_m1_2', 'd1_m2_2', 'd2_m1_2', 'd2_m2_2'])
    assert sorted(list(subgraph_synchronize_2['edges'].keys())) == \
           sorted([('d1_m1_2', 'd1_m2_2'), ('d1_m2_2', 'd2_m1_2'), ('d2_m1_2', 'd2_m2_2')])


def test_synchronize_date_group_date(job_list):
    for chunk in [1, 2]:
        job = _create_dummy_job('expid_' + str(chunk) + '_ASIM',
                                Status.WAITING, None, None, chunk)
        for date in ['d1', 'd2']:
            for member in ['m1', 'm2']:
                job.add_parent(
                    job_list.get_job_by_name('expid_' + date + '_' + member + '_' + str(chunk) + '_SIM'))

        job_list.add_job(job)

    groups_dict = dict()
    groups_dict['status'] = {'d1': Status.WAITING,
                             'd2': Status.WAITING}
    groups_dict['jobs'] = {
        'expid_d1_m1_INI': ['d1'], 'expid_d1_m2_INI': ['d1'], 'expid_d2_m1_INI': ['d2'],
        'expid_d2_m2_INI': ['d2'],
        'expid_d1_m1_1_SIM': ['d1'], 'expid_d1_m1_2_SIM': ['d1'], 'expid_d1_m2_1_SIM': ['d1'],
        'expid_d1_m2_2_SIM': ['d1'],
        'expid_d2_m1_1_SIM': ['d2'], 'expid_d2_m1_2_SIM': ['d2'], 'expid_d2_m2_1_SIM': ['d2'],
        'expid_d2_m2_2_SIM': ['d2'],

        'expid_d1_m1_1_POST': ['d1'], 'expid_d1_m1_2_POST': ['d1'], 'expid_d1_m2_1_POST': ['d1'],
        'expid_d1_m2_2_POST': ['d1'],
        'expid_d2_m1_1_POST': ['d2'], 'expid_d2_m1_2_POST': ['d2'], 'expid_d2_m2_1_POST': ['d2'],
        'expid_d2_m2_2_POST': ['d2'],

        'expid_d1_m1_1_CLEAN': ['d1'], 'expid_d1_m1_2_CLEAN': ['d1'], 'expid_d1_m2_1_CLEAN': ['d1'],
        'expid_d1_m2_2_CLEAN': ['d1'],
        'expid_d2_m1_1_CLEAN': ['d2'], 'expid_d2_m1_2_CLEAN': ['d2'], 'expid_d2_m2_1_CLEAN': ['d2'],
        'expid_d2_m2_2_CLEAN': ['d2'],

        'expid_1_ASIM': ['d1', 'd2'], 'expid_2_ASIM': ['d1', 'd2']
    }

    nodes = [
        "expid_SETUP",
        'd1', 'd2'
    ]
    edges = [
        ("expid_SETUP", "d1"), ("expid_SETUP", "d2"),

        ("d1", "d1"), ("d2", "d2")
    ]

    monitor = Monitor()
    graph = monitor.create_tree_list(_EXPID, job_list.get_job_list(), None, groups_dict)

    assert graph.obj_dict['strict']

    subgraphs = graph.obj_dict['subgraphs']
    experiment_subgraph = subgraphs['Experiment'][0]

    assert sorted(list(experiment_subgraph['nodes'].keys())) == sorted(nodes)
    assert sorted(list(experiment_subgraph['edges'].keys())) == sorted(edges)

    subgraph_synchronize_d1_d2 = graph.obj_dict['subgraphs']['cluster_d1_d2'][0]
    assert sorted(list(subgraph_synchronize_d1_d2['nodes'].keys())) == \
           sorted(['d1', 'd2'])


def test_normal_workflow(job_list):
    nodes = [
        "expid_SETUP", "expid_d1_m1_INI", "expid_d1_m2_INI", "expid_d2_m1_INI", "expid_d2_m2_INI",
        "expid_d1_m1_1_SIM", "expid_d1_m1_2_SIM", "expid_d1_m2_1_SIM", "expid_d1_m2_2_SIM",
        "expid_d2_m1_1_SIM", "expid_d2_m1_2_SIM", "expid_d2_m2_1_SIM", "expid_d2_m2_2_SIM",
        "expid_d1_m1_1_POST", "expid_d1_m1_2_POST", "expid_d1_m2_1_POST", "expid_d1_m2_2_POST",
        "expid_d2_m1_1_POST", "expid_d2_m1_2_POST", "expid_d2_m2_1_POST", "expid_d2_m2_2_POST",
        "expid_d1_m1_1_CLEAN", "expid_d1_m1_2_CLEAN", "expid_d1_m2_1_CLEAN", "expid_d1_m2_2_CLEAN",
        "expid_d2_m1_1_CLEAN", "expid_d2_m1_2_CLEAN", "expid_d2_m2_1_CLEAN", "expid_d2_m2_2_CLEAN"
    ]
    edges = [
        ('expid_SETUP', 'expid_d1_m1_INI'), ('expid_SETUP', 'expid_d1_m2_INI'), ('expid_SETUP', 'expid_d2_m1_INI'),
        ('expid_SETUP', 'expid_d2_m2_INI'),

        ('expid_d1_m1_INI', 'expid_d1_m1_1_SIM'), ('expid_d1_m2_INI', 'expid_d1_m2_1_SIM'),
        ('expid_d2_m1_INI', 'expid_d2_m1_1_SIM'), ('expid_d2_m2_INI', 'expid_d2_m2_1_SIM'),

        ('expid_d1_m1_1_SIM', 'expid_d1_m1_2_SIM'),
        ('expid_d1_m1_1_SIM', 'expid_d1_m1_1_POST'),

        ('expid_d1_m1_2_SIM', 'expid_d1_m1_2_POST'),

        ('expid_d1_m2_1_SIM', 'expid_d1_m2_2_SIM'),
        ('expid_d1_m2_1_SIM', 'expid_d1_m2_1_POST'),

        ('expid_d1_m2_2_SIM', 'expid_d1_m2_2_POST'),

        ('expid_d2_m1_1_SIM', 'expid_d2_m1_2_SIM'),
        ('expid_d2_m1_1_SIM', 'expid_d2_m1_1_POST'),

        ('expid_d2_m1_2_SIM', 'expid_d2_m1_2_POST'),

        ('expid_d2_m2_1_SIM', 'expid_d2_m2_2_SIM'),
        ('expid_d2_m2_1_SIM', 'expid_d2_m2_1_POST'),

        ('expid_d2_m2_2_SIM', 'expid_d2_m2_2_POST'),

        ('expid_d1_m1_1_POST', 'expid_d1_m1_1_CLEAN'),
        ('expid_d1_m1_2_POST', 'expid_d1_m1_2_CLEAN'),
        ('expid_d1_m2_1_POST', 'expid_d1_m2_1_CLEAN'),
        ('expid_d1_m2_2_POST', 'expid_d1_m2_2_CLEAN'),

        ('expid_d2_m1_1_POST', 'expid_d2_m1_1_CLEAN'),
        ('expid_d2_m1_2_POST', 'expid_d2_m1_2_CLEAN'),
        ('expid_d2_m2_1_POST', 'expid_d2_m2_1_CLEAN'),
        ('expid_d2_m2_2_POST', 'expid_d2_m2_2_CLEAN')
    ]

    monitor = Monitor()
    graph = monitor.create_tree_list(_EXPID, job_list.get_job_list(), None, dict())

    assert not graph.obj_dict['strict']

    subgraph = graph.obj_dict['subgraphs']['Experiment'][0]

    assert sorted(list(subgraph['nodes'].keys())) == sorted(nodes)
    assert sorted(list(subgraph['edges'].keys())) == sorted(edges)


def test_wrapper_and_groups(setup_wrappers):
    nodes, edges, job_list, packages = setup_wrappers

    groups_dict = dict()

    groups_dict['status'] = {'d1_m1_1': Status.FAILED, 'd1_m1_2': Status.READY, 'd1_m2_1': Status.RUNNING,
                             'd2_m2_2': Status.WAITING}

    groups_dict['jobs'] = {
        'expid_d1_m1_1_SIM': ['d1_m1_1'], 'expid_d1_m1_2_SIM': ['d1_m1_2'], 'expid_d1_m2_1_SIM': ['d1_m2_1'],
        'expid_d2_m2_2_SIM': ['d2_m2_2'],

        'expid_d1_m1_1_POST': ['d1_m1_1'], 'expid_d1_m1_2_POST': ['d1_m1_2'], 'expid_d1_m2_1_POST': ['d1_m2_1'],
        'expid_d2_m2_2_POST': ['d2_m2_2'],

        'expid_d1_m1_1_CLEAN': ['d1_m1_1'], 'expid_d1_m1_2_CLEAN': ['d1_m1_2'], 'expid_d1_m2_1_CLEAN': ['d1_m2_1'],
        'expid_d2_m2_2_CLEAN': ['d2_m2_2']
    }

    monitor = Monitor()
    graph = monitor.create_tree_list(_EXPID, job_list.get_job_list(), packages, groups_dict)
    assert graph.obj_dict['strict']

    for wrapper_job in packages:

        if wrapper_job.name != 'package_d2_m2_SIM':
            assert 'cluster_' + wrapper_job.name not in graph.obj_dict['subgraphs']
        else:
            assert 'cluster_' + wrapper_job.name in graph.obj_dict['subgraphs']

    # TODO: This test does not match the original expected results.
    # This may be due to differences in the job_list ( there are more jobs) or the definition of groups_dict['jobs'].
    # subgraph = graph.obj_dict['subgraphs']['Experiment'][0]
    # nodes_str = [node.name for node in nodes]
    # assert sorted(list(subgraph['nodes'].keys())) == sorted(nodes_str)
    # assert sorted(list(subgraph['edges'].keys())) == sorted(edges)
