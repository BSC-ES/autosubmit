import shutil
import tempfile

from unittest import TestCase
from mock import Mock

from autosubmit.job.job_common import Status
from autosubmit.job.job_list import JobList
from autosubmit.job.job_list_persistence import JobListPersistenceDb
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory
from random import randrange
from autosubmit.job.job import Job
from autosubmit.monitor.monitor import Monitor
import unittest
class TestJobGraph(TestCase):

    def setUp(self):
        self.experiment_id = 'random-id'
        self.as_conf = Mock()
        self.as_conf.experiment_data = dict()
        self.as_conf.experiment_data["JOBS"] = dict()
        self.as_conf.jobs_data = self.as_conf.experiment_data["JOBS"]
        self.as_conf.experiment_data["PLATFORMS"] = dict()
        self.temp_directory = tempfile.mkdtemp()
        self.job_list = JobList(self.experiment_id, self.as_conf, YAMLParserFactory(),
                                JobListPersistenceDb(self.temp_directory, 'db'))
        self.parser_mock = Mock(spec='SafeConfigParser')

        # Basic workflow with SETUP, INI, SIM, POST, CLEAN
        setup_job = self._createDummyJob('expid_SETUP', Status.READY)
        self.job_list.get_job_list().append(setup_job)

        for date in ['d1', 'd2']:
            for member in ['m1', 'm2']:
                job = self._createDummyJob('expid_' + date + '_' + member + '_' + 'INI', Status.WAITING, date, member)
                job.add_parent(setup_job)
                self.job_list.get_job_list().append(job)

        sections = ['SIM', 'POST', 'CLEAN']
        for section in sections:
            for date in ['d1', 'd2']:
                for member in ['m1', 'm2']:
                    for chunk in [1, 2]:
                        job = self._createDummyJob('expid_' + date + '_' + member + '_' + str(chunk) + '_' + section,
                                                   Status.WAITING, date, member, chunk)
                        if section == 'SIM':
                            if chunk > 1:
                                job.add_parent(self.job_list.get_job_by_name('expid_'+date+'_'+member+'_'+str(chunk-1)+'_SIM'))
                            else:
                                job.add_parent(self.job_list.get_job_by_name('expid_'+date+'_'+member+'_INI'))
                        elif section == 'POST':
                            job.add_parent(self.job_list.get_job_by_name('expid_' + date + '_' + member + '_' + str(chunk) + '_SIM'))
                        elif section == 'CLEAN':
                            job.add_parent(self.job_list.get_job_by_name('expid_' + date + '_' + member + '_' + str(chunk) + '_POST'))
                        self.job_list.get_job_list().append(job)

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_directory)

    unittest.skip("TODO: Grouping changed, this test needs to be updated")
    def test_grouping_date(self):
        groups_dict = dict()
        groups_dict['status'] = {'d1': Status.WAITING, 'd2': Status.WAITING}
        groups_dict['jobs'] = {
            'expid_d1_m1_INI' : ['d1'], 'expid_d1_m2_INI' : ['d1'], 'expid_d2_m1_INI' : ['d2'], 'expid_d2_m2_INI' : ['d2'],
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
        graph = monitor.create_tree_list(self.experiment_id, self.job_list.get_job_list(), None, groups_dict)

        self.assertTrue(graph.obj_dict['strict'])

        subgraph = graph.obj_dict['subgraphs']['Experiment'][0]

        self.assertListEqual(sorted(list(subgraph['nodes'].keys())), sorted(nodes))
        self.assertListEqual(sorted(list(subgraph['edges'].keys())), sorted(edges))

    def test_grouping_member(self):
        groups_dict = dict()
        groups_dict['status'] = {'d1_m1': Status.WAITING,
                                 'd1_m2': Status.WAITING,
                                 'd2_m1': Status.WAITING,
                                 'd2_m2': Status.WAITING}
        groups_dict['jobs'] = {
            'expid_d1_m1_INI': ['d1_m1'], 'expid_d1_m2_INI': ['d1_m2'], 'expid_d2_m1_INI': ['d2_m1'], 'expid_d2_m2_INI': ['d2_m2'],
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
        graph = monitor.create_tree_list(self.experiment_id, self.job_list.get_job_list(), None, groups_dict)

        self.assertTrue(graph.obj_dict['strict'])

        subgraph = graph.obj_dict['subgraphs']['Experiment'][0]

        self.assertListEqual(sorted(list(subgraph['nodes'].keys())), sorted(nodes))
        self.assertListEqual(sorted(list(subgraph['edges'].keys())), sorted(edges))

    def test_grouping_chunk(self):
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
        graph = monitor.create_tree_list(self.experiment_id, self.job_list.get_job_list(), None, groups_dict)

        self.assertTrue(graph.obj_dict['strict'])

        subgraph = graph.obj_dict['subgraphs']['Experiment'][0]

        self.assertListEqual(sorted(list(subgraph['nodes'].keys())), sorted(nodes))
        self.assertListEqual(sorted(list(subgraph['edges'].keys())), sorted(edges))

    def test_grouping_automatic_hide(self):
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
        graph = monitor.create_tree_list(self.experiment_id, self.job_list.get_job_list(), None, groups_dict, hide_groups=True)

        self.assertTrue(graph.obj_dict['strict'])

        subgraph = graph.obj_dict['subgraphs']['Experiment'][0]

        self.assertListEqual(sorted(list(subgraph['nodes'].keys())), sorted(nodes))
        self.assertListEqual(sorted(list(subgraph['edges'].keys())), sorted(edges))

    def test_synchronize_member(self):
        for date in ['d1', 'd2']:
            for chunk in [1, 2]:
                job = self._createDummyJob('expid_' + date + '_' + str(chunk) + '_ASIM',
                                           Status.WAITING, date, None, chunk)
                for member in ['m1', 'm2']:
                    job.add_parent(self.job_list.get_job_by_name('expid_' + date + '_' + member + '_' + str(chunk) + '_SIM'))
                self.job_list.get_job_list().append(job)

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
        graph = monitor.create_tree_list(self.experiment_id, self.job_list.get_job_list(), None, dict())

        self.assertFalse(graph.obj_dict['strict'])

        subgraph = graph.obj_dict['subgraphs']['Experiment'][0]

        self.assertListEqual(sorted(list(subgraph['nodes'].keys())), sorted(nodes))
        self.assertListEqual(sorted(list(subgraph['edges'].keys())), sorted(edges))

    def test_synchronize_date(self):
        for chunk in [1, 2]:
            job = self._createDummyJob('expid_' + str(chunk) + '_ASIM',
                                       Status.WAITING, None, None, chunk)
            for date in ['d1', 'd2']:
                for member in ['m1', 'm2']:
                    job.add_parent(self.job_list.get_job_by_name('expid_' + date + '_' + member + '_' + str(chunk) + '_SIM'))
            self.job_list.get_job_list().append(job)

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
        graph = monitor.create_tree_list(self.experiment_id, self.job_list.get_job_list(), None, dict())

        self.assertFalse(graph.obj_dict['strict'])

        subgraph = graph.obj_dict['subgraphs']['Experiment'][0]

        self.assertListEqual(sorted(list(subgraph['nodes'].keys())), sorted(nodes))
        self.assertListEqual(sorted(list(subgraph['edges'].keys())), sorted(edges))

    def test_wrapper_package(self):
        packages = [('expid', 'package_d1_m1_SIM', 'expid_d1_m1_1_SIM', "02:00"), ('expid', 'package_d1_m1_SIM', 'expid_d1_m1_2_SIM', "02:00"),
                    ('expid', 'package_d2_m2_SIM', 'expid_d2_m2_1_SIM', "02:00"), ('expid', 'package_d2_m2_SIM', 'expid_d2_m2_2_SIM', "02:00")]

        monitor = Monitor()
        graph = monitor.create_tree_list(self.experiment_id, self.job_list.get_job_list(), packages, dict())
        self.assertFalse(graph.obj_dict['strict'])
        for (expid, package, job_name, _) in packages:
            self.assertIn('cluster_'+package, graph.obj_dict['subgraphs'])

    def test_synchronize_member_group_member(self):
        for date in ['d1', 'd2']:
            for chunk in [1, 2]:
                job = self._createDummyJob('expid_' + date + '_' + str(chunk) + '_ASIM',
                                           Status.WAITING, date, None, chunk)
                for member in ['m1', 'm2']:
                    job.add_parent(
                        self.job_list.get_job_by_name('expid_' + date + '_' + member + '_' + str(chunk) + '_SIM'))

                self.job_list.get_job_list().append(job)

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

            'expid_d1_1_ASIM' : ['d1_m1', 'd1_m2'], 'expid_d1_2_ASIM' : ['d1_m1', 'd1_m2'],
            'expid_d2_1_ASIM': ['d2_m1', 'd2_m2'], 'expid_d2_2_ASIM' : ['d2_m1', 'd2_m2']
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
        graph = monitor.create_tree_list(self.experiment_id, self.job_list.get_job_list(), None, groups_dict)

        self.assertTrue(graph.obj_dict['strict'])

        subgraphs = graph.obj_dict['subgraphs']
        experiment_subgraph = subgraphs['Experiment'][0]

        self.assertListEqual(sorted(list(experiment_subgraph['nodes'].keys())), sorted(nodes))
        self.assertListEqual(sorted(list(experiment_subgraph['edges'].keys())), sorted(edges))

        subgraph_synchronize_d1 = graph.obj_dict['subgraphs']['cluster_d1_m1_d1_m2'][0]
        self.assertListEqual(sorted(list(subgraph_synchronize_d1['nodes'].keys())), sorted(['d1_m1', 'd1_m2']))
        self.assertListEqual(sorted(list(subgraph_synchronize_d1['edges'].keys())), sorted([('d1_m1', 'd1_m2')]))

        subgraph_synchronize_d2 = graph.obj_dict['subgraphs']['cluster_d2_m1_d2_m2'][0]
        self.assertListEqual(sorted(list(subgraph_synchronize_d2['nodes'].keys())), sorted(['d2_m1', 'd2_m2']))
        self.assertListEqual(sorted(list(subgraph_synchronize_d2['edges'].keys())), sorted([('d2_m1', 'd2_m2')]))

    def test_synchronize_member_group_chunk(self):
        for date in ['d1', 'd2']:
            for chunk in [1, 2]:
                job = self._createDummyJob('expid_' + date + '_' + str(chunk) + '_ASIM',
                                           Status.WAITING, date, None, chunk)
                for member in ['m1', 'm2']:
                    job.add_parent(
                        self.job_list.get_job_by_name('expid_' + date + '_' + member + '_' + str(chunk) + '_SIM'))
                self.job_list.get_job_list().append(job)

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

            'expid_d1_1_ASIM' : ['d1_m1_1', 'd1_m2_1'], 'expid_d1_2_ASIM' : ['d1_m1_2', 'd1_m2_2'],
            'expid_d2_1_ASIM': ['d2_m1_1', 'd2_m2_1'], 'expid_d2_2_ASIM' : ['d2_m1_2', 'd2_m2_2']
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
        graph = monitor.create_tree_list(self.experiment_id, self.job_list.get_job_list(), None, groups_dict)

        self.assertTrue(graph.obj_dict['strict'])

        subgraphs = graph.obj_dict['subgraphs']
        experiment_subgraph = subgraphs['Experiment'][0]

        self.assertListEqual(sorted(list(experiment_subgraph['nodes'].keys())), sorted(nodes))
        self.assertListEqual(sorted(list(experiment_subgraph['edges'].keys())), sorted(edges))

        subgraph_synchronize_d1_1 = graph.obj_dict['subgraphs']['cluster_d1_m1_1_d1_m2_1'][0]
        self.assertListEqual(sorted(list(subgraph_synchronize_d1_1['nodes'].keys())), sorted(['d1_m1_1', 'd1_m2_1']))
        self.assertListEqual(sorted(list(subgraph_synchronize_d1_1['edges'].keys())), sorted([('d1_m1_1', 'd1_m2_1')]))

        subgraph_synchronize_d1_2 = graph.obj_dict['subgraphs']['cluster_d1_m1_2_d1_m2_2'][0]
        self.assertListEqual(sorted(list(subgraph_synchronize_d1_2['nodes'].keys())), sorted(['d1_m1_2', 'd1_m2_2']))
        self.assertListEqual(sorted(list(subgraph_synchronize_d1_2['edges'].keys())), sorted([('d1_m1_2', 'd1_m2_2')]))

        subgraph_synchronize_d2_1 = graph.obj_dict['subgraphs']['cluster_d2_m1_1_d2_m2_1'][0]
        self.assertListEqual(sorted(list(subgraph_synchronize_d2_1['nodes'].keys())), sorted(['d2_m1_1', 'd2_m2_1']))
        self.assertListEqual(sorted(list(subgraph_synchronize_d2_1['edges'].keys())), sorted([('d2_m1_1', 'd2_m2_1')]))

        subgraph_synchronize_d2_2 = graph.obj_dict['subgraphs']['cluster_d2_m1_2_d2_m2_2'][0]
        self.assertListEqual(sorted(list(subgraph_synchronize_d2_2['nodes'].keys())), sorted(['d2_m1_2', 'd2_m2_2']))
        self.assertListEqual(sorted(list(subgraph_synchronize_d2_2['edges'].keys())), sorted([('d2_m1_2', 'd2_m2_2')]))

    def test_synchronize_member_group_date(self):
        for date in ['d1', 'd2']:
            for chunk in [1, 2]:
                job = self._createDummyJob('expid_' + date + '_' + str(chunk) + '_ASIM',
                                           Status.WAITING, date, None, chunk)
                for member in ['m1', 'm2']:
                    job.add_parent(
                        self.job_list.get_job_by_name('expid_' + date + '_' + member + '_' + str(chunk) + '_SIM'))

                self.job_list.get_job_list().append(job)

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
        graph = monitor.create_tree_list(self.experiment_id, self.job_list.get_job_list(), None, groups_dict)

        self.assertTrue(graph.obj_dict['strict'])

        subgraphs = graph.obj_dict['subgraphs']
        experiment_subgraph = subgraphs['Experiment'][0]

        self.assertListEqual(sorted(list(experiment_subgraph['nodes'].keys())), sorted(nodes))
        self.assertListEqual(sorted(list(experiment_subgraph['edges'].keys())), sorted(edges))

        for subgraph in list(subgraphs.keys()):
            self.assertFalse(subgraph.startswith('cluster'))

    def test_synchronize_date_group_member(self):
        for chunk in [1, 2]:
            job = self._createDummyJob('expid_' + str(chunk) + '_ASIM',
                                           Status.WAITING, None, None, chunk)
            for date in ['d1', 'd2']:
                for member in ['m1', 'm2']:
                    job.add_parent(
                        self.job_list.get_job_by_name('expid_' + date + '_' + member + '_' + str(chunk) + '_SIM'))

            self.job_list.get_job_list().append(job)

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

            'expid_1_ASIM' : ['d1_m1', 'd1_m2', 'd2_m1', 'd2_m2'], 'expid_2_ASIM' : ['d1_m1', 'd1_m2', 'd2_m1', 'd2_m2']
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
        graph = monitor.create_tree_list(self.experiment_id, self.job_list.get_job_list(), None, groups_dict)

        self.assertTrue(graph.obj_dict['strict'])

        subgraphs = graph.obj_dict['subgraphs']
        experiment_subgraph = subgraphs['Experiment'][0]

        self.assertListEqual(sorted(list(experiment_subgraph['nodes'].keys())), sorted(nodes))
        self.assertListEqual(sorted(list(experiment_subgraph['edges'].keys())), sorted(edges))

        subgraph_synchronize_d1_d2 = graph.obj_dict['subgraphs']['cluster_d1_m1_d1_m2_d2_m1_d2_m2'][0]
        self.assertListEqual(sorted(list(subgraph_synchronize_d1_d2['nodes'].keys())), sorted(['d1_m1', 'd1_m2', 'd2_m1', 'd2_m2']))
        self.assertListEqual(sorted(list(subgraph_synchronize_d1_d2['edges'].keys())), sorted([('d1_m1', 'd1_m2'), ('d1_m2', 'd2_m1'), ('d2_m1', 'd2_m2')]))

    def test_synchronize_date_group_chunk(self):
        for chunk in [1, 2]:
            job = self._createDummyJob('expid_' + str(chunk) + '_ASIM',
                                           Status.WAITING, None, None, chunk)
            for date in ['d1', 'd2']:
                for member in ['m1', 'm2']:
                    job.add_parent(
                        self.job_list.get_job_by_name('expid_' + date + '_' + member + '_' + str(chunk) + '_SIM'))

            self.job_list.get_job_list().append(job)

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

            'expid_1_ASIM' : ['d1_m1_1', 'd1_m2_1', 'd2_m1_1', 'd2_m2_1'], 'expid_2_ASIM' : ['d1_m1_2', 'd1_m2_2', 'd2_m1_2', 'd2_m2_2'],
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
        graph = monitor.create_tree_list(self.experiment_id, self.job_list.get_job_list(), None, groups_dict)

        self.assertTrue(graph.obj_dict['strict'])

        subgraphs = graph.obj_dict['subgraphs']
        experiment_subgraph = subgraphs['Experiment'][0]

        #self.assertListEqual(sorted(list(experiment_subgraph['nodes'].keys())), sorted(nodes))
        #self.assertListEqual(sorted(list(experiment_subgraph['edges'].keys())), sorted(edges))

        subgraph_synchronize_1 = graph.obj_dict['subgraphs']['cluster_d1_m1_1_d1_m2_1_d2_m1_1_d2_m2_1'][0]
        self.assertListEqual(sorted(list(subgraph_synchronize_1['nodes'].keys())), sorted(['d1_m1_1', 'd1_m2_1', 'd2_m1_1', 'd2_m2_1']))
        self.assertListEqual(sorted(list(subgraph_synchronize_1['edges'].keys())), sorted([('d1_m1_1', 'd1_m2_1'), ('d1_m2_1', 'd2_m1_1'), ('d2_m1_1', 'd2_m2_1')]))

        subgraph_synchronize_2 = graph.obj_dict['subgraphs']['cluster_d1_m1_2_d1_m2_2_d2_m1_2_d2_m2_2'][0]
        self.assertListEqual(sorted(list(subgraph_synchronize_2['nodes'].keys())), sorted(['d1_m1_2', 'd1_m2_2', 'd2_m1_2', 'd2_m2_2']))
        self.assertListEqual(sorted(list(subgraph_synchronize_2['edges'].keys())), sorted([('d1_m1_2', 'd1_m2_2'), ('d1_m2_2', 'd2_m1_2'), ('d2_m1_2', 'd2_m2_2')]))

    def test_synchronize_date_group_date(self):
        for chunk in [1, 2]:
            job = self._createDummyJob('expid_' + str(chunk) + '_ASIM',
                                           Status.WAITING, None, None, chunk)
            for date in ['d1', 'd2']:
                for member in ['m1', 'm2']:
                    job.add_parent(
                        self.job_list.get_job_by_name('expid_' + date + '_' + member + '_' + str(chunk) + '_SIM'))

            self.job_list.get_job_list().append(job)

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
        graph = monitor.create_tree_list(self.experiment_id, self.job_list.get_job_list(), None, groups_dict)

        self.assertTrue(graph.obj_dict['strict'])

        subgraphs = graph.obj_dict['subgraphs']
        experiment_subgraph = subgraphs['Experiment'][0]

        self.assertListEqual(sorted(list(experiment_subgraph['nodes'].keys())), sorted(nodes))
        self.assertListEqual(sorted(list(experiment_subgraph['edges'].keys())), sorted(edges))

        subgraph_synchronize_d1_d2 = graph.obj_dict['subgraphs']['cluster_d1_d2'][0]
        self.assertListEqual(sorted(list(subgraph_synchronize_d1_d2['nodes'].keys())),
                             sorted(['d1', 'd2']))

    def test_normal_workflow(self):
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
        graph = monitor.create_tree_list(self.experiment_id, self.job_list.get_job_list(), None, dict())

        self.assertFalse(graph.obj_dict['strict'])

        subgraph = graph.obj_dict['subgraphs']['Experiment'][0]

        self.assertListEqual(sorted(list(subgraph['nodes'].keys())), sorted(nodes))
        self.assertListEqual(sorted(list(subgraph['edges'].keys())), sorted(edges))

    def test_wrapper_and_groups(self):
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

        nodes = [
            "expid_SETUP", "expid_d1_m1_INI", "expid_d1_m2_INI", "expid_d2_m1_INI", "expid_d2_m2_INI",
            'd1_m1_1', 'd1_m1_2', 'd1_m2_1', 'd2_m2_2',

            'expid_d1_m2_2_SIM', 'expid_d1_m2_2_POST', 'expid_d1_m2_2_CLEAN',
            'expid_d2_m1_1_SIM', 'expid_d2_m1_1_POST', 'expid_d2_m1_1_CLEAN',
            'expid_d2_m1_2_SIM', 'expid_d2_m1_2_POST', 'expid_d2_m1_2_CLEAN',
            'expid_d2_m2_1_SIM', 'expid_d2_m2_1_POST', 'expid_d2_m2_1_CLEAN'
        ]
        edges = [
            ("expid_SETUP", "expid_d1_m1_INI"), ("expid_SETUP", "expid_d1_m2_INI"), ("expid_SETUP", "expid_d2_m1_INI"),
            ("expid_SETUP", "expid_d2_m2_INI"),

            ("expid_d1_m1_INI", "d1_m1_1"), ("expid_d1_m2_INI", "d1_m2_1"), ("expid_d2_m1_INI", "expid_d2_m1_1_SIM"),
            ("expid_d2_m2_INI", "expid_d2_m2_1_SIM"),

            ("d1_m1_1", "d1_m1_2"), ("d1_m1_1", "d1_m1_1"), ("d1_m1_2", "d1_m1_2"), ("d1_m2_1", "d1_m2_1"), ("d2_m2_2", "d2_m2_2"),

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

        packages = [('expid', 'package_d1_m1_SIM', 'expid_d1_m1_1_SIM', "02:00"),
                    ('expid', 'package_d1_m1_SIM', 'expid_d1_m1_2_SIM', "02:00"),
                    ('expid', 'package_d2_m2_SIM', 'expid_d2_m2_1_SIM', "02:00"),
                    ('expid', 'package_d2_m2_SIM', 'expid_d2_m2_2_SIM', "02:00")]

        monitor = Monitor()
        graph = monitor.create_tree_list(self.experiment_id, self.job_list.get_job_list(), packages, groups_dict)
        self.assertTrue(graph.obj_dict['strict'])

        for (expid, package, job_name, _) in packages:
            if package != 'package_d2_m2_SIM':
                self.assertNotIn('cluster_' + package, graph.obj_dict['subgraphs'])
            else:
                self.assertIn('cluster_' + package, graph.obj_dict['subgraphs'])

        subgraph = graph.obj_dict['subgraphs']['Experiment'][0]

        self.assertListEqual(sorted(list(subgraph['nodes'].keys())), sorted(nodes))
        self.assertListEqual(sorted(list(subgraph['edges'].keys())), sorted(edges))

    def _createDummyJob(self, name, status, date=None, member=None, chunk=None, split=None):
        job_id = randrange(1, 999)
        job = Job(name, job_id, status, 0)
        job.type = randrange(0, 2)

        job.date = date
        job.member = member
        job.chunk = chunk
        job.split = split

        return job
class FakeBasicConfig:
    def __init__(self):
        pass

    DB_DIR = '/dummy/db/dir'
    DB_FILE = '/dummy/db/file'
    DB_PATH = '/dummy/db/path'
    LOCAL_ROOT_DIR = '/dummy/local/root/dir'
    LOCAL_TMP_DIR = '/dummy/local/temp/dir'
    LOCAL_PROJ_DIR = '/dummy/local/proj/dir'
    DEFAULT_PLATFORMS_CONF = ''
    DEFAULT_JOBS_CONF = ''
