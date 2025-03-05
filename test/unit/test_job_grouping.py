import shutil
import tempfile
import inspect

from unittest import TestCase
from mock import Mock
from autosubmit.job.job_list import JobList
from bscearth.utils.date import parse_date, date2str
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory
from autosubmit.job.job_list_persistence import JobListPersistenceDb
from autosubmit.job.job_common import Status
from random import randrange
from autosubmit.job.job import Job
from mock import patch

from autosubmit.job.job_grouping import JobGrouping

class TestJobGrouping(TestCase):

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
        self._createDummyJob('expid_SETUP', Status.READY)

        for date in ['19000101', '19000202']:
            for member in ['m1', 'm2']:
                job = self._createDummyJob('expid_' + date + '_' + member + '_' + 'INI', Status.WAITING, date, member)
                self.job_list.get_job_list().append(job)

        sections = ['SIM', 'POST', 'CLEAN']
        for section in sections:
            for date in ['19000101', '19000202']:
                for member in ['m1', 'm2']:
                    for chunk in [1, 2]:
                        job = self._createDummyJob('expid_' + date + '_' + member + '_' + str(chunk) + '_' + section, Status.WAITING, date, member, chunk)
                        self.job_list.get_job_list().append(job)

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_directory)

    def test_group_by_date(self):
        groups_dict = dict()

        groups_dict['status'] = {'19000101' : Status.WAITING, '19000202' : Status.WAITING}
        groups_dict['jobs'] = {
                                'expid_19000101_m1_INI' : ['19000101'], 'expid_19000101_m2_INI' : ['19000101'], 'expid_19000202_m1_INI' : ['19000202'], 'expid_19000202_m2_INI' : ['19000202'],

                               'expid_19000101_m1_1_SIM': ['19000101'], 'expid_19000101_m1_2_SIM': ['19000101'], 'expid_19000101_m2_1_SIM': ['19000101'], 'expid_19000101_m2_2_SIM': ['19000101'],
                               'expid_19000202_m1_1_SIM': ['19000202'], 'expid_19000202_m1_2_SIM': ['19000202'], 'expid_19000202_m2_1_SIM': ['19000202'], 'expid_19000202_m2_2_SIM': ['19000202'],

                               'expid_19000101_m1_1_POST': ['19000101'], 'expid_19000101_m1_2_POST': ['19000101'], 'expid_19000101_m2_1_POST': ['19000101'], 'expid_19000101_m2_2_POST': ['19000101'],
                               'expid_19000202_m1_1_POST': ['19000202'], 'expid_19000202_m1_2_POST': ['19000202'], 'expid_19000202_m2_1_POST': ['19000202'], 'expid_19000202_m2_2_POST': ['19000202'],

                               'expid_19000101_m1_1_CLEAN': ['19000101'], 'expid_19000101_m1_2_CLEAN': ['19000101'], 'expid_19000101_m2_1_CLEAN': ['19000101'],  'expid_19000101_m2_2_CLEAN': ['19000101'],
                               'expid_19000202_m1_1_CLEAN': ['19000202'], 'expid_19000202_m1_2_CLEAN': ['19000202'], 'expid_19000202_m2_1_CLEAN': ['19000202'], 'expid_19000202_m2_2_CLEAN': ['19000202']
                               }
        
        self.job_list.get_date_list = Mock(return_value=['19000101', '19000202'])
        self.job_list.get_member_list = Mock(return_value=['m1', 'm2'])
        self.job_list.get_chunk_list = Mock(return_value=[1, 2])
        self.job_list.get_date_format = Mock(return_value='')

        side_effect = []
        for job in reversed(self.job_list.get_job_list()):
            if job.date is not None:
                side_effect.append(date2str(job.date, ''))

        with patch('autosubmit.job.job_grouping.date2str', side_effect=side_effect):
            job_grouping = JobGrouping('date', self.job_list.get_job_list(), self.job_list)
            self.assertDictEqual(job_grouping.group_jobs(), groups_dict)

    def test_group_by_member(self):
        groups_dict = dict()

        groups_dict['status'] = {'19000101_m1': Status.WAITING, '19000101_m2': Status.WAITING, '19000202_m1': Status.WAITING, '19000202_m2' : Status.WAITING}
        groups_dict['jobs'] = {
            'expid_19000101_m1_INI': ['19000101_m1'], 'expid_19000101_m2_INI': ['19000101_m2'], 'expid_19000202_m1_INI': ['19000202_m1'], 'expid_19000202_m2_INI': ['19000202_m2'],

            'expid_19000101_m1_1_SIM': ['19000101_m1'], 'expid_19000101_m1_2_SIM': ['19000101_m1'], 'expid_19000101_m2_1_SIM': ['19000101_m2'],
            'expid_19000101_m2_2_SIM': ['19000101_m2'],
            'expid_19000202_m1_1_SIM': ['19000202_m1'], 'expid_19000202_m1_2_SIM': ['19000202_m1'], 'expid_19000202_m2_1_SIM': ['19000202_m2'],
            'expid_19000202_m2_2_SIM': ['19000202_m2'],

            'expid_19000101_m1_1_POST': ['19000101_m1'], 'expid_19000101_m1_2_POST': ['19000101_m1'], 'expid_19000101_m2_1_POST': ['19000101_m2'],
            'expid_19000101_m2_2_POST': ['19000101_m2'],
            'expid_19000202_m1_1_POST': ['19000202_m1'], 'expid_19000202_m1_2_POST': ['19000202_m1'], 'expid_19000202_m2_1_POST': ['19000202_m2'],
            'expid_19000202_m2_2_POST': ['19000202_m2'],

            'expid_19000101_m1_1_CLEAN': ['19000101_m1'], 'expid_19000101_m1_2_CLEAN': ['19000101_m1'], 'expid_19000101_m2_1_CLEAN': ['19000101_m2'],
            'expid_19000101_m2_2_CLEAN': ['19000101_m2'],
            'expid_19000202_m1_1_CLEAN': ['19000202_m1'], 'expid_19000202_m1_2_CLEAN': ['19000202_m1'], 'expid_19000202_m2_1_CLEAN': ['19000202_m2'],
            'expid_19000202_m2_2_CLEAN': ['19000202_m2']
        }

        self.job_list.get_date_list = Mock(return_value=['19000101', '19000202'])
        self.job_list.get_member_list = Mock(return_value=['m1', 'm2'])
        self.job_list.get_chunk_list = Mock(return_value=[1, 2])
        self.job_list.get_date_format = Mock(return_value='')

        side_effect = []
        for job in reversed(self.job_list.get_job_list()):
            if job.member is not None:
                side_effect.append(date2str(job.date, ''))

        with patch('autosubmit.job.job_grouping.date2str', side_effect=side_effect):
            job_grouping = JobGrouping('member', self.job_list.get_job_list(), self.job_list)
            self.assertDictEqual(job_grouping.group_jobs(), groups_dict)

    def test_group_by_chunk(self):
        groups_dict = dict()

        groups_dict['status'] = {'19000101_m1_1': Status.WAITING, '19000101_m1_2': Status.WAITING,
                                 '19000101_m2_1': Status.WAITING, '19000101_m2_2': Status.WAITING,
                                 '19000202_m1_1': Status.WAITING, '19000202_m1_2': Status.WAITING,
                                 '19000202_m2_1': Status.WAITING, '19000202_m2_2': Status.WAITING}
        groups_dict['jobs'] = {
            'expid_19000101_m1_1_SIM': ['19000101_m1_1'], 'expid_19000101_m1_2_SIM': ['19000101_m1_2'], 'expid_19000101_m2_1_SIM': ['19000101_m2_1'],
            'expid_19000101_m2_2_SIM': ['19000101_m2_2'],
            'expid_19000202_m1_1_SIM': ['19000202_m1_1'], 'expid_19000202_m1_2_SIM': ['19000202_m1_2'], 'expid_19000202_m2_1_SIM': ['19000202_m2_1'],
            'expid_19000202_m2_2_SIM': ['19000202_m2_2'],

            'expid_19000101_m1_1_POST': ['19000101_m1_1'], 'expid_19000101_m1_2_POST': ['19000101_m1_2'], 'expid_19000101_m2_1_POST': ['19000101_m2_1'],
            'expid_19000101_m2_2_POST': ['19000101_m2_2'],
            'expid_19000202_m1_1_POST': ['19000202_m1_1'], 'expid_19000202_m1_2_POST': ['19000202_m1_2'], 'expid_19000202_m2_1_POST': ['19000202_m2_1'],
            'expid_19000202_m2_2_POST': ['19000202_m2_2'],

            'expid_19000101_m1_1_CLEAN': ['19000101_m1_1'], 'expid_19000101_m1_2_CLEAN': ['19000101_m1_2'], 'expid_19000101_m2_1_CLEAN': ['19000101_m2_1'],
            'expid_19000101_m2_2_CLEAN': ['19000101_m2_2'],
            'expid_19000202_m1_1_CLEAN': ['19000202_m1_1'], 'expid_19000202_m1_2_CLEAN': ['19000202_m1_2'], 'expid_19000202_m2_1_CLEAN': ['19000202_m2_1'],
            'expid_19000202_m2_2_CLEAN': ['19000202_m2_2']
        }

        self.job_list.get_date_list = Mock(return_value=['19000101', '19000202'])
        self.job_list.get_member_list = Mock(return_value=['m1', 'm2'])
        self.job_list.get_chunk_list = Mock(return_value=[1, 2])
        self.job_list.get_date_format = Mock(return_value='')

        side_effect = []
        for job in reversed(self.job_list.get_job_list()):
            if job.chunk is not None:
                side_effect.append(date2str(job.date, ''))

        with patch('autosubmit.job.job_grouping.date2str', side_effect=side_effect):
            job_grouping = JobGrouping('chunk', self.job_list.get_job_list(), self.job_list)
            self.assertDictEqual(job_grouping.group_jobs(), groups_dict)

    def test_group_by_split(self):
        for date in ['19000101', '19000202']:
            for member in ['m1', 'm2']:
                for chunk in [1, 2]:
                    for split in [1, 2]:
                        job = self._createDummyJob('expid_' + date + '_' + member + '_' + str(chunk) + '_' + str(split) + '_CMORATM',
                                                   Status.WAITING, date, member, chunk, split)
                        self.job_list.get_job_list().append(job)

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
        
        groups_dict['jobs'] =  {
            'expid_19000101_m1_1_1_CMORATM' : ['expid_19000101_m1_1_CMORATM'],
            'expid_19000101_m1_1_2_CMORATM' : ['expid_19000101_m1_1_CMORATM'],
            'expid_19000101_m1_2_1_CMORATM' : ['expid_19000101_m1_2_CMORATM'],
            'expid_19000101_m1_2_2_CMORATM' : ['expid_19000101_m1_2_CMORATM'],
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

        self.job_list.get_date_list = Mock(return_value=['19000101', '19000202'])
        self.job_list.get_member_list = Mock(return_value=['m1', 'm2'])
        self.job_list.get_chunk_list = Mock(return_value=[1, 2])
        self.job_list.get_date_format = Mock(return_value='')

        job_grouping = JobGrouping('split', self.job_list.get_job_list(), self.job_list)
        self.assertDictEqual(job_grouping.group_jobs(), groups_dict)

    def test_automatic_grouping_all(self):
        groups_dict = dict()

        groups_dict['status'] = {'19000101': Status.WAITING, '19000202': Status.WAITING}
        groups_dict['jobs'] = {
            'expid_19000101_m1_INI': ['19000101'], 'expid_19000101_m2_INI': ['19000101'], 'expid_19000202_m1_INI': ['19000202'], 'expid_19000202_m2_INI': ['19000202'],

            'expid_19000101_m1_1_SIM': ['19000101'], 'expid_19000101_m1_2_SIM': ['19000101'], 'expid_19000101_m2_1_SIM': ['19000101'],
            'expid_19000101_m2_2_SIM': ['19000101'],
            'expid_19000202_m1_1_SIM': ['19000202'], 'expid_19000202_m1_2_SIM': ['19000202'], 'expid_19000202_m2_1_SIM': ['19000202'],
            'expid_19000202_m2_2_SIM': ['19000202'],

            'expid_19000101_m1_1_POST': ['19000101'], 'expid_19000101_m1_2_POST': ['19000101'], 'expid_19000101_m2_1_POST': ['19000101'],
            'expid_19000101_m2_2_POST': ['19000101'],
            'expid_19000202_m1_1_POST': ['19000202'], 'expid_19000202_m1_2_POST': ['19000202'], 'expid_19000202_m2_1_POST': ['19000202'],
            'expid_19000202_m2_2_POST': ['19000202'],

            'expid_19000101_m1_1_CLEAN': ['19000101'], 'expid_19000101_m1_2_CLEAN': ['19000101'], 'expid_19000101_m2_1_CLEAN': ['19000101'],
            'expid_19000101_m2_2_CLEAN': ['19000101'],
            'expid_19000202_m1_1_CLEAN': ['19000202'], 'expid_19000202_m1_2_CLEAN': ['19000202'], 'expid_19000202_m2_1_CLEAN': ['19000202'],
            'expid_19000202_m2_2_CLEAN': ['19000202']
        }

        self.job_list.get_date_list = Mock(return_value=['19000101', '19000202'])
        self.job_list.get_member_list = Mock(return_value=['m1', 'm2'])
        self.job_list.get_chunk_list = Mock(return_value=[1, 2])
        self.job_list.get_date_format = Mock(return_value='')

        '''side_effect = []
        for job in reversed(self.job_list.get_job_list()):
            if job.date is not None:
                side_effect.append(date2str(job.date, ''))

        with patch('autosubmit.job.job_grouping.date2str', side_effect=side_effect):'''
        job_grouping = JobGrouping('automatic', self.job_list.get_job_list(), self.job_list)
        grouped = job_grouping.group_jobs()
        self.assertDictEqual(grouped["status"], groups_dict["status"])
        self.assertDictEqual(grouped["jobs"], groups_dict["jobs"])

    def test_automatic_grouping_not_ini(self):
        self.job_list.get_job_by_name('expid_19000101_m1_INI').status = Status.READY
        self.job_list.get_job_by_name('expid_19000101_m2_INI').status = Status.READY
        self.job_list.get_job_by_name('expid_19000202_m1_INI').status = Status.READY
        self.job_list.get_job_by_name('expid_19000202_m2_INI').status = Status.READY

        groups_dict = dict()

        groups_dict['status'] = {'19000101': Status.WAITING, '19000202': Status.WAITING}
        groups_dict['jobs'] = {
            'expid_19000101_m1_1_SIM': ['19000101'], 'expid_19000101_m1_2_SIM': ['19000101'], 'expid_19000101_m2_1_SIM': ['19000101'],
            'expid_19000101_m2_2_SIM': ['19000101'],
            'expid_19000202_m1_1_SIM': ['19000202'], 'expid_19000202_m1_2_SIM': ['19000202'], 'expid_19000202_m2_1_SIM': ['19000202'],
            'expid_19000202_m2_2_SIM': ['19000202'],

            'expid_19000101_m1_1_POST': ['19000101'], 'expid_19000101_m1_2_POST': ['19000101'], 'expid_19000101_m2_1_POST': ['19000101'],
            'expid_19000101_m2_2_POST': ['19000101'],
            'expid_19000202_m1_1_POST': ['19000202'], 'expid_19000202_m1_2_POST': ['19000202'], 'expid_19000202_m2_1_POST': ['19000202'],
            'expid_19000202_m2_2_POST': ['19000202'],

            'expid_19000101_m1_1_CLEAN': ['19000101'], 'expid_19000101_m1_2_CLEAN': ['19000101'], 'expid_19000101_m2_1_CLEAN': ['19000101'],
            'expid_19000101_m2_2_CLEAN': ['19000101'],
            'expid_19000202_m1_1_CLEAN': ['19000202'], 'expid_19000202_m1_2_CLEAN': ['19000202'], 'expid_19000202_m2_1_CLEAN': ['19000202'],
            'expid_19000202_m2_2_CLEAN': ['19000202']
        }

        self.job_list.get_date_list = Mock(return_value=['19000101', '19000202'])
        self.job_list.get_member_list = Mock(return_value=['m1', 'm2'])
        self.job_list.get_chunk_list = Mock(return_value=[1, 2])
        self.job_list.get_date_format = Mock(return_value='')

        side_effect = []
        for job in reversed(self.job_list.get_job_list()):
            if job.date is not None:
                side_effect.append(date2str(job.date, ''))

        with patch('autosubmit.job.job_grouping.date2str', side_effect=side_effect):
            job_grouping = JobGrouping('automatic', self.job_list.get_job_list(), self.job_list)
            self.assertDictEqual(job_grouping.group_jobs(), groups_dict)

    def test_automatic_grouping_splits(self,):
        for date in ['19000101', '19000202']:
            for member in ['m1', 'm2']:
                for chunk in [1, 2]:
                    for split in [1, 2]:
                        job = self._createDummyJob(
                            'expid_' + date + '_' + member + '_' + str(chunk) + '_' + str(split) + '_CMORATM',
                            Status.WAITING, date, member, chunk, split)
                        self.job_list.get_job_list().append(job)

        groups_dict = dict()

        groups_dict['status'] = {'19000101': Status.WAITING, '19000202': Status.WAITING}
        groups_dict['jobs'] = {
            'expid_19000101_m1_INI': ['19000101'], 'expid_19000101_m2_INI': ['19000101'], 'expid_19000202_m1_INI': ['19000202'], 'expid_19000202_m2_INI': ['19000202'],

            'expid_19000101_m1_1_SIM': ['19000101'], 'expid_19000101_m1_2_SIM': ['19000101'], 'expid_19000101_m2_1_SIM': ['19000101'],
            'expid_19000101_m2_2_SIM': ['19000101'],
            'expid_19000202_m1_1_SIM': ['19000202'], 'expid_19000202_m1_2_SIM': ['19000202'], 'expid_19000202_m2_1_SIM': ['19000202'],
            'expid_19000202_m2_2_SIM': ['19000202'],

            'expid_19000101_m1_1_POST': ['19000101'], 'expid_19000101_m1_2_POST': ['19000101'], 'expid_19000101_m2_1_POST': ['19000101'],
            'expid_19000101_m2_2_POST': ['19000101'],
            'expid_19000202_m1_1_POST': ['19000202'], 'expid_19000202_m1_2_POST': ['19000202'], 'expid_19000202_m2_1_POST': ['19000202'],
            'expid_19000202_m2_2_POST': ['19000202'],

            'expid_19000101_m1_1_CLEAN': ['19000101'], 'expid_19000101_m1_2_CLEAN': ['19000101'], 'expid_19000101_m2_1_CLEAN': ['19000101'],
            'expid_19000101_m2_2_CLEAN': ['19000101'],
            'expid_19000202_m1_1_CLEAN': ['19000202'], 'expid_19000202_m1_2_CLEAN': ['19000202'], 'expid_19000202_m2_1_CLEAN': ['19000202'],
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

        self.job_list.get_date_list = Mock(return_value=['19000101', '19000202'])
        self.job_list.get_member_list = Mock(return_value=['m1', 'm2'])
        self.job_list.get_chunk_list = Mock(return_value=[1, 2])
        self.job_list.get_date_format = Mock(return_value='')

        side_effect = []
        for job in reversed(self.job_list.get_job_list()):
            if job.date is not None:
                side_effect.append(date2str(job.date, ''))

        with patch('autosubmit.job.job_grouping.date2str', side_effect=side_effect):
            job_grouping = JobGrouping('automatic', self.job_list.get_job_list(), self.job_list)
            self.assertDictEqual(job_grouping.group_jobs(), groups_dict)

    def test_automatic_grouping_different_status_member(self):
        self.job_list.get_job_by_name('expid_19000101_m1_INI').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m2_INI').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000202_m1_INI').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000202_m2_INI').status = Status.COMPLETED

        self.job_list.get_job_by_name('expid_19000101_m1_1_SIM').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m1_2_SIM').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m1_1_POST').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m1_2_POST').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m1_1_CLEAN').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m1_2_CLEAN').status = Status.COMPLETED

        self.job_list.get_job_by_name('expid_19000101_m2_1_SIM').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m2_1_POST').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m2_1_CLEAN').status = Status.RUNNING

        self.job_list.get_job_by_name('expid_19000101_m2_2_SIM').status = Status.READY

        groups_dict = dict()

        groups_dict['status'] = {'19000101_m1': Status.COMPLETED, '19000202': Status.WAITING}
        groups_dict['jobs'] = {
            'expid_19000101_m1_INI' : ['19000101_m1'],

            'expid_19000101_m1_1_SIM': ['19000101_m1'], 'expid_19000101_m1_2_SIM': ['19000101_m1'],
            'expid_19000202_m1_1_SIM': ['19000202'], 'expid_19000202_m1_2_SIM': ['19000202'], 'expid_19000202_m2_1_SIM': ['19000202'],
            'expid_19000202_m2_2_SIM': ['19000202'],

            'expid_19000101_m1_1_POST': ['19000101_m1'], 'expid_19000101_m1_2_POST': ['19000101_m1'],
            'expid_19000202_m1_1_POST': ['19000202'], 'expid_19000202_m1_2_POST': ['19000202'], 'expid_19000202_m2_1_POST': ['19000202'],
            'expid_19000202_m2_2_POST': ['19000202'],

            'expid_19000101_m1_1_CLEAN': ['19000101_m1'], 'expid_19000101_m1_2_CLEAN': ['19000101_m1'],
            'expid_19000202_m1_1_CLEAN': ['19000202'], 'expid_19000202_m1_2_CLEAN': ['19000202'], 'expid_19000202_m2_1_CLEAN': ['19000202'],
            'expid_19000202_m2_2_CLEAN': ['19000202']
        }

        self.job_list.get_date_list = Mock(return_value=['19000101', '19000202'])
        self.job_list.get_member_list = Mock(return_value=['m1', 'm2'])
        self.job_list.get_chunk_list = Mock(return_value=[1, 2])
        self.job_list.get_date_format = Mock(return_value='')

        side_effect = []
        for job in reversed(self.job_list.get_job_list()):
            if job.date is not None:
                side_effect.append(date2str(job.date, ''))

        with patch('autosubmit.job.job_grouping.date2str', side_effect=side_effect):
            job_grouping = JobGrouping('automatic', self.job_list.get_job_list(), self.job_list)
            self.assertDictEqual(job_grouping.group_jobs(), groups_dict)

    def test_automatic_grouping_different_status_chunk(self):
        self.job_list.get_job_by_name('expid_19000101_m1_INI').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m2_INI').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000202_m1_INI').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000202_m2_INI').status = Status.COMPLETED

        self.job_list.get_job_by_name('expid_19000101_m1_1_SIM').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m1_1_POST').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m1_1_CLEAN').status = Status.COMPLETED

        self.job_list.get_job_by_name('expid_19000101_m1_2_SIM').status = Status.READY

        self.job_list.get_job_by_name('expid_19000101_m2_1_SIM').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m2_1_POST').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m2_1_CLEAN').status = Status.RUNNING

        self.job_list.get_job_by_name('expid_19000101_m2_2_SIM').status = Status.READY

        groups_dict = dict()

        groups_dict['status'] = {'19000101_m1_1': Status.COMPLETED, '19000202': Status.WAITING}
        groups_dict['jobs'] = {
            'expid_19000101_m1_1_SIM': ['19000101_m1_1'],
            'expid_19000202_m1_1_SIM': ['19000202'], 'expid_19000202_m1_2_SIM': ['19000202'], 'expid_19000202_m2_1_SIM': ['19000202'],
            'expid_19000202_m2_2_SIM': ['19000202'],

            'expid_19000101_m1_1_POST': ['19000101_m1_1'],
            'expid_19000202_m1_1_POST': ['19000202'], 'expid_19000202_m1_2_POST': ['19000202'], 'expid_19000202_m2_1_POST': ['19000202'],
            'expid_19000202_m2_2_POST': ['19000202'],

            'expid_19000101_m1_1_CLEAN': ['19000101_m1_1'],
            'expid_19000202_m1_1_CLEAN': ['19000202'], 'expid_19000202_m1_2_CLEAN': ['19000202'], 'expid_19000202_m2_1_CLEAN': ['19000202'],
            'expid_19000202_m2_2_CLEAN': ['19000202']
        }

        self.job_list.get_date_list = Mock(return_value=['19000101', '19000202'])
        self.job_list.get_member_list = Mock(return_value=['m1', 'm2'])
        self.job_list.get_chunk_list = Mock(return_value=[1, 2])
        self.job_list.get_date_format = Mock(return_value='')

        side_effect = []
        for job in reversed(self.job_list.get_job_list()):
            if job.date is not None:
                side_effect.append(date2str(job.date, ''))

        with patch('autosubmit.job.job_grouping.date2str', side_effect=side_effect):
            job_grouping = JobGrouping('automatic', self.job_list.get_job_list(), self.job_list)
            self.assertDictEqual(job_grouping.group_jobs(), groups_dict)

    def test_group_by_member_expand_running(self):
        self.job_list.get_job_by_name('expid_19000101_m1_INI').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m2_INI').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000202_m1_INI').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000202_m2_INI').status = Status.COMPLETED

        self.job_list.get_job_by_name('expid_19000101_m1_1_SIM').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m1_1_POST').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m1_1_CLEAN').status = Status.COMPLETED

        self.job_list.get_job_by_name('expid_19000101_m1_2_SIM').status = Status.READY

        self.job_list.get_job_by_name('expid_19000101_m2_1_SIM').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m2_1_POST').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m2_1_CLEAN').status = Status.RUNNING

        self.job_list.get_job_by_name('expid_19000101_m2_2_SIM').status = Status.READY

        groups_dict = dict()

        groups_dict['status'] = {'19000101_m1': Status.READY, '19000202_m1': Status.WAITING,
                                 '19000202_m2': Status.WAITING}
        groups_dict['jobs'] = {
            'expid_19000101_m1_INI': ['19000101_m1'], 'expid_19000202_m1_INI': ['19000202_m1'],
            'expid_19000202_m2_INI': ['19000202_m2'],

            'expid_19000101_m1_1_SIM': ['19000101_m1'], 'expid_19000101_m1_2_SIM': ['19000101_m1'],
            'expid_19000202_m1_1_SIM': ['19000202_m1'], 'expid_19000202_m1_2_SIM': ['19000202_m1'], 'expid_19000202_m2_1_SIM': ['19000202_m2'],
            'expid_19000202_m2_2_SIM': ['19000202_m2'],

            'expid_19000101_m1_1_POST': ['19000101_m1'], 'expid_19000101_m1_2_POST': ['19000101_m1'],
            'expid_19000202_m1_1_POST': ['19000202_m1'], 'expid_19000202_m1_2_POST': ['19000202_m1'], 'expid_19000202_m2_1_POST': ['19000202_m2'],
            'expid_19000202_m2_2_POST': ['19000202_m2'],

            'expid_19000101_m1_1_CLEAN': ['19000101_m1'], 'expid_19000101_m1_2_CLEAN': ['19000101_m1'],
            'expid_19000202_m1_1_CLEAN': ['19000202_m1'], 'expid_19000202_m1_2_CLEAN': ['19000202_m1'], 'expid_19000202_m2_1_CLEAN': ['19000202_m2'],
            'expid_19000202_m2_2_CLEAN': ['19000202_m2']
        }

        self.job_list.get_date_list = Mock(return_value=['19000101', '19000202'])
        self.job_list.get_member_list = Mock(return_value=['m1', 'm2'])
        self.job_list.get_chunk_list = Mock(return_value=[1, 2])
        self.job_list.get_date_format = Mock(return_value='')

        side_effect = []
        for job in reversed(self.job_list.get_job_list()):
            if job.member is not None:
                side_effect.append(date2str(job.date, ''))

        with patch('autosubmit.job.job_grouping.date2str', side_effect=side_effect):
            job_grouping = JobGrouping('member', self.job_list.get_job_list(), self.job_list, expanded_status=[Status.RUNNING])
            self.assertDictEqual(job_grouping.group_jobs(), groups_dict)

    def test_group_by_chunk_expand_failed_running(self):
        self.job_list.get_job_by_name('expid_19000101_m1_INI').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m2_INI').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000202_m1_INI').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000202_m2_INI').status = Status.COMPLETED

        self.job_list.get_job_by_name('expid_19000101_m1_1_SIM').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m1_1_POST').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m1_1_CLEAN').status = Status.FAILED

        self.job_list.get_job_by_name('expid_19000101_m1_2_SIM').status = Status.READY

        self.job_list.get_job_by_name('expid_19000101_m2_1_SIM').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m2_1_POST').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m2_1_CLEAN').status = Status.RUNNING

        self.job_list.get_job_by_name('expid_19000101_m2_2_SIM').status = Status.READY

        groups_dict = dict()

        groups_dict['status'] = {'19000101_m1_2': Status.READY, '19000101_m2_2': Status.READY,
                                 '19000202_m1_1': Status.WAITING, '19000202_m1_2': Status.WAITING,
                                 '19000202_m2_1': Status.WAITING, '19000202_m2_2': Status.WAITING}
        groups_dict['jobs'] = {
            'expid_19000101_m1_2_SIM': ['19000101_m1_2'],
            'expid_19000101_m2_2_SIM': ['19000101_m2_2'],
            'expid_19000202_m1_1_SIM': ['19000202_m1_1'], 'expid_19000202_m1_2_SIM': ['19000202_m1_2'], 'expid_19000202_m2_1_SIM': ['19000202_m2_1'],
            'expid_19000202_m2_2_SIM': ['19000202_m2_2'],

            'expid_19000101_m1_2_POST': ['19000101_m1_2'],
            'expid_19000101_m2_2_POST': ['19000101_m2_2'],
            'expid_19000202_m1_1_POST': ['19000202_m1_1'], 'expid_19000202_m1_2_POST': ['19000202_m1_2'], 'expid_19000202_m2_1_POST': ['19000202_m2_1'],
            'expid_19000202_m2_2_POST': ['19000202_m2_2'],

            'expid_19000101_m1_2_CLEAN': ['19000101_m1_2'],
            'expid_19000101_m2_2_CLEAN': ['19000101_m2_2'],
            'expid_19000202_m1_1_CLEAN': ['19000202_m1_1'], 'expid_19000202_m1_2_CLEAN': ['19000202_m1_2'], 'expid_19000202_m2_1_CLEAN': ['19000202_m2_1'],
            'expid_19000202_m2_2_CLEAN': ['19000202_m2_2']
        }

        self.job_list.get_date_list = Mock(return_value=['19000101', '19000202'])
        self.job_list.get_member_list = Mock(return_value=['m1', 'm2'])
        self.job_list.get_chunk_list = Mock(return_value=[1, 2])
        self.job_list.get_date_format = Mock(return_value='')

        side_effect = []
        for job in reversed(self.job_list.get_job_list()):
            if job.chunk is not None:
                side_effect.append(date2str(job.date, ''))

        with patch('autosubmit.job.job_grouping.date2str', side_effect=side_effect):
            job_grouping = JobGrouping('chunk', self.job_list.get_job_list(), self.job_list, expanded_status=[Status.RUNNING, Status.FAILED])
            self.assertDictEqual(job_grouping.group_jobs(), groups_dict)

    def test_group_by_member_expand(self):
        self.job_list.get_job_by_name('expid_19000101_m1_INI').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m2_INI').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000202_m1_INI').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000202_m2_INI').status = Status.COMPLETED

        self.job_list.get_job_by_name('expid_19000101_m1_1_SIM').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m1_1_POST').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m1_1_CLEAN').status = Status.COMPLETED

        self.job_list.get_job_by_name('expid_19000101_m1_2_SIM').status = Status.READY

        self.job_list.get_job_by_name('expid_19000101_m2_1_SIM').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m2_1_POST').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m2_1_CLEAN').status = Status.RUNNING

        self.job_list.get_job_by_name('expid_19000101_m2_2_SIM').status = Status.READY

        groups_dict = dict()

        groups_dict['status'] = {'19000101_m1': Status.READY}
        groups_dict['jobs'] = {
            'expid_19000101_m1_INI': ['19000101_m1'],

            'expid_19000101_m1_1_SIM': ['19000101_m1'], 'expid_19000101_m1_2_SIM': ['19000101_m1'],

            'expid_19000101_m1_1_POST': ['19000101_m1'], 'expid_19000101_m1_2_POST': ['19000101_m1'],

            'expid_19000101_m1_1_CLEAN': ['19000101_m1'], 'expid_19000101_m1_2_CLEAN': ['19000101_m1'],
        }

        self.job_list.get_date_list = Mock(return_value=['19000101', '19000202'])
        self.job_list.get_member_list = Mock(return_value=['m1', 'm2'])
        self.job_list.get_chunk_list = Mock(return_value=[1, 2])
        self.job_list.get_date_format = Mock(return_value='')

        side_effect = []
        for job in reversed(self.job_list.get_job_list()):
            if job.member is not None:
                side_effect.append(date2str(job.date, ''))

        with patch('autosubmit.job.job_grouping.date2str', side_effect=side_effect):
            job_grouping = JobGrouping('member', self.job_list.get_job_list(), self.job_list,
                                   expand_list="[ 19000101 [m2] 19000202 [m1 m2] ]")
            self.assertDictEqual(job_grouping.group_jobs(), groups_dict)

    def test_group_by_member_expand_and_running(self, *patches):
        self.job_list.get_job_by_name('expid_19000101_m1_INI').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m2_INI').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000202_m1_INI').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000202_m2_INI').status = Status.COMPLETED

        self.job_list.get_job_by_name('expid_19000101_m1_1_SIM').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m1_1_POST').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m1_1_CLEAN').status = Status.COMPLETED

        self.job_list.get_job_by_name('expid_19000101_m1_2_SIM').status = Status.READY

        self.job_list.get_job_by_name('expid_19000101_m2_1_SIM').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m2_1_POST').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m2_1_CLEAN').status = Status.RUNNING

        self.job_list.get_job_by_name('expid_19000101_m2_2_SIM').status = Status.READY

        self.job_list.get_job_by_name('expid_19000202_m1_1_SIM').status = Status.RUNNING

        groups_dict = dict()

        groups_dict['status'] = {'19000101_m1': Status.READY}
        groups_dict['jobs'] = {
            'expid_19000101_m1_INI': ['19000101_m1'],

            'expid_19000101_m1_1_SIM': ['19000101_m1'], 'expid_19000101_m1_2_SIM': ['19000101_m1'],

            'expid_19000101_m1_1_POST': ['19000101_m1'], 'expid_19000101_m1_2_POST': ['19000101_m1'],

            'expid_19000101_m1_1_CLEAN': ['19000101_m1'], 'expid_19000101_m1_2_CLEAN': ['19000101_m1'],
        }

        self.job_list.get_date_list = Mock(return_value=['19000101', '19000202'])
        self.job_list.get_member_list = Mock(return_value=['m1', 'm2'])
        self.job_list.get_chunk_list = Mock(return_value=[1, 2])
        self.job_list.get_date_format = Mock(return_value='')

        side_effect = []
        for job in reversed(self.job_list.get_job_list()):
            if job.member is not None:
                side_effect.append(date2str(job.date, ''))

        with patch('autosubmit.job.job_grouping.date2str', side_effect=side_effect):
            job_grouping = JobGrouping('member', self.job_list.get_job_list(), self.job_list,
                                   expand_list="[ 19000101 [m2] 19000202 [m2] ]", expanded_status=[Status.RUNNING])
            self.assertDictEqual(job_grouping.group_jobs(), groups_dict)

    def test_group_by_chunk_expand(self, *patches):
        self.job_list.get_job_by_name('expid_19000101_m1_INI').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m2_INI').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000202_m1_INI').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000202_m2_INI').status = Status.COMPLETED

        self.job_list.get_job_by_name('expid_19000101_m1_1_SIM').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m1_1_POST').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m1_1_CLEAN').status = Status.FAILED

        self.job_list.get_job_by_name('expid_19000101_m1_2_SIM').status = Status.READY

        self.job_list.get_job_by_name('expid_19000101_m2_1_SIM').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m2_1_POST').status = Status.COMPLETED
        self.job_list.get_job_by_name('expid_19000101_m2_1_CLEAN').status = Status.RUNNING

        groups_dict = dict()

        groups_dict['status'] = {'19000101_m1_1': Status.FAILED, '19000101_m1_2': Status.READY, '19000101_m2_1': Status.RUNNING, '19000202_m2_2': Status.WAITING}
        groups_dict['jobs'] = {
            'expid_19000101_m1_1_SIM': ['19000101_m1_1'], 'expid_19000101_m1_2_SIM': ['19000101_m1_2'], 'expid_19000101_m2_1_SIM': ['19000101_m2_1'],
            'expid_19000202_m2_2_SIM': ['19000202_m2_2'],

            'expid_19000101_m1_1_POST': ['19000101_m1_1'], 'expid_19000101_m1_2_POST': ['19000101_m1_2'], 'expid_19000101_m2_1_POST': ['19000101_m2_1'],
            'expid_19000202_m2_2_POST': ['19000202_m2_2'],

            'expid_19000101_m1_1_CLEAN': ['19000101_m1_1'], 'expid_19000101_m1_2_CLEAN': ['19000101_m1_2'], 'expid_19000101_m2_1_CLEAN': ['19000101_m2_1'],
            'expid_19000202_m2_2_CLEAN': ['19000202_m2_2']
        }

        self.job_list.get_date_list = Mock(return_value=['19000101', '19000202'])
        self.job_list.get_member_list = Mock(return_value=['m1', 'm2'])
        self.job_list.get_chunk_list = Mock(return_value=[1, 2])
        self.job_list.get_date_format = Mock(return_value='')

        job_grouping = JobGrouping('chunk', self.job_list.get_job_list(), self.job_list,
                               expand_list="[ 19000101 [m2 [2] ] 19000202 [m1 [1 2] m2 [1] ] ]")
        self.assertDictEqual(job_grouping.group_jobs(), groups_dict)

    def test_synchronize_member_group_member(self):
        for date in ['19000101', '19000202']:
            for chunk in [1, 2]:
                job = self._createDummyJob('expid_' + date + '_' + str(chunk) + '_ASIM',
                                           Status.WAITING, date, None, chunk)
                for member in ['m1', 'm2']:
                    job.add_parent(
                        self.job_list.get_job_by_name('expid_' + date + '_' + member + '_' + str(chunk) + '_SIM'))

                self.job_list.get_job_list().append(job)

        groups_dict = dict()
        groups_dict['status'] = {'19000101_m1': Status.WAITING,
                                 '19000101_m2': Status.WAITING,
                                 '19000202_m1': Status.WAITING,
                                 '19000202_m2': Status.WAITING}
        groups_dict['jobs'] = {
            'expid_19000101_m1_INI': ['19000101_m1'], 'expid_19000101_m2_INI': ['19000101_m2'], 'expid_19000202_m1_INI': ['19000202_m1'],
            'expid_19000202_m2_INI': ['19000202_m2'],
            'expid_19000101_m1_1_SIM': ['19000101_m1'], 'expid_19000101_m1_2_SIM': ['19000101_m1'], 'expid_19000101_m2_1_SIM': ['19000101_m2'],
            'expid_19000101_m2_2_SIM': ['19000101_m2'],
            'expid_19000202_m1_1_SIM': ['19000202_m1'], 'expid_19000202_m1_2_SIM': ['19000202_m1'], 'expid_19000202_m2_1_SIM': ['19000202_m2'],
            'expid_19000202_m2_2_SIM': ['19000202_m2'],

            'expid_19000101_m1_1_POST': ['19000101_m1'], 'expid_19000101_m1_2_POST': ['19000101_m1'], 'expid_19000101_m2_1_POST': ['19000101_m2'],
            'expid_19000101_m2_2_POST': ['19000101_m2'],
            'expid_19000202_m1_1_POST': ['19000202_m1'], 'expid_19000202_m1_2_POST': ['19000202_m1'], 'expid_19000202_m2_1_POST': ['19000202_m2'],
            'expid_19000202_m2_2_POST': ['19000202_m2'],

            'expid_19000101_m1_1_CLEAN': ['19000101_m1'], 'expid_19000101_m1_2_CLEAN': ['19000101_m1'], 'expid_19000101_m2_1_CLEAN': ['19000101_m2'],
            'expid_19000101_m2_2_CLEAN': ['19000101_m2'],
            'expid_19000202_m1_1_CLEAN': ['19000202_m1'], 'expid_19000202_m1_2_CLEAN': ['19000202_m1'], 'expid_19000202_m2_1_CLEAN': ['19000202_m2'],
            'expid_19000202_m2_2_CLEAN': ['19000202_m2'],

            'expid_19000101_1_ASIM' : ['19000101_m1', '19000101_m2'], 'expid_19000101_2_ASIM' : ['19000101_m1', '19000101_m2'],
            'expid_19000202_1_ASIM': ['19000202_m1', '19000202_m2'], 'expid_19000202_2_ASIM' : ['19000202_m1', '19000202_m2']
        }

        self.job_list.get_date_list = Mock(return_value=['19000101', '19000202'])
        self.job_list.get_member_list = Mock(return_value=['m1', 'm2'])
        self.job_list.get_chunk_list = Mock(return_value=[1, 2])
        self.job_list.get_date_format = Mock(return_value='')

        job_grouping = JobGrouping('member', self.job_list.get_job_list(), self.job_list)
        self.assertDictEqual(job_grouping.group_jobs(), groups_dict)

    def test_synchronize_member_group_chunk(self):
        for date in ['19000101', '19000202']:
            for chunk in [1, 2]:
                job = self._createDummyJob('expid_' + date + '_' + str(chunk) + '_ASIM',
                                           Status.WAITING, date, None, chunk)
                for member in ['m1', 'm2']:
                    job.add_parent(
                        self.job_list.get_job_by_name('expid_' + date + '_' + member + '_' + str(chunk) + '_SIM'))

                self.job_list.get_job_list().append(job)

        groups_dict = dict()
        groups_dict['status'] = {'19000101_m1_1': Status.WAITING, '19000101_m1_2': Status.WAITING,
                                 '19000101_m2_1': Status.WAITING, '19000101_m2_2': Status.WAITING,
                                 '19000202_m1_1': Status.WAITING, '19000202_m1_2': Status.WAITING,
                                 '19000202_m2_1': Status.WAITING, '19000202_m2_2': Status.WAITING}
        groups_dict['jobs'] = {
            'expid_19000101_m1_1_SIM': ['19000101_m1_1'], 'expid_19000101_m1_2_SIM': ['19000101_m1_2'], 'expid_19000101_m2_1_SIM': ['19000101_m2_1'],
            'expid_19000101_m2_2_SIM': ['19000101_m2_2'],
            'expid_19000202_m1_1_SIM': ['19000202_m1_1'], 'expid_19000202_m1_2_SIM': ['19000202_m1_2'], 'expid_19000202_m2_1_SIM': ['19000202_m2_1'],
            'expid_19000202_m2_2_SIM': ['19000202_m2_2'],

            'expid_19000101_m1_1_POST': ['19000101_m1_1'], 'expid_19000101_m1_2_POST': ['19000101_m1_2'], 'expid_19000101_m2_1_POST': ['19000101_m2_1'],
            'expid_19000101_m2_2_POST': ['19000101_m2_2'],
            'expid_19000202_m1_1_POST': ['19000202_m1_1'], 'expid_19000202_m1_2_POST': ['19000202_m1_2'], 'expid_19000202_m2_1_POST': ['19000202_m2_1'],
            'expid_19000202_m2_2_POST': ['19000202_m2_2'],

            'expid_19000101_m1_1_CLEAN': ['19000101_m1_1'], 'expid_19000101_m1_2_CLEAN': ['19000101_m1_2'], 'expid_19000101_m2_1_CLEAN': ['19000101_m2_1'],
            'expid_19000101_m2_2_CLEAN': ['19000101_m2_2'],
            'expid_19000202_m1_1_CLEAN': ['19000202_m1_1'], 'expid_19000202_m1_2_CLEAN': ['19000202_m1_2'], 'expid_19000202_m2_1_CLEAN': ['19000202_m2_1'],
            'expid_19000202_m2_2_CLEAN': ['19000202_m2_2'],

            'expid_19000101_1_ASIM' : ['19000101_m1_1', '19000101_m2_1'], 'expid_19000101_2_ASIM' : ['19000101_m1_2', '19000101_m2_2'],
            'expid_19000202_1_ASIM': ['19000202_m1_1', '19000202_m2_1'], 'expid_19000202_2_ASIM' : ['19000202_m1_2', '19000202_m2_2']
        }

        self.job_list.get_date_list = Mock(return_value=['19000101', '19000202'])
        self.job_list.get_member_list = Mock(return_value=['m1', 'm2'])
        self.job_list.get_chunk_list = Mock(return_value=[1, 2])
        self.job_list.get_date_format = Mock(return_value='')

        job_grouping = JobGrouping('chunk', self.job_list.get_job_list(), self.job_list)
        self.assertDictEqual(job_grouping.group_jobs(), groups_dict)

    def test_synchronize_member_group_date(self):
        for date in ['19000101', '19000202']:
            for chunk in [1, 2]:
                job = self._createDummyJob('expid_' + date + '_' + str(chunk) + '_ASIM',
                                           Status.WAITING, date, None, chunk)
                for member in ['m1', 'm2']:
                    job.add_parent(
                        self.job_list.get_job_by_name('expid_' + date + '_' + member + '_' + str(chunk) + '_SIM'))

                self.job_list.get_job_list().append(job)

        groups_dict = dict()
        groups_dict['status'] = {'19000101': Status.WAITING,
                                 '19000202': Status.WAITING}
        groups_dict['jobs'] = {
            'expid_19000101_m1_INI': ['19000101'], 'expid_19000101_m2_INI': ['19000101'], 'expid_19000202_m1_INI': ['19000202'],
            'expid_19000202_m2_INI': ['19000202'],
            'expid_19000101_m1_1_SIM': ['19000101'], 'expid_19000101_m1_2_SIM': ['19000101'], 'expid_19000101_m2_1_SIM': ['19000101'],
            'expid_19000101_m2_2_SIM': ['19000101'],
            'expid_19000202_m1_1_SIM': ['19000202'], 'expid_19000202_m1_2_SIM': ['19000202'], 'expid_19000202_m2_1_SIM': ['19000202'],
            'expid_19000202_m2_2_SIM': ['19000202'],

            'expid_19000101_m1_1_POST': ['19000101'], 'expid_19000101_m1_2_POST': ['19000101'], 'expid_19000101_m2_1_POST': ['19000101'],
            'expid_19000101_m2_2_POST': ['19000101'],
            'expid_19000202_m1_1_POST': ['19000202'], 'expid_19000202_m1_2_POST': ['19000202'], 'expid_19000202_m2_1_POST': ['19000202'],
            'expid_19000202_m2_2_POST': ['19000202'],

            'expid_19000101_m1_1_CLEAN': ['19000101'], 'expid_19000101_m1_2_CLEAN': ['19000101'], 'expid_19000101_m2_1_CLEAN': ['19000101'],
            'expid_19000101_m2_2_CLEAN': ['19000101'],
            'expid_19000202_m1_1_CLEAN': ['19000202'], 'expid_19000202_m1_2_CLEAN': ['19000202'], 'expid_19000202_m2_1_CLEAN': ['19000202'],
            'expid_19000202_m2_2_CLEAN': ['19000202'],

            'expid_19000101_1_ASIM': ['19000101'], 'expid_19000101_2_ASIM': ['19000101'],
            'expid_19000202_1_ASIM': ['19000202'], 'expid_19000202_2_ASIM': ['19000202']
        }

        job_grouping = JobGrouping('date', self.job_list.get_job_list(), self.job_list)
        self.assertDictEqual(job_grouping.group_jobs(), groups_dict)

    def test_synchronize_date_group_member(self):
        for chunk in [1, 2]:
            job = self._createDummyJob('expid_' + str(chunk) + '_ASIM',
                                           Status.WAITING, None, None, chunk)
            for date in ['19000101', '19000202']:
                for member in ['m1', 'm2']:
                    job.add_parent(
                        self.job_list.get_job_by_name('expid_' + date + '_' + member + '_' + str(chunk) + '_SIM'))

            self.job_list.get_job_list().append(job)

        groups_dict = dict()
        groups_dict['status'] = {'19000101_m1': Status.WAITING,
                                 '19000101_m2': Status.WAITING,
                                 '19000202_m1': Status.WAITING,
                                 '19000202_m2': Status.WAITING}
        groups_dict['jobs'] = {
            'expid_19000101_m1_INI': ['19000101_m1'], 'expid_19000101_m2_INI': ['19000101_m2'], 'expid_19000202_m1_INI': ['19000202_m1'],
            'expid_19000202_m2_INI': ['19000202_m2'],
            'expid_19000101_m1_1_SIM': ['19000101_m1'], 'expid_19000101_m1_2_SIM': ['19000101_m1'], 'expid_19000101_m2_1_SIM': ['19000101_m2'],
            'expid_19000101_m2_2_SIM': ['19000101_m2'],
            'expid_19000202_m1_1_SIM': ['19000202_m1'], 'expid_19000202_m1_2_SIM': ['19000202_m1'], 'expid_19000202_m2_1_SIM': ['19000202_m2'],
            'expid_19000202_m2_2_SIM': ['19000202_m2'],

            'expid_19000101_m1_1_POST': ['19000101_m1'], 'expid_19000101_m1_2_POST': ['19000101_m1'], 'expid_19000101_m2_1_POST': ['19000101_m2'],
            'expid_19000101_m2_2_POST': ['19000101_m2'],
            'expid_19000202_m1_1_POST': ['19000202_m1'], 'expid_19000202_m1_2_POST': ['19000202_m1'], 'expid_19000202_m2_1_POST': ['19000202_m2'],
            'expid_19000202_m2_2_POST': ['19000202_m2'],

            'expid_19000101_m1_1_CLEAN': ['19000101_m1'], 'expid_19000101_m1_2_CLEAN': ['19000101_m1'], 'expid_19000101_m2_1_CLEAN': ['19000101_m2'],
            'expid_19000101_m2_2_CLEAN': ['19000101_m2'],
            'expid_19000202_m1_1_CLEAN': ['19000202_m1'], 'expid_19000202_m1_2_CLEAN': ['19000202_m1'], 'expid_19000202_m2_1_CLEAN': ['19000202_m2'],
            'expid_19000202_m2_2_CLEAN': ['19000202_m2'],

            'expid_1_ASIM' : ['19000101_m1', '19000101_m2', '19000202_m1', '19000202_m2'], 'expid_2_ASIM' : ['19000101_m1', '19000101_m2', '19000202_m1', '19000202_m2']
        }

        self.job_list.get_date_list = Mock(return_value=['19000101', '19000202'])
        self.job_list.get_member_list = Mock(return_value=['m1', 'm2'])
        self.job_list.get_chunk_list = Mock(return_value=[1, 2])
        self.job_list.get_date_format = Mock(return_value='')

        side_effect = []
        for job in reversed(self.job_list.get_job_list()):
            if job.date is None and job.chunk is not None:
                side_effect.append('19000101')
                side_effect.append('19000101')
                side_effect.append('19000101')
                side_effect.append('19000202')
                side_effect.append('19000202')
                side_effect.append('19000202')
            else:
                side_effect.append(date2str(job.date, ''))

        with patch('autosubmit.job.job_grouping.date2str', side_effect=side_effect):
            job_grouping = JobGrouping('member', self.job_list.get_job_list(), self.job_list)
            self.assertDictEqual(job_grouping.group_jobs(), groups_dict)

    def test_synchronize_date_group_chunk(self):
        for chunk in [1, 2]:
            job = self._createDummyJob('expid_' + str(chunk) + '_ASIM',
                                           Status.WAITING, None, None, chunk)
            for date in ['19000101', '19000202']:
                for member in ['m1', 'm2']:
                    job.add_parent(
                        self.job_list.get_job_by_name('expid_' + date + '_' + member + '_' + str(chunk) + '_SIM'))

            self.job_list.get_job_list().append(job)

        groups_dict = dict()
        groups_dict['status'] = {'19000101_m1_1': Status.WAITING, '19000101_m1_2': Status.WAITING,
                                 '19000101_m2_1': Status.WAITING, '19000101_m2_2': Status.WAITING,
                                 '19000202_m1_1': Status.WAITING, '19000202_m1_2': Status.WAITING,
                                 '19000202_m2_1': Status.WAITING, '19000202_m2_2': Status.WAITING}
        groups_dict['jobs'] = {
            'expid_19000101_m1_1_SIM': ['19000101_m1_1'], 'expid_19000101_m1_2_SIM': ['19000101_m1_2'], 'expid_19000101_m2_1_SIM': ['19000101_m2_1'],
            'expid_19000101_m2_2_SIM': ['19000101_m2_2'],
            'expid_19000202_m1_1_SIM': ['19000202_m1_1'], 'expid_19000202_m1_2_SIM': ['19000202_m1_2'], 'expid_19000202_m2_1_SIM': ['19000202_m2_1'],
            'expid_19000202_m2_2_SIM': ['19000202_m2_2'],

            'expid_19000101_m1_1_POST': ['19000101_m1_1'], 'expid_19000101_m1_2_POST': ['19000101_m1_2'], 'expid_19000101_m2_1_POST': ['19000101_m2_1'],
            'expid_19000101_m2_2_POST': ['19000101_m2_2'],
            'expid_19000202_m1_1_POST': ['19000202_m1_1'], 'expid_19000202_m1_2_POST': ['19000202_m1_2'], 'expid_19000202_m2_1_POST': ['19000202_m2_1'],
            'expid_19000202_m2_2_POST': ['19000202_m2_2'],

            'expid_19000101_m1_1_CLEAN': ['19000101_m1_1'], 'expid_19000101_m1_2_CLEAN': ['19000101_m1_2'], 'expid_19000101_m2_1_CLEAN': ['19000101_m2_1'],
            'expid_19000101_m2_2_CLEAN': ['19000101_m2_2'],
            'expid_19000202_m1_1_CLEAN': ['19000202_m1_1'], 'expid_19000202_m1_2_CLEAN': ['19000202_m1_2'], 'expid_19000202_m2_1_CLEAN': ['19000202_m2_1'],
            'expid_19000202_m2_2_CLEAN': ['19000202_m2_2'],

            'expid_1_ASIM' : ['19000101_m1_1', '19000101_m2_1', '19000202_m1_1', '19000202_m2_1'], 'expid_2_ASIM' : ['19000101_m1_2', '19000101_m2_2', '19000202_m1_2', '19000202_m2_2'],
        }

        self.job_list.get_date_list = Mock(return_value=['19000101', '19000202'])
        self.job_list.get_member_list = Mock(return_value=['m1', 'm2'])
        self.job_list.get_chunk_list = Mock(return_value=[1, 2])
        self.job_list.get_date_format = Mock(return_value='')

        side_effect = []
        for job in reversed(self.job_list.get_job_list()):
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

        with patch('autosubmit.job.job_grouping.date2str', side_effect=side_effect):
            job_grouping = JobGrouping('chunk', self.job_list.get_job_list(), self.job_list)
            self.assertDictEqual(job_grouping.group_jobs(), groups_dict)

    def test_synchronize_date_group_date(self):
        for chunk in [1, 2]:
            job = self._createDummyJob('expid_' + str(chunk) + '_ASIM',
                                           Status.WAITING, None, None, chunk)
            for date in ['19000101', '19000202']:
                for member in ['m1', 'm2']:
                    job.add_parent(
                        self.job_list.get_job_by_name('expid_' + date + '_' + member + '_' + str(chunk) + '_SIM'))

            self.job_list.get_job_list().append(job)

        groups_dict = dict()
        groups_dict['status'] = {'19000101': Status.WAITING,
                                 '19000202': Status.WAITING}
        groups_dict['jobs'] = {
            'expid_19000101_m1_INI': ['19000101'], 'expid_19000101_m2_INI': ['19000101'], 'expid_19000202_m1_INI': ['19000202'],
            'expid_19000202_m2_INI': ['19000202'],
            'expid_19000101_m1_1_SIM': ['19000101'], 'expid_19000101_m1_2_SIM': ['19000101'], 'expid_19000101_m2_1_SIM': ['19000101'],
            'expid_19000101_m2_2_SIM': ['19000101'],
            'expid_19000202_m1_1_SIM': ['19000202'], 'expid_19000202_m1_2_SIM': ['19000202'], 'expid_19000202_m2_1_SIM': ['19000202'],
            'expid_19000202_m2_2_SIM': ['19000202'],

            'expid_19000101_m1_1_POST': ['19000101'], 'expid_19000101_m1_2_POST': ['19000101'], 'expid_19000101_m2_1_POST': ['19000101'],
            'expid_19000101_m2_2_POST': ['19000101'],
            'expid_19000202_m1_1_POST': ['19000202'], 'expid_19000202_m1_2_POST': ['19000202'], 'expid_19000202_m2_1_POST': ['19000202'],
            'expid_19000202_m2_2_POST': ['19000202'],

            'expid_19000101_m1_1_CLEAN': ['19000101'], 'expid_19000101_m1_2_CLEAN': ['19000101'], 'expid_19000101_m2_1_CLEAN': ['19000101'],
            'expid_19000101_m2_2_CLEAN': ['19000101'],
            'expid_19000202_m1_1_CLEAN': ['19000202'], 'expid_19000202_m1_2_CLEAN': ['19000202'], 'expid_19000202_m2_1_CLEAN': ['19000202'],
            'expid_19000202_m2_2_CLEAN': ['19000202'],

            'expid_1_ASIM': ['19000101', '19000202'], 'expid_2_ASIM': ['19000101', '19000202']
        }

        self.job_list.get_date_list = Mock(return_value=['19000101', '19000202'])
        self.job_list.get_member_list = Mock(return_value=['m1', 'm2'])
        self.job_list.get_chunk_list = Mock(return_value=[1, 2])
        self.job_list.get_date_format = Mock(return_value='')

        side_effect = []
        for job in reversed(self.job_list.get_job_list()):
            if job.date is not None:
                side_effect.append(date2str(job.date))
            elif job.chunk is not None:
                side_effect.append('19000101')
                side_effect.append('19000202')

        with patch('autosubmit.job.job_grouping.date2str', side_effect=side_effect):
            job_grouping = JobGrouping('date', self.job_list.get_job_list(), self.job_list)
            self.assertDictEqual(job_grouping.group_jobs(), groups_dict)

    def _createDummyJob(self, name, status, date=None, member=None, chunk=None, split=None):
        job_id = randrange(1, 999)
        job = Job(name, job_id, status, 0)
        job.type = randrange(0, 2)

        job.date = parse_date(date)
        job.member = member
        job.chunk = chunk
        job.split = split

        return job

class FakeBasicConfig:
    def __init__(self):
        pass
    def props(self):
        pr = {}
        for name in dir(self):
            value = getattr(self, name)
            if not name.startswith('__') and not inspect.ismethod(value) and not inspect.isfunction(value):
                pr[name] = value
        return pr
    DB_DIR = '/dummy/db/dir'
    DB_FILE = '/dummy/db/file'
    DB_PATH = '/dummy/db/path'
    LOCAL_ROOT_DIR = '/dummy/local/root/dir'
    LOCAL_TMP_DIR = '/dummy/local/temp/dir'
    LOCAL_PROJ_DIR = '/dummy/local/proj/dir'
    DEFAULT_PLATFORMS_CONF = ''
    DEFAULT_JOBS_CONF = ''
