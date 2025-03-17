import inspect
from unittest.mock import MagicMock

import os
from unittest import TestCase
from copy import copy
import networkx
from networkx import DiGraph
#import patch
from textwrap import dedent
import shutil
import tempfile
from mock import Mock, patch
from random import randrange
from pathlib import Path


from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_common import Type
from autosubmit.job.job_dict import DicJobs
from autosubmit.job.job_list import JobList
from autosubmit.job.job_list_persistence import JobListPersistencePkl
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory
from log.log import AutosubmitCritical


class TestJobList(TestCase):
    def setUp(self):
        self.experiment_id = 'random-id'
        self.as_conf = MagicMock()
        self.as_conf.expid = self.experiment_id
        self.as_conf.experiment_data = dict()
        self.as_conf.experiment_data["JOBS"] = dict()
        self.as_conf.jobs_data = self.as_conf.experiment_data["JOBS"]
        self.as_conf.experiment_data["PLATFORMS"] = dict()
        parameters = {'fake-key': 'fake-value',
                      'fake-key2': 'fake-value2'}
        self.as_conf.load_parameters = Mock(return_value=parameters)
        self.as_conf.default_parameters = {}
        self.temp_directory = tempfile.mkdtemp()
        joblist_persistence = JobListPersistencePkl()
        self.job_list = JobList(self.experiment_id, self.as_conf, YAMLParserFactory(),joblist_persistence)
        # creating jobs for self list
        self.completed_job = self._createDummyJobWithStatus(Status.COMPLETED)
        self.completed_job2 = self._createDummyJobWithStatus(Status.COMPLETED)
        self.completed_job3 = self._createDummyJobWithStatus(Status.COMPLETED)
        self.completed_job4 = self._createDummyJobWithStatus(Status.COMPLETED)

        self.submitted_job = self._createDummyJobWithStatus(Status.SUBMITTED)
        self.submitted_job2 = self._createDummyJobWithStatus(Status.SUBMITTED)
        self.submitted_job3 = self._createDummyJobWithStatus(Status.SUBMITTED)

        self.running_job = self._createDummyJobWithStatus(Status.RUNNING)
        self.running_job2 = self._createDummyJobWithStatus(Status.RUNNING)

        self.queuing_job = self._createDummyJobWithStatus(Status.QUEUING)

        self.failed_job = self._createDummyJobWithStatus(Status.FAILED)
        self.failed_job2 = self._createDummyJobWithStatus(Status.FAILED)
        self.failed_job3 = self._createDummyJobWithStatus(Status.FAILED)
        self.failed_job4 = self._createDummyJobWithStatus(Status.FAILED)

        self.ready_job = self._createDummyJobWithStatus(Status.READY)
        self.ready_job2 = self._createDummyJobWithStatus(Status.READY)
        self.ready_job3 = self._createDummyJobWithStatus(Status.READY)

        self.waiting_job = self._createDummyJobWithStatus(Status.WAITING)
        self.waiting_job2 = self._createDummyJobWithStatus(Status.WAITING)

        self.unknown_job = self._createDummyJobWithStatus(Status.UNKNOWN)

        self.job_list._job_list = [self.completed_job, self.completed_job2, self.completed_job3, self.completed_job4,
                                   self.submitted_job, self.submitted_job2, self.submitted_job3, self.running_job,
                                   self.running_job2, self.queuing_job, self.failed_job, self.failed_job2,
                                   self.failed_job3, self.failed_job4, self.ready_job, self.ready_job2,
                                   self.ready_job3, self.waiting_job, self.waiting_job2, self.unknown_job]

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_directory)

    def test_load(self):
        as_conf = Mock()
        as_conf.experiment_data = dict()
        as_conf.expid = "random-id"
        parser_mock = Mock()
        parser_mock.read = Mock()
        factory = YAMLParserFactory()
        factory.create_parser = Mock(return_value=parser_mock)
        date_list = ['fake-date1', 'fake-date2']
        member_list = ['fake-member1', 'fake-member2']
        num_chunks = 999
        parameters = {'fake-key': 'fake-value',
                      'fake-key2': 'fake-value2'}
        with tempfile.TemporaryDirectory() as temp_dir:
            job_list = self.new_job_list(factory, temp_dir)
            FakeBasicConfig.LOCAL_ROOT_DIR = str(temp_dir)
            Path(temp_dir, self.experiment_id).mkdir()
            for path in [f'{self.experiment_id}/tmp', f'{self.experiment_id}/tmp/ASLOGS',
                         f'{self.experiment_id}/tmp/ASLOGS_{self.experiment_id}', f'{self.experiment_id}/proj',
                         f'{self.experiment_id}/conf', f'{self.experiment_id}/pkl']:
                Path(temp_dir, path).mkdir()
            job_list.changes = Mock(return_value=['random_section', 'random_section'])
            as_conf.detailed_deep_diff = Mock(return_value={})
            # as_conf.get_member_list = Mock(return_value=member_list)
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
            job_list_to_load = self.new_job_list(factory, temp_dir)
            # chmod
            job_list_to_load.load(False)
            self.assertEqual(job_list_to_load._job_list, job_list._job_list)
            job_list_to_load.load(True)
            self.assertEqual(job_list_to_load._job_list, job_list._job_list)
            os.chmod(f'{temp_dir}/{self.experiment_id}/pkl/job_list_random-id.pkl', 0o000)
            # Works with pytest doesn't work in pipeline TODO enable this test
            # with self.assertRaises(AutosubmitCritical):
            #     job_list_to_load.load(False)
            job_list_to_load.load(True)
            self.assertEqual(job_list_to_load._job_list, job_list._job_list)
            os.chmod(f'{temp_dir}/{self.experiment_id}/pkl/job_list_random-id.pkl', 0o777)
            shutil.copy(f'{temp_dir}/{self.experiment_id}/pkl/job_list_random-id.pkl',f'{temp_dir}/{self.experiment_id}/pkl/job_list_random-id_backup.pkl')
            os.remove(f'{temp_dir}/{self.experiment_id}/pkl/job_list_random-id.pkl')
            job_list_to_load.load(False)
            self.assertEqual(job_list_to_load._job_list, job_list._job_list)
            job_list_to_load.load(True)
            self.assertEqual(job_list_to_load._job_list, job_list._job_list)

    def test_get_job_list_returns_the_right_list(self):
        job_list = self.job_list.get_job_list()
        self.assertEqual(self.job_list._job_list, job_list)

    def test_get_completed_returns_only_the_completed(self):
        completed = self.job_list.get_completed()

        self.assertEqual(4, len(completed))
        self.assertTrue(self.completed_job in completed)
        self.assertTrue(self.completed_job2 in completed)
        self.assertTrue(self.completed_job3 in completed)
        self.assertTrue(self.completed_job4 in completed)

    def test_get_submitted_returns_only_the_submitted(self):
        submitted = self.job_list.get_submitted()

        self.assertEqual(3, len(submitted))
        self.assertTrue(self.submitted_job in submitted)
        self.assertTrue(self.submitted_job2 in submitted)
        self.assertTrue(self.submitted_job3 in submitted)

    def test_get_running_returns_only_which_are_running(self):
        running = self.job_list.get_running()

        self.assertEqual(2, len(running))
        self.assertTrue(self.running_job in running)
        self.assertTrue(self.running_job2 in running)

    def test_get_running_returns_only_which_are_queuing(self):
        queuing = self.job_list.get_queuing()

        self.assertEqual(1, len(queuing))
        self.assertTrue(self.queuing_job in queuing)

    def test_get_failed_returns_only_the_failed(self):
        failed = self.job_list.get_failed()

        self.assertEqual(4, len(failed))
        self.assertTrue(self.failed_job in failed)
        self.assertTrue(self.failed_job2 in failed)
        self.assertTrue(self.failed_job3 in failed)
        self.assertTrue(self.failed_job4 in failed)

    def test_get_ready_returns_only_the_ready(self):
        ready = self.job_list.get_ready()

        self.assertEqual(3, len(ready))
        self.assertTrue(self.ready_job in ready)
        self.assertTrue(self.ready_job2 in ready)
        self.assertTrue(self.ready_job3 in ready)

    def test_get_waiting_returns_only_which_are_waiting(self):
        waiting = self.job_list.get_waiting()

        self.assertEqual(2, len(waiting))
        self.assertTrue(self.waiting_job in waiting)
        self.assertTrue(self.waiting_job2 in waiting)

    def test_get_unknown_returns_only_which_are_unknown(self):
        unknown = self.job_list.get_unknown()

        self.assertEqual(1, len(unknown))
        self.assertTrue(self.unknown_job in unknown)

    def test_get_in_queue_returns_only_which_are_queuing_submitted_and_running(self):
        in_queue = self.job_list.get_in_queue()

        self.assertEqual(7, len(in_queue))
        self.assertTrue(self.queuing_job in in_queue)
        self.assertTrue(self.running_job in in_queue)
        self.assertTrue(self.running_job2 in in_queue)
        self.assertTrue(self.submitted_job in in_queue)
        self.assertTrue(self.submitted_job2 in in_queue)
        self.assertTrue(self.submitted_job3 in in_queue)
        self.assertTrue(self.unknown_job in in_queue)

    def test_get_not_in_queue_returns_only_which_are_waiting_and_ready(self):
        not_in_queue = self.job_list.get_not_in_queue()

        self.assertEqual(5, len(not_in_queue))
        self.assertTrue(self.waiting_job in not_in_queue)
        self.assertTrue(self.waiting_job2 in not_in_queue)
        self.assertTrue(self.ready_job in not_in_queue)
        self.assertTrue(self.ready_job2 in not_in_queue)
        self.assertTrue(self.ready_job3 in not_in_queue)

    def test_get_finished_returns_only_which_are_completed_and_failed(self):
        finished = self.job_list.get_finished()

        self.assertEqual(8, len(finished))
        self.assertTrue(self.completed_job in finished)
        self.assertTrue(self.completed_job2 in finished)
        self.assertTrue(self.completed_job3 in finished)
        self.assertTrue(self.completed_job4 in finished)
        self.assertTrue(self.failed_job in finished)
        self.assertTrue(self.failed_job2 in finished)
        self.assertTrue(self.failed_job3 in finished)
        self.assertTrue(self.failed_job4 in finished)

    def test_get_active_returns_only_which_are_in_queue_ready_and_unknown(self):
        active = self.job_list.get_active()

        self.assertEqual(10, len(active))
        self.assertTrue(self.queuing_job in active)
        self.assertTrue(self.running_job in active)
        self.assertTrue(self.running_job2 in active)
        self.assertTrue(self.submitted_job in active)
        self.assertTrue(self.submitted_job2 in active)
        self.assertTrue(self.submitted_job3 in active)
        self.assertTrue(self.ready_job in active)
        self.assertTrue(self.ready_job2 in active)
        self.assertTrue(self.ready_job3 in active)
        self.assertTrue(self.unknown_job in active)

    def test_get_job_by_name_returns_the_expected_job(self):
        job = self.job_list.get_job_by_name(self.completed_job.name)

        self.assertEqual(self.completed_job, job)

    def test_sort_by_name_returns_the_list_of_jobs_well_sorted(self):
        sorted_by_name = self.job_list.sort_by_name()

        for i in range(len(sorted_by_name) - 1):
            self.assertTrue(
                sorted_by_name[i].name <= sorted_by_name[i + 1].name)

    def test_sort_by_id_returns_the_list_of_jobs_well_sorted(self):
        sorted_by_id = self.job_list.sort_by_id()

        for i in range(len(sorted_by_id) - 1):
            self.assertTrue(sorted_by_id[i].id <= sorted_by_id[i + 1].id)

    def test_sort_by_type_returns_the_list_of_jobs_well_sorted(self):
        sorted_by_type = self.job_list.sort_by_type()

        for i in range(len(sorted_by_type) - 1):
            self.assertTrue(
                sorted_by_type[i].type <= sorted_by_type[i + 1].type)

    def test_sort_by_status_returns_the_list_of_jobs_well_sorted(self):
        sorted_by_status = self.job_list.sort_by_status()

        for i in range(len(sorted_by_status) - 1):
            self.assertTrue(
                sorted_by_status[i].status <= sorted_by_status[i + 1].status)

    def test_that_create_method_makes_the_correct_calls(self):
        parser_mock = Mock()
        parser_mock.read = Mock()

        factory = YAMLParserFactory()
        factory.create_parser = Mock(return_value=parser_mock)

        job_list = JobList(self.experiment_id, self.as_conf,
                           factory, JobListPersistencePkl())
        job_list._create_jobs = Mock()
        job_list._add_dependencies = Mock()
        job_list.update_genealogy = Mock()
        job_list._job_list = [Job('random-name', 9999, Status.WAITING, 0),
                              Job('random-name2', 99999, Status.WAITING, 0)]
        date_list = ['fake-date1', 'fake-date2']
        member_list = ['fake-member1', 'fake-member2']
        num_chunks = 999
        chunk_list = list(range(1, num_chunks + 1))
        parameters = {'fake-key': 'fake-value',
                      'fake-key2': 'fake-value2'}
        graph = networkx.DiGraph()
        as_conf = MagicMock()
        job_list.graph = graph
        as_conf.experiment_data = {}
        as_conf.get_platform = Mock(return_value="fake-platform")
        # act
        with patch('autosubmit.job.job.Job.update_parameters', return_value={}):

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
            self.assertEqual(job_list.parameters, parameters)
            self.assertEqual(job_list._date_list, date_list)
            self.assertEqual(job_list._member_list, member_list)
            self.assertEqual(job_list._chunk_list, list(range(1, num_chunks + 1)))

            cj_args, cj_kwargs = job_list._create_jobs.call_args
            self.assertEqual(0, cj_args[2])

            #_add_dependencies(self, date_list, member_list, chunk_list, dic_jobs, option="DEPENDENCIES"):

            job_list._add_dependencies.assert_called_once_with(date_list, member_list, chunk_list, cj_args[0])
            # Adding flag update structure
            job_list.update_genealogy.assert_called_once_with()

            # job doesn't have job.parameters anymore TODO
            # for job in job_list._job_list:
            #     self.assertEqual(parameters, job.parameters)

    def test_that_create_job_method_calls_dic_jobs_method_with_increasing_priority(self):
        # arrange
        dic_mock = Mock()
        dic_mock.read_section = Mock()
        dic_mock.experiment_data = dict()
        dic_mock.experiment_data["JOBS"] = {'fake-section-1': {}, 'fake-section-2': {}}
        # act
        JobList._create_jobs(dic_mock, 0, Type.BASH)

        # arrange
        dic_mock.read_section.assert_any_call(
            'fake-section-1', 0, Type.BASH)
        dic_mock.read_section.assert_any_call(
            'fake-section-2', 1, Type.BASH)

    # autosubmit run -rm "fc0"
    def test_run_member(self):
        parser_mock = Mock()
        parser_mock.read = Mock()
        self.as_conf.get_platform = MagicMock(return_value="fake-platform")

        factory = YAMLParserFactory()
        factory.create_parser = Mock(return_value=parser_mock)
        job_list = JobList(self.experiment_id, self.as_conf,
                           factory, JobListPersistencePkl())
        job_list._create_jobs = Mock()
        job_list._add_dependencies = Mock()
        job_list.update_genealogy = Mock()
        job_list._job_list = [Job('random-name', 9999, Status.WAITING, 0),
                              Job('random-name2', 99999, Status.WAITING, 0)]
        date_list = ['fake-date1', 'fake-date2']
        member_list = ['fake-member1', 'fake-member2']
        num_chunks = 2
        parameters = {'fake-key': 'fake-value',
                      'fake-key2': 'fake-value2'}
        graph = networkx.DiGraph()
        as_conf = MagicMock()
        as_conf.experiment_data = {}
        as_conf.get_platform = Mock(return_value="fake-platform")
        job_list.graph = graph
        # act
        with patch('autosubmit.job.job.Job.update_parameters', return_value={}):

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
            self.assertEqual(len(job_list_aux._job_list), 1)
            job_list_aux = copy(job_list)
            job_list_aux.run_members = "not_exists"
            self.assertEqual(len(job_list_aux._job_list), 0)

    #autosubmit/job/job_list.py:create_dictionary - line 132
    def test_create_dictionary(self):
        parser_mock = Mock()
        parser_mock.read = Mock()
        parameters = {'fake-key': 'fake-value',
                      'fake-key2': 'fake-value2'}
        self.as_conf.experiment_data["JOBS"]  = {'fake-section': parameters, 'fake-section-2': parameters}
        self.as_conf.jobs_data = self.as_conf.experiment_data["JOBS"]
        factory = YAMLParserFactory()
        factory.create_parser = Mock(return_value=parser_mock)
        with patch('os.path.join', return_value='fake-tmp'):
            job_list = JobList(self.experiment_id, self.as_conf,
                               factory, JobListPersistencePkl())
            job_list._create_jobs = Mock()
            job_list._add_dependencies = Mock()
            job_list.update_genealogy = Mock()
            job_list._job_list = [Job('random-name_fake-date1_fake-member1', 9999, Status.WAITING, 0),
                                  Job('random-name2_fake_date2_fake-member2', 99999, Status.WAITING, 0)]
            for job in job_list._job_list:
                job.section = "fake-section"
            date_list = ['fake-date1', 'fake-date2']
            member_list = ['fake-member1', 'fake-member2']
            num_chunks = 2
            graph = networkx.DiGraph()
            job_list.graph = graph
            # act

            with patch('autosubmit.job.job_list._get_submitter', autospec=True) as mock_get_submitter:
                mock_submitter = mock_get_submitter.return_value
                mock_submitter.load_platforms = MagicMock()
                mock_submitter.load_platforms.return_value = ["fake-platform"]
                mock_submitter.platforms = None
                job_list.generate(
                    as_conf=self.as_conf,
                    date_list=date_list,
                    member_list=member_list,
                    num_chunks=num_chunks,
                    chunk_ini=1,
                    parameters=self.as_conf.load_parameters(),
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
            wrapper_jobs = {"WRAPPER_FAKESECTION": 'fake-section'}
            num_chunks = 2
            chunk_ini = 1
            date_format = "day"
            default_retrials = 1
            job_list._get_date = Mock(return_value="fake-date1")

            # act
            job_list.create_dictionary(date_list, member_list, num_chunks, chunk_ini, date_format, default_retrials,
                                       wrapper_jobs, self.as_conf)
            # assert
            self.assertEqual(len(job_list._ordered_jobs_by_date_member["WRAPPER_FAKESECTION"]["fake-date1"]["fake-member1"]), 1)

    def new_job_list(self,factory,temp_dir):
        job_list = JobList(self.experiment_id, self.as_conf,
                           factory, JobListPersistencePkl())
        job_list._persistence_path = f'{str(temp_dir)}/{self.experiment_id}/pkl'


        #job_list._create_jobs = Mock()
        #job_list._add_dependencies = Mock()
        #job_list.update_genealogy = Mock()
        #job_list._job_list = [Job('random-name', 9999, Status.WAITING, 0),
        #                      Job('random-name2', 99999, Status.WAITING, 0)]
        return job_list

    def test_generate_job_list_from_monitor_run(self):
        as_conf = MagicMock()
        as_conf.experiment_data = dict()
        as_conf.experiment_data["JOBS"] = dict()
        as_conf.experiment_data["JOBS"]["fake-section"] = dict()
        as_conf.experiment_data["JOBS"]["fake-section"]["file"] = "fake-file"
        as_conf.experiment_data["JOBS"]["fake-section"]["running"] = "once"
        as_conf.experiment_data["JOBS"]["fake-section2"] = dict()
        as_conf.experiment_data["JOBS"]["fake-section2"]["file"] = "fake-file2"
        as_conf.experiment_data["JOBS"]["fake-section2"]["running"] = "once"
        as_conf.jobs_data = as_conf.experiment_data["JOBS"]
        as_conf.experiment_data["PLATFORMS"] = dict()
        as_conf.experiment_data["PLATFORMS"]["fake-platform"] = dict()
        as_conf.experiment_data["PLATFORMS"]["fake-platform"]["type"] = "fake-type"
        as_conf.experiment_data["PLATFORMS"]["fake-platform"]["name"] = "fake-name"
        as_conf.experiment_data["PLATFORMS"]["fake-platform"]["user"] = "fake-user"

        as_conf.expid = self.experiment_id
        parser_mock = Mock()
        parser_mock.read = Mock()
        factory = YAMLParserFactory()
        factory.create_parser = Mock(return_value=parser_mock)
        date_list = ['fake-date1', 'fake-date2']
        member_list = ['fake-member1', 'fake-member2']
        num_chunks = 999
        chunk_list = list(range(1, num_chunks + 1))
        parameters = {'fake-key': 'fake-value',
                      'fake-key2': 'fake-value2'}
        with tempfile.TemporaryDirectory() as temp_dir:
            job_list = self.new_job_list(factory,temp_dir)
            FakeBasicConfig.LOCAL_ROOT_DIR = str(temp_dir)
            Path(temp_dir, self.experiment_id).mkdir()
            for path in [f'{self.experiment_id}/tmp', f'{self.experiment_id}/tmp/ASLOGS', f'{self.experiment_id}/tmp/ASLOGS_{self.experiment_id}', f'{self.experiment_id}/proj',
                         f'{self.experiment_id}/conf', f'{self.experiment_id}/pkl']:
                Path(temp_dir, path).mkdir()
            job_list.changes = Mock(return_value=['random_section', 'random_section'])
            as_conf.detailed_deep_diff = Mock(return_value={})
            #as_conf.get_member_list = Mock(return_value=member_list)
            with patch('autosubmit.job.job.Job.update_parameters', return_value={}):
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
                job_list2 = self.new_job_list(factory,temp_dir)
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

                #return False
                job_list2.update_from_file = Mock()
                job_list2.update_from_file.return_value = False
                job_list2.update_list(as_conf, False)

                # check that name is the same
                for index,job in enumerate(job_list._job_list):
                    self.assertEqual(job_list2._job_list[index].name, job.name)
                # check that status is the same
                for index,job in enumerate(job_list._job_list):
                    self.assertEqual(job_list2._job_list[index].status, job.status)
                self.assertEqual(job_list2._date_list, job_list._date_list)
                self.assertEqual(job_list2._member_list, job_list._member_list)
                self.assertEqual(job_list2._chunk_list, job_list._chunk_list)
                self.assertEqual(job_list2.parameters, job_list.parameters)
                job_list3 = self.new_job_list(factory,temp_dir)
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
                job_list3.update_from_file = Mock()
                job_list3.update_from_file.return_value = False
                job_list3.update_list(as_conf, False)
                # assert
                # check that name is the same
                for index, job in enumerate(job_list._job_list):
                    self.assertEqual(job_list3._job_list[index].name, job.name)
                # check that status is the same
                for index,job in enumerate(job_list._job_list):
                    self.assertEqual(job_list3._job_list[index].status, job.status)
                self.assertEqual(job_list3._date_list, job_list._date_list)
                self.assertEqual(job_list3._member_list, job_list._member_list)
                self.assertEqual(job_list3._chunk_list, job_list._chunk_list)
                self.assertEqual(job_list3.parameters, job_list.parameters)
                # DELETE WHEN EDGELESS TEST
                job_list3._job_list[0].dependencies = {"not_exist":None}
                job_list3._delete_edgeless_jobs()
                self.assertEqual(len(job_list3._job_list), 1)
                # Update Mayor Version test ( 4.0 -> 4.1)
                job_list3.graph = DiGraph()
                job_list3.save()
                job_list3 = self.new_job_list(factory,temp_dir)
                job_list3.update_genealogy = Mock(wraps=job_list3.update_genealogy)
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
                job_list3.update_genealogy.assert_called_once_with()

                # Test when the graph previous run has more jobs than the current run
                job_list3.graph.add_node("fake-node",job=job_list3._job_list[0])
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
                self.assertEqual(len(job_list3.graph.nodes),len(job_list3._job_list))
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
                self.assertEqual(len(job_list3.graph.nodes), len(job_list3._job_list))
                for node in job_list3.graph.nodes:
                    # if name is in the job_list
                    if node in [job.name for job in job_list3._job_list]:
                        self.assertTrue(job_list3.graph.nodes[node]["job"] in job_list3._job_list)


    def test_find_and_delete_redundant_relations(self):
        problematic_jobs = {'SECTION': {'CHILD': ['parents_names','parents_names1','parents_names2'], 'CHILD2':['parents_names3','parents_names4']}}
        self.setUp()
        with patch('autosubmit.job.job_list.DiGraph.has_successor') as mock_job_list:
            try:
                mock_job_list.return_value = True
                assert self.job_list.find_and_delete_redundant_relations(problematic_jobs) is None
                mock_job_list.return_value = False
                assert self.job_list.find_and_delete_redundant_relations(problematic_jobs) is None

            except Exception as e:
                assert f'Find and delete redundant relations ran into an Error deleting the relationship between parent and child: {e}'

    def test_normalize_to_filters(self):
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
                self.job_list._normalize_to_filters(filter_to, filter_type)
            except Exception as e:
                print(f'Unexpected exception raised: {e}')
                assert not bool(e)





    def _createDummyJobWithStatus(self, status):
        job_name = str(randrange(999999, 999999999))
        job_id = randrange(1, 999)
        job = Job(job_name, job_id, status, 0)
        job.type = randrange(0, 2)
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
    STRUCTURES_DIR = '/dummy/structure/dir'


def test_manage_dependencies():
    """
    testing function _manage_dependencies from job_list
    """
    dependencies_keys = {'dummy=1':
                             { 'test', 'test2' }
                        ,'dummy-2':
                             { 'test', 'test2' },
                        'dummy+3': "", 'dummy*4': "", 'dummy?5': ""
                    }

    experiment_id = 'random-id'

    as_conf = Mock()
    as_conf.experiment_data = dict()
    as_conf.experiment_data["JOBS"] = {}
    as_conf.jobs_data = as_conf.experiment_data["JOBS"]
    as_conf.experiment_data["PLATFORMS"] = {}

    joblist_persistence = JobListPersistencePkl()
    job_list = JobList(experiment_id, as_conf, YAMLParserFactory(),joblist_persistence)

    job = {'dummy':
               {'dummy': 'SIM.sh',
                   'RUNNING': 'once'},
            'RUNNING': 'once',
            'dummy*4':{}
    }

    dic_jobs_fake = DicJobs(['fake-date1', 'fake-date2'],
                            ['fake-member1', 'fake-member2'], list(range(2, 10 + 1)),
                            'H', 1, as_conf)
    dic_jobs_fake.experiment_data["JOBS"] = job
    dependency = job_list._manage_dependencies(dependencies_keys, dic_jobs_fake)
    assert len(dependency) == 3
    for job in dependency:
        assert job in dependencies_keys
