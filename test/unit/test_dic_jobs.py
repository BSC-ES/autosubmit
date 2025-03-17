from bscearth.utils.date import date2str

from datetime import datetime
from unittest import TestCase

from mock import Mock
import math
import shutil
import tempfile
import inspect

from autosubmit.job.job import Job
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory
from autosubmit.job.job_common import Status
from autosubmit.job.job_common import Type
from autosubmit.job.job_dict import DicJobs
from autosubmit.job.job_list import JobList
from autosubmit.job.job_list_persistence import JobListPersistenceDb
from unittest.mock import patch


class TestDicJobs(TestCase):
    def setUp(self):
        self.experiment_id = 'random-id'
        self.as_conf = Mock()

        self.as_conf.experiment_data = dict()
        self.as_conf.experiment_data["DEFAULT"] = {}
        self.as_conf.experiment_data["DEFAULT"]["EXPID"] = self.experiment_id
        self.as_conf.experiment_data["JOBS"] = dict()
        self.as_conf.jobs_data = self.as_conf.experiment_data["JOBS"]
        self.as_conf.experiment_data["PLATFORMS"] = dict()
        self.temp_directory = tempfile.mkdtemp()
        self.job_list = JobList(self.experiment_id, self.as_conf, YAMLParserFactory(),
                                JobListPersistenceDb(self.temp_directory, 'db'))
        self.parser_mock = Mock(spec='SafeConfigParser')
        self.date_list = ['fake-date1', 'fake-date2']
        self.member_list = ["fc1", "fc2", "fc3", "fc4", "fc5", "fc6", "fc7", "fc8", "fc9", "fc10"]
        self.member_list = ['fake-member1', 'fake-member2']
        self.num_chunks = 99
        self.chunk_list = list(range(1, self.num_chunks + 1))
        self.date_format = 'H'
        self.default_retrials = 999
        self.dictionary = DicJobs(self.date_list, self.member_list, self.chunk_list, self.date_format, default_retrials=self.default_retrials,as_conf=self.as_conf)
        self.dictionary.changes = {}

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_directory)
    @patch('autosubmit.job.job_dict.date2str')
    def test_read_section_running_once_create_jobs_once(self, mock_date2str):
        # arrange
        mock_date2str.side_effect = lambda x, y: str(x)
        self.dictionary.compare_section = Mock()

        section = 'fake-section'
        priority = 999
        frequency = 123
        splits = -1
        running= "once"
        options = {
            'FREQUENCY': frequency,
            'PRIORITY': priority,
            'SPLITS': splits,
            'EXCLUDED_LIST_C': [],
            'EXCLUDED_LIST_M': [],
            'RUNNING': running
        }


        self.dictionary.experiment_data = dict()
        self.dictionary.experiment_data["JOBS"] = {}
        self.dictionary.experiment_data["JOBS"][section] = options

        self.dictionary._create_jobs_once = Mock()
        self.dictionary._create_jobs_startdate = Mock()
        self.dictionary._create_jobs_member = Mock()
        self.dictionary._create_jobs_chunk = Mock()
        self.dictionary.compare_section = Mock()

        # act
        self.dictionary.read_section(section, priority, Type.BASH)

        # assert
        self.dictionary._create_jobs_once.assert_called_once_with(section, priority, Type.BASH,splits)
        self.dictionary._create_jobs_startdate.assert_not_called()
        self.dictionary._create_jobs_member.assert_not_called()
        self.dictionary._create_jobs_chunk.assert_not_called()

    @patch('autosubmit.job.job_dict.date2str')
    def test_read_section_running_date_create_jobs_startdate(self, mock_date2str):
        # arrange
        mock_date2str.side_effect = lambda x, y: str(x)
        self.dictionary.compare_section = Mock()

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
        self.dictionary.experiment_data = dict()
        self.dictionary.experiment_data["JOBS"] = {}
        self.dictionary.experiment_data["JOBS"][section] = options
        self.dictionary._create_jobs_once = Mock()
        self.dictionary._create_jobs_startdate = Mock()
        self.dictionary._create_jobs_member = Mock()
        self.dictionary._create_jobs_chunk = Mock()

        # act
        self.dictionary.read_section(section, priority, Type.BASH)

        # assert
        self.dictionary._create_jobs_once.assert_not_called()
        self.dictionary._create_jobs_startdate.assert_called_once_with(section, priority, frequency, Type.BASH, splits)
        self.dictionary._create_jobs_member.assert_not_called()
        self.dictionary._create_jobs_chunk.assert_not_called()

    @patch('autosubmit.job.job_dict.date2str')
    def test_read_section_running_member_create_jobs_member(self, mock_date2str):
        mock_date2str.side_effect = lambda x, y: str(x)
        self.dictionary.compare_section = Mock()

        # arrange
        section = 'fake-section'
        priority = 999
        frequency = 123
        splits = 0
        excluded_list_m = []
        running = "member"
        options = {
            'FREQUENCY': frequency,
            'PRIORITY': priority,
            'SPLITS': splits,
            'EXCLUDED_LIST_C': [],
            'EXCLUDED_LIST_M': [],
            'RUNNING': running
        }

        self.dictionary.experiment_data = dict()
        self.dictionary.experiment_data["JOBS"] = {}
        self.dictionary.experiment_data["JOBS"][section] = options

        self.dictionary._create_jobs_once = Mock()
        self.dictionary._create_jobs_startdate = Mock()
        self.dictionary._create_jobs_member = Mock()
        self.dictionary._create_jobs_chunk = Mock()

        # act
        self.dictionary.read_section(section, priority, Type.BASH)

        # assert
        self.dictionary._create_jobs_once.assert_not_called()
        self.dictionary._create_jobs_startdate.assert_not_called()
        self.dictionary._create_jobs_member.assert_called_once_with(section, priority, frequency, Type.BASH,splits)
        self.dictionary._create_jobs_chunk.assert_not_called()

    @patch('autosubmit.job.job_dict.date2str')
    def test_read_section_running_chunk_create_jobs_chunk(self, mock_date2str):
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

        self.dictionary.experiment_data = dict()
        self.dictionary.experiment_data["JOBS"] = {}
        self.dictionary.experiment_data["JOBS"][section] = options
        self.dictionary._create_jobs_once = Mock()
        self.dictionary._create_jobs_startdate = Mock()
        self.dictionary._create_jobs_member = Mock()
        self.dictionary._create_jobs_chunk = Mock()
        self.dictionary.compare_section = Mock()
        # act
        self.dictionary.read_section(section, options["PRIORITY"], Type.BASH)

        # assert
        self.dictionary._create_jobs_once.assert_not_called()
        self.dictionary._create_jobs_startdate.assert_not_called()
        self.dictionary._create_jobs_member.assert_not_called()
        self.dictionary._create_jobs_chunk.assert_called_once_with(section, options["PRIORITY"], options["FREQUENCY"], Type.BASH, options["SYNCHRONIZE"], options["DELAY"], options["SPLITS"])

    @patch('autosubmit.job.job_dict.date2str')
    def test_build_job_with_existent_job_list_status(self,mock_date2str):
        # arrange
        job_list = [ Job("random-id_fake-date_fc0_2_fake-section", 1, Status.READY, 0), Job("random-id_fake-date_fc0_2_fake-section2", 2, Status.RUNNING, 0)]
        self.dictionary.job_list = {}
        for job in job_list:
            self.dictionary.job_list[job.name] = job.__getstate__()
        mock_date2str.side_effect = lambda x, y: str(x)
        section = 'fake-section'
        priority = 0
        date = "fake-date"
        member = 'fc0'
        chunk = 2
        # act
        section_data = []
        self.dictionary.build_job(section, priority, date, member, chunk,Type.BASH,section_data,splits=1)
        section = 'fake-section2'
        self.dictionary.build_job(section, priority, date, member, chunk, Type.BASH,section_data,splits=1)
        # assert
        self.assertEqual(Status.WAITING, section_data[0].status)
        self.assertEqual(Status.RUNNING, section_data[1].status)

    @patch('autosubmit.job.job_dict.date2str')
    def test_dic_creates_right_jobs_by_startdate(self, mock_date2str):
        # arrange
        mock_date2str.side_effect = lambda x, y: str(x)

        mock_section = Mock()
        mock_section.name = 'fake-section'
        priority = 999
        frequency = 1
        self.dictionary.build_job = Mock(wraps=self.dictionary.build_job)
        # act
        self.dictionary._create_jobs_startdate(mock_section.name, priority, frequency, Type.BASH)

        # assert
        self.assertEqual(len(self.date_list), self.dictionary.build_job.call_count)
        self.assertEqual(len(self.dictionary._dic[mock_section.name]), len(self.date_list))
        for date in self.date_list:
            self.assertEqual(self.dictionary._dic[mock_section.name][date][0].name, f'{self.experiment_id}_{date}_{mock_section.name}')
    @patch('autosubmit.job.job_dict.date2str')
    def test_dic_creates_right_jobs_by_member(self, mock_date2str):
        # arrange
        mock_section = Mock()
        mock_date2str.side_effect = lambda x, y: str(x)
        mock_section.name = 'fake-section'
        priority = 999
        frequency = 1
        self.dictionary.build_job = Mock(wraps=self.dictionary.build_job)

        # act
        self.dictionary._create_jobs_member(mock_section.name, priority, frequency, Type.BASH)

        # assert
        self.assertEqual(len(self.date_list) * len(self.member_list), self.dictionary.build_job.call_count)
        self.assertEqual(len(self.dictionary._dic[mock_section.name]), len(self.date_list))
        for date in self.date_list:
            for member in self.member_list:
                self.assertEqual(self.dictionary._dic[mock_section.name][date][member][0].name, f'{self.experiment_id}_{date}_{member}_{mock_section.name}')

    def test_dic_creates_right_jobs_by_chunk(self):
        # arrange
        mock_section = Mock()
        mock_section.name = 'fake-section'
        priority = 999
        frequency = 1
        self.dictionary.build_job = Mock(return_value=mock_section)

    def test_dic_creates_right_jobs_by_chunk_with_frequency_3(self):
        # arrange
        mock_section = Mock()
        mock_section.name = 'fake-section'
        priority = 999
        frequency = 3
        self.dictionary.build_job = Mock(return_value=mock_section)

        # act
        self.dictionary._create_jobs_chunk(mock_section.name, priority, frequency, Type.BASH)

        # assert
        self.assertEqual(len(self.date_list) * len(self.member_list) * (len(self.chunk_list) / frequency),
                          self.dictionary.build_job.call_count)
        self.assertEqual(len(self.dictionary._dic[mock_section.name]), len(self.date_list))

    def test_dic_creates_right_jobs_by_chunk_with_frequency_4(self):
        # arrange
        mock_section = Mock()
        mock_section.name = 'fake-section'
        priority = 999
        frequency = 4
        self.dictionary.build_job = Mock(return_value=mock_section)

        # act
        self.dictionary._create_jobs_chunk(mock_section.name, priority, frequency, Type.BASH)

        # assert
        # you have to multiply to the round upwards (ceil) of the next division
        self.assertEqual(
            len(self.date_list) * len(self.member_list) * math.ceil(len(self.chunk_list) / float(frequency)),
            self.dictionary.build_job.call_count)
        self.assertEqual(len(self.dictionary._dic[mock_section.name]), len(self.date_list))


    def test_dic_creates_right_jobs_by_chunk_with_date_synchronize(self):
        # arrange
        mock_section = Mock()
        mock_section.name = 'fake-section'
        priority = 999
        frequency = 1
        created_job = 'created_job'
        self.dictionary.build_job = Mock(wraps=self.dictionary.build_job)

        # act
        self.dictionary._create_jobs_chunk(mock_section.name, priority, frequency, Type.BASH, 'date')

        # assert
        self.assertEqual(len(self.chunk_list), self.dictionary.build_job.call_count)
        self.assertEqual(len(self.dictionary._dic[mock_section.name]), len(self.date_list))
        for date in self.date_list:
            for member in self.member_list:
                for chunk in self.chunk_list:
                    self.assertEqual(self.dictionary._dic[mock_section.name][date][member][chunk][0].name, f'{self.experiment_id}_{chunk}_{mock_section.name}')

    def test_dic_creates_right_jobs_by_chunk_with_date_synchronize_and_frequency_4(self):
        # arrange
        mock_section = Mock()
        mock_section.name = 'fake-section'
        priority = 999
        frequency = 4
        self.dictionary.build_job = Mock(return_value=mock_section)

        # act
        self.dictionary._create_jobs_chunk(mock_section.name, priority, frequency, Type.BASH, 'date')

        # assert
        self.assertEqual(math.ceil(len(self.chunk_list) / float(frequency)),
                          self.dictionary.build_job.call_count)
        self.assertEqual(len(self.dictionary._dic[mock_section.name]), len(self.date_list))
    @patch('autosubmit.job.job_dict.date2str')
    def test_dic_creates_right_jobs_by_chunk_with_member_synchronize(self, mock_date2str):
        # patch date2str
        mock_date2str.side_effect = lambda x, y: str(x)
        # arrange
        mock_section = Mock()
        mock_section.name = 'fake-section'
        priority = 999
        frequency = 1
        self.dictionary.build_job = Mock(wraps=self.dictionary.build_job)

        # act
        self.dictionary._create_jobs_chunk(mock_section.name, priority, frequency, Type.BASH, 'member')

        # assert
        self.assertEqual(len(self.date_list) * len(self.chunk_list),
                          self.dictionary.build_job.call_count)
        self.assertEqual(len(self.dictionary._dic[mock_section.name]), len(self.date_list))
        for date in self.date_list:
            for member in self.member_list:
                for chunk in self.chunk_list:
                    self.assertEqual(self.dictionary._dic[mock_section.name][date][member][chunk][0].name, f'{self.experiment_id}_{date}_{chunk}_{mock_section.name}')

    def test_dic_creates_right_jobs_by_chunk_with_member_synchronize_and_frequency_4(self):
        # arrange
        mock_section = Mock()
        mock_section.name = 'fake-section'
        priority = 999
        frequency = 4
        self.dictionary.build_job = Mock(return_value=mock_section)

        # act
        self.dictionary._create_jobs_chunk(mock_section.name, priority, frequency, Type.BASH, 'member')

        # assert
        self.assertEqual(len(self.date_list) * math.ceil(len(self.chunk_list) / float(frequency)),
                          self.dictionary.build_job.call_count)
        self.assertEqual(len(self.dictionary._dic[mock_section.name]), len(self.date_list))

    def test_create_job_creates_a_job_with_right_parameters(self):
        section = 'test'
        priority = 99
        date = datetime(2016, 1, 1)
        member = 'fc0'
        chunk = 'ch0'
        # arrange

        self.dictionary.experiment_data = dict()
        self.dictionary.experiment_data["DEFAULT"] =  dict()
        self.dictionary.experiment_data["DEFAULT"]["EXPID"] = "random-id"
        self.dictionary.experiment_data["JOBS"] = {}
        self.dictionary.experiment_data["PLATFORMS"] = {}
        self.dictionary.experiment_data["CONFIG"] = {}
        self.dictionary.experiment_data["PLATFORMS"]["FAKE-PLATFORM"] = {}
        job_list_mock = Mock()
        job_list_mock.append = Mock()

        # act
        section_data = []
        self.dictionary.build_job(section, priority, date, member, chunk, 'bash', section_data )
        created_job = section_data[0]
        #assert
        self.assertEqual('random-id_2016010100_fc0_ch0_test', created_job.name)
        self.assertEqual(Status.WAITING, created_job.status)
        self.assertEqual(priority, created_job.priority)
        self.assertEqual(section, created_job.section)
        self.assertEqual(date, created_job.date)
        self.assertEqual(member, created_job.member)
        self.assertEqual(chunk, created_job.chunk)
        self.assertEqual(self.date_format, created_job.date_format)
        #self.assertTrue(created_job.wait)
        self.assertEqual(Type.BASH, created_job.type)
        self.assertEqual(None, created_job.executable)
        self.assertTrue(created_job.check)
        self.assertEqual(0, created_job.retrials)

    def test_get_member_returns_the_jobs_if_no_member(self):
        # arrange
        jobs = 'fake-jobs'
        dic = {'any-key': 'any-value'}

        # act
        returned_jobs = self.dictionary._get_member(jobs, dic, 'fake-member', None)  # expected jobs

        # arrange
        self.assertEqual(jobs, returned_jobs)

    def test_get_member_returns_the_jobs_with_the_member(self):
        # arrange
        jobs = ['fake-job']
        dic = {'fake-job2': 'any-value'}
        member = 'fake-job2'

        # act
        returned_jobs = self.dictionary._get_member(jobs, dic, member, None)

        # arrange
        self.assertEqual(['fake-job'] + ['any-value'], returned_jobs)  # expected jobs + member

    def test_get_member_returns_the_jobs_with_the_given_chunk_of_the_member(self):
        # arrange
        jobs = ['fake-job']
        dic = {'fake-job2': {'fake-job3': 'fake'}}
        member = 'fake-job2'

        # act
        returned_jobs = self.dictionary._get_member(jobs, dic, member, 'fake-job3')

        # arrange
        self.assertEqual(['fake-job'] + ['fake'], returned_jobs)  # expected jobs + chunk

    def test_get_member_returns_the_jobs_with_all_the_chunks_of_the_member(self):
        # arrange
        jobs = ['fake-job']
        dic = {'fake-job2': {5: 'fake5', 8: 'fake8', 9: 'fake9'}}
        member = 'fake-job2'

        # act
        returned_jobs = self.dictionary._get_member(jobs, dic, member, None)

        # arrange
        self.assertEqual(['fake-job'] + ['fake5', 'fake8', 'fake9'], returned_jobs)  # expected jobs + all chunks

    def test_get_date_returns_the_jobs_if_no_date(self):
        # arrange
        jobs = 'fake-jobs'
        dic = {'any-key': 'any-value'}

        # act
        returned_jobs = self.dictionary._get_date(jobs, dic, 'whatever', None, None)

        # arrange
        self.assertEqual('fake-jobs', returned_jobs)  # expected jobs

    def test_get_date_returns_the_jobs_with_the_date(self):
        # arrange
        jobs = ['fake-job']
        dic = {'fake-job2': 'any-value'}
        date = 'fake-job2'

        # act
        returned_jobs = self.dictionary._get_date(jobs, dic, date, None, None)

        # arrange
        self.assertEqual(['fake-job'] + ['any-value'], returned_jobs)  # expected jobs + date

    def test_get_date_returns_the_jobs_and_calls_get_member_once_with_the_given_member(self):
        # arrange
        jobs = ['fake-job']
        date_dic = {'fake-job3': 'fake'}
        dic = {'fake-job2': date_dic}
        date = 'fake-job2'
        member = 'fake-member'
        chunk = 'fake-chunk'
        self.dictionary._get_member = Mock()

        # act
        returned_jobs = self.dictionary._get_date(jobs, dic, date, member, chunk)

        # arrange
        self.assertEqual(['fake-job'], returned_jobs)
        self.dictionary._get_member.assert_called_once_with(jobs, date_dic, member, chunk)

    def test_get_date_returns_the_jobs_and_calls_get_member_for_all_its_members(self):
        # arrange
        jobs = ['fake-job']
        date_dic = {'fake-job3': 'fake'}
        dic = {'fake-job2': date_dic}
        date = 'fake-job2'
        chunk = 'fake-chunk'
        self.dictionary._get_member = Mock()

        # act
        returned_jobs = self.dictionary._get_date(jobs, dic, date, None, chunk)

        # arrange
        self.assertEqual(['fake-job'], returned_jobs)
        self.assertEqual(len(self.dictionary._member_list), self.dictionary._get_member.call_count)
        for member in self.dictionary._member_list:
            self.dictionary._get_member.assert_any_call(jobs, date_dic, member, chunk)

    def test_get_jobs_returns_the_job_of_the_section(self):
        # arrange
        section = 'fake-section'
        self.dictionary._dic = {'fake-section': 'fake-job'}

        # act
        returned_jobs = self.dictionary.get_jobs(section)

        # arrange
        self.assertEqual(['fake-job'], returned_jobs)

    def test_get_jobs_calls_get_date_with_given_date(self):
        # arrange
        section = 'fake-section'
        dic = {'fake-job3': 'fake'}
        date = 'fake-date'
        member = 'fake-member'
        chunk = 111
        self.dictionary._dic = {'fake-section': dic}
        self.dictionary._get_date = Mock()

        # act
        returned_jobs = self.dictionary.get_jobs(section, date, member, chunk)

        # arrange
        self.assertEqual(list(), returned_jobs)
        self.dictionary._get_date.assert_called_once_with(list(), dic, date, member, chunk)

    def test_get_jobs_calls_get_date_for_all_its_dates(self):
        # arrange
        section = 'fake-section'
        dic = {'fake-job3': 'fake'}
        member = 'fake-member'
        chunk = 111
        self.dictionary._dic = {'fake-section': dic}
        self.dictionary._get_date = Mock()

        # act
        returned_jobs = self.dictionary.get_jobs(section, member=member, chunk=chunk)

        # arrange
        self.assertEqual(list(), returned_jobs)
        self.assertEqual(len(self.dictionary._date_list), self.dictionary._get_date.call_count)
        for date in self.dictionary._date_list:
            self.dictionary._get_date.assert_any_call(list(), dic, date, member, chunk)

    def test_job_list_returns_the_job_list_by_name(self):
        # act
        job_list = [ Job("child", 1, Status.WAITING, 0), Job("child2", 1, Status.WAITING, 0)]
        self.dictionary.job_list = job_list
        # arrange
        self.assertEqual({'child': job_list[0], 'child2': job_list[1]}, self.dictionary.job_list)

    @patch('autosubmit.job.job_dict.date2str')
    def test_create_jobs_split(self,mock_date2str):
        mock_date2str.side_effect = lambda x, y: str(x)
        section_data = []
        self.dictionary._create_jobs_split(5,'fake-section','fake-date', 'fake-member', 'fake-chunk', 0,Type.BASH, section_data)
        self.assertEqual(5, len(section_data))

    @patch('autosubmit.job.job_dict.date2str')
    def test_create_jobs_split(self,mock_date2str):
        mock_date2str.side_effect = lambda x, y: str(x)
        section_data = []
        self.dictionary._create_jobs_split(5,'fake-section','fake-date', 'fake-member', 'fake-chunk', 0,Type.BASH, section_data)
        self.assertEqual(5, len(section_data))




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
