from unittest import TestCase

from pathlib import Path
import inspect
import tempfile

from mock import MagicMock
from mock import patch

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_list import JobList
from autosubmit.job.job_list_persistence import JobListPersistenceDb
from autosubmit.job.job_packages import JobPackageSimple, JobPackageVertical
from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory
import pytest
from autosubmit.job.job_packages import jobs_in_wrapper_str

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

class TestJobPackage(TestCase):

    def setUpWrappers(self,options):
        # reset
        self.as_conf = None
        self.job_package_wrapper = None
        self.experiment_id = 'random-id'
        self._wrapper_factory = MagicMock()
        self.config = FakeBasicConfig
        self.config.read = MagicMock()
        with patch.object(Path, 'exists') as mock_exists:
            mock_exists.return_value = True
            self.as_conf = AutosubmitConfig(self.experiment_id, self.config, YAMLParserFactory())
        self.as_conf.experiment_data = dict()
        self.as_conf.experiment_data["JOBS"] = dict()
        self.as_conf.experiment_data["PLATFORMS"] = dict()
        self.as_conf.experiment_data["WRAPPERS"] = dict()
        self.temp_directory = tempfile.mkdtemp()
        self.job_list = JobList(self.experiment_id, self.as_conf, YAMLParserFactory(),
                                JobListPersistenceDb(self.temp_directory, 'db'))
        self.parser_mock = MagicMock(spec='SafeConfigParser')
        for job in self.jobs:
            job._init_runtime_parameters()
        self.platform.max_waiting_jobs = 100
        self.platform.total_jobs = 100
        self.as_conf.experiment_data["WRAPPERS"]["WRAPPERS"] = options
        self._wrapper_factory.as_conf = self.as_conf
        self.jobs[0].wallclock = "00:00"
        self.jobs[0]._threads = "1"
        self.jobs[0].tasks = "1"
        self.jobs[0].exclusive = True
        self.jobs[0].queue = "debug"
        self.jobs[0].partition = "debug"
        self.jobs[0].custom_directives = "dummy_directives"
        self.jobs[0].processors = "9"
        self.jobs[0]._processors = "9"
        self.jobs[0]._platform = self.platform
        self.jobs[0].retrials = 0
        self.jobs[1].wallclock = "00:00"
        self.jobs[1]._threads = "1"
        self.jobs[1].tasks = "1"
        self.jobs[1].exclusive = True
        self.jobs[1].queue = "debug2"
        self.jobs[1].partition = "debug2"
        self.jobs[1].custom_directives = "dummy_directives2"
        self.jobs[1].processors = "9"
        self.jobs[1]._processors = "9"
        self.jobs[1]._platform = self.platform
        self.wrapper_type = options.get('TYPE', 'vertical')
        self.wrapper_policy = options.get('POLICY', 'flexible')
        self.wrapper_method = options.get('METHOD', 'ASThread')
        self.jobs_in_wrapper = options.get('JOBS_IN_WRAPPER', 'None')
        self.extensible_wallclock = options.get('EXTEND_WALLCLOCK', 0)
        self.job_package_wrapper = JobPackageVertical(self.jobs,configuration=self.as_conf,wrapper_info=[self.wrapper_type,self.wrapper_policy,self.wrapper_method,self.jobs_in_wrapper,self.extensible_wallclock])
        self.job_list._ordered_jobs_by_date_member["WRAPPERS"] = dict()

    def setUp(self):
        self.platform = MagicMock()
        self.platform.queue = "debug"
        self.platform.partition = "debug"
        self.platform.serial_platform = self.platform
        self.platform.serial_platform.max_wallclock = '24:00'
        self.platform.serial_queue = "debug-serial"
        self.platform.serial_partition = "debug-serial"
        self.jobs = [Job('dummy1', 0, Status.READY, 0),
                     Job('dummy2', 0, Status.READY, 0)]
        for job in self.jobs:
            job._init_runtime_parameters()

        self.jobs[0]._platform = self.jobs[1]._platform = self.platform
        self.job_package = JobPackageSimple(self.jobs)
    def test_default_parameters(self):
        options = {
            'TYPE': "vertical",
            'JOBS_IN_WRAPPER': "None",
            'METHOD': "ASThread",
            'POLICY': "flexible",
            'EXTEND_WALLCLOCK': 0,
        }
        self.setUpWrappers(options)
        self.assertEqual(self.job_package_wrapper.wrapper_type, "vertical")
        self.assertEqual(self.job_package_wrapper.jobs_in_wrapper, "None")
        self.assertEqual(self.job_package_wrapper.wrapper_method, "ASThread")
        self.assertEqual(self.job_package_wrapper.wrapper_policy, "flexible")
        self.assertEqual(self.job_package_wrapper.extensible_wallclock, 0)

        self.assertEqual(self.job_package_wrapper.exclusive, True)
        self.assertEqual(self.job_package_wrapper.inner_retrials, 0)
        self.assertEqual(self.job_package_wrapper.queue, "debug")
        self.assertEqual(self.job_package_wrapper.partition, "debug")
        self.assertEqual(self.job_package_wrapper._threads, "1")
        self.assertEqual(self.job_package_wrapper.tasks, "1")

        options_slurm = {
            'EXCLUSIVE': False,
            'QUEUE': "bsc32",
            'PARTITION': "bsc32",
            'THREADS': "30",
            'TASKS': "40",
            'INNER_RETRIALS': 30,
            'CUSTOM_DIRECTIVES': "['#SBATCH --mem=1000']"
        }
        self.setUpWrappers(options_slurm)
        self.assertEqual(self.job_package_wrapper.exclusive, False)
        self.assertEqual(self.job_package_wrapper.inner_retrials, 30)
        self.assertEqual(self.job_package_wrapper.queue, "bsc32")
        self.assertEqual(self.job_package_wrapper.partition, "bsc32")
        self.assertEqual(self.job_package_wrapper._threads, "30")
        self.assertEqual(self.job_package_wrapper.tasks, "40")
        self.assertEqual(self.job_package_wrapper.custom_directives, ['#SBATCH --mem=1000'])

    def test_job_package_default_init(self):
        with self.assertRaises(Exception):
            JobPackageSimple([])

    def test_job_package_different_platforms_init(self):
        self.jobs[0]._platform = MagicMock()
        self.jobs[1]._platform = MagicMock()
        with self.assertRaises(Exception):
            JobPackageSimple(self.jobs)

    def test_job_package_none_platforms_init(self):
        self.jobs[0]._platform = None
        self.jobs[1]._platform = None
        with self.assertRaises(Exception):
            JobPackageSimple(self.jobs)

    def test_job_package_length(self):
        self.assertEqual(2, len(self.job_package))

    def test_job_package_jobs_getter(self):
        self.assertEqual(self.jobs, self.job_package.jobs)

    def test_job_package_platform_getter(self):
        self.assertEqual(self.platform, self.job_package.platform)



@pytest.fixture
def mock_as_conf():
    class MockAsConf:
        experiment_data = {
            "WRAPPERS": {
                "current_wrapper": {
                    "JOBS_IN_WRAPPER": "job1 job2 job3"
                }
            }
        }
    return MockAsConf()


def test_jobs_in_wrapper_str(mock_as_conf):
    # Arrange
    current_wrapper = "current_wrapper"
    result = jobs_in_wrapper_str(mock_as_conf, current_wrapper)
    assert result == "job1_job2_job3"


def test_job_package_submission(mocker, local):
    # N.B.: AS only calls ``_create_scripts`` if you have less jobs than threads.
    # So we simply set threads to be greater than the amount of jobs.
    jobs = [Job("job1", "1", Status.READY, 0), Job("job2", "2", Status.READY, 0), Job("job3", "3", Status.READY, 0)]
    for job in jobs:
        job.platform = local

    mocker.patch('multiprocessing.cpu_count', return_value=len(jobs) + 1)
    mocker.patch("autosubmit.job.job.Job.update_parameters", return_value={})
    mocker.patch('autosubmit.job.job.Job._get_paramiko_template', return_value="empty")
    for job in jobs:
        job._tmp_path = MagicMock()
        job.file = "fake-file"
        job.custom_directives = []

    job_package = JobPackageSimple(jobs)

    job_package._create_scripts = MagicMock()
    job_package._send_files = MagicMock()
    job_package._do_submission = MagicMock()
    configuration = MagicMock()
    configuration.get_project_dir = MagicMock()
    configuration.get_project_dir.return_value = "fake-proj-dir"
    # act
    job_package.submit(configuration, 'fake-params')
    # assert
    for job in jobs:
        job.update_parameters.assert_called() # Should be called once for each job, but currently it needs two calls (for additional files ) to change the code

    job_package._create_scripts.is_called_once_with()
    job_package._send_files.is_called_once_with()
    job_package._do_submission.is_called_once_with()
