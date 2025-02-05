import inspect
from tempfile import TemporaryDirectory
from unittest.mock import patch

from autosubmit.autosubmit import Autosubmit
from autosubmitconfigparser.config.basicconfig import BasicConfig

from unittest.mock import MagicMock

from unittest import TestCase
import tempfile
from mock import Mock, patch
from random import randrange
from pathlib import Path
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_list import JobList
from autosubmit.job.job_list_persistence import JobListPersistencePkl
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory

"""
This file contains the test for the `autosubmit stop` command. Found in /autosubmit.py line 6079.

positional arguments:
  expid                 experiment identifier, stops the listed expids
                        separated by ","

optional arguments:
  -h, --help            show this help message and exit
  -f, --force           Forces to stop autosubmit process, equivalent to kill
                        -9
  -a, --all             Stop all current running autosubmit processes, will
                        ask for confirmation
  -fa, --force_all      Stop all current running autosubmit processes
  -c, --cancel          Orders to the schedulers to stop active jobs.
  -fs FILTER_STATUS, --filter_status FILTER_STATUS
                        Select the status (one or more) to filter the list of
                        jobs.
  -t STATUS, --target STATUS
                        Final status of killed jobs. Default is FAILED.
"""

class TestStop(TestCase):

    def setUp(self):
        self.experiment_id = 'random-id'
        self.autosubmit = Autosubmit()
        self.original_root_dir = BasicConfig.LOCAL_ROOT_DIR
        self.root_dir = TemporaryDirectory()
        BasicConfig.LOCAL_ROOT_DIR = self.root_dir.name
        self.exp_path = Path(self.root_dir.name, 'a000')
        self.tmp_dir = self.exp_path / BasicConfig.LOCAL_TMP_DIR
        self.aslogs_dir = self.tmp_dir / BasicConfig.LOCAL_ASLOG_DIR
        self.status_path = self.exp_path / 'status'
        self.aslogs_dir.mkdir(parents=True)
        self.status_path.mkdir()
        self.experiment_id = 'random-id'
        self.temp_directory = tempfile.mkdtemp()
        joblist_persistence = JobListPersistencePkl()
        self.as_conf = Mock()
        self.as_conf.experiment_data = dict()
        self.as_conf.experiment_data["JOBS"] = dict()
        self.as_conf.jobs_data = self.as_conf.experiment_data["JOBS"]
        self.as_conf.experiment_data["PLATFORMS"] = dict()
        self.temp_directory = tempfile.mkdtemp()
        joblist_persistence = JobListPersistencePkl()

        self.job_list = JobList(self.experiment_id, FakeBasicConfig, YAMLParserFactory(),joblist_persistence, self.as_conf)
        # creating jobs for self list
        self.completed_job = self._createDummyJobWithStatus(Status.COMPLETED)
        self.submitted_job = self._createDummyJobWithStatus(Status.SUBMITTED)
        self.running_job = self._createDummyJobWithStatus(Status.RUNNING)
        self.queuing_job = self._createDummyJobWithStatus(Status.QUEUING)
        self.failed_job = self._createDummyJobWithStatus(Status.FAILED)
        self.ready_job = self._createDummyJobWithStatus(Status.READY)
        self.waiting_job = self._createDummyJobWithStatus(Status.WAITING)

        self.job_list._job_list = [self.running_job, self.queuing_job]

    def _createDummyJobWithStatus(self, status):
        job_name = str(randrange(999999, 999999999))
        job_id = randrange(1, 999)
        job = Job(job_name, job_id, status, 0)
        job.type = randrange(0, 2)
        return job

    def test_stop_expids(self):
        # mock
        fake_running_process = MagicMock()# process id of the experiment, to mock the process id of the experiment
        fake_running_process.communicate.return_value = (b'bla 0001 bla bla bla', b'')
        fake_running_expid = 'a000,a001' # experiment id of the experiment, to mock the experiment id of the experiment
        with patch('subprocess.Popen', return_value=fake_running_process) as mock_popen:
            with patch('os.kill') as mock_kill:
                mock_job_list = MagicMock()
                mock_job_list.return_value = self.job_list
                with patch('autosubmit.autosubmit.Autosubmit.load_job_list', return_value=mock_job_list):
                    self.autosubmit.stop(fake_running_expid,force=True,all=False,force_all=False,cancel=False,current_status="RUNNING",status="FAILED")

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

