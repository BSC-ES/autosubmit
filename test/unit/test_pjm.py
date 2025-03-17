from unittest import TestCase
from unittest.mock import Mock, MagicMock, patch
from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory
from autosubmit.autosubmit import Autosubmit
import autosubmit.platforms.pjmplatform
import pytest

from pathlib import Path
from autosubmit.platforms.platform import Platform
from autosubmit.platforms.pjmplatform import PJMPlatform
import autosubmit.platforms.headers.pjm_header
from tempfile import TemporaryDirectory
from datetime import datetime
from autosubmit.job.job import Job, Status
import inspect


class FakeBasicConfig:
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
    LOCAL_ASLOG_DIR = '/dummy/local/aslog/dir'
    DEFAULT_PLATFORMS_CONF = ''
    DEFAULT_JOBS_CONF = ''

    @staticmethod
    def read():
        return


class TestPJM(TestCase):

    def setUp(self) -> None:
        self.exp_id = 'a000'
        self.as_conf = MagicMock()
        with patch.object(Path, 'exists') as mock_exists:
            mock_exists.return_value = True
            self.as_conf = AutosubmitConfig(self.exp_id, FakeBasicConfig, YAMLParserFactory())
        self.as_conf.experiment_data = dict()
        self.as_conf.experiment_data["DEFAULT"] = dict()
        self.as_conf.experiment_data["DEFAULT"]["HPCARCH"] = "ARM"
        self.as_conf.experiment_data.update(FakeBasicConfig().props())

        yml_file = Path(__file__).resolve().parent / "files/fake-jobs.yml"
        factory = YAMLParserFactory()
        parser = factory.create_parser()
        parser.data = parser.load(yml_file)
        self.as_conf.experiment_data.update(parser.data)
        yml_file = Path(__file__).resolve().parent / "files/fake-platforms.yml"
        factory = YAMLParserFactory()
        parser = factory.create_parser()
        parser.data = parser.load(yml_file)
        self.as_conf.experiment_data.update(parser.data)
        self.setUp_pjm()

    @patch("builtins.open", MagicMock())
    def setUp_pjm(self):
        MagicMock().write = MagicMock()
        MagicMock().os.path.join = MagicMock()
        self.section = 'ARM'
        self.submitted_ok = "[INFO] PJM 0000 pjsub Job 167661 submitted."
        self.submitted_fail = "[ERR.] PJM 0057 pjsub node=32 is greater than the upper limit (24)."
        self.out = """JOB_ID     ST  REASON                         
167727     EXT COMPLETED               
167728     RNO -               
167729     RNE -               
167730     RUN -               
167732     ACC -               
167733     QUE -               
167734     RNA -               
167735     RNP -               
167736     HLD ASHOLD               
167737     ERR -               
167738     CCL -               
167739     RJT -  
"""
        self.completed_jobs = ["167727"]
        self.running_jobs = ["167728", "167729", "167730"]
        self.queued_jobs = ["167732", "167733", "167734", "167735", "167736"]
        self.failed_jobs = ["167737", "167738", "167739"]
        self.jobs_that_arent_listed = ["3442432423", "238472364782", "1728362138712"]
        self.completed_jobs_cmd = "167727"
        self.running_jobs_cmd = "167728+167729+167730"
        self.queued_jobs_cmd = "167732+167733+167734+167735+167736"
        self.failed_jobs_cmd = "167737+167738+167739"
        self.jobs_that_arent_listed_cmd = "3442432423+238472364782+1728362138712"
        self.submitter = Autosubmit._get_submitter(self.as_conf)
        self.submitter.load_platforms(self.as_conf)
        self.remote_platform = self.submitter.platforms[self.section]

    def test_parse_Alljobs_output(self):
        """Test parsing of all jobs output."""
        for job_id in self.completed_jobs:
            assert self.remote_platform.parse_Alljobs_output(self.out, job_id) in self.remote_platform.job_status[
                "COMPLETED"]
        for job_id in self.failed_jobs:
            assert self.remote_platform.parse_Alljobs_output(self.out, job_id) in self.remote_platform.job_status[
                "FAILED"]
        for job_id in self.queued_jobs:
            assert self.remote_platform.parse_Alljobs_output(self.out, job_id) in self.remote_platform.job_status[
                "QUEUING"]
        for job_id in self.running_jobs:
            assert self.remote_platform.parse_Alljobs_output(self.out, job_id) in self.remote_platform.job_status[
                "RUNNING"]
        for job_id in self.jobs_that_arent_listed:
            assert self.remote_platform.parse_Alljobs_output(self.out, job_id) == []

    def test_get_submitted_job_id(self):
        """Test parsing of submitted job id."""
        output = self.remote_platform.get_submitted_job_id(self.submitted_ok)
        assert output == [167661]
        output = self.remote_platform.get_submitted_job_id(self.submitted_fail)
        assert output == []

    def test_parse_queue_reason(self):
        """Test parsing of queue reason."""
        output = self.remote_platform.parse_queue_reason(self.out, self.completed_jobs[0])
        assert output == "COMPLETED"
