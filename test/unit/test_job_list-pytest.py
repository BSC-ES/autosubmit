import pytest

from autosubmit.job.job_common import Status
from autosubmit.job.job_list_persistence import JobListPersistencePkl
from autosubmit.job.job_list import JobList
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory
from autosubmit.job.job import Job


@pytest.fixture
def prepare_basic_config(tmpdir):
    basic_conf = BasicConfig()
    BasicConfig.DB_DIR = (tmpdir / "exp_root")
    BasicConfig.DB_FILE = "debug.db"
    BasicConfig.LOCAL_ROOT_DIR = (tmpdir / "exp_root")
    BasicConfig.LOCAL_TMP_DIR = "tmp"
    BasicConfig.LOCAL_ASLOG_DIR = "ASLOGS"
    BasicConfig.LOCAL_PROJ_DIR = "proj"
    BasicConfig.DEFAULT_PLATFORMS_CONF = ""
    BasicConfig.CUSTOM_PLATFORMS_PATH = ""
    BasicConfig.DEFAULT_JOBS_CONF = ""
    BasicConfig.SMTP_SERVER = ""
    BasicConfig.MAIL_FROM = ""
    BasicConfig.ALLOWED_HOSTS = ""
    BasicConfig.DENIED_HOSTS = ""
    BasicConfig.CONFIG_FILE_FOUND = False
    return basic_conf


@pytest.fixture(scope='function')
def setup_job_list(autosubmit_config, tmpdir, mocker, prepare_basic_config):
    experiment_id = 'random-id'
    as_conf = autosubmit_config
    as_conf.experiment_data = dict()
    as_conf.experiment_data["JOBS"] = dict()
    as_conf.jobs_data = as_conf.experiment_data["JOBS"]
    as_conf.experiment_data["PLATFORMS"] = dict()
    job_list = JobList(experiment_id, as_conf, YAMLParserFactory(),
                       JobListPersistencePkl())
    dummy_serial_platform = mocker.MagicMock()
    dummy_serial_platform.name = 'serial'
    dummy_platform = mocker.MagicMock()
    dummy_platform.serial_platform = dummy_serial_platform
    dummy_platform.name = 'dummy_platform'

    job_list._platforms = [dummy_platform]
    # add some jobs to the job list
    job = Job("job1", "1", Status.COMPLETED, 0)
    job.section = "SECTION1"
    job_list._job_list.append(job)
    job = Job("job2", "2", Status.WAITING, 0)
    job.section = "SECTION1"
    job_list._job_list.append(job)
    job = Job("job3", "3", Status.COMPLETED, 0)
    job.section = "SECTION2"
    job_list._job_list.append(job)
    return job_list


@pytest.mark.parametrize(
    "section_list, banned_jobs, get_only_non_completed, expected_length, expected_section",
    [
        (["SECTION1"], [], False, 2, "SECTION1"),
        (["SECTION2"], [], False, 1, "SECTION2"),
        (["SECTION1"], [], True, 1, "SECTION1"),
        (["SECTION2"], [], True, 0, "SECTION2"),
        (["SECTION1"], ["job1"], True, 1, "SECTION1"),
    ],
    ids=[
        "all_jobs_in_section1",
        "all_jobs_in_section2",
        "non_completed_jobs_in_section1",
        "non_completed_jobs_in_section2",
        "ban_job1"
    ]
)
def test_get_jobs_by_section(setup_job_list, section_list, banned_jobs, get_only_non_completed, expected_length, expected_section):
    result = setup_job_list.get_jobs_by_section(section_list, banned_jobs, get_only_non_completed)
    assert len(result) == expected_length
    assert all(job.section == expected_section for job in result)
