import pytest
import shutil
from random import randrange

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_list import JobList
from autosubmit.job.job_list_persistence import JobListPersistenceDb
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory
from autosubmitconfigparser.config.basicconfig import BasicConfig


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
    as_conf = autosubmit_config(experiment_id, {})
    as_conf.experiment_data = dict()
    as_conf.experiment_data["JOBS"] = dict()
    as_conf.experiment_data["PLATFORMS"] = dict()
    job_list = JobList(experiment_id, as_conf, YAMLParserFactory(),
                       JobListPersistenceDb(tmpdir, 'db'))
    dummy_serial_platform = mocker.MagicMock()
    dummy_serial_platform.name = 'serial'
    dummy_platform = mocker.MagicMock()
    dummy_platform.serial_platform = dummy_serial_platform
    dummy_platform.name = 'dummy_platform'

    jobs = {
        "completed": [create_dummy_job_with_status(Status.COMPLETED, dummy_platform) for _ in range(4)],
        "submitted": [create_dummy_job_with_status(Status.SUBMITTED, dummy_platform) for _ in range(3)],
        "running": [create_dummy_job_with_status(Status.RUNNING, dummy_platform) for _ in range(2)],
        "queuing": [create_dummy_job_with_status(Status.QUEUING, dummy_platform)],
        "failed": [create_dummy_job_with_status(Status.FAILED, dummy_platform) for _ in range(4)],
        "ready": [create_dummy_job_with_status(Status.READY, dummy_platform) for _ in range(3)],
        "waiting": [create_dummy_job_with_status(Status.WAITING, dummy_platform) for _ in range(2)],
        "unknown": [create_dummy_job_with_status(Status.UNKNOWN, dummy_platform)]
    }

    job_list._job_list = [job for job_list in jobs.values() for job in job_list]
    waiting_job = jobs["waiting"][0]
    waiting_job.parents.update(jobs["ready"] + jobs["completed"] + jobs["failed"] + jobs["submitted"] + jobs["running"] + jobs["queuing"])

    yield job_list, waiting_job, jobs
    shutil.rmtree(tmpdir)


def create_dummy_job_with_status(status, platform):
    job_name = str(randrange(999999, 999999999))
    job_id = randrange(1, 999)
    job = Job(job_name, job_id, status, 0)
    job.type = randrange(0, 2)
    job.platform = platform
    return job


def test_add_edge_job(setup_job_list):
    _, waiting_job, _ = setup_job_list
    special_variables = {"STATUS": Status.VALUE_TO_KEY[Status.COMPLETED], "FROM_STEP": 0}
    for p in waiting_job.parents:
        waiting_job.add_edge_info(p, special_variables)
    for parent in waiting_job.parents:
        assert waiting_job.edge_info[special_variables["STATUS"]][parent.name] == (parent, special_variables.get("FROM_STEP", 0))


def test_add_edge_info_joblist(setup_job_list):
    job_list, waiting_job, jobs = setup_job_list
    special_conditions = {"STATUS": Status.VALUE_TO_KEY[Status.COMPLETED], "FROM_STEP": 0}
    job_list._add_edges_map_info(waiting_job, special_conditions["STATUS"])
    assert len(job_list.jobs_edges.get(Status.VALUE_TO_KEY[Status.COMPLETED], [])) == 1
    job_list._add_edges_map_info(jobs["waiting"][1], special_conditions["STATUS"])
    assert len(job_list.jobs_edges.get(Status.VALUE_TO_KEY[Status.COMPLETED], [])) == 2


def test_check_special_status(setup_job_list):
    job_list, _, jobs = setup_job_list
    job_list.jobs_edges = dict()
    job_a = jobs["completed"][0]
    job_b = jobs["running"][0]
    job_c = jobs["waiting"][0]
    job_c.parents = set()
    job_c.parents.add(job_a)
    job_c.parents.add(job_b)
    # C can start when A is completed and B is running
    job_c.edge_info = {Status.VALUE_TO_KEY[Status.COMPLETED]: {job_a.name: (job_a, 0)}, Status.VALUE_TO_KEY[Status.RUNNING]: {job_b.name: (job_b, 0)}}
    special_conditions = {"STATUS": Status.VALUE_TO_KEY[Status.RUNNING], "FROM_STEP": 0}
    # Test: { A: COMPLETED, B: RUNNING }
    job_list._add_edges_map_info(job_c, special_conditions["STATUS"])
    assert job_c in job_list.check_special_status()  # This function should return the jobs that can start ( they will be put in Status.ready in the update_list funtion )
    # Test: { A: RUNNING, B: RUNNING }, A condition is default ( completed ) and B is running
    job_a.status = Status.RUNNING
    assert job_c not in job_list.check_special_status()
    # Test: { A: RUNNING, B: RUNNING }, setting B and A condition to running
    job_c.edge_info = {Status.VALUE_TO_KEY[Status.RUNNING]: {job_b.name: (job_b, 0), job_a.name: (job_a, 0)}}
    assert job_c in job_list.check_special_status()
    # Test: { A: COMPLETED, B: COMPLETED } # This should always work.
    job_a.status = Status.COMPLETED
    job_b.status = Status.COMPLETED
    assert job_c in job_list.check_special_status()
    # Test: { A: FAILED, B: COMPLETED }
    job_a.status = Status.FAILED
    job_b.status = Status.COMPLETED
    # This may change in #1316
    assert job_c in job_list.check_special_status()
