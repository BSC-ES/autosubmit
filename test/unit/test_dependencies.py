from datetime import datetime

import mock
import pytest
from networkx import DiGraph  # type: ignore
from typing_extensions import Tuple

from autosubmit.config.yamlparser import YAMLParserFactory
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_dict import DicJobs
from autosubmit.job.job_list import JobList
from autosubmit.job.job_utils import Dependency

_MEMBER_LIST = ["fc1", "fc2", "fc3", "fc4", "fc5", "fc6", "fc7", "fc8", "fc9", "fc10"]
_CHUNK_LIST = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
_SPLIT_LIST = [1, 2, 3, 4, 5]
_DATE_LIST = ["20020201", "20020202", "20020203", "20020204", "20020205", "20020206", "20020207",
              "20020208", "20020209", "20020210"]

_EXPID = 'a000'


@pytest.fixture(autouse=True)
def as_conf(autosubmit_config):
    return autosubmit_config(_EXPID, experiment_data={})


@pytest.fixture
def relationships_dates():
    return {
        "DATES_FROM": {
            "20020201": {
                "MEMBERS_FROM": {
                    "fc2": {
                        "DATES_TO": "[20020201:20020202]*,20020203",
                        "MEMBERS_TO": "fc2",
                        "CHUNKS_TO": "all"
                    }
                },
                "SPLITS_FROM": {
                    "ALL": {
                        "SPLITS_TO": "1"
                    }
                }
            }
        }
    }


@pytest.fixture
def relationships_members():
    return {
        "MEMBERS_FROM": {
            "fc2": {
                "SPLITS_FROM": {
                    "ALL": {
                        "DATES_TO": "20020201",
                        "MEMBERS_TO": "fc2",
                        "CHUNKS_TO": "all",
                        "SPLITS_TO": "1"
                    }
                }
            }
        }
    }


@pytest.fixture
def relationships_chunks():
    return {
        "CHUNKS_FROM": {
            "1": {
                "DATES_TO": "20020201",
                "MEMBERS_TO": "fc2",
                "CHUNKS_TO": "all",
                "SPLITS_TO": "1"
            }
        }
    }


@pytest.fixture
def relationships_splits():
    return {
        "SPLITS_FROM": {
            "1": {
                "DATES_TO": "20020201",
                "MEMBERS_TO": "fc2",
                "CHUNKS_TO": "all",
                "SPLITS_TO": "1"
            }
        }
    }


@pytest.fixture
def relationships_general():
    return {
        "DATES_TO": "20020201",
        "MEMBERS_TO": "fc2",
        "CHUNKS_TO": "all",
        "SPLITS_TO": "1"
    }


@pytest.fixture
def test_job(mocker) -> Job:
    # Create a mock Job object
    job = mocker.Mock(wraps=Job)

    # Set the attributes on the mock object
    job.name = "Job1"
    job.job_id = 1
    job.status = Status.READY
    job.priority = 1
    job.date = None
    job.member = None
    job.chunk = None
    job.split = None
    return job


@pytest.fixture
def joblist(tmp_path):
    experiment_id = 'random-id'
    as_conf = mock.Mock()
    as_conf.experiment_data = dict()
    as_conf.experiment_data["JOBS"] = dict()
    as_conf.jobs_data = as_conf.experiment_data["JOBS"]
    as_conf.experiment_data["PLATFORMS"] = dict()
    joblist = JobList(experiment_id, as_conf, YAMLParserFactory())
    joblist._date_list = _DATE_LIST
    joblist._member_list = _MEMBER_LIST
    joblist._chunk_list = _CHUNK_LIST
    joblist._split_list = _SPLIT_LIST
    return joblist


def test_unify_to_filter(joblist):
    """Test the _unify_to_filter function"""
    # :param unified_filter: Single dictionary with all filters_to
    # :param filter_to: Current dictionary that contains the filters_to
    # :param filter_type: "DATES_TO", "MEMBERS_TO", "CHUNKS_TO", "SPLITS_TO"
    # :return: unified_filter
    unified_filter = \
        {
            "DATES_TO": "20020201",
            "MEMBERS_TO": "fc2",
            "CHUNKS_TO": "all",
            "SPLITS_TO": "1"
        }
    filter_to = \
        {
            "DATES_TO": "20020205,[20020207:20020208]",
            "MEMBERS_TO": "fc2,fc3",
            "CHUNKS_TO": "all"
        }
    filter_type = "DATES_TO"
    result = joblist._unify_to_filter(unified_filter, filter_to, filter_type)
    expected_output = \
        {
            "DATES_TO": "20020201,20020205,20020207,20020208,",
            "MEMBERS_TO": "fc2",
            "CHUNKS_TO": "all",
            "SPLITS_TO": "1"
        }
    assert result == expected_output

    unified_filter = \
        {
            "DATES_TO": "20020201",
            "MEMBERS_TO": "fc2",
            "CHUNKS_TO": "all",
            "SPLITS_TO": "all"
        }

    filter_to = \
        {
            "DATES_TO": "20020205",
            "MEMBERS_TO": "fc2,fc3",
            "CHUNKS_TO": "all"
        }

    filter_type = "SPLITS_TO"
    result = joblist._unify_to_filter(unified_filter, filter_to, filter_type)
    expected_output = \
        {
            "DATES_TO": "20020201",
            "MEMBERS_TO": "fc2",
            "CHUNKS_TO": "all",
            "SPLITS_TO": "all,"
        }
    assert result == expected_output

    unified_filter = \
        {
            "DATES_TO": "20020201",
            "MEMBERS_TO": "fc2",
            "CHUNKS_TO": "all",
            "SPLITS_TO": "1,2,3"
        }

    filter_to = \
        {
            "DATES_TO": "20020205",
            "MEMBERS_TO": "fc2,fc3",
            "CHUNKS_TO": "all",
            "SPLITS_TO": ""
        }

    filter_type = "SPLITS_TO"
    result = joblist._unify_to_filter(unified_filter, filter_to, filter_type)
    expected_output = \
        {
            "DATES_TO": "20020201",
            "MEMBERS_TO": "fc2",
            "CHUNKS_TO": "all",
            "SPLITS_TO": "1,2,3,"
        }
    assert result == expected_output

    unified_filter = \
        {
            "DATES_TO": "20020201",
            "MEMBERS_TO": "fc2",
            "CHUNKS_TO": "all",
            "SPLITS_TO": "1,2,natural"
        }

    filter_to = \
        {
            "DATES_TO": "20020205",
            "MEMBERS_TO": "fc2,fc3",
            "CHUNKS_TO": "all",
            "SPLITS_TO": "1,2,natural"
        }

    filter_type = "SPLITS_TO"
    result = joblist._unify_to_filter(unified_filter, filter_to, filter_type)
    expected_output = \
        {
            "DATES_TO": "20020201",
            "MEMBERS_TO": "fc2",
            "CHUNKS_TO": "all",
            "SPLITS_TO": "1,2,natural,"
        }
    assert result == expected_output

    unified_filter = \
        {
            "DATES_TO": "20020201",
            "MEMBERS_TO": "fc2",
            "CHUNKS_TO": "all",
            "SPLITS_TO": ""
        }

    filter_to = \
        {
            "DATES_TO": "20020205",
            "MEMBERS_TO": "fc2,fc3",
            "CHUNKS_TO": "all",
            "SPLITS_TO": {
                1: ["test", "ok"]
            }
        }

    filter_type = "SPLITS_TO"
    result = joblist._unify_to_filter(unified_filter, filter_to, filter_type)
    expected_output = \
        {
            "DATES_TO": "20020201",
            "MEMBERS_TO": "fc2",
            "CHUNKS_TO": "all",
            "SPLITS_TO": "okok']},"
        }
    assert result == expected_output


def test_simple_dependency(joblist, test_job):
    result_d = joblist._check_dates({}, test_job)
    result_m = joblist._check_members({}, test_job)
    result_c = joblist._check_chunks({}, test_job)
    result_s = joblist._check_splits({}, test_job)
    assert result_d == {}
    assert result_m == {}
    assert result_c == {}
    assert result_s == {}


def test_parse_filters_to_check(joblist):
    """Test the _parse_filters_to_check function"""
    result = joblist._parse_filters_to_check("20020201,20020202,20020203", _DATE_LIST)
    expected_output = ["20020201", "20020202", "20020203"]
    assert result == expected_output
    result = joblist._parse_filters_to_check("20020201,[20020203:20020205]", _DATE_LIST)
    expected_output = ["20020201", "20020203", "20020204", "20020205"]
    assert result == expected_output
    result = joblist._parse_filters_to_check("[20020201:20020203],[20020205:20020207]", _DATE_LIST)
    expected_output = ["20020201", "20020202", "20020203", "20020205", "20020206", "20020207"]
    assert result == expected_output
    result = joblist._parse_filters_to_check("20020201", _DATE_LIST)
    expected_output = ["20020201"]
    assert result == expected_output


def test_parse_filter_to_check(joblist):
    # Call the function to get the result
    # Value can have the following formats:
    # a range: [0:], [:N], [0:N], [:-1], [0:N:M] ...
    # a value: N
    # a range with step: [0::M], [::2], [0::3], [::3] ...
    result = joblist._parse_filter_to_check("20020201", _DATE_LIST)
    expected_output = ["20020201"]
    assert result == expected_output
    result = joblist._parse_filter_to_check("[20020201:20020203]", _DATE_LIST)
    expected_output = ["20020201", "20020202", "20020203"]
    assert result == expected_output
    result = joblist._parse_filter_to_check("[20020201:20020203:2]", _DATE_LIST)
    expected_output = ["20020201", "20020203"]
    assert result == expected_output
    result = joblist._parse_filter_to_check("[20020202:]", _DATE_LIST)
    expected_output = _DATE_LIST[1:]
    assert result == expected_output
    result = joblist._parse_filter_to_check("[:20020203]", _DATE_LIST)
    expected_output = _DATE_LIST[:3]
    assert result == expected_output
    result = joblist._parse_filter_to_check("[::2]", _DATE_LIST)
    expected_output = _DATE_LIST[::2]
    assert result == expected_output
    result = joblist._parse_filter_to_check("[20020203::]", _DATE_LIST)
    expected_output = _DATE_LIST[2:]
    assert result == expected_output
    result = joblist._parse_filter_to_check("[:20020203:]", _DATE_LIST)
    expected_output = _DATE_LIST[:3]
    assert result == expected_output
    # test with a member N:N
    result = joblist._parse_filter_to_check("[fc2:fc3]", _MEMBER_LIST)
    expected_output = ["fc2", "fc3"]
    assert result == expected_output
    # test with a chunk
    result = joblist._parse_filter_to_check("[1:2]", _CHUNK_LIST, level_to_check="CHUNKS_FROM")
    expected_output = [1, 2]
    assert result == expected_output
    # test with a split
    result = joblist._parse_filter_to_check("[1:2]", _SPLIT_LIST, level_to_check="SPLITS_FROM")
    expected_output = [1, 2]
    assert result == expected_output


def test_check_dates(joblist, test_job, relationships_dates, relationships_chunks):
    """
    Call the function to get the result
    """
    test_job.date = datetime.strptime("20020201", "%Y%m%d")
    test_job.member = "fc2"
    test_job.chunk = 1
    test_job.split = 1

    relationships_dates["DATES_FROM"]["20020201"].update(relationships_chunks)

    result = joblist._check_dates(relationships_dates, test_job)
    expected_output = {
        "DATES_TO": "20020201",
        "MEMBERS_TO": "fc2",
        "CHUNKS_TO": "all",
        "SPLITS_TO": "1",
        "MIN_TRIGGER_STATUS": "COMPLETED"
    }
    assert result == expected_output

    relationships_dates["DATES_FROM"]["20020201"]["MEMBERS_FROM"] = {}
    relationships_dates["DATES_FROM"]["20020201"]["CHUNKS_FROM"] = {}
    relationships_dates["DATES_FROM"]["20020201"]["SPLITS_FROM"] = {}

    result = joblist._check_dates(relationships_dates, test_job)
    expected_output = {
        "DATES_TO": "none",
        "MEMBERS_TO": "none",
        "CHUNKS_TO": "none",
        "SPLITS_TO": "none",
        "MIN_TRIGGER_STATUS": "COMPLETED"
    }
    assert result == expected_output

    # failure
    test_job.date = datetime.strptime("20020301", "%Y%m%d")
    result = joblist._check_dates(relationships_dates, test_job)
    assert result == {}


def test_check_members(joblist, test_job, relationships_members, relationships_chunks):
    """
    Call the function to get the result
    """
    test_job.date = datetime.strptime("20020201", "%Y%m%d")
    test_job.member = "fc2"

    result = joblist._check_members(relationships_members, test_job)
    expected_output = {
        "DATES_TO": "20020201",
        "MEMBERS_TO": "fc2",
        "CHUNKS_TO": "all",
        "SPLITS_TO": "1",
        "MIN_TRIGGER_STATUS": "COMPLETED"
    }
    assert result == expected_output

    relationships_members["MEMBERS_FROM"]["fc2"].update(relationships_chunks)

    result = joblist._check_members(relationships_members, test_job)
    expected_output = {
        "DATES_TO": "20020201",
        "MEMBERS_TO": "fc2",
        "CHUNKS_TO": "all",
        "SPLITS_TO": "1",
        "MIN_TRIGGER_STATUS": "COMPLETED"

    }
    assert result == expected_output

    relationships_members["MEMBERS_FROM"]["fc2"]["CHUNKS_FROM"] = {}
    relationships_members["MEMBERS_FROM"]["fc2"]["SPLITS_FROM"] = {}

    result = joblist._check_members(relationships_members, test_job)
    expected_output = {
        "DATES_TO": "none",
        "MEMBERS_TO": "none",
        "CHUNKS_TO": "none",
        "SPLITS_TO": "none",
        "MIN_TRIGGER_STATUS": "COMPLETED"
    }
    assert result == expected_output

    test_job.member = "fc3"
    result = joblist._check_members(relationships_members, test_job)
    assert result == {}

    # FAILURE
    test_job.member = "fc99"
    result = joblist._check_members(relationships_members, test_job)
    assert result == {}


def test_check_splits(joblist, test_job, relationships_splits):
    # Call the function to get the result

    test_job.split = 1
    result = joblist._check_splits(relationships_splits, test_job)
    expected_output = {
        "DATES_TO": "20020201",
        "MEMBERS_TO": "fc2",
        "CHUNKS_TO": "all",
        "SPLITS_TO": "1",
        "MIN_TRIGGER_STATUS": "COMPLETED"
    }
    assert result == expected_output
    test_job.split = 2
    result = joblist._check_splits(relationships_splits, test_job)
    assert result == {}
    # failure
    test_job.split = 99
    result = joblist._check_splits(relationships_splits, test_job)
    assert result == {}


def test_check_chunks(joblist, test_job, relationships_chunks):
    """
    Call the function to get the result
    """

    test_job.chunk = 1

    chunks = {
        "CHUNKS_FROM": {
            "1": {
                "SPLITS_FROM": {"5": {"SPLITS_TO": "4"}}
            }
        }
    }

    result = joblist._check_chunks(chunks, test_job)
    expected_output = {'SPLITS_TO': '4', "MIN_TRIGGER_STATUS": "COMPLETED"}

    assert result == expected_output
    chunks = {
        "CHUNKS_FROM": {
            "1": {"SPLITS_FROM": {}}
        }
    }

    result = joblist._check_chunks(chunks, test_job)
    expected_output = {'DATES_TO': 'none', 'MEMBERS_TO': 'none', 'CHUNKS_TO': 'none', 'SPLITS_TO': 'none',
                       "MIN_TRIGGER_STATUS": "COMPLETED"}
    assert result == expected_output

    test_job.chunk = 2
    result = joblist._check_chunks(relationships_chunks, test_job)
    assert result == {}

    # failure
    test_job.chunk = 99
    result = joblist._check_chunks(relationships_chunks, test_job)
    assert result == {}


def test_check_general(joblist, test_job, relationships_general):
    # Call the function to get the result

    test_job.date = datetime.strptime("20020201", "%Y%m%d")
    test_job.member = "fc2"
    test_job.chunk = 1
    test_job.split = 1
    result = joblist._filter_current_job(test_job, relationships_general)
    expected_output = {
        "DATES_TO": "20020201",
        "MEMBERS_TO": "fc2",
        "CHUNKS_TO": "all",
        "SPLITS_TO": "1"
    }
    assert result == expected_output


def test_check_relationship(joblist):
    relationships = {'MEMBERS_FROM': {
        'TestMember,   TestMember2,TestMember3   ': {'CHUNKS_TO': 'None', 'DATES_TO': 'None', 'FROM_STEP': 0,
                                                     'MEMBERS_TO': 'None', 'MIN_TRIGGER_STATUS': 'COMPLETED'}}}
    level_to_check = "MEMBERS_FROM"
    value_to_check = "TestMember"
    result = joblist._check_relationship(relationships, level_to_check, value_to_check)
    expected_output = [
        {'CHUNKS_TO': 'None', 'DATES_TO': 'None', 'FROM_STEP': 0, 'MEMBERS_TO': 'None',
         'MIN_TRIGGER_STATUS': "COMPLETED"}]
    assert result == expected_output
    value_to_check = "TestMember2"
    result = joblist._check_relationship(relationships, level_to_check, value_to_check)
    expected_output = [
        {'CHUNKS_TO': 'None', 'DATES_TO': 'None', 'FROM_STEP': 0, 'MEMBERS_TO': 'None',
         'MIN_TRIGGER_STATUS': 'COMPLETED'}]
    assert result == expected_output
    value_to_check = "TestMember3"
    result = joblist._check_relationship(relationships, level_to_check, value_to_check)
    expected_output = [
        {'CHUNKS_TO': 'None', 'DATES_TO': 'None', 'FROM_STEP': 0, 'MEMBERS_TO': 'None',
         'MIN_TRIGGER_STATUS': 'COMPLETED'}]
    assert result == expected_output
    value_to_check = "TestMember   "
    result = joblist._check_relationship(relationships, level_to_check, value_to_check)
    expected_output = [
        {'CHUNKS_TO': 'None', 'DATES_TO': 'None', 'FROM_STEP': 0, 'MEMBERS_TO': 'None',
         'MIN_TRIGGER_STATUS': 'COMPLETED'}]
    assert result == expected_output
    value_to_check = "   TestMember"
    result = joblist._check_relationship(relationships, level_to_check, value_to_check)
    expected_output = [
        {'CHUNKS_TO': 'None', 'DATES_TO': 'None', 'FROM_STEP': 0, 'MEMBERS_TO': 'None',
         'MIN_TRIGGER_STATUS': 'COMPLETED'}]
    assert result == expected_output
    relationships = {'DATES_FROM': {
        '20000101, 20000102, 20000103 ': {'CHUNKS_TO': 'None', 'DATES_TO': 'None', 'FROM_STEP': 0,
                                          'MEMBERS_TO': 'None', 'MIN_TRIGGER_STATUS': 'COMPLETED'}}}
    value_to_check = datetime(2000, 1, 1)
    result = joblist._check_relationship(relationships, "DATES_FROM", value_to_check)
    expected_output = [
        {'CHUNKS_TO': 'None', 'DATES_TO': 'None', 'FROM_STEP': 0, 'MEMBERS_TO': 'None',
         'MIN_TRIGGER_STATUS': 'COMPLETED'}]
    assert result == expected_output


@pytest.fixture()
def _init_special_conditions(joblist: JobList) -> Tuple[Job, Job, Job, dict, JobList]:
    """
    Initialize a JobList with a job and its parents, and add special conditions to the job.
    This is used to test the add_special_conditions method.
    :param joblist: JobList instance to use for the test
    :return: A tuple containing the job, its parents, special conditions, and the job

    """
    # Method from job_list
    job = Job("child", 1, Status.READY, 1)
    job.section = "child_one"
    job.date = datetime.strptime("20200128", "%Y%m%d")
    job.member = "fc0"
    job.chunk = 1
    job.split = 1
    job.splits = 1
    job.max_checkpoint_step = 0
    job.status = Status.WAITING
    special_conditions = {"MIN_TRIGGER_STATUS": "RUNNING", "FROM_STEP": "2", "FAIL_OK": False}

    parent = Job("parent", 1, Status.READY, 1)
    parent.section = "parent_one"
    parent.date = datetime.strptime("20200128", "%Y%m%d")
    parent.member = "fc0"
    parent.chunk = 1
    parent.split = 1
    parent.splits = 1
    parent.max_checkpoint_step = 0

    parent2 = Job("parent2", 1, Status.READY, 1)
    parent2.section = "parent_two"
    parent2.date = datetime.strptime("20200128", "%Y%m%d")
    parent2.member = "fc0"
    parent2.chunk = 1
    joblist.graph = DiGraph()
    joblist.add_job(job)
    joblist.add_job(parent)
    joblist.add_job(parent2)
    joblist.graph.add_edge(parent.name, job.name)
    joblist.graph.add_edge(parent2.name, job.name)
    joblist.add_special_conditions(job, special_conditions, parent)
    special_conditions = {"MIN_TRIGGER_STATUS": "FAILED", "FROM_STEP": 0, "FAIL_OK": False}
    joblist.add_special_conditions(job, special_conditions, parent2)
    return job, parent, parent2, special_conditions, joblist


def test_add_special_conditions(mocker, _init_special_conditions):
    job, parent, parent2, special_conditions, joblist = _init_special_conditions

    edge = joblist.graph.edges[parent.name, job.name]
    assert job.max_checkpoint_step == 2
    assert edge.get("from_step") == 2
    assert edge.get("min_trigger_status") == "RUNNING"
    assert not edge.get("fail_ok")

    edge = joblist.graph.edges[parent2.name, job.name]
    # Still 2 because it is the MAX value between all dependencies
    assert job.max_checkpoint_step == 2
    assert edge.get("from_step") == 0
    assert edge.get("min_trigger_status") == "FAILED"
    assert not edge.get("fail_ok")


def test_job_dict_get_jobs_filtered(mocker, joblist):
    # Test the get_jobs_filtered function
    as_conf = mocker.Mock()
    as_conf.experiment_data = dict()
    as_conf.experiment_data = {
        'CONFIG': {'AUTOSUBMIT_VERSION': '4.1.2', 'MAXWAITINGJOBS': 20, 'TOTALJOBS': 20, 'SAFETYSLEEPTIME': 10,
                   'RETRIALS': 0}, 'MAIL': {'NOTIFICATIONS': False, 'TO': None},
        'STORAGE': {'TYPE': 'sqlite', 'COPY_REMOTE_LOGS': True},
        'DEFAULT': {'EXPID': 'a03b', 'HPCARCH': 'marenostrum4'},
        'EXPERIMENT': {'DATELIST': '20000101', 'MEMBERS': 'fc0', 'CHUNKSIZEUNIT': 'month', 'CHUNKSIZE': 4,
                       'NUMCHUNKS': 5, 'CHUNKINI': '', 'CALENDAR': 'standard'},
        'PROJECT': {'PROJECT_TYPE': 'none', 'PROJECT_DESTINATION': ''},
        'GIT': {'PROJECT_ORIGIN': '', 'PROJECT_BRANCH': '', 'PROJECT_COMMIT': '', 'PROJECT_SUBMODULES': '',
                'FETCH_SINGLE_BRANCH': True}, 'SVN': {'PROJECT_URL': '', 'PROJECT_REVISION': ''},
        'LOCAL': {'PROJECT_PATH': ''},
        'PROJECT_FILES': {'FILE_PROJECT_CONF': '', 'FILE_JOBS_CONF': '', 'JOB_SCRIPTS_TYPE': ''},
        'RERUN': {'RERUN': False, 'RERUN_JOBLIST': ''}, 'JOBS': {'SIM': {'FILE': 'SIM.sh', 'RUNNING': 'once',
                                                                         'DEPENDENCIES': {'SIM-1': {'CHUNKS_FROM': {
                                                                             'ALL': {'SPLITS_TO': 'previous'}}}},
                                                                         'WALLCLOCK': '00:05', 'SPLITS': 10,
                                                                         'ADDITIONAL_FILES': []},
                                                                 'TEST': {'FILE': 'Test.sh', 'DEPENDENCIES': {
                                                                     'TEST-1': {'CHUNKS_FROM': {
                                                                         'ALL': {'SPLITS_TO': 'previous'}}},
                                                                     'SIM': None}, 'RUNNING': 'once',
                                                                          'WALLCLOCK': '00:05', 'SPLITS': 10,
                                                                          'ADDITIONAL_FILES': []}}, 'PLATFORMS': {
            'MARENOSTRUM4': {'TYPE': 'slurm', 'HOST': 'mn2.bsc.es', 'PROJECT': 'bsc32', 'USER': 'bsc32070',
                             'QUEUE': 'debug', 'SCRATCH_DIR': '/gpfs/scratch', 'ADD_PROJECT_TO_HOST': False,
                             'MAX_WALLCLOCK': '48:00', 'TEMP_DIR': ''},
            'MARENOSTRUM_ARCHIVE': {'TYPE': 'ps', 'HOST': 'dt02.bsc.es', 'PROJECT': 'bsc32', 'USER': None,
                                    'SCRATCH_DIR': '/gpfs/scratch', 'ADD_PROJECT_TO_HOST': False,
                                    'TEST_SUITE': False},
            'TRANSFER_NODE': {'TYPE': 'ps', 'HOST': 'dt01.bsc.es', 'PROJECT': 'bsc32', 'USER': None,
                              'ADD_PROJECT_TO_HOST': False, 'SCRATCH_DIR': '/gpfs/scratch'},
            'TRANSFER_NODE_BSCEARTH000': {'TYPE': 'ps', 'HOST': 'bscearth000', 'USER': None, 'PROJECT': 'Earth',
                                          'ADD_PROJECT_TO_HOST': False, 'QUEUE': 'serial',
                                          'SCRATCH_DIR': '/esarchive/scratch'},
            'BSCEARTH000': {'TYPE': 'ps', 'HOST': 'bscearth000', 'USER': None, 'PROJECT': 'Earth',
                            'ADD_PROJECT_TO_HOST': False, 'QUEUE': 'serial', 'SCRATCH_DIR': '/esarchive/scratch'},
            'NORD3': {'TYPE': 'SLURM', 'HOST': 'nord1.bsc.es', 'PROJECT': 'bsc32', 'USER': None, 'QUEUE': 'debug',
                      'SCRATCH_DIR': '/gpfs/scratch', 'MAX_WALLCLOCK': '48:00'},
            'ECMWF-XC40': {'TYPE': 'ecaccess', 'VERSION': 'pbs', 'HOST': 'cca', 'USER': None, 'PROJECT': 'spesiccf',
                           'ADD_PROJECT_TO_HOST': False, 'SCRATCH_DIR': '/scratch/ms', 'QUEUE': 'np',
                           'SERIAL_QUEUE': 'ns', 'MAX_WALLCLOCK': '48:00'}},
        'ROOTDIR': '/home/dbeltran/new_autosubmit/a03b', 'PROJDIR': '/home/dbeltran/new_autosubmit/a03b/proj/'}
    as_conf.jobs_data = as_conf.experiment_data["JOBS"]
    as_conf.last_experiment_data = as_conf.experiment_data
    dictionary = DicJobs(_DATE_LIST, _MEMBER_LIST, _CHUNK_LIST, "", default_retrials=0,
                         as_conf=as_conf)
    dictionary.read_section("SIM", 1, "bash")
    job = Job("SIM", 1, Status.READY, 1)
    job.date = None
    job.member = None
    job.chunk = None
    job.running = "once"
    job.split = 1
    job.splits = 2
    job.max_checkpoint_step = 0
    job_list = mocker.Mock(wraps=joblist)
    job_list.job_list = [job]
    filters_to = {'SPLITS_TO': "1*\\1"}
    filters_to_of_parent = {'SPLITS_TO': 'previous'}
    natural_chunk = 1
    natural_member = 'fc0'
    section = 'SIM'
    result = dictionary.get_jobs_filtered(section, job, filters_to, None, natural_member, natural_chunk,
                                          filters_to_of_parent)
    expected_output = [dictionary._dic["SIM"][0]]
    assert expected_output == result


def test_normalize_auto_keyword(as_conf, mocker):
    job_list = JobList(
        as_conf.expid,
        as_conf,
        YAMLParserFactory()
    )
    dependency = Dependency("test")

    job = Job(f"{_EXPID}_20001010_fc1_2_1_test", 1, Status.READY, 1)
    job.running = "chunk"
    job.section = "test"
    job.date = "20001010"
    job.member = "fc1"
    job.splits = 5

    job_minus = Job(f"{_EXPID}_20001010_fc1_1_1_minus", 1, Status.READY, 1)
    job_minus.running = "chunk"
    job_minus.section = "minus"
    job_minus.date = "20001010"
    job_minus.member = "fc1"
    job_minus.splits = 40

    job_plus = Job(f"{_EXPID}_20001010_fc1_3_1_plus", 1, Status.READY, 1)
    job_plus.running = "chunk"
    job_plus.section = "plus"
    job_plus.date = "20001010"
    job_plus.member = "fc1"
    job_plus.splits = 50

    job_list.graph = DiGraph()
    job_list.graph.add_node(job.name, job=job)
    job_list.graph.add_node(job_minus.name, job=job_minus)
    job_list.graph.add_node(job_plus.name, job=job_plus)

    dependency.distance = 1
    dependency.relationships = {
        "SPLITS_FROM": {
            "key": {
                "SPLITS_TO": "auto"
            }
        }
    }
    dependency.sign = "-"
    dependency.section = "minus"
    dependency = job_list._normalize_auto_keyword(job, dependency)
    assert dependency.relationships["SPLITS_FROM"]["key"]["SPLITS_TO"] == "40"
    assert job.splits == "40"
    dependency.relationships = {
        "SPLITS_FROM": {
            "key": {
                "SPLITS_TO": "auto"
            }
        }
    }
    dependency.sign = "+"
    dependency.section = "plus"
    dependency = job_list._normalize_auto_keyword(job, dependency)
    assert dependency.relationships["SPLITS_FROM"]["key"]["SPLITS_TO"] == "50"
    assert job.splits == "50"  # Test that the param is assigned

    # Test that the param is not being changed after update_job_parameters
    as_conf.experiment_data["JOBS"] = {}
    as_conf.experiment_data["JOBS"][job.section] = {}
    as_conf.experiment_data["JOBS"][job.section]["SPLITS"] = "auto"
    job.date = None
    mocker.patch("autosubmit.job.job.Job.calendar_split", side_effect=lambda x, y, z: y)
    parameters = as_conf.load_parameters()
    parameters = job.update_job_parameters(as_conf, parameters, True)
    assert job.splits == "50"
    assert parameters["SPLITS"] == "50"
