from datetime import datetime

import pytest
from networkx import DiGraph  # type: ignore

from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.config.yamlparser import YAMLParserFactory
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status, is_edge_satisfied
from autosubmit.job.job_dict import DicJobs
from autosubmit.job.job_list import JobList
from autosubmit.job.job_utils import Dependency
from autosubmit.log.log import AutosubmitCritical

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
def joblist(tmp_path, mocker):
    experiment_id = 'random-id'
    as_conf = mocker.Mock()
    as_conf.experiment_data = {}
    as_conf.experiment_data["JOBS"] = {}
    as_conf.jobs_data = as_conf.experiment_data["JOBS"]
    as_conf.experiment_data["PLATFORMS"] = {}
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
        "ACCEPTED_STATUSES": "COMPLETED,SKIPPED"
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
        "ACCEPTED_STATUSES": "COMPLETED,SKIPPED"
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
        "ACCEPTED_STATUSES": "COMPLETED,SKIPPED"
    }
    assert result == expected_output

    relationships_members["MEMBERS_FROM"]["fc2"].update(relationships_chunks)

    result = joblist._check_members(relationships_members, test_job)
    expected_output = {
        "DATES_TO": "20020201",
        "MEMBERS_TO": "fc2",
        "CHUNKS_TO": "all",
        "SPLITS_TO": "1",
        "ACCEPTED_STATUSES": "COMPLETED,SKIPPED"

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
        "ACCEPTED_STATUSES": "COMPLETED,SKIPPED"
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
        "ACCEPTED_STATUSES": "COMPLETED,SKIPPED"
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
    expected_output = {'SPLITS_TO': '4', "ACCEPTED_STATUSES": "COMPLETED,SKIPPED"}

    assert result == expected_output
    chunks = {
        "CHUNKS_FROM": {
            "1": {"SPLITS_FROM": {}}
        }
    }

    result = joblist._check_chunks(chunks, test_job)
    expected_output = {'DATES_TO': 'none', 'MEMBERS_TO': 'none', 'CHUNKS_TO': 'none', 'SPLITS_TO': 'none',
                       "ACCEPTED_STATUSES": "COMPLETED,SKIPPED"}
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
                                                     'MEMBERS_TO': 'None', 'ACCEPTED_STATUSES': 'COMPLETED'}}}
    level_to_check = "MEMBERS_FROM"
    value_to_check = "TestMember"
    result = joblist._check_relationship(relationships, level_to_check, value_to_check)
    expected_output = [
        {'CHUNKS_TO': 'None', 'DATES_TO': 'None', 'FROM_STEP': 0, 'MEMBERS_TO': 'None',
         'ACCEPTED_STATUSES': "COMPLETED"}]
    assert result == expected_output
    value_to_check = "TestMember2"
    result = joblist._check_relationship(relationships, level_to_check, value_to_check)
    expected_output = [
        {'CHUNKS_TO': 'None', 'DATES_TO': 'None', 'FROM_STEP': 0, 'MEMBERS_TO': 'None',
         'ACCEPTED_STATUSES': 'COMPLETED'}]
    assert result == expected_output
    value_to_check = "TestMember3"
    result = joblist._check_relationship(relationships, level_to_check, value_to_check)
    expected_output = [
        {'CHUNKS_TO': 'None', 'DATES_TO': 'None', 'FROM_STEP': 0, 'MEMBERS_TO': 'None',
         'ACCEPTED_STATUSES': 'COMPLETED'}]
    assert result == expected_output
    value_to_check = "TestMember   "
    result = joblist._check_relationship(relationships, level_to_check, value_to_check)
    expected_output = [
        {'CHUNKS_TO': 'None', 'DATES_TO': 'None', 'FROM_STEP': 0, 'MEMBERS_TO': 'None',
         'ACCEPTED_STATUSES': 'COMPLETED'}]
    assert result == expected_output
    value_to_check = "   TestMember"
    result = joblist._check_relationship(relationships, level_to_check, value_to_check)
    expected_output = [
        {'CHUNKS_TO': 'None', 'DATES_TO': 'None', 'FROM_STEP': 0, 'MEMBERS_TO': 'None',
         'ACCEPTED_STATUSES': 'COMPLETED'}]
    assert result == expected_output
    relationships = {'DATES_FROM': {
        '20000101, 20000102, 20000103 ': {'CHUNKS_TO': 'None', 'DATES_TO': 'None', 'FROM_STEP': 0,
                                          'MEMBERS_TO': 'None', 'ACCEPTED_STATUSES': 'COMPLETED'}}}
    value_to_check = datetime(2000, 1, 1)
    result = joblist._check_relationship(relationships, "DATES_FROM", value_to_check)
    expected_output = [
        {'CHUNKS_TO': 'None', 'DATES_TO': 'None', 'FROM_STEP': 0, 'MEMBERS_TO': 'None',
         'ACCEPTED_STATUSES': 'COMPLETED'}]
    assert result == expected_output


@pytest.fixture()
def _init_special_conditions(joblist: JobList) -> tuple[Job, Job, Job, dict, JobList]:
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
    special_conditions = {"ACCEPTED_STATUSES": "RUNNING", "FROM_STEP": "2"}

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
    special_conditions = {"ACCEPTED_STATUSES": "FAILED", "FROM_STEP": 0}
    joblist.add_special_conditions(job, special_conditions, parent2)
    return job, parent, parent2, special_conditions, joblist


def test_add_special_conditions(_init_special_conditions):
    job, parent, parent2, _special_conditions, joblist = _init_special_conditions

    edge = joblist.graph.edges[parent.name, job.name]
    assert parent.max_checkpoint_step == 2
    assert edge.get("from_step") == 2
    assert edge.get("accepted_statuses") == "RUNNING"

    edge = joblist.graph.edges[parent2.name, job.name]
    # The counter belongs to each parent, so parent2 has no checkpoint requirement
    assert parent.max_checkpoint_step == 2
    assert parent2.max_checkpoint_step == 0
    assert job.max_checkpoint_step == 0
    assert edge.get("from_step") == 0
    assert edge.get("accepted_statuses") == "FAILED"

    # FAILED is valid together with FROM_STEP, not only RUNNING
    joblist.add_special_conditions(
        job, {"ACCEPTED_STATUSES": "FAILED", "FROM_STEP": 2}, parent
    )
    assert joblist.graph.edges[parent.name, job.name]["from_step"] == 2


@pytest.mark.parametrize(
    "from_step, accepted_statuses",
    [(-1, "RUNNING"), ("two", "RUNNING"), (2, "COMPLETED"), (2, None)],
    ids=["negative", "not-an-integer", "missing-running-or-failed", "missing-status"],
)
def test_add_special_conditions_rejects_invalid_from_step(
    joblist: JobList, from_step, accepted_statuses
) -> None:
    """FROM_STEP must be a non-negative integer with RUNNING or FAILED, even without STATUS."""
    job = Job("child", 1, Status.WAITING, 0)
    parent = Job("parent", 2, Status.RUNNING, 0)
    parent.section = "SIM"
    job.section = "POST"
    joblist.graph.add_node(job.name, job=job)
    joblist.graph.add_node(parent.name, job=parent)
    joblist.graph.add_edge(parent.name, job.name, accepted_statuses="COMPLETED,SKIPPED", from_step=0)
    relationships = {"FROM_STEP": from_step}
    if accepted_statuses is not None:
        relationships["ACCEPTED_STATUSES"] = accepted_statuses

    with pytest.raises(AutosubmitCritical, match="FROM_STEP") as exc_info:
        joblist._apply_jobs_edge_info(job, {"SIM": Dependency("SIM", relationships=relationships)})

    assert exc_info.value.code == 7014


def test_normalize_dependencies_uppercases_nested_to_keys():
    """Nested *_TO keys are normalized so the filter parser can read them."""
    dependencies = {
        "SIM-1": {
            "STATUS": "running",
            "CHUNKS_FROM": {"all": {"chunks_to": 1, "dates_to": "20200102"}},
        }
    }

    normalized = AutosubmitConfig._normalize_dependencies(dependencies, "POST")

    filters = normalized["SIM-1"]["CHUNKS_FROM"]["all"]
    assert filters["CHUNKS_TO"] == 1
    assert filters["DATES_TO"] == "20200102"


def test_job_dict_get_jobs_filtered(mocker, joblist):
    # Test the get_jobs_filtered function
    as_conf = mocker.Mock()
    as_conf.experiment_data = {}
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


@pytest.mark.parametrize("parent_splits,child_splits,slice_size", [
    (8, 8, 1),
    (16, 8, 2),
    (15, 5, 3),
    (31, 16, 2),
    (4, 8, 2),
    (3, 9, 3),
])
def test_get_jobs_filtered_grouped_splits(parent_splits, child_splits, slice_size, as_conf):
    """Test grouped split dependency mapping (*\\N) for N-to-1, 1-to-1, and 1-to-N ratios.

    Verifies that every child split resolves to the correct parent split(s) regardless
    of the parent-to-child split ratio and slice size.  The SPLITS_TO filter is set to
    the minimal form ``1*\\<slice_size>`` so the only information used is the slice size
    encoded in the ``*\\N`` suffix; the actual index computation must be purely arithmetic.
    """
    date = datetime(2000, 1, 1)
    as_conf.experiment_data = {
        'DEFAULT': {'EXPID': 'a000'},
        'STORAGE': {'TYPE': 'pkl', 'COPY_REMOTE_LOGS': True},
        'EXPERIMENT': {
            'DATELIST': '20000101', 'MEMBERS': 'fc0', 'CHUNKSIZEUNIT': 'day',
            'CHUNKSIZE': 1, 'NUMCHUNKS': 1, 'CHUNKINI': '', 'CALENDAR': 'standard',
        },
        'JOBS': {
            'PARENT': {
                'FILE': 'parent.sh', 'RUNNING': 'chunk',
                'SPLITS': parent_splits, 'WALLCLOCK': '00:10',
            },
        },
    }
    dictionary = DicJobs([date], ["fc0"], [1], "", default_retrials=0, as_conf=as_conf)
    dictionary.read_section("PARENT", 1, "bash")

    parent_jobs = dictionary._dic["PARENT"][date]["fc0"][1]
    # Minimal filter: only the slice suffix matters; index arithmetic does the rest.
    filters_to = {'SPLITS_TO': f"1*\\{slice_size}"}
    filters_to_of_parent = {}

    is_n_to_1 = child_splits <= parent_splits

    for child_split in range(1, child_splits + 1):
        child_job = Job(f"a000_20000101_fc0_1_{child_split}_CHILD", 1, Status.WAITING, 1)
        child_job.running = "chunk"
        child_job.section = "CHILD"
        child_job.date = date
        child_job.member = "fc0"
        child_job.chunk = 1
        child_job.split = child_split
        child_job.splits = child_splits

        result = dictionary.get_jobs_filtered(
            "PARENT", child_job, filters_to, date, "fc0", 1, filters_to_of_parent,
        )

        if is_n_to_1:
            start = (child_split - 1) * slice_size
            end = min(start + slice_size, parent_splits)
            expected = {j.name for j in parent_jobs[start:end]}
            assert {j.name for j in result} == expected, (
                f"child_split={child_split}: expected parents {expected}, got {[j.name for j in result]}"
            )
        else:
            parent_index = min((child_split - 1) // slice_size, parent_splits - 1)
            expected_name = parent_jobs[parent_index].name
            assert len(result) == 1 and result[0].name == expected_name, (
                f"child_split={child_split}: expected parent {expected_name}, got {[j.name for j in result]}"
            )


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


def test_child_uses_each_parent_checkpoint_step(joblist: JobList) -> None:
    """Each edge uses the checkpoint step of its own parent, not a shared child counter."""
    joblist.disable_save = True
    child = Job("child", 1, Status.WAITING, 0)
    parent_a = Job("parent_a", 2, Status.RUNNING, 0)
    parent_b = Job("parent_b", 3, Status.RUNNING, 0)
    parent_a.current_checkpoint_step = 2
    parent_b.current_checkpoint_step = 2
    joblist.graph.add_node(child.name, job=child)
    for parent, from_step in ((parent_a, 2), (parent_b, 3)):
        joblist.graph.add_node(parent.name, job=parent)
        joblist.graph.add_edge(
            parent.name, child.name, accepted_statuses="RUNNING", from_step=from_step
        )
        child.parents.add(parent)

    joblist._update_waiting_and_delayed_jobs()
    assert child.status == Status.WAITING

    parent_b.current_checkpoint_step = 3
    joblist._update_waiting_and_delayed_jobs()
    assert child.status == Status.READY


def test_unify_to_filters_handles_integer_from_step(joblist: JobList) -> None:
    """FROM_STEP is an integer and must not be lowercased with the *_TO filters."""
    result = joblist._unify_to_filters([{"FROM_STEP": 2}])

    assert result["FROM_STEP"] == 2


@pytest.mark.parametrize(
    "parent_status,accepted_statuses,from_step,parent_checkpoint_step,expected",
    [
        ("RUNNING", "RUNNING", 0, 0, True),
        ("RUNNING", "RUNNING", 3, 3, True),
        ("RUNNING", "RUNNING", 3, 0, False),
        ("COMPLETED", "RUNNING", 0, 0, False),
        ("COMPLETED", "RUNNING,SUCCEEDED", 0, 0, True),
        ("SKIPPED", "RUNNING", 0, 0, False),
        ("SKIPPED", "RUNNING,SUCCEEDED", 0, 0, True),
        ("FAILED", "RUNNING", 0, 0, False),
        ("FAILED", "RUNNING,SUCCEEDED", 0, 0, False),
        ("QUEUING", "RUNNING", 0, 0, False),
        ("SUBMITTED", "RUNNING", 0, 0, False),
        ("READY", "RUNNING", 0, 0, False),
        ("QUEUING", "QUEUING", 0, 0, True),
        ("COMPLETED", "QUEUING", 0, 0, False),
        ("COMPLETED", "QUEUING,SUCCEEDED", 0, 0, True),
        ("SKIPPED", "QUEUING,SUCCEEDED", 0, 0, True),
        ("RUNNING", "QUEUING", 0, 0, False),
        ("QUEUING", "SUBMITTED", 0, 0, False),
        ("READY", "SUBMITTED", 0, 0, False),
        ("COMPLETED", "COMPLETED", 0, 0, True),
        ("COMPLETED", "SKIPPED", 0, 0, False),
        ("SKIPPED", "COMPLETED", 0, 0, False),
        ("SKIPPED", "SKIPPED", 0, 0, True),
        ("COMPLETED", "SUCCEEDED", 0, 0, True),
        ("SKIPPED", "SUCCEEDED", 0, 0, True),
        ("FAILED", "FAILED", 0, 0, True),
        ("COMPLETED", "FAILED", 0, 0, False),
        ("FAILED", "COMPLETED", 0, 0, False),
        ("FAILED", "FAILED,COMPLETED", 0, 0, True),
        ("COMPLETED", "FAILED,COMPLETED", 0, 0, True),
        ("SKIPPED", "FAILED,COMPLETED", 0, 0, False),
        ("FAILED", "FINISHED", 0, 0, True),
        ("COMPLETED", "FINISHED", 0, 0, True),
        ("SKIPPED", "FINISHED", 0, 0, True),
        ("RUNNING", "FINISHED", 0, 0, False),
        (Status.RUNNING, Status.RUNNING, 0, 0, True),
        (Status.COMPLETED, Status.RUNNING, 0, 0, False),
        ("RUNNING", "RUNNING,FAILED", 0, 0, True),
        ("FAILED", "RUNNING,FAILED", 0, 0, True),
        ("COMPLETED", "RUNNING,FAILED", 0, 0, False),
        ("RUNNING", "RUNNING,FAILED", 3, 0, False),
        ("RUNNING", "RUNNING,FAILED", 3, 3, True),
        ("COMPLETED", "RUNNING,FAILED", 3, 0, False),
        ("COMPLETED", "RUNNING,SUCCEEDED", 3, 0, True),
        ("UNKNOWN", "SUCCEEDED", 0, 0, False),
        ("SUSPENDED", "SUCCEEDED", 0, 0, False),
        ("RUNNING", ["RUNNING", "FAILED"], 0, 0, True),
        ("COMPLETED", None, 0, 0, True),
        ("SKIPPED", None, 0, 0, True),
        ("SKIPPED", "COMPLETED", 0, 0, False),
        ("FAILED", "RUNNING,FAILED", 3, 0, False),
        ("FAILED", "RUNNING,FAILED", 3, 3, True),
        ("FAILED", "FINISHED", 2, 1, False),
        ("FAILED", "FINISHED", 2, 2, True),
        ("FAILED", "RUNNING FAILED", 0, 0, True),
        ("COMPLETED", "COMPLETED SKIPPED", 0, 0, True),
    ],
    ids=[
        "running_strict",
        "running_checkpoint_reached",
        "running_checkpoint_pending",
        "completed_blocks_strict_running",
        "completed_meets_relaxed_running",
        "skipped_blocks_strict_running",
        "skipped_meets_relaxed_running",
        "failed_blocks_strict_running",
        "failed_blocks_relaxed_running",
        "queuing_blocks_running",
        "submitted_blocks_running",
        "ready_blocks_running",
        "queuing_meets_queuing",
        "completed_blocks_strict_queuing",
        "completed_meets_relaxed_queuing",
        "skipped_meets_relaxed_queuing",
        "running_blocks_queuing",
        "queuing_blocks_submitted",
        "ready_blocks_submitted",
        "completed_meets_completed",
        "completed_blocks_strict_skipped",
        "skipped_blocks_strict_completed",
        "skipped_meets_skipped",
        "completed_meets_succeeded",
        "skipped_meets_succeeded",
        "failed_meets_failed",
        "completed_blocks_strict_failed",
        "failed_blocks_strict_completed",
        "failed_meets_failed_or_completed",
        "completed_meets_failed_or_completed",
        "skipped_blocks_failed_or_completed",
        "failed_meets_finished",
        "completed_meets_finished",
        "skipped_meets_finished",
        "running_blocks_finished",
        "int_running_strict",
        "int_completed_blocks_strict_running",
        "running_meets_running_or_failed",
        "failed_meets_running_or_failed",
        "completed_blocks_running_or_failed",
        "running_checkpoint_pending_with_failed",
        "running_checkpoint_reached_with_failed",
        "completed_meets_running_or_failed_ignoring_checkpoint",
        "completed_meets_relaxed_running_ignoring_checkpoint",
        "unknown_blocks_edge",
        "suspended_blocks_edge",
        "list_start_condition",
        "default_start_condition",
        "default_accepts_skipped",
        "strict_completed_blocks_skipped",
        "failed_checkpoint_pending",
        "failed_checkpoint_reached",
        "failed_finished_checkpoint_pending",
        "failed_finished_checkpoint_reached",
        "failed_whitespace_start_condition",
        "completed_whitespace_start_condition",
    ],
)
def test_is_edge_satisfied(
    parent_status: int | str,
    accepted_statuses: int | str | list[str] | None,
    from_step: int,
    parent_checkpoint_step: int,
    expected: bool,
) -> None:
    """is_edge_satisfied accepts status codes, names and sets of exact statuses."""
    assert is_edge_satisfied(
        parent_status, accepted_statuses, from_step, parent_checkpoint_step
    ) is expected


@pytest.mark.parametrize(
    "accepted_statuses",
    ["BOGUS", 99, True],
    ids=["invalid_name", "invalid_code", "bool"],
)
def test_is_edge_satisfied_rejects_invalid_start_condition(accepted_statuses):
    """An invalid start condition fails instead of falling back to the default."""
    with pytest.raises(AutosubmitCritical, match="Invalid STATUS") as exc_info:
        is_edge_satisfied("COMPLETED", accepted_statuses)
    assert exc_info.value.code == 7014


def test_is_edge_satisfied_rejects_unordered_start_condition():
    """Unordered containers are rejected because the canonical order must be deterministic."""
    with pytest.raises(AutosubmitCritical, match="ordered list") as exc_info:
        is_edge_satisfied("RUNNING", {"RUNNING", "FAILED"})
    assert exc_info.value.code == 7014


@pytest.mark.parametrize("steps_back,job_split,total_splits,expected_split", [
    ("", 3, 10, 2),
    ("1", 3, 10, 2),
    ("1", 1, 10, None),
    ("2", 5, 10, 3),
    ("2", 2, 10, None),
    ("2", 3, 10, 1),
    ("3", 4, 10, 1),
    ("3", 3, 10, None),
    ("10", 6, 6, None),
    ("10", 10, 6, None),
    ("6", 6, 6, None),
    ("6", 1, 6, None),
    ("4", 4, 10, None),
    ("4", 5, 10, 1),
    ("1", -1, 10, None),
])
def test_apply_splits_filter_previous_n(steps_back, job_split, total_splits, expected_split, as_conf):
    """Test that ``previous`` and ``previous-N`` resolve to the correct split"""
    date = datetime(2000, 1, 1)
    as_conf.experiment_data = {
        'DEFAULT': {'EXPID': 'a000'},
        'STORAGE': {'TYPE': 'pkl', 'COPY_REMOTE_LOGS': True},
        'EXPERIMENT': {
            'DATELIST': '20000101', 'MEMBERS': 'fc0', 'CHUNKSIZEUNIT': 'day',
            'CHUNKSIZE': 1, 'NUMCHUNKS': 1, 'CHUNKINI': '', 'CALENDAR': 'standard',
        },
        'JOBS': {
            'SIM': {
                'FILE': 'sim.sh', 'RUNNING': 'chunk',
                'SPLITS': total_splits, 'WALLCLOCK': '00:05',
            },
        },
    }
    dictionary = DicJobs([date], ["fc0"], [1], "", default_retrials=0, as_conf=as_conf)
    dictionary.read_section("SIM", 1, "bash")
    splits_filter = f"previous-{steps_back}" if steps_back else "previous"
    filters_to = {"SPLITS_TO": splits_filter}
    job = Job(f"a000_20000101_fc0_1_{job_split}_SIM", 0, Status.WAITING, 1)
    job.date = date
    job.member = "fc0"
    job.chunk = 1
    job.running = "chunk"
    job.split = job_split
    job.splits = total_splits
    result = dictionary.get_jobs_filtered("SIM", job, filters_to, date, "fc0", 1, {})
    if expected_split is None:
        assert result == [], f"Expected no result for split={job_split}, filter={splits_filter}"
    else:
        assert len(result) == 1, f"Expected exactly one result for split={job_split}, filter={splits_filter}"
        assert result[0].split == expected_split


@pytest.mark.parametrize("filter_key", ["DATES_TO", "MEMBERS_TO", "CHUNKS_TO"])
@pytest.mark.parametrize("filter_value", ["none", "None", "NONE", "blah_none_blah"])
def test_get_jobs_filtered_none_filter(filter_key, filter_value, as_conf):
    """A 'none' filter at any level must yield an empty job list."""
    date = datetime(2000, 1, 1)
    as_conf.experiment_data = {
        'DEFAULT': {'EXPID': 'a000'},
        'STORAGE': {'TYPE': 'pkl', 'COPY_REMOTE_LOGS': True},
        'EXPERIMENT': {
            'DATELIST': '20000101', 'MEMBERS': 'fc0', 'CHUNKSIZEUNIT': 'day',
            'CHUNKSIZE': 1, 'NUMCHUNKS': 1, 'CHUNKINI': '', 'CALENDAR': 'standard',
        },
        'JOBS': {
            'SIM': {
                'FILE': 'sim.sh', 'RUNNING': 'chunk',
                'SPLITS': 2, 'WALLCLOCK': '00:05',
            },
        },
    }
    dictionary = DicJobs([date], ["fc0"], [1], "", default_retrials=0, as_conf=as_conf)
    dictionary.read_section("SIM", 1, "bash")

    job = Job("a000_20000101_fc0_1_1_SIM", 0, Status.WAITING, 1)
    job.date = date
    job.member = "fc0"
    job.chunk = 1
    job.running = "chunk"
    job.split = 1
    job.splits = 2

    filters_to = {filter_key: filter_value}
    result = dictionary.get_jobs_filtered("SIM", job, filters_to, date, "fc0", 1, {})
    assert result == [], f"Expected empty list for {filter_key}={filter_value}, got {result}"


@pytest.mark.parametrize("filter_key", ["DATES_TO", "MEMBERS_TO", "CHUNKS_TO"])
def test_get_jobs_filtered_no_filter_falls_to_empty(filter_key, as_conf):
    """No filter at a level + no natural key falls through to the default return {}."""
    date = datetime(2000, 1, 1)
    as_conf.experiment_data = {
        'DEFAULT': {'EXPID': 'a000'},
        'STORAGE': {'TYPE': 'pkl', 'COPY_REMOTE_LOGS': True},
        'EXPERIMENT': {
            'DATELIST': '20000101', 'MEMBERS': 'fc0', 'CHUNKSIZEUNIT': 'day',
            'CHUNKSIZE': 1, 'NUMCHUNKS': 1, 'CHUNKINI': '', 'CALENDAR': 'standard',
        },
        'JOBS': {
            'SIM': {
                'FILE': 'sim.sh', 'RUNNING': 'chunk',
                'SPLITS': 2, 'WALLCLOCK': '00:05',
            },
        },
    }
    dictionary = DicJobs([date], ["fc0"], [1], "", default_retrials=0, as_conf=as_conf)
    dictionary.read_section("SIM", 1, "bash")

    job = Job("a000_20000101_fc0_1_1_SIM", 0, Status.WAITING, 1)
    job.date = date
    job.member = "fc0"
    job.chunk = 1
    job.running = "chunk"
    job.split = 1
    job.splits = 2

    filters_to = {}
    natural_date = None if filter_key == "DATES_TO" else date
    natural_member = None if filter_key == "MEMBERS_TO" else "fc0"
    natural_chunk = None if filter_key == "CHUNKS_TO" else 1
    result = dictionary.get_jobs_filtered("SIM", job, filters_to, natural_date, natural_member, natural_chunk, {})
    assert result == [], f"Expected empty list for missing {filter_key}, got {result}"


def test_get_jobs_filtered_split_slice_zero_raises(as_conf):
    """A grouped split filter with slice size 0 must raise a clear ValueError."""
    date = datetime(2000, 1, 1)
    as_conf.experiment_data = {
        'DEFAULT': {'EXPID': 'a000'},
        'STORAGE': {'TYPE': 'pkl', 'COPY_REMOTE_LOGS': True},
        'EXPERIMENT': {
            'DATELIST': '20000101', 'MEMBERS': 'fc0', 'CHUNKSIZEUNIT': 'day',
            'CHUNKSIZE': 1, 'NUMCHUNKS': 1, 'CHUNKINI': '', 'CALENDAR': 'standard',
        },
        'JOBS': {
            'PARENT': {
                'FILE': 'parent.sh', 'RUNNING': 'chunk',
                'SPLITS': 4, 'WALLCLOCK': '00:10',
            },
        },
    }
    dictionary = DicJobs([date], ["fc0"], [1], "", default_retrials=0, as_conf=as_conf)
    dictionary.read_section("PARENT", 1, "bash")

    job = Job("a000_20000101_fc0_1_1_CHILD", 1, Status.WAITING, 1)
    job.date = date
    job.member = "fc0"
    job.chunk = 1
    job.running = "chunk"
    job.split = 1
    job.splits = 4

    filters_to = {'SPLITS_TO': "1*\\0"}
    with pytest.raises(ValueError, match="slice size must be > 0"):
        dictionary.get_jobs_filtered("PARENT", job, filters_to, date, "fc0", 1, {})


@pytest.mark.parametrize("split_value", [-1, 0, None])
def test_get_jobs_filtered_negative_split_with_star(split_value, as_conf):
    """Star-mapping with an invalid split index must not silently index from the end."""
    date = datetime(2000, 1, 1)
    as_conf.experiment_data = {
        'DEFAULT': {'EXPID': 'a000'},
        'STORAGE': {'TYPE': 'pkl', 'COPY_REMOTE_LOGS': True},
        'EXPERIMENT': {
            'DATELIST': '20000101', 'MEMBERS': 'fc0', 'CHUNKSIZEUNIT': 'day',
            'CHUNKSIZE': 1, 'NUMCHUNKS': 1, 'CHUNKINI': '', 'CALENDAR': 'standard',
        },
        'JOBS': {
            'PARENT': {
                'FILE': 'parent.sh', 'RUNNING': 'chunk',
                'SPLITS': 4, 'WALLCLOCK': '00:10',
            },
        },
    }
    dictionary = DicJobs([date], ["fc0"], [1], "", default_retrials=0, as_conf=as_conf)
    dictionary.read_section("PARENT", 1, "bash")

    job = Job("a000_20000101_fc0_1_1_CHILD", 1, Status.WAITING, 1)
    job.date = date
    job.member = "fc0"
    job.chunk = 1
    job.running = "chunk"
    job.split = split_value
    job.splits = 4

    # 1-to-1 star mapping
    filters_to = {'SPLITS_TO': "1*"}
    result = dictionary.get_jobs_filtered("PARENT", job, filters_to, date, "fc0", 1, {})
    assert result == [], f"Expected empty list for split={split_value} with star mapping, got {result}"

    # Grouped star mapping
    filters_to = {'SPLITS_TO': "1*\\2"}
    result = dictionary.get_jobs_filtered("PARENT", job, filters_to, date, "fc0", 1, {})
    assert result == [], f"Expected empty list for split={split_value} with grouped star mapping, got {result}"


def _make_parent_jobs(count, prefix="PARENT", splits=1):
    jobs = []
    for i in range(count):
        job = Job(f"{prefix}_{i}", i, Status.WAITING, 1)
        job.splits = splits
        jobs.append(job)
    return jobs


def _make_split_jobs(split_values, prefix="parent"):
    jobs = []
    for v in split_values:
        job = Job(f"{prefix}_{v}", 1, Status.WAITING, 1)
        job.split = v
        jobs.append(job)
    return jobs


@pytest.mark.parametrize("part,parent_count,parent_splits,job_split,job_splits,previous,expected_idx", [
    pytest.param("1*",    0, 1, 1, 1, False, -1, id="empty-list"),
    pytest.param("1*",    5, 1, 1, 1, False,  0, id="simple-1to1-first"),
    pytest.param("1*",    5, 1, 3, 1, False,  2, id="simple-1to1-middle"),
    pytest.param("1*",    5, 1, 5, 1, False,  4, id="simple-1to1-last"),
    pytest.param("1*",    3, 1, 10, 1, False, -1, id="simple-out-of-range"),
    pytest.param("1*\\2", 0, 1, 1, 1, False, -1, id="grouped-empty-list"),
    pytest.param("1*\\2", 2, 2, 5, 8, True,   1, id="nto1-previous"),
    pytest.param("1*\\2", 3, 8, 5, 2, True,  -1, id="1ton-previous-empty-slice"),
])
def test_resolve_star_splits(part, parent_count, parent_splits, job_split, job_splits, previous, expected_idx):
    """All _resolve_star_splits branches: empty guard, simple 1-to-1, grouped with/without previous."""
    parents = _make_parent_jobs(parent_count, splits=parent_splits)
    job = Job("child", 1, Status.WAITING, 1)
    job.split = job_split
    job.splits = job_splits
    filters_to_of_parent = {"SPLITS_TO": "previous"} if previous else {}
    result = DicJobs._resolve_star_splits(parents, job, part, filters_to_of_parent)
    if expected_idx == -1:
        assert result == set()
    else:
        assert result == {parents[expected_idx]}


@pytest.mark.parametrize("filter_value,split_values,child_split,expected_splits", [
    pytest.param("none",    [None, -1, 0, 1, 2, 3], None,  {None, -1, 0},    id="none"),
    pytest.param("natural", [-1, 0, 1, 2, 3, 4],    2,     {-1, 0, 2},       id="natural"),
])
def test_apply_splits_filter(as_conf, filter_value, split_values, child_split, expected_splits):
    """SPLITS_TO=none and SPLITS_TO=natural filter correct splits."""
    dictionary = DicJobs([], [], [], "", default_retrials=0, as_conf=as_conf)
    parents = _make_split_jobs(split_values)
    child = Job("child", 2, Status.WAITING, 1)
    child.split = child_split
    result = dictionary._apply_splits_filter(parents, child, filter_value, {})
    assert child.name not in {j.name for j in result}
    assert {j.split for j in result} == expected_splits


@pytest.mark.parametrize("relationships_input,expected", [
    pytest.param(
        {"DATES_TO": "20220101", "MEMBERS_TO": "fc0"},
        {"DATES_TO": "20220101", "MEMBERS_TO": "fc0"},
        id="keeps_to_keys",
    ),
    pytest.param(
        {
            "STATUS": "COMPLETED",
            "FROM_STEP": 0, "ACCEPTED_STATUSES": "COMPLETED",
            "DATES_TO": "20220101",
        },
        {"DATES_TO": "20220101"},
        id="filters_out_non_to_keys",
    ),
    pytest.param(
        {"FROM_STEP": 5},
        {},
        id="only_non_to_keys_returns_empty",
    ),
    pytest.param({}, {}, id="empty_input_returns_empty"),
    pytest.param(
        {"DATES_TO": "natural", "MEMBERS_TO": "natural",
         "CHUNKS_TO": "natural", "SPLITS_TO": "natural"},
        {},
        id="all_natural_returns_empty",
    ),
    pytest.param(
        {"DATES_TO": "natural", "MEMBERS_TO": "fc0"},
        {"DATES_TO": "natural", "MEMBERS_TO": "fc0"},
        id="mixed_natural_and_non_natural",
    ),
])
def test_get_filters_to_apply(joblist, mocker, relationships_input, expected):
    mocker.patch.object(joblist, "_filter_current_job", return_value=relationships_input)
    dependency = Dependency(section="test")
    result = joblist.get_filters_to_apply(mocker.Mock(), dependency)
    assert result == expected
