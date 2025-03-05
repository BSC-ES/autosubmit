from unittest.mock import Mock

import inspect
import mock
import tempfile
import unittest
from copy import deepcopy
from datetime import datetime

from mock import patch
from mock.mock import MagicMock

from autosubmit.autosubmit import Autosubmit
from autosubmit.job.job_dict import DicJobs
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_list import JobList
from autosubmit.job.job_list_persistence import JobListPersistenceDb
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory

from networkx import DiGraph
from autosubmit.job.job_utils import Dependency


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


class TestJobList(unittest.TestCase):
    def setUp(self):
        self.experiment_id = 'random-id'
        self.as_conf = mock.Mock()
        self.as_conf.experiment_data = dict()
        self.as_conf.experiment_data["JOBS"] = dict()
        self.as_conf.jobs_data = self.as_conf.experiment_data["JOBS"]
        self.as_conf.experiment_data["PLATFORMS"] = dict()
        self.temp_directory = tempfile.mkdtemp()
        self.JobList = JobList(self.experiment_id, self.as_conf, YAMLParserFactory(),
                               JobListPersistenceDb(self.temp_directory, 'db'))
        self.date_list = ["20020201", "20020202", "20020203", "20020204", "20020205", "20020206", "20020207",
                          "20020208", "20020209", "20020210"]
        self.member_list = ["fc1", "fc2", "fc3", "fc4", "fc5", "fc6", "fc7", "fc8", "fc9", "fc10"]
        self.chunk_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        self.split_list = [1, 2, 3, 4, 5]
        self.JobList._date_list = self.date_list
        self.JobList._member_list = self.member_list
        self.JobList._chunk_list = self.chunk_list
        self.JobList._split_list = self.split_list

        # Define common test case inputs here
        self.relationships_dates = {
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
        self.relationships_dates_optional = deepcopy(self.relationships_dates)
        self.relationships_dates_optional["DATES_FROM"]["20020201"]["MEMBERS_FROM"] = {
            "fc2?": {"DATES_TO": "20020201", "MEMBERS_TO": "fc2", "CHUNKS_TO": "all", "SPLITS_TO": "5"}}
        self.relationships_dates_optional["DATES_FROM"]["20020201"]["SPLITS_FROM"] = {"ALL": {"SPLITS_TO": "1?"}}

        self.relationships_members = {
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
        self.relationships_chunks = {
            "CHUNKS_FROM": {
                "1": {
                    "DATES_TO": "20020201",
                    "MEMBERS_TO": "fc2",
                    "CHUNKS_TO": "all",
                    "SPLITS_TO": "1"
                }
            }
        }
        self.relationships_chunks2 = {
            "CHUNKS_FROM": {
                "1": {
                    "DATES_TO": "20020201",
                    "MEMBERS_TO": "fc2",
                    "CHUNKS_TO": "all",
                    "SPLITS_TO": "1"
                },
                "2": {
                    "SPLITS_FROM": {
                        "5": {
                            "SPLITS_TO": "2"
                        }
                    }
                }
            }
        }

        self.relationships_splits = {
            "SPLITS_FROM": {
                "1": {
                    "DATES_TO": "20020201",
                    "MEMBERS_TO": "fc2",
                    "CHUNKS_TO": "all",
                    "SPLITS_TO": "1"
                }
            }
        }

        self.relationships_general = {
            "DATES_TO": "20020201",
            "MEMBERS_TO": "fc2",
            "CHUNKS_TO": "all",
            "SPLITS_TO": "1"
        }
        self.relationships_general_1_to_1 = {
            "DATES_TO": "20020201",
            "MEMBERS_TO": "fc2",
            "CHUNKS_TO": "all",
            "SPLITS_TO": "1*,2*,3*,4*,5*"
        }
        # Create a mock Job object
        self.mock_job = Mock(wraps=Job)

        # Set the attributes on the mock object
        self.mock_job.name = "Job1"
        self.mock_job.job_id = 1
        self.mock_job.status = Status.READY
        self.mock_job.priority = 1
        self.mock_job.date = None
        self.mock_job.member = None
        self.mock_job.chunk = None
        self.mock_job.split = None

    def test_unify_to_filter(self):
        """Test the _unify_to_fitler function"""
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
        result = self.JobList._unify_to_filter(unified_filter, filter_to, filter_type)
        expected_output = \
            {
                "DATES_TO": "20020201,20020205,20020207,20020208,",
                "MEMBERS_TO": "fc2",
                "CHUNKS_TO": "all",
                "SPLITS_TO": "1"
            }
        self.assertEqual(result, expected_output)


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
        result = self.JobList._unify_to_filter(unified_filter, filter_to, filter_type)
        expected_output = \
            {
                "DATES_TO": "20020201",
                "MEMBERS_TO": "fc2",
                "CHUNKS_TO": "all",
                "SPLITS_TO": "all,"
            }
        self.assertEqual(result, expected_output)


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
        result = self.JobList._unify_to_filter(unified_filter, filter_to, filter_type)
        expected_output = \
            {
                "DATES_TO": "20020201",
                "MEMBERS_TO": "fc2",
                "CHUNKS_TO": "all",
                "SPLITS_TO": "1,2,3,"
            }
        self.assertEqual(result, expected_output)


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
        result = self.JobList._unify_to_filter(unified_filter, filter_to, filter_type)
        expected_output = \
            {
                "DATES_TO": "20020201",
                "MEMBERS_TO": "fc2",
                "CHUNKS_TO": "all",
                "SPLITS_TO": "1,2,natural,"
            }
        self.assertEqual(result, expected_output)


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
        result = self.JobList._unify_to_filter(unified_filter, filter_to, filter_type)
        expected_output = \
            {
                "DATES_TO": "20020201",
                "MEMBERS_TO": "fc2",
                "CHUNKS_TO": "all",
                "SPLITS_TO": "okok']},"
            }
        self.assertEqual(result, expected_output)


    def test_simple_dependency(self):
        result_d = self.JobList._check_dates({}, self.mock_job)
        result_m = self.JobList._check_members({}, self.mock_job)
        result_c = self.JobList._check_chunks({}, self.mock_job)
        result_s = self.JobList._check_splits({}, self.mock_job)
        self.assertEqual(result_d, {})
        self.assertEqual(result_m, {})
        self.assertEqual(result_c, {})
        self.assertEqual(result_s, {})

    def test_parse_filters_to_check(self):
        """Test the _parse_filters_to_check function"""
        result = self.JobList._parse_filters_to_check("20020201,20020202,20020203", self.date_list)
        expected_output = ["20020201", "20020202", "20020203"]
        self.assertEqual(result, expected_output)
        result = self.JobList._parse_filters_to_check("20020201,[20020203:20020205]", self.date_list)
        expected_output = ["20020201", "20020203", "20020204", "20020205"]
        self.assertEqual(result, expected_output)
        result = self.JobList._parse_filters_to_check("[20020201:20020203],[20020205:20020207]", self.date_list)
        expected_output = ["20020201", "20020202", "20020203", "20020205", "20020206", "20020207"]
        self.assertEqual(result, expected_output)
        result = self.JobList._parse_filters_to_check("20020201", self.date_list)
        expected_output = ["20020201"]
        self.assertEqual(result, expected_output)

    def test_parse_filter_to_check(self):
        # Call the function to get the result
        # Value can have the following formats:
        # a range: [0:], [:N], [0:N], [:-1], [0:N:M] ...
        # a value: N
        # a range with step: [0::M], [::2], [0::3], [::3] ...
        result = self.JobList._parse_filter_to_check("20020201", self.date_list)
        expected_output = ["20020201"]
        self.assertEqual(result, expected_output)
        result = self.JobList._parse_filter_to_check("[20020201:20020203]", self.date_list)
        expected_output = ["20020201", "20020202", "20020203"]
        self.assertEqual(result, expected_output)
        result = self.JobList._parse_filter_to_check("[20020201:20020203:2]", self.date_list)
        expected_output = ["20020201", "20020203"]
        self.assertEqual(result, expected_output)
        result = self.JobList._parse_filter_to_check("[20020202:]", self.date_list)
        expected_output = self.date_list[1:]
        self.assertEqual(result, expected_output)
        result = self.JobList._parse_filter_to_check("[:20020203]", self.date_list)
        expected_output = self.date_list[:3]
        self.assertEqual(result, expected_output)
        result = self.JobList._parse_filter_to_check("[::2]", self.date_list)
        expected_output = self.date_list[::2]
        self.assertEqual(result, expected_output)
        result = self.JobList._parse_filter_to_check("[20020203::]", self.date_list)
        expected_output = self.date_list[2:]
        self.assertEqual(result, expected_output)
        result = self.JobList._parse_filter_to_check("[:20020203:]", self.date_list)
        expected_output = self.date_list[:3]
        self.assertEqual(result, expected_output)
        # test with a member N:N
        result = self.JobList._parse_filter_to_check("[fc2:fc3]", self.member_list)
        expected_output = ["fc2", "fc3"]
        self.assertEqual(result, expected_output)
        # test with a chunk
        result = self.JobList._parse_filter_to_check("[1:2]", self.chunk_list, level_to_check="CHUNKS_FROM")
        expected_output = [1, 2]
        self.assertEqual(result, expected_output)
        # test with a split
        result = self.JobList._parse_filter_to_check("[1:2]", self.split_list, level_to_check="SPLITS_FROM")
        expected_output = [1, 2]
        self.assertEqual(result, expected_output)

    def test_check_dates(self):
        """
        Call the function to get the result
        """
        self.mock_job.date = datetime.strptime("20020201", "%Y%m%d")
        self.mock_job.member = "fc2"
        self.mock_job.chunk = 1
        self.mock_job.split = 1

        self.relationships_dates["DATES_FROM"]["20020201"].update(self.relationships_chunks)

        result = self.JobList._check_dates(self.relationships_dates, self.mock_job)
        expected_output = {
            "DATES_TO": "20020201",
            "MEMBERS_TO": "fc2",
            "CHUNKS_TO": "all",
            "SPLITS_TO": "1"
        }
        self.assertEqual(result, expected_output)

        self.relationships_dates["DATES_FROM"]["20020201"]["MEMBERS_FROM"] = {}
        self.relationships_dates["DATES_FROM"]["20020201"]["CHUNKS_FROM"] = {}
        self.relationships_dates["DATES_FROM"]["20020201"]["SPLITS_FROM"] = {}

        result = self.JobList._check_dates(self.relationships_dates, self.mock_job)
        expected_output = {
            "DATES_TO": "none",
            "MEMBERS_TO": "none",
            "CHUNKS_TO": "none",
            "SPLITS_TO": "none"
        }
        self.assertEqual(result, expected_output)

        # failure
        self.mock_job.date = datetime.strptime("20020301", "%Y%m%d")
        result = self.JobList._check_dates(self.relationships_dates, self.mock_job)
        self.assertEqual(result, {})


    def test_check_members(self):
        """
        Call the function to get the result
        """
        self.mock_job.date = datetime.strptime("20020201", "%Y%m%d")
        self.mock_job.member = "fc2"

        result = self.JobList._check_members(self.relationships_members, self.mock_job)
        expected_output = {
            "DATES_TO": "20020201",
            "MEMBERS_TO": "fc2",
            "CHUNKS_TO": "all",
            "SPLITS_TO": "1"
        }
        self.assertEqual(result, expected_output)

        self.relationships_members["MEMBERS_FROM"]["fc2"].update(self.relationships_chunks)

        result = self.JobList._check_members(self.relationships_members, self.mock_job)
        expected_output = {
            "DATES_TO": "20020201",
            "MEMBERS_TO": "fc2",
            "CHUNKS_TO": "all",
            "SPLITS_TO": "1"
        }
        self.assertEqual(result, expected_output)

        self.relationships_members["MEMBERS_FROM"]["fc2"]["CHUNKS_FROM"] = {}
        self.relationships_members["MEMBERS_FROM"]["fc2"]["SPLITS_FROM"] = {}

        result = self.JobList._check_members(self.relationships_members, self.mock_job)
        expected_output = {
            "DATES_TO": "none",
            "MEMBERS_TO": "none",
            "CHUNKS_TO": "none",
            "SPLITS_TO": "none"
        }
        self.assertEqual(result, expected_output)

        self.mock_job.member = "fc3"
        result = self.JobList._check_members(self.relationships_members, self.mock_job)
        self.assertEqual(result, {})

        # FAILURE
        self.mock_job.member = "fc99"
        result = self.JobList._check_members(self.relationships_members, self.mock_job)
        self.assertEqual(result, {})

    def test_check_splits(self):
        # Call the function to get the result

        self.mock_job.split = 1
        result = self.JobList._check_splits(self.relationships_splits, self.mock_job)
        expected_output = {
            "DATES_TO": "20020201",
            "MEMBERS_TO": "fc2",
            "CHUNKS_TO": "all",
            "SPLITS_TO": "1"
        }
        self.assertEqual(result, expected_output)
        self.mock_job.split = 2
        result = self.JobList._check_splits(self.relationships_splits, self.mock_job)
        self.assertEqual(result, {})
        # failure
        self.mock_job.split = 99
        result = self.JobList._check_splits(self.relationships_splits, self.mock_job)
        self.assertEqual(result, {})

    def test_check_chunks(self):
        """
        Call the function to get the result
        """

        self.mock_job.chunk = 1

        chunks = {
            "CHUNKS_FROM": {
                "1": {
                    "SPLITS_FROM": { "5": {"SPLITS_TO": "4"} }
                }
            }
        }

        result = self.JobList._check_chunks(chunks, self.mock_job)
        expected_output = {'SPLITS_TO': '4'}

        self.assertEqual(result, expected_output)
        chunks = {
            "CHUNKS_FROM": {
                "1": { "SPLITS_FROM": { } }
            }
        }

        result = self.JobList._check_chunks(chunks, self.mock_job)
        expected_output = {'DATES_TO': 'none', 'MEMBERS_TO': 'none', 'CHUNKS_TO': 'none', 'SPLITS_TO': 'none'}
        self.assertEqual(result, expected_output)

        self.mock_job.chunk = 2
        result = self.JobList._check_chunks(self.relationships_chunks, self.mock_job)
        self.assertEqual(result, {})

        # failure
        self.mock_job.chunk = 99
        result = self.JobList._check_chunks(self.relationships_chunks, self.mock_job)
        self.assertEqual(result, {})

    def test_check_general(self):
        # Call the function to get the result

        self.mock_job.date = datetime.strptime("20020201", "%Y%m%d")
        self.mock_job.member = "fc2"
        self.mock_job.chunk = 1
        self.mock_job.split = 1
        result = self.JobList._filter_current_job(self.mock_job, self.relationships_general)
        expected_output = {
            "DATES_TO": "20020201",
            "MEMBERS_TO": "fc2",
            "CHUNKS_TO": "all",
            "SPLITS_TO": "1"
        }
        self.assertEqual(result, expected_output)

    def test_check_relationship(self):
        relationships = {'MEMBERS_FROM': {
            'TestMember,   TestMember2,TestMember3   ': {'CHUNKS_TO': 'None', 'DATES_TO': 'None', 'FROM_STEP': None,
                                                         'MEMBERS_TO': 'None', 'STATUS': None}}}
        level_to_check = "MEMBERS_FROM"
        value_to_check = "TestMember"
        result = self.JobList._check_relationship(relationships, level_to_check, value_to_check)
        expected_output = [
            {'CHUNKS_TO': 'None', 'DATES_TO': 'None', 'FROM_STEP': None, 'MEMBERS_TO': 'None', 'STATUS': None}]
        self.assertEqual(result, expected_output)
        value_to_check = "TestMember2"
        result = self.JobList._check_relationship(relationships, level_to_check, value_to_check)
        expected_output = [
            {'CHUNKS_TO': 'None', 'DATES_TO': 'None', 'FROM_STEP': None, 'MEMBERS_TO': 'None', 'STATUS': None}]
        self.assertEqual(result, expected_output)
        value_to_check = "TestMember3"
        result = self.JobList._check_relationship(relationships, level_to_check, value_to_check)
        expected_output = [
            {'CHUNKS_TO': 'None', 'DATES_TO': 'None', 'FROM_STEP': None, 'MEMBERS_TO': 'None', 'STATUS': None}]
        self.assertEqual(result, expected_output)
        value_to_check = "TestMember   "
        result = self.JobList._check_relationship(relationships, level_to_check, value_to_check)
        expected_output = [
            {'CHUNKS_TO': 'None', 'DATES_TO': 'None', 'FROM_STEP': None, 'MEMBERS_TO': 'None', 'STATUS': None}]
        self.assertEqual(result, expected_output)
        value_to_check = "   TestMember"
        result = self.JobList._check_relationship(relationships, level_to_check, value_to_check)
        expected_output = [
            {'CHUNKS_TO': 'None', 'DATES_TO': 'None', 'FROM_STEP': None, 'MEMBERS_TO': 'None', 'STATUS': None}]
        self.assertEqual(result, expected_output)
        relationships = {'DATES_FROM': {
            '20000101, 20000102, 20000103 ': {'CHUNKS_TO': 'None', 'DATES_TO': 'None', 'FROM_STEP': None,
                                                         'MEMBERS_TO': 'None', 'STATUS': True}}}
        value_to_check = datetime(2000, 1, 1)
        result = self.JobList._check_relationship(relationships, "DATES_FROM", value_to_check)
        expected_output = [
            {'CHUNKS_TO': 'None', 'DATES_TO': 'None', 'FROM_STEP': None, 'MEMBERS_TO': 'None', 'STATUS': True}]
        self.assertEqual(result, expected_output)

    def test_add_special_conditions(self):
        # Method from job_list
        job = Job("child", 1, Status.READY, 1)
        job.section = "child_one"
        job.date = datetime.strptime("20200128", "%Y%m%d")
        job.member = "fc0"
        job.chunk = 1
        job.split = 1
        job.splits = 1
        job.max_checkpoint_step = 0
        special_conditions = {"STATUS": "RUNNING", "FROM_STEP": "2"}
        only_marked_status = False
        filters_to_apply = {"DATES_TO": "all", "MEMBERS_TO": "all", "CHUNKS_TO": "all", "SPLITS_TO": "all"}
        parent = Job("parent", 1, Status.READY, 1)
        parent.section = "parent_one"
        parent.date = datetime.strptime("20200128", "%Y%m%d")
        parent.member = "fc0"
        parent.chunk = 1
        parent.split = 1
        parent.splits = 1
        parent.max_checkpoint_step = 0
        job.status = Status.READY
        job_list = Mock(wraps=self.JobList)
        job_list._job_list = [job, parent]
        job_list.add_special_conditions(job, special_conditions, filters_to_apply, parent)
        # self.JobList.jobs_edges
        # job.edges = self.JobList.jobs_edges[job.name]
        # assert
        self.assertEqual(job.max_checkpoint_step, 2)
        value = job.edge_info.get("RUNNING", "").get("parent", ())
        self.assertEqual((value[0].name, value[1]), (parent.name, "2"))
        self.assertEqual(len(job.edge_info.get("RUNNING", "")), 1)

        self.assertEqual(str(job_list.jobs_edges.get("RUNNING", ())), str({job}))
        only_marked_status = False
        parent2 = Job("parent2", 1, Status.READY, 1)
        parent2.section = "parent_two"
        parent2.date = datetime.strptime("20200128", "%Y%m%d")
        parent2.member = "fc0"
        parent2.chunk = 1

        job_list.add_special_conditions(job, special_conditions, filters_to_apply, parent2)
        value = job.edge_info.get("RUNNING", "").get("parent2", ())
        self.assertEqual(len(job.edge_info.get("RUNNING", "")), 2)
        self.assertEqual((value[0].name, value[1]), (parent2.name, "2"))
        self.assertEqual(str(job_list.jobs_edges.get("RUNNING", ())), str({job}))
        job_list.add_special_conditions(job, special_conditions, filters_to_apply, parent2)
        self.assertEqual(len(job.edge_info.get("RUNNING", "")), 2)

    def test_add_special_conditions_chunks_to_once(self):
        # Method from job_list
        job = Job("child", 1, Status.WAITING, 1)
        job.section = "child_one"
        job.date = datetime.strptime("20200128", "%Y%m%d")
        job.member = "fc0"
        job.chunk = 1
        job.split = 1
        job.splits = 1
        job.max_checkpoint_step = 0

        job_two = Job("child", 1, Status.WAITING, 1)
        job_two.section = "child_one"
        job_two.date = datetime.strptime("20200128", "%Y%m%d")
        job_two.member = "fc0"
        job_two.chunk = 2
        job_two.split = 1
        job_two.splits = 1
        job_two.max_checkpoint_step = 0

        special_conditions = {"STATUS": "RUNNING", "FROM_STEP": "1"}
        special_conditions_two = {"STATUS": "RUNNING", "FROM_STEP": "2"}

        parent = Job("parent", 1, Status.RUNNING, 1)
        parent.section = "parent_one"
        parent.date = datetime.strptime("20200128", "%Y%m%d")
        parent.member = None
        parent.chunk = None
        parent.split = None
        parent.splits = None
        parent.max_checkpoint_step = 0
        job.status = Status.WAITING
        job_two.status = Status.WAITING

        job_list = Mock(wraps=self.JobList)
        job_list._job_list = [job, job_two, parent]

        dependency = MagicMock()
        dependency.relationships = {'CHUNKS_FROM': {'1': {'FROM_STEP': '1'}, '2': {'FROM_STEP': '2'}, }, 'STATUS': 'RUNNING'}
        filters_to_apply = job_list.get_filters_to_apply(job, dependency)
        filters_to_apply_two = job_list.get_filters_to_apply(job_two, dependency)

        assert filters_to_apply == {}
        assert filters_to_apply_two == {}

        job_list.add_special_conditions(job, special_conditions, filters_to_apply, parent)
        job_list.add_special_conditions(job_two, special_conditions_two, filters_to_apply_two, parent)

        dependency = MagicMock()
        dependency.relationships = {'CHUNKS_FROM': {'1': {'FROM_STEP': '1', 'CHUNKS_TO':'natural'}, '2': {'FROM_STEP': '2', 'CHUNKS_TO':'natural'}, }, 'STATUS': 'RUNNING'}
        filters_to_apply = job_list.get_filters_to_apply(job, dependency)
        filters_to_apply_two = job_list.get_filters_to_apply(job_two, dependency)

        assert filters_to_apply == {}
        assert filters_to_apply_two == {}

        job_list.add_special_conditions(job, special_conditions, filters_to_apply, parent)
        job_list.add_special_conditions(job_two, special_conditions_two, filters_to_apply_two, parent)

        self.assertEqual(job.max_checkpoint_step, 1)
        self.assertEqual(job_two.max_checkpoint_step, 2)

        value = job.edge_info.get("RUNNING", "").get("parent", ())
        self.assertEqual((value[0].name, value[1]), (parent.name, "1"))
        self.assertEqual(len(job.edge_info.get("RUNNING", "")), 1)

        value_two = job_two.edge_info.get("RUNNING", "").get("parent", ())
        self.assertEqual((value_two[0].name, value_two[1]), (parent.name, "2"))
        self.assertEqual(len(job_two.edge_info.get("RUNNING", "")), 1)

        dependency = MagicMock()
        dependency.relationships = {'CHUNKS_FROM': {'1': {'FROM_STEP': '1', 'CHUNKS_TO':'natural', 'DATES_TO': "dummy"}, '2': {'FROM_STEP': '2', 'CHUNKS_TO':'natural', 'DATES_TO': "dummy"}, }, 'STATUS': 'RUNNING'}
        filters_to_apply = job_list.get_filters_to_apply(job, dependency)
        filters_to_apply_two = job_list.get_filters_to_apply(job_two, dependency)

        assert filters_to_apply == {'CHUNKS_TO': 'natural', 'DATES_TO': 'dummy'}
        assert filters_to_apply_two == {'CHUNKS_TO': 'natural', 'DATES_TO': 'dummy'}

    @patch('autosubmit.job.job_dict.date2str')
    def test_jobdict_get_jobs_filtered(self, mock_date2str):
        # Test the get_jobs_filtered function
        # DicJobs.get_jobs_filtered(self, section, job, filters_to, natural_date, natural_member, natural_chunk,filters_to_of_parent)
        as_conf = mock.Mock()
        as_conf.experiment_data = dict()
        as_conf.experiment_data = {'CONFIG': {'AUTOSUBMIT_VERSION': '4.1.2', 'MAXWAITINGJOBS': 20, 'TOTALJOBS': 20, 'SAFETYSLEEPTIME': 10, 'RETRIALS': 0}, 'MAIL': {'NOTIFICATIONS': False, 'TO': None}, 'STORAGE': {'TYPE': 'pkl', 'COPY_REMOTE_LOGS': True}, 'DEFAULT': {'EXPID': 'a03b', 'HPCARCH': 'marenostrum4'}, 'EXPERIMENT': {'DATELIST': '20000101', 'MEMBERS': 'fc0', 'CHUNKSIZEUNIT': 'month', 'CHUNKSIZE': 4, 'NUMCHUNKS': 5, 'CHUNKINI': '', 'CALENDAR': 'standard'}, 'PROJECT': {'PROJECT_TYPE': 'none', 'PROJECT_DESTINATION': ''}, 'GIT': {'PROJECT_ORIGIN': '', 'PROJECT_BRANCH': '', 'PROJECT_COMMIT': '', 'PROJECT_SUBMODULES': '', 'FETCH_SINGLE_BRANCH': True}, 'SVN': {'PROJECT_URL': '', 'PROJECT_REVISION': ''}, 'LOCAL': {'PROJECT_PATH': ''}, 'PROJECT_FILES': {'FILE_PROJECT_CONF': '', 'FILE_JOBS_CONF': '', 'JOB_SCRIPTS_TYPE': ''}, 'RERUN': {'RERUN': False, 'RERUN_JOBLIST': ''}, 'JOBS': {'SIM': {'FILE': 'SIM.sh', 'RUNNING': 'once', 'DEPENDENCIES': {'SIM-1': {'CHUNKS_FROM': {'ALL': {'SPLITS_TO': 'previous'}}}}, 'WALLCLOCK': '00:05', 'SPLITS': 10, 'ADDITIONAL_FILES': []}, 'TEST': {'FILE': 'Test.sh', 'DEPENDENCIES': {'TEST-1': {'CHUNKS_FROM': {'ALL': {'SPLITS_TO': 'previous'}}}, 'SIM': None}, 'RUNNING': 'once', 'WALLCLOCK': '00:05', 'SPLITS': 10, 'ADDITIONAL_FILES': []}}, 'PLATFORMS': {'MARENOSTRUM4': {'TYPE': 'slurm', 'HOST': 'mn2.bsc.es', 'PROJECT': 'bsc32', 'USER': 'bsc32070', 'QUEUE': 'debug', 'SCRATCH_DIR': '/gpfs/scratch', 'ADD_PROJECT_TO_HOST': False, 'MAX_WALLCLOCK': '48:00', 'TEMP_DIR': ''}, 'MARENOSTRUM_ARCHIVE': {'TYPE': 'ps', 'HOST': 'dt02.bsc.es', 'PROJECT': 'bsc32', 'USER': None, 'SCRATCH_DIR': '/gpfs/scratch', 'ADD_PROJECT_TO_HOST': False, 'TEST_SUITE': False}, 'TRANSFER_NODE': {'TYPE': 'ps', 'HOST': 'dt01.bsc.es', 'PROJECT': 'bsc32', 'USER': None, 'ADD_PROJECT_TO_HOST': False, 'SCRATCH_DIR': '/gpfs/scratch'}, 'TRANSFER_NODE_BSCEARTH000': {'TYPE': 'ps', 'HOST': 'bscearth000', 'USER': None, 'PROJECT': 'Earth', 'ADD_PROJECT_TO_HOST': False, 'QUEUE': 'serial', 'SCRATCH_DIR': '/esarchive/scratch'}, 'BSCEARTH000': {'TYPE': 'ps', 'HOST': 'bscearth000', 'USER': None, 'PROJECT': 'Earth', 'ADD_PROJECT_TO_HOST': False, 'QUEUE': 'serial', 'SCRATCH_DIR': '/esarchive/scratch'}, 'NORD3': {'TYPE': 'SLURM', 'HOST': 'nord1.bsc.es', 'PROJECT': 'bsc32', 'USER': None, 'QUEUE': 'debug', 'SCRATCH_DIR': '/gpfs/scratch', 'MAX_WALLCLOCK': '48:00'}, 'ECMWF-XC40': {'TYPE': 'ecaccess', 'VERSION': 'pbs', 'HOST': 'cca', 'USER': None, 'PROJECT': 'spesiccf', 'ADD_PROJECT_TO_HOST': False, 'SCRATCH_DIR': '/scratch/ms', 'QUEUE': 'np', 'SERIAL_QUEUE': 'ns', 'MAX_WALLCLOCK': '48:00'}}, 'ROOTDIR': '/home/dbeltran/new_autosubmit/a03b', 'PROJDIR': '/home/dbeltran/new_autosubmit/a03b/proj/'}
        as_conf.jobs_data = as_conf.experiment_data["JOBS"]
        as_conf.last_experiment_data = as_conf.experiment_data
        as_conf.detailed_deep_diff = Mock()
        as_conf.detailed_deep_diff.return_value = {}
        self.dictionary = DicJobs(self.date_list, self.member_list, self.chunk_list, "", default_retrials=0,as_conf=as_conf)
        self.dictionary.read_section("SIM", 1, "bash")
        job = Job("SIM", 1, Status.READY, 1)
        job.date = None
        job.member = None
        job.chunk = None
        job.running = "once"
        job.split = 1
        job.splits = 2
        job.max_checkpoint_step = 0
        job_list = Mock(wraps=self.JobList)
        job_list._job_list = [job]
        filters_to = {'SPLITS_TO': "1*\\1"}
        filters_to_of_parent = {'SPLITS_TO': 'previous'}
        natural_chunk = 1
        natural_member = 'fc0'
        section = 'SIM'
        result = self.dictionary.get_jobs_filtered(section, job, filters_to, None, natural_member, natural_chunk, filters_to_of_parent)
        expected_output = [self.dictionary._dic["SIM"][0]]
        self.assertEqual(expected_output, result)


def test_normalize_auto_keyword(autosubmit_config, mocker):
    as_conf = autosubmit_config('a000', experiment_data={

    })
    job_list = JobList(
        as_conf.expid,
        as_conf,
        YAMLParserFactory(),
        Autosubmit._get_job_list_persistence(as_conf.expid, as_conf)
    )
    dependency = Dependency("test")

    job = Job("a000_20001010_fc1_2_1_test", 1, Status.READY, 1)
    job.running = "chunk"
    job.section = "test"
    job.date = "20001010"
    job.member = "fc1"
    job.splits = 5

    job_minus = Job("a000_20001010_fc1_1_1_minus", 1, Status.READY, 1)
    job_minus.running = "chunk"
    job_minus.section = "minus"
    job_minus.date = "20001010"
    job_minus.member = "fc1"
    job_minus.splits = 40

    job_plus = Job("a000_20001010_fc1_3_1_plus", 1, Status.READY, 1)
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


if __name__ == '__main__':
    unittest.main()
