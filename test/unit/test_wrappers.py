from unittest.mock import patch

import mock
from bscearth.utils.date import sum_str_hours
from operator import attrgetter

import shutil
import tempfile
import inspect

from unittest import TestCase
from mock import MagicMock

import log.log
from autosubmit.job.job_packager import JobPackager
from autosubmit.job.job_packages import JobPackageVertical, JobPackageHorizontal, JobPackageHorizontalVertical, \
    JobPackageVerticalHorizontal, JobPackageSimple
from autosubmit.job.job import Job
from autosubmit.job.job_list import JobList
from autosubmit.job.job_dict import DicJobs
from autosubmit.job.job_utils import Dependency
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory
from autosubmit.job.job_list_persistence import JobListPersistenceDb
from autosubmit.job.job_common import Status
from random import randrange
from collections import OrderedDict


class TestWrappers(TestCase):

    @classmethod
    def setUpClass(cls):
        # set up different unused_figs to be used in the test methods
        cls.workflows = dict()
        cls.workflows['basic'] = dict()
        cls.workflows['synchronize_date'] = dict()
        cls.workflows['synchronize_member'] = dict()
        cls.workflows['running_member'] = dict()
        cls.workflows['running_date'] = dict()
        cls.workflows['running_once'] = dict()

        cls.workflows['basic']['sections'] = OrderedDict()
        cls.workflows['basic']['sections']["s1"] = dict()
        cls.workflows['basic']['sections']["s1"]["RUNNING"] = "member"
        cls.workflows['basic']['sections']["s1"]["WALLCLOCK"] = '00:50'

        cls.workflows['basic']['sections']["s2"] = dict()
        cls.workflows['basic']['sections']["s2"]["RUNNING"] = "chunk"
        cls.workflows['basic']['sections']["s2"]["WALLCLOCK"] = '00:10'
        cls.workflows['basic']['sections']["s2"]["DEPENDENCIES"] = "s1 s2-1"

        cls.workflows['basic']['sections']["s3"] = dict()
        cls.workflows['basic']['sections']["s3"]["RUNNING"] = "chunk"
        cls.workflows['basic']['sections']["s3"]["WALLCLOCK"] = '00:20'
        cls.workflows['basic']['sections']["s3"]["DEPENDENCIES"] = "s2"

        cls.workflows['basic']['sections']["s4"] = dict()
        cls.workflows['basic']['sections']["s4"]["RUNNING"] = "chunk"
        cls.workflows['basic']['sections']["s4"]["WALLCLOCK"] = '00:30'
        cls.workflows['basic']['sections']["s4"]["DEPENDENCIES"] = "s3"

        cls.workflows['synchronize_date']['sections'] = OrderedDict()
        cls.workflows['synchronize_date']['sections']["s1"] = dict()
        cls.workflows['synchronize_date']['sections']["s1"]["RUNNING"] = "member"
        cls.workflows['synchronize_date']['sections']["s1"]["WALLCLOCK"] = '00:50'

        cls.workflows['synchronize_date']['sections']["s2"] = dict()
        cls.workflows['synchronize_date']['sections']["s2"]["RUNNING"] = "chunk"
        cls.workflows['synchronize_date']['sections']["s2"]["WALLCLOCK"] = '00:10'
        cls.workflows['synchronize_date']['sections']["s2"]["DEPENDENCIES"] = "s1 s2-1"

        cls.workflows['synchronize_date']['sections']["s3"] = dict()
        cls.workflows['synchronize_date']['sections']["s3"]["RUNNING"] = "chunk"
        cls.workflows['synchronize_date']['sections']["s3"]["WALLCLOCK"] = '00:20'
        cls.workflows['synchronize_date']['sections']["s3"]["DEPENDENCIES"] = "s2"

        cls.workflows['synchronize_date']['sections']["s4"] = dict()
        cls.workflows['synchronize_date']['sections']["s4"]["RUNNING"] = "chunk"
        cls.workflows['synchronize_date']['sections']["s4"]["WALLCLOCK"] = '00:30'
        cls.workflows['synchronize_date']['sections']["s4"]["DEPENDENCIES"] = "s3"

        cls.workflows['synchronize_date']['sections']["s5"] = dict()
        cls.workflows['synchronize_date']['sections']["s5"]["RUNNING"] = "chunk"
        cls.workflows['synchronize_date']['sections']["s5"]["SYNCHRONIZE"] = "date"
        cls.workflows['synchronize_date']['sections']["s5"]["WALLCLOCK"] = '00:30'
        cls.workflows['synchronize_date']['sections']["s5"]["DEPENDENCIES"] = "s2"

        cls.workflows['synchronize_member']['sections'] = OrderedDict()
        cls.workflows['synchronize_member']['sections']["s1"] = dict()
        cls.workflows['synchronize_member']['sections']["s1"]["RUNNING"] = "member"
        cls.workflows['synchronize_member']['sections']["s1"]["WALLCLOCK"] = '00:50'

        cls.workflows['synchronize_member']['sections']["s2"] = dict()
        cls.workflows['synchronize_member']['sections']["s2"]["RUNNING"] = "chunk"
        cls.workflows['synchronize_member']['sections']["s2"]["WALLCLOCK"] = '00:10'
        cls.workflows['synchronize_member']['sections']["s2"]["DEPENDENCIES"] = "s1 s2-1"

        cls.workflows['synchronize_member']['sections']["s3"] = dict()
        cls.workflows['synchronize_member']['sections']["s3"]["RUNNING"] = "chunk"
        cls.workflows['synchronize_member']['sections']["s3"]["WALLCLOCK"] = '00:20'
        cls.workflows['synchronize_member']['sections']["s3"]["DEPENDENCIES"] = "s2"

        cls.workflows['synchronize_member']['sections']["s4"] = dict()
        cls.workflows['synchronize_member']['sections']["s4"]["RUNNING"] = "chunk"
        cls.workflows['synchronize_member']['sections']["s4"]["WALLCLOCK"] = '00:30'
        cls.workflows['synchronize_member']['sections']["s4"]["DEPENDENCIES"] = "s3"

        cls.workflows['synchronize_member']['sections']["s5"] = dict()
        cls.workflows['synchronize_member']['sections']["s5"]["RUNNING"] = "chunk"
        cls.workflows['synchronize_member']['sections']["s5"]["SYNCHRONIZE"] = "member"
        cls.workflows['synchronize_member']['sections']["s5"]["WALLCLOCK"] = '00:30'
        cls.workflows['synchronize_member']['sections']["s5"]["DEPENDENCIES"] = "s2"

        cls.workflows['running_date']['sections'] = OrderedDict()
        cls.workflows['running_date']['sections']["s1"] = dict()
        cls.workflows['running_date']['sections']["s1"]["RUNNING"] = "member"
        cls.workflows['running_date']['sections']["s1"]["WALLCLOCK"] = '00:50'

        cls.workflows['running_date']['sections']["s2"] = dict()
        cls.workflows['running_date']['sections']["s2"]["RUNNING"] = "chunk"
        cls.workflows['running_date']['sections']["s2"]["WALLCLOCK"] = '00:10'
        cls.workflows['running_date']['sections']["s2"]["DEPENDENCIES"] = "s1 s2-1"

        cls.workflows['running_date']['sections']["s3"] = dict()
        cls.workflows['running_date']['sections']["s3"]["RUNNING"] = "chunk"
        cls.workflows['running_date']['sections']["s3"]["WALLCLOCK"] = '00:20'
        cls.workflows['running_date']['sections']["s3"]["DEPENDENCIES"] = "s2"

        cls.workflows['running_date']['sections']["s4"] = dict()
        cls.workflows['running_date']['sections']["s4"]["RUNNING"] = "chunk"
        cls.workflows['running_date']['sections']["s4"]["WALLCLOCK"] = '00:30'
        cls.workflows['running_date']['sections']["s4"]["DEPENDENCIES"] = "s3"

        cls.workflows['running_date']['sections']["s5"] = dict()
        cls.workflows['running_date']['sections']["s5"]["RUNNING"] = "date"
        cls.workflows['running_date']['sections']["s5"]["WALLCLOCK"] = '00:30'
        cls.workflows['running_date']['sections']["s5"]["DEPENDENCIES"] = "s2"

        cls.workflows['running_once']['sections'] = OrderedDict()
        cls.workflows['running_once']['sections']["s1"] = dict()
        cls.workflows['running_once']['sections']["s1"]["RUNNING"] = "member"
        cls.workflows['running_once']['sections']["s1"]["WALLCLOCK"] = '00:50'

        cls.workflows['running_once']['sections']["s2"] = dict()
        cls.workflows['running_once']['sections']["s2"]["RUNNING"] = "chunk"
        cls.workflows['running_once']['sections']["s2"]["WALLCLOCK"] = '00:10'
        cls.workflows['running_once']['sections']["s2"]["DEPENDENCIES"] = "s1 s2-1"

        cls.workflows['running_once']['sections']["s3"] = dict()
        cls.workflows['running_once']['sections']["s3"]["RUNNING"] = "chunk"
        cls.workflows['running_once']['sections']["s3"]["WALLCLOCK"] = '00:20'
        cls.workflows['running_once']['sections']["s3"]["DEPENDENCIES"] = "s2"

        cls.workflows['running_once']['sections']["s4"] = dict()
        cls.workflows['running_once']['sections']["s4"]["RUNNING"] = "chunk"
        cls.workflows['running_once']['sections']["s4"]["WALLCLOCK"] = '00:30'
        cls.workflows['running_once']['sections']["s4"]["DEPENDENCIES"] = "s3"

        cls.workflows['running_once']['sections']["s5"] = dict()
        cls.workflows['running_once']['sections']["s5"]["RUNNING"] = "once"
        cls.workflows['running_once']['sections']["s5"]["WALLCLOCK"] = '00:30'
        cls.workflows['running_once']['sections']["s5"]["DEPENDENCIES"] = "s2"

    def setUp(self):
        self.experiment_id = 'random-id'
        self._wrapper_factory = MagicMock()

        self.config = FakeBasicConfig
        self._platform = MagicMock()
        self.as_conf = MagicMock()
        self.as_conf.experiment_data = dict()
        self.as_conf.experiment_data["JOBS"] = dict()
        self.as_conf.jobs_data = self.as_conf.experiment_data["JOBS"]

        self.as_conf.experiment_data["PLATFORMS"] = dict()
        self.as_conf.experiment_data["WRAPPERS"] = dict()
        self.temp_directory = tempfile.mkdtemp()
        self.job_list = JobList(self.experiment_id, self.as_conf, YAMLParserFactory(),
                                JobListPersistenceDb(self.temp_directory, 'db'))

        self.parser_mock = MagicMock(spec='SafeConfigParser')

        self._platform.max_waiting_jobs = 100
        self._platform.total_jobs = 100
        self.config.get_wrapper_type = MagicMock(return_value='vertical')
        self.config.get_wrapper_export = MagicMock(return_value='')
        self.config.get_wrapper_jobs = MagicMock(return_value='None')
        self.config.get_wrapper_method = MagicMock(return_value='ASThread')
        self.config.get_wrapper_queue = MagicMock(return_value='debug')
        self.config.get_wrapper_policy = MagicMock(return_value='flexible')
        self.config.get_extensible_wallclock = MagicMock(return_value=0)
        self.config.get_retrials = MagicMock(return_value=0)
        options = {
            'TYPE': "vertical",
            'JOBS_IN_WRAPPER': "None",
            'EXPORT': "none",
            'METHOD': "ASThread",
            'QUEUE': "debug",
            'POLICY': "flexible",
            'RETRIALS': 0,
            'EXTEND_WALLCLOCK': 0
        }
        self.as_conf.experiment_data["WRAPPERS"]["WRAPPERS"] = options
        self.as_conf.experiment_data["WRAPPERS"]["CURRENT_WRAPPER"] = options
        self._wrapper_factory.as_conf = self.as_conf
        self.job_packager = JobPackager(
            self.as_conf, self._platform, self.job_list)
        self.job_list._ordered_jobs_by_date_member["WRAPPERS"] = dict()
        self.wrapper_info = ['vertical', 'flexible', 'asthread', ['SIM'], 0, self.as_conf]

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_directory)

    ### ONE SECTION WRAPPER ###
    def test_returned_packages(self):
        self.current_wrapper_section = {}
        date_list = ["d1", "d2"]
        member_list = ["m1", "m2"]
        chunk_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        for section, s_value in self.workflows['basic']['sections'].items():
            self.as_conf.jobs_data[section] = s_value
        self._createDummyJobs(
            self.workflows['basic'], date_list, member_list, chunk_list)

        self.job_list.get_job_by_name(
            'expid_d1_m1_s1').status = Status.COMPLETED
        self.job_list.get_job_by_name(
            'expid_d1_m2_s1').status = Status.COMPLETED

        self.job_list.get_job_by_name('expid_d1_m1_1_s2').status = Status.READY
        self.job_list.get_job_by_name('expid_d1_m2_1_s2').status = Status.READY

        max_jobs = 20
        max_wrapped_jobs = 20
        max_wallclock = '10:00'

        d1_m1_1_s2 = self.job_list.get_job_by_name('expid_d1_m1_1_s2')
        d1_m1_2_s2 = self.job_list.get_job_by_name('expid_d1_m1_2_s2')
        d1_m1_3_s2 = self.job_list.get_job_by_name('expid_d1_m1_3_s2')
        d1_m1_4_s2 = self.job_list.get_job_by_name('expid_d1_m1_4_s2')
        d1_m1_5_s2 = self.job_list.get_job_by_name('expid_d1_m1_5_s2')
        d1_m1_6_s2 = self.job_list.get_job_by_name('expid_d1_m1_6_s2')
        d1_m1_7_s2 = self.job_list.get_job_by_name('expid_d1_m1_7_s2')
        d1_m1_8_s2 = self.job_list.get_job_by_name('expid_d1_m1_8_s2')
        d1_m1_9_s2 = self.job_list.get_job_by_name('expid_d1_m1_9_s2')
        d1_m1_10_s2 = self.job_list.get_job_by_name('expid_d1_m1_10_s2')

        d1_m2_1_s2 = self.job_list.get_job_by_name('expid_d1_m2_1_s2')
        d1_m2_2_s2 = self.job_list.get_job_by_name('expid_d1_m2_2_s2')
        d1_m2_3_s2 = self.job_list.get_job_by_name('expid_d1_m2_3_s2')
        d1_m2_4_s2 = self.job_list.get_job_by_name('expid_d1_m2_4_s2')
        d1_m2_5_s2 = self.job_list.get_job_by_name('expid_d1_m2_5_s2')
        d1_m2_6_s2 = self.job_list.get_job_by_name('expid_d1_m2_6_s2')
        d1_m2_7_s2 = self.job_list.get_job_by_name('expid_d1_m2_7_s2')
        d1_m2_8_s2 = self.job_list.get_job_by_name('expid_d1_m2_8_s2')
        d1_m2_9_s2 = self.job_list.get_job_by_name('expid_d1_m2_9_s2')
        d1_m2_10_s2 = self.job_list.get_job_by_name('expid_d1_m2_10_s2')
        self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"] = dict()
        self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"]["m1"] = [d1_m1_1_s2, d1_m1_2_s2, d1_m1_3_s2,
                                                                              d1_m1_4_s2, d1_m1_5_s2, d1_m1_6_s2,
                                                                              d1_m1_7_s2, d1_m1_8_s2, d1_m1_9_s2,
                                                                              d1_m1_10_s2]

        self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"]["m2"] = [d1_m2_1_s2, d1_m2_2_s2, d1_m2_3_s2,
                                                                              d1_m2_4_s2, d1_m2_5_s2, d1_m2_6_s2,
                                                                              d1_m2_7_s2, d1_m2_8_s2, d1_m2_9_s2,
                                                                              d1_m2_10_s2]
        section_list = [d1_m1_1_s2, d1_m2_1_s2]
        self.job_packager.current_wrapper_section = "WRAPPERS"
        self.job_packager.max_jobs = max_jobs
        self.job_packager.retrials = 0
        self.job_packager._platform.max_wallclock = max_wallclock
        self.job_packager.wrapper_type = 'vertical'

        max_wrapped_job_by_section = {}
        max_wrapped_job_by_section["s1"] = max_wrapped_jobs
        max_wrapped_job_by_section["s2"] = max_wrapped_jobs
        max_wrapped_job_by_section["s3"] = max_wrapped_jobs
        max_wrapped_job_by_section["s4"] = max_wrapped_jobs
        wrapper_limits = dict()
        wrapper_limits["max"] = max_wrapped_jobs
        wrapper_limits["max_v"] = max_wrapped_jobs
        wrapper_limits["max_h"] = max_wrapped_jobs
        wrapper_limits["min"] = 2
        wrapper_limits["min_v"] = 2
        wrapper_limits["min_h"] = 2
        wrapper_limits["max_by_section"] = max_wrapped_job_by_section

        with mock.patch("autosubmit.job.job.Job.update_parameters", return_value={}):
            returned_packages = self.job_packager._build_vertical_packages(
                section_list, wrapper_limits, self.wrapper_info)

            package_m1_s2 = [d1_m1_1_s2, d1_m1_2_s2, d1_m1_3_s2, d1_m1_4_s2, d1_m1_5_s2, d1_m1_6_s2, d1_m1_7_s2,
                             d1_m1_8_s2,
                             d1_m1_9_s2, d1_m1_10_s2]
            package_m2_s2 = [d1_m2_1_s2, d1_m2_2_s2, d1_m2_3_s2, d1_m2_4_s2, d1_m2_5_s2, d1_m2_6_s2, d1_m2_7_s2,
                             d1_m2_8_s2,
                             d1_m2_9_s2, d1_m2_10_s2]

            packages = [JobPackageVertical(package_m1_s2, configuration=self.as_conf),
                        JobPackageVertical(package_m2_s2, configuration=self.as_conf)]

            # returned_packages = returned_packages[]
            for i in range(0, len(returned_packages)):
                self.assertListEqual(returned_packages[i]._jobs, packages[i]._jobs)

    def test_returned_packages_max_jobs(self):
        with mock.patch("autosubmit.job.job.Job.update_parameters", return_value={}):

            date_list = ["d1", "d2"]
            member_list = ["m1", "m2"]
            chunk_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

            for section, s_value in self.workflows['basic']['sections'].items():
                self.as_conf.jobs_data[section] = s_value
            self._createDummyJobs(
                self.workflows['basic'], date_list, member_list, chunk_list)

            self.job_list.get_job_by_name(
                'expid_d1_m1_s1').status = Status.COMPLETED
            self.job_list.get_job_by_name(
                'expid_d1_m2_s1').status = Status.COMPLETED

            self.job_list.get_job_by_name('expid_d1_m1_1_s2').status = Status.READY
            self.job_list.get_job_by_name('expid_d1_m2_1_s2').status = Status.READY

            max_jobs = 12
            max_wrapped_jobs = 10
            max_wallclock = '10:00'

            d1_m1_1_s2 = self.job_list.get_job_by_name('expid_d1_m1_1_s2')
            d1_m1_2_s2 = self.job_list.get_job_by_name('expid_d1_m1_2_s2')
            d1_m1_3_s2 = self.job_list.get_job_by_name('expid_d1_m1_3_s2')
            d1_m1_4_s2 = self.job_list.get_job_by_name('expid_d1_m1_4_s2')
            d1_m1_5_s2 = self.job_list.get_job_by_name('expid_d1_m1_5_s2')
            d1_m1_6_s2 = self.job_list.get_job_by_name('expid_d1_m1_6_s2')
            d1_m1_7_s2 = self.job_list.get_job_by_name('expid_d1_m1_7_s2')
            d1_m1_8_s2 = self.job_list.get_job_by_name('expid_d1_m1_8_s2')
            d1_m1_9_s2 = self.job_list.get_job_by_name('expid_d1_m1_9_s2')
            d1_m1_10_s2 = self.job_list.get_job_by_name('expid_d1_m1_10_s2')

            d1_m2_1_s2 = self.job_list.get_job_by_name('expid_d1_m2_1_s2')
            d1_m2_2_s2 = self.job_list.get_job_by_name('expid_d1_m2_2_s2')
            d1_m2_3_s2 = self.job_list.get_job_by_name('expid_d1_m2_3_s2')
            d1_m2_4_s2 = self.job_list.get_job_by_name('expid_d1_m2_4_s2')
            d1_m2_5_s2 = self.job_list.get_job_by_name('expid_d1_m2_5_s2')
            d1_m2_6_s2 = self.job_list.get_job_by_name('expid_d1_m2_6_s2')
            d1_m2_7_s2 = self.job_list.get_job_by_name('expid_d1_m2_7_s2')
            d1_m2_8_s2 = self.job_list.get_job_by_name('expid_d1_m2_8_s2')
            d1_m2_9_s2 = self.job_list.get_job_by_name('expid_d1_m2_9_s2')
            d1_m2_10_s2 = self.job_list.get_job_by_name('expid_d1_m2_10_s2')
            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"] = dict()
            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"]["m1"] = [d1_m1_1_s2, d1_m1_2_s2, d1_m1_3_s2,
                                                                                  d1_m1_4_s2, d1_m1_5_s2, d1_m1_6_s2,
                                                                                  d1_m1_7_s2, d1_m1_8_s2, d1_m1_9_s2,
                                                                                  d1_m1_10_s2]

            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"]["m2"] = [d1_m2_1_s2, d1_m2_2_s2, d1_m2_3_s2,
                                                                                  d1_m2_4_s2, d1_m2_5_s2, d1_m2_6_s2,
                                                                                  d1_m2_7_s2, d1_m2_8_s2, d1_m2_9_s2,
                                                                                  d1_m2_10_s2]

            section_list = [d1_m1_1_s2, d1_m2_1_s2]

            self.job_packager.max_jobs = max_jobs
            self.job_packager._platform.max_wallclock = max_wallclock
            self.job_packager.wrapper_type = 'vertical'
            self.job_packager.retrials = 0
            max_wrapped_job_by_section = {}
            max_wrapped_job_by_section["s1"] = max_wrapped_jobs
            max_wrapped_job_by_section["s2"] = max_wrapped_jobs
            max_wrapped_job_by_section["s3"] = max_wrapped_jobs
            max_wrapped_job_by_section["s4"] = max_wrapped_jobs
            wrapper_limits = dict()
            wrapper_limits["max"] = max_wrapped_jobs
            wrapper_limits["max_v"] = max_wrapped_jobs
            wrapper_limits["max_h"] = max_wrapped_jobs
            wrapper_limits["min"] = 2
            wrapper_limits["min_v"] = 2
            wrapper_limits["min_h"] = 2
            wrapper_limits["max_by_section"] = max_wrapped_job_by_section
            returned_packages = self.job_packager._build_vertical_packages(
                section_list, wrapper_limits, wrapper_info=self.wrapper_info)

            package_m1_s2 = [d1_m1_1_s2, d1_m1_2_s2, d1_m1_3_s2, d1_m1_4_s2, d1_m1_5_s2, d1_m1_6_s2, d1_m1_7_s2,
                             d1_m1_8_s2,
                             d1_m1_9_s2, d1_m1_10_s2]
            package_m2_s2 = [d1_m2_1_s2, d1_m2_2_s2, d1_m2_3_s2, d1_m2_4_s2, d1_m2_5_s2, d1_m2_6_s2, d1_m2_7_s2,
                             d1_m2_8_s2,
                             d1_m2_9_s2, d1_m2_10_s2]

            packages = [JobPackageVertical(
                package_m1_s2, configuration=self.as_conf, wrapper_info=self.wrapper_info),
                JobPackageVertical(package_m2_s2, configuration=self.as_conf, wrapper_info=self.wrapper_info)]

            for i in range(0, len(returned_packages)):
                self.assertListEqual(returned_packages[i]._jobs, packages[i]._jobs)

    def test_returned_packages_max_wrapped_jobs(self):
        with mock.patch("autosubmit.job.job.Job.update_parameters", return_value={}):

            date_list = ["d1", "d2"]
            member_list = ["m1", "m2"]
            chunk_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
            for section, s_value in self.workflows['basic']['sections'].items():
                self.as_conf.jobs_data[section] = s_value
            self._createDummyJobs(
                self.workflows['basic'], date_list, member_list, chunk_list)

            self.job_list.get_job_by_name(
                'expid_d1_m1_s1').status = Status.COMPLETED
            self.job_list.get_job_by_name(
                'expid_d1_m2_s1').status = Status.COMPLETED

            self.job_list.get_job_by_name('expid_d1_m1_1_s2').status = Status.READY
            self.job_list.get_job_by_name('expid_d1_m2_1_s2').status = Status.READY

            max_jobs = 20
            max_wrapped_jobs = 5
            max_wallclock = '10:00'

            d1_m1_1_s2 = self.job_list.get_job_by_name('expid_d1_m1_1_s2')
            d1_m1_2_s2 = self.job_list.get_job_by_name('expid_d1_m1_2_s2')
            d1_m1_3_s2 = self.job_list.get_job_by_name('expid_d1_m1_3_s2')
            d1_m1_4_s2 = self.job_list.get_job_by_name('expid_d1_m1_4_s2')
            d1_m1_5_s2 = self.job_list.get_job_by_name('expid_d1_m1_5_s2')

            d1_m2_1_s2 = self.job_list.get_job_by_name('expid_d1_m2_1_s2')
            d1_m2_2_s2 = self.job_list.get_job_by_name('expid_d1_m2_2_s2')
            d1_m2_3_s2 = self.job_list.get_job_by_name('expid_d1_m2_3_s2')
            d1_m2_4_s2 = self.job_list.get_job_by_name('expid_d1_m2_4_s2')
            d1_m2_5_s2 = self.job_list.get_job_by_name('expid_d1_m2_5_s2')
            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"] = dict()
            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"]["m1"] = [d1_m1_1_s2, d1_m1_2_s2, d1_m1_3_s2,
                                                                                  d1_m1_4_s2, d1_m1_5_s2]

            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"]["m2"] = [d1_m2_1_s2, d1_m2_2_s2, d1_m2_3_s2,
                                                                                  d1_m2_4_s2, d1_m2_5_s2]

            section_list = [d1_m1_1_s2, d1_m2_1_s2]

            self.job_packager.max_jobs = max_jobs
            self.job_packager._platform.max_wallclock = max_wallclock
            self.job_packager.wrapper_type = 'vertical'
            self.job_packager.retrials = 0
            max_wrapped_job_by_section = {}
            max_wrapped_job_by_section["s1"] = max_wrapped_jobs
            max_wrapped_job_by_section["s2"] = max_wrapped_jobs
            max_wrapped_job_by_section["s3"] = max_wrapped_jobs
            max_wrapped_job_by_section["s4"] = max_wrapped_jobs
            wrapper_limits = dict()
            wrapper_limits["max"] = max_wrapped_jobs
            wrapper_limits["max_v"] = max_wrapped_jobs
            wrapper_limits["max_h"] = max_wrapped_jobs
            wrapper_limits["min"] = 2
            wrapper_limits["min_v"] = 2
            wrapper_limits["min_h"] = 2
            wrapper_limits["max_by_section"] = max_wrapped_job_by_section
            returned_packages = self.job_packager._build_vertical_packages(
                section_list, wrapper_limits, self.wrapper_info)

            package_m1_s2 = [d1_m1_1_s2, d1_m1_2_s2,
                             d1_m1_3_s2, d1_m1_4_s2, d1_m1_5_s2]
            package_m2_s2 = [d1_m2_1_s2, d1_m2_2_s2,
                             d1_m2_3_s2, d1_m2_4_s2, d1_m2_5_s2]

            packages = [JobPackageVertical(
                package_m1_s2, configuration=self.as_conf, wrapper_info=self.wrapper_info),
                JobPackageVertical(package_m2_s2, configuration=self.as_conf, wrapper_info=self.wrapper_info)]

            # returned_packages = returned_packages[0]
            for i in range(0, len(returned_packages)):
                self.assertListEqual(returned_packages[i]._jobs, packages[i]._jobs)

    def test_returned_packages_max_wallclock(self):
        with mock.patch("autosubmit.job.job.Job.update_parameters", return_value={}):

            date_list = ["d1", "d2"]
            member_list = ["m1", "m2"]
            chunk_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
            for section, s_value in self.workflows['basic']['sections'].items():
                self.as_conf.jobs_data[section] = s_value
            self._createDummyJobs(
                self.workflows['basic'], date_list, member_list, chunk_list)

            self.job_list.get_job_by_name(
                'expid_d1_m1_s1').status = Status.COMPLETED
            self.job_list.get_job_by_name(
                'expid_d1_m2_s1').status = Status.COMPLETED

            self.job_list.get_job_by_name('expid_d1_m1_1_s2').status = Status.READY
            self.job_list.get_job_by_name('expid_d1_m2_1_s2').status = Status.READY

            max_jobs = 20
            max_wrapped_jobs = 15
            max_wallclock = '00:50'

            d1_m1_1_s2 = self.job_list.get_job_by_name('expid_d1_m1_1_s2')
            d1_m1_2_s2 = self.job_list.get_job_by_name('expid_d1_m1_2_s2')
            d1_m1_3_s2 = self.job_list.get_job_by_name('expid_d1_m1_3_s2')
            d1_m1_4_s2 = self.job_list.get_job_by_name('expid_d1_m1_4_s2')
            d1_m1_5_s2 = self.job_list.get_job_by_name('expid_d1_m1_5_s2')

            d1_m2_1_s2 = self.job_list.get_job_by_name('expid_d1_m2_1_s2')
            d1_m2_2_s2 = self.job_list.get_job_by_name('expid_d1_m2_2_s2')
            d1_m2_3_s2 = self.job_list.get_job_by_name('expid_d1_m2_3_s2')
            d1_m2_4_s2 = self.job_list.get_job_by_name('expid_d1_m2_4_s2')
            d1_m2_5_s2 = self.job_list.get_job_by_name('expid_d1_m2_5_s2')
            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"] = dict()
            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"]["m1"] = [d1_m1_1_s2, d1_m1_2_s2, d1_m1_3_s2,
                                                                                  d1_m1_4_s2, d1_m1_5_s2]

            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"]["m2"] = [d1_m2_1_s2, d1_m2_2_s2, d1_m2_3_s2,
                                                                                  d1_m2_4_s2, d1_m2_5_s2]

            section_list = [d1_m1_1_s2, d1_m2_1_s2]

            self.job_packager.max_jobs = max_jobs
            self.job_packager._platform.max_wallclock = max_wallclock
            self.job_packager.wrapper_type = 'vertical'
            self.job_packager.retrials = 0
            max_wrapped_job_by_section = {}
            max_wrapped_job_by_section["s1"] = max_wrapped_jobs
            max_wrapped_job_by_section["s2"] = max_wrapped_jobs
            max_wrapped_job_by_section["s3"] = max_wrapped_jobs
            max_wrapped_job_by_section["s4"] = max_wrapped_jobs
            wrapper_limits = dict()
            wrapper_limits["max"] = max_wrapped_jobs
            wrapper_limits["max_v"] = max_wrapped_jobs
            wrapper_limits["max_h"] = max_wrapped_jobs
            wrapper_limits["min"] = 2
            wrapper_limits["min_v"] = 2
            wrapper_limits["min_h"] = 2
            wrapper_limits["max_by_section"] = max_wrapped_job_by_section
            returned_packages = self.job_packager._build_vertical_packages(
                section_list, wrapper_limits, self.wrapper_info)

            package_m1_s2 = [d1_m1_1_s2, d1_m1_2_s2,
                             d1_m1_3_s2, d1_m1_4_s2, d1_m1_5_s2]
            package_m2_s2 = [d1_m2_1_s2, d1_m2_2_s2,
                             d1_m2_3_s2, d1_m2_4_s2, d1_m2_5_s2]

            packages = [JobPackageVertical(
                package_m1_s2, configuration=self.as_conf, wrapper_info=self.wrapper_info),
                JobPackageVertical(package_m2_s2, configuration=self.as_conf, wrapper_info=self.wrapper_info)]

            # returned_packages = returned_packages[0]
            for i in range(0, len(returned_packages)):
                self.assertListEqual(returned_packages[i]._jobs, packages[i]._jobs)

    def test_returned_packages_section_not_self_dependent(self):
        with mock.patch("autosubmit.job.job.Job.update_parameters", return_value={}):

            date_list = ["d1", "d2"]
            member_list = ["m1", "m2"]
            chunk_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
            for section, s_value in self.workflows['basic']['sections'].items():
                self.as_conf.jobs_data[section] = s_value
            self._createDummyJobs(
                self.workflows['basic'], date_list, member_list, chunk_list)

            self.job_list.get_job_by_name(
                'expid_d1_m1_s1').status = Status.COMPLETED
            self.job_list.get_job_by_name(
                'expid_d1_m2_s1').status = Status.COMPLETED
            self.job_list.get_job_by_name(
                'expid_d1_m1_1_s2').status = Status.COMPLETED
            self.job_list.get_job_by_name(
                'expid_d1_m2_1_s2').status = Status.COMPLETED

            self.job_list.get_job_by_name('expid_d1_m1_1_s3').status = Status.READY
            self.job_list.get_job_by_name('expid_d1_m2_1_s3').status = Status.READY

            max_jobs = 20
            max_wrapped_jobs = 20
            max_wallclock = '10:00'

            d1_m1_1_s3 = self.job_list.get_job_by_name('expid_d1_m1_1_s3')
            d1_m2_1_s3 = self.job_list.get_job_by_name('expid_d1_m2_1_s3')
            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"] = dict()
            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"]["m1"] = [d1_m1_1_s3]

            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"]["m2"] = [d1_m2_1_s3]

            section_list = [d1_m1_1_s3, d1_m2_1_s3]

            self.job_packager.max_jobs = max_jobs
            self.job_packager._platform.max_wallclock = max_wallclock
            self.job_packager.wrapper_type = 'vertical'
            self.job_packager.retrials = 0
            max_wrapped_job_by_section = {}
            max_wrapped_job_by_section["s1"] = max_wrapped_jobs
            max_wrapped_job_by_section["s2"] = max_wrapped_jobs
            max_wrapped_job_by_section["s3"] = max_wrapped_jobs
            max_wrapped_job_by_section["s4"] = max_wrapped_jobs
            wrapper_limits = dict()
            wrapper_limits["max"] = max_wrapped_jobs
            wrapper_limits["max_v"] = max_wrapped_jobs
            wrapper_limits["max_h"] = max_wrapped_jobs
            wrapper_limits["min"] = 2
            wrapper_limits["min_v"] = 2
            wrapper_limits["min_h"] = 2
            wrapper_limits["max_by_section"] = max_wrapped_job_by_section
            returned_packages = self.job_packager._build_vertical_packages(
                section_list, wrapper_limits, self.wrapper_info)
            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"] = dict()
            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"]["m1"] = [d1_m1_1_s3]

            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"]["m2"] = [d1_m2_1_s3]

            package_m1_s2 = [d1_m1_1_s3]
            package_m2_s2 = [d1_m2_1_s3]

            packages = [JobPackageVertical(
                package_m1_s2, configuration=self.as_conf),
                JobPackageVertical(package_m2_s2, configuration=self.as_conf)]

            # returned_packages = returned_packages[0]
            for i in range(0, len(returned_packages)):
                self.assertListEqual(returned_packages[i]._jobs, packages[i]._jobs)

    ### MIXED WRAPPER ###
    def test_returned_packages_mixed_wrapper(self):
        with mock.patch("autosubmit.job.job.Job.update_parameters", return_value={}):

            date_list = ["d1"]
            member_list = ["m1", "m2"]
            chunk_list = [1, 2, 3, 4]
            for section, s_value in self.workflows['basic']['sections'].items():
                self.as_conf.jobs_data[section] = s_value
            self._createDummyJobs(
                self.workflows['basic'], date_list, member_list, chunk_list)

            self.job_list.get_job_by_name(
                'expid_d1_m1_s1').status = Status.COMPLETED
            self.job_list.get_job_by_name(
                'expid_d1_m2_s1').status = Status.COMPLETED

            self.job_list.get_job_by_name('expid_d1_m1_1_s2').status = Status.READY
            self.job_list.get_job_by_name('expid_d1_m2_1_s2').status = Status.READY

            wrapper_expression = "s2 s3"
            max_jobs = 18
            max_wrapped_jobs = 18
            max_wallclock = '10:00'

            d1_m1_1_s2 = self.job_list.get_job_by_name('expid_d1_m1_1_s2')
            d1_m1_2_s2 = self.job_list.get_job_by_name('expid_d1_m1_2_s2')
            d1_m1_3_s2 = self.job_list.get_job_by_name('expid_d1_m1_3_s2')
            d1_m1_4_s2 = self.job_list.get_job_by_name('expid_d1_m1_4_s2')
            d1_m2_1_s2 = self.job_list.get_job_by_name('expid_d1_m2_1_s2')
            d1_m2_2_s2 = self.job_list.get_job_by_name('expid_d1_m2_2_s2')
            d1_m2_3_s2 = self.job_list.get_job_by_name('expid_d1_m2_3_s2')
            d1_m2_4_s2 = self.job_list.get_job_by_name('expid_d1_m2_4_s2')

            d1_m1_1_s3 = self.job_list.get_job_by_name('expid_d1_m1_1_s3')
            d1_m1_2_s3 = self.job_list.get_job_by_name('expid_d1_m1_2_s3')
            d1_m1_3_s3 = self.job_list.get_job_by_name('expid_d1_m1_3_s3')
            d1_m1_4_s3 = self.job_list.get_job_by_name('expid_d1_m1_4_s3')
            d1_m2_1_s3 = self.job_list.get_job_by_name('expid_d1_m2_1_s3')
            d1_m2_2_s3 = self.job_list.get_job_by_name('expid_d1_m2_2_s3')
            d1_m2_3_s3 = self.job_list.get_job_by_name('expid_d1_m2_3_s3')
            d1_m2_4_s3 = self.job_list.get_job_by_name('expid_d1_m2_4_s3')

            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"] = dict()
            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"]["m1"] = [d1_m1_1_s2, d1_m1_1_s3, d1_m1_2_s2,
                                                                                  d1_m1_2_s3,
                                                                                  d1_m1_3_s2, d1_m1_3_s3, d1_m1_4_s2,
                                                                                  d1_m1_4_s3]

            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"]["m2"] = [d1_m2_1_s2, d1_m2_1_s3, d1_m2_2_s2,
                                                                                  d1_m2_2_s3,
                                                                                  d1_m2_3_s2, d1_m2_3_s3, d1_m2_4_s2,
                                                                                  d1_m2_4_s3]

            section_list = [d1_m1_1_s2, d1_m2_1_s2]

            self.job_packager.max_jobs = max_jobs
            self.job_packager._platform.max_wallclock = max_wallclock
            self.job_packager.wrapper_type = 'vertical'
            self.job_packager.retrials = 0
            self.job_packager.jobs_in_wrapper = wrapper_expression
            max_wrapped_job_by_section = {}
            max_wrapped_job_by_section["s1"] = max_wrapped_jobs
            max_wrapped_job_by_section["s2"] = max_wrapped_jobs
            max_wrapped_job_by_section["s3"] = max_wrapped_jobs
            max_wrapped_job_by_section["s4"] = max_wrapped_jobs
            wrapper_limits = dict()
            wrapper_limits["max"] = max_wrapped_jobs
            wrapper_limits["max_v"] = max_wrapped_jobs
            wrapper_limits["max_h"] = max_wrapped_jobs
            wrapper_limits["min"] = 2
            wrapper_limits["min_v"] = 2
            wrapper_limits["min_h"] = 2
            wrapper_limits["max_by_section"] = max_wrapped_job_by_section
            returned_packages = self.job_packager._build_vertical_packages(
                section_list, wrapper_limits, wrapper_info=self.wrapper_info)

            package_m1_s2_s3 = [d1_m1_1_s2, d1_m1_1_s3, d1_m1_2_s2, d1_m1_2_s3, d1_m1_3_s2, d1_m1_3_s3, d1_m1_4_s2,
                                d1_m1_4_s3]
            package_m2_s2_s3 = [d1_m2_1_s2, d1_m2_1_s3, d1_m2_2_s2, d1_m2_2_s3, d1_m2_3_s2, d1_m2_3_s3, d1_m2_4_s2,
                                d1_m2_4_s3]

            packages = [JobPackageVertical(
                package_m1_s2_s3, configuration=self.as_conf, wrapper_info=self.wrapper_info),
                JobPackageVertical(package_m2_s2_s3, configuration=self.as_conf, wrapper_info=self.wrapper_info)]

            # returned_packages = returned_packages[0]
            for i in range(0, len(returned_packages)):
                self.assertListEqual(returned_packages[i]._jobs, packages[i]._jobs)

    def test_returned_packages_parent_failed_mixed_wrapper(self):
        with mock.patch("autosubmit.job.job.Job.update_parameters", return_value={}):

            date_list = ["d1"]
            member_list = ["m1", "m2"]
            chunk_list = [1, 2, 3, 4]
            for section, s_value in self.workflows['basic']['sections'].items():
                self.as_conf.jobs_data[section] = s_value
            self._createDummyJobs(
                self.workflows['basic'], date_list, member_list, chunk_list)

            self.job_list.get_job_by_name(
                'expid_d1_m1_s1').status = Status.COMPLETED
            self.job_list.get_job_by_name('expid_d1_m2_s1').status = Status.FAILED

            self.job_list.get_job_by_name('expid_d1_m1_1_s2').status = Status.READY

            wrapper_expression = "s2 s3"
            max_jobs = 18
            max_wrapped_jobs = 18
            max_wallclock = '10:00'

            d1_m1_1_s2 = self.job_list.get_job_by_name('expid_d1_m1_1_s2')
            d1_m1_2_s2 = self.job_list.get_job_by_name('expid_d1_m1_2_s2')
            d1_m1_3_s2 = self.job_list.get_job_by_name('expid_d1_m1_3_s2')
            d1_m1_4_s2 = self.job_list.get_job_by_name('expid_d1_m1_4_s2')
            d1_m2_1_s2 = self.job_list.get_job_by_name('expid_d1_m2_1_s2')
            d1_m2_2_s2 = self.job_list.get_job_by_name('expid_d1_m2_2_s2')
            d1_m2_3_s2 = self.job_list.get_job_by_name('expid_d1_m2_3_s2')
            d1_m2_4_s2 = self.job_list.get_job_by_name('expid_d1_m2_4_s2')

            d1_m1_1_s3 = self.job_list.get_job_by_name('expid_d1_m1_1_s3')
            d1_m1_2_s3 = self.job_list.get_job_by_name('expid_d1_m1_2_s3')
            d1_m1_3_s3 = self.job_list.get_job_by_name('expid_d1_m1_3_s3')
            d1_m1_4_s3 = self.job_list.get_job_by_name('expid_d1_m1_4_s3')
            d1_m2_1_s3 = self.job_list.get_job_by_name('expid_d1_m2_1_s3')
            d1_m2_2_s3 = self.job_list.get_job_by_name('expid_d1_m2_2_s3')
            d1_m2_3_s3 = self.job_list.get_job_by_name('expid_d1_m2_3_s3')
            d1_m2_4_s3 = self.job_list.get_job_by_name('expid_d1_m2_4_s3')
            self.job_list._ordered_jobs_by_date_member["WRAPPERS"] = dict()
            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"] = dict()
            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"]["m1"] = [d1_m1_1_s2, d1_m1_1_s3, d1_m1_2_s2,
                                                                                  d1_m1_2_s3,
                                                                                  d1_m1_3_s2, d1_m1_3_s3, d1_m1_4_s2,
                                                                                  d1_m1_4_s3]

            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"]["m2"] = [d1_m2_1_s2, d1_m2_1_s3, d1_m2_2_s2,
                                                                                  d1_m2_2_s3,
                                                                                  d1_m2_3_s2, d1_m2_3_s3, d1_m2_4_s2,
                                                                                  d1_m2_4_s3]

            section_list = [d1_m1_1_s2]

            self.job_packager.max_jobs = max_jobs
            self.job_packager._platform.max_wallclock = max_wallclock
            self.job_packager.wrapper_type = 'vertical'
            self.job_packager.jobs_in_wrapper = wrapper_expression
            self.job_packager.retrials = 0
            max_wrapper_job_by_section = {}
            max_wrapper_job_by_section["s1"] = max_wrapped_jobs
            max_wrapper_job_by_section["s2"] = max_wrapped_jobs
            max_wrapper_job_by_section["s3"] = max_wrapped_jobs
            max_wrapper_job_by_section["s4"] = max_wrapped_jobs
            wrapper_limits = dict()
            wrapper_limits["max"] = max_wrapped_jobs
            wrapper_limits["max_v"] = max_wrapped_jobs
            wrapper_limits["max_h"] = max_wrapped_jobs
            wrapper_limits["min"] = 2
            wrapper_limits["min_v"] = 2
            wrapper_limits["min_h"] = 2
            wrapper_limits["max_by_section"] = max_wrapper_job_by_section
            returned_packages = self.job_packager._build_vertical_packages(
                section_list, wrapper_limits, wrapper_info=self.wrapper_info)

            package_m1_s2_s3 = [d1_m1_1_s2, d1_m1_1_s3, d1_m1_2_s2, d1_m1_2_s3, d1_m1_3_s2, d1_m1_3_s3, d1_m1_4_s2,
                                d1_m1_4_s3]

            packages = [
                JobPackageVertical(package_m1_s2_s3, configuration=self.as_conf, wrapper_info=self.wrapper_info)]

            # returned_packages = returned_packages[0]
            for i in range(0, len(returned_packages)):
                self.assertListEqual(returned_packages[i]._jobs, packages[i]._jobs)

    def test_returned_packages_max_jobs_mixed_wrapper(self):
        with mock.patch("autosubmit.job.job.Job.update_parameters", return_value={}):

            wrapper_expression = "s2 s3"
            max_jobs = 10
            max_wrapped_jobs = 10
            max_wallclock = '10:00'

            date_list = ["d1"]
            member_list = ["m1", "m2"]
            chunk_list = [1, 2, 3, 4]
            for section, s_value in self.workflows['basic']['sections'].items():
                self.as_conf.jobs_data[section] = s_value
            self._createDummyJobs(
                self.workflows['basic'], date_list, member_list, chunk_list)

            self.job_list.get_job_by_name(
                'expid_d1_m1_s1').status = Status.COMPLETED
            self.job_list.get_job_by_name(
                'expid_d1_m2_s1').status = Status.COMPLETED

            self.job_list.get_job_by_name('expid_d1_m1_1_s2').status = Status.READY
            self.job_list.get_job_by_name('expid_d1_m2_1_s2').status = Status.READY

            d1_m1_1_s2 = self.job_list.get_job_by_name('expid_d1_m1_1_s2')
            d1_m1_2_s2 = self.job_list.get_job_by_name('expid_d1_m1_2_s2')
            d1_m1_3_s2 = self.job_list.get_job_by_name('expid_d1_m1_3_s2')
            d1_m1_4_s2 = self.job_list.get_job_by_name('expid_d1_m1_4_s2')
            d1_m2_1_s2 = self.job_list.get_job_by_name('expid_d1_m2_1_s2')
            d1_m2_2_s2 = self.job_list.get_job_by_name('expid_d1_m2_2_s2')
            d1_m2_3_s2 = self.job_list.get_job_by_name('expid_d1_m2_3_s2')
            d1_m2_4_s2 = self.job_list.get_job_by_name('expid_d1_m2_4_s2')

            d1_m1_1_s3 = self.job_list.get_job_by_name('expid_d1_m1_1_s3')
            d1_m1_2_s3 = self.job_list.get_job_by_name('expid_d1_m1_2_s3')
            d1_m1_3_s3 = self.job_list.get_job_by_name('expid_d1_m1_3_s3')
            d1_m1_4_s3 = self.job_list.get_job_by_name('expid_d1_m1_4_s3')
            d1_m2_1_s3 = self.job_list.get_job_by_name('expid_d1_m2_1_s3')
            d1_m2_2_s3 = self.job_list.get_job_by_name('expid_d1_m2_2_s3')
            d1_m2_3_s3 = self.job_list.get_job_by_name('expid_d1_m2_3_s3')
            d1_m2_4_s3 = self.job_list.get_job_by_name('expid_d1_m2_4_s3')

            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"] = dict()
            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"]["m1"] = [d1_m1_1_s2, d1_m1_1_s3, d1_m1_2_s2,
                                                                                  d1_m1_2_s3,
                                                                                  d1_m1_3_s2, d1_m1_3_s3, d1_m1_4_s2,
                                                                                  d1_m1_4_s3]

            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"]["m2"] = [d1_m2_1_s2, d1_m2_1_s3, d1_m2_2_s2,
                                                                                  d1_m2_2_s3,
                                                                                  d1_m2_3_s2, d1_m2_3_s3, d1_m2_4_s2,
                                                                                  d1_m2_4_s3]

            section_list = [d1_m1_1_s2, d1_m2_1_s2]

            self.job_packager.max_jobs = max_jobs
            self.job_packager.retrials = 0
            self.job_packager._platform.max_wallclock = max_wallclock
            self.job_packager.wrapper_type = 'vertical'
            self.job_packager.jobs_in_wrapper = wrapper_expression
            max_wrapped_job_by_section = {}
            max_wrapped_job_by_section["s1"] = max_wrapped_jobs
            max_wrapped_job_by_section["s2"] = max_wrapped_jobs
            max_wrapped_job_by_section["s3"] = max_wrapped_jobs
            max_wrapped_job_by_section["s4"] = max_wrapped_jobs
            wrapper_limits = dict()
            wrapper_limits["max"] = max_wrapped_jobs
            wrapper_limits["max_v"] = max_wrapped_jobs
            wrapper_limits["max_h"] = max_wrapped_jobs
            wrapper_limits["min"] = 2
            wrapper_limits["min_v"] = 2
            wrapper_limits["min_h"] = 2
            wrapper_limits["max_by_section"] = max_wrapped_job_by_section
            returned_packages = self.job_packager._build_vertical_packages(
                section_list, wrapper_limits, wrapper_info=self.wrapper_info)

            package_m1_s2_s3 = [d1_m1_1_s2, d1_m1_1_s3, d1_m1_2_s2, d1_m1_2_s3, d1_m1_3_s2, d1_m1_3_s3, d1_m1_4_s2,
                                d1_m1_4_s3]
            package_m2_s2_s3 = [d1_m2_1_s2, d1_m2_1_s3, d1_m2_2_s2, d1_m2_2_s3, d1_m2_3_s2, d1_m2_3_s3, d1_m2_4_s2,
                                d1_m2_4_s3]

            packages = [JobPackageVertical(
                package_m1_s2_s3, configuration=self.as_conf, wrapper_info=self.wrapper_info),
                JobPackageVertical(package_m2_s2_s3, configuration=self.as_conf, wrapper_info=self.wrapper_info)]

            # returned_packages = returned_packages[0]
            # print("test_returned_packages_max_jobs_mixed_wrapper")
            for i in range(0, len(returned_packages)):
                # print("Element " + str(i))
                # print("Returned from packager")
                # for job in returned_packages[i]._jobs:
                #     print(job.name)
                # print("Build for test")
                # for _job in packages[i]._jobs:
                #     print(_job.name)
                self.assertListEqual(returned_packages[i]._jobs, packages[i]._jobs)

    def test_returned_packages_max_wrapped_jobs_mixed_wrapper(self):
        with mock.patch("autosubmit.job.job.Job.update_parameters", return_value={}):

            wrapper_expression = "s2 s3"
            max_jobs = 15
            max_wrapped_jobs = 5
            max_wallclock = '10:00'

            date_list = ["d1"]
            member_list = ["m1", "m2"]
            chunk_list = [1, 2, 3, 4]
            for section, s_value in self.workflows['basic']['sections'].items():
                self.as_conf.jobs_data[section] = s_value
            self._createDummyJobs(
                self.workflows['basic'], date_list, member_list, chunk_list)

            self.job_list.get_job_by_name(
                'expid_d1_m1_s1').status = Status.COMPLETED
            self.job_list.get_job_by_name(
                'expid_d1_m2_s1').status = Status.COMPLETED

            self.job_list.get_job_by_name('expid_d1_m1_1_s2').status = Status.READY
            self.job_list.get_job_by_name('expid_d1_m2_1_s2').status = Status.READY

            d1_m1_1_s2 = self.job_list.get_job_by_name('expid_d1_m1_1_s2')
            d1_m1_2_s2 = self.job_list.get_job_by_name('expid_d1_m1_2_s2')
            d1_m1_3_s2 = self.job_list.get_job_by_name('expid_d1_m1_3_s2')
            d1_m1_4_s2 = self.job_list.get_job_by_name('expid_d1_m1_4_s2')
            d1_m2_1_s2 = self.job_list.get_job_by_name('expid_d1_m2_1_s2')
            d1_m2_2_s2 = self.job_list.get_job_by_name('expid_d1_m2_2_s2')
            d1_m2_3_s2 = self.job_list.get_job_by_name('expid_d1_m2_3_s2')
            d1_m2_4_s2 = self.job_list.get_job_by_name('expid_d1_m2_4_s2')

            d1_m1_1_s3 = self.job_list.get_job_by_name('expid_d1_m1_1_s3')
            d1_m1_2_s3 = self.job_list.get_job_by_name('expid_d1_m1_2_s3')
            d1_m1_3_s3 = self.job_list.get_job_by_name('expid_d1_m1_3_s3')
            d1_m1_4_s3 = self.job_list.get_job_by_name('expid_d1_m1_4_s3')
            d1_m2_1_s3 = self.job_list.get_job_by_name('expid_d1_m2_1_s3')
            d1_m2_2_s3 = self.job_list.get_job_by_name('expid_d1_m2_2_s3')
            d1_m2_3_s3 = self.job_list.get_job_by_name('expid_d1_m2_3_s3')
            d1_m2_4_s3 = self.job_list.get_job_by_name('expid_d1_m2_4_s3')

            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"] = dict()
            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"]["m1"] = [d1_m1_1_s2, d1_m1_1_s3, d1_m1_2_s2,
                                                                                  d1_m1_2_s3,
                                                                                  d1_m1_3_s2, d1_m1_3_s3, d1_m1_4_s2,
                                                                                  d1_m1_4_s3]

            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"]["m2"] = [d1_m2_1_s2, d1_m2_1_s3, d1_m2_2_s2,
                                                                                  d1_m2_2_s3,
                                                                                  d1_m2_3_s2, d1_m2_3_s3, d1_m2_4_s2,
                                                                                  d1_m2_4_s3]

            section_list = [d1_m1_1_s2, d1_m2_1_s2]

            self.job_packager.max_jobs = max_jobs
            self.job_packager.retrials = 0
            self.job_packager._platform.max_wallclock = max_wallclock
            self.job_packager.wrapper_type = 'vertical'
            self.job_packager.jobs_in_wrapper = wrapper_expression
            max_wrapped_job_by_section = {}
            max_wrapped_job_by_section["s1"] = max_wrapped_jobs
            max_wrapped_job_by_section["s2"] = max_wrapped_jobs
            max_wrapped_job_by_section["s3"] = max_wrapped_jobs
            max_wrapped_job_by_section["s4"] = max_wrapped_jobs
            wrapper_limits = dict()
            wrapper_limits["max"] = max_wrapped_jobs
            wrapper_limits["max_v"] = max_wrapped_jobs
            wrapper_limits["max_h"] = max_wrapped_jobs
            wrapper_limits["min"] = 2
            wrapper_limits["min_v"] = 2
            wrapper_limits["min_h"] = 2
            wrapper_limits["max_by_section"] = max_wrapped_job_by_section
            returned_packages = self.job_packager._build_vertical_packages(
                section_list, wrapper_limits, wrapper_info=self.wrapper_info)

            package_m1_s2_s3 = [d1_m1_1_s2, d1_m1_1_s3,
                                d1_m1_2_s2, d1_m1_2_s3, d1_m1_3_s2]
            package_m2_s2_s3 = [d1_m2_1_s2, d1_m2_1_s3,
                                d1_m2_2_s2, d1_m2_2_s3, d1_m2_3_s2]

            packages = [JobPackageVertical(
                package_m1_s2_s3, configuration=self.as_conf, wrapper_info=self.wrapper_info),
                JobPackageVertical(package_m2_s2_s3, configuration=self.as_conf, wrapper_info=self.wrapper_info)]

            # returned_packages = returned_packages[0]
            for i in range(0, len(returned_packages)):
                self.assertListEqual(returned_packages[i]._jobs, packages[i]._jobs)

    def test_returned_packages_max_wallclock_mixed_wrapper(self):
        with mock.patch("autosubmit.job.job.Job.update_parameters", return_value={}):

            date_list = ["d1"]
            member_list = ["m1", "m2"]
            chunk_list = [1, 2, 3, 4]
            for section, s_value in self.workflows['basic']['sections'].items():
                self.as_conf.jobs_data[section] = s_value
            self._createDummyJobs(
                self.workflows['basic'], date_list, member_list, chunk_list)

            self.job_list.get_job_by_name(
                'expid_d1_m1_s1').status = Status.COMPLETED
            self.job_list.get_job_by_name(
                'expid_d1_m2_s1').status = Status.COMPLETED

            self.job_list.get_job_by_name('expid_d1_m1_1_s2').status = Status.READY
            self.job_list.get_job_by_name('expid_d1_m2_1_s2').status = Status.READY

            wrapper_expression = "s2 s3"
            max_jobs = 18
            max_wrapped_jobs = 18
            max_wallclock = '01:00'

            d1_m1_1_s2 = self.job_list.get_job_by_name('expid_d1_m1_1_s2')
            d1_m1_2_s2 = self.job_list.get_job_by_name('expid_d1_m1_2_s2')
            d1_m1_3_s2 = self.job_list.get_job_by_name('expid_d1_m1_3_s2')
            d1_m1_4_s2 = self.job_list.get_job_by_name('expid_d1_m1_4_s2')
            d1_m2_1_s2 = self.job_list.get_job_by_name('expid_d1_m2_1_s2')
            d1_m2_2_s2 = self.job_list.get_job_by_name('expid_d1_m2_2_s2')
            d1_m2_3_s2 = self.job_list.get_job_by_name('expid_d1_m2_3_s2')
            d1_m2_4_s2 = self.job_list.get_job_by_name('expid_d1_m2_4_s2')

            d1_m1_1_s3 = self.job_list.get_job_by_name('expid_d1_m1_1_s3')
            d1_m1_2_s3 = self.job_list.get_job_by_name('expid_d1_m1_2_s3')
            d1_m1_3_s3 = self.job_list.get_job_by_name('expid_d1_m1_3_s3')
            d1_m1_4_s3 = self.job_list.get_job_by_name('expid_d1_m1_4_s3')
            d1_m2_1_s3 = self.job_list.get_job_by_name('expid_d1_m2_1_s3')
            d1_m2_2_s3 = self.job_list.get_job_by_name('expid_d1_m2_2_s3')
            d1_m2_3_s3 = self.job_list.get_job_by_name('expid_d1_m2_3_s3')
            d1_m2_4_s3 = self.job_list.get_job_by_name('expid_d1_m2_4_s3')

            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"] = dict()
            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"]["m1"] = [d1_m1_1_s2, d1_m1_1_s3, d1_m1_2_s2,
                                                                                  d1_m1_2_s3,
                                                                                  d1_m1_3_s2, d1_m1_3_s3, d1_m1_4_s2,
                                                                                  d1_m1_4_s3]

            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"]["m2"] = [d1_m2_1_s2, d1_m2_1_s3, d1_m2_2_s2,
                                                                                  d1_m2_2_s3,
                                                                                  d1_m2_3_s2, d1_m2_3_s3, d1_m2_4_s2,
                                                                                  d1_m2_4_s3]

            section_list = [d1_m1_1_s2, d1_m2_1_s2]

            self.job_packager.max_jobs = max_jobs
            self.job_packager._platform.max_wallclock = max_wallclock
            self.job_packager.wrapper_type = 'vertical'
            self.job_packager.retrials = 0
            self.job_packager.jobs_in_wrapper = wrapper_expression
            max_wrapped_job_by_section = {}
            max_wrapped_job_by_section["s1"] = max_wrapped_jobs
            max_wrapped_job_by_section["s2"] = max_wrapped_jobs
            max_wrapped_job_by_section["s3"] = max_wrapped_jobs
            max_wrapped_job_by_section["s4"] = max_wrapped_jobs
            wrapper_limits = dict()
            wrapper_limits["max"] = max_wrapped_jobs
            wrapper_limits["max_v"] = max_wrapped_jobs
            wrapper_limits["max_h"] = max_wrapped_jobs
            wrapper_limits["min"] = 2
            wrapper_limits["min_v"] = 2
            wrapper_limits["min_h"] = 2
            wrapper_limits["max_by_section"] = max_wrapped_job_by_section
            returned_packages = self.job_packager._build_vertical_packages(
                section_list, wrapper_limits, wrapper_info=self.wrapper_info)

            package_m1_s2_s3 = [d1_m1_1_s2, d1_m1_1_s3, d1_m1_2_s2, d1_m1_2_s3]
            package_m2_s2_s3 = [d1_m2_1_s2, d1_m2_1_s3, d1_m2_2_s2, d1_m2_2_s3]

            packages = [JobPackageVertical(
                package_m1_s2_s3, configuration=self.as_conf, wrapper_info=self.wrapper_info),
                JobPackageVertical(package_m2_s2_s3, configuration=self.as_conf, wrapper_info=self.wrapper_info)]

            # returned_packages = returned_packages[0]
            for i in range(0, len(returned_packages)):
                self.assertListEqual(returned_packages[i]._jobs, packages[i]._jobs)

    def test_returned_packages_first_chunks_completed_mixed_wrapper(self):
        with mock.patch("autosubmit.job.job.Job.update_parameters", return_value={}):

            date_list = ["d1"]
            member_list = ["m1", "m2"]
            chunk_list = [1, 2, 3, 4]
            for section, s_value in self.workflows['basic']['sections'].items():
                self.as_conf.jobs_data[section] = s_value
            self._createDummyJobs(
                self.workflows['basic'], date_list, member_list, chunk_list)

            self.job_list.get_job_by_name(
                'expid_d1_m1_s1').status = Status.COMPLETED
            self.job_list.get_job_by_name(
                'expid_d1_m2_s1').status = Status.COMPLETED

            self.job_list.get_job_by_name(
                'expid_d1_m1_1_s2').status = Status.COMPLETED
            self.job_list.get_job_by_name(
                'expid_d1_m1_2_s2').status = Status.COMPLETED
            self.job_list.get_job_by_name(
                'expid_d1_m1_3_s2').status = Status.COMPLETED
            self.job_list.get_job_by_name(
                'expid_d1_m2_1_s2').status = Status.COMPLETED
            self.job_list.get_job_by_name(
                'expid_d1_m2_2_s2').status = Status.COMPLETED
            self.job_list.get_job_by_name(
                'expid_d1_m1_1_s3').status = Status.COMPLETED
            self.job_list.get_job_by_name(
                'expid_d1_m2_1_s3').status = Status.COMPLETED
            self.job_list.get_job_by_name(
                'expid_d1_m2_2_s3').status = Status.COMPLETED

            self.job_list.get_job_by_name('expid_d1_m1_4_s2').status = Status.READY
            self.job_list.get_job_by_name('expid_d1_m2_3_s2').status = Status.READY
            self.job_list.get_job_by_name('expid_d1_m1_2_s3').status = Status.READY

            d1_m1_1_s2 = self.job_list.get_job_by_name('expid_d1_m1_1_s2')
            d1_m1_2_s2 = self.job_list.get_job_by_name('expid_d1_m1_2_s2')
            d1_m1_3_s2 = self.job_list.get_job_by_name('expid_d1_m1_3_s2')
            d1_m1_4_s2 = self.job_list.get_job_by_name('expid_d1_m1_4_s2')
            d1_m2_1_s2 = self.job_list.get_job_by_name('expid_d1_m2_1_s2')
            d1_m2_2_s2 = self.job_list.get_job_by_name('expid_d1_m2_2_s2')
            d1_m2_3_s2 = self.job_list.get_job_by_name('expid_d1_m2_3_s2')
            d1_m2_4_s2 = self.job_list.get_job_by_name('expid_d1_m2_4_s2')

            d1_m1_1_s3 = self.job_list.get_job_by_name('expid_d1_m1_1_s3')
            d1_m1_2_s3 = self.job_list.get_job_by_name('expid_d1_m1_2_s3')
            d1_m1_3_s3 = self.job_list.get_job_by_name('expid_d1_m1_3_s3')
            d1_m1_4_s3 = self.job_list.get_job_by_name('expid_d1_m1_4_s3')
            d1_m2_1_s3 = self.job_list.get_job_by_name('expid_d1_m2_1_s3')
            d1_m2_2_s3 = self.job_list.get_job_by_name('expid_d1_m2_2_s3')
            d1_m2_3_s3 = self.job_list.get_job_by_name('expid_d1_m2_3_s3')
            d1_m2_4_s3 = self.job_list.get_job_by_name('expid_d1_m2_4_s3')

            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"] = dict()
            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"]["m1"] = [d1_m1_1_s2, d1_m1_1_s3, d1_m1_2_s2,
                                                                                  d1_m1_2_s3, d1_m1_3_s2,
                                                                                  d1_m1_3_s3, d1_m1_4_s2, d1_m1_4_s3]

            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"]["m2"] = [d1_m2_1_s2, d1_m2_1_s3, d1_m2_2_s2,
                                                                                  d1_m2_2_s3, d1_m2_3_s2,
                                                                                  d1_m2_3_s3, d1_m2_4_s2, d1_m2_4_s3]

            wrapper_expression = "s2 s3"
            max_wrapped_jobs = 18
            max_jobs = 18
            max_wallclock = '10:00'

            section_list = [d1_m1_2_s3, d1_m1_4_s2, d1_m2_3_s2]

            self.job_packager.max_jobs = max_jobs
            self.job_packager._platform.max_wallclock = max_wallclock
            self.job_packager.wrapper_type = 'vertical'
            self.job_packager.retrials = 0
            self.job_packager.jobs_in_wrapper = wrapper_expression
            max_wrapped_job_by_section = {}
            max_wrapped_job_by_section["s1"] = max_wrapped_jobs
            max_wrapped_job_by_section["s2"] = max_wrapped_jobs
            max_wrapped_job_by_section["s3"] = max_wrapped_jobs
            max_wrapped_job_by_section["s4"] = max_wrapped_jobs
            wrapper_limits = dict()
            wrapper_limits["max"] = max_wrapped_jobs
            wrapper_limits["max_v"] = max_wrapped_jobs
            wrapper_limits["max_h"] = max_wrapped_jobs
            wrapper_limits["min"] = 2
            wrapper_limits["min_v"] = 2
            wrapper_limits["min_h"] = 2
            wrapper_limits["max_by_section"] = max_wrapped_job_by_section
            returned_packages = self.job_packager._build_vertical_packages(
                section_list, wrapper_limits, wrapper_info=self.wrapper_info)

            package_m1_s2_s3 = [d1_m1_2_s3, d1_m1_3_s3, d1_m1_4_s2, d1_m1_4_s3]
            package_m2_s2_s3 = [d1_m2_3_s2, d1_m2_3_s3, d1_m2_4_s2, d1_m2_4_s3]

            packages = [JobPackageVertical(
                package_m1_s2_s3, configuration=self.as_conf, wrapper_info=self.wrapper_info),
                JobPackageVertical(package_m2_s2_s3, configuration=self.as_conf, wrapper_info=self.wrapper_info)]

            # returned_packages = returned_packages[0]
            for i in range(0, len(returned_packages)):
                self.assertListEqual(returned_packages[i]._jobs, packages[i]._jobs)

    def test_ordered_dict_jobs_simple_workflow_mixed_wrapper(self):
        with mock.patch("autosubmit.job.job.Job.update_parameters", return_value={}):
            date_list = ["d1"]
            member_list = ["m1", "m2"]
            chunk_list = [1, 2, 3, 4]
            for section, s_value in self.workflows['basic']['sections'].items():
                self.as_conf.jobs_data[section] = s_value
            self._createDummyJobs(
                self.workflows['basic'], date_list, member_list, chunk_list)

            self.job_list.get_job_by_name(
                'expid_d1_m1_s1').status = Status.COMPLETED
            self.job_list.get_job_by_name(
                'expid_d1_m2_s1').status = Status.COMPLETED

            self.job_list.get_job_by_name('expid_d1_m1_1_s2').status = Status.READY
            self.job_list.get_job_by_name('expid_d1_m2_1_s2').status = Status.READY

            d1_m1_1_s2 = self.job_list.get_job_by_name('expid_d1_m1_1_s2')
            d1_m1_2_s2 = self.job_list.get_job_by_name('expid_d1_m1_2_s2')
            d1_m1_3_s2 = self.job_list.get_job_by_name('expid_d1_m1_3_s2')
            d1_m1_4_s2 = self.job_list.get_job_by_name('expid_d1_m1_4_s2')
            d1_m2_1_s2 = self.job_list.get_job_by_name('expid_d1_m2_1_s2')
            d1_m2_2_s2 = self.job_list.get_job_by_name('expid_d1_m2_2_s2')
            d1_m2_3_s2 = self.job_list.get_job_by_name('expid_d1_m2_3_s2')
            d1_m2_4_s2 = self.job_list.get_job_by_name('expid_d1_m2_4_s2')

            d1_m1_1_s3 = self.job_list.get_job_by_name('expid_d1_m1_1_s3')
            d1_m1_2_s3 = self.job_list.get_job_by_name('expid_d1_m1_2_s3')
            d1_m1_3_s3 = self.job_list.get_job_by_name('expid_d1_m1_3_s3')
            d1_m1_4_s3 = self.job_list.get_job_by_name('expid_d1_m1_4_s3')
            d1_m2_1_s3 = self.job_list.get_job_by_name('expid_d1_m2_1_s3')
            d1_m2_2_s3 = self.job_list.get_job_by_name('expid_d1_m2_2_s3')
            d1_m2_3_s3 = self.job_list.get_job_by_name('expid_d1_m2_3_s3')
            d1_m2_4_s3 = self.job_list.get_job_by_name('expid_d1_m2_4_s3')

            self.parser_mock.has_option = MagicMock(return_value=True)
            self.parser_mock.get = MagicMock(return_value="chunk")
            self.job_list._get_date = MagicMock(return_value='d1')

            ordered_jobs_by_date_member = dict()
            ordered_jobs_by_date_member["d1"] = dict()
            ordered_jobs_by_date_member["d1"]["m1"] = [d1_m1_1_s2, d1_m1_1_s3, d1_m1_2_s2, d1_m1_2_s3, d1_m1_3_s2,
                                                       d1_m1_3_s3, d1_m1_4_s2, d1_m1_4_s3]

            ordered_jobs_by_date_member["d1"]["m2"] = [d1_m2_1_s2, d1_m2_1_s3, d1_m2_2_s2, d1_m2_2_s3, d1_m2_3_s2,
                                                       d1_m2_3_s3, d1_m2_4_s2, d1_m2_4_s3]

            self.assertDictEqual(self.job_list._create_sorted_dict_jobs(
                "s2 s3"), ordered_jobs_by_date_member)

    def test_ordered_dict_jobs_running_date_mixed_wrapper(self):
        with mock.patch("autosubmit.job.job.Job.update_parameters", return_value={}):
            date_list = ["d1", "d2"]
            member_list = ["m1", "m2"]
            chunk_list = [1, 2, 3, 4]
            for section, s_value in self.workflows['running_date']['sections'].items():
                self.as_conf.jobs_data[section] = s_value
            self._createDummyJobs(
                self.workflows['running_date'], date_list, member_list, chunk_list)

            self.parser_mock.has_option = MagicMock(return_value=True)
            self.parser_mock.get = MagicMock(side_effect=["chunk", "chunk", "date"])
            self.job_list._get_date = MagicMock(side_effect=['d1', 'd2'])

            d1_m1_1_s2 = self.job_list.get_job_by_name('expid_d1_m1_1_s2')
            d1_m1_2_s2 = self.job_list.get_job_by_name('expid_d1_m1_2_s2')
            d1_m1_3_s2 = self.job_list.get_job_by_name('expid_d1_m1_3_s2')
            d1_m1_4_s2 = self.job_list.get_job_by_name('expid_d1_m1_4_s2')
            d1_m2_1_s2 = self.job_list.get_job_by_name('expid_d1_m2_1_s2')
            d1_m2_2_s2 = self.job_list.get_job_by_name('expid_d1_m2_2_s2')
            d1_m2_3_s2 = self.job_list.get_job_by_name('expid_d1_m2_3_s2')
            d1_m2_4_s2 = self.job_list.get_job_by_name('expid_d1_m2_4_s2')

            d1_m1_1_s3 = self.job_list.get_job_by_name('expid_d1_m1_1_s3')
            d1_m1_2_s3 = self.job_list.get_job_by_name('expid_d1_m1_2_s3')
            d1_m1_3_s3 = self.job_list.get_job_by_name('expid_d1_m1_3_s3')
            d1_m1_4_s3 = self.job_list.get_job_by_name('expid_d1_m1_4_s3')
            d1_m2_1_s3 = self.job_list.get_job_by_name('expid_d1_m2_1_s3')
            d1_m2_2_s3 = self.job_list.get_job_by_name('expid_d1_m2_2_s3')
            d1_m2_3_s3 = self.job_list.get_job_by_name('expid_d1_m2_3_s3')
            d1_m2_4_s3 = self.job_list.get_job_by_name('expid_d1_m2_4_s3')

            d1_s5 = self.job_list.get_job_by_name('expid_d1_s5')

            d2_m1_1_s2 = self.job_list.get_job_by_name('expid_d2_m1_1_s2')
            d2_m1_2_s2 = self.job_list.get_job_by_name('expid_d2_m1_2_s2')
            d2_m1_3_s2 = self.job_list.get_job_by_name('expid_d2_m1_3_s2')
            d2_m1_4_s2 = self.job_list.get_job_by_name('expid_d2_m1_4_s2')
            d2_m2_1_s2 = self.job_list.get_job_by_name('expid_d2_m2_1_s2')
            d2_m2_2_s2 = self.job_list.get_job_by_name('expid_d2_m2_2_s2')
            d2_m2_3_s2 = self.job_list.get_job_by_name('expid_d2_m2_3_s2')
            d2_m2_4_s2 = self.job_list.get_job_by_name('expid_d2_m2_4_s2')

            d2_m1_1_s3 = self.job_list.get_job_by_name('expid_d2_m1_1_s3')
            d2_m1_2_s3 = self.job_list.get_job_by_name('expid_d2_m1_2_s3')
            d2_m1_3_s3 = self.job_list.get_job_by_name('expid_d2_m1_3_s3')
            d2_m1_4_s3 = self.job_list.get_job_by_name('expid_d2_m1_4_s3')
            d2_m2_1_s3 = self.job_list.get_job_by_name('expid_d2_m2_1_s3')
            d2_m2_2_s3 = self.job_list.get_job_by_name('expid_d2_m2_2_s3')
            d2_m2_3_s3 = self.job_list.get_job_by_name('expid_d2_m2_3_s3')
            d2_m2_4_s3 = self.job_list.get_job_by_name('expid_d2_m2_4_s3')

            d2_s5 = self.job_list.get_job_by_name('expid_d2_s5')

            ordered_jobs_by_date_member = dict()
            ordered_jobs_by_date_member["d1"] = dict()
            ordered_jobs_by_date_member["d1"]["m1"] = [d1_m1_1_s2, d1_m1_1_s3, d1_m1_2_s2, d1_m1_2_s3, d1_m1_3_s2,
                                                       d1_m1_3_s3, d1_m1_4_s2, d1_m1_4_s3]

            ordered_jobs_by_date_member["d1"]["m2"] = [d1_m2_1_s2, d1_m2_1_s3, d1_m2_2_s2, d1_m2_2_s3, d1_m2_3_s2,
                                                       d1_m2_3_s3, d1_m2_4_s2, d1_m2_4_s3, d1_s5]
            ordered_jobs_by_date_member["d2"] = dict()
            ordered_jobs_by_date_member["d2"]["m1"] = [d2_m1_1_s2, d2_m1_1_s3, d2_m1_2_s2, d2_m1_2_s3, d2_m1_3_s2,
                                                       d2_m1_3_s3, d2_m1_4_s2, d2_m1_4_s3]

            ordered_jobs_by_date_member["d2"]["m2"] = [d2_m2_1_s2, d2_m2_1_s3, d2_m2_2_s2, d2_m2_2_s3, d2_m2_3_s2,
                                                       d2_m2_3_s3, d2_m2_4_s2, d2_m2_4_s3, d2_s5]

            self.assertDictEqual(self.job_list._create_sorted_dict_jobs(
                "s2 s3 s5"), ordered_jobs_by_date_member)

    def test_ordered_dict_jobs_running_once_mixed_wrapper(self):
        with mock.patch("autosubmit.job.job.Job.update_parameters", return_value={}):
            date_list = ["d1", "d2"]
            member_list = ["m1", "m2"]
            chunk_list = [1, 2, 3, 4]
            for section, s_value in self.workflows['running_once']['sections'].items():
                self.as_conf.jobs_data[section] = s_value
            self._createDummyJobs(
                self.workflows['running_once'], date_list, member_list, chunk_list)

            self.parser_mock.has_option = MagicMock(return_value=True)
            self.parser_mock.get = MagicMock(side_effect=["chunk", "chunk", "once"])
            self.job_list._get_date = MagicMock(side_effect=['d2', 'd1', 'd2'])

            d1_m1_1_s2 = self.job_list.get_job_by_name('expid_d1_m1_1_s2')
            d1_m1_2_s2 = self.job_list.get_job_by_name('expid_d1_m1_2_s2')
            d1_m1_3_s2 = self.job_list.get_job_by_name('expid_d1_m1_3_s2')
            d1_m1_4_s2 = self.job_list.get_job_by_name('expid_d1_m1_4_s2')
            d1_m2_1_s2 = self.job_list.get_job_by_name('expid_d1_m2_1_s2')
            d1_m2_2_s2 = self.job_list.get_job_by_name('expid_d1_m2_2_s2')
            d1_m2_3_s2 = self.job_list.get_job_by_name('expid_d1_m2_3_s2')
            d1_m2_4_s2 = self.job_list.get_job_by_name('expid_d1_m2_4_s2')

            d1_m1_1_s3 = self.job_list.get_job_by_name('expid_d1_m1_1_s3')
            d1_m1_2_s3 = self.job_list.get_job_by_name('expid_d1_m1_2_s3')
            d1_m1_3_s3 = self.job_list.get_job_by_name('expid_d1_m1_3_s3')
            d1_m1_4_s3 = self.job_list.get_job_by_name('expid_d1_m1_4_s3')
            d1_m2_1_s3 = self.job_list.get_job_by_name('expid_d1_m2_1_s3')
            d1_m2_2_s3 = self.job_list.get_job_by_name('expid_d1_m2_2_s3')
            d1_m2_3_s3 = self.job_list.get_job_by_name('expid_d1_m2_3_s3')
            d1_m2_4_s3 = self.job_list.get_job_by_name('expid_d1_m2_4_s3')

            d2_m1_1_s2 = self.job_list.get_job_by_name('expid_d2_m1_1_s2')
            d2_m1_2_s2 = self.job_list.get_job_by_name('expid_d2_m1_2_s2')
            d2_m1_3_s2 = self.job_list.get_job_by_name('expid_d2_m1_3_s2')
            d2_m1_4_s2 = self.job_list.get_job_by_name('expid_d2_m1_4_s2')
            d2_m2_1_s2 = self.job_list.get_job_by_name('expid_d2_m2_1_s2')
            d2_m2_2_s2 = self.job_list.get_job_by_name('expid_d2_m2_2_s2')
            d2_m2_3_s2 = self.job_list.get_job_by_name('expid_d2_m2_3_s2')
            d2_m2_4_s2 = self.job_list.get_job_by_name('expid_d2_m2_4_s2')

            d2_m1_1_s3 = self.job_list.get_job_by_name('expid_d2_m1_1_s3')
            d2_m1_2_s3 = self.job_list.get_job_by_name('expid_d2_m1_2_s3')
            d2_m1_3_s3 = self.job_list.get_job_by_name('expid_d2_m1_3_s3')
            d2_m1_4_s3 = self.job_list.get_job_by_name('expid_d2_m1_4_s3')
            d2_m2_1_s3 = self.job_list.get_job_by_name('expid_d2_m2_1_s3')
            d2_m2_2_s3 = self.job_list.get_job_by_name('expid_d2_m2_2_s3')
            d2_m2_3_s3 = self.job_list.get_job_by_name('expid_d2_m2_3_s3')
            d2_m2_4_s3 = self.job_list.get_job_by_name('expid_d2_m2_4_s3')

            s5 = self.job_list.get_job_by_name('expid_s5')

            ordered_jobs_by_date_member = dict()
            ordered_jobs_by_date_member["d1"] = dict()
            ordered_jobs_by_date_member["d1"]["m1"] = [d1_m1_1_s2, d1_m1_1_s3, d1_m1_2_s2, d1_m1_2_s3, d1_m1_3_s2,
                                                       d1_m1_3_s3, d1_m1_4_s2, d1_m1_4_s3]

            ordered_jobs_by_date_member["d1"]["m2"] = [d1_m2_1_s2, d1_m2_1_s3, d1_m2_2_s2, d1_m2_2_s3, d1_m2_3_s2,
                                                       d1_m2_3_s3, d1_m2_4_s2, d1_m2_4_s3]
            ordered_jobs_by_date_member["d2"] = dict()
            ordered_jobs_by_date_member["d2"]["m1"] = [d2_m1_1_s2, d2_m1_1_s3, d2_m1_2_s2, d2_m1_2_s3, d2_m1_3_s2,
                                                       d2_m1_3_s3, d2_m1_4_s2, d2_m1_4_s3]

            ordered_jobs_by_date_member["d2"]["m2"] = [d2_m2_1_s2, d2_m2_1_s3, d2_m2_2_s2, d2_m2_2_s3, d2_m2_3_s2,
                                                       d2_m2_3_s3, d2_m2_4_s2, d2_m2_4_s3, s5]

            self.assertDictEqual(self.job_list._create_sorted_dict_jobs(
                "s2 s3 s5"), ordered_jobs_by_date_member)

    def test_ordered_dict_jobs_synchronize_date_mixed_wrapper(self):
        with mock.patch("autosubmit.job.job.Job.update_parameters", return_value={}):
            date_list = ["d1", "d2"]
            member_list = ["m1", "m2"]
            chunk_list = [1, 2, 3, 4]
            for section, s_value in self.workflows['synchronize_date']['sections'].items():
                self.as_conf.jobs_data[section] = s_value
            self._createDummyJobs(
                self.workflows['synchronize_date'], date_list, member_list, chunk_list)

            self.parser_mock.has_option = MagicMock(return_value=True)
            self.parser_mock.get = MagicMock(return_value="chunk")
            self.job_list._get_date = MagicMock(
                side_effect=['d2', 'd2', 'd2', 'd2', 'd1', 'd2'])

            d1_m1_1_s2 = self.job_list.get_job_by_name('expid_d1_m1_1_s2')
            d1_m1_2_s2 = self.job_list.get_job_by_name('expid_d1_m1_2_s2')
            d1_m1_3_s2 = self.job_list.get_job_by_name('expid_d1_m1_3_s2')
            d1_m1_4_s2 = self.job_list.get_job_by_name('expid_d1_m1_4_s2')
            d1_m2_1_s2 = self.job_list.get_job_by_name('expid_d1_m2_1_s2')
            d1_m2_2_s2 = self.job_list.get_job_by_name('expid_d1_m2_2_s2')
            d1_m2_3_s2 = self.job_list.get_job_by_name('expid_d1_m2_3_s2')
            d1_m2_4_s2 = self.job_list.get_job_by_name('expid_d1_m2_4_s2')

            d1_m1_1_s3 = self.job_list.get_job_by_name('expid_d1_m1_1_s3')
            d1_m1_2_s3 = self.job_list.get_job_by_name('expid_d1_m1_2_s3')
            d1_m1_3_s3 = self.job_list.get_job_by_name('expid_d1_m1_3_s3')
            d1_m1_4_s3 = self.job_list.get_job_by_name('expid_d1_m1_4_s3')
            d1_m2_1_s3 = self.job_list.get_job_by_name('expid_d1_m2_1_s3')
            d1_m2_2_s3 = self.job_list.get_job_by_name('expid_d1_m2_2_s3')
            d1_m2_3_s3 = self.job_list.get_job_by_name('expid_d1_m2_3_s3')
            d1_m2_4_s3 = self.job_list.get_job_by_name('expid_d1_m2_4_s3')

            d2_m1_1_s2 = self.job_list.get_job_by_name('expid_d2_m1_1_s2')
            d2_m1_2_s2 = self.job_list.get_job_by_name('expid_d2_m1_2_s2')
            d2_m1_3_s2 = self.job_list.get_job_by_name('expid_d2_m1_3_s2')
            d2_m1_4_s2 = self.job_list.get_job_by_name('expid_d2_m1_4_s2')
            d2_m2_1_s2 = self.job_list.get_job_by_name('expid_d2_m2_1_s2')
            d2_m2_2_s2 = self.job_list.get_job_by_name('expid_d2_m2_2_s2')
            d2_m2_3_s2 = self.job_list.get_job_by_name('expid_d2_m2_3_s2')
            d2_m2_4_s2 = self.job_list.get_job_by_name('expid_d2_m2_4_s2')

            d2_m1_1_s3 = self.job_list.get_job_by_name('expid_d2_m1_1_s3')
            d2_m1_2_s3 = self.job_list.get_job_by_name('expid_d2_m1_2_s3')
            d2_m1_3_s3 = self.job_list.get_job_by_name('expid_d2_m1_3_s3')
            d2_m1_4_s3 = self.job_list.get_job_by_name('expid_d2_m1_4_s3')
            d2_m2_1_s3 = self.job_list.get_job_by_name('expid_d2_m2_1_s3')
            d2_m2_2_s3 = self.job_list.get_job_by_name('expid_d2_m2_2_s3')
            d2_m2_3_s3 = self.job_list.get_job_by_name('expid_d2_m2_3_s3')
            d2_m2_4_s3 = self.job_list.get_job_by_name('expid_d2_m2_4_s3')

            _1_s5 = self.job_list.get_job_by_name('expid_1_s5')
            _2_s5 = self.job_list.get_job_by_name('expid_2_s5')
            _3_s5 = self.job_list.get_job_by_name('expid_3_s5')
            _4_s5 = self.job_list.get_job_by_name('expid_4_s5')

            ordered_jobs_by_date_member = dict()
            ordered_jobs_by_date_member["d1"] = dict()
            ordered_jobs_by_date_member["d1"]["m1"] = [d1_m1_1_s2, d1_m1_1_s3, d1_m1_2_s2, d1_m1_2_s3, d1_m1_3_s2,
                                                       d1_m1_3_s3, d1_m1_4_s2, d1_m1_4_s3]

            ordered_jobs_by_date_member["d1"]["m2"] = [d1_m2_1_s2, d1_m2_1_s3, d1_m2_2_s2, d1_m2_2_s3, d1_m2_3_s2,
                                                       d1_m2_3_s3, d1_m2_4_s2, d1_m2_4_s3]
            ordered_jobs_by_date_member["d2"] = dict()
            ordered_jobs_by_date_member["d2"]["m1"] = [d2_m1_1_s2, d2_m1_1_s3, d2_m1_2_s2, d2_m1_2_s3, d2_m1_3_s2,
                                                       d2_m1_3_s3, d2_m1_4_s2, d2_m1_4_s3]

            ordered_jobs_by_date_member["d2"]["m2"] = [d2_m2_1_s2, d2_m2_1_s3, _1_s5, d2_m2_2_s2, d2_m2_2_s3, _2_s5,
                                                       d2_m2_3_s2,
                                                       d2_m2_3_s3, _3_s5, d2_m2_4_s2, d2_m2_4_s3, _4_s5]

            self.assertDictEqual(self.job_list._create_sorted_dict_jobs(
                "s2 s3 s5"), ordered_jobs_by_date_member)

    def test_ordered_dict_jobs_synchronize_member_mixed_wrapper(self):
        with mock.patch("autosubmit.job.job.Job.update_parameters", return_value={}):
            date_list = ["d1", "d2"]
            member_list = ["m1", "m2"]
            chunk_list = [1, 2, 3, 4]
            for section, s_value in self.workflows['synchronize_member']['sections'].items():
                self.as_conf.jobs_data[section] = s_value
            self._createDummyJobs(
                self.workflows['synchronize_member'], date_list, member_list, chunk_list)

            self.parser_mock.has_option = MagicMock(return_value=True)
            self.parser_mock.get = MagicMock(return_value="chunk")
            self.job_list._get_date = MagicMock(side_effect=['d1', 'd2'])

            d1_m1_1_s2 = self.job_list.get_job_by_name('expid_d1_m1_1_s2')
            d1_m1_2_s2 = self.job_list.get_job_by_name('expid_d1_m1_2_s2')
            d1_m1_3_s2 = self.job_list.get_job_by_name('expid_d1_m1_3_s2')
            d1_m1_4_s2 = self.job_list.get_job_by_name('expid_d1_m1_4_s2')
            d1_m2_1_s2 = self.job_list.get_job_by_name('expid_d1_m2_1_s2')
            d1_m2_2_s2 = self.job_list.get_job_by_name('expid_d1_m2_2_s2')
            d1_m2_3_s2 = self.job_list.get_job_by_name('expid_d1_m2_3_s2')
            d1_m2_4_s2 = self.job_list.get_job_by_name('expid_d1_m2_4_s2')

            d1_m1_1_s3 = self.job_list.get_job_by_name('expid_d1_m1_1_s3')
            d1_m1_2_s3 = self.job_list.get_job_by_name('expid_d1_m1_2_s3')
            d1_m1_3_s3 = self.job_list.get_job_by_name('expid_d1_m1_3_s3')
            d1_m1_4_s3 = self.job_list.get_job_by_name('expid_d1_m1_4_s3')
            d1_m2_1_s3 = self.job_list.get_job_by_name('expid_d1_m2_1_s3')
            d1_m2_2_s3 = self.job_list.get_job_by_name('expid_d1_m2_2_s3')
            d1_m2_3_s3 = self.job_list.get_job_by_name('expid_d1_m2_3_s3')
            d1_m2_4_s3 = self.job_list.get_job_by_name('expid_d1_m2_4_s3')

            d2_m1_1_s2 = self.job_list.get_job_by_name('expid_d2_m1_1_s2')
            d2_m1_2_s2 = self.job_list.get_job_by_name('expid_d2_m1_2_s2')
            d2_m1_3_s2 = self.job_list.get_job_by_name('expid_d2_m1_3_s2')
            d2_m1_4_s2 = self.job_list.get_job_by_name('expid_d2_m1_4_s2')
            d2_m2_1_s2 = self.job_list.get_job_by_name('expid_d2_m2_1_s2')
            d2_m2_2_s2 = self.job_list.get_job_by_name('expid_d2_m2_2_s2')
            d2_m2_3_s2 = self.job_list.get_job_by_name('expid_d2_m2_3_s2')
            d2_m2_4_s2 = self.job_list.get_job_by_name('expid_d2_m2_4_s2')

            d2_m1_1_s3 = self.job_list.get_job_by_name('expid_d2_m1_1_s3')
            d2_m1_2_s3 = self.job_list.get_job_by_name('expid_d2_m1_2_s3')
            d2_m1_3_s3 = self.job_list.get_job_by_name('expid_d2_m1_3_s3')
            d2_m1_4_s3 = self.job_list.get_job_by_name('expid_d2_m1_4_s3')
            d2_m2_1_s3 = self.job_list.get_job_by_name('expid_d2_m2_1_s3')
            d2_m2_2_s3 = self.job_list.get_job_by_name('expid_d2_m2_2_s3')
            d2_m2_3_s3 = self.job_list.get_job_by_name('expid_d2_m2_3_s3')
            d2_m2_4_s3 = self.job_list.get_job_by_name('expid_d2_m2_4_s3')

            d1_1_s5 = self.job_list.get_job_by_name('expid_d1_1_s5')
            d1_2_s5 = self.job_list.get_job_by_name('expid_d1_2_s5')
            d1_3_s5 = self.job_list.get_job_by_name('expid_d1_3_s5')
            d1_4_s5 = self.job_list.get_job_by_name('expid_d1_4_s5')

            d2_1_s5 = self.job_list.get_job_by_name('expid_d2_1_s5')
            d2_2_s5 = self.job_list.get_job_by_name('expid_d2_2_s5')
            d2_3_s5 = self.job_list.get_job_by_name('expid_d2_3_s5')
            d2_4_s5 = self.job_list.get_job_by_name('expid_d2_4_s5')

            ordered_jobs_by_date_member = dict()
            ordered_jobs_by_date_member["d1"] = dict()
            ordered_jobs_by_date_member["d1"]["m1"] = [d1_m1_1_s2, d1_m1_1_s3, d1_m1_2_s2, d1_m1_2_s3, d1_m1_3_s2,
                                                       d1_m1_3_s3, d1_m1_4_s2, d1_m1_4_s3]

            ordered_jobs_by_date_member["d1"]["m2"] = [d1_m2_1_s2, d1_m2_1_s3, d1_1_s5, d1_m2_2_s2, d1_m2_2_s3, d1_2_s5,
                                                       d1_m2_3_s2,
                                                       d1_m2_3_s3, d1_3_s5, d1_m2_4_s2, d1_m2_4_s3, d1_4_s5]
            ordered_jobs_by_date_member["d2"] = dict()
            ordered_jobs_by_date_member["d2"]["m1"] = [d2_m1_1_s2, d2_m1_1_s3, d2_m1_2_s2, d2_m1_2_s3, d2_m1_3_s2,
                                                       d2_m1_3_s3, d2_m1_4_s2, d2_m1_4_s3]

            ordered_jobs_by_date_member["d2"]["m2"] = [d2_m2_1_s2, d2_m2_1_s3, d2_1_s5, d2_m2_2_s2, d2_m2_2_s3, d2_2_s5,
                                                       d2_m2_3_s2,
                                                       d2_m2_3_s3, d2_3_s5, d2_m2_4_s2, d2_m2_4_s3, d2_4_s5]

            self.assertDictEqual(self.job_list._create_sorted_dict_jobs(
                "s2 s3 s5"), ordered_jobs_by_date_member)

    def test_check_real_package_wrapper_limits(self):
        with mock.patch("autosubmit.job.job.Job.update_parameters", return_value={}):

            # want to test self.job_packager.check_real_package_wrapper_limits(package,max_jobs_to_submit,packages_to_submit)
            date_list = ["d1"]
            member_list = ["m1", "m2"]
            chunk_list = [1, 2, 3, 4]
            for section, s_value in self.workflows['basic']['sections'].items():
                self.as_conf.jobs_data[section] = s_value
            self._createDummyJobs(
                self.workflows['basic'], date_list, member_list, chunk_list)

            self.job_list.get_job_by_name(
                'expid_d1_m1_s1').status = Status.COMPLETED
            self.job_list.get_job_by_name(
                'expid_d1_m2_s1').status = Status.COMPLETED

            self.job_list.get_job_by_name('expid_d1_m1_1_s2').status = Status.READY
            self.job_list.get_job_by_name('expid_d1_m2_1_s2').status = Status.READY

            wrapper_expression = "s2 s3"
            d1_m1_1_s2 = self.job_list.get_job_by_name('expid_d1_m1_1_s2')
            d1_m1_2_s2 = self.job_list.get_job_by_name('expid_d1_m1_2_s2')
            d1_m1_3_s2 = self.job_list.get_job_by_name('expid_d1_m1_3_s2')
            d1_m1_4_s2 = self.job_list.get_job_by_name('expid_d1_m1_4_s2')
            d1_m2_1_s2 = self.job_list.get_job_by_name('expid_d1_m2_1_s2')
            d1_m2_2_s2 = self.job_list.get_job_by_name('expid_d1_m2_2_s2')
            d1_m2_3_s2 = self.job_list.get_job_by_name('expid_d1_m2_3_s2')
            d1_m2_4_s2 = self.job_list.get_job_by_name('expid_d1_m2_4_s2')

            d1_m1_1_s3 = self.job_list.get_job_by_name('expid_d1_m1_1_s3')
            d1_m1_2_s3 = self.job_list.get_job_by_name('expid_d1_m1_2_s3')
            d1_m1_3_s3 = self.job_list.get_job_by_name('expid_d1_m1_3_s3')
            d1_m1_4_s3 = self.job_list.get_job_by_name('expid_d1_m1_4_s3')
            d1_m2_1_s3 = self.job_list.get_job_by_name('expid_d1_m2_1_s3')
            d1_m2_2_s3 = self.job_list.get_job_by_name('expid_d1_m2_2_s3')
            d1_m2_3_s3 = self.job_list.get_job_by_name('expid_d1_m2_3_s3')
            d1_m2_4_s3 = self.job_list.get_job_by_name('expid_d1_m2_4_s3')

            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"] = dict()
            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"]["m1"] = [d1_m1_1_s2, d1_m1_1_s3, d1_m1_2_s2,
                                                                                  d1_m1_2_s3,
                                                                                  d1_m1_3_s2, d1_m1_3_s3, d1_m1_4_s2,
                                                                                  d1_m1_4_s3]

            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"]["m2"] = [d1_m2_1_s2, d1_m2_1_s3, d1_m2_2_s2,
                                                                                  d1_m2_2_s3,
                                                                                  d1_m2_3_s2, d1_m2_3_s3, d1_m2_4_s2,
                                                                                  d1_m2_4_s3]

            self.job_packager.jobs_in_wrapper = wrapper_expression

            self.job_packager.retrials = 0
            # test vertical-wrapper
            self.job_packager.wrapper_type["WRAPPER_V"] = 'vertical'
            self.job_packager.current_wrapper_section = "WRAPPER_V"
            self.as_conf.experiment_data["WRAPPERS"][self.job_packager.current_wrapper_section] = {}
            self.as_conf.experiment_data["WRAPPERS"][self.job_packager.current_wrapper_section]["TYPE"] = "vertical"
            self.as_conf.experiment_data["WRAPPERS"][self.job_packager.current_wrapper_section][
                "JOBS_IN_WRAPPER"] = "S2 S3"
            package_m1_s2_s3 = [d1_m1_1_s2, d1_m1_1_s3, d1_m1_2_s2, d1_m1_2_s3]
            package_m2_s2_s3 = [d1_m2_1_s2, d1_m2_1_s3, d1_m2_2_s2, d1_m2_2_s3]

            packages_v = [JobPackageVertical(
                package_m1_s2_s3, configuration=self.as_conf),
                JobPackageVertical(package_m2_s2_s3, configuration=self.as_conf)]

            for package in packages_v:
                min_v, min_h, balanced = self.job_packager.check_real_package_wrapper_limits(package)
                self.assertTrue(balanced)
                self.assertEqual(min_v, 4)
                self.assertEqual(min_h, 1)
            # test horizontal-wrapper

            self.job_packager.wrapper_type["WRAPPER_H"] = 'horizontal'
            self.job_packager.current_wrapper_section = "WRAPPER_H"
            self.as_conf.experiment_data["WRAPPERS"][self.job_packager.current_wrapper_section] = {}
            self.as_conf.experiment_data["WRAPPERS"][self.job_packager.current_wrapper_section]["TYPE"] = "horizontal"
            self.as_conf.experiment_data["WRAPPERS"][self.job_packager.current_wrapper_section][
                "JOBS_IN_WRAPPER"] = "S2 S3"
            packages_h = [JobPackageHorizontal(
                package_m1_s2_s3, configuration=self.as_conf),
                JobPackageHorizontal(package_m2_s2_s3, configuration=self.as_conf)]
            for package in packages_h:
                min_v, min_h, balanced = self.job_packager.check_real_package_wrapper_limits(package)
                self.assertTrue(balanced)
                self.assertEqual(min_v, 1)
                self.assertEqual(min_h, 4)
            # test horizontal-vertical
            self.job_packager.wrapper_type["WRAPPER_HV"] = 'horizontal-vertical'
            self.job_packager.current_wrapper_section = "WRAPPER_HV"
            self.as_conf.experiment_data["WRAPPERS"][self.job_packager.current_wrapper_section] = {}
            self.as_conf.experiment_data["WRAPPERS"][self.job_packager.current_wrapper_section][
                "TYPE"] = "horizontal-vertical"
            self.as_conf.experiment_data["WRAPPERS"][self.job_packager.current_wrapper_section][
                "JOBS_IN_WRAPPER"] = "S2 S3"
            jobs_resources = dict()
            ####
            total_wallclock = '00:00'
            self._current_processors = 0
            current_package = [package_m1_s2_s3, package_m2_s2_s3]
            max_procs = 99999
            ####
            packages_hv = [
                JobPackageHorizontalVertical(current_package, max_procs, total_wallclock, jobs_resources=jobs_resources,
                                             configuration=self.as_conf,
                                             wrapper_section=self.job_packager.current_wrapper_section)]

            for package in packages_hv:
                min_v, min_h, balanced = self.job_packager.check_real_package_wrapper_limits(package)
                self.assertTrue(balanced)
                self.assertEqual(min_v, 2)
                self.assertEqual(min_h, 4)
            # unbalanced package
            unbalanced_package = [d1_m2_1_s2, d1_m2_1_s3, d1_m2_2_s2]
            current_package = [package_m1_s2_s3, unbalanced_package, package_m2_s2_s3]
            packages_hv_unbalanced = [
                JobPackageHorizontalVertical(current_package, max_procs, total_wallclock, jobs_resources=jobs_resources,
                                             configuration=self.as_conf,
                                             wrapper_section=self.job_packager.current_wrapper_section)]
            for package in packages_hv_unbalanced:
                min_v, min_h, balanced = self.job_packager.check_real_package_wrapper_limits(package)
                self.assertFalse(balanced)
                self.assertEqual(min_v, 3)
                self.assertEqual(min_h, 3)
            # test vertical-horizontal
            self.job_packager.wrapper_type["WRAPPER_VH"] = 'vertical-horizontal'
            self.job_packager.current_wrapper_section = "WRAPPER_VH"
            self.as_conf.experiment_data["WRAPPERS"][self.job_packager.current_wrapper_section] = {}
            self.as_conf.experiment_data["WRAPPERS"][self.job_packager.current_wrapper_section][
                "TYPE"] = "vertical-horizontal"
            self.as_conf.experiment_data["WRAPPERS"][self.job_packager.current_wrapper_section][
                "JOBS_IN_WRAPPER"] = "S2 S3"
            current_package = [package_m1_s2_s3, package_m2_s2_s3]
            packages_vh = [JobPackageVerticalHorizontal(
                current_package, max_procs, total_wallclock, jobs_resources=jobs_resources, configuration=self.as_conf,
                wrapper_section=self.job_packager.current_wrapper_section)]
            for package in packages_vh:
                min_v, min_h, balanced = self.job_packager.check_real_package_wrapper_limits(package)
                self.assertTrue(balanced)
                self.assertEqual(min_v, 4)
                self.assertEqual(min_h, 2)
            current_package = [package_m1_s2_s3, unbalanced_package, package_m2_s2_s3]
            packages_vh_unbalanced = [JobPackageVerticalHorizontal(
                current_package, max_procs, total_wallclock, jobs_resources=jobs_resources, configuration=self.as_conf,
                wrapper_section=self.job_packager.current_wrapper_section)]
            for package in packages_vh_unbalanced:
                min_v, min_h, balanced = self.job_packager.check_real_package_wrapper_limits(package)
                self.assertFalse(balanced)
                self.assertEqual(min_v, 3)
                self.assertEqual(min_h, 3)

    def test_check_jobs_to_run_first(self):
        with mock.patch("autosubmit.job.job.Job.update_parameters", return_value={}):

            # want to test self.job_packager.check_jobs_to_run_first(package)
            date_list = ["d1"]
            member_list = ["m1", "m2"]
            chunk_list = [1, 2, 3, 4]
            for section, s_value in self.workflows['basic']['sections'].items():
                self.as_conf.jobs_data[section] = s_value
            self._createDummyJobs(
                self.workflows['basic'], date_list, member_list, chunk_list)

            self.job_list.get_job_by_name(
                'expid_d1_m1_s1').status = Status.COMPLETED
            self.job_list.get_job_by_name(
                'expid_d1_m2_s1').status = Status.COMPLETED

            self.job_list.get_job_by_name('expid_d1_m1_1_s2').status = Status.READY
            self.job_list.get_job_by_name('expid_d1_m2_1_s2').status = Status.READY

            wrapper_expression = "s2 s3"
            d1_m1_1_s2 = self.job_list.get_job_by_name('expid_d1_m1_1_s2')
            d1_m1_2_s2 = self.job_list.get_job_by_name('expid_d1_m1_2_s2')
            d1_m1_3_s2 = self.job_list.get_job_by_name('expid_d1_m1_3_s2')
            d1_m1_4_s2 = self.job_list.get_job_by_name('expid_d1_m1_4_s2')
            d1_m2_1_s2 = self.job_list.get_job_by_name('expid_d1_m2_1_s2')
            d1_m2_2_s2 = self.job_list.get_job_by_name('expid_d1_m2_2_s2')
            d1_m2_3_s2 = self.job_list.get_job_by_name('expid_d1_m2_3_s2')
            d1_m2_4_s2 = self.job_list.get_job_by_name('expid_d1_m2_4_s2')

            d1_m1_1_s3 = self.job_list.get_job_by_name('expid_d1_m1_1_s3')
            d1_m1_2_s3 = self.job_list.get_job_by_name('expid_d1_m1_2_s3')
            d1_m1_3_s3 = self.job_list.get_job_by_name('expid_d1_m1_3_s3')
            d1_m1_4_s3 = self.job_list.get_job_by_name('expid_d1_m1_4_s3')
            d1_m2_1_s3 = self.job_list.get_job_by_name('expid_d1_m2_1_s3')
            d1_m2_2_s3 = self.job_list.get_job_by_name('expid_d1_m2_2_s3')
            d1_m2_3_s3 = self.job_list.get_job_by_name('expid_d1_m2_3_s3')
            d1_m2_4_s3 = self.job_list.get_job_by_name('expid_d1_m2_4_s3')

            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"] = dict()
            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"]["m1"] = [d1_m1_1_s2, d1_m1_1_s3, d1_m1_2_s2,
                                                                                  d1_m1_2_s3,
                                                                                  d1_m1_3_s2, d1_m1_3_s3, d1_m1_4_s2,
                                                                                  d1_m1_4_s3]

            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"]["m2"] = [d1_m2_1_s2, d1_m2_1_s3, d1_m2_2_s2,
                                                                                  d1_m2_2_s3,
                                                                                  d1_m2_3_s2, d1_m2_3_s3, d1_m2_4_s2,
                                                                                  d1_m2_4_s3]

            self.job_packager.jobs_in_wrapper = wrapper_expression

            self.job_packager.retrials = 0
            # test vertical-wrapper
            self.job_packager.wrapper_type["WRAPPER_V"] = 'vertical'
            self.job_packager.current_wrapper_section = "WRAPPER_V"
            self.as_conf.experiment_data["WRAPPERS"][self.job_packager.current_wrapper_section] = {}
            self.as_conf.experiment_data["WRAPPERS"][self.job_packager.current_wrapper_section]["TYPE"] = "vertical"
            self.as_conf.experiment_data["WRAPPERS"][self.job_packager.current_wrapper_section][
                "JOBS_IN_WRAPPER"] = "S2 S3"
            package_m1_s2_s3 = [d1_m1_1_s2, d1_m1_1_s3, d1_m1_2_s2, d1_m1_2_s3]

            packages_v = [JobPackageVertical(package_m1_s2_s3, configuration=self.as_conf)]
            self.job_packager._jobs_list.jobs_to_run_first = []
            for p in packages_v:
                p2, run_first = self.job_packager.check_jobs_to_run_first(p)
                self.assertEqual(p2.jobs, p.jobs)
                self.assertEqual(run_first, False)
            self.job_packager._jobs_list.jobs_to_run_first = [d1_m1_1_s2, d1_m1_1_s3]
            for p in packages_v:
                p2, run_first = self.job_packager.check_jobs_to_run_first(p)
                self.assertEqual(p2.jobs, [d1_m1_1_s2, d1_m1_1_s3])
                self.assertEqual(run_first, True)

    def test_calculate_wrapper_bounds(self):
        with mock.patch("autosubmit.job.job.Job.update_parameters", return_value={}):
            # want to test self.job_packager.calculate_wrapper_bounds(section_list)
            self.job_packager.current_wrapper_section = "WRAPPER"
            self.job_packager._as_config.experiment_data["WRAPPERS"][self.job_packager.current_wrapper_section] = {}
            self.job_packager._as_config.experiment_data["WRAPPERS"][self.job_packager.current_wrapper_section][
                "TYPE"] = "vertical"
            self.job_packager._as_config.experiment_data["WRAPPERS"][self.job_packager.current_wrapper_section][
                "JOBS_IN_WRAPPER"] = "S2 S3"
            section_list = ["S2", "S3"]
            # default wrapper limits
            wrapper_limits = {'max': 9999999,
                              'max_by_section': {'S2': 9999999, 'S3': 9999999},
                              'max_h': 9999999,
                              'max_v': 9999999,
                              'min': 1,
                              'min_h': 1,
                              'min_v': 1,
                              'real_min': 2
                              }
            returned_wrapper_limits = self.job_packager.calculate_wrapper_bounds(section_list)
            self.assertDictEqual(returned_wrapper_limits, wrapper_limits)
            self.job_packager._as_config.experiment_data["WRAPPERS"]["MIN_WRAPPED"] = 3
            self.job_packager._as_config.experiment_data["WRAPPERS"]["MAX_WRAPPED"] = 5
            self.job_packager._as_config.experiment_data["WRAPPERS"]["MIN_WRAPPED_H"] = 2
            self.job_packager._as_config.experiment_data["WRAPPERS"]["MIN_WRAPPED_V"] = 3
            self.job_packager._as_config.experiment_data["WRAPPERS"]["MAX_WRAPPED_H"] = 4
            self.job_packager._as_config.experiment_data["WRAPPERS"]["MAX_WRAPPED_V"] = 5

            wrapper_limits = {'max': 5 * 4,
                              'max_by_section': {'S2': 5 * 4, 'S3': 5 * 4},
                              'max_h': 4,
                              'max_v': 5 * 4,
                              'min': 3,
                              'min_h': 2,
                              'min_v': 3,
                              'real_min': 3
                              }
            returned_wrapper_limits = self.job_packager.calculate_wrapper_bounds(section_list)
            self.assertDictEqual(returned_wrapper_limits, wrapper_limits)

            self.job_packager._as_config.experiment_data["WRAPPERS"][self.job_packager.current_wrapper_section][
                "TYPE"] = "horizontal"
            wrapper_limits = {'max': 5 * 4,
                              'max_by_section': {'S2': 5 * 4, 'S3': 5 * 4},
                              'max_h': 4 * 5,
                              'max_v': 5,
                              'min': 3,
                              'min_h': 2,
                              'min_v': 3,
                              'real_min': 3,
                              }
            returned_wrapper_limits = self.job_packager.calculate_wrapper_bounds(section_list)
            self.assertDictEqual(returned_wrapper_limits, wrapper_limits)

            self.job_packager._as_config.experiment_data["WRAPPERS"][self.job_packager.current_wrapper_section][
                "TYPE"] = "horizontal-vertical"
            wrapper_limits = {'max': 5 * 4,
                              'max_by_section': {'S2': 5 * 4, 'S3': 5 * 4},
                              'max_h': 4,
                              'max_v': 5,
                              'min': 3,
                              'min_h': 2,
                              'min_v': 3,
                              'real_min': 3
                              }
            returned_wrapper_limits = self.job_packager.calculate_wrapper_bounds(section_list)
            self.assertDictEqual(returned_wrapper_limits, wrapper_limits)

            self.job_packager._as_config.experiment_data["WRAPPERS"][self.job_packager.current_wrapper_section][
                "TYPE"] = "vertical-horizontal"
            wrapper_limits = {'max': 5 * 4,
                              'max_by_section': {'S2': 5 * 4, 'S3': 5 * 4},
                              'max_h': 4,
                              'max_v': 5,
                              'min': 3,
                              'min_h': 2,
                              'min_v': 3,
                              'real_min': 3
                              }
            returned_wrapper_limits = self.job_packager.calculate_wrapper_bounds(section_list)
            self.assertDictEqual(returned_wrapper_limits, wrapper_limits)

            self.job_packager._as_config.experiment_data["WRAPPERS"][self.job_packager.current_wrapper_section][
                "MIN_WRAPPED"] = 3
            self.job_packager._as_config.experiment_data["WRAPPERS"][self.job_packager.current_wrapper_section][
                "MAX_WRAPPED"] = 5
            self.job_packager._as_config.experiment_data["WRAPPERS"][self.job_packager.current_wrapper_section][
                "MIN_WRAPPED_H"] = 2
            self.job_packager._as_config.experiment_data["WRAPPERS"][self.job_packager.current_wrapper_section][
                "MIN_WRAPPED_V"] = 3
            self.job_packager._as_config.experiment_data["WRAPPERS"][self.job_packager.current_wrapper_section][
                "MAX_WRAPPED_H"] = 4
            self.job_packager._as_config.experiment_data["WRAPPERS"][self.job_packager.current_wrapper_section][
                "MAX_WRAPPED_V"] = 5

            returned_wrapper_limits = self.job_packager.calculate_wrapper_bounds(section_list)
            self.assertDictEqual(returned_wrapper_limits, wrapper_limits)

            del self.job_packager._as_config.experiment_data["WRAPPERS"]["MIN_WRAPPED"]
            del self.job_packager._as_config.experiment_data["WRAPPERS"]["MAX_WRAPPED"]
            del self.job_packager._as_config.experiment_data["WRAPPERS"]["MIN_WRAPPED_H"]
            del self.job_packager._as_config.experiment_data["WRAPPERS"]["MIN_WRAPPED_V"]
            del self.job_packager._as_config.experiment_data["WRAPPERS"]["MAX_WRAPPED_H"]
            del self.job_packager._as_config.experiment_data["WRAPPERS"]["MAX_WRAPPED_V"]
            returned_wrapper_limits = self.job_packager.calculate_wrapper_bounds(section_list)
            self.assertDictEqual(returned_wrapper_limits, wrapper_limits)

            wrapper_limits = {'max': 5 * 4,
                              'max_by_section': {'S2': 5 * 4, 'S3': 5 * 4},
                              'max_h': 4,
                              'max_v': 5,
                              'min': 3,
                              'min_h': 2,
                              'min_v': 3,
                              'real_min': 3
                              }
            returned_wrapper_limits = self.job_packager.calculate_wrapper_bounds(section_list)
            self.assertDictEqual(returned_wrapper_limits, wrapper_limits)

    def test_check_packages_respect_wrapper_policy(self):
        with mock.patch("autosubmit.job.job.Job.update_parameters", return_value={}):

            # want to test self.job_packager.check_packages_respect_wrapper_policy(built_packages_tmp,packages_to_submit,max_jobs_to_submit,wrapper_limits)
            date_list = ["d1"]
            member_list = ["m1", "m2"]
            chunk_list = [1, 2, 3, 4]
            for section, s_value in self.workflows['basic']['sections'].items():
                self.as_conf.jobs_data[section] = s_value
            self._createDummyJobs(
                self.workflows['basic'], date_list, member_list, chunk_list)

            self.job_list.get_job_by_name(
                'expid_d1_m1_s1').status = Status.COMPLETED
            self.job_list.get_job_by_name(
                'expid_d1_m2_s1').status = Status.COMPLETED

            self.job_list.get_job_by_name('expid_d1_m1_1_s2').status = Status.READY
            self.job_list.get_job_by_name('expid_d1_m2_1_s2').status = Status.READY

            wrapper_expression = "s2 s3"
            d1_m1_1_s2 = self.job_list.get_job_by_name('expid_d1_m1_1_s2')
            d1_m1_2_s2 = self.job_list.get_job_by_name('expid_d1_m1_2_s2')
            d1_m1_3_s2 = self.job_list.get_job_by_name('expid_d1_m1_3_s2')
            d1_m1_4_s2 = self.job_list.get_job_by_name('expid_d1_m1_4_s2')
            d1_m2_1_s2 = self.job_list.get_job_by_name('expid_d1_m2_1_s2')
            d1_m2_2_s2 = self.job_list.get_job_by_name('expid_d1_m2_2_s2')
            d1_m2_3_s2 = self.job_list.get_job_by_name('expid_d1_m2_3_s2')
            d1_m2_4_s2 = self.job_list.get_job_by_name('expid_d1_m2_4_s2')

            d1_m1_1_s3 = self.job_list.get_job_by_name('expid_d1_m1_1_s3')
            d1_m1_2_s3 = self.job_list.get_job_by_name('expid_d1_m1_2_s3')
            d1_m1_3_s3 = self.job_list.get_job_by_name('expid_d1_m1_3_s3')
            d1_m1_4_s3 = self.job_list.get_job_by_name('expid_d1_m1_4_s3')
            d1_m2_1_s3 = self.job_list.get_job_by_name('expid_d1_m2_1_s3')
            d1_m2_2_s3 = self.job_list.get_job_by_name('expid_d1_m2_2_s3')
            d1_m2_3_s3 = self.job_list.get_job_by_name('expid_d1_m2_3_s3')
            d1_m2_4_s3 = self.job_list.get_job_by_name('expid_d1_m2_4_s3')

            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"] = dict()
            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"]["m1"] = [d1_m1_1_s2, d1_m1_1_s3, d1_m1_2_s2,
                                                                                  d1_m1_2_s3,
                                                                                  d1_m1_3_s2, d1_m1_3_s3, d1_m1_4_s2,
                                                                                  d1_m1_4_s3]

            self.job_list._ordered_jobs_by_date_member["WRAPPERS"]["d1"]["m2"] = [d1_m2_1_s2, d1_m2_1_s3, d1_m2_2_s2,
                                                                                  d1_m2_2_s3,
                                                                                  d1_m2_3_s2, d1_m2_3_s3, d1_m2_4_s2,
                                                                                  d1_m2_4_s3]

            self.job_packager.jobs_in_wrapper = wrapper_expression

            self.job_packager.retrials = 0
            # test vertical-wrapper
            self.job_packager.wrapper_type["WRAPPER_V"] = 'vertical'
            self.job_packager.current_wrapper_section = "WRAPPER_V"
            self.as_conf.experiment_data["WRAPPERS"][self.job_packager.current_wrapper_section] = {}
            self.as_conf.experiment_data["WRAPPERS"][self.job_packager.current_wrapper_section]["TYPE"] = "horizontal"
            self.as_conf.experiment_data["WRAPPERS"][self.job_packager.current_wrapper_section][
                "JOBS_IN_WRAPPER"] = "S2 S3"
            packages_to_submit = []
            max_jobs_to_submit = 2
            wrapper_limits = {'max': 9999999,
                              'max_by_section': {'S2': 9999999, 'S3': 9999999},
                              'max_h': 9999999,
                              'max_v': 9999999,
                              'min': 2,
                              'min_h': 1,
                              'min_v': 2,
                              'real_min': 2
                              }
            package = [d1_m1_1_s2, d1_m1_1_s2, d1_m1_1_s2, d1_m1_1_s2, d1_m1_1_s2]
            packages_h = [JobPackageHorizontal(
                package, configuration=self.as_conf)]

            self.job_packager.wrapper_policy = {}
            self.job_packager.wrapper_policy["WRAPPER_V"] = "flexible"
            packages_to_submit2, max_jobs_to_submit2 = self.job_packager.check_packages_respect_wrapper_policy(
                packages_h, packages_to_submit,
                max_jobs_to_submit, wrapper_limits)
            self.assertEqual(max_jobs_to_submit2, max_jobs_to_submit - 1)
            self.assertEqual(packages_to_submit2, packages_h)

            wrapper_limits = {'max': 2,
                              'max_by_section': {'S2': 2, 'S3': 2},
                              'max_h': 2,
                              'max_v': 2,
                              'min': 2,
                              'min_h': 2,
                              'min_v': 2,
                              'real_min': 2
                              }
            self.job_packager.jobs_in_wrapper = {self.job_packager.current_wrapper_section: {'S2': 2, 'S3': 2}}
            packages_to_submit = []
            packages_to_submit2, max_jobs_to_submit2 = self.job_packager.check_packages_respect_wrapper_policy(
                packages_h, packages_to_submit,
                max_jobs_to_submit, wrapper_limits)
            self.assertEqual(max_jobs_to_submit2, 0)
            self.assertEqual(len(packages_to_submit2), 2)
            for p in packages_to_submit2:
                self.assertEqual(type(p), JobPackageSimple)

            self.job_packager.wrapper_policy["WRAPPER_V"] = "mixed"
            packages_to_submit = []
            with self.assertRaises(log.log.AutosubmitCritical):
                self.job_packager.check_packages_respect_wrapper_policy(packages_h, packages_to_submit,
                                                                        max_jobs_to_submit, wrapper_limits)
            self.job_packager.wrapper_policy["WRAPPER_V"] = "strict"
            packages_to_submit = []
            with self.assertRaises(log.log.AutosubmitCritical):
                self.job_packager.check_packages_respect_wrapper_policy(packages_h, packages_to_submit,
                                                                        max_jobs_to_submit, wrapper_limits)

    # def test_build_packages(self):
    # want to test self.job_packager.build_packages()
    # TODO: implement this test in the future

    def _createDummyJobs(self, sections_dict, date_list, member_list, chunk_list):
        for section, section_dict in sections_dict.get('sections').items():
            running = section_dict['RUNNING']
            wallclock = section_dict['WALLCLOCK']

            if running == 'once':
                name = 'expid_' + section
                job = self._createDummyJob(name, wallclock, section)
                self.job_list._job_list.append(job)
            elif running == 'date':
                for date in date_list:
                    name = 'expid_' + date + "_" + section
                    job = self._createDummyJob(name, wallclock, section, date)
                    self.job_list._job_list.append(job)
            elif running == 'member':
                for date in date_list:
                    for member in member_list:
                        name = 'expid_' + date + "_" + member + "_" + section
                        job = self._createDummyJob(
                            name, wallclock, section, date, member)
                        self.job_list._job_list.append(job)
            elif running == 'chunk':
                synchronize_type = section_dict['SYNCHRONIZE'] if 'SYNCHRONIZE' in section_dict else None
                if synchronize_type == 'date':
                    for chunk in chunk_list:
                        name = 'expid_' + str(chunk) + "_" + section
                        job = self._createDummyJob(
                            name, wallclock, section, None, None, chunk)
                        self.job_list._job_list.append(job)
                elif synchronize_type == 'member':
                    for date in date_list:
                        for chunk in chunk_list:
                            name = 'expid_' + date + "_" + \
                                   str(chunk) + "_" + section
                            job = self._createDummyJob(
                                name, wallclock, section, date, None, chunk)
                            self.job_list._job_list.append(job)
                else:
                    for date in date_list:
                        for member in member_list:
                            for chunk in chunk_list:
                                name = 'expid_' + date + "_" + member + \
                                       "_" + str(chunk) + "_" + section
                                job = self._createDummyJob(
                                    name, wallclock, section, date, member, chunk)
                                self.job_list._job_list.append(job)

        self.job_list._date_list = date_list
        self.job_list._member_list = member_list
        self.job_list._chunk_list = chunk_list

        self.job_list._dic_jobs = DicJobs(date_list, member_list, chunk_list, "", 0, self.as_conf)
        self._manage_dependencies(sections_dict)
        for job in self.job_list.get_job_list():
            job._init_runtime_parameters()
            # job.update_parameters = MagicMock()

    def _manage_dependencies(self, sections_dict):
        for job in self.job_list.get_job_list():
            section = job.section
            dependencies = sections_dict['sections'][section][
                'DEPENDENCIES'] if 'DEPENDENCIES' in sections_dict['sections'][section] else ''
            self._manage_job_dependencies(job, dependencies, sections_dict)

    def _manage_job_dependencies(self, job, dependencies, sections_dict):
        for key in dependencies.split():
            if '-' not in key:
                dependency = Dependency(key)
            else:
                sign = '-' if '-' in key else '+'
                key_split = key.split(sign)
                section = key_split[0]
                distance = key_split[1]
                dependency_running_type = sections_dict['sections'][section]['RUNNING']
                dependency = Dependency(section, int(
                    distance), dependency_running_type, sign)

            skip, (chunk, member, date) = self.job_list._calculate_dependency_metadata(job.chunk,
                                                                                       self.job_list.get_chunk_list(),
                                                                                       job.member,
                                                                                       self.job_list.get_member_list(),
                                                                                       job.date,
                                                                                       self.job_list.get_date_list(),
                                                                                       dependency)
            if skip:
                continue

            for parent in self._filter_jobs(dependency.section, date, member, chunk):
                job.add_parent(parent)

    def _filter_jobs(self, section, date=None, member=None, chunk=None):
        # TODO: improve the efficiency
        jobs = [job for job in self.job_list.get_job_list() if
                job.section == section and job.date == date and job.member == member and job.chunk == chunk]
        return jobs

    def _createDummyJob(self, name, total_wallclock, section, date=None, member=None, chunk=None):
        job_id = randrange(1, 999)
        job = Job(name, job_id, Status.WAITING, 0)
        job.type = randrange(0, 2)
        job.hold = False
        job.wallclock = total_wallclock
        job.platform = self._platform

        job.date = date
        job.member = member
        job.chunk = chunk
        job.section = section

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
