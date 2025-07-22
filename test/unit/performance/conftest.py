# Copyright 2015-2025 Earth Sciences Department, BSC-CNS
#
# This file is part of Autosubmit.
#
# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>. 

import pytest 

from autosubmit.job.job import Job 
from autosubmit.performance.type_job.SIM.SIM_performance import SIMPerformance
from test.unit.performance.support_job import JobTestPerformance
from test.unit.performance.support_job import TransformToJob
from test.unit.performance.utils import Utils
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmit.notifications.mail_notifier import MailNotifier

# -- fixtures for Job creation and performance calculations

@pytest.fixture
def job_factory():
    """
    Factory fixture to create Job instances from JobTestPerformance instances.
    """
    transformer = TransformToJob()
    
    class JobFactory:

        @staticmethod
        def create_job(test_job: JobTestPerformance) -> Job:
            return transformer.transform(test_job)
    
        @staticmethod
        def create_job_list(test_job_list: list[JobTestPerformance]) -> list[Job]:
            return [JobFactory.create_job(test_job) for test_job in test_job_list]
    
    return JobFactory()

@pytest.fixture
def naive_job(job_factory) -> Job:
    """
    Fixture to create a sample Job instance using the factory.
    """
    test_job = JobTestPerformance(
        name="test_job_001",
        status="COMPLETED",
        section="SIM", 
        chunk_size="12",
        chunk_size_unit="month"
    )
    
    test_job.set_start_timestamp("2025-07-17 14:30:25")
    test_job.set_finish_timestamp("2025-07-17 15:30:25")
    
    return job_factory.create_job(test_job)


# -- fixtures for MailNotifier and Platform

@pytest.fixture
def mock_basic_config(mocker):
    mock_config = mocker.Mock()
    mock_config.MAIL_FROM = "test@example.com"
    mock_config.SMTP_SERVER = "smtp.example.com"
    mock_config.expid_aslog_dir.side_effect = lambda exp_id: BasicConfig.expid_aslog_dir(exp_id)
    return mock_config


@pytest.fixture
def mock_smtp(mocker):
    return mocker.patch('autosubmit.notifications.mail_notifier.smtplib.SMTP', autospec=True) 
    # This line mocks the SMTP class in the mail_notifier module

@pytest.fixture
def mock_mail_notifier(mock_basic_config):
    return MailNotifier(mock_basic_config)

# -- fixtures for Performance class and Utils

@pytest.fixture 
def sim_performance() -> SIMPerformance: 
    """
    Fixture to provide an instance of the Performance class.
    """
    return SIMPerformance() 

@pytest.fixture
def sim_performance_with_mail_notifier(mock_mail_notifier) -> SIMPerformance:
    """
    Fixture to provide the Performance class with a MailNotifier instance.
    """
    performance_instance = SIMPerformance()
    performance_instance.set_mail_notifier(mock_mail_notifier)
    return performance_instance

@pytest.fixture
def utils() -> Utils:
    """
    Fixture to provide the Utils class for calculations.
    """
    return Utils()