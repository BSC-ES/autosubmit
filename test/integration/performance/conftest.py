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

import copy
from dataclasses import dataclass
from autosubmit.platforms.slurmplatform import SlurmPlatform
import pytest 
from test.integration.test_mail import fake_smtp_server, mail_notifier as integration_mail_notifier
from autosubmit.performance.base_performance import BasePerformance
import requests

# Platforms Configuration

@dataclass
class PlatformConfig:
    """Configuration for a platform."""
    config: dict[str, any]
    platform_class: type

    def create_platform(self, expid: str, platform_name: str, as_conf):
        """
        Create a platform instance based on the configuration.
        
        :param expid: Experiment ID.
        :param platform_name: Name of the platform.
        :param as_conf: Autosubmit configuration object.

        :return: An instance of the platform class.
        """
        return self.platform_class(expid, platform_name, config=as_conf.experiment_data, auth_password=None)

PLATFORMS = {
    'slurm': PlatformConfig(
        config={
            'TYPE': 'slurm',
            'HOST': 'localDocker',
            'USER': 'root',
            'PROJECT': 'group',
            'QUEUE': 'gp_debug',
            'SCRATCH_DIR': '/tmp/scratch',
            'TEMP_DIR': '',
            'MAX_WALLCLOCK': '00:15',
            'ADD_PROJECT_TO_HOST': False, 
        },
        platform_class= SlurmPlatform
    ),
}

@pytest.fixture(params=list(PLATFORMS.keys()))
def platform_type(request):
    """Fixture that parametrizes over available platforms."""
    return request.param

@pytest.fixture
def platform_config(platform_type: str) -> PlatformConfig:
    """
    Fixture to get the configuration for a specific platform type.
    
    :param platform_type: The type of platform to configure.
    :return: A PlatformConfig instance for the specified platform type.
    """
    return PLATFORMS[platform_type]

@pytest.fixture
def platform_name(platform_type: str) -> str:
    """
    Fixture to get the name of the platform.
    
    :param platform_type: The type of platform.
    :return: The name of the platform.
    """
    return f"TEST_{platform_type.upper()}"

_EXPID = "t000"

@pytest.fixture
def expid() -> str:
    """
    Fixture that provides the experiment ID for testing.
    
    :return: The experiment ID to use in tests.
    """
    return _EXPID

@pytest.fixture
def experiment_data_factory(platform_name: str, platform_config: PlatformConfig):
    """
    Factory to create experiment data for testing.
    
    :param platform_name: The name of the platform.
    :param platform_config: The configuration for the platform.
    :return: A function that creates experiment data.
    """
    def _create_experiment_data(experiment_data: dict) -> dict:
        """
        Create experiment data with the specified configuration.
        
        :param experiment_data: The base experiment data.
        :return: A dictionary containing the complete experiment data.
        """
        
        result = copy.deepcopy(experiment_data)
    
        for job_name, job_config in result['JOBS'].items():  
            job_config['PLATFORM'] = platform_name

        result['PLATFORMS'] = {}  

        result['PLATFORMS'][platform_name] = platform_config.config.copy()

        return result
    
    return _create_experiment_data

@pytest.fixture
def create_platform(platform_config: PlatformConfig):
    """
    Fixture to create a platform instance.
    
    :param platform_config: The configuration for the platform.
    :return: A function that creates a platform instance.
    """
    
    def _create_platform(expid: str, platform_name: str, as_conf):
        """
        Create a platform instance based on the experiment ID and platform name.
        
        :param expid: Experiment ID.
        :param platform_name: Name of the platform.
        :param as_conf: If True, return the configuration instead of the instance.
        
        :return: An instance of the platform class or its configuration.
        """
        return platform_config.create_platform(expid, platform_name, as_conf)
    
    return _create_platform

# Mail Configuration 

@pytest.fixture
def performance_mail_notifier(integration_mail_notifier):
    """
    Fixture to provide the performance mail notifier.

    :param integration_mail_notifier: The MailNotifier instance that use Docker from test_mail.
    :return: The performance mail notifier instance.
    """
    return integration_mail_notifier

@pytest.fixture(autouse=True)
def mock_base_performance_notifier(performance_mail_notifier, mocker):
    """
    Mock the BasePerformance mail notifier to use the performance mail notifier.

    :param performance_mail_notifier: The MailNotifier instance with Docker.
    :param mocker: The pytest-mock fixture for mocking.
    """
    mocker.patch('autosubmit.performance.base_performance.MailNotifier', return_value=performance_mail_notifier)

@pytest.fixture(autouse=True)
def clear_mail_api(fake_smtp_server):
    """
    Clear the mail API before each test.

    :param fake_smtp_server: The fixture that provides the fake SMTP server.
    """
    _, api_base = fake_smtp_server
    requests.delete(f"{api_base}/api/v1/messages")

@pytest.fixture
def check_mail_sent(fake_smtp_server):
    """
    Fixture that provides a function to check if exactly one mail was sent.
    
    :param fake_smtp_server: The fixture that provides the fake SMTP server.
    :return: A function that returns True if exactly one email was sent, False otherwise.
    """
    _, api_base = fake_smtp_server
    
    def _check_mail_sent() -> bool:
        """
        Check if exactly one email was sent.
        
        :return: True if exactly one email was sent, False otherwise.
        """
        resp = requests.get(f"{api_base}/api/v2/messages")
        emails_data = resp.json()
        
        return emails_data["count"] == 1
    
    return _check_mail_sent

