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

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional, NamedTuple
from autosubmit.notifications.mail_notifier import MailNotifier
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmitconfigparser.config.configcommon import AutosubmitConfig

if TYPE_CHECKING:
    from autosubmit.job.job import Job

class PerformanceMetricInfo(NamedTuple):
    """
    Class to hold information about a performance metric.

    Attributes:
        metric (str): The name of the metric.
        under_threshold (bool): Whether the metric is under the threshold.
        value (float): The current value of the metric.
        threshold (float): The threshold value for the metric.
        under_performance (Optional[float]): The percentage of underperformance, if applicable.
    """
    metric: str
    under_threshold: bool
    value: float
    threshold: float
    under_performance: Optional[float] = None
       

class BasePerformance(ABC):
    """Base class for performance metrics calculation"""

    _mail_notifier = MailNotifier(BasicConfig()) # Default MailNotifier with BasicConfig 

    def __init__(self, autosubmit_config: Optional[AutosubmitConfig] = None):
        """
        Initialize the BasePerformance class.

        :param autosubmit_config: Autosubmit configuration containing performance settings.
        :type autosubmit_config: Optional[AutosubmitConfig]
        """
        self._autosubmit_config = autosubmit_config

    @abstractmethod
    def compute_and_check_performance_metrics(self, job: 'Job') -> list[PerformanceMetricInfo]:
        """
        Compute performance metrics for a job.

        :param job: Job instance containing the necessary attributes.
        :type job: Job

        :return: A list of PerformanceMetricInfo instances containing metric details.
        :rtype: list[PerformanceMetricInfo]
        """
        pass

    # Build mail message for the metrics

    def _template_metric_message(self, metric_info: PerformanceMetricInfo, job: 'Job') -> str:
        """
        Generate a message template for the performance metric.

        :param metric_info: PerformanceMetricInfo instance containing metric details.
        :type metric_info: PerformanceMetricInfo

        :param job: Job instance containing the necessary attributes.
        :type job: Job

        :return: A formatted message string.
        :rtype: str
        """
        return f"""
        🧪 Experiment ID: {job.expid}
        🎯 Job Name: {job.name}

        Metric: {metric_info.metric}
        📉 Current Value: {metric_info.value:.4f}
        🎚️ Expected Threshold: {metric_info.threshold}

        🔍 Performance is {metric_info.under_performance:.1f}% below expected threshold.
        """
    
    # Mail notifier setter 

    def set_mail_notifier(self, mail_notifier: MailNotifier):
        """
        Set the MailNotifier instance for the Performance class.

        :param mail_notifier: An instance of MailNotifier to handle email notifications.
        :type mail_notifier: MailNotifier
        """
        
        self._mail_notifier = mail_notifier

    # Autosubmit configuration setter 

    def set_autosubmit_config(self, autosubmit_config: AutosubmitConfig):
        """
        Set the Autosubmit configuration for the Performance class.

        :param autosubmit_config: An instance of AutosubmitConfig containing performance settings.
        :type autosubmit_config: AutosubmitConfig
        """
        self._autosubmit_config = autosubmit_config

    # Get Autosubmit configuration

    def _get_mail_recipients(self) -> list[str]:
        """
        Get the email recipients for performance notifications from the Autosubmit configuration.

        :return: A list of email addresses to notify.
        :rtype: list[str]
        """
        try:
            if not self._autosubmit_config:
                raise ValueError("Autosubmit configuration is not set.")
                
            performance_config = self._autosubmit_config.experiment_data.get('PERFORMANCE', {})     
                
            notify_to = performance_config.get('NOTIFY_TO', [])

            if not notify_to:
                raise ValueError("No email recipients configured for performance notifications.")
            
            return notify_to

        except Exception as e:
            raise ValueError(f"Error retrieving email recipients: {e}") 


    
