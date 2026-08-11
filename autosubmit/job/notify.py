# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
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

"""Job notification."""

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.job.job_common import Status
from autosubmit.notifications.mail_notifier import MailNotifier
from autosubmit.notifications.notifier import Notifier


def job_notify(as_conf, expid, job):
    if as_conf.get_notifications() == "true" and Status.VALUE_TO_KEY[job.status] in job.notify_on:
        Notifier.notify_status_change(
            MailNotifier(BasicConfig),
            expid,
            job.name,
            Status.VALUE_TO_KEY[job.prev_status],
            Status.VALUE_TO_KEY[job.status],
            as_conf.experiment_data["MAIL"]["TO"],
        )


# TODO: It would probably make sense to have an autosubmit.wrappers package.
def wrapper_notify(as_conf, expid, wrapper_job):
    if as_conf.get_notifications() == "true":
        for inner_job in wrapper_job.job_list:
            job_notify(as_conf, expid, inner_job)
