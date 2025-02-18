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

from autosubmit.notifications.mail_notifier import MailNotifier
from autosubmit.job.job_common import Status
from autosubmit.platforms.platform import Platform

from typing import cast

FROM_EMAIL = 'notifier@localhost'
TO_EMAIL = ['user@localhost']
SMTP_HOST = 'localhost'
SMTP_PORT = 1025
EXPID = 'a000'
JOB_NAME = 'SIM'

config = type('', (), {
    'MAIL_FROM': FROM_EMAIL,
    'SMTP_SERVER': f'{SMTP_HOST}:{SMTP_PORT}'
})()

MAIL_NOTIFIER = MailNotifier(config)

# Status change

MAIL_NOTIFIER.notify_status_change(
    EXPID, JOB_NAME,
    Status.VALUE_TO_KEY[Status.RUNNING],
    Status.VALUE_TO_KEY[Status.FAILED],
    TO_EMAIL
)

# Exp status


platform = cast(Platform, type('', (), {'host': 'localhost', 'name': 'fake-local'}))

MAIL_NOTIFIER.notify_experiment_status(
    EXPID,
    TO_EMAIL,
    platform
)
