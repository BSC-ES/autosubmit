from autosubmit.notifications.mail_notifier import MailNotifier
from autosubmit.job.job_common import Status

from typing import cast

FROM_EMAIL = 'notifier@localhost'
TO_EMAIL = ['bruno.depaulakinoshita@bsc.es']
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


platform = cast("Platform", type('', (), {'host': 'localhost', 'name': 'fake-local'}))

MAIL_NOTIFIER.notify_experiment_status(
    EXPID,
    TO_EMAIL,
    platform
)
