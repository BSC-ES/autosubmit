import email.utils
from email.mime.text import MIMEText
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional

import pytest

from autosubmit.job.job_common import Status
from autosubmit.notifications.mail_notifier import MailNotifier
from autosubmitconfigparser.config.basicconfig import BasicConfig
from log.log import Log


# -- fixtures


@pytest.fixture
def mock_basic_config(mocker):
    mock_config = mocker.Mock()
    mock_config.MAIL_FROM = "test@example.com"
    mock_config.SMTP_SERVER = "smtp.example.com"
    mock_config.expid_aslog_dir.side_effect = lambda exp_id: BasicConfig.expid_aslog_dir(
        exp_id)
    return mock_config


@pytest.fixture
def mock_smtp(mocker):
    return mocker.patch(
        'autosubmit.notifications.mail_notifier.smtplib.SMTP',
        autospec=True
    )


@pytest.fixture
def mock_platform(mocker):
    mock_platform = mocker.Mock()
    mock_platform.name = "Test Platform"
    mock_platform.host = "test.host.com"
    return mock_platform


@pytest.fixture
def mail_notifier(mock_basic_config):
    return MailNotifier(mock_basic_config)

# --- tests


@pytest.mark.parametrize(
    "number_of_files, sendmail_error, compress_error, attach_error",
    [
        # No errors, no log files compressed.
        (0, None, None, None),

        # No errors, one log file compressed.
        (1, None, None, None),

        # No errors, three log files, one file compressed.
        (3, None, None, None),

        # STMP error.
        (0, Exception("SMTP server error"), None, None),

        # ZIP error.
        (1, None, ValueError('Zip error'), None),

        # Attach error.
        (1, None, None, ValueError('Attach error'))
    ],
    ids=[
        "No files. No errors",
        "One file. Attach a single file. No errors",
        "Three files. Attach a single file. No errors",
        "SMTP server error",
        "Zip error",
        "Attach error"
    ]
)
def test_compress_file(
        mock_basic_config,
        mock_platform,
        mock_smtp,
        mocker,
        mail_notifier,
        number_of_files: int,
        sendmail_error: Optional[Exception],
        compress_error: Optional[Exception],
        attach_error: Optional[Exception]
):
    with TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        if sendmail_error:
            mock_smtp.side_effect = sendmail_error

        if compress_error:
            mock_compress = mocker.patch(
                'autosubmit.notifications.mail_notifier.zipfile.ZipFile')
            mock_compress.side_effect = compress_error

        if attach_error:
            mock_message = mocker.patch(
                'autosubmit.notifications.mail_notifier.MIMEApplication')
            mock_message.side_effect = attach_error

        mock_printlog = mocker.patch.object(Log, 'printlog')

        for _ in range(number_of_files):
            test_file = temp_path / "test_file_run.log"
            with open(test_file, 'w') as f:
                f.write("file data 1")
                f.flush()

        mocker.patch.object(
            BasicConfig,
            'expid_aslog_dir',
            return_value=Path(temp_dir))

        mail_notifier.notify_experiment_status(
            'a000', ['recipient@example.com'], mock_platform)  # type: ignore

        if sendmail_error:
            mock_printlog.assert_called_once()
            log_calls = [call[0][0] for call in mock_printlog.call_args_list]
            assert 'Traceback' not in log_calls
        elif compress_error:
            mock_printlog.assert_called_once()
            exception_raised = mock_printlog.call_args_list[0][1]
            assert 'error has occurred while compressing' in exception_raised['message']
            assert 6011 == exception_raised['code']
        elif attach_error:
            mock_printlog.assert_called_once()
            exception_raised = mock_printlog.call_args_list[0][1]
            assert 'error has occurred while attaching' in exception_raised['message']
            assert 6011 == exception_raised['code']
        else:
            mock_printlog.assert_not_called()

            # First we call sendmail, then we call quit. Thus, the [0].
            # The first arguments are he sender and recipient. Third
            # (or [2]) is the MIME message.
            message_arg = mock_smtp.method_calls[0].args[2]

            if number_of_files > 0:
                assert '.zip' in message_arg
            else:
                assert '.zip' not in message_arg


@pytest.mark.parametrize(
    "sendmail_error, expected_log_message",
    [
        # Normal case: No errors, should not log anything
        # No logs are expected, everything works fine
        (None, None),

        # Log connection error: Simulate an error while sending email
        (Exception("SMTP server error"),
         'Trace:SMTP server error\nAn error has occurred while sending a mail for the job Job1')
    ],
    ids=[
        "Normal case: No errors",
        "Log connection error (SMTP server error)"
    ]
)
def test_notify_status_change(
        mock_basic_config,
        mock_smtp,
        mocker,
        mail_notifier,
        sendmail_error: Optional[Exception],
        expected_log_message):
    exp_id = 'a123'
    job_name = 'Job1'
    prev_status = Status.VALUE_TO_KEY[Status.RUNNING]
    status = Status.VALUE_TO_KEY[Status.COMPLETED]
    mail_to = ['recipient@example.com']

    mock_smtp = mocker.patch(
        'autosubmit.notifications.mail_notifier.smtplib.SMTP')
    if sendmail_error:
        mock_smtp.side_effect = sendmail_error
    mock_printlog = mocker.patch.object(Log, 'printlog')

    mail_notifier.notify_status_change(
        exp_id, job_name, prev_status, status, mail_to)

    message_text = "Generated message"
    message = MIMEText(message_text)
    message['From'] = email.utils.formataddr(
        ('Autosubmit', mail_notifier.config.MAIL_FROM))
    message['Subject'] = f'[Autosubmit] The job {job_name} status has changed to {str(status)}'
    message['Date'] = email.utils.formatdate(localtime=True)

    if expected_log_message:
        mock_printlog.assert_called_once_with(
            expected_log_message, 6011)
        log_calls = [call[0][0]
                     for call in mock_printlog.call_args_list]
        assert 'Traceback' not in log_calls
    else:
        mock_printlog.assert_not_called()
