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

import email.utils
import socket
from email.mime.text import MIMEText
from pathlib import Path
from smtplib import SMTPException
from tempfile import TemporaryDirectory

import pytest

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.job.job_common import Status
from autosubmit.log.log import Log
from autosubmit.notifications.mail_notifier import MailNotifier, _check_mail_address

# -- fixtures


@pytest.fixture
def mock_basic_config(monkeypatch):
    monkeypatch.setattr(BasicConfig, 'MAIL_FROM', 'test@example.com')
    monkeypatch.setattr(BasicConfig, "SMTP_SERVER", "smtp.example.com")
    monkeypatch.setattr(BasicConfig, 'expid_aslog_dir',lambda exp_id: BasicConfig.expid_aslog_dir(
        exp_id))


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


def _normalize_mail_text(text: str) -> str:
    """Normalize e-mail text for stable equality assertions."""
    return text.replace("\r\n", "\n").strip()


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
        (0, SMTPException("SMTP server error"), None, None),

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
        number_of_files: int,
        sendmail_error: Exception | None,
        compress_error: Exception | None,
        attach_error: Exception | None
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

        MailNotifier().notify_experiment_status('a000', ['recipient@example.com'], mock_platform)

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

            # The SMTP connection is used as a context manager, so sendmail
            # is called on the object returned by ``__enter__``. The first
            # arguments are the sender and recipient; the third (or [2]) is
            # the MIME message.
            sendmail_call = next(c for c in mock_smtp.mock_calls if c[0].endswith('sendmail'))
            message_arg = sendmail_call.args[2]

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
        (SMTPException("SMTP server error"),
         'Trace: SMTP server error\nAn error has occurred while sending a notification mail')
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
        sendmail_error: Exception | None,
        expected_log_message):
    exp_id = 'a123'
    job_name = 'Job1'
    prev_status = Status.VALUE_TO_KEY[Status.RUNNING]
    status = Status.VALUE_TO_KEY[Status.COMPLETED]
    mail_to = ['recipient@example.com']

    mock_smtp = mocker.patch('autosubmit.notifications.mail_notifier.smtplib.SMTP')
    if sendmail_error:
        mock_smtp.side_effect = sendmail_error
    mock_printlog = mocker.patch.object(Log, 'printlog')

    MailNotifier().notify_status_change(exp_id, job_name, prev_status, status, mail_to)

    message_text = "Generated message"
    message = MIMEText(message_text)
    message['From'] = email.utils.formataddr(('Autosubmit', BasicConfig.MAIL_FROM))
    message['Subject'] = f'[Autosubmit] The job {job_name} status has changed to {str(status)}'
    message['Date'] = email.utils.formatdate(localtime=True)

    if expected_log_message:
        mock_printlog.assert_called_once_with(expected_log_message, 6011)
        log_calls = [call[0][0] for call in mock_printlog.call_args_list]
        assert 'Traceback' not in log_calls
    else:
        mock_printlog.assert_not_called()


@pytest.mark.parametrize(
    "sendmail_error,expected_log_message",
    [
        (SMTPException("SMTP server error"),
         'Trace: SMTP server error\nAn error has occurred while sending a notification mail')
    ],
    ids=[
        "Log connection error (SMTP server error)"
    ]
)
def test_notify_cpmip_threshold_violations_errors(
        mock_basic_config, mocker, sendmail_error: Exception | None, expected_log_message):
    """Test the possible errors in ``notify_cpmip_threshold_violations``.

    The success path is tested in integration tests.
    """
    exp_id = 'a123'
    job_name = 'Job1'
    mail_to = ['recipient@example.com']
    violations = {
        "SYPD": {
            "threshold": 5.0,
            "accepted_error": 10,
            "comparison": "greater_than",
            "bound": 4.5,
            "real_value": 3.9,
        }
    }

    mock_smtp = mocker.patch('autosubmit.notifications.mail_notifier.smtplib.SMTP')
    if sendmail_error:
        mock_smtp.side_effect = sendmail_error
    mock_printlog = mocker.patch.object(Log, 'printlog')

    MailNotifier().notify_cpmip_threshold_violations(exp_id, job_name, violations, mail_to)

    mock_printlog.assert_called_once_with(expected_log_message, 6011)
    log_calls = [call[0][0] for call in mock_printlog.call_args_list]
    assert 'Traceback' not in log_calls


@pytest.mark.parametrize(
    "mail_to, expected",
    [
        ("a@bsc.es", ["a@bsc.es"]),
        ("a@bsc.es, b@bsc.es", ["a@bsc.es", "b@bsc.es"]),
        ("a@bsc.es b@bsc.es;c@bsc.es", ["a@bsc.es", "b@bsc.es", "c@bsc.es"]),
        (["a@bsc.es", " b@bsc.es"], ["a@bsc.es", "b@bsc.es"]),
        (("a@bsc.es",), ["a@bsc.es"]),
        (["a@bsc.es, b@bsc.es"], ["a@bsc.es", "b@bsc.es"]),
    ],
    ids=[
        "Single string",
        "Comma-separated string",
        "Mixed separators",
        "List with whitespace",
        "Tuple",
        "List item with commas",
    ]
)
def test_check_mail_address_valid(mail_to, expected):
    assert _check_mail_address(mail_to) == expected


@pytest.mark.parametrize(
    "mail_to, error_message",
    [
        (None, "must be an email or a list of emails"),
        ({"to": "a@bsc.es"}, "must be an email or a list of emails"),
        ("", "Empty recipient list"),
        ([], "Empty recipient list"),
        ("a@bsc.es, b@", "Invalid email in recipient list: b@"),
    ],
    ids=["None", "Dict", "Empty string", "Empty list", "Invalid address"]
)
def test_check_mail_address_invalid(mail_to, error_message):
    with pytest.raises(ValueError, match=error_message):
        _check_mail_address(mail_to)


@pytest.mark.parametrize("mail_to", ["", None, "not-an-email"])
def test_notify_status_change_invalid_recipients_does_not_raise(mock_basic_config, mocker, mail_to):
    mock_send_mail = mocker.patch.object(MailNotifier, '_send_mail')
    mock_printlog = mocker.patch.object(Log, 'printlog')

    MailNotifier().notify_status_change('a123', 'Job1', 'RUNNING', 'FAILED', mail_to)

    mock_send_mail.assert_not_called()
    mock_printlog.assert_called_once()
    assert 'Check MAIL.TO' in mock_printlog.call_args[0][0]


@pytest.mark.parametrize(
    "error",
    [ConnectionRefusedError("refused"), TimeoutError("timed out"), socket.gaierror("unknown host")],
    ids=["Connection refused", "Timeout", "Unknown host"]
)
def test_notify_status_change_connection_error_does_not_raise(mock_basic_config, mocker, error):
    mocker.patch('autosubmit.notifications.mail_notifier.smtplib.SMTP', side_effect=error)
    mock_printlog = mocker.patch.object(Log, 'printlog')

    MailNotifier().notify_status_change('a123', 'Job1', 'RUNNING', 'FAILED', ['a@bsc.es'])

    mock_printlog.assert_called_once()
    assert 'error has occurred while sending a notification mail' in mock_printlog.call_args[0][0]


def test_notify_status_change_accepts_string_recipients(mock_basic_config, mocker):
    mock_send_mail = mocker.patch.object(MailNotifier, '_send_mail')

    MailNotifier().notify_status_change('a123', 'Job1', 'RUNNING', 'FAILED', 'a@bsc.es, b@bsc.es')

    mock_send_mail.assert_called_once()
    recipients = mock_send_mail.call_args[0][1]
    assert recipients == [email.utils.formataddr((m, m)) for m in ['a@bsc.es', 'b@bsc.es']]

