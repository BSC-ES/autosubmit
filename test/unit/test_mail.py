import pytest
import tempfile
from unittest import mock
from pathlib import Path
from autosubmit.notifications.mail_notifier import MailNotifier
from autosubmitconfigparser.config.basicconfig import BasicConfig

@pytest.fixture
def mock_basic_config():
    mock_config = mock.Mock()
    mock_config.MAIL_FROM = "test@example.com"
    mock_config.SMTP_SERVER = "smtp.example.com"
    return mock_config

@pytest.fixture
def mail_notifier(mock_basic_config):
    return MailNotifier(mock_basic_config)

def test_send_mail(mail_notifier):
    with mock.patch('smtplib.SMTP') as mock_smtp:
        mock_server = mock.Mock()
        mock_smtp.return_value = mock_server
        mock_server.sendmail = mock.Mock()
        
        mail_notifier._send_mail('sender@example.com', 'recipient@example.com', mock.Mock())

        mock_server.sendmail.assert_called_once_with('sender@example.com', 'recipient@example.com', mock.ANY)

def test_attach_files(mail_notifier):
    with tempfile.TemporaryDirectory() as temp_dir:
        file1 = Path(temp_dir) / "file1_run.log"
        file2 = Path(temp_dir) / "file2_run.log"
        file1.write_text("file data 1")
        file2.write_text("file data 2")
        
        assert file1.exists()
        assert file2.exists()

        mock_files = [file1, file2]
        
        with mock.patch("builtins.open", mock.mock_open(read_data="file data")):
            message = mock.Mock()
            mail_notifier._attach_files(message, mock_files)
            
            assert message.attach.call_count == len(mock_files)

def test_notify_experiment_status(mail_notifier):
    original_local_root_dir = BasicConfig.LOCAL_ROOT_DIR
    with tempfile.TemporaryDirectory() as temp_dir:
        BasicConfig.LOCAL_ROOT_DIR = temp_dir 
        exp_id = '1234'
        aslogs_dir = Path(temp_dir) / exp_id / "tmp" / "ASLOGS"
        aslogs_dir.mkdir(parents=True, exist_ok=True)

        file1 = aslogs_dir / "1234_run.log"
        file2 = aslogs_dir / "1235_run.log"

        file1.write_text("file data 1")
        file2.write_text("file data 2")

        assert file1.exists()
        assert file2.exists()

        with mock.patch.object(mail_notifier, '_generate_message_experiment_status', return_value="Test message"):

            with mock.patch.object(mail_notifier, '_send_mail') as mock_send_mail, \
                 mock.patch.object(mail_notifier, '_attach_files') as mock_attach_files:

                mail_to = ['recipient@example.com']
                platform = mock.Mock()
                platform.name = "Test Platform"
                platform.host = "test.host.com"

                mail_notifier.notify_experiment_status(exp_id, mail_to, platform)

                mail_notifier._generate_message_experiment_status.assert_called_once_with(exp_id, platform)

                mock_send_mail.assert_called_once_with(mail_notifier.config.MAIL_FROM, 'recipient@example.com', mock.ANY)

                mock_attach_files.assert_called_once_with(mock.ANY, [file1, file2])
    
    # Reset the local root dir.
    BasicConfig.LOCAL_ROOT_DIR = original_local_root_dir
