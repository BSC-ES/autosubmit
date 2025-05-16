import pytest
import requests
from typing import List
from testcontainers.core.container import DockerContainer
from autosubmit.notifications.mail_notifier import MailNotifier
from autosubmit.job.job_common import Status
from typing import cast

SMTP_PORT = 1025
API_PORT = 8025

@pytest.fixture(scope="module")
def fake_smtp_server():
    """Start fake SMTP server container"""
    with DockerContainer("mailhog/mailhog") \
        .with_exposed_ports(SMTP_PORT, API_PORT) as container:
        
        smtp_port = container.get_exposed_port(SMTP_PORT)
        api_port = container.get_exposed_port(API_PORT)
        smtp_host = container.get_container_host_ip()
        api_base = f"http://{smtp_host}:{api_port}"

        for _ in range(10):
            try:
                requests.get(f"{api_base}/api/v2/messages")
                break
            except requests.ConnectionError:
                import time
                time.sleep(1)

        yield {
            "smtp_host": smtp_host,
            "smtp_port": smtp_port,
            "api_base": api_base,
        }
        requests.delete(f"{api_base}/api/v2/messages")


@pytest.fixture
def mail_notifier(fake_smtp_server, tmp_path):
    smtp_host = fake_smtp_server["smtp_host"]
    smtp_port = fake_smtp_server["smtp_port"]
    
    def expid_aslog_dir(expid):
        exp_dir = tmp_path / "aslog" / expid
        exp_dir.mkdir(parents=True)
        (exp_dir / "dummy_run.log").write_text("Log entry: simulation started.")
        return exp_dir

    config = type('Config', (), {
        'MAIL_FROM': 'notifier@localhost',
        'SMTP_SERVER': f'{smtp_host}:{smtp_port}',
        'expid_aslog_dir': staticmethod(expid_aslog_dir),
    })()
    return MailNotifier(config)


def check_metadata(emails: List[str], expected_subject: str, expid: str, sender: str, recipients: List[str])-> None:
    subject = [email["Content"]["Headers"]["Subject"][0] for email in emails]   
    assert (expected_subject in s for s in subject)
    body = [email["Content"]["Body"] for email in emails]
    assert (expid in b for b in body)
    
    for email in emails:
        assert sender in email["Raw"]["From"]
        for recipient in recipients:
            assert recipient in email["Raw"]["To"]


def test_notify_status_change(mail_notifier, fake_smtp_server):
    api_base = fake_smtp_server["api_base"]
    expid = 'a000'
    job_name = 'SIM'
    to_email = ['test@example.com']
    requests.delete(f"{api_base}/api/v1/messages")

    mail_notifier.notify_status_change(
        expid, job_name,
        Status.VALUE_TO_KEY[Status.RUNNING], # previous status
        Status.VALUE_TO_KEY[Status.FAILED], # new status
        to_email
    )

    resp = requests.get(f"{api_base}/api/v2/messages")
    emails = resp.json()["items"]
    assert len(emails) == 1 
    check_metadata(emails, "status has changed to FAILED", expid, 'notifier@localhost', to_email)


def test_experiment_status(mail_notifier, fake_smtp_server):
    api_base = fake_smtp_server["api_base"]
    expid = 'a000'
    job_name = 'SIM'
    to_email = ['test@example.com']
    requests.delete(f"{api_base}/api/v1/messages")

    platform = cast(
        "Platform", type(
            '', (), {
                'host': 'localhost', 'name': 'fake-local'}))
    mail_notifier.notify_experiment_status(
        expid,
        to_email,
        platform
    )

    resp = requests.get(f"{api_base}/api/v2/messages")
    assert resp.json()["count"] == 1
    emails = resp.json()["items"]
    check_metadata(emails, "platform is malfunctioning", expid, 'notifier@localhost', to_email)
    
    bodies = [
            email["Content"]["Body"]
            for email in emails
            ]
    assert ('Name="dummy_run.log.zip"' in b for b in bodies)
    # TODO: test content of compressed file?

# empty list, invalid mail (regex?), multiple recipients
@pytest.mark.parametrize(
    "list_recipients, expected",
    [([], False), (['test'], False), (['test@mail.com', 'test2@mail.com'], True)]
)
def test_recipients_list(mail_notifier, fake_smtp_server, list_recipients, expected):
    api_base = fake_smtp_server["api_base"]
    expid = 'a000'
    job_name = 'SIM'
    requests.delete(f"{api_base}/api/v2/messages")

    mail_notifier.notify_status_change(
        expid, job_name,
        Status.VALUE_TO_KEY[Status.RUNNING], # previous status
        Status.VALUE_TO_KEY[Status.FAILED], # new status
        list_recipients 
    )

