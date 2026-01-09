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

from typing import Generator, Tuple
from unittest.mock import Mock

import pytest
import requests
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from autosubmit.job.job_common import Status
from autosubmit.notifications.mail_notifier import MailNotifier
from autosubmit.platforms.platform import Platform
from test.integration.test_utils.networking import get_free_port


@pytest.fixture(scope="module")
def fake_smtp_server() -> Generator[Tuple[int, str], None, None]:
    """Start fake SMTP server container.
    :return: A tuple with the SMTP port, and the SMTP test server API base URL """
    smtp_port = get_free_port()
    api_port = get_free_port()
    with DockerContainer(image="mailhog/mailhog", remove=True) \
            .with_bind_ports(1025, smtp_port) \
            .with_bind_ports(8025, api_port) as container:
        wait_for_logs(container, 'Serving under')
        api_base = f"http://127.0.0.1:{api_port}"
        yield smtp_port, api_base
        requests.delete(f"{api_base}/api/v2/messages")


@pytest.fixture
def mail_notifier(fake_smtp_server, tmp_path):
    smtp_port, _ = fake_smtp_server

    def expid_aslog_dir(expid):
        exp_dir = tmp_path / "aslog" / expid
        exp_dir.mkdir(parents=True)
        (exp_dir / "dummy_run.log").write_text("Log entry: simulation started.")
        return exp_dir

    config = type('Config', (), {
        'MAIL_FROM': 'notifier@localhost',
        'SMTP_SERVER': f'127.0.0.1:{smtp_port}',
        'expid_aslog_dir': staticmethod(expid_aslog_dir),
    })()
    return MailNotifier(config)


@pytest.fixture
def mock_platform() -> Platform:
    mock = Mock(spec=Platform)
    mock.host = 'localhost'
    mock.name = 'fake-local'
    return mock


def _check_metadata(
        emails: list[dict],
        expected_subject: str,
        expid: str,
        sender: str,
        recipients: list[str]) -> None:
    subject = [email["Content"]["Headers"]["Subject"][0] for email in emails]
    assert (expected_subject in s for s in subject)
    body = [email["Content"]["Body"] for email in emails]
    assert (expid in b for b in body)

    for email in emails:
        assert sender in email["Raw"]["From"]
        for recipient in recipients:
            assert recipient in email["Raw"]["To"]


@pytest.mark.docker
def test_notify_status_change(mail_notifier, fake_smtp_server):
    _, api_base = fake_smtp_server
    expid = 'a000'
    job_name = 'SIM'
    to_email = ['test@example.com']
    requests.delete(f"{api_base}/api/v1/messages")

    mail_notifier.notify_status_change(
        expid, job_name,
        Status.VALUE_TO_KEY[Status.RUNNING],  # previous status
        Status.VALUE_TO_KEY[Status.FAILED],  # new status
        to_email
    )

    resp = requests.get(f"{api_base}/api/v2/messages")
    assert resp.json()["count"] == 1
    emails = resp.json()["items"]
    _check_metadata(emails, "status has changed to FAILED",
                   expid, 'notifier@localhost', to_email)


@pytest.mark.docker
def test_experiment_status(mail_notifier, fake_smtp_server, mock_platform):
    _, api_base = fake_smtp_server
    expid = 'a000'
    to_email = ['test@example.com']
    requests.delete(f"{api_base}/api/v1/messages")

    platform = mock_platform
    mail_notifier.notify_experiment_status(
        expid,
        to_email,
        platform
    )

    resp = requests.get(f"{api_base}/api/v2/messages")
    assert resp.json()["count"] == 1
    emails = resp.json()["items"]
    _check_metadata(
        emails,
        "platform is malfunctioning",
        expid,
        'notifier@localhost',
        to_email)

    bodies = [
        email["Content"]["Body"]
        for email in emails
    ]
    assert ('Name="dummy_run.log.zip"' in b for b in bodies)
    # TODO: test content of compressed file?


@pytest.mark.parametrize(
    "list_recipients, expected_error_message",
    [("test", "Recipients of mail notifications must be a list of emails!"),
        ([], "Empty recipient list"),
        (['test'], "Invalid email in recipient list"),
        (['test@mail.com', 'test2@mail.com'], None)]
)
@pytest.mark.docker
def test_recipients_list(
        mail_notifier,
        fake_smtp_server,
        list_recipients,
        expected_error_message):
    _, api_base = fake_smtp_server
    expid = 'a000'
    job_name = 'SIM'
    requests.delete(f"{api_base}/api/v1/messages")

    if expected_error_message:
        with pytest.raises(ValueError, match=expected_error_message):
            mail_notifier.notify_status_change(
                expid, job_name,
                Status.VALUE_TO_KEY[Status.RUNNING],
                Status.VALUE_TO_KEY[Status.FAILED],
                list_recipients
            )
    else:
        mail_notifier.notify_status_change(
            expid, job_name,
            Status.VALUE_TO_KEY[Status.RUNNING],
            Status.VALUE_TO_KEY[Status.FAILED],
            list_recipients
        )
        resp = requests.get(f"{api_base}/api/v2/messages")
        assert len(resp.json()["items"][0]["Raw"]
                   ["To"]) == len(list_recipients)
