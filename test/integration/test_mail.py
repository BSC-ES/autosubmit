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

from typing import Any, Callable, Generator, Optional, Union, TYPE_CHECKING

import pytest
import requests

from autosubmit.job.job_common import Status
from autosubmit.notifications.mail_notifier import MailNotifier
from test.integration.test_utils.docker import get_mail_container, prepare_and_test_mail_container

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from integration.conftest import AutosubmitExperiment
    from integration.conftest import AutosubmitExperimentFixture
    from pathlib import Path
    from requests import Response


def _get_messages(api_base: str) -> 'Response':
    return requests.get(f"{api_base}/api/v2/messages")


def _find_email_by_subject(search_text: str, emails) -> Any:
    return next(
        (
            e for e in emails
            if any(
                search_text in subject
                for subject in e["Content"]["Headers"].get("Subject", [])
            )
        ),
        None,
    )


@pytest.fixture
def fake_smtp_server() -> Generator[tuple[int, str], None, None]:
    """Start a fake SMTP server container.
    :return: A tuple with the SMTP port, and the SMTP test server API base URL """
    container, smtp_port, api_base = get_mail_container()
    with container:
        prepare_and_test_mail_container(container)

        yield smtp_port, api_base
        # requests.delete(f"{api_base}/api/v2/messages")


@pytest.fixture
def create_mail_notifier() -> Callable[['AutosubmitExperiment', int], MailNotifier]:
    """Factory fixture to create a MailNotifier instance."""

    def _create_mail_notifier(autosubmit_experiment: 'AutosubmitExperiment', smtp_port: int):
        exp_path: 'Path' = autosubmit_experiment.exp_path
        with (exp_path / 'dummy_run.log') as f:
            f.write_text("Log entry: simulation started.")

        basic_config = autosubmit_experiment.as_conf.basic_config
        basic_config.MAIL_FROM = 'notifier@localhost'
        basic_config.SMTP_SERVER = f'127.0.0.1:{smtp_port}'

        return MailNotifier(basic_config)

    return _create_mail_notifier


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
def test_notify_status_change(
        autosubmit_exp: 'AutosubmitExperimentFixture',
        create_mail_notifier: Callable[['AutosubmitExperiment', int], MailNotifier],
        fake_smtp_server: tuple[int, str]):
    smtp_port, api_base = fake_smtp_server
    job_name = 'SIM'
    to_email = ['test@example.com']

    exp = autosubmit_exp()

    mail_notifier = create_mail_notifier(exp, smtp_port)
    mail_notifier.notify_status_change(
        exp.expid, job_name,
        Status.VALUE_TO_KEY[Status.RUNNING],  # previous status
        Status.VALUE_TO_KEY[Status.FAILED],   # new status
        to_email
    )

    resp = _get_messages(api_base)
    assert resp.json()["count"] == 1
    emails = resp.json()["items"]
    _check_metadata(emails, "status has changed to FAILED", exp.expid, 'notifier@localhost', to_email)


@pytest.mark.docker
def test_experiment_status(
        autosubmit_exp: 'AutosubmitExperimentFixture',
        create_mail_notifier: Callable[['AutosubmitExperiment', int], MailNotifier],
        fake_smtp_server: tuple[int, str]):
    exp = autosubmit_exp()

    smtp_port, api_base = fake_smtp_server
    mail_notifier = create_mail_notifier(exp, smtp_port)
    to_email = ['test@example.com']

    mail_notifier.notify_experiment_status(exp.expid, to_email, exp.platform)

    resp = _get_messages(api_base)
    assert resp.json()["count"] == 1
    emails = resp.json()["items"]
    _check_metadata(emails, "platform is malfunctioning", exp.expid, 'notifier@localhost', to_email)

    bodies = [email["Content"]["Body"] for email in emails]
    assert ('Name="dummy_run.log.zip"' in b for b in bodies)
    # TODO: test content of compressed file?


@pytest.mark.docker
def test_notify_cpmip_threshold_violations(
        autosubmit_exp: 'AutosubmitExperimentFixture',
        create_mail_notifier: Callable[['AutosubmitExperiment', int], MailNotifier],
        fake_smtp_server: tuple[int, str]):
    exp = autosubmit_exp()

    smtp_port, api_base = fake_smtp_server
    mail_notifier = create_mail_notifier(exp, smtp_port)

    job_name = 'SIM'
    to_email = ['test@example.com']
    violations = {
        "SYPD": {
            "threshold": 5.0,
            "accepted_error": 10,
            "comparison": "greater_than",
            "bound": 4.5,
            "real_value": 3.9,
        },
        "LATENCY": {
            "threshold": 10.0,
            "accepted_error": 5,
            "comparison": "less_than",
            "bound": 10.5,
            "real_value": 10.6,
        },
    }

    mail_notifier.notify_cpmip_threshold_violations(exp.expid, job_name, violations, to_email)

    resp = _get_messages(api_base)
    emails: Any = resp.json()["items"]
    _check_metadata(emails, "CPMIP Threshold Violation detected", exp.expid, 'notifier@localhost', to_email)

    cpmip_email = _find_email_by_subject("CPMIP Threshold Violation", emails)
    assert cpmip_email is not None, "No CPMIP Threshold Violation email found in mailbox"

    body = cpmip_email["Content"]["Body"].replace("\r\n", "\n").strip()

    # Notification body
    assert f"Experiment id:  {exp.expid}" in body
    assert f"Job name: {job_name}" in body
    assert "The following CPMIP metrics violated their configured thresholds:" in body

    # Latency metric
    assert "Metric: LATENCY" in body
    assert "Comparison: must be <= effective bound (less_than)" in body
    assert "Configured threshold: 10.0" in body
    assert "Accepted error (%): 5" in body
    assert "Effective bound: 10.5" in body
    assert "Observed value: 10.6" in body

    # Latency SYPD
    assert "Metric: SYPD" in body
    assert "Comparison: must be >= effective bound (greater_than)" in body
    assert "Configured threshold: 5.0" in body
    assert "Accepted error (%): 10" in body
    assert "Effective bound: 4.5" in body
    assert "Observed value: 3.9" in body


@pytest.mark.parametrize(
    "list_recipients, expected_error_message",
    [("test", "Recipients of mail notifications must be a list of emails!"),
     ([], "Empty recipient list"),
     (['test'], "Invalid email in recipient list"),
     (['test@mail.com', 'test2@mail.com'], None)]
)
@pytest.mark.docker
def test_recipients_list(
        autosubmit_exp: 'AutosubmitExperimentFixture',
        create_mail_notifier: Callable[['AutosubmitExperiment', int], MailNotifier],
        fake_smtp_server: tuple[int, str],
        list_recipients: Union[str, list[str]],
        expected_error_message: Optional[str]):
    smtp_port, api_base = fake_smtp_server
    job_name = 'SIM'

    exp = autosubmit_exp()
    mail_notifier = create_mail_notifier(exp, smtp_port)

    if expected_error_message:
        with pytest.raises(ValueError, match=expected_error_message):
            mail_notifier.notify_status_change(exp.expid, job_name, Status.VALUE_TO_KEY[Status.RUNNING],
                Status.VALUE_TO_KEY[Status.FAILED], list_recipients  # type: ignore
            )
    else:
        mail_notifier.notify_status_change(
            exp.expid, job_name, Status.VALUE_TO_KEY[Status.RUNNING], Status.VALUE_TO_KEY[Status.FAILED],
            list_recipients  # type: ignore
        )
        response = _get_messages(api_base)
        assert len(response.json()["items"][0]["Raw"]["To"]) == len(list_recipients)
