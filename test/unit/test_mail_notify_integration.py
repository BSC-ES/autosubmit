"""Integration tests for MailNotifier.notify_status_change.

Very small tests that exercise the notify_status_change path end-to-end
while ensuring no real SMTP network calls are performed. Uses pytest
monkeypatching to replace the network send with a fake.
"""
from __future__ import annotations

from typing import Any
from email.mime.text import MIMEText
import email.utils

import pytest

from autosubmit.notifications.mail_notifier import MailNotifier
from autosubmit.log.log import AutosubmitError, AutosubmitCritical


class SimpleConfig:
    """Minimal config with required attributes for MailNotifier."""

    def __init__(self) -> None:
        self.MAIL_FROM = "autosubmit@example.com"
        self.SMTP_SERVER = "smtp.example.com"


def test_notify_status_change_calls_send_mail(monkeypatch: Any) -> None:
    """notify_status_change calls _send_mail with formatted recipients and message."""
    cfg = SimpleConfig()
    notifier = MailNotifier(cfg)

    recorded: dict[str, Any] = {}

    def fake_send_mail(mail_from: str, mail_to: list[str], message: MIMEText) -> None:
        recorded["mail_from"] = mail_from
        recorded["mail_to"] = mail_to
        recorded["message"] = message

    # replace the network operation
    monkeypatch.setattr(notifier, "_send_mail", fake_send_mail)

    recipients = ["alice@example.com", "bob@example.org"]
    notifier.notify_status_change("ABCD", "job1", "RUNNING", "DONE", recipients)

    assert recorded
    # mail_from forwarded
    assert recorded["mail_from"] == cfg.MAIL_FROM
    # recipients formatted list passed to _send_mail
    assert all("@" in addr for addr in recorded["mail_to"])  # basic sanity
    # message subject contains job name and status
    message = recorded["message"]
    assert "job1" in message["Subject"]
    assert "DONE" in message["Subject"]


def test_notify_status_change_failed_invokes_collect_but_does_not_send_attached(monkeypatch: Any) -> None:
    """When status is FAILED the code collects logfiles on a deepcopy of the message.

    This test ensures _collect_logfiles is invoked with a different message instance
    than the one actually delivered by _send_mail (no network attachments on the
    real outgoing message).
    """
    cfg = SimpleConfig()
    notifier = MailNotifier(cfg)

    called: dict[str, Any] = {"collect_id": None, "send_id": None, "collect_called": False, "send_called": False}

    def fake_collect(msg: Any, exp_id: str) -> None:
        # record identity of message used for collection
        called["collect_called"] = True
        called["collect_id"] = id(msg)

    def fake_send_mail(mail_from: str, mail_to: list[str], message: MIMEText) -> None:
        called["send_called"] = True
        called["send_id"] = id(message)

    # patch the methods on the instance
    monkeypatch.setattr(notifier, "_collect_logfiles", fake_collect)
    monkeypatch.setattr(notifier, "_send_mail", fake_send_mail)

    recipients = ["x@example.com"]
    notifier.notify_status_change("ABCD", "job-fail", "RUNNING", "FAILED", recipients)

    assert called["collect_called"] is True
    assert called["send_called"] is True
    # ensure the collected message instance is different from the one that was sent
    assert called["collect_id"] != called["send_id"]
