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

"""Unit tests for PsPlatform."""

import pytest

from autosubmit.platforms.psplatform import PsPlatform

# Simulated `ps -eo pid,cmd` output used across several tests.
_PS_OUTPUT = (
    "12301 bash /path/a000_INI.cmd\n"
    "12302 bash /path/a000_SIM.cmd\n"
    "12303 bash /path/a000_INI.cmd\n"
)


def test_get_submitted_jobs_by_name_returns_max_pid(
    ps_platform: PsPlatform,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return the highest matching PID for each submitted script.

    :param ps_platform: PsPlatform fixture under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    def _send_command(command: str, **_) -> bool:
        ps_platform._ssh_output = _PS_OUTPUT
        return True

    monkeypatch.setattr(ps_platform, "send_command", _send_command)

    result = ps_platform.get_submitted_jobs_by_name(["a000_INI.cmd", "a000_SIM.cmd"])

    assert result == ['12303', '12302']


def test_get_submitted_jobs_by_name_returns_empty_when_any_missing(
    ps_platform: PsPlatform,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return an empty list when any submitted script has no matching process.

    :param ps_platform: PsPlatform fixture under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    def _send_command(command: str, **_) -> bool:
        ps_platform._ssh_output = _PS_OUTPUT
        return True

    monkeypatch.setattr(ps_platform, "send_command", _send_command)

    result = ps_platform.get_submitted_jobs_by_name(["a000_INI.cmd", "a000_MISSING.cmd"])

    assert result == []


def test_get_submitted_jobs_by_name_returns_empty_when_send_command_fails(
    ps_platform: PsPlatform,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return an empty list when the remote ps command fails.

    :param ps_platform: PsPlatform fixture under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    monkeypatch.setattr(ps_platform, "send_command", lambda *a, **kw: False)

    result = ps_platform.get_submitted_jobs_by_name(["a000_INI.cmd"])

    assert result == []


def test_get_submitted_jobs_by_name_excludes_pre_existing_pids(
    ps_platform: PsPlatform,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Filter out PIDs that were present before submission.

    :param ps_platform: PsPlatform fixture under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    ps_platform._pre_submission_pids = {"a000_INI": {12301}}

    def _send_command(command: str, **_) -> bool:
        ps_platform._ssh_output = _PS_OUTPUT
        return True

    monkeypatch.setattr(ps_platform, "send_command", _send_command)

    result = ps_platform.get_submitted_jobs_by_name(["a000_INI.cmd", "a000_SIM.cmd"])

    assert result == ['12303', '12302']


def test_get_submitted_jobs_by_name_returns_empty_when_all_pids_pre_existing(
    ps_platform: PsPlatform,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return empty when every matching PID predates the submission.

    :param ps_platform: PsPlatform fixture under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    ps_platform._pre_submission_pids = {"a000_INI": {12301, 12303}}

    def _send_command(command: str, **_) -> bool:
        ps_platform._ssh_output = _PS_OUTPUT
        return True

    monkeypatch.setattr(ps_platform, "send_command", _send_command)

    result = ps_platform.get_submitted_jobs_by_name(["a000_INI.cmd", "a000_SIM.cmd"])

    assert result == []


def test_get_submitted_jobs_by_name_issues_single_ps_command(
    ps_platform: PsPlatform,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify that exactly one ps command is issued for any number of scripts.

    :param ps_platform: PsPlatform fixture under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    sent_commands: list[str] = []

    def _send_command(command: str, **_) -> bool:
        sent_commands.append(command)
        ps_platform._ssh_output = _PS_OUTPUT
        return True

    monkeypatch.setattr(ps_platform, "send_command", _send_command)

    ps_platform.get_submitted_jobs_by_name(["a000_INI.cmd", "a000_SIM.cmd"])

    assert len(sent_commands) == 1
    assert "ps" in sent_commands[0]


def test_pre_submission_snapshot_captures_matching_pids(
    ps_platform: PsPlatform,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Populate _pre_submission_pids with running remote PIDs for the given scripts.

    :param ps_platform: PsPlatform fixture under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    def _send_command(command: str, **_) -> bool:
        ps_platform._ssh_output = _PS_OUTPUT
        return True

    monkeypatch.setattr(ps_platform, "send_command", _send_command)

    ps_platform._pre_submission_snapshot(["a000_INI.cmd", "a000_SIM.cmd"])

    assert ps_platform._pre_submission_pids == {
        "a000_INI": {12301, 12303},
        "a000_SIM": {12302},
    }


def test_pre_submission_snapshot_ignores_errors(
    ps_platform: PsPlatform,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Leave _pre_submission_pids unchanged when the remote ps command fails.

    :param ps_platform: PsPlatform fixture under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    ps_platform._pre_submission_pids = {"stale": {1}}

    def _send_command(command: str, **_) -> bool:
        raise RuntimeError("SSH failure")

    monkeypatch.setattr(ps_platform, "send_command", _send_command)

    ps_platform._pre_submission_snapshot(["a000_INI.cmd"])

    assert ps_platform._pre_submission_pids == {"stale": {1}}


def test_submit_multiple_jobs_clears_and_snapshots_before_parent(
    ps_platform: PsPlatform,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify that _pre_submission_pids is cleared and snapshot is taken before submission.

    Since PsPlatform no longer overrides submit_multiple_jobs, this verifies
    that the base ParamikoPlatform correctly clears the state and calls the snapshot
    using PsPlatform's _get_process_list_output.

    :param ps_platform: PsPlatform fixture under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    ps_platform._pre_submission_pids = {"stale": {999}}

    snapshot_calls: list[list[str]] = []
    original_snapshot = ps_platform._pre_submission_snapshot

    def _snapshot(script_names: list) -> None:
        snapshot_calls.append(list(script_names))
        original_snapshot(script_names)

    monkeypatch.setattr(ps_platform, "_pre_submission_snapshot", _snapshot)
    monkeypatch.setattr(ps_platform, "send_command", lambda *a, **kw: False)

    ps_platform._pre_submission_pids = {}
    ps_platform._pre_submission_snapshot(["a.cmd", "b.cmd"])

    assert snapshot_calls == [["a.cmd", "b.cmd"]]
    assert ps_platform._pre_submission_pids == {}


def test_get_process_list_output_returns_ssh_output(
    ps_platform: PsPlatform,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_get_process_list_output returns the SSH ps output.

    :param ps_platform: PsPlatform fixture under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    def _send_command(command: str, **_) -> bool:
        ps_platform._ssh_output = _PS_OUTPUT
        return True

    monkeypatch.setattr(ps_platform, "send_command", _send_command)

    assert ps_platform._get_process_list_output() == _PS_OUTPUT


def test_get_process_list_output_returns_empty_when_send_command_fails(
    ps_platform: PsPlatform,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_get_process_list_output returns an empty string when the SSH command fails.

    :param ps_platform: PsPlatform fixture under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    monkeypatch.setattr(ps_platform, "send_command", lambda *a, **kw: False)

    assert ps_platform._get_process_list_output() == ""
