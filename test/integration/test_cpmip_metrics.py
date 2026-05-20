# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Autosubmit. If not, see <http://www.gnu.org/licenses/>.

"""Integration tests for CPMIP threshold notifications via ``autosubmit run``."""

from pathlib import Path
from typing import Any

import pytest
from ruamel.yaml import YAML

from test.integration.test_mail import (  # noqa: F401  (pytest fixture imports)
    configured_mail,
    fake_smtp_server,
)
from test.integration.test_utils.docker import get_mailhog_messages


pytestmark = [pytest.mark.docker, pytest.mark.slurm, pytest.mark.ssh]

# The experiment runs one SIM chunk of exactly one calendar year, so simulated
# years equals 1.0. Processors is fixed to keep CPU-hour metrics deterministic.
SIM_PROCESSORS = 2
SIM_SIMULATED_YEARS = 1.0

VIOLATION_THRESHOLDS = {
    "SYPD": {"THRESHOLD": 0.01, "COMPARISON": "less_than", "%_ACCEPTED_ERROR": 0},
    "CHSY": {"THRESHOLD": 0.0001, "COMPARISON": "less_than", "%_ACCEPTED_ERROR": 0},
    "CORE_HOURS": {"THRESHOLD": 0.0001, "COMPARISON": "less_than", "%_ACCEPTED_ERROR": 0},
}
NON_VIOLATION_THRESHOLDS = {
    "SYPD": {"THRESHOLD": 1.0, "COMPARISON": "greater_than", "%_ACCEPTED_ERROR": 0},
}


def _experiment_data(thresholds: dict, notifications: str = "true") -> dict:
    """Build the experiment YAML payload with a single SIM job pinned to the Slurm fixture."""
    fake_platforms = YAML().load(Path(__file__).resolve().parents[1] / "files/fake-platforms.yml")
    return {
        "DEFAULT": {"HPCARCH": "TEST_SLURM"},
        "MAIL": {"NOTIFICATIONS": notifications, "TO": ["user@example.com"]},
        "PLATFORMS": {"TEST_SLURM": fake_platforms["PLATFORMS"]["TEST_SLURM"]},
        "EXPERIMENT": {
            "DATELIST": "20200101",
            "MEMBERS": "fc0",
            "CHUNKSIZE": 1,
            "CHUNKSIZEUNIT": "year",
            "NUMCHUNKS": 1,
            "CALENDAR": "standard",
        },
        "JOBS": {
            "SIM": {
                "RUNNING": "chunk",
                "PLATFORM": "TEST_SLURM",
                "WALLCLOCK": "00:05",
                "PROCESSORS": SIM_PROCESSORS,
                "SCRIPT": "echo CPMIP integration test; sleep 3",
                "CPMIP_THRESHOLDS": thresholds,
            }
        },
    }


def _run_experiment(autosubmit_exp, configure_mail, thresholds: dict, notifications: str = "true"):
    """Create a fresh experiment, wire mail config, and run it to completion."""
    exp = autosubmit_exp(experiment_data=_experiment_data(thresholds, notifications))
    _, api_base = configure_mail(exp)
    exp.autosubmit._check_ownership_and_set_last_command(exp.as_conf, exp.expid, "run")

    exit_code = exp.autosubmit.run_experiment(exp.expid)
    assert exit_code == 0, f"autosubmit run exited with {exit_code}"
    return exp, api_base


def _find_sim_stat_file(exp) -> Path:
    """Locate the SIM job stat file written by the (single) successful chunk."""
    stat_file = next((exp.exp_path / "tmp").glob(f"{exp.expid}_*_SIM_STAT_0"), None)
    assert stat_file is not None, "SIM stat file was not recovered"
    return stat_file


def _read_stat_timestamps(stat_file: Path) -> tuple[int, int]:
    """Return ``(start_time, end_time)`` (Unix seconds) from the SIM stat file."""
    lines = stat_file.read_text().strip().splitlines()
    assert len(lines) >= 2, f"Unexpected stat file format: {stat_file}"
    start_time, end_time = int(lines[0]), int(lines[1])
    assert end_time > start_time, f"Non-positive runtime in stat file: {stat_file}"
    return start_time, end_time


def _compute_expected_metrics(stat_file: Path) -> tuple[float, float, float]:
    """Recompute (SYPD, CHSY, CORE_HOURS) from the stat file using the CPMIP definitions.

    SYPD        = simulated_years * 24h / runtime_hours
    CHSY        = ncpus * runtime_hours / simulated_years
    CORE_HOURS  = ncpus * runtime_hours

    Recomputing here (instead of calling ``CPMIPMetrics``) ensures the test
    fails if either the production formula or the mail rendering drift.
    """
    start_time, end_time = _read_stat_timestamps(stat_file)
    runtime_hours = (end_time - start_time) / 3600.0
    sypd = SIM_SIMULATED_YEARS * 24.0 / runtime_hours
    chsy = SIM_PROCESSORS * runtime_hours / SIM_SIMULATED_YEARS
    core_hours = SIM_PROCESSORS * runtime_hours
    return sypd, chsy, core_hours


def _assert_inbox_count(api_base: str, expected: int) -> list[dict[str, Any]]:
    """Assert the MailHog inbox holds ``expected`` messages, returning them on match."""
    messages = get_mailhog_messages(api_base).json()["items"]
    assert len(messages) == expected, (
        f"Expected {expected} mail(s) in inbox, found {len(messages)}"
    )
    return messages


def _assert_violation_email(email: dict[str, Any], expid: str, job_name: str,
                            stat_file: Path) -> None:
    """Verify headers, recipients and body of the CPMIP violation mail."""
    assert "CPMIP Threshold Violation detected" in email["Content"]["Headers"]["Subject"][0]
    assert "notifier@localhost" in email["Raw"]["From"]
    assert any("user@example.com" in recipient for recipient in email["Raw"]["To"])

    expected_sypd, expected_chsy, expected_core_hours = _compute_expected_metrics(stat_file)
    body = email["Content"]["Body"].replace("\r\n", "\n").strip()

    # Preamble
    assert f"Experiment id:  {expid}" in body
    assert f"Job name: {job_name}" in body
    assert "The following CPMIP metrics violated their configured thresholds:" in body

    # SYPD violation: 0.01 less_than, observed value must equal the recomputed SYPD
    assert "Metric: SYPD" in body
    assert "Comparison: must be <= effective bound (less_than)" in body
    assert "Configured threshold: 0.01" in body
    assert "Accepted error (%): 0" in body
    assert "Effective bound: 0.01" in body
    assert f"Observed value: {expected_sypd}" in body

    # CHSY violation: 0.0001 less_than, observed value must equal the recomputed CHSY
    assert "Metric: CHSY" in body
    assert "Configured threshold: 0.0001" in body
    assert "Effective bound: 0.0001" in body
    assert f"Observed value: {expected_chsy}" in body

    # CORE_HOURS violation: 0.0001 less_than, observed value must equal the recomputed core-hours
    assert "Metric: CORE_HOURS" in body
    assert f"Observed value: {expected_core_hours}" in body


@pytest.mark.parametrize(
    "thresholds, notifications, expected_inbox_count",
    [
        (VIOLATION_THRESHOLDS, "true", 1),
        (NON_VIOLATION_THRESHOLDS, "true", 0),
        (VIOLATION_THRESHOLDS, "false", 0),
        (NON_VIOLATION_THRESHOLDS, "false", 0),
    ],
    ids=[
        "violation_emits_notification",
        "no_violation",
        "violation_but_notifications_off",
        "no_violation_and_notifications_off",
    ],
)
def test_cpmip_notification(autosubmit_exp, configured_mail, slurm_server,  # noqa: F811
                             thresholds, notifications, expected_inbox_count):
    """Cover every (violation, notifications-enabled) combination in one place."""
    exp, api_base = _run_experiment(
        autosubmit_exp, configured_mail, thresholds, notifications=notifications
    )

    messages = _assert_inbox_count(api_base, expected=expected_inbox_count)
    if expected_inbox_count == 0:
        return

    stat_file = _find_sim_stat_file(exp)
    job_name = stat_file.name.replace("_STAT_0", "")
    _assert_violation_email(messages[0], exp.expid, job_name, stat_file)
