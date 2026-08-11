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

from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from ruamel.yaml import YAML

# noinspection PyProtectedMember
from autosubmit.scripts._traceability import _format_command

# noinspection PyProtectedMember
from autosubmit.scripts.autosubmit import _autosubmit

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

    from test.integration.conftest import (
        AutosubmitExperiment,
        AutosubmitExperimentFixture,
    )


@pytest.fixture
def test_experiment(
    get_next_expid,
    autosubmit_exp: "AutosubmitExperimentFixture",
    mocker: "MockerFixture",
) -> "AutosubmitExperiment":
    test_files_path = Path(__file__).resolve().parents[2]
    fake_jobs: dict = YAML().load(test_files_path / "files/fake-jobs.yml")
    fake_platforms: dict = YAML().load(test_files_path / "files/fake-platforms.yml")

    mocker.patch(
        "autosubmit.experiment.manage.user_yes_no_query",
        return_value=True,
    )

    return autosubmit_exp(
        expid=get_next_expid(),
        experiment_data={
            "DEFAULT": {"HPCARCH": "TEST_SLURM"},
            **fake_jobs,
            **fake_platforms,
        },
    )


def _parse_command(command: list[str], expid: str) -> list[str]:
    """Replaces ``{expid}`` by the experiment ID."""
    return [c.format(expid=expid) for c in command]


def _assert_run_result(r: bool | int):
    """Assert that the run result is ``True`` or ``0`` (i.e., truthy)."""
    if type(r) is int:
        assert r == 0
    else:
        assert r


@pytest.mark.parametrize(
    "command",
    [
        ["autosubmit", "configure"],
        ["autosubmit", "expid", "-dm", "-H", "local", "-d", "Tutorial"],
        ["autosubmit", "delete", "{expid}"],
        ["autosubmit", "monitor", "{expid}", "--hide"],  # TODO
        ["autosubmit", "stats", "{expid}"],  # TODO
        ["autosubmit", "clean", "{expid}"],
        ["autosubmit", "inspect", "{expid}"],  # TODO
        ["autosubmit", "report", "{expid}"],  # TODO
        ["autosubmit", "describe", "{expid}"],
        # ['autosubmit', 'migrate', '-fs', 'Any', '{expid}'],
        ["autosubmit", "create", "{expid}", "--hide"],
        [
            "autosubmit",
            "setstatus",
            "{expid}",
            "-t",
            "READY",
            "-fs",
            "WAITING",
            "--hide",
        ],  # TODO
        [
            "autosubmit",
            "testcase",
            "-dm",
            "-H",
            "local",
            "-d",
            "Tutorial",
            "-c",
            "1",
            "-m",
            "fc0",
            "-s",
            "19651101",
        ],
        # TODO
        ["autosubmit", "refresh", "{expid}"],  # TODO
        ["autosubmit", "updateversion", "{expid}"],  # TODO
        ["autosubmit", "upgrade", "{expid}"],  # TODO
        ["autosubmit", "archive", "{expid}"],  # TODO
        ["autosubmit", "readme"],  # TODO
        ["autosubmit", "changelog"],  # TODO
        # ['autosubmit', 'dbfix', '{expid}'],  # TODO
        ["autosubmit", "updatedescrip", "{expid}", "description"],
        ["autosubmit", "cat-log", "{expid}"],
        ["autosubmit", "stop", "-a"],
        [
            "autosubmit",
            "testcase",
            "-y",
            "{expid}",
            "-H",
            "Marenostrum5",
            "-d",
            "Testing Suite MAIN FESOM",
        ],
        ["autosubmit", "manpages"],
    ],
    ids=[
        "configure",
        "expid",
        "delete",
        "monitor",
        "stats",
        "clean",
        "inspect",
        "report",
        "describe",
        # 'migrate',
        "create",
        "setstatus",
        "testcase",
        "refresh",
        "updateversion",
        "upgrade",
        "archive",
        "readme",
        "changelog",
        "updatedescrip",
        "cat-log",
        "stop",
        "testcase copy",
        "manpages",
    ],
)
def test_run_command(command: list[str], test_experiment: "AutosubmitExperiment"):
    """Test the is simply used to check if commands are not broken on runtime, it doesn't check behaviour or output

    TODO: improve quality of the test in order to validate each scenario and its outputs
    TODO: commands that have a TODO at its side needs behaviour tests
    """
    command = _parse_command(command, test_experiment.expid)
    r = _autosubmit(command[1:])
    _assert_run_result(r)


@pytest.mark.parametrize(
    "command",
    [
        [
            "autosubmit",
            "setstatus",
            "{expid}",
            "-t",
            "READY",
            "-fs",
            "WAITING",
            "--hide",
        ],
        [
            "autosubmit",
            "setstatus",
            "{expid}",
            "-t",
            "READY",
            "-fs",
            "WAITING",
            "--hide",
            "-plt",
        ],
        [
            "autosubmit",
            "setstatus",
            "{expid}",
            "-t",
            "READY",
            "-fs",
            "WAITING",
            "--hide",
            "-np",
        ],
        [
            "autosubmit",
            "setstatus",
            "{expid}",
            "-t",
            "READY",
            "-fs",
            "WAITING",
            "--hide",
            "-np",
            "-plt",
        ],
        [
            "autosubmit",
            "create",
            "{expid}",
            "--hide",
        ],
        [
            "autosubmit",
            "create",
            "{expid}",
            "--hide",
            "-plt",
        ],
        [
            "autosubmit",
            "create",
            "{expid}",
            "--hide",
            "-np",
        ],
        [
            "autosubmit",
            "create",
            "{expid}",
            "--hide",
            "-np",
            "-plt",
        ],
        [
            "autosubmit",
            "recovery",
            "{expid}",
            "--hide",
            "--offline",  # prevent connection to platform
        ],
        [
            "autosubmit",
            "recovery",
            "{expid}",
            "--hide",
            "--offline",  # prevent connection to platform
            "-plt",
        ],
        [
            "autosubmit",
            "recovery",
            "{expid}",
            "--hide",
            "--offline",  # prevent connection to platform
            "-np",
        ],
        [
            "autosubmit",
            "recovery",
            "{expid}",
            "--hide",
            "--offline",  # prevent connection to platform
            "-np",
            "-plt",
        ],
    ],
    ids=[
        "setstatus",
        "setstatus with plot",
        "setstatus with no plot",
        "setstatus with no plot and plot",
        "create",
        "create with plot",
        "create with no plot",
        "create with no plot and plot",
        "recovery",
        "recovery with plot",
        "recovery with no plot",
        "recovery with no plot and plot",
    ],
)
def test_run_command_plot_behavior(
    command: list[str], test_experiment: "AutosubmitExperiment"
):
    """Test the plot behaviour of the setstatus, create and recovery commands."""
    has_both_plot_flags = "-np" in command and "-plt" in command

    command = _parse_command(command, test_experiment.expid)
    if has_both_plot_flags:
        with pytest.raises(SystemExit) as cm:
            _autosubmit(command[1:])
        assert cm.value.code == 2
    else:
        r = _autosubmit(command[1:])
        _assert_run_result(r)


@pytest.mark.parametrize(
    "command",
    [
        ["autosubmit", "install"],
        ["autosubmit", "-lc", "ERROR", "-lf", "WARNING", "run", "{expid}"],
        ["autosubmit", "recovery", "{expid}", "--hide"],
        ["autosubmit", "provenance", "{expid}", "--rocrate"],
    ],
    ids=["install", "run", "recovery", "provenance"],
)
def test_run_command_raises_autosubmit(
    command: list[str], test_experiment: "AutosubmitExperiment", mocker: "MockerFixture"
):
    """Test the is simply used to check if commands are not broken on runtime.

    It doesn't check behaviour or output.
    """
    command = _parse_command(command, test_experiment.expid)
    if "run" in command:
        r = _autosubmit(command[1:])
        assert r == 7014
    elif "install" in command:
        mocked_log = mocker.patch("autosubmit.install.Log")
        r = _autosubmit(command[1:])
        assert r == 1
        assert "Database already exists" in mocked_log.error.call_args[0][0]
    elif "recovery" in command:
        r = _autosubmit(command[1:])
        # Can't establish a connection to a platform.
        assert r == 7050
    elif "provenance" in command:
        r = _autosubmit(command[1:])
        # RO-Crate key is missing
        assert r == 7012


@pytest.mark.parametrize(
    "command, warning_log_message",
    [
        (
            ["autosubmit", "recovery", "{expid}", "--no_recover_logs", "--offline"],
            "no_recover_logs is deprecated",
        ),
    ],
    ids=["recovery with no_recover_logs"],
)
def test_run_command_logs_warning(
    command: list[str],
    warning_log_message: str,
    test_experiment: "AutosubmitExperiment",
):
    command = _parse_command(command, test_experiment.expid)

    with pytest.warns(UserWarning, match=warning_log_message):
        _autosubmit(command[1:])


@pytest.mark.parametrize(
    "command,expected",
    [
        (
            ["autosubmit", "report", "{expid}", "-all"],
            {
                "required_top_level": ("HPCARCH",),
                "required_section_keys": ("FILE", "RUNNING"),
            },
        )
    ],
    ids=["report -all"],
)
def test_run_report_command(
    command: list[str],
    expected: dict,
    test_experiment: "AutosubmitExperiment",
):
    """Validate `autosubmit report -all` output (issue #1043).

    Checks that the parameter list contains the expected content and is free
    of the two redundancy patterns the issue describes: global keys duplicated
    under every ``JOBS.<section>.*`` prefix, and sibling jobs re-nested under each
    section (JOBS.<X>.JOBS.<Y>.*).
    """

    command = _parse_command(command, test_experiment.expid)
    r = _autosubmit(command[1:])
    _assert_run_result(r)

    report = next(
        Path(test_experiment.tmp_dir).glob(
            f"{test_experiment.expid}_parameter_list_*.txt"
        )
    )
    keys = {
        line.split("=", 1)[0] for line in report.read_text().splitlines() if "=" in line
    }

    for k in expected["required_top_level"]:
        assert k in keys, f"Top-level key {k!r} missing from report"

    sections = {k.split(".")[1] for k in keys if k.startswith("JOBS.")}
    assert sections, "Report contains no JOBS.<section>.* entries"

    for section in sections:
        for suffix in expected["required_section_keys"]:
            assert f"JOBS.{section}.{suffix}" in keys, (
                f"Static key JOBS.{section}.{suffix} missing from report"
            )

    sibling_nested = [k for k in keys if k.startswith("JOBS.") and ".JOBS." in k]
    assert not sibling_nested, (
        f"Sibling jobs re-nested under sections (issue #1043): {sibling_nested[:3]}"
    )

    top_level_namespaces = {
        k.split(".", 1)[0] for k in keys if "." in k and not k.startswith("JOBS.")
    }
    for section in sections:
        for ns in top_level_namespaces:
            duplicated = [k for k in keys if k.startswith(f"JOBS.{section}.{ns}.")]
            assert not duplicated, (
                f"Global namespace {ns!r} duplicated under JOBS.{section}.* "
                f"(issue #1043): {duplicated[:3]}"
            )


@pytest.mark.parametrize(
    "template_content,expected_output",
    [
        ("%%EXPERIMENT.CHUNKSIZEUNIT%%", "%EXPERIMENT.CHUNKSIZEUNIT%"),
        ("%^EXPERIMENT.CHUNKSIZE%", "-"),
        ("% EXPERIMENT.CHUNKSIZE %", "% EXPERIMENT.CHUNKSIZE %"),
        ("%INVALID_KEY%", "-"),
        ("50%% off", "50%% off"),
    ],
    ids=[
        "escape_renders_literal",
        "invalid_char_in_key_is_unknown",
        "whitespace_breaks_placeholder",
        "unknown_key_renders_dash",
        "stray_double_percent_left_alone",
    ],
)
def test_run_report_template_edge_cases(
    template_content: str,
    expected_output: str,
    tmp_path: Path,
    test_experiment: "AutosubmitExperiment",
):
    """Validate template-substitution edge cases for `autosubmit report -t`."""
    template_path = tmp_path / "template.txt"
    template_path.write_text(template_content)

    command = ["autosubmit", "report", test_experiment.expid, "-t", str(template_path)]
    r = _autosubmit(command[1:])
    _assert_run_result(r)

    report = next(
        Path(test_experiment.tmp_dir).glob(f"{test_experiment.expid}_report_*")
    )
    rendered = report.read_text().rstrip("\n")
    assert rendered == expected_output


def test_autosubmit_help(capsys):
    _autosubmit(["--help"])
    captured = capsys.readouterr()

    assert "Autosubmit is open-source software" in captured.out
    assert "migrate" not in captured.out
    assert "setstatus" in captured.out


def test_format_command_redacts_database_url():
    command = _format_command(
        [
            "configure",
            "--database-backend",
            "postgres",
            "--database-conn-url",
            "postgresql://user:password@host:5432/autosubmit",
            "--token=abc",
        ]
    )

    assert "password" not in command
    assert "REDACTED" in command

    assert "--token=abc" not in command
    assert "--token=<REDACTED>" in command
