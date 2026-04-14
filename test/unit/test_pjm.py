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


from collections import OrderedDict
from pathlib import Path

import pytest

from autosubmit.config.yamlparser import YAMLParserFactory
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_packages import JobPackageSimple, JobPackageVertical, JobPackageHorizontal
from autosubmit.platforms.paramiko_submitter import ParamikoSubmitter
from autosubmit.platforms.pjmplatform import PJMPlatform

_EXPECTED_COMPLETED_JOBS = ["167727"]
_EXPECTED_OUTPUT = """JOB_ID     ST  REASON                         
167727     EXT COMPLETED               
167728     RNO -               
167729     RNE -               
167730     RUN -               
167732     ACC -               
167733     QUE -               
167734     RNA -               
167735     RNP -               
167736     HLD ASHOLD               
167737     ERR -               
167738     CCL -               
167739     RJT -  
"""


@pytest.fixture
def as_conf(autosubmit_config, tmpdir):
    exp_data = {
        "WRAPPERS": {
            "WRAPPERS": {
                "JOBS_IN_WRAPPER": "dummysection"
            }
        },
        "PLATFORMS": {
            "pytest-slurm": {
                "type": "slurm",
                "host": "localhost",
                "user": "user",
                "project": "project",
                "scratch_dir": "/scratch",
                "QUEUE": "queue",
                "ADD_PROJECT_TO_HOST": False,
                "MAX_WALLCLOCK": "00:01",
                "TEMP_DIR": "",
                "MAX_PROCESSORS": 99999,
            },
        },
        "LOCAL_ROOT_DIR": str(tmpdir),
        "LOCAL_TMP_DIR": str(tmpdir),
        "LOCAL_PROJ_DIR": str(tmpdir),
        "LOCAL_ASLOG_DIR": str(tmpdir),
    }
    as_conf = autosubmit_config("dummy-expid", exp_data)
    return as_conf


@pytest.fixture
def pjm_platform(as_conf):
    platform = PJMPlatform(expid="dummy-expid", name='pytest-slurm', config=as_conf.experiment_data)
    return platform


@pytest.fixture
def create_packages(as_conf, pjm_platform):
    simple_jobs = [Job("dummy-1", 1, Status.SUBMITTED, 0)]
    vertical_jobs = [Job("dummy-1", 1, Status.SUBMITTED, 0), Job("dummy-2", 2, Status.SUBMITTED, 0),
                     Job("dummy-3", 3, Status.SUBMITTED, 0)]
    horizontal_jobs = [Job("dummy-1", 1, Status.SUBMITTED, 0), Job("dummy-2", 2, Status.SUBMITTED, 0),
                       Job("dummy-3", 3, Status.SUBMITTED, 0)]
    for job in simple_jobs + vertical_jobs + horizontal_jobs:
        job._platform = pjm_platform
        job._platform.name = pjm_platform.name
        job.platform_name = pjm_platform.name
        job.processors = 2
        job.section = "dummysection"
        job._init_runtime_parameters()
        job.wallclock = "00:01"
    packages = [
        JobPackageSimple(simple_jobs),
        JobPackageVertical(vertical_jobs, configuration=as_conf),
        JobPackageHorizontal(horizontal_jobs, configuration=as_conf),
    ]
    for package in packages:
        if not isinstance(package, JobPackageSimple):
            package._name = "wrapped"
    return packages


@pytest.fixture
def remote_platform(autosubmit_config, autosubmit):
    as_conf = autosubmit_config("a000", {
        'DEFAULT': {
            'HPCARCH': 'ARM'
        }
    })

    yml_file = Path(__file__).resolve().parents[1] / "files/fake-jobs.yml"
    factory = YAMLParserFactory()
    parser = factory.create_parser()
    parser.data = parser.load(yml_file)
    as_conf.experiment_data.update(parser.data)
    yml_file = Path(__file__).resolve().parents[1] / "files/fake-platforms.yml"
    factory = YAMLParserFactory()
    parser = factory.create_parser()
    parser.data = parser.load(yml_file)
    as_conf.experiment_data.update(parser.data)

    submitter = ParamikoSubmitter(as_conf=as_conf)
    return submitter.platforms['ARM']


def test_parse_all_jobs_output(remote_platform):
    """Test parsing of all jobs output."""
    running_jobs = ["167728", "167729", "167730"]
    queued_jobs = ["167732", "167733", "167734", "167735", "167736"]
    failed_jobs = ["167737", "167738", "167739"]
    jobs_that_arent_listed = ["3442432423", "238472364782", "1728362138712"]
    for job_id in _EXPECTED_COMPLETED_JOBS:
        assert remote_platform.parse_all_jobs_output(_EXPECTED_OUTPUT, job_id) in remote_platform.job_status[
            "COMPLETED"]
    for job_id in failed_jobs:
        assert remote_platform.parse_all_jobs_output(_EXPECTED_OUTPUT, job_id) in remote_platform.job_status[
            "FAILED"]
    for job_id in queued_jobs:
        assert remote_platform.parse_all_jobs_output(_EXPECTED_OUTPUT, job_id) in remote_platform.job_status[
            "QUEUING"]
    for job_id in running_jobs:
        assert remote_platform.parse_all_jobs_output(_EXPECTED_OUTPUT, job_id) in remote_platform.job_status[
            "RUNNING"]
    for job_id in jobs_that_arent_listed:
        assert remote_platform.parse_all_jobs_output(_EXPECTED_OUTPUT, job_id) == []


def test_get_submitted_job_id(remote_platform):
    """Test parsing of submitted job id."""
    submitted_ok = "[INFO] PJM 0000 pjsub Job 167661 submitted."
    submitted_fail = "[ERR.] PJM 0057 pjsub node=32 is greater than the upper limit (24)."
    output = remote_platform.get_submitted_job_id(submitted_ok)
    assert output == [167661]
    output = remote_platform.get_submitted_job_id(submitted_fail)
    assert output == []


def test_parse_queue_reason(remote_platform):
    """Test parsing of queue reason."""
    output = remote_platform.parse_queue_reason(_EXPECTED_OUTPUT, _EXPECTED_COMPLETED_JOBS[0])
    assert output == "COMPLETED"


def test_process_ready_jobs_valid_packages_to_submit(mocker, pjm_platform, create_packages):
    jobs_id = [1, 2, 3]
    scripts_to_submit = OrderedDict(
        [
            ("dummy-1.cmd", create_packages[0]),
            ("wrapped-1.cmd", create_packages[1]),
            ("wrapped-2.cmd", create_packages[2]),
        ]
    )

    mocker.patch.object(pjm_platform, "submit_multiple_jobs", return_value=jobs_id)
    mocker.patch.object(pjm_platform, "_check_and_cancel_duplicated_job_names", return_value=None)

    pjm_platform.process_ready_jobs(scripts_to_submit)

    for i, package in enumerate(create_packages):
        for job in package.jobs:
            assert job.id == str(jobs_id[i])
            assert job.status == Status.SUBMITTED


def test_get_submitted_jobs_by_name_returns_max_id(
        pjm_platform: PJMPlatform,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return the highest job ID found for each queried script.

    :param pjm_platform: PJM platform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """

    def _send_command(cmd, **_):
        pjm_platform._ssh_output = "  1001  job_a\n  1002  job_a\n"

    monkeypatch.setattr(pjm_platform, "send_command", _send_command)

    result = pjm_platform.get_submitted_jobs_by_name(["job_a.cmd"])

    assert result == [1002]


def test_get_submitted_jobs_by_name_returns_empty_when_no_digits(
        pjm_platform: PJMPlatform,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return an empty list when the output contains no numeric job IDs.

    :param pjm_platform: PJM platform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """

    def _send_command(cmd, **_):
        pjm_platform._ssh_output = "JOB_ID  NAME\n"

    monkeypatch.setattr(pjm_platform, "send_command", _send_command)

    result = pjm_platform.get_submitted_jobs_by_name(["job_a.cmd"])

    assert result == []


def test_get_submitted_jobs_by_name_returns_empty_when_one_script_missing(
        pjm_platform: PJMPlatform,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return an empty list when one of the queried scripts has no match.

    :param pjm_platform: PJM platform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    outputs = iter(["  1001  job_a\n", ""])

    def _send_command(cmd, **_):
        pjm_platform._ssh_output = next(outputs)

    monkeypatch.setattr(pjm_platform, "send_command", _send_command)

    result = pjm_platform.get_submitted_jobs_by_name(["job_a.cmd", "job_b.cmd"])

    assert result == []


def test_get_job_names_cmd_empty_list_returns_empty(
        pjm_platform: PJMPlatform,
) -> None:
    """Return empty when no job names are given.

    :param pjm_platform: PJM platform under test.
    """
    assert pjm_platform._get_job_names_cmd([]) == ""


def test_get_job_names_cmd_nonempty(
        pjm_platform: PJMPlatform,
) -> None:
    """grouping for a non-empty job-name list.

    :param pjm_platform: PJM platform under test.
    """
    cmd = pjm_platform._get_job_names_cmd(["job_a", "job_b"])

    assert "job_a" in cmd
    assert "job_b" in cmd


def test_cancel_jobs_empty_list_sends_no_command(
        pjm_platform: PJMPlatform,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Send no command when the list of job IDs to cancel is empty.

    :param pjm_platform: PJM platform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    sent: list[str] = []
    monkeypatch.setattr(pjm_platform, "send_command", lambda cmd, **_: sent.append(cmd))

    pjm_platform.cancel_jobs([])

    assert sent == []


def test_cancel_jobs_single_id_sends_pjdel_command(
        pjm_platform: PJMPlatform,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Send a single pjdel command for one job ID.

    :param pjm_platform: PJM platform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    sent: list[str] = []
    monkeypatch.setattr(pjm_platform, "send_command", lambda cmd, **_: sent.append(cmd))

    pjm_platform.cancel_jobs(["1001"])

    assert sent == ["pjdel 1001"]


@pytest.mark.parametrize("job_ids,expected_command", [
    (["1001", "1002", "1003"], "pjdel 1001 1002 1003"),
    (["1001", "1002"], "pjdel 1001 1002"),
])
def test_cancel_jobs_multiple(
        pjm_platform: PJMPlatform,
        monkeypatch: pytest.MonkeyPatch,
        job_ids: list[str],
        expected_command: str,
) -> None:
    """Send one space-joined pjdel command for multiple job IDs.

    :param pjm_platform: PJM platform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    :param job_ids: Job IDs to cancel.
    :param expected_command: Expected command string.
    """
    sent: list[str] = []
    monkeypatch.setattr(pjm_platform, "send_command", lambda cmd, **_: sent.append(cmd))

    pjm_platform.cancel_jobs(job_ids)

    assert len(sent) == 1
    assert sent[0] == expected_command
