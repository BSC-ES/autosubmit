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

"""Unit tests for the PBS platform."""

from collections import OrderedDict

import pytest

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_packages import JobPackageSimple
from autosubmit.log.log import AutosubmitError
from autosubmit.platforms.pbsplatform import PBSPlatform


@pytest.fixture
def platform(autosubmit_config):
    expid = 'a000'
    as_conf = autosubmit_config(expid, experiment_data={})
    return PBSPlatform(expid='a000', name='local', config=as_conf.experiment_data)


def test_properties(platform):
    props = {
        'name': 'foo',
        'host': 'localhost1',
        'user': 'sam',
        'project': 'proj1',
        'budget': 100,
        'reservation': 1,
        'exclusivity': True,
        'hyperthreading': True,
        'type': 'SuperPBS',
        'scratch': '/scratch/1',
        'project_dir': '/proj1',
        'root_dir': '/root_1',
        'partition': 'inter',
        'queue': 'prio1'
    }
    for prop, value in props.items():
        setattr(platform, prop, value)
    for prop, value in props.items():
        assert value == getattr(platform, prop)


@pytest.fixture
def as_conf(autosubmit_config, tmpdir):
    exp_data = {
        "PLATFORMS": {
            "pytest-pbs": {
                "type": "pbs",
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
    return autosubmit_config("dummy-expid", exp_data)


@pytest.fixture
def create_packages(as_conf, pbs_platform):
    simple_jobs_1 = [Job("dummy-1", 1, Status.SUBMITTED, 0)]
    simple_jobs_2 = [Job("dummy-1", 1, Status.SUBMITTED, 0),
                     Job("dummy-2", 2, Status.SUBMITTED, 0),
                     Job("dummy-3", 3, Status.SUBMITTED, 0)]
    simple_jobs_3 = [Job("dummy-1", 1, Status.SUBMITTED, 0),
                     Job("dummy-2", 2, Status.SUBMITTED, 0),
                     Job("dummy-3", 3, Status.SUBMITTED, 0)]
    for job in simple_jobs_1 + simple_jobs_2 + simple_jobs_3:
        job._platform = pbs_platform
        job._platform.name = pbs_platform.name
        job.platform_name = pbs_platform.name
        job.processors = 2
        job.section = "dummysection"
        job.wallclock = "00:01"
    packages = [
        JobPackageSimple(simple_jobs_1),
        JobPackageSimple(simple_jobs_2),
        JobPackageSimple(simple_jobs_3),
    ]
    return packages

def test_get_header(pbs_platform):
    job = Job("dummy", 10000, Status.SUBMITTED, 0)

    job.het = dict()
    job.het["HETSIZE"] = 0

    parameters = dict()

    parameters['TASKS'] = '0'
    parameters['NODES'] = '0'
    parameters['MEMORY'] = ''
    parameters['NUMTHREADS'] = '0'
    parameters['RESERVATION'] = ''
    parameters['CURRENT_QUEUE'] = ''
    parameters['CURRENT_PROJ'] = ''
    parameters['MEMORY_PER_TASK'] = ''
    parameters['CUSTOM_DIRECTIVES'] = ''

    pbs_platform.header.HEADER = '%OUT_LOG_DIRECTIVE%%ERR_LOG_DIRECTIVE%%QUEUE_DIRECTIVE%%TASKS_PER_NODE_DIRECTIVE%%THREADS_PER_TASK_DIRECTIVE%%CUSTOM_DIRECTIVES%%ACCOUNT_DIRECTIVE%%NODES_DIRECTIVE%%RESERVATION_DIRECTIVE%%MEMORY_DIRECTIVE%%MEMORY_PER_TASK_DIRECTIVE%'
    assert pbs_platform.get_header(job, parameters) == 'dummy.cmd.out.0dummy.cmd.err.0PBS -l select=1'

    parameters['TASKS'] = '2'
    parameters['NODES'] = '2'
    parameters['MEMORY'] = '100kb'
    parameters['NUMTHREADS'] = '2'
    parameters['RESERVATION'] = 'x'
    parameters['CURRENT_QUEUE'] = 'debug'
    parameters['CURRENT_PROJ'] = 'project'
    parameters['MEMORY_PER_TASK'] = '100kb'
    parameters['CUSTOM_DIRECTIVES'] = 'custom'

    pbs_platform.header.HEADER = '%OUT_LOG_DIRECTIVE%%ERR_LOG_DIRECTIVE%%QUEUE_DIRECTIVE%%TASKS_PER_NODE_DIRECTIVE%%THREADS_PER_TASK_DIRECTIVE%%CUSTOM_DIRECTIVES%%ACCOUNT_DIRECTIVE%%NODES_DIRECTIVE%%RESERVATION_DIRECTIVE%%MEMORY_DIRECTIVE%%MEMORY_PER_TASK_DIRECTIVE%'
    assert pbs_platform.get_header(job,
                                   parameters) == 'dummy.cmd.out.0dummy.cmd.err.0PBS -q debug:mpiprocs=2:ompthreads=2c\nu\ns\nt\no\nmPBS -W group_list=projectPBS -l select=2PBS -W x=x:mem=100kb:vmem=100kb'


def test_pbs_platform_constructor(mocker, tmp_path, pbs_platform):
    assert pbs_platform.name == 'pytest-pbs'
    assert pbs_platform.expid == 'a000'
    assert pbs_platform.header is not None
    assert pbs_platform.wrapper is None
    assert len(pbs_platform.job_status) == 4


def test_pbs_platform_submit_multiple_jobs_raises_autosubmit_error_with_trace(
        platform: PBSPlatform,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AutosubmitError from submit_multiple_jobs propagates through process_ready_jobs.

    :param platform: PBS platform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    ae = AutosubmitError(message='violates resource limits', code=123, trace='ERR!')
    monkeypatch.setattr(platform, "submit_multiple_jobs", lambda _: (_ for _ in ()).throw(ae))

    with pytest.raises(AutosubmitError) as cm:
        platform.process_ready_jobs(OrderedDict([("dummy.cmd", object())]))

    assert cm.value.trace == ae.trace
    assert cm.value.message == ae.message


def test_submit_multiple_jobs_raises_when_no_job_ids_are_recovered(
        pbs_platform: PBSPlatform,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise AutosubmitError when no job IDs can be recovered after submission.

    :param pbs_platform: PBS platform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    monkeypatch.setattr(pbs_platform, "send_command", lambda *a, **k: True)
    monkeypatch.setattr(pbs_platform, "get_multi_submit_cmd", lambda *a, **k: "qsub job.cmd")
    monkeypatch.setattr(pbs_platform, "get_submitted_job_id", lambda *a, **k: [])
    monkeypatch.setattr(pbs_platform, "get_submitted_jobs_by_name", lambda *a, **k: [])
    monkeypatch.setattr(pbs_platform, "_pre_submission_snapshot", lambda *a, **k: None)
    pbs_platform._ssh_output = "submission output without recoverable ids"

    with pytest.raises(AutosubmitError):
        pbs_platform.submit_multiple_jobs({"job.cmd": object()})


@pytest.mark.parametrize('ssh_return,job_id,result', [
    ("Miyabi stop\n\nNo job", '', ''),
    ("Miyabi stop\n\nJOB_ID STATUS\n1116786 FINISH", '1116786', 'FINISH'),
    ("Miyabi stop\n\nJOB_ID STATUS\n1116786 FINISH\n1116787 QUEUED", '1116787', 'QUEUED')
], ids=['empty', 'one job', 'multiple jobs'])
def test_parse_all_jobs_output(pbs_platform, ssh_return, job_id, result):
    """Parse the status of a specific job ID from qstat output.

    :param pbs_platform: PBS platform under test.
    :param ssh_return: Simulated qstat output.
    :param job_id: Job ID to look up.
    :param result: Expected status string.
    """
    assert pbs_platform.parse_all_jobs_output(ssh_return, job_id) == result


@pytest.mark.parametrize("output,expected_job_ids", [
    ("1116786.opbs\n1116787.opbs\n", [1116786, 1116787]),
    ("1116786.opbs\n", [1116786]),
    ("1116786\n", [1116786]),
], ids=["multiple-submissions", "single-submission", "no-server-suffix"])
def test_get_submitted_job_id_parses_batched_output(
        pbs_platform: PBSPlatform,
        output: str,
        expected_job_ids: list,
) -> None:
    """Parse all PBS job IDs from a batched qsub output.

    :param pbs_platform: PBS platform under test.
    :param output: Raw qsub command output.
    :param expected_job_ids: Expected parsed job identifiers.
    """
    assert pbs_platform.get_submitted_job_id(output) == expected_job_ids


@pytest.mark.parametrize("script_names,ssh_output,expected_ids,expected_cmd_count,expected_filter", [
    (["job_a.cmd"], "1001.server  job_a\n", [1001], 1, "job_a"),
    (["job_a.cmd"], "1001.server  job_a\n1002.server  job_a\n", [1002], 1, "job_a"),
    (["job_a.cmd"], "", [], 1, "job_a"),
    (["job_a.cmd", "job_b.cmd"], "1001.server  job_a\n2001.server  job_b\n", [1001, 2001], 1, "job_a|job_b"),
    (["job_a.cmd", "job_b.cmd"], "1001.server  job_a\n", [], 1, "job_a|job_b"),
    (["job_a.cmd", "job_b.cmd"], "1001.server  job_a\n1002.server  job_a\n2001.server  job_b\n", [1002, 2001], 1, "job_a|job_b"),
], ids=["single-single", "single-max", "single-empty", "multi-all", "multi-missing", "multi-max"])
def test_get_submitted_jobs_by_name(
        pbs_platform: PBSPlatform,
        monkeypatch: pytest.MonkeyPatch,
        script_names: list[str],
        ssh_output: str,
        expected_ids: list[int],
        expected_cmd_count: int,
        expected_filter: str,
) -> None:
    """Return correct job IDs and issue exactly one batched qstat call.

    :param pbs_platform: PBS platform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    :param script_names: Script filenames passed to ``get_submitted_jobs_by_name``.
    :param ssh_output: Simulated ``qstat`` output.
    :param expected_ids: Expected list of job IDs.
    :param expected_cmd_count: Expected number of SSH commands issued.
    :param expected_filter: Expected substring in the issued command.
    """
    sent: list[str] = []

    def _send(cmd, **_):
        sent.append(cmd)

    monkeypatch.setattr(pbs_platform, "send_command", _send)
    monkeypatch.setattr(pbs_platform, "get_ssh_output", lambda: ssh_output)

    assert pbs_platform.get_submitted_jobs_by_name(script_names) == expected_ids
    assert len(sent) == expected_cmd_count
    assert expected_filter in sent[0]


@pytest.mark.parametrize("job_ids,expected_commands", [
    ([], []),
    (["101"], ["qdel 101"]),
    (["101", "202"], ["qdel 101 202"]),
    (["101", "202", "303"], ["qdel 101 202 303"]),
], ids=["empty", "single", "two", "three"])
def test_cancel_jobs(
        pbs_platform: PBSPlatform,
        monkeypatch: pytest.MonkeyPatch,
        job_ids: list[str],
        expected_commands: list[str],
) -> None:
    """Send the correct ``qdel`` command (or none) for the given job IDs.

    :param pbs_platform: PBS platform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    :param job_ids: Job IDs to cancel.
    :param expected_commands: Expected sequence of sent commands.
    """
    sent: list[str] = []
    monkeypatch.setattr(pbs_platform, "send_command", lambda cmd, **_: sent.append(cmd))

    pbs_platform.cancel_jobs(job_ids)

    assert sent == expected_commands


@pytest.mark.parametrize("job_names,expect_empty", [
    ([], True),
    (["job_a"], False),
    (["job_a", "job_b"], False),
    (["job_a", "job_b", "job_c"], False),
], ids=["empty-list", "single", "two", "three"])
def test_get_job_names_cmd(
        pbs_platform: PBSPlatform,
        job_names: list[str],
        expect_empty: bool,
) -> None:
    """Return empty string for no names; otherwise a single qstat call with pipe-joined filter.

    :param pbs_platform: PBS platform under test.
    :param job_names: Job names to query.
    :param expect_empty: Whether an empty string is expected.
    """
    cmd = pbs_platform._get_job_names_cmd(job_names)

    if expect_empty:
        assert cmd == ""
    else:
        assert "qstat" in cmd
        assert "|".join(job_names) in cmd
        assert "awk" in cmd
        assert cmd.count("qstat") == 1


def test_process_ready_jobs_valid_packages_to_submit(
        pbs_platform: PBSPlatform,
        monkeypatch: pytest.MonkeyPatch,
        create_packages,
) -> None:
    """Assign submitted job IDs to packages in submission order.

    :param pbs_platform: PBS platform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    :param create_packages: Pre-built job packages fixture.
    """
    jobs_id = [1, 2, 3]
    scripts_to_submit = OrderedDict([
        ("dummy-1.cmd", create_packages[0]),
        ("dummy-1-2-3.cmd", create_packages[1]),
        ("dummy-1-2-3-b.cmd", create_packages[2]),
    ])

    monkeypatch.setattr(pbs_platform, "submit_multiple_jobs", lambda _: jobs_id)
    monkeypatch.setattr(pbs_platform, "_check_and_cancel_duplicated_job_names", lambda _: None)

    pbs_platform.process_ready_jobs(scripts_to_submit)

    for i, package in enumerate(create_packages):
        for job in package.jobs:
            assert job.id == jobs_id[i]
            assert job.status == Status.SUBMITTED


def test_process_ready_jobs_assigns_string_job_ids(
        pbs_platform: PBSPlatform,
        monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Assign package identifiers as strings when the scheduler returns integers.

    :param pbs_platform: PBS platform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    """
    assigned_job_ids: list[str] = []

    class DummyPackage:
        """Capture the package job identifier assigned by the platform."""

        def process_jobs_to_submit(self, job_id: str) -> None:
            """Store the assigned job identifier."""
            assigned_job_ids.append(job_id)

    scripts_to_submit = OrderedDict([
        ("job_a.cmd", DummyPackage()),
        ("job_b.cmd", DummyPackage()),
    ])
    monkeypatch.setattr(pbs_platform, "submit_multiple_jobs", lambda _: [1116786, 1116787])
    monkeypatch.setattr(pbs_platform, "_check_and_cancel_duplicated_job_names", lambda _: None)

    pbs_platform.process_ready_jobs(scripts_to_submit)

    assert assigned_job_ids == [1116786, 1116787]


@pytest.mark.parametrize("ssh_output,expected_cancelled", [
    ("job_a:1116786\n", []),
    ("job_a:1116786,1116787\n", ["1116786"]),
], ids=["no-duplicates", "with-duplicates"])
def test_check_and_cancel_duplicated_job_names(
        pbs_platform: PBSPlatform,
        monkeypatch: pytest.MonkeyPatch,
        ssh_output: str,
        expected_cancelled: list[str],
) -> None:
    """Cancel the oldest ID only when a job name appears more than once.

    :param pbs_platform: PBS platform under test.
    :param monkeypatch: Pytest monkeypatch fixture.
    :param ssh_output: Simulated scheduler output with job-name groups.
    :param expected_cancelled: Expected list of cancelled job IDs.
    """
    monkeypatch.setattr(pbs_platform, "send_command", lambda cmd, **_: None)
    pbs_platform._ssh_output = ssh_output

    cancelled: list[str] = []
    monkeypatch.setattr(pbs_platform, "cancel_jobs", lambda ids: cancelled.extend(ids))

    pbs_platform._check_and_cancel_duplicated_job_names({"job_a.cmd": None})

    assert cancelled == expected_cancelled
