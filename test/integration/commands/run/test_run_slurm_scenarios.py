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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

"""Tests that run diverse scenarios for Autosubmit run with a ``slurm`` platform
and checks the database for expected results."""
import time
from getpass import getuser
from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING

import pytest
from ruamel.yaml import YAML

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.log.log import AutosubmitCritical
from test.integration.commands.run.conftest import _check_db_fields, _assert_exit_code, _check_files_recovered, \
    _assert_db_fields, _assert_files_recovered, run_in_thread, assert_run_results
from test.integration.conftest import AutosubmitExperimentFixture

if TYPE_CHECKING:
    from docker.models.containers import Container


# -- Tests

@pytest.mark.docker
@pytest.mark.xdist_group("slurm")
@pytest.mark.slurm
@pytest.mark.ssh
@pytest.mark.timeout(300)
@pytest.mark.parametrize("jobs_data,expected_db_entries,final_status,wrapper_type", [
    # Success
    (dedent("""\

    EXPERIMENT:
        NUMCHUNKS: '3'
    JOBS:
        job:
            SCRIPT: |
                echo "Hello World with id=Success"
                sleep 1
            PLATFORM: TEST_SLURM
            RUNNING: chunk
            wallclock: 00:01
    """), 3, "COMPLETED", "simple"),  # No wrappers, simple type

    # Success wrapper
    (dedent("""\
    EXPERIMENT:
        NUMCHUNKS: '2'
    JOBS:
        job:
            SCRIPT: |
                echo "Hello World with id=Success + wrappers"
                sleep 1
            DEPENDENCIES: job-1
            PLATFORM: TEST_SLURM
            RUNNING: chunk
            wallclock: 00:01

        job2:
            SCRIPT: |
                echo "Hello World with id=Success + wrappers"
                sleep 1
            DEPENDENCIES: job2-1
            PLATFORM: TEST_SLURM
            RUNNING: chunk
            wallclock: 00:01

    wrappers:
        wrapper:
            JOBS_IN_WRAPPER: job
            TYPE: vertical
            policy: flexible

        wrapper2:
            JOBS_IN_WRAPPER: job2
            TYPE: vertical
            policy: flexible

    """), 4, "COMPLETED", "vertical"),  # Wrappers present, vertical type

    # Failure
    (dedent("""\
    EXPERIMENT:
        NUMCHUNKS: '2'
    JOBS:
        job:
            SCRIPT: |
                sleep 2
                d_echo "Hello World with id=FAILED"
            PLATFORM: TEST_SLURM
            RUNNING: chunk
            wallclock: 00:01
            retrials: 2  

    """), (2 + 1) * 2, "FAILED", "simple"),  # No wrappers, simple type

    # Failure wrappers
    (dedent("""\
    JOBS:
        job:
            SCRIPT: |
                sleep 2
                d_echo "Hello World with id=FAILED + wrappers"
            PLATFORM: TEST_SLURM
            DEPENDENCIES: job-1
            RUNNING: chunk
            wallclock: 00:10
            retrials: 2
    wrappers:
        wrapper:
            JOBS_IN_WRAPPER: job
            TYPE: vertical
            policy: flexible

    """), (2 + 1) * 1, "FAILED", "vertical"),  # Wrappers present, vertical type

    (dedent("""\
EXPERIMENT:
    NUMCHUNKS: '2'
JOBS:
    job:
        SCRIPT: |
            echo "Hello World with id=Success + wrappers"
            sleep 1
        PLATFORM: TEST_SLURM
        RUNNING: chunk
        wallclock: 00:01

wrappers:
    wrapper:
        JOBS_IN_WRAPPER: job
        TYPE: horizontal
PLATFORMS:
    TEST_SLURM:
        ADD_PROJECT_TO_HOST: 'False'
        HOST: '127.0.0.1'
        PROJECT: 'group'
        QUEUE: 'gp_debug'
        SCRATCH_DIR: '/tmp/scratch/'
        TEMP_DIR: ''
        TYPE: 'slurm'
        USER: 'root'
        MAX_WALLCLOCK: '02:00'
        MAX_PROCESSORS: '4'
        PROCESSORS_PER_NODE: '4'
"""), 2, "COMPLETED", "horizontal")

], ids=["Success", "Success with wrapper", "Failure", "Failure with wrapper", "Success with horizontal wrapper"])
def test_run_uninterrupted(
        autosubmit_exp,
        jobs_data: str,
        expected_db_entries,
        final_status,
        wrapper_type,
        slurm_server: 'Container',
        prepare_scratch,
        general_data
):
    yaml = YAML(typ='rt')
    as_exp = autosubmit_exp(experiment_data=general_data | yaml.load(jobs_data), include_jobs=False, create=True)
    prepare_scratch(expid=as_exp.expid)
    as_conf = as_exp.as_conf
    exp_path = Path(BasicConfig.LOCAL_ROOT_DIR, as_exp.expid)
    tmp_path = Path(exp_path, BasicConfig.LOCAL_TMP_DIR)
    log_dir = tmp_path / f"LOG_{as_exp.expid}"
    as_conf.set_last_as_command('run')

    # Run the experiment
    exit_code = as_exp.autosubmit.run_experiment(expid=as_exp.expid)
    _assert_exit_code(final_status, exit_code)

    # Check and display results
    run_tmpdir = Path(as_conf.basic_config.LOCAL_ROOT_DIR)

    db_check_list = _check_db_fields(run_tmpdir, expected_db_entries, final_status, as_exp.expid, wrapper_type)
    files_check_list = _check_files_recovered(as_conf, log_dir, expected_files=expected_db_entries * 2)
    assert_run_results(db_check_list, files_check_list, run_tmpdir, as_exp.expid)


@pytest.mark.docker
@pytest.mark.slurm
@pytest.mark.ssh
@pytest.mark.timeout(300)
@pytest.mark.parametrize("jobs_data,expected_db_entries,final_status,wrapper_type", [
    # Success
    (dedent("""\
    EXPERIMENT:
        NUMCHUNKS: '3'
    JOBS:
        job:
            SCRIPT: |
                echo "Hello World with id=Success"
                sleep 1
            PLATFORM: TEST_SLURM
            RUNNING: chunk
            wallclock: 00:01
    """), 3, "COMPLETED", "simple"),  # No wrappers, simple type

    # Success wrapper
    (dedent("""\
    EXPERIMENT:
        NUMCHUNKS: '2'
    JOBS:
        job:
            SCRIPT: |
                echo "Hello World with id=Success + wrappers"
                sleep 1
            DEPENDENCIES: job-1
            PLATFORM: TEST_SLURM
            RUNNING: chunk
            wallclock: 00:01

        job2:
            SCRIPT: |
                echo "Hello World with id=Success + wrappers"
                sleep 1
            DEPENDENCIES: job2-1
            PLATFORM: TEST_SLURM
            RUNNING: chunk
            wallclock: 00:01

    wrappers:
        wrapper:
            JOBS_IN_WRAPPER: job
            TYPE: vertical
            policy: flexible

        wrapper2:
            JOBS_IN_WRAPPER: job2
            TYPE: vertical
            policy: flexible

    """), 4, "COMPLETED", "vertical"),  # Wrappers present, vertical type

    # Failure
    (dedent("""\
    EXPERIMENT:
        NUMCHUNKS: '2'
    JOBS:
        job:
            SCRIPT: |
                sleep 2
                d_echo "Hello World with id=FAILED"
            PLATFORM: TEST_SLURM
            RUNNING: chunk
            wallclock: 00:01
            retrials: 2  # In local, it started to fail at 18 retrials.

    """), (2 + 1) * 2, "FAILED", "simple"),  # No wrappers, simple type

    # Failure wrappers
    (dedent("""\
    JOBS:
        job:
            SCRIPT: |
                sleep 2
                d_echo "Hello World with id=FAILED + wrappers"
            PLATFORM: TEST_SLURM
            DEPENDENCIES: job-1
            RUNNING: chunk
            wallclock: 00:10
            retrials: 2
    wrappers:
        wrapper:
            JOBS_IN_WRAPPER: job
            TYPE: vertical
            policy: flexible

    """), (2 + 1) * 1, "FAILED", "vertical"),  # Wrappers present, vertical type

    (dedent("""\
EXPERIMENT:
    NUMCHUNKS: '2'
JOBS:
    job:
        SCRIPT: |
            echo "Hello World with id=Success + wrappers"
            sleep 1
        PLATFORM: TEST_SLURM
        RUNNING: chunk
        wallclock: 00:01

wrappers:
    wrapper:
        JOBS_IN_WRAPPER: job
        TYPE: horizontal
PLATFORMS:
    TEST_SLURM:
        ADD_PROJECT_TO_HOST: 'False'
        HOST: '127.0.0.1'
        PROJECT: 'group'
        QUEUE: 'gp_debug'
        SCRATCH_DIR: '/tmp/scratch/'
        TEMP_DIR: ''
        TYPE: 'slurm'
        USER: 'root'
        MAX_WALLCLOCK: '02:00'
        MAX_PROCESSORS: '4'
        PROCESSORS_PER_NODE: '4'
"""), 2, "COMPLETED", "horizontal")

], ids=[
    "Success",
    "Success with wrapper",
    "Failure",
    "Failure with wrapper",
    "Success with horizontal wrapper"
])
def test_run_interrupted(
        autosubmit_exp,
        jobs_data: str,
        expected_db_entries,
        final_status,
        wrapper_type,
        slurm_server: 'Container',
        prepare_scratch,
        general_data,
):
    yaml = YAML(typ='rt')
    as_exp = autosubmit_exp(experiment_data=general_data | yaml.load(jobs_data), include_jobs=False, create=True)
    prepare_scratch(expid=as_exp.expid)
    as_conf = as_exp.as_conf
    exp_path = Path(BasicConfig.LOCAL_ROOT_DIR, as_exp.expid)
    tmp_path = Path(exp_path, BasicConfig.LOCAL_TMP_DIR)
    log_dir = tmp_path / f"LOG_{as_exp.expid}"
    as_conf.set_last_as_command('run')

    # Run the experiment
    as_thread, result, stop_event = run_in_thread(
        as_exp.autosubmit.run_experiment,
        expid=as_exp.expid
    )

    time.sleep(3)

    if as_thread.is_alive():
        stop_event.set()  # signal "terminate"
        as_thread.join(timeout=2)

    assert not as_thread.is_alive(), "Autosubmit thread did not stop as expected."

    exit_code = as_exp.autosubmit.run_experiment(expid=as_exp.expid)

    # Check and display results
    run_tmpdir = Path(as_conf.basic_config.LOCAL_ROOT_DIR)

    db_check_list = _check_db_fields(run_tmpdir, expected_db_entries, final_status, as_exp.expid)
    _assert_db_fields(db_check_list)

    files_check_list = _check_files_recovered(as_conf, log_dir, expected_files=expected_db_entries * 2)
    _assert_files_recovered(files_check_list)

    _assert_exit_code(final_status, exit_code)


@pytest.mark.docker
@pytest.mark.timeout(300)
@pytest.mark.slurm
@pytest.mark.ssh
@pytest.mark.parametrize("jobs_data,final_status", [
    (dedent("""\
EXPERIMENT:
    NUMCHUNKS: 2
PROJECT:
    PROJECT_TYPE: local
    PROJECT_DIRECTORY: local_project
LOCAL:
    PROJECT_PATH: "tofill"
JOBS:
    job:
        FILE: 
            - "test.sh"
            - "additional1.sh"
            - "additional2.sh"
        PLATFORM: TEST_SLURM
        RUNNING: chunk
        wallclock: 00:01
PLATFORMS:
    TEST_SLURM:
        ADD_PROJECT_TO_HOST: 'False'
        HOST: '127.0.0.1'
        MAX_WALLCLOCK: '00:03'
        PROJECT: 'group'
        QUEUE: 'gp_debug'
        SCRATCH_DIR: '/tmp/scratch/'
        TEMP_DIR: ''
        TYPE: 'slurm'
        USER: 'root'
    """), "COMPLETED"),
    (dedent("""\
PROJECT:
    PROJECT_TYPE: local
    PROJECT_DIRECTORY: local_project
LOCAL:
    PROJECT_PATH: "tofill"
JOBS:
    job:
        FILE: 
            - "test.sh"
            - "additional1.sh"
            - "thisdoesntexists.sh"
        PLATFORM: TEST_SLURM
        DEPENDENCIES:
          job-1:
        RUNNING: chunk
        wallclock: 00:01
PLATFORMS:
    TEST_SLURM:
        ADD_PROJECT_TO_HOST: 'False'
        HOST: '127.0.0.1'
        MAX_WALLCLOCK: '00:03'
        PROJECT: 'group'
        QUEUE: 'gp_debug'
        SCRATCH_DIR: '/tmp/scratch/'
        TEMP_DIR: ''
        TYPE: 'slurm'
        USER: 'root'
"""), "FAILED"),
], ids=["All files exist", "One file missing"])
@pytest.mark.parametrize("include_wrappers", [False, True], ids=["no_wrappers", "wrappers"])
def test_run_with_additional_files(
        jobs_data: str,
        final_status: str,
        include_wrappers: bool,
        autosubmit_exp,
        slurm_server: 'Container',
        tmp_path,
):
    yaml = YAML(typ='rt')
    project_path = Path(tmp_path) / "org_templates"
    jobs_data = jobs_data.replace("tofill", str(project_path))
    project_path.mkdir(parents=True, exist_ok=True)

    (project_path / "test.sh").write_text('echo "main script."\n')
    (project_path / "additional1.sh").write_text('echo "additional file 1."\n')
    (project_path / "additional2.sh").write_text('echo "additional file 2."\n')

    experiment_data_yaml = yaml.load(jobs_data)
    if include_wrappers:
        wrappers_dict = {
            "WRAPPERS": {
                "WRAPPER": {
                    "JOBS_IN_WRAPPER": "JOB",
                    "TYPE": "vertical",
                }
            },
        }
        experiment_data_yaml.update(wrappers_dict)

    as_exp = autosubmit_exp(experiment_data=experiment_data_yaml, include_jobs=False, create=True)
    as_exp.as_conf.set_last_as_command('run')

    if final_status == "FAILED":
        with pytest.raises(AutosubmitCritical):
            as_exp.autosubmit.run_experiment(expid=as_exp.expid)
    else:
        exit_code = as_exp.autosubmit.run_experiment(expid=as_exp.expid)
        _assert_exit_code(final_status, exit_code)
        project_remote_path = f"/tmp/scratch/group/root/{as_exp.expid}/LOG_{as_exp.expid}"
        for additional_filename in ["additional1.sh", "additional2.sh"]:
            for chunk in range(1, 1 + experiment_data_yaml.get("EXPERIMENT", {}).get("NUMCHUNKS", 1)):
                remote_name = additional_filename.replace(".sh", f'_20000101_fc0_{chunk}_JOB')
                command = f"cat {project_remote_path}/{remote_name}"
                exit_code, output = slurm_server.exec_run(["bash", "-c", command])
                assert exit_code == 0, f"File {additional_filename} not found in remote project path."


@pytest.mark.docker
@pytest.mark.slurm
@pytest.mark.ssh
@pytest.mark.timeout(300)
@pytest.mark.parametrize("wrappers, run_type", [
    (
            {
                "WRAPPERS": {
                    "MAX_WRAPPED": 2,
                    "WRAPPER": {"JOBS_IN_WRAPPER": "job_some", "TYPE": "horizontal"},
                    "SECOND_WRAPPER": {"JOBS_IN_WRAPPER": "other_some", "TYPE": "horizontal"},
                }
            },
            "run",
    ),
    (
            {
                "WRAPPERS": {
                    "WRAPPER": {
                        "JOBS_IN_WRAPPER": "job_some",
                        "TYPE": "horizontal",
                        "MAX_WRAPPED": 2,
                    },
                    "SECOND_WRAPPER": {
                        "JOBS_IN_WRAPPER": "other_some",
                        "TYPE": "horizontal",
                        "MAX_WRAPPED": 2,
                    },
                }
            },
            "run",
    ),
    (
            {
                "WRAPPERS": {
                    "MAX_WRAPPED": 2,
                    "WRAPPER": {"JOBS_IN_WRAPPER": "job_some", "TYPE": "horizontal"},
                    "SECOND_WRAPPER": {"JOBS_IN_WRAPPER": "other_some", "TYPE": "horizontal"},
                }
            },
            "inspect",
    ),
    (
            {
                "WRAPPERS": {
                    "WRAPPER": {
                        "JOBS_IN_WRAPPER": "job_some",
                        "TYPE": "horizontal",
                        "MAX_WRAPPED": 2,
                    },
                    "SECOND_WRAPPER": {
                        "JOBS_IN_WRAPPER": "other_some",
                        "TYPE": "horizontal",
                        "MAX_WRAPPED": 2,
                    },
                }
            },
            "inspect",
    ),
    (
            {
                "WRAPPERS": {
                    "WRAPPER": {
                        "JOBS_IN_WRAPPER": "job_some&other_some",
                        "TYPE": "horizontal-vertical",
                        "MAX_WRAPPED": 2,
                        "MIN_WRAPPED": 1
                    },
                }
            },
            "&inspect",
    ),
    (
            {
                "WRAPPERS": {
                    "WRAPPER": {
                        "JOBS_IN_WRAPPER": "job_some&other_some",
                        "TYPE": "horizontal-vertical",
                        "MAX_WRAPPED": 2,
                        "MIN_WRAPPED": 1
                    },
                }
            },
            "quick-inspect",
    ),
    (
            {
                "WRAPPERS": {
                    "WRAPPER": {
                        "JOBS_IN_WRAPPER": "[1,2,3,4]",
                        "TYPE": "horizontal-vertical",
                        "MAX_WRAPPED": 2,
                        "MIN_WRAPPED": 1
                    },
                }
            },
            "invalid-inspect1",
    ),
    (
            {
                "WRAPPERS": {
                    "WRAPPER": {
                        "JOBS_IN_WRAPPER": "[&]",
                        "TYPE": "horizontal-vertical",
                        "MAX_WRAPPED": 2,
                        "MIN_WRAPPED": 1
                    },
                }
            },
            "invalid-inspect2",
    ),
    (
            {
                "WRAPPERS": {
                    "WRAPPER": {
                        "JOBS_IN_WRAPPER": "&",
                        "TYPE": "horizontal-vertical",
                        "MAX_WRAPPED": 2,
                        "MIN_WRAPPED": 1
                    },
                }
            },
            "invalid-inspect3",
    ),
    (
            {
                "WRAPPERS": {
                    "WRAPPER": {
                        "JOBS_IN_WRAPPER": ",",
                        "TYPE": "horizontal-vertical",
                        "MAX_WRAPPED": 2,
                        "MIN_WRAPPED": 1
                    },
                }
            },
            "invalid-inspect4",
    ),
    (
            {
                "WRAPPERS": {
                    "WRAPPER": {
                        "JOBS_IN_WRAPPER": "",
                        "TYPE": "horizontal-vertical",
                        "MAX_WRAPPED": 2,
                        "MIN_WRAPPED": 1
                    },
                }
            },
            "invalid-inspect-empty-string",
    ),
    (
            {
                "WRAPPERS": {
                    "WRAPPER": {
                        "JOBS_IN_WRAPPER": "[',']",  # it is empty, because the "," is stripped somewhere
                        "TYPE": "horizontal-vertical",
                        "MAX_WRAPPED": 2,
                        "MIN_WRAPPED": 1
                    },
                }
            },
            "invalid-inspect6-empty",
    ),
    (
            {
                "WRAPPERS": {
                    "WRAPPER": {
                        "JOBS_IN_WRAPPER": "['&']",
                        "TYPE": "horizontal-vertical",
                        "MAX_WRAPPED": 2,
                        "MIN_WRAPPED": 1
                    },
                }
            },
            "invalid-inspect8",
    ),
    (
            {
                "WRAPPERS": {
                    "WRAPPER": {
                        "JOBS_IN_WRAPPER": [],
                        "TYPE": "horizontal-vertical",
                        "MAX_WRAPPED": 2,
                        "MIN_WRAPPED": 1
                    },
                }
            },
            "invalid-inspect-empty-list",
    ),
])
def test_wrapper_config(
        wrappers: dict,
        run_type: str,
        autosubmit_exp,
        slurm_server: 'Container',
        tmp_path,
):
    experiment_data = {
        "EXPERIMENT": {"MEMBERS": "fc0 fc1 fc2 fc3"},
        "PROJECT": {"PROJECT_TYPE": "None", "PROJECT_DIRECTORY": "local_project"},
        "JOBS": {
            "job_some": {
                "SCRIPT": "echo 'Hello World'",
                "PLATFORM": "TEST_SLURM",
                "RUNNING": "member",
                "wallclock": "00:01",
            },
            "other_some": {
                "SCRIPT": "echo 'Hello World'",
                "PLATFORM": "TEST_SLURM",
                "RUNNING": "member",
                "wallclock": "00:01",
            },
        },
        "PLATFORMS": {
            "TEST_SLURM": {
                "ADD_PROJECT_TO_HOST": "False",
                "HOST": "127.0.0.1",
                "MAX_WALLCLOCK": "00:03",
                "PROJECT": "group",
                "QUEUE": "gp_debug",
                "SCRATCH_DIR": "/tmp/scratch/",
                "TEMP_DIR": "",
                "TYPE": "slurm",
                "USER": "root",
                "PROCESSORS_PER_NODE": "4",
                "MAX_PROCESSORS": "4",
            }
        },
    }

    if run_type.startswith("invalid"):
        with pytest.raises(AutosubmitCritical):
            autosubmit_exp(experiment_data=experiment_data | wrappers, include_jobs=False, create=True,
                           check_wrappers=True)
    else:
        as_exp = autosubmit_exp(experiment_data=experiment_data | wrappers, include_jobs=False, create=True)

        if run_type == "run":
            as_exp.as_conf.set_last_as_command('run')
            as_exp.autosubmit.run_experiment(expid=as_exp.expid)
        else:
            as_exp.as_conf.set_last_as_command('inspect')
            as_exp.autosubmit.inspect(
                expid=as_exp.expid,
                lst=None,  # type: ignore
                check_wrapper=True,
                force=True,
                filter_chunks=None,  # type: ignore
                filter_section=None,  # type: ignore
                filter_status=None,  # type: ignore
                quick=True if run_type == "quick-inspect" else False
            )
        templates_dir = Path(tmp_path) / as_exp.expid / "tmp"
        asthread_files = list(templates_dir.rglob("*ASThread*"))
        if run_type == "run" or run_type == "inspect":
            # 8 jobs in total, 2 wrappers with max 2 jobs each -> 4 ASThread files expected
            assert len(asthread_files) == 2 + 2


_MULTIPLE_VERTICAL_WRAPPERS_PARAMS = [
    # Four vertical wrappers (three succeed, one fails) + two bare jobs (one success, one fail) — 3 chunks each
    (dedent("""\
    EXPERIMENT:
        NUMCHUNKS: '3'
    JOBS:
        # --- wrapped jobs ---
        job_success1:
            SCRIPT: |
                echo "Hello from wrapped success job 1 chunk=%CHUNK%"
                sleep 1
            DEPENDENCIES: job_success1-1
            PLATFORM: TEST_SLURM
            RUNNING: chunk
            wallclock: 00:01

        job_success2:
            SCRIPT: |
                echo "Hello from wrapped success job 2 chunk=%CHUNK%"
                sleep 1
            DEPENDENCIES: job_success2-1
            PLATFORM: TEST_SLURM
            RUNNING: chunk
            wallclock: 00:01

        job_success3:
            SCRIPT: |
                echo "Hello from wrapped success job 3 chunk=%CHUNK%"
                sleep 1
            DEPENDENCIES: job_success3-1
            PLATFORM: TEST_SLURM
            RUNNING: chunk
            wallclock: 00:01

        job_wrapped_fail:
            SCRIPT: |
                sleep 2
                d_echo "Hello from wrapped failing job chunk=%CHUNK%"
            DEPENDENCIES: job_wrapped_fail-1
            PLATFORM: TEST_SLURM
            RUNNING: chunk
            wallclock: 00:10
            retrials: 1

        # --- unwrapped jobs ---
        job_nowrap_success:
            SCRIPT: |
                echo "Hello from bare success job chunk=%CHUNK%"
                sleep 1
            PLATFORM: TEST_SLURM
            RUNNING: chunk
            wallclock: 00:01

        job_nowrap_fail:
            SCRIPT: |
                sleep 2
                d_echo "Hello from bare failing job chunk=%CHUNK%"
            PLATFORM: TEST_SLURM
            RUNNING: chunk
            wallclock: 00:10
            retrials: 1

    wrappers:
        wrapper_success1:
            JOBS_IN_WRAPPER: job_success1
            TYPE: vertical
            policy: flexible

        wrapper_success2:
            JOBS_IN_WRAPPER: job_success2
            TYPE: vertical
            policy: flexible

        wrapper_success3:
            JOBS_IN_WRAPPER: job_success3
            TYPE: vertical
            policy: flexible

        wrapper_fail:
            JOBS_IN_WRAPPER: job_wrapped_fail
            TYPE: vertical
            policy: flexible

    """),
     # wrapped success:  3+3+3 chunks (1 entry each)
     # wrapped fail:     3 chunks * (1 retrial + 1) entries
     # bare success:     3 chunks (1 entry each)
     # bare fail:        3 chunks * (1 retrial + 1) entries
     3 + 3 + 3 + (1 + 1) * 3 + 3 + (1 + 1) * 3,
     "FAILED",
     "vertical"),
]

_MULTIPLE_VERTICAL_WRAPPERS_IDS = [
    "Four vertical wrappers (3 success + 1 fail) plus two bare jobs (1 success + 1 fail)",
]


@pytest.mark.docker
@pytest.mark.xdist_group("slurm")
@pytest.mark.slurm
@pytest.mark.ssh
@pytest.mark.timeout(300)
@pytest.mark.parametrize(
    "jobs_data,expected_db_entries,final_status,wrapper_type",
    _MULTIPLE_VERTICAL_WRAPPERS_PARAMS,
    ids=_MULTIPLE_VERTICAL_WRAPPERS_IDS,
)
def test_run_uninterrupted_multiple_vertical_wrappers(
        autosubmit_exp: 'AutosubmitExperimentFixture',
        jobs_data: str,
        expected_db_entries: int,
        final_status: str,
        wrapper_type: str,
        slurm_server: 'Container',
        prepare_scratch,
        general_data: dict,
) -> None:
    """
    Test ``autosubmit run`` (uninterrupted) with multiple vertical wrappers that mix
    successful and failing jobs, verifying database entries and recovered log files.

    :param autosubmit_exp: Fixture that creates and manages an Autosubmit experiment.
    :type autosubmit_exp: AutosubmitExperimentFixture
    :param jobs_data: YAML string with the experiment jobs/wrappers configuration.
    :type jobs_data: str
    :param expected_db_entries: Expected total number of rows in ``job_data`` table.
    :type expected_db_entries: int
    :param final_status: Expected final experiment status (``'COMPLETED'`` or ``'FAILED'``).
    :type final_status: str
    :param wrapper_type: Wrapper type used in this scenario (e.g. ``'vertical'``).
    :type wrapper_type: str
    :param slurm_server: Docker container running the Slurm scheduler.
    :type slurm_server: Container
    :param prepare_scratch: Fixture that sets up the scratch directory for the experiment.
    :param general_data: Common experiment configuration shared across tests.
    :type general_data: dict
    """
    yaml = YAML(typ='rt')
    as_exp = autosubmit_exp(experiment_data=general_data | yaml.load(jobs_data), include_jobs=False, create=True)
    prepare_scratch(expid=as_exp.expid)
    as_conf = as_exp.as_conf
    exp_path = Path(BasicConfig.LOCAL_ROOT_DIR, as_exp.expid)
    tmp_path = Path(exp_path, BasicConfig.LOCAL_TMP_DIR)
    log_dir = tmp_path / f"LOG_{as_exp.expid}"
    as_conf.set_last_as_command('run')

    exit_code = as_exp.autosubmit.run_experiment(expid=as_exp.expid)
    _assert_exit_code(final_status, exit_code)

    run_tmpdir = Path(as_conf.basic_config.LOCAL_ROOT_DIR)
    db_check_list = _check_db_fields(run_tmpdir, expected_db_entries, final_status, as_exp.expid, wrapper_type)
    files_check_list = _check_files_recovered(as_conf, log_dir, expected_files=expected_db_entries * 2)
    assert_run_results(db_check_list, files_check_list, run_tmpdir, as_exp.expid)


@pytest.mark.docker
@pytest.mark.xdist_group("slurm")
@pytest.mark.slurm
@pytest.mark.ssh
@pytest.mark.timeout(300)
@pytest.mark.parametrize(
    "jobs_data,expected_db_entries,final_status,wrapper_type",
    _MULTIPLE_VERTICAL_WRAPPERS_PARAMS,
    ids=_MULTIPLE_VERTICAL_WRAPPERS_IDS,
)
def test_run_interrupted_multiple_vertical_wrappers(
        autosubmit_exp: 'AutosubmitExperimentFixture',
        jobs_data: str,
        expected_db_entries: int,
        final_status: str,
        wrapper_type: str,
        slurm_server: 'Container',
        prepare_scratch,
        general_data: dict,
) -> None:
    """
    Test ``autosubmit run`` (interrupted mid-run then resumed) with multiple vertical
    wrappers that mix successful and failing jobs, verifying database entries and
    recovered log files after the full run completes.

    :param autosubmit_exp: Fixture that creates and manages an Autosubmit experiment.
    :type autosubmit_exp: AutosubmitExperimentFixture
    :param jobs_data: YAML string with the experiment jobs/wrappers configuration.
    :type jobs_data: str
    :param expected_db_entries: Expected total number of rows in ``job_data`` table.
    :type expected_db_entries: int
    :param final_status: Expected final experiment status (``'COMPLETED'`` or ``'FAILED'``).
    :type final_status: str
    :param wrapper_type: Wrapper type used in this scenario (e.g. ``'vertical'``).
    :type wrapper_type: str
    :param slurm_server: Docker container running the Slurm scheduler.
    :type slurm_server: Container
    :param prepare_scratch: Fixture that sets up the scratch directory for the experiment.
    :param general_data: Common experiment configuration shared across tests.
    :type general_data: dict
    """
    yaml = YAML(typ='rt')
    as_exp = autosubmit_exp(experiment_data=general_data | yaml.load(jobs_data), include_jobs=False, create=True)
    prepare_scratch(expid=as_exp.expid)
    as_conf = as_exp.as_conf
    exp_path = Path(BasicConfig.LOCAL_ROOT_DIR, as_exp.expid)
    tmp_path = Path(exp_path, BasicConfig.LOCAL_TMP_DIR)
    log_dir = tmp_path / f"LOG_{as_exp.expid}"
    as_conf.set_last_as_command('run')

    # First run: interrupt after 3 seconds
    as_thread, result, stop_event = run_in_thread(
        as_exp.autosubmit.run_experiment,
        expid=as_exp.expid
    )

    time.sleep(3)

    if as_thread.is_alive():
        stop_event.set()
        as_thread.join(timeout=2)

    assert not as_thread.is_alive(), "Autosubmit thread did not stop as expected."

    # Second run: resume until completion
    exit_code = as_exp.autosubmit.run_experiment(expid=as_exp.expid)
    _assert_exit_code(final_status, exit_code)

    run_tmpdir = Path(as_conf.basic_config.LOCAL_ROOT_DIR)
    db_check_list = _check_db_fields(run_tmpdir, expected_db_entries, final_status, as_exp.expid, wrapper_type)
    files_check_list = _check_files_recovered(as_conf, log_dir, expected_files=expected_db_entries * 2)
    assert_run_results(db_check_list, files_check_list, run_tmpdir, as_exp.expid)


def test_inspect_wrappers(tmp_path, autosubmit_exp: 'AutosubmitExperimentFixture'):
    """Test inspect with wrappers."""
    user = getuser()
    exp = autosubmit_exp(experiment_data={
        'DEFAULT': {
            'HPCARCH': 'TEST_PS'
        },
        'PLATFORMS': {
            'TEST_PS': {
                'CUSTOM_DIR': 'test',
                'CUSTOM_DIR_POINTS_TO_OTHER_DIR': '%TEST_REFERENCE%',
                'TYPE': 'ps',
                'HOST': 'localhost',
                'USER': user,
                'SCRATCH_DIR': str(tmp_path),
                'MAX_WALLCLOCK': '00:30'
            }
        },
        'JOBS': {
            'A': {
                'SCRIPT': 'echo "Hello World"',
                'RUNNING': 'once',
                'PLATFORM': 'TEST_PS'
            }
        },
        'WRAPPERS': {
            'MIN_WRAPPED': 1,
            'TEST_WRAPPER': {
                'TYPE': 'vertical',
                'JOBS_IN_WRAPPER': 'A'
            }
        }
    }, include_jobs=False, create=True)
    exp.as_conf.set_last_as_command('inspect')

    # Inspect
    exp.autosubmit.inspect(
        expid=exp.expid,
        lst=None,  # type: ignore
        check_wrapper=True,
        force=True,
        filter_chunks=None,  # type: ignore
        filter_section=None,  # type: ignore
        filter_status=None,  # type: ignore
        quick=True
    )

    templates_dir = Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR) / exp.expid / BasicConfig.LOCAL_TMP_DIR
    templates_generated = [t for t in templates_dir.glob(f"{exp.expid}*.cmd")]

    assert len(templates_generated) == 1


@pytest.mark.ssh
@pytest.mark.slurm
@pytest.mark.docker
@pytest.mark.parametrize("jobs_data,final_status", [
    # Failure
    (dedent("""\
    CONFIG:
        SAFETYSLEEPTIME: 0
    EXPERIMENT:
        NUMCHUNKS: '2'
    JOBS:
        job:
            SCRIPT: |
                echo "Hello World with id=Success"
            PLATFORM: TEST_SLURM
            RUNNING: chunk
            wallclock: 00:01
            retrials: 1  
    """), "Success"),
], ids=["Create -> Run Completed -> Create -> Run Completed"])
def test_rerun_expid(
        autosubmit_exp,
        general_data,
        jobs_data,
        final_status,
        slurm_server
):
    yaml = YAML(typ='rt')
    jobs_data_yaml = yaml.load(jobs_data)
    as_exp = autosubmit_exp(experiment_data=general_data | jobs_data_yaml, include_jobs=False, create=True)
    as_conf = as_exp.as_conf
    as_conf.set_last_as_command('run')

    exit_code = as_exp.autosubmit.run_experiment(as_exp.expid)
    _assert_exit_code(final_status, exit_code)
    _assert_exit_code("SUCCESS", exit_code)

    as_exp.autosubmit.create(as_exp.expid, noplot=True, hide=False, force=True, check_wrappers=False)
    exit_code = as_exp.autosubmit.run_experiment(as_exp.expid)
    _assert_exit_code("SUCCESS", exit_code)


@pytest.mark.ssh
@pytest.mark.slurm
@pytest.mark.docker
@pytest.mark.parametrize("jobs_data,final_status", [
    (dedent("""\
    CONFIG:
        SAFETYSLEEPTIME: 0
    EXPERIMENT:
        NUMCHUNKS: '2'
    JOBS:
        job:
            SCRIPT: |
                print("Hello World with id=Success")
            PLATFORM: TEST_SLURM
            RUNNING: chunk
            wallclock: 00:01
            retrials: 1
            type: python3
    """), "COMPLETED"),
    (dedent("""\
    CONFIG:
        SAFETYSLEEPTIME: 0
    EXPERIMENT:
        NUMCHUNKS: '2'
    JOBS:
        job:
            SCRIPT: |
                raise RuntimeError("Intentional failure")
            PLATFORM: TEST_SLURM
            RUNNING: chunk
            wallclock: 00:01
            retrials: 1
            type: python3
    """), "FAILED"),
], ids=["Python3 script succeeds", "Python3 script fails"])
def test_run_python3_job(
        autosubmit_exp,
        general_data,
        jobs_data,
        final_status,
        slurm_server
):
    """Run a job using python3 script type and assert the expected final status."""
    yaml = YAML(typ='rt')
    jobs_data_yaml = yaml.load(jobs_data)
    as_exp = autosubmit_exp(experiment_data=general_data | jobs_data_yaml, include_jobs=False, create=True)
    as_conf = as_exp.as_conf
    as_conf.set_last_as_command('run')

    exit_code = as_exp.autosubmit.run_experiment(as_exp.expid)
    _assert_exit_code(final_status, exit_code)
