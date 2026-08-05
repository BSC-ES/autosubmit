# Copyright 2015-2026 Earth Sciences Department, BSC-CNS

# This file is part of Autosubmit.

# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.
import os
from pathlib import Path
import re
from typing import Any

import pytest

from autosubmit.profiler.profiler import Profiler

# https://github.com/BSC-ES/autosubmit/issues/1332


def _scenario_id(members: str, chunks: str, splits: str, filter_type: str | None = None) -> str:
    """Return the abbreviated scenario id shown in the reports and plots."""
    base = f"{len(members.split())}m/{chunks}c/{splits}s"
    return f"{base}·{filter_type}" if filter_type else base


def prepare_setstatus_recovery(as_exp, tmp_path: Path, job_names_to_recover, slurm_server: Any) -> Any:
    """Generates some completed and stat files in the scratch directory to simulate completed jobs.

    :param as_exp: The Autosubmit experiment object.
    :param tmp_path: The temporary path for the experiment.
    :param job_names_to_recover: The list of job names to recover.
    :param slurm_server: The SLURM server container.
    :type as_exp: Any
    :type tmp_path: Path
    :type job_names_to_recover: Any
    :type slurm_server: Any
    """
    slurm_root = f"/tmp/scratch/group/root/{as_exp.expid}/"
    log_dir = Path(slurm_root) / f'LOG_{as_exp.expid}/'
    local_completed_dir = tmp_path / as_exp.expid / "tmp" / f'LOG_{as_exp.expid}/'
    # combining this with the touch, makes the touch generates a folder instead of a file. I have no idea why.
    slurm_server.exec_run(f'mkdir -p {log_dir}')

    cmds = []
    for name in job_names_to_recover:
        if "LOCAL" in name:
            local_completed_dir.mkdir(parents=True, exist_ok=True)
            (local_completed_dir / f"{name}_COMPLETED").touch()
        else:
            cmds.append(f'touch {log_dir}/{name}_COMPLETED')
    full_cmd = " && ".join(cmds)
    slurm_server.exec_run(full_cmd)


def prepare_yml(members, chunks, splits) -> dict:
    """Fixture to prepare a jobs.yml file for testing."""
    return {
        "CONFIG": {
            'MAXWAITINGJOBS': 1000,
            'TOTALJOBS': 1000,
            'SAFETYSLEEPTIME': 0,
        },
        "DEFAULT": {
            "HPCARCH": "TEST_SLURM",
        },
        "EXPERIMENT": {
            "MEMBERS": members,
            "CHUNKSIZEUNIT": "month",
            "SPLITSIZEUNIT": "day",
            "CHUNKSIZE": "1",
            "NUMCHUNKS": chunks,
            "CALENDAR": "standard",
            "DATELIST": "20200101",
        },
        "PLATFORMS": {
            "TEST_SLURM": {
                "TYPE": "slurm",
                "HOST": "127.0.0.1",
                "PROJECT": "group",
                "QUEUE": "gp_debug",
                "SCRATCH_DIR": "/tmp/scratch/",
                "USER": "root",
                "MAX_WALLCLOCK": "02:00",
                "MAX_PROCESSORS": "4",
                "PROCESSORS_PER_NODE": "4",
            }
        },
        "JOBS": {
            "LOCAL_SETUP": {
                "SCRIPT": "sleep 0",
                "RUNNING": "once",
                "CHECK": "on_submission",
            },
            "SYNCHRONIZE": {
                "SCRIPT": "sleep 0",
                "DEPENDENCIES": {"LOCAL_SETUP": {}},
                "RUNNING": "once",
                "CHECK": "on_submission",
            },
            "REMOTE_SETUP": {
                "SCRIPT": "sleep 0",
                "DEPENDENCIES": {"SYNCHRONIZE": {}},
                "RUNNING": "once",
                "CHECK": "on_submission",
            },
            "DN": {
                "SCRIPT": "sleep 0",
                "DEPENDENCIES": {
                    "REMOTE_SETUP": {},
                    "DN": {"SPLITS_FROM": {"ALL": {"SPLITS_TO": "previous"}}},
                    "DN-1": {},
                },
                "RUNNING": "chunk",
                "CHECK": "on_submission",
                "SPLITS": splits,
            },
            "OPA_ENERGY_INDICATORS": {
                "DEPENDENCIES": {
                    "DN": {"SPLITS_FROM": {"ALL": {"SPLITS_TO": "[1:auto]*\\1"}}},
                    "OPA_ENERGY_INDICATORS": {
                        "SPLITS_FROM": {"ALL": {"SPLITS_TO": "previous"}}
                    },
                    "OPA_ENERGY_INDICATORS-1": {},
                },
                "SCRIPT": "sleep 0",
                "RUNNING": "chunk",
                "CHECK": "on_submission",
                "SPLITS": splits,
            },
            "APP_ENERGY_INDICATORS": {
                "SCRIPT": "sleep 0",
                "RUNNING": "chunk",
                "CHECK": "on_submission",
                "DEPENDENCIES": {
                    "OPA_ENERGY_INDICATORS": {
                        "SPLITS_FROM": {"ALL": {"SPLITS_TO": "[1:auto]*\\1"}}
                    },
                    "OPA_ENERGYTDIG2": {
                        "STATUS": "FAILED",
                        "SPLITS_FROM": {"ALL": {"SPLITS_TO": "[1:auto]*\\1"}},
                        "ANY_FINAL_STATUS_IS_VALID": False,
                    },
                    "OPA_ENERGYTDIG1": {
                        "STATUS": "FAILED",
                        "SPLITS_FROM": {"ALL": {"SPLITS_TO": "[1:auto]*\\1"}},
                        "ANY_FINAL_STATUS_IS_VALID": False,
                    },
                    "APP_ENERGY_INDICATORS": {
                        "SPLITS_FROM": {"ALL": {"SPLITS_TO": "previous"}}
                    },
                    "APP_ENERGY_INDICATORS-1": {},
                },
                "SPLITS": splits,
            },
            "OPA_ENERGYTDIG1": {
                "DEPENDENCIES": {
                    "DN": {"SPLITS_FROM": {"ALL": {"SPLITS_TO": "[1:auto]*\\1"}}},
                    "OPA_ENERGYTDIG1": {
                        "SPLITS_FROM": {"ALL": {"SPLITS_TO": "previous"}}
                    },
                    "OPA_ENERGYTDIG1-1": {"STATUS": "FAILED?"},
                },
                "SCRIPT": "sleep 0",
                "RUNNING": "chunk",
                "CHECK": "on_submission",
                "SPLITS": splits,
            },
        }
    }


def _find(pattern: str, text: str, default: Any = None) -> Any:
    """Return the first regex capture group match, or the default value."""
    match = re.search(pattern, text)
    return match.group(1) if match else default


def _to_mib(value: str, unit: str) -> float:
    """Convert a profiler size value expressed in ``unit`` to MiB."""
    amount = float(value)
    if unit == "KiB":
        return amount / 1024
    if unit == "MiB":
        return amount
    if unit == "GiB":
        return amount * 1024
    if unit == "TiB":
        return amount * 1024 * 1024
    if unit == "PiB":
        return amount * 1024 * 1024 * 1024
    return amount / (1024 * 1024)  # B


def _collect_profiler_metrics(as_exp: Any, test_type: str, run_id: str, tmp_path: Path) -> dict:
    """Extract the profiler metrics from the latest profile report.

    The metrics are stored per-run in the pytest-benchmark ``extra_info`` so the
    comparison scripts can evaluate them together with the wall-clock stats.

    :param as_exp: The Autosubmit experiment object.
    :param test_type: The type of run (e.g., 'create', 'run', 'recovery').
    :param run_id: Unique identifier for the test run.
    :param tmp_path: The temporary path for the experiment.
    :rtype: dict
    :return: A dictionary with the profiler metrics.
    """
    profile_path = tmp_path / as_exp.expid / "tmp" / "profile"

    metric_files = list(profile_path.glob("*.txt"))
    if not metric_files:
        pytest.fail("No profile files found")
    metric_files.sort(key=os.path.getmtime)
    text = metric_files[-1].read_text(encoding="UTF-8")

    job_list = as_exp.autosubmit.load_job_list(as_exp.expid, as_exp.as_conf, new=False, full_load=True)
    total_dependencies = len(job_list.graph.edges)
    total_jobs = len(job_list.graph.nodes)

    memory_match = re.search(r"FINAL MEMORY: (\d+\.\d+) ([A-Za-z]+)\.", text)
    memory_consumption = _to_mib(memory_match.group(1), memory_match.group(2)) if memory_match else None

    # Disk usage (sqlite only for now)
    db_path = Path(tmp_path / as_exp.expid / "db" / "job_list.db")
    metadata_db = Path(tmp_path / "metadata" / "data" / f"job_data_{as_exp.expid}.db")

    if db_path.exists():
        db_size = db_path.stat().st_size / (1024 * 1024)  # in MiB
    else:
        db_size = 0

    if metadata_db.exists():
        metadata_size = metadata_db.stat().st_size / (1024 * 1024)  # in MiB
    else:
        metadata_size = 0

    fd_grow = _find(r"FILE DESCRIPTORS GROW: (\d+)", text)
    mem_grow_match = re.search(r"MEMORY GROW: (-?\d+\.\d+) ([A-Za-z]+)\.", text)
    if mem_grow_match:
        mem_grow = _to_mib(mem_grow_match.group(1), mem_grow_match.group(2))
    else:
        mem_grow = None
    obj_grow = _find(r"OBJECTS GROW: (\d+)", text)

    return {
        "test type": test_type,
        "ID": run_id,
        "Memory consumption(MiB)": float(memory_consumption) if memory_consumption else None,
        "Historical DB Disk Usage(MiB)": float(metadata_size),
        "Job list DB Usage": float(db_size),
        "Total Jobs": total_jobs,
        "Total Dependencies": total_dependencies,
        "FD GROW": fd_grow,
        "MEM GROW(MIB)": mem_grow,
        "OBJ GROW": obj_grow,
    }


@pytest.mark.parametrize("members,chunks,splits",
                         [
                             pytest.param("fc0", "1", "1", marks=[pytest.mark.profile, pytest.mark.profilelong]),
                             pytest.param("fc0 fc1 fc2 fc3", "2", "5",
                                          marks=[pytest.mark.profile, pytest.mark.profilelong]),
                             pytest.param("fc0 fc1 fc2 fc3", "2", "10",
                                          marks=[pytest.mark.profile, pytest.mark.profilelong]),
                             pytest.param("fc0 fc1 fc2 fc3 fc4 fc5 fc6 fc7 fc8", "2", "30",
                                          marks=[pytest.mark.profilelong]),
                         ],
                         ids=[
                             "1member_1chunk_1split",
                             "4members_2chunks_5splits",
                             "4members_2chunks_10splits",
                             "9members_2chunks_30splits",
                         ],
                         )
@pytest.mark.timeout(300)
def test_autosubmit_create_profile_metrics(benchmark, tmp_path: Path, autosubmit_exp, general_data, members,
                                           chunks, splits):
    """Integration/performance test for `autosubmit create` with profiling enabled."""
    test_type = "create"
    current_id = _scenario_id(members, chunks, splits)

    yaml_data = prepare_yml(members=members, chunks=chunks, splits=splits)
    as_exp = autosubmit_exp(experiment_data=yaml_data, include_jobs=False, create=False)

    benchmark.pedantic(
        lambda: as_exp.autosubmit.create(as_exp.expid, noplot=True, hide=False, force=True, profile=True)
    )
    benchmark.extra_info.update(_collect_profiler_metrics(as_exp, test_type, current_id, tmp_path))


@pytest.mark.parametrize("members,chunks,splits,max_iterations,test_type",
                         [
                             pytest.param("fc0", "1", "1", 0, "run",
                                          marks=[pytest.mark.profile, pytest.mark.profilelong]),
                             pytest.param("fc0 fc1", "2", "2", 0, "run",
                                          marks=[pytest.mark.profile, pytest.mark.profilelong]),
                             pytest.param("fc0 fc1 fc2 fc3", "2", "5", 0, "run", marks=[pytest.mark.profilelong]),
                             pytest.param("fc0 fc1 fc2 fc3", "2", "10", 0, "run_heavy", marks=[pytest.mark.profilelong]),
                             pytest.param("fc0 fc1 fc2 fc3 fc4 fc5 fc6 fc7 fc8", "5", "100", 15, "run_heavy",
                                          marks=[pytest.mark.profile, pytest.mark.profilelong]),
                         ],
                         ids=[
                             "1member_1chunk_1split",
                             "2members_2chunks_2splits",
                             "4members_2chunks_5splits",
                             "4members_2chunks_10splits",
                             "MEM_TEST"
                         ],
                         )
@pytest.mark.timeout(1800)
def test_autosubmit_run_profile_metrics(benchmark, tmp_path: Path, autosubmit_exp, general_data, members, chunks,
                                        splits, max_iterations, slurm_server, test_type):
    """Integration/performance test for `autosubmit run` with profiling enabled."""
    current_id = _scenario_id(members, chunks, splits)
    yaml_data = prepare_yml(members=members, chunks=chunks, splits=splits)
    as_exp = autosubmit_exp(experiment_data=yaml_data, include_jobs=False, create=True)
    as_exp.as_conf.set_last_as_command('run')
    benchmark.pedantic(
        lambda: as_exp.autosubmit.run_experiment(
            as_exp.expid, profile=True, trace=False, profile_max_iterations=max_iterations
        )
    )
    benchmark.extra_info.update(_collect_profiler_metrics(as_exp, test_type, current_id, tmp_path))


@pytest.mark.parametrize("members,chunks,splits",
                         [
                             pytest.param("fc0", "1", "1", marks=[pytest.mark.profile, pytest.mark.profilelong]),
                             pytest.param("fc0 fc1 fc2 fc3", "2", "5",
                                          marks=[pytest.mark.profile, pytest.mark.profilelong]),
                             pytest.param("fc0 fc1 fc2 fc3", "2", "10",
                                          marks=[pytest.mark.profile, pytest.mark.profilelong]),
                             pytest.param("fc0 fc1 fc2 fc3 fc4 fc5 fc6 fc7 fc8", "2", "30",
                                          marks=[pytest.mark.profilelong]),
                         ],
                         ids=[
                             "1member_1chunk_1split",
                             "4members_2chunks_5splits",
                             "4members_2chunks_10splits",
                             "9members_2chunks_30splits",
                         ],
                         )
@pytest.mark.timeout(600)
def test_autosubmit_recovery_profile_metrics(benchmark, tmp_path: Path, autosubmit_exp, general_data, members, chunks,
                                             splits, slurm_server):
    """Integration/performance test for `autosubmit recovery` with profiling enabled."""
    test_type = "recovery"
    current_id = _scenario_id(members, chunks, splits)
    yaml_data = prepare_yml(members=members, chunks=chunks, splits=splits)
    as_exp = autosubmit_exp(experiment_data=yaml_data, include_jobs=False, create=True)
    as_exp.as_conf.set_last_as_command('recovery')
    job_list = as_exp.autosubmit.load_job_list(as_exp.expid, as_exp.as_conf, new=False, full_load=True)
    job_names = [job.name for job in job_list.get_job_list()]
    prepare_setstatus_recovery(as_exp, tmp_path, job_names, slurm_server)

    def _recovery() -> None:
        prof: Profiler = Profiler(as_exp.expid)
        prof.start()
        as_exp.autosubmit.recovery(
            as_exp.expid,
            noplot=True,
            save=True,
            all_jobs=True,
            hide=True,
            group_by="date",
            expand=[],
            expand_status=[],
            detail=True,
            force=True,
            offline=False,
        )
        prof.stop()

    benchmark.pedantic(_recovery)
    benchmark.extra_info.update(_collect_profiler_metrics(as_exp, test_type, current_id, tmp_path))


def do_setstatus(as_exp_, fl=None, ftcs=None, fs=None, ft=None, target="WAITING"):
    target = target.upper()
    as_exp_.autosubmit.set_status(
        as_exp_.expid,
        noplot=True,
        save=True,
        final=target,
        filter_list=fl,
        filter_chunks=None,
        filter_status=fs,
        filter_section=ft,
        filter_type_chunk=None,
        filter_type_chunk_split=ftcs,
        hide=False,
        group_by=None,
        expand=[],
        expand_status=[],
        check_wrapper=False,
        detail=False
    )


@pytest.mark.timeout(600)
@pytest.mark.parametrize("members,chunks,splits,filter_type",
                         [
                             pytest.param("fc0", "1", "1", "ftcs",
                                          marks=[pytest.mark.profile, pytest.mark.profilelong]),
                             pytest.param("fc0 fc1 fc2 fc3", "2", "5", "ftcs",
                                          marks=[pytest.mark.profile, pytest.mark.profilelong]),
                             pytest.param("fc0 fc1 fc2 fc3", "2", "10", "ftcs",
                                          marks=[pytest.mark.profile, pytest.mark.profilelong]),
                             pytest.param("fc0", "1", "1", "ft",
                                          marks=[pytest.mark.profile, pytest.mark.profilelong]),
                             pytest.param("fc0 fc1 fc2 fc3", "2", "5", "ft",
                                          marks=[pytest.mark.profile, pytest.mark.profilelong]),
                             pytest.param("fc0 fc1 fc2 fc3", "2", "10", "ft",
                                          marks=[pytest.mark.profile, pytest.mark.profilelong]),
                             pytest.param("fc0", "1", "1", "fs",
                                          marks=[pytest.mark.profile, pytest.mark.profilelong]),
                             pytest.param("fc0 fc1 fc2 fc3", "2", "5", "fs",
                                          marks=[pytest.mark.profile, pytest.mark.profilelong]),
                             pytest.param("fc0 fc1 fc2 fc3", "2", "10", "fs",
                                          marks=[pytest.mark.profile, pytest.mark.profilelong]),
                             pytest.param("fc0", "1", "1", "fl",
                                          marks=[pytest.mark.profile, pytest.mark.profilelong]),
                             pytest.param("fc0 fc1 fc2 fc3", "2", "5", "fl",
                                          marks=[pytest.mark.profile, pytest.mark.profilelong]),
                             pytest.param("fc0 fc1 fc2 fc3", "2", "10", "fl",
                                          marks=[pytest.mark.profile, pytest.mark.profilelong]),
                             pytest.param("fc0 fc1 fc2 fc3 fc4 fc5 fc6 fc7 fc8", "2", "30", "ftcs",
                                          marks=[pytest.mark.profilelong]),
                             pytest.param("fc0 fc1 fc2 fc3 fc4 fc5 fc6 fc7 fc8", "2", "30", "ft",
                                          marks=[pytest.mark.profilelong]),
                             pytest.param("fc0 fc1 fc2 fc3 fc4 fc5 fc6 fc7 fc8", "2", "30", "fs",
                                          marks=[pytest.mark.profilelong]),
                             pytest.param("fc0 fc1 fc2 fc3 fc4 fc5 fc6 fc7 fc8", "2", "30", "fl",
                                          marks=[pytest.mark.profilelong]),
                         ],
                         ids=[
                             "1member_1chunk_1split_ftcs",
                             "4members_2chunks_5splits_ftcs",
                             "4members_2chunks_10splits_ftcs",
                             "1member_1chunk_1split_ft",
                             "4members_2chunks_5splits_ft",
                             "4members_2chunks_10splits_ft",
                             "1member_1chunk_1split_fs",
                             "4members_2chunks_5splits_fs",
                             "4members_2chunks_10splits_fs",
                             "1member_1chunk_1split_fl",
                             "4members_2chunks_5splits_fl",
                             "4members_2chunks_10splits_fl",
                             "9members_2chunks_30splits_ftcs",
                             "9members_2chunks_30splits_ft",
                             "9members_2chunks_30splits_fs",
                             "9members_2chunks_30splits_fl",
                         ],
                         )
def test_autosubmit_setstatus_profile_metrics(benchmark, tmp_path: Path, autosubmit_exp, general_data, members, chunks,
                                              splits, slurm_server, filter_type):
    """Integration/performance test for `autosubmit setstatus` with profiling enabled."""

    test_type = "setstatus"
    current_id = _scenario_id(members, chunks, splits, filter_type)
    yaml_data = prepare_yml(members=members, chunks=chunks, splits=splits)
    as_exp = autosubmit_exp(experiment_data=yaml_data, include_jobs=False, create=True)
    as_exp.as_conf.set_last_as_command('recovery')
    job_list = as_exp.autosubmit.load_job_list(as_exp.expid, as_exp.as_conf, new=False, full_load=True)
    job_names = [job.name for job in job_list.get_job_list()]
    prepare_setstatus_recovery(as_exp, tmp_path, job_names, slurm_server)
    fl_filter_names = " ".join(job_names)
    ftcs_filter = "[20200101 [ fc0 fc1 fc2 fc3 fc4 [ 1-2 ] ] ],Any"
    ft_filter = "LOCAL_SETUP, SYNCHRONIZE, REMOTE_SETUP, DN, OPA_ENERGY_INDICATORS, APP_ENERGY_INDICATORS, OPA_ENERGYTDIG1"
    fs = "WAITING"
    target = "COMPLETED"

    def _setstatus() -> None:
        prof: Profiler = Profiler(as_exp.expid)
        prof.start()
        do_setstatus(
            as_exp,
            fl=fl_filter_names if filter_type.lower() == "fl" else None,
            ftcs=ftcs_filter if filter_type.lower() == "ftcs" else None,
            fs=fs if filter_type.lower() == "fs" else None,
            ft=ft_filter if filter_type.lower() == "ft" else None,
            target=target
        )
        prof.stop()

    benchmark.pedantic(_setstatus)
    benchmark.extra_info.update(_collect_profiler_metrics(as_exp, test_type, current_id, tmp_path))
