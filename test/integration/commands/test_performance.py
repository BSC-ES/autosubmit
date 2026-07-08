import os
from pathlib import Path
import re
from typing import Any, Container

import pytest

from autosubmit.profiler.profiler import Profiler


# https://github.com/BSC-ES/autosubmit/issues/1332

def autosubmit_version():
    """Reads the version number from the VERSION file."""
    with open(Path(__file__).parent.parent.parent.parent / "VERSION", "r") as file:
        content = file.read()
    return content.strip(" \n")


def prepare_setstatus_recovery(as_exp, tmp_path: Path, job_names_to_recover, slurm_server: 'Container') -> Any:
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


def parse_metrics(as_exp: Any, test_type: str, run_id: str, tmp_path: Path):
    """Parse the profiler output and export metrics to CSV files.
    :param as_exp: The Autosubmit experiment object.
    :param test_type: The type of run (e.g., 'create', 'run', 'recovery').
    :param run_id: Unique identifier for the test run.
    :param tmp_path: The temporary path for the experiment.
    """
    profile_path = tmp_path / as_exp.expid / "tmp" / "profile"

    metric_files = list(profile_path.glob("*.txt"))
    if not metric_files:
        pytest.fail("No profile files found")
    metric_files.sort(key=os.path.getmtime)
    latest_file = metric_files[-1]
    with open(latest_file, "r") as file:
        text = file.read()

    export_to_csv(test_type, run_id, as_exp, text, tmp_path)
    export_to_csv_by_iteration(test_type, run_id, text)


def _write_csv(path: Path, header: str, lines: list[str]) -> None:
    """Write or append lines to a CSV file, writing the header if needed.

    :param path: The path to the CSV file.
    :param header: The header line to write if the file is new or has a mismatched header.
    :param lines: The data lines to append.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists() or path.read_text().splitlines()[0].strip() != header:
        with open(path, "w") as file:
            file.write(header + "\n")
    with open(path, "a") as file:
        file.write("\n".join(lines) + "\n")
    print(f"Metrics saved to {path}")


def export_to_csv(
        test_type: str,
        run_id: str,
        as_exp: Any,
        profiler_text: str,
        tmp_path: Path
) -> None:
    """Export summary performance metrics to a versioned CSV file.
    :param test_type: The type of run (e.g., 'create', 'run', 'recovery').
    :param run_id: Unique identifier for the test run.
    :param as_exp: The Autosubmit experiment object.
    :param profiler_text: Raw text output from the profiler.
    :param tmp_path: The temporary path for the experiment.
    """
    header = "test type,ID,Time Taken(Seconds),Memory consumption(MiB),Historical DB Disk Usage(MiB),Job list DB Usage,Total Jobs,Total Dependencies,FD GROW,MEM GROW(MIB),OBJ GROW"

    job_list = as_exp.autosubmit.load_job_list(as_exp.expid, as_exp.as_conf, new=False, full_load=True)
    total_dependencies = len(job_list.graph.edges)
    total_jobs = len(job_list.graph.nodes)
    time_pattern = r"in (\d+\.\d+) seconds"
    time_match = re.search(time_pattern, profiler_text)
    time_taken = time_match.group(1) if time_match else None

    memory_pattern = r"FINAL MEMORY: (\d+\.\d+) MiB."
    memory_match = re.search(memory_pattern, profiler_text)
    memory_consumption = memory_match.group(1) if memory_match else None

    if not memory_consumption:
        memory_pattern = r"FINAL MEMORY: (\d+\.\d+) GiB."
        memory_match = re.search(memory_pattern, profiler_text)
        memory_consumption = memory_match.group(1) if memory_match else None
        # convert to MIB
        if memory_consumption:
            memory_consumption = float(memory_consumption) * 1024
    # Disk usage (sqlite only for now)

    db_path = Path(tmp_path / as_exp.expid / "db" / "job_list.db")
    metadata_db = Path(tmp_path / "metadata" / "data" / f"job_data_{as_exp.expid}.db")

    if db_path.exists():
        db_size = db_path.stat().st_size / (1024 * 1024)  # in MiB
    else:
        db_size = '0'

    if metadata_db.exists():
        metadata_size = metadata_db.stat().st_size / (1024 * 1024)  # in MiB
    else:
        metadata_size = 0

    fd_grow_pattern = r"FILE DESCRIPTORS GROW: (\d+)"
    fd_grow_match = re.search(fd_grow_pattern, profiler_text)
    fd_grow = fd_grow_match.group(1) if fd_grow_match else None
    mem_grow_pattern = r"MEMORY GROW: (\d+)"
    mem_grow_match = re.search(mem_grow_pattern, profiler_text)
    mem_grow = mem_grow_match.group(1) if mem_grow_match else None
    obj_grow_pattern = r"OBJECTS GROW: (\d+)"
    obj_grow_match = re.search(obj_grow_pattern, profiler_text)
    obj_grow = obj_grow_match.group(1) if obj_grow_match else None

    as_version = autosubmit_version()
    path = Path(
        __file__).parent.parent.parent.parent / ".benchmarks" / "artifacts" / f"performance-metrics-{as_version}.csv"
    line = f"{test_type},{run_id},{time_taken},{memory_consumption},{metadata_size},{db_size},{total_jobs},{total_dependencies},{fd_grow},{mem_grow},{obj_grow}"
    _write_csv(path, header, [line])


def export_to_csv_by_iteration(test_type: str, run_id: str, profiler_text: str) -> None:
    """Export per-iteration profiler metrics to a versioned CSV file.

    :param test_type: The type of run (e.g., 'create', 'run', 'recovery').
    :param run_id: Unique identifier for the test run.
    :param profiler_text: Raw text output from the profiler.
    """

    header = "test type,ID,Iteration,Memory consumption(MiB),Objects,File Descriptors,Loaded Jobs,Loaded Edges"
    iteration_numbers = list(map(int, re.findall(r"Iteration (\d+):", profiler_text)))
    last_iteration = max(iteration_numbers) if iteration_numbers else 0

    if not last_iteration:
        return

    iteration_csv_lines = []
    for i in range(1, last_iteration + 1):
        mem_match = re.search(rf"Iteration {i}: Memory: (\d+\.\d+) MiB", profiler_text, re.IGNORECASE)
        obj_match = re.search(rf"Iteration {i}: Objects: (\d+)", profiler_text, re.IGNORECASE)
        fd_match = re.search(rf"Iteration {i}: File Descriptors: (\d+)", profiler_text, re.IGNORECASE)
        jobs_match = re.search(rf"Iteration {i}: Loaded Jobs: (\d+)", profiler_text, re.IGNORECASE)
        edges_match = re.search(rf"Iteration {i}: Loaded Edges: (\d+)", profiler_text, re.IGNORECASE)
        mem = mem_match.group(1) if mem_match else None
        obj = obj_match.group(1) if obj_match else None
        fd = fd_match.group(1) if fd_match else None
        jobs = jobs_match.group(1) if jobs_match else None
        edges = edges_match.group(1) if edges_match else None
        iteration_csv_lines.append(f"{test_type},{run_id},{i},{mem},{obj},{fd},{jobs},{edges}")

    as_version = autosubmit_version()
    path = Path(
        __file__).parent.parent.parent.parent / ".benchmarks" / "artifacts" / f"performance-metrics-by-iteration-{as_version}.csv"
    _write_csv(path, header, iteration_csv_lines)


@pytest.mark.parametrize("members,chunks,splits",
                         [
                             pytest.param("fc0", "1", "1", marks=[pytest.mark.profile, pytest.mark.profilelong]),
                             pytest.param("fc0 fc1 fc2 fc3", "2", "5",
                                          marks=[pytest.mark.profile, pytest.mark.profilelong]),
                             pytest.param("fc0 fc1 fc2 fc3", "2", "10",
                                          marks=[pytest.mark.profile, pytest.mark.profilelong]),
                         ],
                         ids=[
                             "1member_1chunk_1split",
                             "4members_2chunks_5splits",
                             "4members_2chunks_10splits",
                         ],
                         )
def test_autosubmit_create_profile_metrics(tmp_path: Path, autosubmit_exp, general_data, members,
                                           chunks, splits):
    """Integration/performance test for `autosubmit create` with profiling enabled."""
    members_name = members.replace(" ", "_")
    test_type = "create"
    current_id = f"{members_name}_{chunks}_{splits}"

    yaml_data = prepare_yml(members=members, chunks=chunks, splits=splits)
    as_exp = autosubmit_exp(experiment_data=yaml_data, include_jobs=False, create=False)

    as_exp.autosubmit.create(as_exp.expid, noplot=True, hide=False, force=True, profile=True)

    parse_metrics(as_exp, test_type=test_type, run_id=current_id, tmp_path=tmp_path)


@pytest.mark.parametrize("members,chunks,splits,max_iterations,test_type",
                         [
                             pytest.param("fc0", "1", "1", 0, "run",
                                          marks=[pytest.mark.profile, pytest.mark.profilelong]),
                             pytest.param("fc0 fc1", "2", "2", 0, "run",
                                          marks=[pytest.mark.profile, pytest.mark.profilelong]),
                             pytest.param("fc0 fc1 fc2 fc3", "2", "5", 0, "run", marks=[pytest.mark.profilelong]),
                             pytest.param("fc0 fc1 fc2 fc3", "2", "10", 0, "run", marks=[pytest.mark.profilelong]),
                             pytest.param("fc0 fc1 fc2 fc3 fc4 fc5 fc6 fc7 fc8", "5", "100", 10, "run_heavy",
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
def test_autosubmit_run_profile_metrics(tmp_path: Path, autosubmit_exp, general_data, members, chunks,
                                        splits, max_iterations, slurm_server, test_type):
    """Integration/performance test for `autosubmit create` with profiling enabled."""
    members_name = members.replace(" ", "_")
    current_id = f"{members_name}_{chunks}_{splits}"
    yaml_data = prepare_yml(members=members, chunks=chunks, splits=splits)
    as_exp = autosubmit_exp(experiment_data=yaml_data, include_jobs=False, create=True)
    as_exp.as_conf.set_last_as_command('run')
    as_exp.autosubmit.run_experiment(as_exp.expid, profile=True, trace=False, profile_max_iterations=max_iterations)
    parse_metrics(as_exp, test_type=test_type, run_id=current_id, tmp_path=tmp_path)


@pytest.mark.parametrize("members,chunks,splits",
                         [
                             pytest.param("fc0", "1", "1", marks=[pytest.mark.profile, pytest.mark.profilelong]),
                             pytest.param("fc0 fc1 fc2 fc3", "2", "5",
                                          marks=[pytest.mark.profile, pytest.mark.profilelong]),
                             pytest.param("fc0 fc1 fc2 fc3", "2", "10",
                                          marks=[pytest.mark.profile, pytest.mark.profilelong]),
                         ],
                         ids=[
                             "1member_1chunk_1split",
                             "4members_2chunks_5splits",
                             "4members_2chunks_10splits",
                         ],
                         )
def test_autosubmit_recovery_profile_metrics(tmp_path: Path, autosubmit_exp, general_data, members, chunks, splits,
                                             slurm_server):
    """Integration/performance test for `autosubmit recovery` with profiling enabled."""
    members_name = members.replace(" ", "_")
    test_type = "recovery"
    current_id = f"{members_name}_{chunks}_{splits}"
    yaml_data = prepare_yml(members=members, chunks=chunks, splits=splits)
    as_exp = autosubmit_exp(experiment_data=yaml_data, include_jobs=False, create=True)
    as_exp.as_conf.set_last_as_command('recovery')
    job_list = as_exp.autosubmit.load_job_list(as_exp.expid, as_exp.as_conf, new=False, full_load=True)
    job_names = [job.name for job in job_list.get_job_list()]
    prepare_setstatus_recovery(as_exp, tmp_path, job_names, slurm_server)
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
    parse_metrics(as_exp, test_type=test_type, run_id=current_id, tmp_path=tmp_path)


def do_setstatus(as_exp_, fl=None, fc=None, fct=None, ftcs=None, fs=None, ft=None, target="WAITING"):
    target = target.upper()
    as_exp_.autosubmit.set_status(
        as_exp_.expid,
        noplot=True,
        save=True,
        final=target,
        filter_list=fl,
        filter_chunks=fc,
        filter_status=fs,
        filter_section=ft,
        filter_type_chunk=fct,
        filter_type_chunk_split=ftcs,
        hide=False,
        group_by=None,
        expand=[],
        expand_status=[],
        check_wrapper=False,
        detail=False
    )


@pytest.mark.profile
@pytest.mark.profilelong
@pytest.mark.parametrize("members,chunks,splits,filter_type",
                         [
                             pytest.param("fc0", "1", "1", "ftcs"),
                             pytest.param("fc0 fc1 fc2 fc3", "2", "5", "ftcs"),
                             pytest.param("fc0 fc1 fc2 fc3", "2", "10", "ftcs"),
                             pytest.param("fc0", "1", "1", "ft"),
                             pytest.param("fc0 fc1 fc2 fc3", "2", "5", "ft"),
                             pytest.param("fc0 fc1 fc2 fc3", "2", "10", "ft"),
                             pytest.param("fc0", "1", "1", "fs"),
                             pytest.param("fc0 fc1 fc2 fc3", "2", "5", "fs"),
                             pytest.param("fc0 fc1 fc2 fc3", "2", "10", "fs"),
                             pytest.param("fc0", "1", "1", "fl"),
                             pytest.param("fc0 fc1 fc2 fc3", "2", "5", "fl"),
                             pytest.param("fc0 fc1 fc2 fc3", "2", "10", "fl"),
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
                         ],
                         )
def test_autosubmit_setstatus_profile_metrics(tmp_path: Path, autosubmit_exp, general_data, members, chunks, splits,
                                              slurm_server, filter_type):
    """Integration/performance test for `autosubmit setstatus` with profiling enabled."""

    members_name = members.replace(" ", "_")
    test_type = "setstatus"
    current_id = f"{members_name}_{chunks}_{splits}_{filter_type}"
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

    prof: Profiler = Profiler(as_exp.expid)
    prof.start()
    do_setstatus(
        as_exp,
        fl=fl_filter_names if filter_type.lower() == "fl" else None,
        fc=None,  # no need, it shares code with ftcs
        fct=None,  # no need, it shares code with ftcs
        ftcs=ftcs_filter if filter_type.lower() == "ftcs" else None,
        fs=fs if filter_type.lower() == "fs" else None,
        ft=ft_filter if filter_type.lower() == "ft" else None,
        target=target
    )
    prof.stop()
    parse_metrics(as_exp, test_type=test_type, run_id=current_id, tmp_path=tmp_path)
