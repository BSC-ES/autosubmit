import os
from pathlib import Path
import re
from typing import Any

import pytest

from autosubmit.config.basicconfig import BasicConfig


# https://github.com/BSC-ES/autosubmit/issues/1332

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


def parse_metrics(as_exp: BasicConfig, run_id: str, tmp_path: Path, overwrite_ref: bool = False):
    profile_path = tmp_path / as_exp.expid / "tmp" / "profile"
    job_list = as_exp.autosubmit.load_job_list(as_exp.expid, as_exp.as_conf, new=False, full_load=True)

    total_dependencies = len(job_list.graph.edges)
    total_jobs = len(job_list.get_job_list())
    metric_files = list(profile_path.glob("*.txt"))
    if not metric_files:
        pytest.fail("No profile files found")
    metric_files.sort(key=os.path.getmtime)
    latest_file = metric_files[-1]
    with open(latest_file, "r") as file:
        text = file.read()

    time_pattern = r"in (\d+\.\d+) seconds"
    time_match = re.search(time_pattern, text)

    # time to complete the command
    time_taken = time_match.group(1) if time_match else None
    # memory_usage
    memory_pattern = r"MEMORY CONSUMPTION: (\d+\.\d+) MiB."
    memory_match = re.search(memory_pattern, text)
    memory_consumption = memory_match.group(1) if memory_match else None

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

    print(f"Time taken: {time_taken} seconds")
    print(f"Memory consumption: {memory_consumption} MiB")
    print(f"Disk Usage (Joblist): {db_size:.2f} MiB")
    print(f"Disk Usage (historical): {metadata_size:.2f} MiB")
    print(f"Total jobs: {total_jobs}")
    print(f'Total dependencies: {total_dependencies}')
    header = "ID, Time Taken, Memory consumption, Disk Usage(Historical), Disk Usage(Joblist), Total Jobs, Total Dependencies"
    # Export to csv
    if overwrite_ref:
        path = Path(__file__).parent / "ref_metrics.csv"
        if not path.exists():
            with open(path, "w") as file:
                file.write(header+"\n")
        else:
            with open(path, "r") as file:
                header_line = file.readline()
            if not header_line.strip() == header:
                with open(path, "w") as file:
                    file.write(file.write(header+"\n"))
        with open(path, "a") as file:
            file.write(
                f"{run_id},{time_taken},{memory_consumption},{metadata_size:.2f},{db_size:.2f},{total_jobs},{total_dependencies}\n")
    else:
        path = Path(__file__).parent / "new_metrics.csv"
        with open(path, "w") as file:
            file.write(file.write(header+"\n"))
            file.write(
                f"{run_id},{time_taken},{memory_consumption},{metadata_size:.2f},{db_size:.2f},{total_jobs},{total_dependencies}\n")

    print(f"Metrics saved to {path}")


def compare_metrics_with_reference(current_id, error_threadhold):
    """Compare the metrics with reference metrics."""

    metric_paths = [Path(__file__).parent / "ref_metrics.csv", Path(__file__).parent / "new_metrics.csv"]

    if not all(path.exists() for path in metric_paths):
        pytest.fail("Reference or new metrics file does not exist.")

    metrics_data: list[dict[str, Any]] = []

    for path in metric_paths:
        with open(path, "r") as file:
            lines = file.readlines()
        for line in lines[::-1]:
            if current_id in line:
                parts = line.strip().split(",")
                metrics_data.append({
                    "id": parts[0],
                    "time_taken": float(parts[1]),
                    "memory": float(parts[2]),
                    "disk_usage_historical": float(parts[3]),
                    "disk_usage_joblist": float(parts[4]),
                    "total_jobs": int(parts[5]),
                    "total_dependencies": int(parts[6]),
                })
                break

    # compare values
    ref_metrics, new_metrics = metrics_data
    for key in ["memory", "disk_usage_historical", "disk_usage_joblist", "total_jobs", "total_dependencies",
                "time_taken"]:
        ref_value = ref_metrics[key]
        new_value = new_metrics[key]
        if ref_value == 0:
            continue
        error = abs(new_value - ref_value) / ref_value
        print(f"Comparing {key}: reference={ref_value}, new={new_value}, error={error:.4f}")
        if error > error_threadhold:
            pytest.fail(f"Metric {key} exceeded error threshold: {error:.4f} > {error_threadhold}")


@pytest.mark.performance
@pytest.mark.parametrize("members,chunks,splits,error_threadhold", [
    ("fc0", "1", "1", 0.1),
], ids=[
    "1member_1chunk_1split",
])
def test_autosubmit_create_profile_metrics(tmp_path: Path, autosubmit_exp, prepare_scratch, general_data, members,
                                           chunks, splits, error_threadhold):
    """Integration/performance test for `autosubmit create` with profiling enabled.
    """
    overwrite_ref = False
    current_id = f"create_{members}_{chunks}_{splits}"

    yaml_data = prepare_yml(members=members, chunks=chunks, splits=splits)
    as_exp = autosubmit_exp(experiment_data=yaml_data, include_jobs=False, create=False)

    as_exp.autosubmit.create(as_exp.expid, noplot=True, hide=False, force=True, profile=True)

    parse_metrics(as_exp, run_id=current_id, tmp_path=tmp_path, overwrite_ref=overwrite_ref)

    if not overwrite_ref:
        compare_metrics_with_reference(current_id, error_threadhold)


@pytest.mark.performance
@pytest.mark.docker
@pytest.mark.slurm
@pytest.mark.ssh
@pytest.mark.parametrize("members,chunks,splits,error_threadhold", [
    ("fc0 fc1 fc2 fc3", "2", "5", 0.1),
], ids=[
    "4members_2chunks_5splits",
])
def test_autosubmit_run_profile_metrics(tmp_path: Path, autosubmit_exp, prepare_scratch, general_data, members, chunks,
                                        splits, error_threadhold, slurm_server):
    """Integration/performance test for `autosubmit create` with profiling enabled."""
    overwrite_ref = True
    current_id = f"run_{members}_{chunks}_{splits}"

    yaml_data = prepare_yml(members=members, chunks=chunks, splits=splits)
    as_exp = autosubmit_exp(experiment_data=yaml_data, include_jobs=False, create=False)
    as_exp.as_conf.set_last_as_command('run')

    as_exp.autosubmit.run_experiment(as_exp.expid, profile=True)

    parse_metrics(as_exp, run_id=current_id, tmp_path=tmp_path, overwrite_ref=overwrite_ref)

    if not overwrite_ref:
        compare_metrics_with_reference(current_id, error_threadhold)
