import os
from pathlib import Path
import re

import pytest

from autosubmit.config.basicconfig import BasicConfig


# https://github.com/BSC-ES/autosubmit/issues/1332

def prepare_yml(members, chunks, splits) -> dict:
    """Fixture to prepare a jobs.yml file for testing."""
    return {
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
    print(f"Time taken: {time_taken} seconds")
    print(f"Memory consumption: {memory_consumption} MiB")

    print(f"Total jobs: {total_jobs}")
    print(f'Total dependencies: {total_dependencies}')
    # Export to csv
    if overwrite_ref:
        path = Path(__file__).parent / "ref_metrics.csv"
        with open(path, "r") as file:
            header_line = file.readline()
        if not header_line.strip() == "ID, Memory consumption, Total jobs, Total Dependencies, Time Taken":
            with open(path, "w") as file:
                file.write("ID, Memory consumption, Total jobs, Total Dependencies, Time Taken\n")
        with open(path, "a") as file:
            file.write(f"{run_id}, {time_taken},{memory_consumption},{total_jobs},{total_dependencies}\n")
    else:
        path = Path(__file__).parent / "new_metrics.csv"
        with open(path, "w") as file:
            file.write("ID, Memory consumption, Total jobs, Total Dependencies, Time Taken\n")
            file.write(f"{run_id}, {time_taken},{memory_consumption},{total_jobs},{total_dependencies}\n")

    print(f"Metrics saved to {path}")

def compare_metrics_with_reference(current_id, error_threadhold):
    ref_path = Path(__file__).parent / "ref_metrics.csv"
    new_path = Path(__file__).parent / "new_metrics.csv"

    if not ref_path.exists():
        pytest.fail("Reference metrics file does not exist. Please run the test with overwrite_ref=True to create it.")

    ref_metrics = {}
    with open(ref_path, "r") as file:
        for line in file[1:]:
            parts = line.strip().split(", ")
            ref_metrics[parts[0]] = {
                "memory": float(parts[1]),
                "total_jobs": int(parts[2]),
                "total_dependencies": int(parts[3]),
                "time_taken": float(parts[4]),
            }

    new_metrics = {}
    with open(new_path, "r") as file:
        next(file)  # Skip header
        for line in file:
            parts = line.strip().split(", ")
            new_metrics[parts[0]] = {
                "memory": float(parts[1]),
                "total_jobs": int(parts[2]),
                "total_dependencies": int(parts[3]),
                "time_taken": float(parts[4]),
            }

    if current_id not in ref_metrics or current_id not in new_metrics:
        pytest.fail(f"Metrics for ID {current_id} not found in reference or new metrics.")

    ref = ref_metrics[current_id]
    new = new_metrics[current_id]

    for key in ["memory", "time_taken"]:
        ref_value = ref[key]
        new_value = new[key]
        if ref_value == 0:
            pytest.fail(f"Reference value for {key} is zero, cannot compute relative error.")
        relative_error = abs(new_value - ref_value) / ref_value
        if relative_error > error_threadhold:
            pytest.fail(f"{key} for ID {current_id} exceeds error threshold: {relative_error:.2%} > {error_threadhold:.2%}")


@pytest.mark.performance
@pytest.mark.parametrize("members,chunks,splits,error_threadhold", [
    ("fc0", "1", "1", 0.1),
], ids=[
    "1member_1chunk_1split",
])
def test_autosubmit_create_profile_metrics(tmp_path: Path, autosubmit_exp, prepare_scratch, general_data, members, chunks, splits, error_threadhold):
    """Integration/performance test for `autosubmit create` with profiling enabled.
    """
    current_id = f"create_{members}_{chunks}_{splits}"

    yaml_data = prepare_yml(members=members, chunks=chunks, splits=splits)
    as_exp = autosubmit_exp(experiment_data=yaml_data, include_jobs=False, create=False)

    as_exp.autosubmit.create(as_exp.expid, noplot=True, hide=False, force=True, profile=True)

    parse_metrics(as_exp, run_id=current_id, tmp_path=tmp_path, overwrite_ref=False)
    compare_metrics_with_reference(current_id, error_threadhold)



# @pytest.mark.docker
# @pytest.mark.xdist_group("slurm")
# @pytest.mark.slurm
# @pytest.mark.ssh
# @pytest.mark.timeout(300)
