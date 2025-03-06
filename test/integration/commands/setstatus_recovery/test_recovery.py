from pathlib import Path
from typing import Any
from autosubmit.job.job_common import Status

import pytest

_EXPID = 't000'


@pytest.fixture(scope="function", autouse=True)
def as_exp(autosubmit_exp, general_data, experiment_data, jobs_data):
    config_data = general_data | experiment_data | jobs_data
    return autosubmit_exp(_EXPID, experiment_data=config_data, include_jobs=False, create=True)


@pytest.fixture
def submitter(as_exp):
    submitter = as_exp.autosubmit._get_submitter(as_exp.as_conf)
    submitter.load_platforms(as_exp.as_conf)
    return submitter


@pytest.fixture
def job_list(as_exp, submitter):
    return as_exp.autosubmit.load_job_list(
        as_exp.expid, as_exp.as_conf, new=False, full_load=True, submitter=submitter,
        check_failed_jobs=True)


@pytest.fixture
def prepare_scratch(tmp_path: Path, job_list, job_names_to_recover, slurm_server) -> Any:
    """
    Generates some completed and stat files in the scratch directory to simulate completed jobs.

    :param tmp_path: Temporary directory unique to the test.
    :type tmp_path: Path
    :return: Configured experiment object.
    :rtype: Any
    """
    slurm_root = f"/tmp/scratch/group/root/{_EXPID}/"
    log_dir = Path(slurm_root) / f'LOG_{_EXPID}/'
    local_completed_dir = tmp_path / _EXPID / "tmp" / f'LOG_{_EXPID}/'
    slurm_server.exec(f'mkdir -p {log_dir}')  # combining this with the touch, makes the touch generates a folder instead of a file. I have no idea why.

    cmds = []
    for name in job_names_to_recover:
        if "LOCAL" in name:
            local_completed_dir.mkdir(parents=True, exist_ok=True)
            (local_completed_dir / f"{name}_COMPLETED").touch()
        else:
            cmds.append(f'touch {log_dir}/{name}_COMPLETED')
    full_cmd = " && ".join(cmds)
    slurm_server.exec(full_cmd)


@pytest.fixture
def job_names_to_recover(job_list):
    return [job.name for job in job_list.get_job_list() if job.split == 1]


@pytest.mark.slurm
def test_online_recovery(as_exp, prepare_scratch, submitter, slurm_server, job_names_to_recover):
    """
    Test the recovery of an experiment.

    :param as_exp: The Autosubmit experiment object.
    :param prepare_scratch: Fixture to prepare the scratch directory.
    :type as_exp: Any
    :type prepare_scratch: Any
    """

    as_exp.autosubmit.recovery(
        as_exp.expid,
        noplot=True,
        save=True,
        all_jobs=True,
        hide=False,
        group_by=None,
        expand=[],
        expand_status=[],
        detail=False,
        force=False,
        offline=False
    )
    job_list_ = as_exp.autosubmit.load_job_list(
        as_exp.expid, as_exp.as_conf, new=False, full_load=True,
        check_failed_jobs=True)

    completed_jobs = [job.name for job in job_list_.get_job_list() if job.status == Status.COMPLETED]

    for name in job_names_to_recover:
        assert name in completed_jobs
