from getpass import getuser
from pathlib import Path

import pytest

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.platforms.paramiko_submitter import ParamikoSubmitter

slurm_conf = {
    "LOCAL_ROOT_DIR": "/tmp/autosubmit/",
    "LOCAL_ASLOG_DIR": "logs",
    "PLATFORMS": {
        "TEST_SLURM": {
            "TYPE": "slurm",
            "HOST": "127.0.0.1",
            "PROJECT": "group",
            "QUEUE": "gp_debug",
            "SCRATCH_DIR": "/tmp/scratch/",
            "TEMP_DIR": "",
            "USER": "root",
            "MAX_WALLCLOCK": "02:00",
            "MAX_PROCESSORS": "4",
            "PROCESSORS_PER_NODE": "4",
            "ADD_PROJECT_TO_HOST": "False"
        }
    }
}


@pytest.mark.docker
@pytest.mark.ssh
@pytest.mark.parametrize("remote_logs", [True, False])
def test_retrieve_logfiles(autosubmit_exp, tmp_path, slurm_server, mocker, remote_logs):
    as_exp = autosubmit_exp(experiment_data=slurm_conf, include_jobs=False, create=True)
    as_exp.as_conf.experiment_data['ROOTDIR'] = str(tmp_path)
    as_exp.as_conf.experiment_data['DEFAULT']['HPCARCH'] = 'TEST_SLURM'
    as_exp.as_conf.experiment_data['AS_ENV_CURRENT_USER'] = getuser()
    submitter = ParamikoSubmitter(as_conf=as_exp.as_conf)
    submitter.load_platforms(as_exp.as_conf)
    slurm_platform = submitter.platforms['TEST_SLURM']
    job = Job('some', 'job_id', status=Status.WAITING, priority=0, loaded_data=None)
    job.expid = as_exp.expid
    job._tmp_path = tmp_path / as_exp.expid / 'tmp'
    job.platform = slurm_platform
    job.platform.connect(None)
    Path(f'{tmp_path}/{as_exp.expid}/tmp/some_TOTAL_STATS').touch()
    if remote_logs:
        slurm_server.exec_run(f"mkdir -p {job.platform.remote_log_dir}")
        job.remote_logs = ("some.cmd.out.0", "some.cmd.err.0")
        job.local_logs = ("some.cmd.out.0", "some.cmd.err.0")
        slurm_server.exec_run(f"touch {job.platform.remote_log_dir}/some.cmd.out.0")
        slurm_server.exec_run(f"touch {job.platform.remote_log_dir}/some.cmd.err.0")
        slurm_server.exec_run(f"touch {job.platform.remote_log_dir}/some_COMPLETED")
        slurm_server.exec_run(f"printf '%s\n' 19704922 19704923 19704924 19704925 > {job.platform.remote_log_dir}/some_STAT_0")

        report = job.retrieve_logfiles()
        assert report.all_succeeded
        assert len(report.attempts) == 1
        assert report.attempts[0].success
        assert report.final_updated_log == 1
    else:
        report = job.retrieve_logfiles()
        assert not report.all_succeeded
        assert report.final_updated_log == 0
        slurm_server.exec_run(f"mkdir -p {job.platform.remote_log_dir}")
        report = job.retrieve_logfiles()
        assert not report.all_succeeded
        assert report.final_updated_log == 0


@pytest.mark.docker
@pytest.mark.ssh
@pytest.mark.parametrize("remote_logs", [True, False])
def test_retrieve_logfiles_with_retrials(autosubmit_exp, tmp_path, slurm_server, mocker, remote_logs):
    as_exp = autosubmit_exp(experiment_data=slurm_conf, include_jobs=False, create=True)
    as_exp.as_conf.experiment_data['ROOTDIR'] = str(tmp_path)
    as_exp.as_conf.experiment_data['DEFAULT']['HPCARCH'] = 'TEST_SLURM'
    as_exp.as_conf.experiment_data['AS_ENV_CURRENT_USER'] = getuser()
    submitter = ParamikoSubmitter(as_conf=as_exp.as_conf)
    submitter.load_platforms(as_exp.as_conf)
    slurm_platform = submitter.platforms['TEST_SLURM']
    jobs = [Job('some0', 'job_id', status=Status.WAITING, priority=0, loaded_data=None), Job('some1', 'job_id2', status=Status.WAITING, priority=0, loaded_data=None)]

    for i, job in enumerate(jobs):
        job.retrials = len(jobs)
        job.fail_count = i
        job.expid = as_exp.expid
        job._tmp_path = tmp_path / as_exp.expid / 'tmp'
        job.platform = slurm_platform
        job.platform.connect(None)
        Path(f'{tmp_path}/{as_exp.expid}/tmp/{job.name}_TOTAL_STATS').touch()
        slurm_server.exec_run(f"mkdir -p {job.platform.remote_log_dir}")
        slurm_server.exec_run(f"touch {job.platform.remote_log_dir}/{job.name}_COMPLETED")
        if remote_logs:
            # Create remote log files for all attempts up to fail_count
            for attempt in range(job.fail_count + 1):
                slurm_server.exec_run(f"touch {job.platform.remote_log_dir}/{job.name}.cmd.out.{attempt}")
                slurm_server.exec_run(f"touch {job.platform.remote_log_dir}/{job.name}.cmd.err.{attempt}")
                slurm_server.exec_run(f"printf '%s\n' 19704922 19704923 19704924 19704925 > {job.platform.remote_log_dir}/{job.name}_STAT_{attempt}")

    for job in jobs:
        if remote_logs:
            slurm_server.exec_run(f"mkdir -p {job.platform.remote_log_dir}")
            job.remote_logs = (f"{job.name}.cmd.out.{job.fail_count}", f"{job.name}.cmd.err.{job.fail_count}")
            job.local_logs = (f"{job.name}.cmd.out.{job.fail_count}", f"{job.name}.cmd.err.{job.fail_count}")
            report = job.retrieve_logfiles()
            assert report.all_succeeded
            assert len(report.attempts) == job.fail_count + 1
            assert report.final_updated_log == job.fail_count + 1
        else:
            report = job.retrieve_logfiles()
            assert not report.all_succeeded
            assert report.final_updated_log == 0
            slurm_server.exec_run(f"mkdir -p {job.platform.remote_log_dir}")
            report = job.retrieve_logfiles()
            assert not report.all_succeeded
            assert report.final_updated_log == 0
            slurm_server.exec_run(f"rm -rf {job.platform.remote_log_dir}")
