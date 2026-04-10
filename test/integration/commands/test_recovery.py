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


from getpass import getuser
from pathlib import Path
from typing import Any, TYPE_CHECKING

import pytest

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.history.data_classes.job_data import JobData
from autosubmit.history.database_managers.experiment_history_db_manager import SqlAlchemyExperimentHistoryDbManager
from autosubmit.job.job_common import Status
from autosubmit.log.log import AutosubmitCritical
from autosubmit.platforms.paramiko_submitter import ParamikoSubmitter

if TYPE_CHECKING:
    from docker.models.containers import Container


@pytest.fixture(scope="function")
def as_exp(autosubmit_exp, general_data, experiment_data, jobs_data):
    config_data = general_data | experiment_data | jobs_data
    return autosubmit_exp(experiment_data=config_data, include_jobs=False, create=True)


@pytest.fixture(scope="function")
def submitter(as_exp):
    submitter = as_exp.autosubmit._get_submitter(as_exp.as_conf)
    submitter.load_platforms(as_exp.as_conf)
    return submitter


@pytest.fixture(scope="function")
def job_list(as_exp, submitter):
    return as_exp.autosubmit.load_job_list(
        as_exp.expid, as_exp.as_conf, new=False)


@pytest.fixture(scope="function")
def prepare_scratch(as_exp, tmp_path: Path, job_list, job_names_to_recover, slurm_server: 'Container') -> Any:
    """Generates some completed and stat files in the scratch directory to simulate completed jobs.

    :param as_exp: The Autosubmit experiment object.
    :param tmp_path: The temporary path for the experiment.
    :param job_list: The job list object.
    :param job_names_to_recover: The list of job names to recover.
    :param slurm_server: The SLURM server container.
    :type as_exp: Any
    :type tmp_path: Path
    :type job_list: Any
    :type job_names_to_recover: Any
    :type slurm_server: Any
    """
    slurm_root = f"/tmp/scratch/group/{getuser()}/{as_exp.expid}/"
    log_dir = Path(slurm_root) / f'LOG_{as_exp.expid}/'
    local_completed_dir = tmp_path / as_exp.expid / "tmp" / f'LOG_{as_exp.expid}/'
    # combining this with the touch, makes the touch generates a folder instead of a file. I have no idea why.
    slurm_server.exec_run(['bash', '-c', f'mkdir -p {log_dir}'])

    cmds = []
    for name in job_names_to_recover:
        if "LOCAL" in name:
            local_completed_dir.mkdir(parents=True, exist_ok=True)
            (local_completed_dir / f"{name}_COMPLETED").touch()
        else:
            cmds.append(f'touch {log_dir}/{name}_COMPLETED')
    full_cmd = " && ".join(cmds)
    # exec_run with a string uses shlex.split which breaks shell operators like &&
    slurm_server.exec_run(['bash', '-c', full_cmd])


@pytest.fixture(scope="function")
def job_names_to_recover(job_list):
    return [job.name for job in job_list.get_job_list() if job.split == 1 or job.split == 3]


def reset(as_exp_, target="WAITING"):
    job_list_ = as_exp_.autosubmit.load_job_list(
        as_exp_.expid, as_exp_.as_conf, new=False
    )

    job_names = " ".join([job.name for job in job_list_.get_job_list()])
    as_exp_.autosubmit.set_status(
        as_exp_.expid,
        noplot=True,
        save=True,
        final=target,
        filter_list=job_names,
        filter_chunks=None,
        filter_status=None,
        filter_section=None,
        filter_type_chunk=None,
        filter_type_chunk_split=None,
        hide=False,
        group_by=None,
        expand=[],
        expand_status=[],
        check_wrapper=False,
        detail=False,
    )

    return as_exp_.autosubmit.load_job_list(as_exp_.expid, as_exp_.as_conf, new=False)


def do_recovery(as_exp, fl=None, fc=None, fs=None, ft=None, all_jobs=True):

    as_exp.autosubmit.recovery(
        as_exp.expid,
        noplot=False,
        save=True,
        all_jobs=all_jobs,
        hide=True,
        group_by="date",
        expand=[],
        expand_status=[],
        detail=True,
        force=True,
        offline=True,
        filter_list=fl,
        filter_chunks=fc,
        filter_status=fs,
        filter_section=ft,
    )

    return as_exp.autosubmit.load_job_list(as_exp.expid, as_exp.as_conf, new=False)


@pytest.mark.docker
@pytest.mark.slurm
@pytest.mark.ssh
@pytest.mark.parametrize("active_jobs,force", [
    (True, True),
    (True, False),
    (False, True),
    (False, False),
], ids=[
    "Active_jobs&Force == recover_all",
    "Active_jobs&No_Force == raise_error",
    "No_Active_jobs&Force == recover_all",
    "No_Active_jobs&No_Force == recover_all",
])
def test_online_recovery(
        as_exp,
        prepare_scratch,
        submitter,
        slurm_server,
        job_names_to_recover,
        active_jobs: bool,
        force: bool
):
    """Test the recovery of an experiment.

    :param as_exp: The Autosubmit experiment object.
    :param prepare_scratch: Fixture to prepare the scratch directory.
    """
    job_list_ = as_exp.autosubmit.load_job_list(as_exp.expid, as_exp.as_conf, new=False)
    db_manager = SqlAlchemyExperimentHistoryDbManager(as_exp.expid, BasicConfig.JOBDATA_DIR, f'job_data_{as_exp.expid}.db')
    db_manager.initialize()
    # Save fails if the platform is not set. In 4.2 this will not happen.
    submitter = ParamikoSubmitter(as_conf=as_exp.as_conf)
    submitter.load_platforms(as_exp.as_conf)
    platforms = submitter.platforms

    for job in job_list_.get_job_list():
        if not job.platform:
            job.platform = platforms[job.platform_name]
        if job.name in job_names_to_recover:
            if active_jobs:
                job.status = Status.RUNNING
            else:
                job.status = Status.WAITING

    job_list_.save()

    if active_jobs and not force:
        with pytest.raises(AutosubmitCritical):
            as_exp.autosubmit.recovery(
                as_exp.expid,
                noplot=False,
                save=True,
                all_jobs=True,
                hide=True,
                group_by="date",
                expand=[],
                expand_status=[],
                detail=True,
                force=force,
                offline=False
            )
    else:
        as_exp.autosubmit.recovery(
            as_exp.expid,
            noplot=False,
            save=True,
            all_jobs=True,
            hide=True,
            group_by="date",
            expand=[],
            expand_status=[],
            detail=True,
            force=force,
            offline=False
        )

        job_list_ = as_exp.autosubmit.load_job_list(as_exp.expid, as_exp.as_conf, new=False)

        completed_jobs = [job.name for job in job_list_.get_job_list() if job.status == Status.COMPLETED]

        for name in job_names_to_recover:
            # 2nd split is not completed, so the 3rd split was marked as the COMPLETED file was found, and then WAITING.
            split_number = name.split('_')[-2]
            if split_number == "3":
                assert name not in completed_jobs
            else:
                assert name in completed_jobs


@pytest.mark.parametrize("active_jobs,force", [
    (True, True),
    (True, False),
    (False, True),
    (False, False),
], ids=[
    "Active_jobs&Force == recover_all",
    "Active_jobs&No_Force == raise_error",
    "No_Active_jobs&Force == recover_all",
    "No_Active_jobs&No_Force == recover_all",
])
def test_offline_recovery(as_exp, tmp_path, submitter, job_names_to_recover, active_jobs, force):
    try:
        job_names_to_recover = [name for name in job_names_to_recover if "LOCAL" not in name]
        as_exp.as_conf.set_last_as_command('recovery')

        db_manager = SqlAlchemyExperimentHistoryDbManager(as_exp.expid, BasicConfig.JOBDATA_DIR, f'job_data_{as_exp.expid}.db')

        db_manager.initialize()
        job_list_ = as_exp.autosubmit.load_job_list(
            as_exp.expid, as_exp.as_conf, new=False)

        submitter = as_exp.autosubmit._get_submitter(as_exp.as_conf)
        submitter.load_platforms(as_exp.as_conf)
        platforms = submitter.platforms

        for job in job_list_.get_job_list():
            if not job.platform:
                job.platform = platforms[job.platform_name]
            if job.name in job_names_to_recover:
                if active_jobs:
                    job.status = Status.RUNNING
                else:
                    job.status = Status.WAITING

            job_data_dc = JobData(_id=0,
                                  counter=0,
                                  job_name=job.name,
                                  submit=11111,
                                  status="COMPLETED",
                                  rowtype=0,
                                  ncpus=0,
                                  wallclock="00:01",
                                  qos="debug",
                                  date=job.date,
                                  member=job.member,
                                  section=job.section,
                                  chunk=job.chunk,
                                  platform=job.platform_name,
                                  job_id=job.id,
                                  children=None,
                                  run_id=1,
                                  workflow_commit=None)
            db_manager._insert_job_data(job_data_dc)
            job_data_dc = JobData(_id=0,
                                  counter=1,
                                  job_name=job.name,
                                  submit=11111,
                                  status="FAILED",
                                  rowtype=0,
                                  ncpus=0,
                                  wallclock="00:01",
                                  qos="debug",
                                  date=job.date,
                                  member=job.member,
                                  section=job.section,
                                  chunk=job.chunk,
                                  platform=job.platform_name,
                                  job_id=job.id,
                                  children=None,
                                  run_id=2,
                                  workflow_commit=None)
            db_manager._insert_job_data(job_data_dc)
            job_data_dc = JobData(_id=0,
                                  counter=2,
                                  job_name=job.name,
                                  submit=11111,
                                  status="COMPLETED",
                                  rowtype=0,
                                  ncpus=0,
                                  wallclock="00:01",
                                  qos="debug",
                                  date=job.date,
                                  member=job.member,
                                  section=job.section,
                                  chunk=job.chunk,
                                  platform=job.platform_name,
                                  job_id=job.id,
                                  children=None,
                                  run_id=3,
                                  workflow_commit=None)
            db_manager._insert_job_data(job_data_dc)
        job_list_.save()

        if active_jobs and not force:
            with pytest.raises(AutosubmitCritical):
                as_exp.autosubmit.recovery(
                    as_exp.expid,
                    noplot=False,
                    save=True,
                    all_jobs=True,
                    hide=True,
                    group_by="date",
                    expand=[],
                    expand_status=[],
                    detail=True,
                    force=force,
                    offline=True
                )
        else:
            as_exp.autosubmit.recovery(
                as_exp.expid,
                noplot=False,
                save=True,
                all_jobs=True,
                hide=True,
                group_by="date",
                expand=[],
                expand_status=[],
                detail=True,
                force=force,
                offline=True
            )
            job_list__ = as_exp.autosubmit.load_job_list(
                as_exp.expid, as_exp.as_conf, new=False)

            completed_jobs = [job.name for job in job_list__.get_job_list() if job.status == Status.COMPLETED]

            for name in job_names_to_recover:
                # 2nd split is not completed, so the 3º split was marked as COMPLETED and then WAITING
                split_number = name.split('_')[-2]
                if split_number == "3":
                    assert name not in completed_jobs
                else:
                    assert name in completed_jobs

    except BaseException as e:  # TODO fix this test to work in parallel
        print(str(e))
        pytest.xfail("Offline recovery test is flaky, needs investigation. It always works when launched alone or with setstatus/recovery tests")


@pytest.mark.parametrize("noplot", [True, False])
def test_recovery_noplot_calls_generate_output(as_exp, mocker, noplot):
    """Test that recovery calls generate_output when noplot is False and does not call it when noplot is True."""
    db_manager = SqlAlchemyExperimentHistoryDbManager(
        as_exp.expid, BasicConfig.JOBDATA_DIR, f"job_data_{as_exp.expid}.db"
    )
    db_manager.initialize()

    mock_generate_output = mocker.patch(
        "autosubmit.monitor.monitor.Monitor.generate_output"
    )

    as_exp.autosubmit.recovery(
        as_exp.expid,
        noplot=noplot,
        save=True,
        all_jobs=True,
        hide=True,
        group_by="date",
        expand=[],
        expand_status=[],
        detail=True,
        force=False,
        offline=True,
    )

    if noplot:
        mock_generate_output.assert_not_called()
    else:
        mock_generate_output.assert_called_once()


def test_recovery_combined_filters(as_exp, mocker):
    """Test that the recovery when multiple filters are used, selected jobs must match the intersection of the filters."""
    # Just test that the combination of filters works, not to check the recovery itself
    # The recovery method is mocked to just check the filters

    db_manager = SqlAlchemyExperimentHistoryDbManager(
        as_exp.expid, BasicConfig.JOBDATA_DIR, f"job_data_{as_exp.expid}.db"
    )
    db_manager.initialize()

    reset(as_exp, "WAITING")

    job_list = as_exp.autosubmit.load_job_list(as_exp.expid, as_exp.as_conf, new=False)
    all_job_names = [job.name for job in job_list.get_job_list()]

    target_job = f"{as_exp.expid}_20200101_fc0_1_1_LOCALJOB"
    no_matching_job = f"{as_exp.expid}_20200101_fc0_1_1_PSJOB"
    filter_list = f"{target_job} {no_matching_job}"

    mocker.patch(
        "autosubmit.autosubmit.Autosubmit.online_recovery", return_value=all_job_names
    )

    job_list = do_recovery(
        as_exp, fl=filter_list, fc="[20200101 [fc0 [1] ] ]", ft="LOCALJOB", fs="WAITING"
    )

    completed_jobs = [
        job.name for job in job_list.get_job_list() if job.status == Status.COMPLETED
    ]

    assert len(completed_jobs) == 1
    assert completed_jobs[0] == target_job
