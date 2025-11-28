from autosubmit.config.basicconfig import BasicConfig
from autosubmit.history.database_managers.experiment_history_db_manager import SqlAlchemyExperimentHistoryDbManager
from autosubmit.job.job_common import Status
import pytest

from autosubmit.log.log import AutosubmitCritical


@pytest.fixture(scope="function")
def as_exp(autosubmit_exp, general_data, experiment_data, jobs_data):
    config_data = general_data | experiment_data | jobs_data
    return autosubmit_exp(experiment_data=config_data, include_jobs=False, create=True)


def reset(as_exp_, target="WAITING"):
    job_list_ = as_exp_.autosubmit.load_job_list(
        as_exp_.expid, as_exp_.as_conf, new=False)

    job_names = " ".join([job.name for job in job_list_.get_job_list()])
    do_setstatus(as_exp_, fl=job_names, target=target)
    return job_list_


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
    return as_exp_.autosubmit.load_job_list(
        as_exp_.expid, as_exp_.as_conf, new=False)


@pytest.mark.slurm
@pytest.mark.parametrize("reset_target", ["RUNNING", "WAITING"], ids=["Online", "Offline"])
def test_set_status(as_exp, slurm_server, reset_target):
    """Tests the setstatus command with various filters in an offline scenario."""
    db_manager = SqlAlchemyExperimentHistoryDbManager(as_exp.expid, BasicConfig.JOBDATA_DIR, f'job_data_{as_exp.expid}.db')
    db_manager.initialize()
    fl_filter_names = f"{as_exp.expid}_20200101_fc0_1_1_LOCALJOB {as_exp.expid}_20200101_fc0_1_1_PSJOB {as_exp.expid}_20200101_fc0_1_1_SLURMJOB"
    fc_filter = "[20200101 [ fc0 [1] ] ]"
    fct_filter = "[20200101 [ fc0 [1] ] ],LOCALJOB"
    ftcs_filter = "[20200101 [ fc0 [1] ] ],LOCALJOB [2]"
    ft_filter = "LOCALJOB"
    fs = "WAITING"
    target = "COMPLETED"

    job_list_ = do_setstatus(as_exp, fl=fl_filter_names, target=target)
    completed_jobs = [job.name for job in job_list_.get_job_list() if job.status == Status.COMPLETED and job.name in fl_filter_names]
    assert len(completed_jobs) == 3
    reset(as_exp, reset_target)

    job_list_ = do_setstatus(as_exp, fc=fc_filter, target=target)
    completed_jobs = [job.name for job in job_list_.get_job_list() if job.status == Status.COMPLETED and job.chunk == 1]
    assert len(completed_jobs) == 9
    reset(as_exp, reset_target)

    job_list_ = do_setstatus(as_exp, fct=fct_filter, target=target)
    completed_jobs = [job.name for job in job_list_.get_job_list() if job.status == Status.COMPLETED and job.section == "LOCALJOB"]
    assert len(completed_jobs) == 3
    reset(as_exp, reset_target)

    job_list_ = do_setstatus(as_exp, ftcs=ftcs_filter, target=target)
    completed_jobs = [job.name for job in job_list_.get_job_list() if job.status == Status.COMPLETED and job.split == 2 and job.section == "LOCALJOB"]
    assert len(completed_jobs) == 1
    reset(as_exp, reset_target)

    job_list_ = do_setstatus(as_exp, ft=ft_filter, target=target)
    completed_jobs = [job.name for job in job_list_.get_job_list() if job.status == Status.COMPLETED and job.section == "LOCALJOB"]
    assert len(completed_jobs) == 24
    reset(as_exp, reset_target)

    if reset_target == "RUNNING":
        with pytest.raises(AutosubmitCritical):
            do_setstatus(as_exp, fs=fs, target=target)
    else:
        job_list_ = do_setstatus(as_exp, fs=fs, target=target)
        completed_jobs = [job.name for job in job_list_.get_job_list() if job.status == Status.COMPLETED]
        assert len(completed_jobs) == len(job_list_.get_job_list())


@pytest.mark.parametrize(
    "ftcs_filter, expected_jobs",
    [
        # # Bad ones
        ("", 0),
        ("Any", 0),
        ("LOCALJOB", 0),
        ("[", 0),
        ("]", 0),
        ("[20200101", 0),
        ("[20200101 [", 0),
        ("[20200101 [ fc0", 0),
        ("[20200101 [ fc0 [", 0),
        ("[20200101 [ fc0 ]", 0),
        ("[20200101 [ fc0 1", 0),
        ("[20200101 [ fc0 [1] ", 0),
        ("[ fc0 [1] ] ", 0),
        ("[1]", 0),
        (",LOCALJOB", 0),
        ("20200101 [ fc0 [1] ] ]", 0),
        ("[20200101]", 0),
        ("[20200101 [fc0] ]", 0),
        ("[[20200101 [fc0] ]]", 0),
        ("[[20200101 [ fc0 [1] ] ]]", 0),
        ("[20200101 [ fc0 [1] ] ],LOCALJOB [", 0),
        ("[20200101 [ fc0 [1] ] ],LOCALJOB ]", 0),
        ("[20200101 [ fc0 [1] ] ],LOCALJOB 1", 0),
        ("[20200101 [ fc0 [1] ] ],LOCALJOB [[1]]", 0),
        ("[20200101 [ fc0 [1] ] ],LOCALJOB [1-]", 0),
        ("[20200101 [ fc0 [1] ] ],LOCALJOB [1:]", 0),
        ("[20200101 [ fc0 [1] ] ],LOCALJOB [-]", 0),
        ("[20200101 [ fc0 [1] ] ],LOCALJOB [:]", 0),
        #
        # # Good ones // Testing chunk_formula
        ("[20200101 [ fc0 [1] ] ]", 9),
        ("[20200101 [ fc0 [1] fc1 [1] ] ]", 18),
        ("[20200101 [ fc0 [1] fc1 [1] ] 20200102 [ fc0 [1] fc1 [1] ] ]", 36),
        ("[20200101 [ fc0 [1-2] ] ]", 18),
        ("[20200101 [ fc0 [1-2] fc1 [1-2] ] ]", 36),
        ("[20200101 [ fc0 [1-2] fc1 [1-2] ] 20200102 [ fc0 [1-2] fc1 [1-2] ] ]", 72),
        ("[20200101 [ Any [1] ] ]", 18),
        ("[20200101 [ Any [1-2] ] ]", 36),
        ("[20200101 [ fc0 [Any] ] ]", 18),
        ("[20200101 [ fc0 [Any] fc1 [Any] ] ]", 36),
        ("[20200101 [ fc0 [Any] fc1 [Any] ] 20200102 [ fc0 [Any] fc1 [Any] ] ]", 72),
        ("[ Any [ fc0 [1] ] ]", 18),
        ("[ Any [ fc0 [1-2] ] ]", 36),
        ("[ Any [ fc0 [Any] ] ]", 36),
        ("[ Any [ fc0 [Any] fc1 [Any] ] ]", 72),
        ("[ Any [ fc0 [Any] fc1 [Any] ] 20200102 [ fc0 [Any] fc1 [Any] ] ]", 72),

        # Good ones // Testing sections and splits
        ("[20200101 [ fc0 [1] ] ],LOCALJOB", 3),
        ("[20200101 [ fc0 [1] ] ],Any", 9),
        ("[20200101 [ fc0 [1] ] ],LOCALJOB [2]", 1),
        ("[20200101 [ fc0 [1] ] ],LOCALJOB [1-2]", 2),
        ("[20200101 [ fc0 [1] ] ],LOCALJOB [1:2]", 2),
        ("[20200101 [ fc0 [1] ] ],Any [2]", 3),
        ("[20200101 [ fc0 [1] ] ],Any [1-2]", 6),
        ("[20200101 [ fc0 [1] ] ],Any [1:2]", 6),
        ("[20200101 [ fc0 [1] ] ],Any [Any]", 9),
        ("[20200101 [ fc0 [1] ] ],Any []", 9),

    ],
)
def test_set_status_ftcs(as_exp: object, ftcs_filter: str, expected_jobs: int):
    """Tests the setstatus command with various filters in an offline scenario.

    The conftest as_exp fixture has:
    - 2 dates: 20200101, 20200102
    - 2 members: fc0, fc1
    - Jobs sections: LOCALJOB, PSJOB, SLURMJOB
    - Each job has 3 splits

    :param as_exp: The autosubmit experiment fixture.
    :type as_exp: AutosubmitExperiment
    :param ftcs_filter: The filter to apply.
    :type ftcs_filter: str
    :param expected_jobs: The expected number of jobs to be set to COMPLETED.
    :type expected_jobs: int
    """

    db_manager = SqlAlchemyExperimentHistoryDbManager(as_exp.expid, BasicConfig.JOBDATA_DIR, f'job_data_{as_exp.expid}.db')
    db_manager.initialize()
    target = "COMPLETED"

    # TODO: We should have one single filter ( fc or fct or ftcs ). Or a different name as they now do the same.
    if not expected_jobs:
        with pytest.raises(AutosubmitCritical) as validation_err:
            do_setstatus(as_exp, fc=ftcs_filter, target=target)
        print(validation_err.value)
    else:
        job_list_ = do_setstatus(as_exp, fc=ftcs_filter, target=target)
        completed_jobs = [job.name for job in job_list_.get_job_list() if job.status == Status.COMPLETED]
        assert len(completed_jobs) == expected_jobs

    reset(as_exp, "WAITING")

    if not expected_jobs:
        with pytest.raises(AutosubmitCritical) as validation_err:
            do_setstatus(as_exp, fct=ftcs_filter, target=target)
        print(validation_err.value)
    else:
        job_list_ = do_setstatus(as_exp, fct=ftcs_filter, target=target)
        completed_jobs = [job.name for job in job_list_.get_job_list() if job.status == Status.COMPLETED]
        assert len(completed_jobs) == expected_jobs

    reset(as_exp, "WAITING")

    if not expected_jobs:
        with pytest.raises(AutosubmitCritical) as validation_err:
            do_setstatus(as_exp, ftcs=ftcs_filter, target=target)
        print(validation_err.value)
    else:
        job_list_ = do_setstatus(as_exp, ftcs=ftcs_filter, target=target)
        completed_jobs = [job.name for job in job_list_.get_job_list() if job.status == Status.COMPLETED]
        assert len(completed_jobs) == expected_jobs
