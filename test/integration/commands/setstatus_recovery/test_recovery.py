import pytest
@pytest.mark.slurm
def test_recovery(autosubmit_exp, general_data, experiment_data, jobs_data):
    """
    Test the recovery of an experiment.

    :param as_exp: An AutosubmitExperiment instance.
    :type as_exp: AutosubmitExperiment
    :param general_data: General configuration data.
    :type general_data: Dict[str, object]
    :param experiment_data: Experiment-specific configuration data.
    :type experiment_data: Dict[str, object]
    :param jobs_data: Job-specific configuration data.
    :type jobs_data: Dict[str, object]
    """
    config_data = general_data | experiment_data | jobs_data

    autosubmit_exp(experiment_data=config_data, include_jobs=False, create=True)
    # Todo the actual test
