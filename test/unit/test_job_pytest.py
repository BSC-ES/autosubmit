import pytest

from autosubmit.job.job import Job
from autosubmit.platforms.psplatform import PsPlatform


def create_job_and_update_parameters(autosubmit_config, experiment_data):
    as_conf = autosubmit_config("test-expid", experiment_data)
    as_conf.experiment_data = as_conf.deep_normalize(as_conf.experiment_data)
    as_conf.experiment_data = as_conf.normalize_variables(as_conf.experiment_data, must_exists=True)
    as_conf.experiment_data = as_conf.deep_read_loops(as_conf.experiment_data)
    as_conf.experiment_data = as_conf.substitute_dynamic_variables(as_conf.experiment_data)
    as_conf.experiment_data = as_conf.parse_data_loops(as_conf.experiment_data)
    # Create some jobs
    job = Job('A', '1', 0, 1)
    platform = PsPlatform(expid='a000', name='DUMMY_PLATFORM', config=as_conf.experiment_data)
    job.section = 'RANDOM-SECTION'
    job.platform = platform
    job.update_parameters(as_conf, {})
    return job


@pytest.mark.parametrize('experiment_data, expected_data', [(
    {
        'JOBS': {
            'RANDOM-SECTION': {
                'FILE': "test.sh",
                'PLATFORM': 'DUMMY_PLATFORM',
                'TEST': "%other%",
            },
        },
        'PLATFORMS': {
            'dummy_platform': {
                'type': 'ps',
                'whatever': 'dummy_value',
                'whatever2': 'dummy_value2',
                'CUSTOM_DIRECTIVES': ['$SBATCH directive1', '$SBATCH directive2'],
            },
        },
        'OTHER': "%CURRENT_WHATEVER%/%CURRENT_WHATEVER2%",
        'ROOTDIR': 'dummy_rootdir',
        'LOCAL_TMP_DIR': 'dummy_tmpdir',
        'LOCAL_ROOT_DIR': 'dummy_rootdir',
    },
    {
        'CURRENT_FILE': "test.sh",
        'CURRENT_PLATFORM': 'DUMMY_PLATFORM',
        'CURRENT_WHATEVER': 'dummy_value',
        'CURRENT_WHATEVER2': 'dummy_value2',
        'CURRENT_TEST': 'dummy_value/dummy_value2',

    }
)])
def test_update_parameters_current_variables(autosubmit_config, experiment_data, expected_data):
    job = create_job_and_update_parameters(autosubmit_config, experiment_data)
    for key, value in expected_data.items():
        assert job.parameters[key] == value


@pytest.mark.parametrize('experiment_data, attributes_to_check', [(
    {
        'JOBS': {
            'RANDOM-SECTION': {
                'FILE': "test.sh",
                'PLATFORM': 'DUMMY_PLATFORM',
                'NOTIFY_ON': 'COMPLETED',
            },
        },
        'PLATFORMS': {
            'dummy_platform': {
                'type': 'ps',
            },
        },
        'ROOTDIR': 'dummy_rootdir',
        'LOCAL_TMP_DIR': 'dummy_tmpdir',
        'LOCAL_ROOT_DIR': 'dummy_rootdir',
    },
    {'notify_on': ['COMPLETED']}
)])
def test_update_parameters_attributes(autosubmit_config, experiment_data, attributes_to_check):
    job = create_job_and_update_parameters(autosubmit_config, experiment_data)
    for attr in attributes_to_check:
        assert hasattr(job, attr)
        assert getattr(job, attr) == attributes_to_check[attr]
