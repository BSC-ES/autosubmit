import pytest


# Moved from unit tests to integration tests as it builds jobs and platforms using YAML data, without mocks.
# It also touchs more than one module, making it more of an integration test.
# Probabily we could move other tests from unit to integration for the same reason.
# In this file we're testing job parameters, TODO probabily these tests can have less redudant code.

def test_hetjob(tmp_path, autosubmit_exp):
    """Test job platforms with a platform. Builds job and platform using YAML data, without mocks."""
    het_data: dict = {
        "JOBS": {
            "HETJOB_A": {
                "FILE": "a",
                "PLATFORM": "TEST_SLURM",
                "RUNNING": "once",
                "WALLCLOCK": "00:30",
                "MEMORY": [0, 0],
                "NODES": [3, 1],
                "TASKS": [32, 32],
                "THREADS": [4, 4],
            },
        },
        'PLATFORMS': {
            'TEST_SLURM': {
                'TYPE': 'slurm',
                'ADD_PROJECT_TO_HOST': 'False',
                'HOST': '127.0.0.1',
                'MAX_WALLCLOCK': '48:00',
                'PROJECT': 'group',
                'QUEUE': 'gp_debug',
                'SCRATCH_DIR': '/tmp/scratch',
                'TEMP_DIR': '',
                'USER': 'root',
                'PROCESSORS': '20',
                'MAX_PROCESSORS': '128',
                'PROCESSORS_PER_NODE': '128',
                "CUSTOM_DIRECTIVES": [
                    ["#SBATCH --export=ALL", "#SBATCH --distribution=block:cyclic", "#SBATCH --exclusive"],
                    ["#SBATCH --export=ALL", "#SBATCH --distribution=block:cyclic:fcyclic", "#SBATCH --exclusive"],
                ],
            },
        }
    }
    as_exp = autosubmit_exp(experiment_data=het_data, include_jobs=False, create=True)
    job_list = as_exp.autosubmit.load_job_list(as_exp.expid, as_exp.as_conf, monitor=False, new=True, full_load=True, submitter=None, check_failed_jobs=False)
    for job in job_list.get_job_list():
        parameters = job.update_parameters(as_exp.as_conf, set_attributes=True)
        job.update_content(as_exp.as_conf, parameters)

        # Asserts the script is valid. There shouldn't be variables in the script that aren't in the parameters.
        checked = job.check_script(as_exp.as_conf, parameters)
        assert checked


@pytest.mark.parametrize("reservation", [
    None,
    '',
    '  ',
    'some-string',
    'a',
    '123',
    'True',
])
def test_reservation(tmp_path, autosubmit_exp, reservation):
    """Test job platforms with a platform. Builds job and platform using YAML data, without mocks."""
    exp_data: dict = {
        "JOBS": {
            "A": {
                "FILE": "a",
                "PLATFORM": "TEST_SLURM",
                "RUNNING": "once",
                "WALLCLOCK": "00:30",
                "RESERVATION": reservation,
            },
        },
        'PLATFORMS': {
            'TEST_SLURM': {
                'TYPE': 'slurm',
                'ADD_PROJECT_TO_HOST': 'False',
                'HOST': '127.0.0.1',
                'MAX_WALLCLOCK': '48:00',
                'PROJECT': 'group',
                'QUEUE': 'gp_debug',
                'SCRATCH_DIR': '/tmp/scratch',
                'TEMP_DIR': '',
                'USER': 'root',
                'PROCESSORS': '20',
                'MAX_PROCESSORS': '128',
                'PROCESSORS_PER_NODE': '128',
            },
        }
    }
    as_exp = autosubmit_exp(experiment_data=exp_data, include_jobs=False, create=True)
    job_list = as_exp.autosubmit.load_job_list(as_exp.expid, as_exp.as_conf, monitor=False, new=True, full_load=True, submitter=None,
                                               check_failed_jobs=False)
    for job in job_list.get_job_list():
        parameters = job.update_parameters(as_exp.as_conf, set_attributes=True)
        job.update_content(as_exp.as_conf, parameters)

        # Asserts the script is valid. There shouldn't be variables in the script that aren't in the parameters.
        checked = job.check_script(as_exp.as_conf, parameters)
        assert checked

        if not reservation or (isinstance(reservation, str) and not reservation.strip()):
            template_content, additional_templates = job.update_content(as_exp.as_conf, parameters)
            assert not additional_templates
            assert '#SBATCH --reservation' not in template_content
        else:
            assert reservation == parameters['JOBS.A.RESERVATION']

            template_content, additional_templates = job.update_content(as_exp.as_conf, parameters)
            assert not additional_templates
            assert f'#SBATCH --reservation={reservation}' in template_content
