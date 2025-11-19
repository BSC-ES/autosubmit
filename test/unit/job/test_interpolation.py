from autosubmit.job.job import Job
from autosubmitconfigparser.config.configcommon import AutosubmitConfig


def test_hpcrootdir_interpolation(autosubmit_config):
    as_conf = autosubmit_config('zz11', {
        'JOBS': {
            'A': {
                'PLATFORM': 'mare',
                'SCRIPT': 'sleep 999',
                'RUNNING': 'once'
            }
        },
        'PLATFORMS': {
            'mare': {
                'TYPE': 'slurm',
                'HOST': 'localhost',
                'USER': 'root'
            }
        }
    })

    assert 'ROOTDIR' not in as_conf.experiment_data
    assert 'HPCROOTDIR' not in as_conf.experiment_data
    j = Job(name='bla', job_id='bla')