# Copyright 2015-2025 Earth Sciences Department, BSC-CNS
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

import pytest 

@pytest.mark.parametrize('experiment_config',[
    # Simple Workflow
    {
        'JOBS': {
            'SIM': {
                'RUNNING': 'once',
                'SCRIPT': 'echo "This is job ${SLURM_JOB_ID} with name ${SLURM_JOB_NAME}"',
            }
        },
    },

    # Dependency Workflow 
    {
        'JOBS': {
            'SIM': {
                'RUNNING': 'chunk',
                'SCRIPT': 'echo "This is job ${SLURM_JOB_ID} with name ${SLURM_JOB_NAME}"',
            },
            'SIM_2': {
                'RUNNING': 'chunk',
                'SCRIPT': 'echo "This is job ${SLURM_JOB_ID} with name ${SLURM_JOB_NAME}"',
                'DEPENDENCIES': 'SIM',
            },
        },
    },
], ids=[
    'Simple Workflow',
    'Dependency Workflow',
])
def test_succesful_run(
    autosubmit_exp, 
    experiment_data_factory,
    create_platform,
    platform_name: str,
    platform_type: str,
    expid: str,
    experiment_config: dict
):
    """Runs naive tests for different job configurations and platforms."""

    experiment_data = experiment_data_factory(experiment_config)

    exp = autosubmit_exp(expid, experiment_data=experiment_data)

    create_platform(expid, platform_name, exp.as_conf)

    exp.autosubmit._check_ownership_and_set_last_command(exp.as_conf, exp.expid, 'run')

    assert 0 == exp.autosubmit.run_experiment(expid), f"Experiment failed in platform {platform_type}"