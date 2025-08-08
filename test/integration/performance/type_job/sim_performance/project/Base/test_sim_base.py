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

class TestSimPerformanceBaseNaive:
    """
    Naive tests for Base SIM performance class.
    """

    @pytest.mark.parametrize('experiment_config, mail_expected', [
        # Simple Workflow - Mail sent (performance below threshold)
        ({
            'EXPERIMENT': {
                'DATELIST': '20000101',
                'MEMBERS': 'fc0',
                'CHUNKSIZEUNIT': 'day',
                'CHUNKSIZE': '1',
                'NUMCHUNKS': '2',
                'CHUNKINI': '',
                'CALENDAR': 'standard',
            },
            'JOBS': {
                'SIM': {
                    'RUNNING': 'chunk',
                    'SCRIPT': 'sleep 5; echo "Slow simulation"',  # Slow script - performance will be below threshold (1.0)
                    'WALLCLOCK': '00:10',
                }
            },
            'PERFORMANCE': {
                'PROJECT': 'DEFAULT',
                'SECTION': ['SIM'],
                'NOTIFY_ON': ['COMPLETED'],
                'NOTIFY_TO': ['test@example.com'],
            },
        }, True),
        
        # Simple Workflow - No mail sent (performance above threshold)
        ({
            'EXPERIMENT': {
                'DATELIST': '20000101',
                'MEMBERS': 'fc0',
                'CHUNKSIZEUNIT': 'hour',
                'CHUNKSIZE': '1',
                'NUMCHUNKS': '2',
                'CHUNKINI': '',
                'CALENDAR': 'standard',
            },
            'JOBS': {
                'SIM': {
                    'RUNNING': 'chunk',
                    'SCRIPT': 'echo "Fast simulation"',  # Fast script - performance will be above threshold (1.0)
                    'WALLCLOCK': '00:10',
                }
            },
            'PERFORMANCE': {
                'PROJECT': 'DEFAULT',
                'SECTION': ['SIM'],
                'NOTIFY_ON': ['COMPLETED'],
                'NOTIFY_TO': ['test@example.com'],
            },
        }, False),
    ], ids=[
        'Simple Workflow - Performance Below Threshold (Mail Sent)',
        'Simple Workflow - Performance Above Threshold (No Mail)',
    ])
    def test_simple_workflow_performance_notification(
        self,
        autosubmit_exp, 
        experiment_data_factory,
        create_platform,
        platform_name: str,
        platform_type: str,
        expid: str,
        experiment_config: dict,
        mail_expected: bool,
        check_mail_sent
    ):
        """Test simple workflow performance monitoring and mail notification."""

        experiment_data = experiment_data_factory(experiment_config)

        exp = autosubmit_exp(expid, experiment_data=experiment_data)

        create_platform(expid, platform_name, exp.as_conf)

        exp.autosubmit._check_ownership_and_set_last_command(exp.as_conf, exp.expid, 'run')

        assert 0 == exp.autosubmit.run_experiment(expid), f"Experiment failed on platform {platform_type}"

        assert check_mail_sent() == mail_expected, f"Mail notification expectation failed. Expected: {mail_expected}, Got: {check_mail_sent()}"


    @pytest.mark.parametrize('experiment_config, mail_expected', [
        # Complex Workflow - Mail sent (performance below threshold)
        ({
            'EXPERIMENT': {
                'DATELIST': '20000101 20000102',
                'MEMBERS': 'fc0',
                'CHUNKSIZEUNIT': 'day',
                'CHUNKSIZE': '1',
                'NUMCHUNKS': '3',
                'CHUNKINI': '',
                'CALENDAR': 'standard',
            },
            'JOBS': {
                'SIM': {
                    'RUNNING': 'chunk',
                    'SCRIPT': 'sleep 10; echo "Complex slow simulation"',  # Very slow - performance will be below threshold
                    'WALLCLOCK': '00:10',
                },
                'SIM_2': {
                    'RUNNING': 'chunk',
                    'SCRIPT': 'sleep 8; echo "Dependent slow simulation"',  # Also slow
                    'DEPENDENCIES': 'SIM',
                    'WALLCLOCK': '00:10',
                },
            },
            'PERFORMANCE': {
                'PROJECT': 'DEFAULT',
                'SECTION': ['SIM'],  # Both SIM and SIM_2 are in SIM section
                'NOTIFY_ON': ['COMPLETED'],
                'NOTIFY_TO': ['test@example.com', 'admin@example.com'],
            },
        }, True),
        
        # Complex Workflow - No mail sent (performance above threshold)
        ({
            'EXPERIMENT': {
                'DATELIST': '20000101 20000102',
                'MEMBERS': 'fc0',
                'CHUNKSIZEUNIT': 'hour',
                'CHUNKSIZE': '12',  # Larger chunk size with hour unit - easier to exceed threshold
                'NUMCHUNKS': '3',
                'CHUNKINI': '',
                'CALENDAR': 'standard',
            },
            'JOBS': {
                'SIM': {
                    'RUNNING': 'chunk',
                    'SCRIPT': 'echo "Fast complex simulation"',  # Fast script
                    'WALLCLOCK': '00:10',
                },
                'SIM_2': {
                    'RUNNING': 'chunk',
                    'SCRIPT': 'echo "Fast dependent simulation"',  # Fast script
                    'DEPENDENCIES': 'SIM',
                    'WALLCLOCK': '00:10',
                },
            },
            'PERFORMANCE': {
                'PROJECT': 'DEFAULT',
                'SECTION': ['SIM'],
                'NOTIFY_ON': ['COMPLETED'],
                'NOTIFY_TO': ['test@example.com', 'admin@example.com'],
            },
        }, False),
    ], ids=[
        'Complex Workflow - Performance Below Threshold (Mail Sent)',
        'Complex Workflow - Performance Above Threshold (No Mail)',
    ])
    def test_complex_workflow_performance_notification(
        self,
        autosubmit_exp, 
        experiment_data_factory,
        create_platform,
        platform_name: str,
        platform_type: str,
        expid: str,
        experiment_config: dict,
        mail_expected: bool,
        check_mail_sent,
        mocker
    ):
        """Test complex workflow performance monitoring and mail notification."""

        print('Here we go!')

        experiment_data = experiment_data_factory(experiment_config)

        exp = autosubmit_exp(expid, experiment_data=experiment_data)

        create_platform(expid, platform_name, exp.as_conf)

        exp.autosubmit._check_ownership_and_set_last_command(exp.as_conf, exp.expid, 'run')

        assert 0 == exp.autosubmit.run_experiment(expid), f"Experiment failed on platform {platform_type}"
        
        assert check_mail_sent() == mail_expected, f"Mail notification expectation failed. Expected: {mail_expected}, Got: {check_mail_sent()}"