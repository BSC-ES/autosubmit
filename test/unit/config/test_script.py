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


def test_check_jobs_conf_script_correct(autosubmit_config):
    as_conf = autosubmit_config(expid='a000', experiment_data={
        "JOBS":
            {
                "JOB2": {
                    "SCRIPT": "hello",
                    "RUNNING": "once"
                },
                "JOB1": {
                    "SCRIPT": "hello",
                    "DEPENDENCIES": {"JOB2": {}},
                    "RUNNING": "once"
                },
            }
    }, ignore_file_path=True)

    result = as_conf.check_jobs_conf()
    assert result is True
    assert not as_conf.wrong_config
    assert not as_conf.warn_config


@pytest.mark.parametrize(
    'experiment_data',
    [
        # FILE instead of SCRIPT
        ({
            "JOBS": {
                "JOB2": {
                    "SCRIPT": "hello",
                    "RUNNING": "once"
                },
                "JOB1": {
                    "File": "file",
                    "DEPENDENCIES": {"JOB2": {}},
                    "RUNNING": "once"
                },
            }
        }),
        # No FILE and no SCRIPT
        ({
            "JOBS": {
                "JOB2": {
                    "SCRIPT": "hello",
                    "RUNNING": "once"
                },
                "JOB1": {
                    "DEPENDENCIES": {"JOB2": {}},
                    "RUNNING": "once"
                },
            }
        })]
)
def test_check_jobs_conf_script_error(autosubmit_config, experiment_data):
    as_conf = autosubmit_config(expid='a000', experiment_data=experiment_data, ignore_file_path=True)
    result = as_conf.check_jobs_conf()
    assert result is False
    assert len(as_conf.wrong_config['Jobs']) == 1
    assert 'Mandatory FILE parameter not found' in as_conf.wrong_config['Jobs'][0][1]
    assert not as_conf.warn_config['Jobs']
