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

import pytest


@pytest.mark.parametrize(
    'create_wrappers',
    [True, False]
)
def test_job_platform_is_a_placeholder(create_wrappers: bool, autosubmit_exp):
    exp = autosubmit_exp(experiment_data={
        'DEFAULT': {
            'HPCARCH': 'MARENOSTRUM5'
        },
        'PLATFORMS': {
            'MARENOSTRUM5': {
                'HOST': 'localhost',
                'USER': 'user',
                'TYPE': 'ps',
                'OFFLINE_DN_PLATFORM': 'marenostrum5-transfer'
            },
            'MARENOSTRUM5-TRANSFER': {
                'HOST': 'localhost',
                'USER': 'user',
                'TYPE': 'ps'
            }
        },
        'JOBS': {
            'DN': {
                'SCRIPT': 'echo "%DEFAULT.HPCARCH%"',
                'PLATFORM': '%HPCOFFLINE_DN_PLATFORM%'
            }
        }
    }, wrapper=create_wrappers)

    assert exp.as_conf.jobs_data['DN']['SCRIPT'] == 'echo "MARENOSTRUM5"'
    assert exp.as_conf.jobs_data['DN']['PLATFORM'].lower() == 'marenostrum5-transfer'
