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

from autosubmit.log.log import AutosubmitCritical


@pytest.mark.parametrize("data, must_fail", [
    pytest.param(
        {
            "JOBS": {
                "job1": {
                    "WALLCLOCK": "00:20",
                }
            }
        },
        False,
        id="Default wallclock no platform wallclock"
    ),
    pytest.param(
        {
            "JOBS": {
                "job1": {
                    "WALLCLOCK": "48:00",
                }
            }
        },
        True,
        id="Higher wallclock than default"
    ),
    pytest.param(
        {
            "JOBS": {
                "job1": {
                    "WALLCLOCK": "01:50",
                    "PLATFORM": "test"
                }
            },
            "PLATFORMS": {
                "test": {
                    "MAX_WALLCLOCK": "01:30"
                }
            }
        },
        True,
        id="Higher wallclock than platform"
    ),
    pytest.param(
        {
            "JOBS": {
                "job1": {
                    "WALLCLOCK": "01:00",
                    "PLATFORM": "test"
                }
            },
            "PLATFORMS": {
                "test": {
                    "MAX_WALLCLOCK": "01:30"
                }
            }
        },
        False,
        id="Lower wallclock than platform"
    ),
    pytest.param(
        {
            "JOBS": {
                "job1": {
                    "PLATFORM": "test"
                }
            },
            "PLATFORMS": {
                "test": {
                    "MAX_WALLCLOCK": "01:30"
                }
            }
        },
        False,
        id="Empty wallclock"
    ),
])
def test_validate_conf(autosubmit_config, data: dict, must_fail):
    as_conf = autosubmit_config(expid='t000', experiment_data=data)
    if must_fail:
        with pytest.raises(AutosubmitCritical):
            as_conf.validate_config(True)
    else:
        assert as_conf.validate_config(True)
    assert as_conf.validate_config(False) is not must_fail
