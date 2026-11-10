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

# TODO Improve this in the future.
from contextlib import nullcontext as does_not_raise


def test_check_conf_files(autosubmit_config):
    config_dict = {
        "CONFIG": {
            "AUTOSUBMIT_VERSION": "4.1.12",
            "TOTALJOBS": 20,
            "MAXWAITINGJOBS": 20
        },
        "DEFAULT": {
            "EXPID": "",
            "HPCARCH": "",
            "CUSTOM_CONFIG": ""
        },
        "PROJECT": {
            "PROJECT_TYPE": "git",
            "PROJECT_DESTINATION": "git_project"
        },
        "GIT": {
            "PROJECT_ORIGIN": "",
            "PROJECT_BRANCH": "",
            "PROJECT_COMMIT": "",
            "PROJECT_SUBMODULES": "",
            "FETCH_SINGLE_BRANCH": True
        },
        "JOBS": {
            "JOB1": {
                "WALLCLOCK": "01:00",
                "PLATFORM": "test"
            }
        },
        "PLATFORMS": {
            "test": {
                "MAX_WALLCLOCK": "01:30"
            }
        },
    }

    as_conf = autosubmit_config(expid='t000', experiment_data=config_dict)
    with does_not_raise():
        as_conf.check_conf_files()
