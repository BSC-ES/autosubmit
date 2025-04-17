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

from autosubmit.platforms.paramiko_submitter import ParamikoSubmitter
from log.log import AutosubmitCritical


@pytest.mark.parametrize("config", [
    {
        "DEFAULT": {
            "HPCARCH": "PYTEST-UNDEFINED",
        },
        "LOCAL_ROOT_DIR": "blabla",
        "LOCAL_TMP_DIR": 'tmp',
        "PLATFORMS": {
            "PYTEST-UNDEFINED": {
                "host": "",
                "user": "",
                "project": "",
                "scratch_dir": "",
                "MAX_WALLCLOCK": "",
                "DISABLE_RECOVERY_THREADS": True
            }
        },
        "JOBS": {
            "job1": {
                "PLATFORM": "PYTEST-UNDEFINED",
                "SCRIPT": "echo 'hello world'",
            },
        }
    },
    {
        "DEFAULT": {
            "HPCARCH": "PYTEST-UNSUPPORTED",
        },
        "LOCAL_ROOT_DIR": "blabla",
        "LOCAL_TMP_DIR": 'tmp',
        "PLATFORMS": {
            "PYTEST-UNSUPPORTED": {
                "TYPE": "unknown",
                "host": "",
                "user": "",
                "project": "",
                "scratch_dir": "",
                "MAX_WALLCLOCK": "",
                "DISABLE_RECOVERY_THREADS": True
            }
        },
        "JOBS": {
            "job1": {
                "PLATFORM": "PYTEST-UNSUPPORTED",
                "SCRIPT": "echo 'hello world'",
            },
        }
    }
], ids=["Undefined", "Unsupported"])
def test_load_platforms(autosubmit_config, config):
    experiment_id = 'random-id'
    as_conf = autosubmit_config(experiment_id, config)
    submitter = ParamikoSubmitter()
    with pytest.raises(AutosubmitCritical):
        submitter.load_platforms(as_conf)
