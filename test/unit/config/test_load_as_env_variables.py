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

import os

import pytest


@pytest.mark.parametrize(
    "data,expected_data,sudo_user",
    [
        (
                {
                    "DEFAULT": {
                        "CUSTOM_CONFIG": {
                            "POST": "%AS_ENV_PLATFORMS_PATH%"
                        }
                    },
                },
                {
                    "DEFAULT": {
                        "CUSTOM_CONFIG": {
                            "POST": "%AS_ENV_PLATFORMS_PATH%"
                        }
                    },
                    "AS_ENV_PLATFORMS_PATH": "test",
                    "AS_ENV_CURRENT_USER": "dummy",
                },
                False,
        ),
        (
                {
                    "DEFAULT": {
                        "CUSTOM_CONFIG": {
                            "POST": "%AS_ENV_PLATFORMS_PATH%"
                        }
                    },
                },
                {
                    "DEFAULT": {
                        "CUSTOM_CONFIG": {
                            "POST": "%AS_ENV_PLATFORMS_PATH%"
                        }
                    },
                    "AS_ENV_PLATFORMS_PATH": "test",
                    "AS_ENV_CURRENT_USER": "test_user",
                },
                True,
        ),
    ],
    ids=["Check environment variables with SUDO", "Check environment variables without SUDO"]
)
def test_as_env_variables(autosubmit_config, data, expected_data, sudo_user):
    os.environ["AS_ENV_PLATFORMS_PATH"] = "test"
    if sudo_user:
        os.environ["SUDO_USER"] = "test_user"
    else:
        os.environ.pop("SUDO_USER", None)
        os.environ["USER"] = "dummy"
    as_conf = autosubmit_config(
        expid='a000',
        experiment_data=data)
    as_conf.experiment_data = as_conf.load_as_env_variables(as_conf.experiment_data)

    assert as_conf.experiment_data['DEFAULT']['CUSTOM_CONFIG'] == expected_data['DEFAULT']['CUSTOM_CONFIG']

    for k, v in {k: v for k, v in expected_data.items() if k != 'DEFAULT'}.items():
        assert as_conf.experiment_data[k] == v
