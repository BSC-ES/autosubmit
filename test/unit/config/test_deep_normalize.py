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


@pytest.mark.parametrize(
    "data, expected_data",
    [
        (
                {
                    "JOBS": {
                        "job1": {
                            "FOR": {
                                "NAME": "var",
                                "lowercase": True
                            }
                        }
                    },
                    "var": ["test", "test2", "test3"]
                },
                {
                    "JOBS": {
                        "JOB1": {
                            "FOR": {
                                "NAME": "var",
                                "LOWERCASE": True
                            }
                        }
                    },
                    "VAR": ["test", "test2", "test3"]
                }
        ),
        (
                {
                    "FOR": {
                        "DEPENDENCIES": [
                            {
                                "APP_ENERGY_ONSHORE": {
                                    "SPLITS_FROM": {
                                        "all": {
                                            "SPLITS_TO": "previous"
                                        }
                                    }
                                }
                            }
                        ]
                    },
                    "foo": ["bar", "baz"],
                    "1": ["one", "two"],
                    "3": "three"
                },
                {
                    "FOR": {
                        "DEPENDENCIES": [
                            {
                                "APP_ENERGY_ONSHORE": {
                                    "SPLITS_FROM": {
                                        "ALL": {
                                            "SPLITS_TO": "previous"
                                        }
                                    }
                                }
                            }
                        ]
                    },
                    "FOO": ["bar", "baz"],
                    "1": ["one", "two"],
                    "3": "three"
                }
        )
    ]
)
def test_normalize_variables(autosubmit_config, data, expected_data):
    as_conf = autosubmit_config(expid='t000', experiment_data=data)
    normalized_data = as_conf.deep_normalize(data)
    assert normalized_data == expected_data
    normalized_data = as_conf.deep_normalize(normalized_data)
    assert normalized_data == expected_data
