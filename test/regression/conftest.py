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

"""Fixtures for regression tests."""

import pytest


@pytest.fixture(scope='session', autouse=True)
def experiment_config_fixture(session_mocker):
    # TODO: There are unit tests that fail without this fixture. Those unit tests are good candidates
    #       to be rewritten or made into integration tests without mocks.
    session_mocker.patch(
        'autosubmit.config.configcommon.get_experiment_description',
        return_value=[['test experiment']]
    )

