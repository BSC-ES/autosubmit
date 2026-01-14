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

"""Autosubmit tests.

These folders contain all the tests run to ensure the quality of the code in the Autosubmit tool,
here you can find Unit, Integration, and Regression Tests.
"""

import os

import pytest


# This might help when you have no clue about what's happening.
# from autosubmit.log.log import Log
# Log.set_console_level('DEBUG')


@pytest.fixture(scope='session', autouse=True)
def disable_system_clear(session_mocker):
    """Fixture that disables ``os.system('clear')``.

    Autosubmit clears the terminal in certain command calls, which makes
    a lot harder to investigate when you chained shell commands with ``&&``
    or you have multiple Pytest tests in a session, as you will have only
    the last log in the terminal.
    """
    real_os_system = os.system

    def ignore_clear(cmd):
        if cmd == "clear":
            return 0
        return real_os_system(cmd)

    session_mocker.patch("os.system", side_effect=ignore_clear)
