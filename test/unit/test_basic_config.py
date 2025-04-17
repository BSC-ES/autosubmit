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
from mock import Mock
from mock import patch

from autosubmitconfigparser.config.basicconfig import BasicConfig

"""TODO: This class has a static private (__named) method which is impossible to be tested.

IMHO this kind of static private methods are not a good practise in terms of testing.

Read about this on the below article:
https://googletesting.blogspot.com.es/2008/12/static-methods-are-death-to-testability.html
"""


def test_update_config_set_the_right_db_path():
    # arrange
    BasicConfig.DB_PATH = 'fake-path'
    # act
    BasicConfig._update_config()
    # assert
    assert os.path.join(BasicConfig.DB_DIR, BasicConfig.DB_FILE) == BasicConfig.DB_PATH


def test_read_makes_the_right_method_calls():
    # arrange
    with patch('autosubmitconfigparser.config.basicconfig.BasicConfig._update_config', Mock()):
        # act
        BasicConfig.read()
        # assert
        BasicConfig._update_config.assert_called_once_with()  # type: ignore
