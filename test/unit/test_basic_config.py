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
from mock import Mock, patch, call
from textwrap import dedent

import pytest

from autosubmit.config.basicconfig import BasicConfig

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
    with patch('autosubmit.config.basicconfig.BasicConfig._update_config', Mock()):
        # act
        BasicConfig.read()
        # assert
        BasicConfig._update_config.assert_called_once_with()  # type: ignore

@pytest.mark.parametrize(
    'etc_rc, legacy_etc_rc',
    [
        [os.path.join("/etc", "autosubmitrc"), os.path.join("/etc", ".autosubmitrc")],
        [os.path.join("/etc", "autosubmitrc"), None],
        [None, os.path.join("/etc", ".autosubmitrc")]
    ]
)

def test_read_loads_etc_files_with_priority(
    etc_rc,
    legacy_etc_rc
):
    """
    Test that the read method loads configuration files with the correct priority.
    """
    # mock the os.environ dictionary to empty values to prevent entering the first conditional
    with patch.dict(os.environ, {}, clear=True):
        # mock os.path.exists
        with patch("autosubmit.config.basicconfig.os.path.exists") as mock_exists:
            # mock __read_file_config
            # As it's a private static method, access it with name mangling: _BasicConfig__read_file_config
            with patch(
                "autosubmit.config.basicconfig.BasicConfig._BasicConfig__read_file_config"
            ) as mock_read:
                # mock _update_config
                with patch(
                    "autosubmit.config.basicconfig.BasicConfig._update_config", Mock()
                ):
                    filename = "autosubmitrc"
                    local_rc = os.path.join("", "." + filename)
                    home_rc = os.path.join(os.path.expanduser("~"), "." + filename)
                    legacy_etc_rc = legacy_etc_rc
                    etc_rc = etc_rc
                    # mock os.path.exists to return True for the two /etc files and False for the others
                    mock_exists.side_effect = lambda x: x in [etc_rc, legacy_etc_rc]

                    BasicConfig.read()

                    if etc_rc and legacy_etc_rc:
                        # assert and check that the files are checked in the right order
                        mock_exists.assert_has_calls(
                            [
                                call(local_rc),
                                call(home_rc),
                                call(etc_rc),
                            ],
                            any_order=False,
                        )
                        assert mock_read.call_args_list == [
                            call(etc_rc)
                        ]
                        assert mock_read.call_count == 1
                    
                    elif legacy_etc_rc and not etc_rc:
                        # if only legacy_etc_rc exists only legacy_etc_rc should be read
                        mock_exists.assert_called_with(legacy_etc_rc)
                        assert mock_read.call_args_list == [
                            call(legacy_etc_rc)
                        ]
                        assert mock_read.call_count == 1
                    
                    elif etc_rc and not legacy_etc_rc:
                        # if only etc_rc exists -> only etc_rc should be read
                        mock_exists.assert_called_with(etc_rc)
                        assert mock_read.call_args_list == [
                            call(etc_rc)
                        ]
                        assert mock_read.call_count == 1
                    
                    else:
                        # if none of the files exist no file should be read
                        mock_exists.assert_has_calls(
                            [
                                call(etc_rc),
                                call(legacy_etc_rc),
                            ],
                            any_order=False,
                        )
                        assert mock_read.call_count == 0


def test_read_overwrites_config_with_etc_files(tmp_path):
    """
    Precedence: if two autosubmitrc files exist, 
    the /etc/ version should take precedence over the /etc/.autosubmitrc version
    """
    filename = "autosubmitrc"
    legacy_etc_rc =  tmp_path / ("." + filename)
    etc_rc = tmp_path / filename

    legacy_db_dir = tmp_path / "legacy.db"
    etc_db_dir = tmp_path / "etc.db"

    legacy_etc_rc.write_text(dedent(f"""
        [database]
        path = {legacy_db_dir}
        filename = legacy.db
    """))
        
    with open(etc_rc, 'w') as f:
        f.write(dedent(f"""
        [database]
        path = {etc_db_dir}
        filename = etc.db
    """))

    # original values
    original_db_dir = BasicConfig.DB_DIR
    original_db_file = BasicConfig.DB_FILE
    original_db_path = BasicConfig.DB_PATH
    original_config_file_found = BasicConfig.CONFIG_FILE_FOUND
    try:
        # reset config to force reading the files again
        BasicConfig.CONFIG_FILE_FOUND = False
        
        # act: read files in order: legacy first, modern second
        BasicConfig._BasicConfig__read_file_config(str(legacy_etc_rc))
        BasicConfig._BasicConfig__read_file_config(str(etc_rc))
        BasicConfig._update_config()

        # assert
        assert BasicConfig.CONFIG_FILE_FOUND is True
        assert BasicConfig.DB_DIR == str(etc_db_dir)
        assert BasicConfig.DB_FILE == "etc.db"
        assert BasicConfig.DB_PATH == os.path.join(str(etc_db_dir), "etc.db")

    finally:
        # restore original values
        BasicConfig.DB_DIR = original_db_dir
        BasicConfig.DB_FILE = original_db_file
        BasicConfig.DB_PATH = original_db_path
        BasicConfig.CONFIG_FILE_FOUND = original_config_file_found
