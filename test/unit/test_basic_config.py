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
    "user_config, home_user_config, etc_rc, legacy_etc_rc",
    [
        [True, True, True, True],
        [True, True, True, False],
        [True, True, False, True],
        [True, True, False, False],
        [True, False, True, True],
        [True, False, True, False],
        [True, False, False, True],
        [True, False, False, False],
        [False, True, True, True],
        [False, True, True, False],
        [False, True, False, True],
        [False, True, False, False],
        [False, False, True, True],
        [False, False, True, False],
        [False, False, False, True],
        [False, False, False, False]
    ],
)
def test_read_loads_etc_files_with_priority(user_config, home_user_config, etc_rc, legacy_etc_rc):
    """Test read precedence among local, home and /etc rc files."""
    with patch.dict(os.environ, {}, clear=True):
        with patch("autosubmit.config.basicconfig.os.path.exists") as mock_exists:
            with patch(
                "autosubmit.config.basicconfig.BasicConfig._BasicConfig__read_file_config"
            ) as mock_read:
                with patch(
                    "autosubmit.config.basicconfig.BasicConfig._update_config", Mock()
                ):
                    with patch("autosubmit.config.basicconfig.Log.warning") as mock_log_warning:
                        filename = "autosubmitrc"
                        user_config_path = os.path.join("", "." + filename)
                        home_user_config_path = os.path.join(os.path.expanduser("~"), "." + filename)
                        etc_rc_path = os.path.join("/etc", filename)
                        legacy_etc_rc_path = os.path.join("/etc", "." + filename)

                        mock_exists.side_effect = lambda path: (
                            (user_config and path == os.path.join("", "." + filename)) or
                            (home_user_config and path == os.path.join(os.path.expanduser("~"), "." + filename)) or
                            (etc_rc and path == os.path.join("/etc", filename)) or
                            (legacy_etc_rc and path == os.path.join("/etc", "." + filename))
                        )

                        BasicConfig.read()

                        expected_read_calls = []
                        if user_config:
                            expected_read_calls = [call(user_config_path)]
                        elif home_user_config:
                            expected_read_calls = [call(home_user_config_path)]
                        else:
                            if legacy_etc_rc:
                                expected_read_calls.append(call(legacy_etc_rc_path))
                            if etc_rc:
                                expected_read_calls.append(call(etc_rc_path))

                        assert mock_read.call_args_list == expected_read_calls

                        if (not user_config) and (not home_user_config) and legacy_etc_rc:
                            mock_log_warning.assert_called_once_with(
                                "The legacy configuration file /etc/.autosubmitrc is deprecated and will be removed in future versions. Please, rename it to /etc/autosubmitrc"
                            )
                        else:
                            mock_log_warning.assert_not_called()


def test_read_overwrites_config_with_etc_files(tmp_path):
    """
    Precedence: if two autosubmitrc files exist,
    the /etc/autosubmitrc version should take precedence over the /etc/.autosubmitrc version
    """
    filename = "autosubmitrc"
    legacy_etc_rc = tmp_path / ("." + filename)
    etc_rc = tmp_path / filename

    legacy_db_dir = tmp_path / "legacy.db"
    etc_db_dir = tmp_path / "etc.db"

    legacy_etc_rc.write_text(
        dedent(f"""
        [database]
        path = {legacy_db_dir}
        filename = legacy.db
    """)
    )

    with open(etc_rc, "w") as f:
        f.write(
            dedent(f"""
        [database]
        path = {etc_db_dir}
        filename = etc.db
    """)
        )

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
