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

from pathlib import Path
from typing import Union

import pytest

from autosubmit.helpers.utils import get_rc_path, recover_stale_job_data, strtobool, user_yes_no_query
from autosubmit.history.experiment_history import ExperimentHistory


@pytest.mark.parametrize(
    'val,expected',
    [
        # yes
        ('y', True),
        ('yes', True),
        ('t', True),
        ('true', True),
        ('on', True),
        ('1', True),
        ('YES', True),
        ('TrUE', True),
        # no
        ('no', False),
        ('n', False),
        ('f', False),
        ('F', False),
        ('false', False),
        ('off', False),
        ('OFF', False),
        ('0', False),
        # invalid
        ('Yay', ValueError),
        ('Nay', ValueError),
        ('Nah', ValueError),
        ('2', ValueError),
    ]
)
def test_strtobool(val, expected):
    if expected is ValueError:
        with pytest.raises(expected):
            strtobool(val)
    else:
        assert expected == strtobool(val)


@pytest.mark.parametrize(
    'expected,machine,local,env_vars',
    [
        (Path('/tmp/hello/scooby/doo/ooo.txt'), True, True, {
            'AUTOSUBMIT_CONFIGURATION': '/tmp/hello/scooby/doo/ooo.txt'
        }),
        (Path('/etc/autosubmitrc'), True, True, {}),
        (Path('/etc/autosubmitrc'), True, False, {}),
        (Path('./.autosubmitrc'), False, True, {}),
        (Path(Path.home(), '.autosubmitrc'), False, False, {})
    ],
    ids=[
        'Use env var',
        'Use machine, even if local is true',
        'Use machine',
        'Use local',
        'Use home'
    ]
)
def test_get_rc_path(expected: Path, machine: bool, local: bool, env_vars: dict, mocker):
    mocker.patch.dict('autosubmit.helpers.utils.os.environ', env_vars, clear=True)

    assert expected == get_rc_path(machine, local)


@pytest.mark.parametrize(
    'answer,expected_or_error',
    [
        ('y', True),
        ('n', False),
        ('', Exception)
    ]
)
def test_user_yes_no_query(answer: str, expected_or_error: Union[bool, Exception], mocker):
    mocked_sys = mocker.patch('autosubmit.helpers.utils.sys')
    if expected_or_error is ValueError:
        mocker.patch('autosubmit.helpers.utils.input', return_value=[expected_or_error, 'y'])
        answer = user_yes_no_query(answer)
        assert mocked_sys.stdout.write.call_count == 2
        assert 'Please respond with ' in mocked_sys.stdout.write.call_args_list[0][1][0]
        assert answer
    if expected_or_error is Exception:
        mocker.patch('autosubmit.helpers.utils.input', return_value=expected_or_error)
        with pytest.raises(expected_or_error):  # type: ignore
            user_yes_no_query('Sure?')
    else:
        mocker.patch('autosubmit.helpers.utils.input', return_value=answer)
        assert expected_or_error == user_yes_no_query('Sure?')


def test_recover_stale_job_data_no_db(tmp_path, mocker):
    """recover_stale_job_data returns early when the job_data DB file does not exist."""
    mocker.patch('autosubmit.helpers.utils.BasicConfig.JOBDATA_DIR', str(tmp_path))
    recover_stale_job_data("a000", mocker.MagicMock())


@pytest.mark.parametrize("has_stale", [False, True], ids=["no_stale", "with_stale"])
def test_recover_stale_job_data_stale_variants(tmp_path, mocker, has_stale):
    """recover_stale_job_data handles empty and non-empty stale results."""
    mocker.patch('autosubmit.helpers.utils.BasicConfig.JOBDATA_DIR', str(tmp_path))
    Path(tmp_path / "job_data_a000.db").touch()
    mocker.patch.object(ExperimentHistory, 'get_stale_rows', return_value=(
        [mocker.MagicMock(platform="TEST", job_name="a000_j1", fail_count=0)] if has_stale else []
    ))
    as_conf = mocker.MagicMock()
    recover_stale_job_data("a000", as_conf)


def test_recover_stale_job_data_uses_existing_platform(tmp_path, mocker):
    """recover_stale_job_data reuses a connected platform when available."""
    mocker.patch('autosubmit.helpers.utils.BasicConfig.JOBDATA_DIR', str(tmp_path))
    Path(tmp_path / "job_data_a000.db").touch()
    mocker.patch.object(ExperimentHistory, 'get_stale_rows', return_value=[
        mocker.MagicMock(platform="TEST", job_name="a000_j1", fail_count=0)
    ])
    mock_plat = mocker.MagicMock()
    mock_plat.connected = True
    mock_plat.check_file_exists.return_value = False
    recover_stale_job_data("a000", mocker.MagicMock(), {"TEST": mock_plat})
    mock_plat.check_file_exists.assert_called_once_with("a000_j1_STAT_0")


def test_recover_stale_job_data_builds_when_disconnected(tmp_path, mocker):
    """recover_stale_job_data builds a new platform when existing one is disconnected."""
    mocker.patch('autosubmit.helpers.utils.BasicConfig.JOBDATA_DIR', str(tmp_path))
    Path(tmp_path / "job_data_a000.db").touch()
    mocker.patch.object(ExperimentHistory, 'get_stale_rows', return_value=[
        mocker.MagicMock(platform="TEST", job_name="a000_j1", fail_count=0)
    ])
    mock_build = mocker.patch('autosubmit.helpers.utils.build_and_connect_platform')
    mock_new_plat = mocker.MagicMock()
    mock_new_plat.connected = True
    mock_build.return_value = mock_new_plat
    mock_new_plat.check_file_exists.return_value = False
    as_conf = mocker.MagicMock()
    recover_stale_job_data("a000", as_conf, {"TEST": mocker.MagicMock(connected=False)})
    mock_build.assert_called_once_with("TEST", as_conf, "a000")
    mock_new_plat.check_file_exists.assert_called_once_with("a000_j1_STAT_0")


def test_recover_stale_job_data_builds_new_platform(tmp_path, mocker):
    """recover_stale_job_data calls build_and_connect_platform when no platforms dict is provided."""
    mocker.patch('autosubmit.helpers.utils.BasicConfig.JOBDATA_DIR', str(tmp_path))
    Path(tmp_path / "job_data_a000.db").touch()
    mocker.patch.object(ExperimentHistory, 'get_stale_rows', return_value=[
        mocker.MagicMock(platform="TEST", job_name="a000_j1", fail_count=0)
    ])
    mock_build = mocker.patch('autosubmit.helpers.utils.build_and_connect_platform')
    mock_plat = mocker.MagicMock()
    mock_plat.connected = True
    mock_build.return_value = mock_plat
    mock_plat.check_file_exists.return_value = False
    as_conf = mocker.MagicMock()
    recover_stale_job_data("a000", as_conf)
    mock_build.assert_called_once_with("TEST", as_conf, "a000")
    mock_plat.check_file_exists.assert_called_once_with("a000_j1_STAT_0")


@pytest.mark.parametrize("content,start,finish", [
    pytest.param("1781078007\n1781078010\nCOMPLETED\n", 1781078007, 1781078010, id="realistic_completed"),
    pytest.param("1781078007\n1781078010\nFAILED\n", 1781078007, 1781078010, id="realistic_failed"),
    pytest.param("500\n", 500, 0, id="only_start_line"),
    pytest.param("500\n\n", 500, 0, id="trailing_empty_line"),
])
def test_recover_stale_job_data_with_stat(tmp_path, mocker, content, start, finish):
    """recover_stale_job_data fetches STAT file remotely, reads timestamps and updates job_data."""
    mocker.patch('autosubmit.helpers.utils.BasicConfig.JOBDATA_DIR', str(tmp_path))
    mocker.patch('autosubmit.helpers.utils.BasicConfig.LOCAL_ROOT_DIR', str(tmp_path))
    Path(tmp_path / "job_data_a000.db").touch()
    mocker.patch.object(ExperimentHistory, 'get_stale_rows', return_value=[
        mocker.MagicMock(platform="TEST", job_name="a000_j1", fail_count=0)
    ])
    mock_plat = mocker.MagicMock()
    mock_plat.connected = True
    mock_plat.check_file_exists.return_value = True
    mock_plat.get_file.return_value = True
    mocker.patch.object(Path, 'exists', return_value=True)
    mocker.patch.object(Path, 'read_text', return_value=content)
    mocker.patch.object(ExperimentHistory, 'update_job_data_values')
    recover_stale_job_data("a000", mocker.MagicMock(), {"TEST": mock_plat})
    mock_plat.check_file_exists.assert_called_once_with("a000_j1_STAT_0")
    mock_plat.get_file.assert_called_once_with("a000_j1_STAT_0", True)
    ExperimentHistory.update_job_data_values.assert_called_once_with("a000_j1", 0, start, finish)

