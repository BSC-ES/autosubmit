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

from autosubmit.helpers.utils import get_rc_path, strtobool, user_yes_no_query


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
        (Path('/etc/.autosubmitrc'), True, True, {}),
        (Path('/etc/.autosubmitrc'), True, False, {}),
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
