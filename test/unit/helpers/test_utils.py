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

import pytest

from autosubmit.helpers.utils import strtobool, get_rc_path


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
