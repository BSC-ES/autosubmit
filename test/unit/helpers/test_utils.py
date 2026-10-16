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
from types import SimpleNamespace

import pytest

from autosubmit.helpers.utils import (
    bytes_to_unit,
    get_rc_path,
    memory_release_interval,
    memory_release_mode,
    release_memory_to_os,
    should_release_memory,
    strtobool,
    to_mib,
    user_yes_no_query,
)


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
def test_user_yes_no_query(answer: str, expected_or_error: bool | Exception, mocker):
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


def _as_conf(config: dict | None) -> SimpleNamespace:
    """Build a minimal config stub for the memory-release helpers."""
    return SimpleNamespace(experiment_data={} if config is None else {'RUNTIME': config})


@pytest.mark.parametrize(
    'config,basic_config,expected',
    [
        # (RUNTIME section, autosubmitrc [runtime] default, expected)
        (None, 'off', 'off'),
        ({}, 'off', 'off'),
        ({'MEMORY_RELEASE_MODE': 'off'}, 'interval', 'off'),
        ({'MEMORY_RELEASE_MODE': 'on_unload'}, 'off', 'on_unload'),
        ({'MEMORY_RELEASE_MODE': 'interval'}, 'off', 'interval'),
        ({'MEMORY_RELEASE_MODE': 'INTERVAL'}, 'off', 'interval'),
        ({'MEMORY_RELEASE_MODE': ' On_Unload '}, 'off', 'on_unload'),
        ({'MEMORY_RELEASE_MODE': 'bogus'}, 'off', 'off'),
        # No experiment value: falls back to the autosubmitrc default.
        (None, 'interval', 'interval'),
        ({}, 'interval', 'interval'),
        # The experiment value wins over the autosubmitrc default.
        ({'MEMORY_RELEASE_MODE': 'on_unload'}, 'interval', 'on_unload'),
    ],
)
def test_memory_release_mode(config, basic_config, expected, monkeypatch):
    monkeypatch.setattr('autosubmit.helpers.utils.BasicConfig.MEMORY_RELEASE_MODE', basic_config)
    assert memory_release_mode(_as_conf(config)) == expected


def test_memory_release_mode_without_config(monkeypatch):
    monkeypatch.setattr('autosubmit.helpers.utils.BasicConfig.MEMORY_RELEASE_MODE', 'off')
    assert memory_release_mode(None) == 'off'


@pytest.mark.parametrize(
    'config,basic_config,expected',
    [
        # (RUNTIME section, autosubmitrc [runtime] default, expected)
        (None, 0, 0),
        ({}, 0, 0),
        ({'MEMORY_RELEASE_INTERVAL': 5}, 0, 5),
        ({'MEMORY_RELEASE_INTERVAL': '5'}, 0, 5),
        ({'MEMORY_RELEASE_INTERVAL': -1}, 0, 0),
        ({'MEMORY_RELEASE_INTERVAL': 'abc'}, 0, 0),
        # No experiment value: falls back to the autosubmitrc default.
        (None, 7, 7),
        ({}, 7, 7),
        # The experiment value wins over the autosubmitrc default.
        ({'MEMORY_RELEASE_INTERVAL': 3}, 7, 3),
    ],
)
def test_memory_release_interval(config, basic_config, expected, monkeypatch):
    monkeypatch.setattr('autosubmit.helpers.utils.BasicConfig.MEMORY_RELEASE_INTERVAL', basic_config)
    assert memory_release_interval(_as_conf(config)) == expected


def test_memory_release_interval_without_config(monkeypatch):
    monkeypatch.setattr('autosubmit.helpers.utils.BasicConfig.MEMORY_RELEASE_INTERVAL', 0)
    assert memory_release_interval(None) == 0


@pytest.mark.parametrize(
    'mode,interval,iteration,expected',
    [
        ('off', 5, 5, False),
        ('on_unload', 5, 5, False),
        ('interval', 0, 5, False),
        ('interval', 5, 4, False),
        ('interval', 5, 5, True),
        ('interval', 1, 3, True),
    ],
)
def test_should_release_memory(mode, interval, iteration, expected):
    assert should_release_memory(mode, interval, iteration) is expected


@pytest.mark.parametrize(
    'value,expected',
    [
        (0, (0, 'B')),
        (1023, (1023, 'B')),
        (1024, (1.0, 'KiB')),
        (1024 * 1024, (1.0, 'MiB')),
        (-2048, (-2.0, 'KiB')),
        (2 * 1024 ** 3, (2.0, 'GiB')),
        (2 * 1024 ** 5, (2.0, 'PiB')),
        (1024 ** 6, (1024.0, 'PiB')),  # cannot go beyond PiB
    ],
)
def test_bytes_to_unit(value, expected):
    assert bytes_to_unit(value) == expected


@pytest.mark.parametrize(
    'value,unit,expected',
    [
        ('1048576', 'B', 1.0),
        (1024, 'KiB', 1.0),
        ('1.5', 'MiB', 1.5),
        (1, 'GiB', 1024.0),
        (1, 'TiB', 1024.0 * 1024),
        (1, 'PiB', 1024.0 ** 3),
        (1, 'unknown', 1.0 / (1024 * 1024)),
    ],
)
def test_to_mib(value, unit, expected):
    assert to_mib(value, unit) == expected


def test_release_memory_to_os_calls_gc_and_trim(mocker):
    collect = mocker.patch('gc.collect')
    trim = mocker.patch('autosubmit.helpers.utils._malloc_trim')
    assert release_memory_to_os() is None
    collect.assert_called_once()
    trim.assert_called_once_with(0)


def test_release_memory_to_os_is_noop_without_malloc_trim(mocker, monkeypatch):
    collect = mocker.patch('gc.collect')
    monkeypatch.setattr('autosubmit.helpers.utils._malloc_trim', None)
    assert release_memory_to_os() is None
    collect.assert_called_once()


def test_release_memory_to_os_suppresses_oserror(mocker):
    mocker.patch('gc.collect')
    mocker.patch('autosubmit.helpers.utils._malloc_trim', side_effect=OSError)
    assert release_memory_to_os() is None

