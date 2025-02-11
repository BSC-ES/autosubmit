# Copyright 2015 Earth Sciences Department, BSC-CNS
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

from collections import namedtuple

from autosubmit.autosubmit import Autosubmit
from autosubmit.scripts.autosubmit import main


def test_autosubmit_script_main(mocker, autosubmit_config):
    """Test that the autosubmit script exit code.

    It must exit with the same value returned by the ``main`` function.
    """
    as_conf = autosubmit_config('a000', {})
    mocker.patch('sys.argv', ['autosubmit', '-v'])
    mocker.patch('autosubmit.autosubmit.BasicConfig', as_conf.basic_config)
    as_log = mocker.patch('autosubmit.autosubmit.Log')
    exit_code = main()
    assert as_log.info.call_args[0][0] == Autosubmit.autosubmit_version
    assert exit_code == 0


def test_autosubmit_script_readme(mocker, capsys, autosubmit_config):
    """Test that the readme command is executed and returns 0.

    At the moment readme is still returning a boolean. Hopefully, that
    will be fixed in the near future. This test can stay just to make
    sure the command is working (it was not when this test was written).
    """
    as_conf = autosubmit_config('a000', {})
    mocker.patch('sys.argv', ['autosubmit', 'readme'])
    mocker.patch('autosubmit.autosubmit.BasicConfig', as_conf.basic_config)
    exit_code = main()
    stdout, _ = capsys.readouterr()
    assert 'lightweight' in stdout
    assert exit_code == 0


def test_autosubmit_script_error_raised(mocker):
    command = 'inspect'
    expid = 'fail'
    mocker.patch('sys.argv', ['autosubmit', command, expid])
    mocker.patch('autosubmit.scripts.autosubmit.exit_from_error', return_value=127)

    Args = namedtuple('Args', ['command', 'expid'])
    args = Args(command, expid)
    mocker.patch('autosubmit.scripts.autosubmit.Autosubmit.parse_args', return_value=(0, args))
    mocker.patch('autosubmit.scripts.autosubmit.Autosubmit.run_command', side_effect=ValueError)
    exit_code = main()
    assert exit_code == 127
