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

from typing import Optional

import pytest

from autosubmit.autosubmit import Autosubmit
from autosubmit.log.log import AutosubmitCritical


@pytest.mark.parametrize(
    'command,args',
    [
        ('create', ['--fail-this-command-please-sir']),
        ('versioning', []),
        ('', [])
    ],
    ids=[
        'Invalid args for create',
        'Invalid subcommand',
        'No command provided'
    ]
)
def test_invalid_commands(command, args, mocker):
    """Test invalid usages of the ``autosubmit`` command and subcommands."""
    mocker.patch('sys.argv', [command] + args)
    status, args = Autosubmit.parse_args()

    assert not args and status != 0


@pytest.mark.parametrize(
    "exception,raised,status",
    [
        (SystemExit, None, 1),
        (BaseException, AutosubmitCritical, None),
        (ValueError, AutosubmitCritical, None),
    ],
    ids=[
        "SystemExit raised for invalid args",
        "AutosubmitCritical raised for BaseException",
        "AutosubmitCritical raised for ValueError",
    ],
)
def test_exceptions_raised(exception: BaseException, raised: BaseException, status: Optional[int], mocker):
    """Test exceptions being raised (for whatever reason) when running commands."""
    mocker.patch('autosubmit.autosubmit.MyParser', **{'side_effect': exception})

    if raised:
        with pytest.raises(raised):
            Autosubmit.parse_args()
            print('OK')
    else:
        assert status
        status_returned, _ = Autosubmit.parse_args()
        assert status_returned == status


@pytest.mark.parametrize(
    "command",
    ["setstatus", "monitor", "recovery"],
    ids=["setstatus", "monitor", "recovery"],
)
def test_combined_filters_parsed_for_commands(mocker, command):
    """Test combined filters parse correctly for multiple commands."""
    base_args = [
        "autosubmit",
        command,
        "a000",
        "-fl",
        "a000_20200101_fc0_1_1_LOCALJOB",
        "-fc",
        "[20200101 [ fc0 [1] ] ]",
        "-ft",
        "LOCALJOB",
        "-fs",
        "WAITING",
    ]

    # setstatus requires target status (-t)
    if command == "setstatus":
        base_args += ["-t", "READY"]

    mocker.patch("sys.argv", base_args)
    status, args = Autosubmit.parse_args()

    assert status == 0
    assert args.command == command
    assert args.list == "a000_20200101_fc0_1_1_LOCALJOB"
    assert args.filter_chunks == "[20200101 [ fc0 [1] ] ]"
    assert args.filter_type == "LOCALJOB"
    assert args.filter_status == "WAITING"


@pytest.mark.parametrize(
    "command",
    ["setstatus", "monitor", "recovery"],
    ids=["setstatus", "monitor", "recovery"],
)
def test_command_accepts_section_splits_in_ft(mocker, command):
    """Test command accepts section/split syntax in ``-ft``."""
    base_args = [
        "autosubmit",
        command,
        "a000",
        "-ft",
        "LOCALJOB [ 1 2 5-8 ]",
    ]

    # setstatus requires target status (-t)
    if command == "setstatus":
        base_args += ["-t", "READY"]

    mocker.patch("sys.argv", base_args)
    status, args = Autosubmit.parse_args()

    assert status == 0
    assert args.command == command
    assert args.filter_type == "LOCALJOB [ 1 2 5-8 ]"
