# File: test/unit/test_commands.py
import pytest
from typing import Optional

from autosubmit.autosubmit import Autosubmit
from log.log import AutosubmitCritical


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
    'exception,raised,status',
    [
        (SystemExit, None, 1),
        (BaseException, AutosubmitCritical, None),
        (ValueError, AutosubmitCritical, None)
    ]
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
