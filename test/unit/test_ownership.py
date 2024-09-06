import pytest
from autosubmit.autosubmit import Autosubmit
from log.log import AutosubmitCritical


@pytest.fixture
def mock_as_conf(mocker):
    return mocker.MagicMock()


@pytest.fixture
def mock_owner_user_same(mocker):
    mock = mocker.patch('os.getuid')
    mock.return_value = 1  # user
    mock2 = mocker.patch('pathlib.Path.stat')
    mock2.return_value.st_uid = 1  # owner
    mock3 = mocker.patch('pwd.getpwuid')
    mock3.return_value.pw_name = "test1"
    return mock, mock2, mock3


@pytest.fixture
def mock_owner_user_diff(mocker):
    mock = mocker.patch('os.getuid')
    mock.return_value = 1  # user
    mock2 = mocker.patch('pathlib.Path.stat')
    mock2.return_value.st_uid = 2  # owner
    mock3 = mocker.patch('pwd.getpwuid')
    mock3.return_value.pw_name = "test1"
    return mock, mock2, mock3


def test_check_ownership_and_set_last_command_same_owner(mock_as_conf, mock_owner_user_same):
    autosubmit = Autosubmit()
    expid = "testexpid"
    for command in ['monitor', 'other']:
        owner, eadmin, current_owner = autosubmit._check_ownership_and_set_last_command(mock_as_conf, expid, command)
        assert owner is True
        assert eadmin is False
        assert current_owner == "test1"
        assert mock_as_conf.set_last_as_command.called


def test_check_ownership_and_set_last_command_diff_owner(mock_as_conf, mock_owner_user_diff):
    autosubmit = Autosubmit()
    expid = "testexpid"
    command = "monitor"
    owner, eadmin, current_owner = autosubmit._check_ownership_and_set_last_command(mock_as_conf, expid, command)
    assert owner is False
    assert eadmin is False
    assert current_owner == "test1"
    assert not mock_as_conf.set_last_as_command.called
    command = "other"
    with pytest.raises(AutosubmitCritical):
        autosubmit._check_ownership_and_set_last_command(mock_as_conf, expid, command)
