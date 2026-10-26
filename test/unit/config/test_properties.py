# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
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

import pytest


@pytest.mark.parametrize("is_owner", [True, False])
def test_is_current_real_user_owner(autosubmit_config, mocker, is_owner, tmp_path):
    """Test if the real user (``AS_ENV_CURRENT_USER``) owns the current experiment."""
    as_conf = autosubmit_config(
        expid="a000",
        experiment_data={"ROOTDIR": str(tmp_path), "AS_ENV_CURRENT_USER": "dummy"},
    )
    owner_uid = tmp_path.stat().st_uid
    mocker.patch(
        "pwd.getpwnam",
        return_value=mocker.Mock(pw_uid=owner_uid if is_owner else owner_uid + 1),
    )
    assert as_conf.is_current_real_user_owner == is_owner


def test_is_current_real_user_owner_unknown_user(autosubmit_config, tmp_path):
    """Test that ``False`` is returned when ``AS_ENV_CURRENT_USER`` is not in the user db."""
    as_conf = autosubmit_config(
        expid="a000",
        experiment_data={
            "ROOTDIR": str(tmp_path),
            "AS_ENV_CURRENT_USER": "user_that_does_not_exist",
        },
    )
    assert as_conf.is_current_real_user_owner is False


def test_is_current_real_user_owner_does_not_resolve_owner_name(
    autosubmit_config, mocker, tmp_path
):
    """Test that the user database is not queried when the current user is the owner of the experiment.
    A missing passwd entry for the owner of the experiment should not cause an exception.
    """
    as_conf = autosubmit_config(
        expid="a000",
        experiment_data={"ROOTDIR": str(tmp_path), "AS_ENV_CURRENT_USER": "dummy"},
    )
    mocker.patch(
        "pwd.getpwnam", return_value=mocker.Mock(pw_uid=tmp_path.stat().st_uid)
    )
    mocked_getpwuid = mocker.patch("pwd.getpwuid")
    assert as_conf.is_current_real_user_owner is True
    mocked_getpwuid.assert_not_called()


@pytest.mark.parametrize(
    "experiment_data",
    [
        {},
        {"AS_ENV_CURRENT_USER": "dummy"},
        {"ROOTDIR": "/dummy/rootdir"},
        {"ROOTDIR": "/dummy/rootdir", "AS_ENV_CURRENT_USER": ""},
    ],
    ids=[
        "Missing rootdir and user",
        "Missing rootdir",
        "Missing user",
        "Empty user",
    ],
)
def test_is_current_real_user_owner_missing_data(autosubmit_config, experiment_data):
    """Test that ``False`` is returned when the experiment data is missing required fields."""
    as_conf = autosubmit_config(expid="a000", experiment_data=experiment_data)
    assert as_conf.is_current_real_user_owner is False


@pytest.mark.parametrize("is_owner", [True, False])
def test_is_current_logged_user_owner(autosubmit_config, mocker, is_owner, tmp_path):
    """Test that the ownership of the experiment is decided comparing UIDs."""
    as_conf = autosubmit_config(
        expid="a000", experiment_data={"ROOTDIR": f"{str(tmp_path)}"}
    )
    owner_uid = tmp_path.stat().st_uid
    mocker.patch("os.getuid", return_value=owner_uid if is_owner else owner_uid + 1)
    assert as_conf.is_current_logged_user_owner == is_owner


@pytest.mark.parametrize(
    "experiment_data", [{}, {"ROOTDIR": ""}], ids=["Missing rootdir", "Empty rootdir"]
)
def test_is_current_logged_user_owner_missing_data(autosubmit_config, experiment_data):
    """Test that ``is_current_logged_user_owner`` returns ``False`` when ``ROOTDIR`` is not defined."""
    as_conf = autosubmit_config(expid="a000", experiment_data=experiment_data)
    assert as_conf.is_current_logged_user_owner is False


def test_is_current_logged_user_owner_missing_rootdir(autosubmit_config, tmp_path):
    """Test that ``is_current_logged_user_owner`` returns ``False`` when ``ROOTDIR`` does not exist."""
    as_conf = autosubmit_config(
        expid="a000", experiment_data={"ROOTDIR": str(tmp_path / "does_not_exist")}
    )
    assert as_conf.is_current_logged_user_owner is False
