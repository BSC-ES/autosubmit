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

from autosubmit.config.configcommon import AutosubmitConfig

# Tests for ``_get_experiment_owner_uid``: read the folder owner


def test_owner_uid_is_the_folder_owner(autosubmit_config, tmp_path):
    """It returns the UID of the user owning ``ROOTDIR``."""
    as_conf = autosubmit_config(
        expid="a000", experiment_data={"ROOTDIR": str(tmp_path)}
    )

    assert as_conf._get_experiment_owner_uid() == tmp_path.stat().st_uid


def test_owner_uid_is_none_folder_not_exists(autosubmit_config, tmp_path):
    """It returns ``None`` if ``ROOTDIR`` does not exist."""
    as_conf = autosubmit_config(
        expid="a000", experiment_data={"ROOTDIR": str(tmp_path / "not_exists")}
    )

    assert as_conf._get_experiment_owner_uid() is None


def test_owner_uid_is_none_Without_rootdir(autosubmit_config):
    """It returns ``None`` if there's no experiment folder configured."""
    as_conf = autosubmit_config(expid="a000", experiment_data={})

    assert as_conf._get_experiment_owner_uid() is None


# Tests for ``is_current_user_owner``: compare against ``os.getuid()``


def test_logged_user_owns_the_experiment(autosubmit_config, tmp_path):
    """It returns ``True`` if the current user is the owner of ``ROOTDIR``."""
    as_conf = autosubmit_config(
        expid="a000", experiment_data={"ROOTDIR": str(tmp_path)}
    )

    assert as_conf.is_current_logged_user_owner is True


def test_logged_user_is_not_the_owner(autosubmit_config, mocker, tmp_path):
    """It returns ``False`` if the current user is not the owner of ``ROOTDIR``."""
    as_conf = autosubmit_config(
        expid="a000", experiment_data={"ROOTDIR": str(tmp_path)}
    )
    another_uid = tmp_path.stat().st_uid + 1  # Diff user
    mocker.patch("os.getuid", return_value=another_uid)

    assert as_conf.is_current_logged_user_owner is False


def test_logged_user_owns_experiment_when_uid_zero(autosubmit_config, mocker):
    """It returns ``True`` if the current user is root (uid=0)."""
    as_conf = autosubmit_config(expid="a000", experiment_data={})
    mocker.patch.object(AutosubmitConfig, "_get_experiment_owner_uid", return_value=0)
    mocker.patch("os.getuid", return_value=0)

    assert as_conf.is_current_logged_user_owner is True


# Tests for ``is_current_real_user_owner``: compare against ``AS_ENV_CURRENT_USER``


def test_real_user_owns_the_experiment(autosubmit_config, mocker, tmp_path):
    """It returns ``True`` when ``AS_ENV_CURRENT_USER`` resolved to the folder owner UID."""
    experiment_data = {
        "ROOTDIR": str(tmp_path),
        "AS_ENV_CURRENT_USER": "authorized_user",
    }
    as_conf = autosubmit_config(expid="a000", experiment_data=experiment_data)
    mocker.patch(
        "pwd.getpwnam", return_value=mocker.Mock(pw_uid=tmp_path.stat().st_uid)
    )

    assert as_conf.is_current_real_user_owner is True


def test_real_user_is_not_the_owner_not_configured(autosubmit_config, tmp_path):
    """It returns ``False`` when ``AS_ENV_CURRENT_USER`` is not configured."""
    experiment_data = {
        "ROOTDIR": str(tmp_path),
        "AS_ENV_CURRENT_USER": None,
    }
    as_conf = autosubmit_config(expid="a000", experiment_data=experiment_data)

    assert as_conf.is_current_real_user_owner is False


def test_real_user_is_not_the_owner_unk_user(autosubmit_config, mocker, tmp_path):
    """It returns ``False`` when ``AS_ENV_CURRENT_USER`` is in the user db."""
    experiment_data = {
        "ROOTDIR": str(tmp_path),
        "AS_ENV_CURRENT_USER": "deleted_user",
    }
    as_conf = autosubmit_config(expid="a000", experiment_data=experiment_data)
    mocker.patch("pwd.getpwnam", side_effect=KeyError("getpwnam(): name not found"))

    assert as_conf.is_current_real_user_owner is False


def test_real_user_is_not_the_owner_without_rootdir(autosubmit_config, mocker):
    """It returns ``False`` when ``ROOTDIR`` is not configured."""
    as_conf = autosubmit_config(
        expid="a000",
        experiment_data={
            "ROOTDIR": None,
            "AS_ENV_CURRENT_USER": "authorized_user",
        },
    )
    mocker_getpwnam = mocker.patch("pwd.getpwnam")

    assert as_conf.is_current_real_user_owner is False
    # It should not query the user db
    mocker_getpwnam.assert_not_called()


def test_real_user_owns_the_experiment_when_uid_zero(autosubmit_config, mocker):
    """It returns ``True`` if the current user is root (uid=0)."""
    as_conf = autosubmit_config(
        expid="a000",
        experiment_data={
            "ROOTDIR": None,
            "AS_ENV_CURRENT_USER": "root",
        },
    )
    mocker.patch.object(AutosubmitConfig, "_get_experiment_owner_uid", return_value=0)
    mocker.patch("os.getuid", return_value=0)

    assert as_conf.is_current_real_user_owner is True


def test_folder_owner_uid_is_never_resolved_to_name(
    autosubmit_config, mocker, tmp_path
):
    """Test that the owner UID is compared as a number, not as a name, to avoid querying the user db.

    Resolving it to a name (``pwd.getpwuid``) would raise a ``KeyError`` for folders
    owner by a UID that is not in the user db such as deleted users or a container UID.
    """
    experiment_data = {
        "ROOTDIR": str(tmp_path),
        "AS_ENV_CURRENT_USER": "authorized_user",
    }
    as_conf = autosubmit_config(expid="a000", experiment_data=experiment_data)
    mocker.patch(
        "pwd.getpwnam", return_value=mocker.Mock(pw_uid=tmp_path.stat().st_uid)
    )
    mocker_getpwuid = mocker.patch("pwd.getpwuid")

    assert as_conf.is_current_real_user_owner is True
    # It should not query the user db for the folder owner
    mocker_getpwuid.assert_not_called()
