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

"""Unit tests for the ``autosubmit.experiment.describe`` module."""

from datetime import datetime
from pathlib import Path

import pytest

from autosubmit.experiment.describe import (
    ExperimentDescription,
    describe_experiment,
    get_experiment_ids,
    log_experiment_description,
)
from autosubmit.log.log import AutosubmitCritical


@pytest.mark.parametrize(
    ("experiment_ids", "user", "database_ids", "expected"),
    [
        (
            "",
            "*",
            ["a003", "a001", "a002"],
            (["a001", "a002", "a003"], []),
        ),
        (
            "*",
            "*",
            ["a003", "a001", "a002"],
            (["a001", "a002", "a003"], []),
        ),
    ],
    ids=[
        "empty_experiment_ids",
        "wildcard_experiment_ids",
    ],
)
def test_get_experiment_ids_all_users(
    mocker,
    experiment_ids: str,
    user: str,
    database_ids: list[str],
    expected: tuple[list[str], list[str]],
) -> None:
    """Test that empty and wildcard experiment IDs select all users.

    :param mocker: Pytest mocker fixture.
    :param experiment_ids: Experiment ID filter to test.
    :param user: User filter to test.
    :param database_ids: Experiment IDs returned by the database.
    :param expected: Expected matching and not-found experiment IDs.
    """
    mocker.patch(
        "autosubmit.experiment.describe.get_experiment_expids",
        return_value=database_ids,
    )

    assert get_experiment_ids(experiment_ids, user) == expected


def test_get_experiment_ids_filters_by_user(mocker) -> None:
    """Test that experiments are filtered according to their owner.

    :param mocker: Pytest mocker fixture.
    """
    mocker.patch(
        "autosubmit.experiment.describe.get_experiment_expids",
        return_value=["a001", "a002", "a003"],
    )
    mocker.patch(
        "autosubmit.experiment.describe.getpwnam",
        return_value=mocker.Mock(pw_uid=1000),
    )
    get_owner = mocker.patch(
        "autosubmit.experiment.describe.get_experiment_owner",
        side_effect=[
            ("kinow", 1000, True, False),
            ("root", 0, False, False),
            ("kinow", 1000, True, False),
        ],
    )

    result = get_experiment_ids("", "kinow")

    assert result == (["a001", "a003"], [])
    assert get_owner.call_count == 3


def test_get_experiment_ids_filters_requested_ids_by_user(mocker) -> None:
    """Test that explicitly requested experiments are filtered by owner.

    :param mocker: Pytest mocker fixture.
    """
    mocker.patch(
        "autosubmit.experiment.describe.get_experiment_expids",
        return_value=["a001", "a002", "a003"],
    )
    mocker.patch(
        "autosubmit.experiment.describe.getpwnam",
        return_value=mocker.Mock(pw_uid=1000),
    )
    mocker.patch(
        "autosubmit.experiment.describe.get_experiment_owner",
        side_effect=[
            ("kinow", 1000, True, False),
            ("root", 0, False, False),
        ],
    )

    result = get_experiment_ids("A001, a002", "kinow")

    assert result == (["a001"], [])


def test_get_experiment_ids_returns_not_found_ids(mocker) -> None:
    """Test that requested experiment IDs missing from the database are returned.

    :param mocker: Pytest mocker fixture.
    """
    mocker.patch(
        "autosubmit.experiment.describe.get_experiment_expids",
        return_value=["a000"],
    )

    result = get_experiment_ids("a999,a000", "*")

    assert result == (["a000"], ["a999"])


def test_get_experiment_ids_preserves_not_found_when_user_filter_matches_nothing(
    mocker,
) -> None:
    """Test that missing IDs are preserved when the user filter matches nothing.

    :param mocker: Pytest mocker fixture.
    """
    mocker.patch(
        "autosubmit.experiment.describe.get_experiment_expids",
        return_value=["a000"],
    )
    mocker.patch(
        "autosubmit.experiment.describe.getpwnam",
        return_value=mocker.Mock(pw_uid=1000),
    )
    mocker.patch(
        "autosubmit.experiment.describe.get_experiment_owner",
        return_value=("root", 0, False, False),
    )

    result = get_experiment_ids("a999,a000", "kinow")

    assert result == ([], ["a999"])


def test_get_experiment_ids_returns_no_experiments_for_unknown_user(mocker) -> None:
    """Test that an unknown user produces no matching experiments.

    :param mocker: Pytest mocker fixture.
    """
    mocker.patch(
        "autosubmit.experiment.describe.get_experiment_expids",
        return_value=["a000", "a001"],
    )
    mocker.patch(
        "autosubmit.experiment.describe.getpwnam",
        side_effect=KeyError,
    )

    result = get_experiment_ids("", "unknown-user")

    assert result == ([], [])


def test_describe_experiment_from_configuration(mocker, tmp_path: Path) -> None:
    """Test that experiment details are read from the configuration.

    :param mocker: Pytest mocker fixture.
    :param tmp_path: Temporary directory fixture used for the configuration.
    """
    conf_path = tmp_path / "autosubmit.yml"
    conf_path.touch()

    created = datetime(2026, 8, 31, 10, 52, 39)

    mock_config = mocker.patch("autosubmit.experiment.describe.AutosubmitConfig")
    mock_config_instance = mock_config.return_value
    mock_config_instance.conf_folder_yaml = str(conf_path)
    mock_config_instance.get_svn_project_url.return_value = ""
    mock_config_instance.get_git_project_origin.return_value = (
        "https://example.com/model.git"
    )
    mock_config_instance.get_git_project_branch.return_value = "main"
    mock_config_instance.get_platform.return_value = "HPC"

    mocker.patch(
        "autosubmit.experiment.describe.get_experiment_owner",
        return_value=("kinow", 1000, True, False),
    )
    mocker.patch(
        "autosubmit.experiment.describe.ParamikoSubmitter",
        return_value=mocker.Mock(platforms=["HPC"]),
    )
    mocker.patch(
        "autosubmit.experiment.describe.get_experiment_description",
        return_value=[("My experiment",)],
    )

    import os

    os.utime(conf_path, (created.timestamp(), created.timestamp()))

    result = describe_experiment("a001")

    assert result == ExperimentDescription(
        user="kinow",
        created=created,
        model="https://example.com/model.git",
        branch="main",
        hpc="HPC",
        description="My experiment",
    )


def test_describe_experiment_uses_svn_url_for_model_and_branch(
    mocker,
    tmp_path: Path,
) -> None:
    """Test that the SVN URL is used for both model and branch.

    :param mocker: Pytest mocker fixture.
    :param tmp_path: Temporary directory fixture used for the configuration.
    """
    conf_path = tmp_path / "autosubmit.yml"
    conf_path.touch()

    mock_config = mocker.patch("autosubmit.experiment.describe.AutosubmitConfig")
    mock_config_instance = mock_config.return_value
    mock_config_instance.conf_folder_yaml = str(conf_path)
    mock_config_instance.get_svn_project_url.return_value = (
        "https://svn.example.com/model"
    )
    mock_config_instance.get_git_project_origin.return_value = "unused"
    mock_config_instance.get_git_project_branch.return_value = "unused"
    mock_config_instance.get_platform.return_value = "LOCAL"

    mocker.patch(
        "autosubmit.experiment.describe.get_experiment_owner",
        return_value=("kinow", 1000, True, False),
    )
    mocker.patch(
        "autosubmit.experiment.describe.ParamikoSubmitter",
        return_value=mocker.Mock(platforms=["LOCAL"]),
    )
    mocker.patch(
        "autosubmit.experiment.describe.get_experiment_description",
        return_value=[],
    )

    result = describe_experiment("a001")

    assert result.model == "https://svn.example.com/model"
    assert result.branch == "https://svn.example.com/model"
    assert result.description == ""


@pytest.mark.parametrize(
    ("origin", "branch", "expected_model", "expected_branch"),
    [
        ("", "", "Not Found", "Not Found"),
        (
            "https://example.com/model.git",
            "",
            "https://example.com/model.git",
            "Not Found",
        ),
        ("", "main", "Not Found", "main"),
    ],
    ids=[
        "missing_origin_and_branch",
        "missing_branch",
        "missing_origin",
    ],
)
def test_describe_experiment_missing_git_values(
    mocker,
    tmp_path: Path,
    origin: str,
    branch: str,
    expected_model: str,
    expected_branch: str,
) -> None:
    """Test that missing Git values are replaced with ``Not Found``.

    :param mocker: Pytest mocker fixture.
    :param tmp_path: Temporary directory fixture used for the configuration.
    :param origin: Git project origin returned by the configuration.
    :param branch: Git branch returned by the configuration.
    :param expected_model: Expected model value.
    :param expected_branch: Expected branch value.
    """
    conf_path = tmp_path / "autosubmit.yml"
    conf_path.touch()

    mock_config = mocker.patch("autosubmit.experiment.describe.AutosubmitConfig")
    mock_config_instance = mock_config.return_value
    mock_config_instance.conf_folder_yaml = str(conf_path)
    mock_config_instance.get_svn_project_url.return_value = ""
    mock_config_instance.get_git_project_origin.return_value = origin
    mock_config_instance.get_git_project_branch.return_value = branch
    mock_config_instance.get_platform.return_value = "LOCAL"

    mocker.patch(
        "autosubmit.experiment.describe.get_experiment_owner",
        return_value=("kinow", 1000, True, False),
    )
    mocker.patch(
        "autosubmit.experiment.describe.ParamikoSubmitter",
        return_value=mocker.Mock(platforms=["LOCAL"]),
    )
    mocker.patch(
        "autosubmit.experiment.describe.get_experiment_description",
        return_value=[],
    )

    result = describe_experiment("a001")

    assert result.model == expected_model
    assert result.branch == expected_branch


def test_describe_experiment_uses_uid_when_owner_is_deleted(
    mocker,
    tmp_path: Path,
) -> None:
    """Test that the owner UID is used when the owner no longer exists.

    :param mocker: Pytest mocker fixture.
    :param tmp_path: Temporary directory fixture used for the configuration.
    """
    conf_path = tmp_path / "autosubmit.yml"
    conf_path.touch()

    mock_config = mocker.patch("autosubmit.experiment.describe.AutosubmitConfig")
    mock_config_instance = mock_config.return_value
    mock_config_instance.conf_folder_yaml = str(conf_path)
    mock_config_instance.get_svn_project_url.return_value = ""
    mock_config_instance.get_git_project_origin.return_value = ""
    mock_config_instance.get_git_project_branch.return_value = ""
    mock_config_instance.get_platform.return_value = "LOCAL"

    mocker.patch(
        "autosubmit.experiment.describe.get_experiment_owner",
        return_value=(None, 12345, False, False),
    )
    mocker.patch(
        "autosubmit.experiment.describe.ParamikoSubmitter",
        return_value=mocker.Mock(platforms=["LOCAL"]),
    )
    mocker.patch(
        "autosubmit.experiment.describe.get_experiment_description",
        return_value=[],
    )

    result = describe_experiment("a001")

    assert result.user == "12345"


def test_describe_experiment_uses_database_snapshot_when_configuration_fails(
    mocker,
) -> None:
    """Test that the database snapshot is used when configuration is unavailable.

    :param mocker: Pytest mocker fixture.
    """
    mocker.patch(
        "autosubmit.experiment.describe.AutosubmitConfig",
        side_effect=OSError("configuration unavailable"),
    )
    mocker.patch(
        "autosubmit.experiment.describe.ExperimentDetails",
        return_value=mocker.Mock(
            get_details=mocker.Mock(
                return_value={
                    "user": "kinow",
                    "created": datetime(2026, 8, 31, 10, 52, 39),
                    "model": "model",
                    "branch": "main",
                    "hpc": "HPC",
                }
            )
        ),
    )
    mocker.patch(
        "autosubmit.experiment.describe.get_experiment_description",
        return_value=[("description",)],
    )

    result = describe_experiment("a001")

    assert result == ExperimentDescription(
        user="kinow",
        created=datetime(2026, 8, 31, 10, 52, 39),
        model="model",
        branch="main",
        hpc="HPC",
        description="description",
    )


def test_describe_experiment_raises_when_configuration_and_snapshot_fail(
    mocker,
) -> None:
    """Test that AutosubmitCritical is raised when no fallback is available.

    :param mocker: Pytest mocker fixture.
    :raises AutosubmitCritical: If neither configuration nor database snapshot
        can provide the experiment details.
    """
    mocker.patch(
        "autosubmit.experiment.describe.AutosubmitConfig",
        side_effect=OSError("configuration unavailable"),
    )
    mocker.patch(
        "autosubmit.experiment.describe.ExperimentDetails",
        return_value=mocker.Mock(get_details=mocker.Mock(return_value=None)),
    )

    with pytest.raises(AutosubmitCritical):
        describe_experiment("a001")


def test_describe_experiment_raises_when_no_platforms_are_available(
    mocker,
    tmp_path: Path,
) -> None:
    """Test that AutosubmitCritical is raised when no platform is configured.

    :param mocker: Pytest mocker fixture.
    :param tmp_path: Temporary directory fixture used for the configuration.
    :raises AutosubmitCritical: If the experiment has no available platforms.
    """
    conf_path = tmp_path / "autosubmit.yml"
    conf_path.touch()

    mock_config = mocker.patch("autosubmit.experiment.describe.AutosubmitConfig")
    mock_config_instance = mock_config.return_value
    mock_config_instance.conf_folder_yaml = str(conf_path)

    mocker.patch(
        "autosubmit.experiment.describe.get_experiment_owner",
        return_value=("kinow", 1000, True, False),
    )
    mocker.patch(
        "autosubmit.experiment.describe.ParamikoSubmitter",
        return_value=mocker.Mock(platforms=[]),
    )

    with pytest.raises(AutosubmitCritical):
        describe_experiment("a001")


def test_log_experiment_description(mocker, tmp_path: Path) -> None:
    """Test that an experiment description is logged in the expected format.

    :param mocker: Pytest mocker fixture.
    :param tmp_path: Temporary directory used as the Autosubmit root.
    """
    mocker.patch(
        "autosubmit.experiment.describe.BasicConfig.LOCAL_ROOT_DIR",
        str(tmp_path),
    )
    log = mocker.patch("autosubmit.experiment.describe.Log.info")

    experiment = ExperimentDescription(
        user="kinow",
        created=datetime(2026, 8, 31, 10, 52, 39),
        model="model",
        branch="main",
        hpc="LOCAL",
        description="My experiment",
    )

    log_experiment_description("a001", experiment)

    assert log.call_args_list == [
        mocker.call(""),
        mocker.call("Experiment a001"),
        mocker.call("  Owner:       kinow"),
        mocker.call(f"  Location:    {tmp_path / 'a001'}"),
        mocker.call("  Created:     2026-08-31 10:52:39"),
        mocker.call("  Model:       model"),
        mocker.call("  Branch:      main"),
        mocker.call("  HPC:         LOCAL"),
        mocker.call("  Description: My experiment"),
    ]
