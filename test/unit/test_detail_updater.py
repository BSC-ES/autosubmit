from unittest.mock import MagicMock, patch
from autosubmit.experiment.detail_updater import (
    ExperimentDetails,
    ExperimentDetailsRepository,
)


def test_details_properties(mocker):
    # TODO: mocked create_experiment_details_repository as it fails intermittently with
    #       sqlite3.OperationalError: unable to open database file
    mocker.patch('autosubmit.experiment.detail_updater.ExperimentDetailsRepository')
    exp_details = ExperimentDetails("a000", init_reload=False)

    exp_details.exp_id = 0

    mock_as_conf = MagicMock()
    mock_as_conf.get_project_type.return_value = "git"
    mock_as_conf.get_git_project_origin.return_value = "my_git_origin"
    mock_as_conf.get_git_project_branch.return_value = "my_git_branch"
    mock_as_conf.get_platform.return_value = "my_platform"

    exp_details.as_conf = mock_as_conf

    assert exp_details.hpc == "my_platform"

    assert exp_details.model == "my_git_origin"
    assert exp_details.branch == "my_git_branch"


def test_details_repository(tmpdir):
    with patch("autosubmit.experiment.detail_updater.BasicConfig") as mock_basic_config:
        mock_basic_config.DB_PATH = str(tmpdir / "test_details_repository.db")

        details_repo = ExperimentDetailsRepository()

        new_data = {
            "exp_id": 10,
            "user": "foo",
            "created": "2024-04-11T13:34:41+02:00",
            "model": "my_model",
            "branch": "NA",
            "hpc": "MN5",
        }

        # Insert data
        details_repo.upsert_details(
            exp_id=new_data["exp_id"],
            user=new_data["user"],
            created=new_data["created"],
            model=new_data["model"],
            branch=new_data["branch"],
            hpc=new_data["hpc"],
        )
        result = details_repo.get_details(new_data["exp_id"])
        assert result == new_data

        # Update data
        updated_data = {
            "exp_id": 10,
            "user": "bar",
            "created": "2024-04-11T13:34:41+02:00",
            "model": "my_model",
            "branch": "NA",
            "hpc": "MN5",
        }
        details_repo.upsert_details(
            exp_id=updated_data["exp_id"],
            user=updated_data["user"],
            created=updated_data["created"],
            model=updated_data["model"],
            branch=updated_data["branch"],
            hpc=updated_data["hpc"],
        )
        result = details_repo.get_details(updated_data["exp_id"])
        assert result == updated_data

        # Delete data
        details_repo.delete_details(updated_data["exp_id"])
        result = details_repo.get_details(updated_data["exp_id"])
        assert result is None
