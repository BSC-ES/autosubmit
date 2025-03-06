import locale
import pytest
from pathlib import Path
from pkg_resources import resource_string

from autosubmit.database import db_common
from autosubmitconfigparser.config.basicconfig import BasicConfig
from log.log import AutosubmitCritical


@pytest.mark.parametrize(
    "db_engine",
    [
        # postgres
        pytest.param("postgres", marks=[pytest.mark.postgres]),
        # sqlite
        "sqlite",
    ],
)
def test_db_common(tmp_path: Path, db_engine: str, request):
    """Regression tests for ``db_common.py``.
    Tests for regression issues in ``db_common.py`` functions, and
    for compatibility issues with the new functions for SQLAlchemy.
    The parameters allow the test to be run with different engine+options.
    You can also mark certain tests belonging to a group (e.g. postgres)
    so that they are skipped/executed selectively in CICD environments.
    """

    # Dynamically load the fixture for that DB,
    # ref: https://stackoverflow.com/a/64348247.
    request.getfixturevalue(f"as_db_{db_engine}")

    create_db_query = ""

    # The only differences in this test for SQLite and SQLAlchemy are
    # i) the SQL query used to create a DB, ii) we check that the sqlite
    # database file was created and iii) we load a different fixture for
    # sqlite and sqlalchemy (to mock ``BasicConfig`` and run a container
    # for sqlalchemy).
    is_sqlite = db_engine == "sqlite"
    if is_sqlite:
        # Code copied from ``autosubmit.py``.
        create_db_query = resource_string(
            "autosubmit.database", "data/autosubmit.sql"
        ).decode(locale.getlocale()[1])

    assert db_common.create_db(create_db_query)

    if is_sqlite:
        assert Path(BasicConfig.DB_PATH).exists()

    # Test last name used
    assert "empty" == db_common.last_name_used()
    assert "empty" == db_common.last_name_used(test=True)
    assert "empty" == db_common.last_name_used(operational=True)

    new_exp = {
        "name": "a700",
        "description": "Description",
        "autosubmit_version": "4.0.0",
    }

    # Experiment doesn't exist yet
    with pytest.raises(Exception):
        db_common.check_experiment_exists(new_exp["name"])

    # Test save
    db_common.save_experiment(
        new_exp["name"], new_exp["description"], new_exp["autosubmit_version"]
    )
    assert db_common.check_experiment_exists(
        new_exp["name"], error_on_inexistence=False
    )
    assert db_common.last_name_used() == new_exp["name"]

    # Get version
    assert (
        db_common.get_autosubmit_version(new_exp["name"])
        == new_exp["autosubmit_version"]
    )
    new_version = "v4.1.0"
    db_common.update_experiment_descrip_version(new_exp["name"], version=new_version)
    assert db_common.get_autosubmit_version(new_exp["name"]) == new_version

    # Update description
    assert (
        db_common.get_experiment_descrip(new_exp["name"])[0][0]
        == new_exp["description"]
    )
    new_desc = "New Description"
    db_common.update_experiment_descrip_version(new_exp["name"], description=new_desc)
    assert db_common.get_experiment_descrip(new_exp["name"])[0][0] == new_desc

    # Update back both: description and version
    db_common.update_experiment_descrip_version(
        new_exp["name"],
        description=new_exp["description"],
        version=new_exp["autosubmit_version"],
    )
    assert (
        db_common.get_experiment_descrip(new_exp["name"])[0][0]
        == new_exp["description"]
        and db_common.get_autosubmit_version(new_exp["name"])
        == new_exp["autosubmit_version"]
    )

    # Delete experiment
    assert db_common.delete_experiment(new_exp["name"])
    with pytest.raises(AutosubmitCritical):
        assert (
            db_common.get_autosubmit_version(new_exp["name"])
            == new_exp["autosubmit_version"]
        )
