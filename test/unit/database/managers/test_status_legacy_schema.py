# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
#
# Regression test for #3427: legacy as_times.db tables have no primary key on
# exp_id, so SQLite rejects the ON CONFLICT upsert used by create_exp_status.

from sqlalchemy import create_engine, text

from autosubmit.database.managers.status import SqlAlchemyExperimentStatusDbManager


def _legacy_manager():
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE experiment_status ("
            "exp_id integer NOT NULL, name text NOT NULL, status text NOT NULL, "
            "seconds_diff integer NOT NULL, modified text NOT NULL)"
        ))
    manager = SqlAlchemyExperimentStatusDbManager.__new__(SqlAlchemyExperimentStatusDbManager)
    manager.status_engine = engine
    return manager


def test_create_exp_status_works_on_legacy_table_without_unique_index():
    manager = _legacy_manager()

    assert manager.create_exp_status(7, "a001", "RUNNING") > 0
    row = manager.get_experiment_status_row_by_exp_id(7)
    assert row is not None and row.status == "RUNNING"

    # A second call must update the existing row rather than insert a duplicate.
    assert manager.create_exp_status(7, "a001", "READY") > 0
    rows = manager.status_engine.connect().execute(
        text("SELECT status FROM experiment_status WHERE exp_id = 7")
    ).fetchall()
    assert [r[0] for r in rows] == ["READY"]
