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

"""Integration tests for ``autosubmit.experiment.lock``."""

import pytest
from sqlalchemy import text

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.database.session import get_engine

# noinspection PyProtectedMember
from autosubmit.experiment.lock import (
    _QUERY_OBTAIN_LOCK,
    _QUERY_RELEASE_LOCK,
    experiment_lock,
)


@pytest.mark.docker
@pytest.mark.postgres
@pytest.mark.parametrize("as_db", ["postgres"], indirect=True)
def test_postgres_experiment_lock(as_db, autosubmit_exp):
    """If an experiment has a lock, other DB connections fail to acquire it."""
    exp1 = autosubmit_exp(experiment_data={})
    exp2 = autosubmit_exp(experiment_data={})

    engine = get_engine(BasicConfig.DB_PATH)

    with experiment_lock(exp1.expid):
        # ``exp1`` already has a lock; let's open a new connection and try to
        # obtain it...
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            acquired = conn.execute(
                text(_QUERY_OBTAIN_LOCK),
                {"key": int(exp1.expid, 36)},
            ).scalar_one()

            assert acquired is False, (
                f"The experiment {exp1.expid} has not acquired the lock!"
            )

            # BUT! Another experiment can still obtain a lock within the session,
            # which we don't plan on doing, but we are only testing what's valid...
            acquired = conn.execute(
                text(_QUERY_OBTAIN_LOCK),
                {"key": int(exp2.expid, 36)},
            ).scalar_one()

            assert acquired is True

            # Release lock of ``exp2``, the context manager closing will release ``exp1``'s.
            conn.execute(
                text(_QUERY_RELEASE_LOCK),
                {"key": int(exp2.expid, 36)},
            ).scalar_one()

    # Here, the context manager is closed, so now we must be able to obtain
    # the lock.
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        acquired = conn.execute(
            text(_QUERY_OBTAIN_LOCK),
            {"key": int(exp1.expid, 36)},
        ).scalar_one()

        assert acquired is True

        conn.execute(
            text(_QUERY_RELEASE_LOCK),
            {"key": int(exp1.expid, 36)},
        )


@pytest.mark.docker
@pytest.mark.postgres
@pytest.mark.parametrize("as_db", ["postgres"], indirect=True)
def test_postgres_experiment_lock_released_on_exception(as_db, autosubmit_exp):
    """An experiment lock is released even if the command raises an exception."""
    exp = autosubmit_exp(experiment_data={})
    engine = get_engine(BasicConfig.DB_PATH)

    with pytest.raises(ValueError, match="Command failed"), experiment_lock(exp.expid):
        raise ValueError("Command failed")

    # A new PostgreSQL session can acquire the lock.
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        acquired = conn.execute(
            text(_QUERY_OBTAIN_LOCK),
            {"key": int(exp.expid, 36)},
        ).scalar_one()

        assert acquired is True

        conn.execute(
            text(_QUERY_RELEASE_LOCK),
            {"key": int(exp.expid, 36)},
        )


@pytest.mark.docker
@pytest.mark.postgres
@pytest.mark.parametrize("as_db", ["postgres"], indirect=True)
def test_postgres_experiment_lock_timeout(as_db, autosubmit_exp):
    """Raise ExperimentAlreadyLockedException when another session holds the lock."""
    from autosubmit.exceptions import ExperimentAlreadyLockedException

    exp = autosubmit_exp(experiment_data={})

    with experiment_lock(exp.expid):
        with pytest.raises(ExperimentAlreadyLockedException):
            with experiment_lock(exp.expid, timeout=0):
                pass
