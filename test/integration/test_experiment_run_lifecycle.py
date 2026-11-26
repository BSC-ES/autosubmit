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

"""Integration tests for the ``experiment_run`` lifecycle."""

from autosubmit.history.data_classes.experiment_run import ExperimentRun
from autosubmit.history.experiment_history import ExperimentHistory

_STATUS_COUNTS = {
    "COMPLETED": 0,
    "FAILED": 0,
    "QUEUING": 0,
    "SUBMITTED": 0,
    "RUNNING": 0,
    "SUSPENDED": 0,
    "TOTAL": 1,
}


def _process(history: ExperimentHistory) -> ExperimentRun | None:
    return history.process_status_changes(
        job_list=[],
        chunk_unit="month",
        chunk_size=1,
        status_counts=_STATUS_COUNTS,
    )


def test_run_continues_the_current_run(autosubmit_exp) -> None:
    """A plain ``run`` reuses the current run id. Only ``create`` opens a new one."""
    exp = autosubmit_exp(create=False)
    history = ExperimentHistory(exp.expid)
    history.initialize_database()

    first_run = _process(history)
    assert first_run is not None
    assert first_run.run_id == 1
    assert first_run.pending_create == 0

    second_run = _process(history)
    assert second_run is not None
    assert second_run.run_id == first_run.run_id

    history.set_pending_create(1)
    third_run = _process(history)
    assert third_run is not None
    assert third_run.run_id != first_run.run_id
    assert third_run.pending_create == 0


def test_recovery_keeps_the_current_run(autosubmit_exp) -> None:
    """``create`` followed by ``recovery`` must resume the current run."""
    exp = autosubmit_exp(create=False)
    history = ExperimentHistory(exp.expid)
    history.initialize_database()

    first_run = _process(history)
    assert first_run is not None

    history.set_pending_create(1)
    history.set_pending_create(0)

    recovered_run = _process(history)
    assert recovered_run is not None
    assert recovered_run.run_id == first_run.run_id
