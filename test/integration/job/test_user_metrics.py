# Copyright 2015-2025 Earth Sciences Department, BSC-CNS
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

"""Integration tests for ``autosubmit.job.user_metrics``."""

from typing import TYPE_CHECKING

import pytest

from autosubmit.job.metrics_processor import UserMetricRepository

if TYPE_CHECKING:
    # noinspection PyProtectedMember
    from _pytest._py.path import LocalPath


_EXPID = "t123"


@pytest.mark.docker
@pytest.mark.postgres
def test_store_metric(as_db: str, autosubmit_exp, tmp_path: 'LocalPath'):
    exp = autosubmit_exp(_EXPID, include_jobs=True)

    user_metric_repository = UserMetricRepository(exp.expid)
    user_metric_repository.store_metric(
        run_id=1,
        job_name="test_job",
        metric_name="test_metric",
        metric_value="test_value",
    )

    # Check if the metric is stored in the database
    with user_metric_repository.engine.connect() as conn:
        result = conn.execute(
            user_metric_repository.table.select().where(
                user_metric_repository.table.c.run_id == 1,
                user_metric_repository.table.c.job_name == "test_job",
                user_metric_repository.table.c.metric_name == "test_metric",
            )
        ).first()
        assert result is not None
        assert result.metric_value == "test_value"
