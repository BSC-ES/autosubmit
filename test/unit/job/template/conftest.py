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

"""Fixtures for Autosubmit job template unit tests."""

from collections.abc import Callable
from os import environ

import pytest

SCHEDULER_JOB_ID_ENV_VARS: tuple[str, ...] = (
    'SLURM_JOBID',
    'PBS_JOBID',
    'JOB_ID',
    'LSB_JOBID',
    'LOADL_STEP_ID',
    'PJM_JOBID',
)
"""Scheduler environment variables that may hold the job id, in priority order."""

AS_JOB_ID_CASES: list[tuple[dict[str, str], str]] = [
    *(({var: '12345'}, '12345') for var in SCHEDULER_JOB_ID_ENV_VARS),
    ({'SLURM_JOBID': '111', 'PBS_JOBID': '222'}, '111'),
]
"""Environment overrides and the expected job id for each ``AS_JOB_ID`` case."""


@pytest.fixture
def clean_env() -> Callable[..., dict[str, str]]:
    """Return a factory that builds an environment without scheduler job id variables.

    Any keyword argument is added on top, so tests control exactly which scheduler
    variable (if any) is visible to the generated script.
    """

    def _clean_env(**overrides: str) -> dict[str, str]:
        """Return a copy of ``os.environ`` without scheduler job id variables.

        :param overrides: Environment variables to add or override.
        """
        env = {k: v for k, v in environ.items() if k not in SCHEDULER_JOB_ID_ENV_VARS}
        env.update(overrides)
        return env

    return _clean_env


@pytest.fixture(params=AS_JOB_ID_CASES, ids=[*SCHEDULER_JOB_ID_ENV_VARS, 'priority'])
def as_job_id_case(request: pytest.FixtureRequest) -> tuple[dict[str, str], str]:
    """Environment overrides and the expected job id for one case.

    :param request: pytest fixture request holding the parametrized value.
    """
    return request.param
