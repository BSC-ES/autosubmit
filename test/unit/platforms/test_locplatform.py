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

"""Tests for the Autosubmit Slurm platform."""

import pytest

from textwrap import dedent

from autosubmit.platforms.slurmplatform import SlurmPlatform


@pytest.fixture
def slurm_platform(autosubmit_config) -> SlurmPlatform:
    """Fixture to create a Slurm platform object."""
    as_conf = autosubmit_config('a000', {})
    return SlurmPlatform(as_conf.expid, 'TestSlurmPlatform', as_conf.experiment_data)


def test_parse_job_finish_data(slurm_platform):
    """Test that the sacct output is correctly parsed by Autosubmit."""
    output = dedent('''\
        JobID                     Submit               Start                 End    Elapsed ConsumedEnergy
        ------------ ------------------- ------------------- ------------------- ---------- --------------
        15994954     2025-02-24T16:11:33 2025-02-24T16:11:42 2025-02-24T16:21:30   00:09:48        883.55K
        15994954.ba+ 2025-02-24T16:11:42 2025-02-24T16:11:42 2025-02-24T16:21:30   00:09:48        497.36K
        15994954.ex+ 2025-02-24T16:11:42 2025-02-24T16:11:42 2025-02-24T16:21:30   00:09:48        883.55K
        15994954.0   2025-02-24T16:11:47 2025-02-24T16:11:47 2025-02-24T16:11:52   00:00:05              0
        15994954.1   2025-02-24T16:12:17 2025-02-24T16:12:17 2025-02-24T16:21:22   00:09:05        844.90K
    ''')
    tuple_data = slurm_platform.parse_job_finish_data(output, packed=False)
    assert tuple_data is not None
