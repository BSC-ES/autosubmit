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

"""Unit tests for the wrapper_factory module."""
import pytest

from autosubmit.platforms.wrappers.wrapper_factory import SlurmWrapperFactory
from autosubmit.platforms.wrappers.wrapper_builder import BashVerticalWrapperBuilder
from autosubmit.platforms.slurmplatform import SlurmPlatform


_EXPID = 't000'


@pytest.fixture
def slurm_platform(tmp_path):
    return SlurmPlatform(_EXPID, 'slurm-platform', {
        'LOCAL_ROOT_DIR': str(tmp_path),
        'LOCAL_ASLOG_DIR': str(tmp_path / 'ASLOG')
    }, None)


@pytest.fixture
def wrapper_builder_kwargs() -> dict:
    return {
        'retrials': 1,
        'header_directive': '',
        'jobs_scripts': '',
        'threads': 1,
        'num_processors': 1,
        'num_processors_value': 1,
        'expid': _EXPID,
        'jobs_resources': '',
        'allocated_nodes': '',
        'wallclock_by_level': '',
        'name': 'WRAPPER_V'
    }


def test_constructor(slurm_platform):
    wrapper_factory = SlurmWrapperFactory(slurm_platform)

    assert wrapper_factory.as_conf is None
    assert wrapper_factory.platform is slurm_platform
    assert wrapper_factory.wrapper_director
    assert 'this platform' in wrapper_factory.exception


def test_get_wrapper(slurm_platform: SlurmPlatform, wrapper_builder_kwargs: dict, mocker):
    wrapper_factory = SlurmWrapperFactory(slurm_platform)

    wrapper_data = mocker.MagicMock()
    wrapper_data.het = {
        'HETSIZE': 2
    }
    wallclock = '00:30'

    wrapper_builder_kwargs['wrapper_data'] = wrapper_data
    wrapper_builder_kwargs['wallclock'] = wallclock

    wrapper_factory.get_wrapper(BashVerticalWrapperBuilder, **wrapper_builder_kwargs)
