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

from typing import Union

import pytest

from autosubmit.platforms.slurmplatform import SlurmPlatform
from autosubmit.platforms.wrappers.wrapper_builder import BashVerticalWrapperBuilder
from autosubmit.platforms.wrappers.wrapper_factory import SlurmWrapperFactory, SrunHorizontalWrapperBuilder, \
    SrunVerticalHorizontalWrapperBuilder, PythonHorizontalWrapperBuilder, PythonVerticalWrapperBuilder, \
    PythonVerticalHorizontalWrapperBuilder, PythonHorizontalVerticalWrapperBuilder, EcWrapperFactory

_EXPID = 't000'


@pytest.fixture
def slurm_platform(tmp_path):
    """Return a dummy Slurm platform -- NOTE: this platform is not usable, just for testing!"""
    return SlurmPlatform(_EXPID, 'slurm-platform', {
        'LOCAL_ROOT_DIR': str(tmp_path),
        'LOCAL_ASLOG_DIR': str(tmp_path / 'ASLOG')
    }, None)


@pytest.fixture
def wrapper_builder_kwargs() -> dict:
    """Return the base arguments for the wrapper builder.

    TODO: this can probably be improved with a configuration class? Something less error-prone and
          easier to test than a Python dictionary with key-values of different types (also easier
          to validate the type of parameters).
    """
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
    """Test the constructor of the wrapper factory (called by the child Slurm wrapper factory object)."""
    wrapper_factory = SlurmWrapperFactory(slurm_platform)

    assert wrapper_factory.as_conf is None
    assert wrapper_factory.platform is slurm_platform
    assert wrapper_factory.wrapper_director
    assert 'this platform' in wrapper_factory.exception


@pytest.mark.parametrize(
    'method,expected_horizontal_clazz,expected_vertical_horizontal_clazz',
    [
        ['srun', SrunHorizontalWrapperBuilder, SrunVerticalHorizontalWrapperBuilder],
        ['threads', PythonHorizontalWrapperBuilder, PythonVerticalHorizontalWrapperBuilder],
    ]
)
def test_get_wrapper_slurm(method: str, expected_horizontal_clazz: type, expected_vertical_horizontal_clazz: type,
                           slurm_platform: SlurmPlatform, wrapper_builder_kwargs: dict, mocker):
    """Test that we can call ``get_wrapper`` for different Slurm wrapper configurations."""
    wrapper_factory = SlurmWrapperFactory(slurm_platform)

    wrapper_data = mocker.MagicMock()
    wrapper_data.het = {
        'HETSIZE': 0,
        'CURRENT_QUEUE': {
            0: 'debug'
        }
    }

    wallclock = '00:30'
    project = 'bsc32'

    wrapper_builder_kwargs['wrapper_data'] = wrapper_data
    wrapper_builder_kwargs['wallclock'] = wallclock
    wrapper_builder_kwargs['dependency'] = ''
    wrapper_builder_kwargs['project'] = project

    wrapper_builder_kwargs['method'] = method

    wrapper_cmd = wrapper_factory.get_wrapper(BashVerticalWrapperBuilder, **wrapper_builder_kwargs)
    assert f'-A {project}' in wrapper_cmd
    assert wallclock in wrapper_cmd

    horizontal_wrapper = wrapper_factory.horizontal_wrapper(**wrapper_builder_kwargs)
    assert type(horizontal_wrapper) is expected_horizontal_clazz

    vertical_horizontal_wrapper = wrapper_factory.hybrid_wrapper_vertical_horizontal(**wrapper_builder_kwargs)
    assert type(vertical_horizontal_wrapper) is expected_vertical_horizontal_clazz

    vertical_wrapper = wrapper_factory.vertical_wrapper(**wrapper_builder_kwargs)
    assert type(vertical_wrapper) is PythonVerticalWrapperBuilder

    horizontal_vertical_wrapper = wrapper_factory.hybrid_wrapper_horizontal_vertical(**wrapper_builder_kwargs)
    assert type(horizontal_vertical_wrapper) is PythonHorizontalVerticalWrapperBuilder

    assert '--reservation=operationals' in wrapper_factory.reservation_directive('operationals')


def test_wrapper_factory_slurm_placeholders(slurm_platform: SlurmPlatform, wrapper_builder_kwargs: dict, mocker):
    """Test that the Wrapper Factory is using placeholders correctly."""
    wrapper_factory = SlurmWrapperFactory(slurm_platform)

    wrapper_data = mocker.MagicMock()
    wrapper_data.het = {
        'HETSIZE': 0,
        'CURRENT_QUEUE': {
            0: 'debug'
        }
    }
    wrapper_data.jobs = []
    mocked_job = mocker.MagicMock()
    mocked_job.parameters = {
        'VAR1': 'Italy',
        'VAR2': '\\$CHOUGH'
    }
    wrapper_data.jobs.append(mocked_job)
    wallclock = '00:30'
    project = 'bsc32'

    wrapper_builder_kwargs['wrapper_data'] = wrapper_data
    wrapper_builder_kwargs['wallclock'] = wallclock
    wrapper_builder_kwargs['dependency'] = ''
    wrapper_builder_kwargs['project'] = project
    wrapper_builder_kwargs['method'] = 'srun'
    wrapper_builder_kwargs['jobs_scripts'] = ['echo %VAR1%', 'echo %VAR2%', 'echo %VAR3%']
    wrapper_cmd = wrapper_factory.get_wrapper(BashVerticalWrapperBuilder, **wrapper_builder_kwargs)

    assert 'Italy' in wrapper_cmd
    assert 'CHOUGH' in wrapper_cmd
    assert 'VAR3' not in wrapper_cmd


def test_wrapper_factory_slurm_hetsize_greater_than_one(slurm_platform: SlurmPlatform, wrapper_builder_kwargs: dict, mocker):
    """Test that specifying HETSIZE returns the right job/components."""
    wrapper_factory = SlurmWrapperFactory(slurm_platform)

    wrapper_data = mocker.MagicMock()
    wrapper_data.het = {
        'HETSIZE': 2,
        'CURRENT_QUEUE': {
            0: 'debug',
            1: 'debug',
            2: 'debug'
        }
    }
    wallclock = '00:30'
    project = 'bsc32'

    wrapper_builder_kwargs['wrapper_data'] = wrapper_data
    wrapper_builder_kwargs['wallclock'] = wallclock
    wrapper_builder_kwargs['dependency'] = ''
    wrapper_builder_kwargs['project'] = project
    wrapper_builder_kwargs['method'] = 'srun'
    wrapper_builder_kwargs['jobs_scripts'] = []

    # FIXME: We have a bug in header_directives, and in the Slurm Header:
    #        https://github.com/BSC-ES/autosubmit/issues/2660
    #        Once that's fixed we can remove this mocked object.
    mocker.patch.object(wrapper_factory, 'header_directives', return_value='')

    spy_builder = mocker.MagicMock(wraps=BashVerticalWrapperBuilder)

    assert 'nodes' not in wrapper_builder_kwargs

    assert wrapper_factory.get_wrapper(spy_builder, **wrapper_builder_kwargs)

    assert spy_builder.call_count == 1
    # Because the ``get_wrapper`` call will set nodes when hetsize is less or equal to one.
    assert 'nodes' not in spy_builder.call_args_list[0].kwargs


@pytest.mark.parametrize(
    'num_processors_value,nodes,expected',
    [
        [1, '1', '#SBATCH -n 1'],
        [0, 3, '#'],
        [3, 2, '#SBATCH -n 3'],
        ['1', 1, '#SBATCH -n 1'],
        ['K', '1', '#SBATCH -n 1'],
        ['1', 'K', '#SBATCH -n 1'],
    ]
)
def test_wrapper_factory_slurm_num_processors(
        num_processors_value: Union[str, int], nodes: Union[str, int], expected: str,
        slurm_platform: SlurmPlatform, wrapper_builder_kwargs: dict, mocker):
    """Test that the wrapper calculates the number of processors correctly."""
    wrapper_factory = SlurmWrapperFactory(slurm_platform)

    wrapper_data = mocker.MagicMock()
    wrapper_data.het = {
        'HETSIZE': 0,
        'CURRENT_QUEUE': {}
    }
    wrapper_data.nodes = nodes
    wallclock = '00:30'
    project = 'bsc32'

    wrapper_builder_kwargs['wrapper_data'] = wrapper_data
    wrapper_builder_kwargs['wallclock'] = wallclock
    wrapper_builder_kwargs['dependency'] = ''
    wrapper_builder_kwargs['project'] = project
    wrapper_builder_kwargs['method'] = 'srun'
    wrapper_builder_kwargs['jobs_scripts'] = []

    wrapper_builder_kwargs['num_processors_value'] = num_processors_value

    # FIXME: We have a bug in header_directives, and in the Slurm Header:
    #        https://github.com/BSC-ES/autosubmit/issues/2660
    #        Once that's fixed we can remove this mocked object.
    mocker.patch.object(wrapper_factory, 'header_directives', return_value='')

    spy_builder = mocker.MagicMock(wraps=BashVerticalWrapperBuilder)

    assert wrapper_factory.get_wrapper(spy_builder, **wrapper_builder_kwargs)

    assert spy_builder.call_count == 1

    num_processors = spy_builder.call_args_list[0].kwargs['num_processors']
    assert num_processors == expected


def test_ec_wrapper_factory(slurm_platform: SlurmPlatform, wrapper_builder_kwargs: dict, mocker):
    """A quick test for the EC platform wrapper factory.

    TODO: This may or may not be working, as we are not testing the EC platform very well.
    """
    wrapper_factory = EcWrapperFactory(slurm_platform)

    wrapper_data = mocker.MagicMock()
    wrapper_data.het = {
        'HETSIZE': 0,
        'CURRENT_QUEUE': {
            0: 'debug'
        }
    }

    wallclock = '00:30'
    project = 'bsc32'

    wrapper_builder_kwargs['wrapper_data'] = wrapper_data
    wrapper_builder_kwargs['wallclock'] = wallclock
    wrapper_builder_kwargs['dependency'] = ''
    wrapper_builder_kwargs['project'] = project
    wrapper_builder_kwargs['num_processors_value'] = 1
    wrapper_builder_kwargs['queue'] = 'debug'
    wrapper_builder_kwargs['partition'] = 'debug'
    wrapper_builder_kwargs['nodes'] = 2
    wrapper_builder_kwargs['tasks'] = 1
    wrapper_builder_kwargs['exclusive'] = True
    wrapper_builder_kwargs['custom_directives'] = []
    wrapper_builder_kwargs['method'] = 'srun'
    wrapper_builder_kwargs['executable'] = '/bin/bash'

    assert wrapper_factory.vertical_wrapper(**wrapper_builder_kwargs)
    assert wrapper_factory.horizontal_wrapper(**wrapper_builder_kwargs)
    assert wrapper_factory.header_directives(**wrapper_builder_kwargs)
    assert wrapper_factory.queue_directive('debug')
    assert 'PBS' in wrapper_factory.dependency_directive('job_a')

    assert '#' == wrapper_factory.reservation_directive('operationals')

    assert '' == wrapper_factory.get_custom_directives('')

    assert 'test a\ntest b' == wrapper_factory.get_custom_directives(['test a', 'test b'])

    assert '' == wrapper_factory.allocated_nodes()
