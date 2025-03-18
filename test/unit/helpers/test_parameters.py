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

from autosubmit.helpers.parameters import (
    autosubmit_parameter,
    autosubmit_parameters,
    PARAMETERS
)

"""Tests for the ``helpers.parameters`` module."""


def test_autosubmit_decorator():
    """Test the ``autosubmit_decorator``."""

    PARAMETERS.clear()

    parameter_name = 'JOBNAME'
    parameter_group = 'PLATFORM'

    class Job:
        @property
        @autosubmit_parameter(name=parameter_name, group=parameter_group)
        def name(self):
            """This parameter is the job name."""
            return 'FOO'

    job = Job()

    assert 'FOO' == job.name
    assert len(PARAMETERS) > 0
    # Defaults to the module name if not provided! So the class name
    # ``Job`` becomes ``JOB``.
    assert parameter_group in PARAMETERS
    assert parameter_name in PARAMETERS[parameter_group]
    assert 'This parameter is the job name.' == PARAMETERS[parameter_group][parameter_name]


def test_autosubmit_decorator_using_array():
    """Test the ``autosubmit_decorator``."""

    PARAMETERS.clear()

    parameter_names = ['JOBNAME', 'JOB____NAME']
    parameter_group = 'PLATFORM'

    class Job:
        @property
        @autosubmit_parameter(name=parameter_names, group=parameter_group)
        def name(self):
            """This parameter is the job name."""
            return 'FOO'

    job = Job()

    assert 'FOO' == job.name
    assert len(PARAMETERS) > 0
    # Defaults to the module name if not provided! So the class name
    # ``Job`` becomes ``JOB``.
    assert parameter_group in PARAMETERS
    for parameter_name in parameter_names:
        assert parameter_name in PARAMETERS[parameter_group]
        assert 'This parameter is the job name.' == PARAMETERS[parameter_group][parameter_name]


def test_autosubmit_decorator_no_group():
    """Test the ``autosubmit_decorator`` when ``group`` is not provided."""

    PARAMETERS.clear()

    parameter_name = 'JOBNAME'

    class Job:
        @property
        @autosubmit_parameter(name=parameter_name)
        def name(self):
            """This parameter is the job name."""
            return 'FOO'

    job = Job()

    assert 'FOO' == job.name
    assert len(PARAMETERS) > 0
    # Defaults to the module name if not provided! So the class name
    # ``Job`` becomes ``JOB``.
    assert Job.__name__.upper() in PARAMETERS
    assert parameter_name in PARAMETERS[Job.__name__.upper()]
    assert 'This parameter is the job name.' == PARAMETERS[Job.__name__.upper()][parameter_name]


def test_autosubmit_class_decorator():
    """Test the ``autosubmit_decorator`` when ``group`` is not provided."""

    PARAMETERS.clear()

    @autosubmit_parameters(parameters={
        'job': {
            'JOBNAME': 'The value!'
        }
    })
    class Job:
        @property
        def name(self):
            """This parameter is the job name."""
            return 'FOO'

    job = Job()

    assert 'FOO' == job.name
    assert len(PARAMETERS) > 0
    assert 'JOB' in PARAMETERS
    assert 'JOBNAME' in PARAMETERS['JOB']
    assert 'The value!' == PARAMETERS['JOB']['JOBNAME']
