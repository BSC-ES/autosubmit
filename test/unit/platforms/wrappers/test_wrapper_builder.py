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

"""Unit tests for the wrapper_builder module."""

import pytest
from autosubmit.platforms.wrappers.wrapper_builder import WrapperDirector, FluxWrapperBuilder

_EXPID = 't000'
_WRAPPER_NAME = 't000_WRAPPER_SECT1_SECT2_17791879795588_0_4'

@pytest.fixture
def wrapper_builder_kwargs() -> dict:
    """Return the base arguments for the wrapper builder."""
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
        'name': _WRAPPER_NAME
    }

@pytest.fixture
def build_flux_wrapper_script(wrapper_builder_kwargs, mocker):
    """Return a function that builds a Flux wrapper script with the given custom environment setup."""
    def _wrapper_script(custom_env_setup: str) -> str:
        wrapper_data = mocker.MagicMock()
        wrapper_data.custom_env_setup = custom_env_setup

        kwargs = dict(wrapper_builder_kwargs)
        kwargs["wrapper_data"] = wrapper_data

        director = WrapperDirector()
        builder = FluxWrapperBuilder(**kwargs)
        return director.construct(builder)

    return _wrapper_script

def test_slurm_flux_wrapper_builder(build_flux_wrapper_script):
    """Test that the FluxWrapperBuilder correctly builds the wrapper script."""
    wrapper_script = build_flux_wrapper_script(custom_env_setup = "")
    assert "srun --cpu-bind=none" in wrapper_script
    assert "flux start" in wrapper_script
    assert "flux_runner_" in wrapper_script

def test_custom_env_setup_delegated(build_flux_wrapper_script):
    """
    Test that the custom environment setup is correctly included in the wrapper script for a delegated wrapper.
    Uses FluxWrapperBuilder as an example, but the same must apply to any delegated wrapper.
    """
    custom_env_setup = 'echo "Custom environment setup"'

    wrapper_script = build_flux_wrapper_script(custom_env_setup=custom_env_setup)
    assert custom_env_setup in wrapper_script

    wrapper_script = build_flux_wrapper_script(custom_env_setup='')
    assert "No commands provided" in wrapper_script

def test_unique_name_for_delegated_scripts(build_flux_wrapper_script):
    """
    Ensure that the unique part of the wrapper name is present in the delegated wrapper script.
    Uses FluxWrapperBuilder as an example, but the same must apply to any delegated wrapper.
    """
    wrapper_script = build_flux_wrapper_script(custom_env_setup='')
    assert "SECT1_SECT2_17791879795588_0_4" in wrapper_script
