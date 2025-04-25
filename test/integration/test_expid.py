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

"""Tests for ``autosubmit expid``."""

from pathlib import Path
from log.log import AutosubmitCritical
from autosubmitconfigparser.config.basicconfig import BasicConfig
import pytest


@pytest.mark.parametrize(
    'type_flag,',
    [
        'op',
        'ev'
    ]
)
def test_copy_experiment(type_flag, autosubmit_exp):
    """Test that we can copy experiment using flags for operational and evaluation experiment types."""
    base_experiment = autosubmit_exp('t000', experiment_data={})

    is_operational = type_flag == 'op'
    is_evaluation = type_flag == 'ev'

    expid = base_experiment.autosubmit.expid(
        'test',
        hpc='local',
        copy_id=base_experiment.expid,
        operational=is_operational,
        evaluation=is_evaluation
    )

    assert expid

@pytest.mark.parametrize(
    'type_flag,',
    [
        'op',
        'ev'
    ]
)
def test_expid_mutually_exclusive_arguments(type_flag, autosubmit):
    """Test for issue 2280, where mutually exclusive arguments like op/ev flags and min were not working."""
    is_operational = type_flag == 'op'
    is_evaluation = type_flag == 'ev'

    expid = autosubmit.expid(
        'test',
        hpc='local',
        operational=is_operational,
        evaluation=is_evaluation,
        use_local_minimal=True
    )

    assert expid

@pytest.mark.parametrize(
        'has_min_yaml',
        [
            True,
            False
        ]
)
def test_copy_minimal(has_min_yaml, autosubmit_exp, autosubmit):
    """
    Test for issue 2280, ensure autousbmit expid -min --copy expid_id cannot be used if the experiment
    does not have a expid_id/conf/minimal.yml file
    """
    base_experiment = autosubmit_exp('t000', experiment_data={})

    expid = base_experiment.autosubmit.expid(
            'test',
            hpc='local',
            minimal_configuration=True
    )
    
    minimal_file = Path(BasicConfig.LOCAL_ROOT_DIR) / expid / "conf" / "minimal.yml"
    if has_min_yaml:
        minimal_file.write_text("test")
        expid2 = autosubmit.expid(
                minimal_configuration=True,
                copy_id=expid,
                description="Pytest experiment")
        assert expid2
        
        minimal2 = Path(BasicConfig.LOCAL_ROOT_DIR) / expid2 / "conf" / "minimal.yml"
        content = minimal2.read_text()
        assert content == "test", f"Unexpected content: {content}"
    else:
        minimal_file.unlink()
        with pytest.raises(AutosubmitCritical) as exc_info:
            autosubmit.expid(
                    minimal_configuration=True,
                    copy_id=expid,
                    description="Pytest experiment")
            assert exc_info.value.code == 7011
            assert "minimal.yml" in str(exc_info.value)
