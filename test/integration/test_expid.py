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
from collections.abc import Callable
from pathlib import Path

import pytest
from autosubmitconfigparser.config.basicconfig import BasicConfig
from autosubmit.autosubmit import Autosubmit
from log.log import AutosubmitCritical


@pytest.mark.parametrize(
    'type_flag,',
    [
        'op',
        'ev'
    ]
)
def test_copy_experiment(type_flag: str, autosubmit_exp: Callable, autosubmit: Autosubmit) -> None:
    """Test that we can copy experiment using flags for operational and evaluation experiment types.

    :param type_flag: Variable to check which kind of flag it is.
    :type type_flag: bool
    :param autosubmit_exp: Autosubmit interface that instantiate with an experiment.
    :type autosubmit_exp: Callable
    :param autosubmit: Autosubmit interface that instantiate with no experiment.
    :type autosubmit: Autosubmit

    :return: None
    """
    base_experiment = autosubmit_exp('t000', experiment_data={})

    is_operational = type_flag == 'op'
    is_evaluation = type_flag == 'ev'

    expid = autosubmit.expid(
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
def test_expid_mutually_exclusive_arguments(type_flag: str, autosubmit: Autosubmit) -> None:
    """Test for issue 2280, where mutually exclusive arguments like op/ev flags and min were not working.

    :param type_flag: Variable to check which kind of flag it is.
    :type type_flag: bool
    :param autosubmit: Autosubmit interface that instantiate with no experiment.
    :type autosubmit: Autosubmit

    :return: None
    """
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
def test_copy_minimal(has_min_yaml: bool, autosubmit: Autosubmit) -> None:
    """
    Test for issue 2280, ensure autousbmit expid -min --copy expid_id cannot be used if the experiment
    does not have a expid_id/conf/minimal.yml file

    :param has_min_yaml: Variable to simulate if experiment has minimal or not.
    :type has_min_yaml: bool
    :param autosubmit: Autosubmit interface that instantiate with no experiment.
    :type autosubmit: Autosubmit

    :return: None
    """

    expid = autosubmit.expid(
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


def test_create_expid_default_hpc(autosubmit: Autosubmit) -> None:
    """Create expid with the default hcp value (no -H flag defined).

    .. code-block:: console

        autosubmit expid -d "test descript"

    :param autosubmit: Autosubmit interface that instantiate with no experiment.
    :type autosubmit: Autosubmit

    :return: None
    """
    # create default expid
    experiment_id = autosubmit.expid(
        'experiment_id',
        "",
        minimal_configuration=True
    )

    # capture the platform using the "describe"
    describe = autosubmit.describe(experiment_id)
    hpc_result  = describe[4].lower()

    assert hpc_result == "local"


@pytest.mark.parametrize("fake_hpc, expected_hpc", [
    ("mn5", "mn5"),
    ("", "local"), ])
def test_create_expid_flag_hpc(fake_hpc: str, expected_hpc: str, autosubmit: Autosubmit) -> None:
    """Create expid using the flag -H. Defining a value for the flag and not defining any value for that flag.

    .. code-block:: console

        autosubmit expid -H ithaca -d "experiment"
        autosubmit expid -H "" -d "experiment"

    :param fake_hpc: The value for the -H flag (hpc value).
    :type fake_hpc: str
    :param expected_hpc: The value it is expected for the variable hpc.
    :type expected_hpc: str
    :param autosubmit: Autosubmit interface that instantiate with no experiment.
    :type autosubmit: Autosubmit

    :return: None
    """
    # create default expid with know hpc

    experiment_id = autosubmit.expid(
        'experiment',
        fake_hpc,
        minimal_configuration=True
    )

    # capture the platform using the "describe"
    describe_experiment = autosubmit.describe(experiment_id)
    hpc_result_experiment = describe_experiment[4].lower()

    assert hpc_result_experiment == expected_hpc


@pytest.mark.parametrize("fake_hpc, expected_hpc", [
    ("mn5", "mn5"),
    ("", "local"),
])
def test_copy_expid(fake_hpc: str, expected_hpc: str, autosubmit: Autosubmit) -> None:
    """Copy an experiment without indicating which is the new HPC platform

    .. code-block:: console

        autosubmit expid -d "original" -H "<PLATFORM>"
        autosubmit expid -y a000 -d ""

    :param fake_hpc: The value for the -H flag (hpc value).
    :type fake_hpc: str
    :param expected_hpc: The value it is expected for the variable hpc.
    :type expected_hpc: str
    :param autosubmit: Autosubmit interface that instantiate with no experiment.
    :type autosubmit: Autosubmit

    :return: None
    """
    # create default expid with know hpc

    original_id = autosubmit.expid(
        'original',
        fake_hpc,
        minimal_configuration=True
    )

    copy_id = autosubmit.expid('copy', "", original_id)

    # capture the platform using the "describe"
    describe_copy = autosubmit.describe(copy_id)
    hpc_result_copy = describe_copy[4].lower()

    assert hpc_result_copy == expected_hpc


def test_copy_expid_no(autosubmit: Autosubmit) -> None:
    """Copying expid, but choosing another HPC value must create a new experiment with the chosen HPC value

    .. code-block:: console

        autosubmit expid -y a000 -h local -d "experiment is about..."

    :param autosubmit: Autosubmit interface that instantiates with no experiment.
    :type autosubmit: Autosubmit

    :return: None
    """
    # create default expid with know hpc
    fake_hpc = "mn5"
    new_hpc = "local"

    experiment_id = autosubmit.expid('original', fake_hpc)
    copy_experiment_id = autosubmit.expid("copy experiment", new_hpc, experiment_id)
    # capture the platform using the "describe"
    describe = autosubmit.describe(copy_experiment_id)
    hpc_result = describe[4].lower()

    assert hpc_result != new_hpc
