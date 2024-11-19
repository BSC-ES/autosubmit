from typing import Callable

import pytest

from autosubmit.autosubmit import Autosubmit
from .conftest import ExperimentFactory

"""Test to check the functionality of the -y flag. It will copy the hpc too."""

_EXPERIMENT_DESCRIPTION = "test descript"


def test_create_expid_default_hpc(create_experiment: ExperimentFactory) -> None:
    """Create expid with the default hcp value (no -H flag defined).

    .. code-block:: console

        autosubmit expid  -d "test descript"

    :param create_autosubmit_exp: fixture that creates Autosubmit experiments.
    :type create_autosubmit_exp: Callable
    :return: None
    """
    # create default expid
    platform_name = "local"
    experiment = create_experiment(description=_EXPERIMENT_DESCRIPTION, platform=platform_name)
    expid = experiment.expid
    # capture the platform using the "describe"
    describe = Autosubmit.describe(expid)
    hpc_result = describe[4].lower()

    assert hpc_result == platform_name


@pytest.mark.parametrize("fake_hpc, expected_hpc", [
    ("mn5", "mn5"),
    ("", "local"), ])
def test_create_expid_flag_hpc(fake_hpc, expected_hpc, create_experiment: ExperimentFactory) -> None:
    """Create expid using the flag -H. Defining a value for the flag and not defining any value for that flag.

    .. code-block:: console

        autosubmit expid -H ithaca -d "experiment is about..."
        autosubmit expid -H "" -d "experiment is about..."

    :param fake_hpc: The value for the -H flag (hpc value)
    :param expected_hpc: The value it is expected for the variable hpc
    """
    # create default expid with know hpc
    experiment = create_experiment(description=_EXPERIMENT_DESCRIPTION, platform=fake_hpc)
    expid = experiment.expid
    # capture the platform using the "describe"
    describe = Autosubmit.describe(expid)
    hpc_result = describe[4].lower()

    assert hpc_result == expected_hpc


@pytest.mark.parametrize("fake_hpc, expected_hpc", [
    ("mn5", "mn5"),
    ("", "local"),
])
def test_copy_expid(fake_hpc, expected_hpc, create_experiment: ExperimentFactory) -> None:
    """Copy an experiment without indicating which is the new HPC platform

    .. code-block:: console

        autosubmit expid -y a000 -d "experiment is about..."

    :param fake_hpc: The value for the -H flag (hpc value)
    :param expected_hpc: The value it is expected for the variable hpc
    """
    # create default expid with know hpc
    experiment = create_experiment(description=_EXPERIMENT_DESCRIPTION, platform=fake_hpc)
    expid = experiment.expid
    # copy expid
    copy_expid = Autosubmit.expid(_EXPERIMENT_DESCRIPTION, "", expid)
    # capture the platform using the "describe"
    describe = Autosubmit.describe(copy_expid)
    hpc_result = describe[4].lower()

    assert hpc_result == expected_hpc


# copy expid with specific hpc should not change the hpc value
# autosubmit expid -y a000 -h local -d "experiment is about..."
def test_copy_expid_no(create_experiment: ExperimentFactory) -> None:
    """Copy an experiment with specific HPC platform

    .. code-block:: console

        autosubmit expid -y a000 -h local -d "experiment is about..."
    """
    # create default expid with know hpc
    fake_hpc = "mn5"
    new_hpc = "local"
    experiment = create_experiment(description=_EXPERIMENT_DESCRIPTION, platform=fake_hpc)
    expid = experiment.expid
    copy_expid = Autosubmit.expid(_EXPERIMENT_DESCRIPTION, new_hpc, expid)
    # capture the platform using the "describe"
    describe = Autosubmit.describe(copy_expid)
    hpc_result = describe[4].lower()

    assert hpc_result != new_hpc
