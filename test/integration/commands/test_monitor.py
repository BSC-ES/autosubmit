from pathlib import Path

from autosubmit.config.basicconfig import BasicConfig
import pytest
from .conftest import wrapped_jobs


def new_as_exp(autosubmit_exp, general_data, experiment_data, wrapper_type: str, structure: dict, sizes: dict):
    config_data = general_data | experiment_data | wrapped_jobs(wrapper_type, structure, sizes)
    return autosubmit_exp(experiment_data=config_data, include_jobs=False, create=True)


@pytest.mark.parametrize("wrapper_type,min_trigger_status,from_step", [
    # ("simple", "READY", 0),
    # ("simple", "QUEUING", 0),
    # ("simple", "RUNNING", 0),
    # ("simple", "COMPLETED", 0),
    # ("simple", "FAILED", 0),
    # ("simple", "READY", 3),
    # ("simple", "QUEUING", 3),
    # ("simple", "RUNNING", 3),
    # ("simple", "COMPLETED", 3),
    # ("simple", "FAILED", 3),

    ('vertical', None, None),
    #("vertical", "READY", 0),
    # ("vertical", "QUEUING", 0),
    # ("vertical", "RUNNING", 0),
    # ("vertical", "COMPLETED", 0),
    # ("vertical", "FAILED", 0),
    # ("vertical", "READY", 3),
    # ("vertical", "QUEUING", 3),
    # ("vertical", "RUNNING", 3),
    # ("vertical", "COMPLETED", 3),
    # ("vertical", "FAILED", 3),
    #
    # ("horizontal", "READY", 0),
    # ("horizontal", "QUEUING", 0),
    # ("horizontal", "RUNNING", 0),
    # ("horizontal", "COMPLETED", 0),
    # ("horizontal", "FAILED", 0),
    # ("horizontal", "READY", 3),
    # ("horizontal", "QUEUING", 3),
    # ("horizontal", "RUNNING", 3),
    # ("horizontal", "COMPLETED", 3),
    # ("horizontal", "FAILED", 3),
    #
    # ("vertical-horizontal", "READY", 0),
    # ("vertical-horizontal", "QUEUING", 0),
    # ("vertical-horizontal", "RUNNING", 0),
    # ("vertical-horizontal", "COMPLETED", 0),
    # ("vertical-horizontal", "FAILED", 0),
    # ("vertical-horizontal", "READY", 3),
    # ("vertical-horizontal", "QUEUING", 3),
    # ("vertical-horizontal", "RUNNING", 3),
    # ("vertical-horizontal", "COMPLETED", 3),
    # ("vertical-horizontal", "FAILED", 3),
    #
    # ("horizontal-vertical", "READY", 0),
    # ("horizontal-vertical", "QUEUING", 0),
    # ("horizontal-vertical", "RUNNING", 0),
    # ("horizontal-vertical", "COMPLETED", 0),
    # ("horizontal-vertical", "FAILED", 0),
    # ("horizontal-vertical", "READY", 3),
    # ("horizontal-vertical", "QUEUING", 3),
    # ("horizontal-vertical", "RUNNING", 3),
    # ("horizontal-vertical", "COMPLETED", 3),
    # ("horizontal-vertical", "FAILED", 3),
])
def test_monitor(autosubmit_exp, wrapper_type, min_trigger_status, from_step, general_data, experiment_data):
    """Test the monitor of an experiment."""

    as_exp = new_as_exp(
        autosubmit_exp=autosubmit_exp,
        general_data=general_data,
        experiment_data=experiment_data,
        wrapper_type=wrapper_type,
        structure={'min_trigger_status': min_trigger_status, 'from_step': from_step} if min_trigger_status and from_step is not None else {},
        sizes={"MIN_V": 2, "MAX_V": 2, "MIN_H": 2, "MAX_H": 2}
    )

    # TODO: For now we will just check that the pdf is generated without any autosubmit error.
    # TODO: Show in the -d option, the wrapped jobs
    as_exp.autosubmit.monitor(
        as_exp.expid,
        hide=False, # disables the open() of the pdf after generation
        group_by=None,
        expand=[],
        expand_status=[],
        detail=True,
        file_format="pdf",
        lst=None,
        filter_chunks=None,
        filter_status=None,
        filter_section=None,
        check_wrapper=None if wrapper_type == "simple" else True,
    )

    # check that plot folder is generated
    plot_folder = Path(BasicConfig.LOCAL_ROOT_DIR) / as_exp.expid / "plot"
    assert plot_folder.exists()
    for plot_file in plot_folder.glob("*"):
        assert plot_file.stat().st_size > 0

    # Check that jobs_data.db contains the preview table and the wrapped jobs there
