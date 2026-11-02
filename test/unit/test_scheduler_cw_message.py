"""Log copy for the -cw/--check_wrapper auxiliary job list (#3393)."""

import inspect

from autosubmit.scheduler import generate_scripts_andor_wrappers


def test_auxiliary_job_list_warning_names_cw_flag_correctly():
    source = inspect.getsource(generate_scripts_andor_wrappers)
    assert "-CW" not in source
    assert "-cw/--check_wrapper" in source
    assert "auxiliary job list because of" in source
    assert "job_list used for" not in source
