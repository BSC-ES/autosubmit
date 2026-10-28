from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from autosubmit.job.job_list import JobList
from autosubmit.workflow.manage import inspect


@pytest.mark.parametrize(
    "lock_acquired, force, check_wrapper, expected_method",
    [
        (True, False, False, "get_job_list"),
        (True, True, False, "get_job_list"),
        (False, False, False, "get_unsubmitted"),
        (False, True, False, "get_unsubmitted"),
        (True, False, True, "get_uncompleted"),
        (True, True, True, "get_uncompleted"),
        (False, False, True, "get_unsubmitted"),
        (False, True, True, "get_uncompleted"),
    ],
)
def test_inspect_lock_behavior(
    lock_acquired,
    force,
    check_wrapper,
    expected_method,
):
    # Arrange
    job_list = MagicMock(spec=JobList)

    jobs = [
        SimpleNamespace(name="job1", section="SIM", file="job1.sh"),
        SimpleNamespace(name="job2", section="POST", file="job2.sh"),
    ]

    getattr(job_list, expected_method).return_value = jobs

    as_conf = MagicMock()
    as_conf.experiment_data = {"WRAPPERS": {}}

    with (
        patch(
            "autosubmit.workflow.manage.load_config",
            return_value=as_conf,
        ),
        patch(
            "autosubmit.workflow.manage.load_job_list",
            return_value=job_list,
        ),
        patch(
            "autosubmit.workflow.manage.generate_scripts_andor_wrappers",
        ) as generate_scripts,
        patch("autosubmit.workflow.manage.os.system"),
        patch("autosubmit.workflow.manage.signal.signal"),
    ):
        # Act
        inspect(
            "a000",
            "",
            "",
            "",
            "",
            force=force,
            check_wrapper=check_wrapper,
            quick=False,
            lock_acquired=lock_acquired,
        )

    # Assert
    job_list.clear_wrappers_db.assert_called()

    expected_jobs = (
        job_list.get_uncompleted.return_value
        if expected_method == "get_uncompleted"
        else job_list.get_unsubmitted.return_value
        if expected_method == "get_unsubmitted"
        else job_list.get_job_list.return_value
    )

    generate_scripts.assert_called_once_with(
        as_conf,
        job_list,
        only_wrappers=False,
        jobs=expected_jobs,
    )
