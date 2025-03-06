import pytest
from collections import OrderedDict
from autosubmit.job.job import WrapperJob, Job
from autosubmit.job.job_common import Status


class DummyJob(Job):
    def __init__(self, name: str):
        super().__init__(name, 1, Status.READY, 0)
        self.new_status = None
        self.wrapper_type = "default"


class DummyWrapperJob(WrapperJob):
    def __init__(self, *args, check_time_return="dummy_time", check_inner_job_wallclock_return=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._test_check_time_return = check_time_return
        self._test_check_inner_job_wallclock_return = check_inner_job_wallclock_return

    def _check_time(self, *args, **kwargs):
        return self._test_check_time_return

    def _check_inner_job_wallclock(self, job):
        return self._test_check_inner_job_wallclock_return


@pytest.mark.parametrize(
    "line,check_inner_job_wallclock,expected_status_vertical,expected_status_whatever,expect_in_running_jobs_start",
    [
        ("job 19700203 19700204", True, Status.RUNNING, Status.FAILED, True),
        ("job 19700203 ", False, Status.RUNNING, Status.RUNNING, True),
        ("job 19700203 ", True, Status.RUNNING, Status.RUNNING, True),
        ("job 19700203 19700205", False, Status.RUNNING, Status.RUNNING, True),
        ("", None, None, None, False),
    ],
    ids=[
        "Overwallclock, stop running",
        "Finish time missing, keep running",
        "Finish time missing, keep running",
        "Not overwallclock, keep running",
        "Return without do anything"
    ]
)
def test_handle_job_status_line_parametrized(line, check_inner_job_wallclock, expected_status_vertical, expected_status_whatever, expect_in_running_jobs_start, mocker):
    """
    Parametrized test for _handle_job_status_line covering both vertical and non-vertical wrapper_type for each scenario.
    """
    for wrapper_type, expected_status in [("vertical", expected_status_vertical), ("whatever", expected_status_whatever)]:
        wrapper_job = DummyWrapperJob(
            name="wrapper",
            job_id=1,
            status=Status.READY,
            priority=0,
            job_list=[],
            total_wallclock="01:00:00",
            num_processors=1,
            platform=mocker.Mock(),
            as_config=mocker.Mock(),
            hold=False,
            check_inner_job_wallclock_return=check_inner_job_wallclock
        )
        if not line:
            jobs_dict = OrderedDict()
            wrapper_job.running_jobs_start = {}
            wrapper_job._handle_job_status_line(line, jobs_dict, False)
            continue
        job_name = line.split()[0]
        job = DummyJob(job_name)
        job.wrapper_type = wrapper_type
        jobs_dict = OrderedDict([(job_name, job)])
        wrapper_job.running_jobs_start = {}
        wrapper_job._handle_job_status_line(line, jobs_dict, False)
        assert job.new_status == expected_status, f"Failed for line={line}, wrapper_type={wrapper_type}"
        if expect_in_running_jobs_start and line:
            assert job in wrapper_job.running_jobs_start
        else:
            assert job not in wrapper_job.running_jobs_start
