import time
from datetime import datetime, timedelta
from random import seed, randint, choice
from typing import Any, Dict, List, Tuple

import pytest

from autosubmit.job.job import Job
from autosubmit.statistics.jobs_stat import JobStat
from autosubmit.statistics.statistics import Statistics
from autosubmit.statistics.stats_summary import StatsSummary
from autosubmit.statistics.utils import timedelta2hours
from autosubmit.job.job_common import Status

NUM_JOBS = 5  # modify this value to test with different job number
MAX_NUM_RETRIALS_PER_JOB = 20  # modify this value to test with different retrials number


@pytest.fixture()
def job_with_different_retrials(mocker):
    job_aux = Job(name="example_name", job_id="example_id", status="COMPLETED", priority=0)
    job_aux.processors = "1"
    job_aux.wallclock = '00:05'
    job_aux.section = "example_section"
    job_aux.member = "example_member"
    job_aux.chunk = "example_chunk"
    job_aux.processors_per_node = "1"
    job_aux.tasks = "1"
    job_aux.nodes = "1"
    job_aux.exclusive = "example_exclusive"
    job_aux.retrials = 7

    retrials = [
        [
            datetime(2024, 3, 2, 15, 24, 16),
            datetime(2024, 3, 2, 15, 26, 14),
            datetime(2024, 3, 3, 00, 10, 7),
            "COMPLETED"
        ],
        [
            datetime(2024, 3, 2, 15, 17, 31),
            datetime(2024, 3, 2, 15, 23, 45),
            datetime(2024, 3, 2, 15, 24, 45),
            "FAILED"
        ],
        [
            datetime(2024, 3, 2, 15, 17, 31),
            datetime(1970, 1, 1, 2, 00, 00),
            datetime(2024, 3, 2, 15, 23, 45),
            "FAILED"
        ],
        [
            datetime(2024, 3, 2, 15, 17, 31),
            datetime(2024, 3, 2, 15, 23, 45),
            datetime(1970, 1, 1, 2, 00, 00),
            "FAILED"
        ],
        [
            datetime(2024, 3, 2, 15, 17, 31),
            datetime(2024, 3, 2, 15, 23, 45),
            "FAILED"
        ],
        [
            datetime(2024, 3, 2, 15, 17, 31),
            datetime(1970, 1, 1, 2, 00, 00),
            "FAILED"
        ],
        [
            datetime(2024, 3, 2, 15, 17, 31),
            "FAILED"
        ]
    ]
    mocker.patch("autosubmit.job.job.Job.get_last_retrials", return_value=retrials)

    job_stat_aux = JobStat("example_name", 1, float(5) / 60, "example_section",
                           "example_date", "example_member", "example_chunk", "1",
                           "1", "1", "example_exclusive")

    job_stat_aux.submit_time = retrials[len(retrials) - 1][0]
    job_stat_aux.start_time = None
    job_stat_aux.finish_time = None
    job_stat_aux.completed_queue_time = timedelta(seconds=118)
    job_stat_aux.completed_run_time = timedelta(seconds=31433)
    job_stat_aux.failed_queue_time = timedelta(seconds=374) * 3 + timedelta() * 2
    job_stat_aux.failed_run_time = timedelta(seconds=60) + timedelta(days=19784, seconds=48225) + timedelta()
    job_stat_aux.retrial_count = 7
    job_stat_aux.completed_retrial_count = 1
    job_stat_aux.failed_retrial_count = 6

    return [job_aux], job_stat_aux


@pytest.fixture()
def jobs(mocker) -> List[Job]:
    """
    :return: Jobs with random attributes and retrials.
    """
    jobs = []
    seed(time.time())
    submit_time = datetime(2023, 1, 1, 10, 0, 0)
    start_time = datetime(2023, 1, 1, 10, 30, 0)
    end_time = datetime(2023, 1, 1, 11, 0, 0)
    completed_retrial = [submit_time, start_time, end_time, "COMPLETED"]
    partial_retrials = [
        [submit_time, start_time, end_time, ""],
        [submit_time, start_time, ""],
        [submit_time, ""],
        [""]
    ]
    job_statuses = Status.LOGICAL_ORDER
    for i in range(NUM_JOBS):
        status = job_statuses[i % len(job_statuses)]  # random status
        job_aux = Job(
            name="example_name_" + str(i),
            job_id="example_id_" + str(i),
            status=status,
            priority=i
        )

        # Custom values for job attributes
        job_aux.processors = str(i)
        job_aux.wallclock = '00:05'
        job_aux.section = "example_section_" + str(i)
        job_aux.member = "example_member_" + str(i)
        job_aux.chunk = "example_chunk_" + str(i)
        job_aux.processors_per_node = str(i)
        job_aux.tasks = str(i)
        job_aux.nodes = str(i)
        job_aux.exclusive = "example_exclusive_" + str(i)

        num_retrials = randint(1, MAX_NUM_RETRIALS_PER_JOB)  # random number of retrials, grater than 0
        retrials = []

        for j in range(num_retrials):
            if j < num_retrials - 1:
                retrial = completed_retrial
            else:
                if job_aux.status == "COMPLETED":
                    retrial = completed_retrial
                else:
                    retrial = choice(partial_retrials)
                    if len(retrial) == 1:
                        retrial[0] = job_aux.status
                    elif len(retrial) == 2:
                        retrial[1] = job_aux.status
                    elif len(retrial) == 3:
                        retrial[2] = job_aux.status
                    else:
                        retrial[3] = job_aux.status
            retrials.append(retrial)
        mocker.patch("autosubmit.job.job.Job.get_last_retrials", return_value=retrials)
        jobs.append(job_aux)

    return jobs


@pytest.fixture()
def job_stats() -> List[JobStat]:
    """Create a list of ``JobStat`` with the same length as the ``NUM_JOBS`` constant."""
    job_stats_list = []

    for i in range(NUM_JOBS):
        job_stat_timedelta = timedelta()
        job_stat = JobStat(
            name="example_name" + str(i),
            processors=i,
            wallclock=float(i),
            section="",
            date="",
            member="",
            chunk="",
            processors_per_node="",
            tasks="",
            nodes="",
            exclusive="")
        job_stat.submit_time = datetime(2023, 1, 1, 10, 0, 0)
        job_stat.start_time = datetime(2023, 1, 1, 10, 30, 0)
        job_stat.finish_time = datetime(2023, 1, 1, 11, 0, 0)
        job_stat.completed_queue_time = job_stat_timedelta
        job_stat.completed_run_time = job_stat_timedelta
        job_stat.failed_queue_time = job_stat_timedelta
        job_stat.failed_run_time = job_stat_timedelta
        job_stat.retrial_count = i
        job_stat.completed_retrial_count = i
        job_stat.failed_retrial_count = i
        job_stats_list.append(job_stat)
    return job_stats_list


@pytest.fixture()
def statistics(jobs: List[Job], job_stats) -> Statistics:
    return Statistics(
        jobs=jobs,
        start=datetime(2023, 1, 1, 10, 0, 0),
        end=datetime(2023, 1, 1, 11, 0, 0),
        queue_time_fix={},
        jobs_stat=job_stats)


@pytest.fixture()
def summary(request):
    """Create a test statistics summary instance."""
    summary = StatsSummary()
    summary.submitted_count = (NUM_JOBS * (NUM_JOBS - 1)) // 2
    summary.run_count = (NUM_JOBS * (NUM_JOBS - 1)) // 2
    summary.completed_count = (NUM_JOBS * (NUM_JOBS - 1)) // 2
    summary.failed_count = (NUM_JOBS * (NUM_JOBS - 1)) // 2
    summary.expected_consumption = (NUM_JOBS * (NUM_JOBS - 1)) / 2
    summary.real_consumption = timedelta2hours(timedelta() + timedelta()) * NUM_JOBS
    summary.failed_real_consumption = timedelta2hours(timedelta() + timedelta()) * NUM_JOBS
    summary.expected_cpu_consumption = NUM_JOBS * (NUM_JOBS - 1) * (2 * NUM_JOBS - 1) / 6
    summary.cpu_consumption = sum(
        timedelta2hours(i * timedelta()) + timedelta2hours(i * timedelta()) for i in range(NUM_JOBS))
    summary.failed_cpu_consumption = sum(timedelta2hours(i * timedelta()) for i in range(NUM_JOBS))
    summary.total_queue_time = sum(timedelta2hours(timedelta() + timedelta()) for _ in range(NUM_JOBS))
    summary.cpu_consumption_percentage = 0.0
    return summary


@pytest.fixture()
def summary_as_list(summary: StatsSummary) -> List[str]:
    """Return the summary as a list of strings."""
    return [
        "Summary: ",
        f"CPU Consumption Percentage  :  {str(summary.cpu_consumption_percentage)}%",
        f"Total Queue Time  :  {round(summary.total_queue_time, 2)} hrs.",
        f"Submitted Count  :  {summary.submitted_count}",
        f"Run Count  :  {summary.run_count}",
        f"Completed Count  :  {summary.completed_count}",
        f"Failed Count  :  {summary.failed_count}",
        f"Expected Consumption  :  {round(summary.expected_consumption, 4)} hrs.",
        f"Real Consumption  :  {round(summary.real_consumption, 4)} hrs.",
        f"Failed Real Consumption  :  {round(summary.failed_real_consumption, 4)} hrs.",
        f"Expected CPU Consumption  :  {round(summary.expected_cpu_consumption, 4)} hrs.",
        f"CPU Consumption  :  {round(summary.cpu_consumption, 4)} hrs.",
        f"Failed CPU Consumption  :  {round(summary.failed_cpu_consumption, 4)} hrs."
    ]


@pytest.fixture()
def statistics_old_format() -> Dict[str, Any]:
    """Create an instance of ``Statistics`` but with the old format."""
    start_times = [datetime(2023, 1, 1, 10, 30, 0) for _ in range(NUM_JOBS)]
    end_times = [datetime(2023, 1, 1, 11, 0, 0) for _ in range(NUM_JOBS)]
    queued = [timedelta2hours(timedelta()) for _ in range(NUM_JOBS)]
    run = [timedelta2hours(timedelta()) for _ in range(NUM_JOBS)]
    failed_jobs = [i for i in range(NUM_JOBS)]
    max_fail = 0 if len(failed_jobs) == 0 else max(failed_jobs)
    fail_run = [timedelta2hours(timedelta()) for _ in range(NUM_JOBS)]
    fail_queued = [timedelta2hours(timedelta()) for _ in range(NUM_JOBS)]
    wallclocks = [float(i) for i in range(NUM_JOBS)]
    threshold = 0.0 if len(wallclocks) == 0 else max(wallclocks)
    max_queue = 0.0 if len(queued) == 0 else max(queued)
    max_run = 0.0 if len(run) == 0 else max(run)
    max_fail_queue = 0.0 if len(fail_queued) == 0 else max(fail_queued)
    max_fail_run = 0.0 if len(fail_run) == 0 else max(fail_run)
    max_time = max(max_queue, max_run, max_fail_queue, max_fail_run, threshold)

    return {
        'start_times': start_times,
        'end_times': end_times,
        'queued': queued,
        'run': run,
        'failed_jobs': failed_jobs,
        'max_fail': max_fail,
        'fail_run': fail_run,
        'fail_queued': fail_queued,
        'wallclocks': wallclocks,
        'threshold': threshold,
        'max_queue': max_queue,
        'max_run': max_run,
        'max_fail_queue': max_fail_queue,
        'max_fail_run': max_fail_run,
        'max_time': max_time
    }


@pytest.fixture()
def failed_jobs(job_stats) -> Dict[str, int]:
    """Create a dictionary with the failed jobs."""
    return {
        job_stats.name: idx
        for idx, job_stats in enumerate(job_stats)
        if idx > 0
    }


# -- tests --


def test_build_statistics_object(jobs: List[Job]) -> None:
    """Test that building a statistics object by chaining build calls works as expected."""
    exp_stats = (
        Statistics(
            jobs=jobs,
            start=datetime(2023, 1, 1, 10, 0, 0),
            end=datetime(2023, 1, 1, 11, 0, 0),
            queue_time_fix={}).
        calculate_statistics().
        calculate_summary().
        make_old_format().
        build_failed_jobs()
    )
    assert exp_stats.summary_list is not None


def test_calculate_statistics(statistics: Statistics, job_with_different_retrials: Tuple[List[Job], JobStat]) -> None:
    """Test that the statistics object is correctly built and obtained values are correct."""
    statistics._jobs = job_with_different_retrials[0]

    statistics.calculate_statistics()
    this = statistics.jobs_stat[0]
    that = job_with_different_retrials[1]

    # Times
    time_vars = ['submit_time', 'start_time', 'finish_time']

    for var in time_vars:
        assert getattr(this, var) == getattr(that, var)
    # Retrials
    retrials_vars = ['retrial_count', 'completed_retrial_count', 'failed_retrial_count']

    for var in retrials_vars:
        assert getattr(this, var) == getattr(that, var)
    # Queue/run times
    queue_run_vars = [
        'completed_queue_time', 'completed_queue_time', 'completed_run_time',
        'failed_queue_time', 'failed_run_time'
    ]

    for var in queue_run_vars:
        assert getattr(this, var) == getattr(that, var)

    # Times
    time_vars = ['submit_time', 'start_time', 'finish_time']

    for var in time_vars:
        assert getattr(this, var) == getattr(that, var)
    # Retrials
    retrials_vars = ['retrial_count', 'completed_retrial_count', 'failed_retrial_count']

    for var in retrials_vars:
        assert getattr(this, var) == getattr(that, var)
    # Queue/run times
    queue_run_vars = [
        'completed_queue_time', 'completed_queue_time', 'completed_run_time',
        'failed_queue_time', 'failed_run_time'
    ]

    for var in queue_run_vars:
        assert getattr(this, var) == getattr(that, var)


def test_calculate_summary(statistics: Statistics, summary: StatsSummary, job_stats: List[JobStat]) -> None:
    """Test that the summary is correctly calculated."""
    statistics.jobs_stat = job_stats  # TODO: Had to assign jobs_stats again here, as otherwise the test would fail -- investigate why"
    statistics.calculate_summary()
    statistics_summary = statistics.summary
    # Counter
    counter_vars = ['submitted_count', 'run_count', 'completed_count']

    for var in counter_vars:
        assert getattr(statistics_summary, var) == getattr(summary, var)
    # Consumption
    consumption_vars = ['expected_consumption', 'real_consumption', 'failed_real_consumption']

    for var in consumption_vars:
        assert getattr(statistics_summary, var) == getattr(summary, var)
    # CPU Consumption
    cpu_consumption_vars = [
        'expected_cpu_consumption', 'cpu_consumption', 'failed_cpu_consumption', 'total_queue_time',
        'cpu_consumption_percentage'
    ]

    for var in cpu_consumption_vars:
        assert getattr(statistics_summary, var) == getattr(summary, var)


def test_get_summary_as_list(statistics: Statistics, summary_as_list: List[str]) -> None:
    """Test that the summary is correctly converted to a list of strings."""
    statistics.calculate_summary()
    summary_as_list = statistics.summary_list

    assert summary_as_list == summary_as_list


def test_make_old_format(statistics: Statistics, statistics_old_format, job_stats: List[JobStat]) -> None:
    """Test that attributes of old and new statistics objects have the same value.

    Old is a dictionary. New is an object. Check the access method in the assertion.
    """
    statistics.jobs_stat = job_stats  # TODO: Had to assign jobs_stats again here, as otherwise the test would fail -- investigate why"
    statistics.make_old_format()

    for var in [
        'start_times', 'end_times', 'queued', 'run', 'failed_jobs', 'max_fail',
        'fail_run', 'fail_queued', 'wallclocks', 'threshold', 'max_time'
    ]:
        assert getattr(statistics, var) == statistics_old_format[var]


def test_build_failed_job_only(statistics: Statistics, failed_jobs, job_stats: List[JobStat]) -> None:
    """Test that failed jobs are correctly built."""
    statistics.jobs_stat = job_stats  # TODO: Had to assign jobs_stats again here, as otherwise the test would fail -- investigate why"
    statistics.make_old_format()
    statistics.build_failed_jobs()

    assert statistics.failed_jobs_dict == failed_jobs
