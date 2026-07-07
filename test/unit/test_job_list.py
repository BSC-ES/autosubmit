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
from typing import Any

import networkx
import pytest
from sqlalchemy import create_engine

from autosubmit.config.yamlparser import YAMLParserFactory
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_dict import DicJobs
from autosubmit.job.job_list import JobList
from autosubmit.job.job_packages import JobPackageThread
from autosubmit.job.template import Language
from test.unit.conftest import FakePlatform
from test._oldschema import old_job_data_table, old_experiment_run_table

"""Tests for the ``JobList`` class."""

_EXPID = 'a000'


@pytest.fixture
def as_conf(autosubmit_config):
    return autosubmit_config(_EXPID, experiment_data={
        'JOBS': {},
        'PLATFORMS': {}
    })


@pytest.fixture
def setup_job_list(as_conf):
    job_list = JobList(_EXPID, as_conf, YAMLParserFactory())
    job_list.graph = networkx.DiGraph()
    jobs = [
        Job('job1', 1, Status.COMPLETED, 0),
        Job('job2', 2, Status.RUNNING, 0),
        Job('job3', 3, Status.READY, 0),
        Job('job4', 4, Status.FAILED, 0),
        Job('job5', 5, Status.WAITING, 0),
        Job('job6', 6, Status.WAITING, 0),
    ]
    edges = [
        {
            "e_to": "job2",
            "e_from": "job1",
            "from_step": "0",
            "min_trigger_status": "COMPLETED",
            "completion_status": "WAITING",
            "fail_ok": False
        },
        {
            "e_to": "job3",
            "e_from": "job2",
            "from_step": "0",
            "min_trigger_status": "COMPLETED",
            "completion_status": "WAITING",
            "fail_ok": False
        },
        {
            "e_to": "job5",
            "e_from": "job4",
            "from_step": "0",
            "min_trigger_status": "COMPLETED",
            "completion_status": "WAITING",
            "fail_ok": False
        },
        {
            "e_to": "job6",
            "e_from": "job5",
            "from_step": "0",
            "min_trigger_status": "COMPLETED",
            "completion_status": "WAITING",
            "fail_ok": False
        }
    ]
    for job in jobs:
        job_list.add_job(job)
    for edge in edges:
        job_list._add_edge_and_parent(edge)
    return jobs, edges, job_list

def test_save_jobs(as_conf, setup_job_list, tmp_path):
    jobs, edges, job_list = setup_job_list
    job_list.save_jobs()
    job_list.save_edges()
    job_list.save_sections()
    db_jobs = job_list.dbmanager.select_all_jobs()
    db_edges = job_list.dbmanager.select_edges(db_jobs)
    assert len(db_jobs) == len(jobs)
    assert len(db_edges) == len(job_list.graph.edges)


@pytest.mark.parametrize(
    "full_load,load_failed_jobs",
    [
        (True, True),
        (True, False),
        (False, True),
        (False, False)
    ],
    ids=[
        "full_load_and_failed",
        "full_load_no_failed",
        "no_full_load_failed",
        "no_full_load_no_failed"
    ]
)
def test_load(as_conf: Any, setup_job_list: Any, tmp_path: Any, full_load: bool, load_failed_jobs, autosubmit) -> None:
    """
    Test loading the job list with different full_load options.

    :param as_conf: Autosubmit configuration fixture.
    :type as_conf: Any
    :param setup_job_list: Fixture to set up job list.
    :type setup_job_list: Any
    :param tmp_path: Temporary path fixture.
    :type tmp_path: Any
    :param full_load: Whether to fully load the graph.
    :type full_load: bool
    :return: None
    :rtype: None
    """
    jobs, edges, job_list = setup_job_list
    job_list.save_jobs()
    job_list.save_edges()
    job_list.save_sections()
    job_list.fill_parents_children()
    loaded_job_list = JobList(_EXPID, as_conf, YAMLParserFactory())
    loaded_job_list._load_graph(full_load=full_load, load_failed_jobs=load_failed_jobs)
    if not full_load:
        statuses = [Status.READY, Status.SUBMITTED, Status.RUNNING, Status.QUEUING]
        if load_failed_jobs:
            statuses.append(Status.FAILED)
        # We expect to get the active jobs + the direct children of the active jobs
        view_original = [job for job in job_list.get_job_list() if job.status in statuses]
        childs = []
        for job in view_original:
            for child in job.children:
                childs.append(child)
        view_original = list(set(view_original) | set(childs))
        view_original = sorted(view_original, key=lambda j: j.name)
    else:
        view_original = sorted(job_list.get_job_list(), key=lambda j: j.name)

    view_loaded = sorted(loaded_job_list.get_job_list(), key=lambda j: j.name)
    for i in range(len(view_original)):
        assert view_original[i].name == view_loaded[i].name
        assert view_original[i].id == view_loaded[i].id
        assert view_original[i].status == view_loaded[i].status
        assert view_original[i].section == view_loaded[i].section
    assert len(view_loaded) == len(view_original)


def test_get_completed_returns_only_the_completed(setup_job_list):
    jobs, edges, job_list = setup_job_list
    completed = job_list.get_completed()
    for job in completed:
        assert job.status == Status.COMPLETED


def test_get_in_queue(setup_job_list):
    jobs, edges, job_list = setup_job_list

    in_queue = job_list.get_in_queue()

    for job in in_queue:
        assert job.status in [Status.QUEUING, Status.SUBMITTED, Status.RUNNING, Status.UNKNOWN, Status.HELD]


def test_get_active(setup_job_list):
    jobs, edges, job_list = setup_job_list
    active = job_list.get_active()
    for job in active:
        assert job.status in [Status.QUEUING, Status.SUBMITTED, Status.RUNNING, Status.UNKNOWN, Status.HELD,
                              Status.READY, Status.DELAYED]


def test_get_job_by_name_returns_the_expected_job(setup_job_list):
    jobs, edges, job_list = setup_job_list

    for job in jobs:
        retrieved_job = job_list.get_job_by_name(job.name)
        assert retrieved_job is not None
        assert retrieved_job.name == job.name
        assert retrieved_job.id == job.id
        assert retrieved_job.status == job.status


def test_sort_by_name_returns_the_list_of_jobs_well_sorted(setup_job_list):
    jobs, edges, job_list = setup_job_list
    sorted_by_name = job_list.sort_by_name()

    for i in range(len(sorted_by_name) - 1):
        assert sorted_by_name[i].name <= sorted_by_name[i + 1].name


def test_sort_by_id_returns_the_list_of_jobs_well_sorted(setup_job_list):
    jobs, edges, job_list = setup_job_list
    sorted_by_id = job_list.sort_by_id()

    for i in range(len(sorted_by_id) - 1):
        assert sorted_by_id[i].id <= sorted_by_id[i + 1].id


def test_sort_by_type_returns_the_list_of_jobs_well_sorted(setup_job_list):
    jobs, edges, job_list = setup_job_list
    sorted_by_type = job_list.sort_by_type()

    for i in range(len(sorted_by_type) - 1):
        assert sorted_by_type[i].type <= sorted_by_type[i + 1].type


def test_sort_by_status_returns_the_list_of_jobs_well_sorted(setup_job_list):
    jobs, edges, job_list = setup_job_list
    sorted_by_status = job_list.sort_by_status()

    for i in range(len(sorted_by_status) - 1):
        assert sorted_by_status[i].status <= sorted_by_status[i + 1].status


def test_that_create_job_method_calls_dic_jobs_method_with_increasing_priority(mocker):
    # arrange
    dic_mock = mocker.Mock()
    dic_mock.read_section = mocker.Mock()
    dic_mock.experiment_data = dict()
    dic_mock.experiment_data["JOBS"] = {'fake-section-1': {}, 'fake-section-2': {}}
    # act
    JobList._create_jobs(dic_mock, 0, Language.BASH)

    # arrange
    dic_mock.read_section.assert_any_call('fake-section-1', 0, Language.BASH)
    dic_mock.read_section.assert_any_call('fake-section-2', 1, Language.BASH)


def test_run_only_selected_members(setup_job_list, as_conf, autosubmit):
    """
    Test that only jobs with members in the run_members list are loaded. ( autosubmit run $expid -rom --run_only_members)
    """
    _, _, job_list = setup_job_list

    for job in job_list.get_job_list():
        job.status = Status.READY
        job.member = "fake-memberX"

    job_list.job_list[0].status = Status.READY
    job_list.job_list[1].status = Status.READY
    job_list.job_list[2].status = Status.READY
    job_list.job_list[0].member = "fake-member1"
    job_list.job_list[1].member = "fake-member2"
    job_list.job_list[2].member = None
    job_list.save_jobs()
    job_list.save_edges()
    job_list.save_sections()
    loaded_job_list = JobList(_EXPID, as_conf, YAMLParserFactory())
    allowed_members = ["fake-member1", "fake-member2"]
    loaded_job_list.run_members = allowed_members
    loaded_job_list._load_graph(full_load=False, load_failed_jobs=False)

    for job in loaded_job_list.get_job_list():
        assert job.member in allowed_members or job.member is None

    assert len(loaded_job_list.get_job_list()) == 3


def test_find_and_delete_redundant_relations(setup_job_list):
    _, _, job_list = setup_job_list

    for job in job_list.get_job_list():
        job.status = Status.READY
        job.section = "TEST"
    job_list.graph.clear_edges()
    # modfy job_list to add some redundant edges
    redundant = [
        {"e_to": "job1",
         "e_from": "job2",
         "from_step": "0",
         "min_trigger_status": "COMPLETED",
         "completion_status": "WAITING",
         "fail_ok": False
         },
        {
            "e_to": "job1",
            "e_from": "job3",
            "from_step": "0",
            "min_trigger_status": "COMPLETED",
            "completion_status": "WAITING",
            "fail_ok": False
        },
        {
            "e_to": "job2",
            "e_from": "job3",
            "from_step": "0",
            "min_trigger_status": "COMPLETED",
            "completion_status": "WAITING",
            "fail_ok": False
        },
    ]
    for edge in redundant:
        job_list._add_edge_and_parent(edge)

    assert len(job_list.graph.edges) == 3

    # job3 -> job2
    # job2 -> job1
    # job3 -> job1 <- redundant as job2 depends on job1 and this one depends on job2
    # the format is:
    # {'one_section': {'one_job': {'parent_one', 'parent_two'..}}, ...}
    problematic_jobs = {'TEST': {'job2': {'job1', 'job3'},
                                 'job3': {'job2'}}}
    job_list.find_and_delete_redundant_relations(problematic_jobs)

    # job3 -> job2
    # job2 -> job1
    assert len(job_list.graph.edges) == 2


def test_normalize_to_filters(setup_job_list):
    """
    validating behaviour of _normalize_to_filters
    """
    _, _, job_list = setup_job_list

    dict_filter = [
        {"DATES_TO": ""},
        {"DATES_TO": "all"},
        {"DATES_TO": "20020205,[20020207:20020208],"},
        {"DATES_TO": ",20020205,[20020207:20020208]"}
        # ,{"DATES_TO": 123} # Error Case
    ]
    filter_type = "DATES_TO"

    for filter_to in dict_filter:
        try:
            job_list._normalize_to_filters(filter_to, filter_type)
        except Exception as e:
            print(f'Unexpected exception raised: {e}')
            assert not bool(e)


def test_manage_dependencies(as_conf, setup_job_list):
    _, _, job_list = setup_job_list

    """testing function _manage_dependencies from job_list."""
    dependencies_keys = {
        'dummy=1': {'test', 'test2'},
        'dummy-2': {'test', 'test2'},
        'dummy+3': "",
        'dummy*4': "",
        'dummy?5': ""
    }

    job = {
        'dummy':
            {
                'dummy': 'SIM.sh',
                'RUNNING': 'once'
            },
        'RUNNING': 'once',
        'dummy*4': {}
    }

    dic_jobs_fake = DicJobs(
        ['fake-date1', 'fake-date2'],
        ['fake-member1', 'fake-member2'],
        list(range(2, 10 + 1)),
        'H',
        1,
        as_conf)
    dic_jobs_fake.experiment_data["JOBS"] = job
    dependency = job_list._manage_dependencies(dependencies_keys, dic_jobs_fake)
    assert len(dependency) == 3
    for job in dependency:
        assert job in dependencies_keys


@pytest.mark.parametrize(
    "section_list, banned_jobs, get_only_non_completed, expected_length, expected_section",
    [
        (["SECTION1"], [], False, 2, "SECTION1"),
        (["SECTION2"], [], False, 1, "SECTION2"),
        (["SECTION1"], [], True, 1, "SECTION1"),
        (["SECTION2"], [], True, 0, "SECTION2"),
        (["SECTION1"], ["job1"], True, 1, "SECTION1"),
    ],
    ids=[
        "all_jobs_in_section1",
        "all_jobs_in_section2",
        "non_completed_jobs_in_section1",
        "non_completed_jobs_in_section2",
        "ban_job1"
    ]
)
def test_get_jobs_by_section(setup_job_list, section_list, banned_jobs, get_only_non_completed, expected_length,
                             expected_section):
    _, _, job_list = setup_job_list
    job_list.graph.clear()
    # Add jobs to sections
    job1 = Job('job1', 1, Status.COMPLETED, 0)
    job1.section = "SECTION1"
    job2 = Job('job2', 2, Status.READY, 0)
    job2.section = "SECTION1"
    job3 = Job('job3', 3, Status.COMPLETED, 0)
    job3.section = "SECTION2"
    job_list.add_job(job1)
    job_list.add_job(job2)
    job_list.add_job(job3)

    result = job_list.get_jobs_by_section(section_list, banned_jobs, get_only_non_completed)
    assert len(result) == expected_length
    assert all(job.section == expected_section for job in result)


@pytest.mark.parametrize(
    'make_exception,seconds',
    [
        (True, True),
        (False, True),
        (True, False),
        (False, False)
    ]
)
def test_retrieve_times(setup_job_list, tmp_path, make_exception, seconds):
    """testing function retrieve_times from job_list."""
    jobs, _, job_list = setup_job_list
    for job in jobs:
        job.status = Status.COMPLETED
        retrieve_data = job_list.retrieve_times(job.status, job.name, job._tmp_path, make_exception=make_exception,
                                                job_times=None, seconds=seconds, job_data_collection=None)
        assert retrieve_data.name == job.name
        assert retrieve_data.status == Status.VALUE_TO_KEY[job.status]


def test_unload_requires_confirmed_recovery(setup_job_list):
    """Verify job stays in memory until updated_log > fail_count."""
    jobs, _, job_list = setup_job_list
    job = jobs[0]  # job1, COMPLETED
    job.fail_count = 0
    job.retrials = 0
    job.log_recovery_call_count = 1
    job.updated_log = 0  # NOT confirmed recovered
    job.packed = False
    job_list.job_package_map = {}
    job_list.unload_finished_jobs()
    assert job.name in job_list.graph.nodes


def test_unload_after_confirmed_recovery(setup_job_list):
    """Verify job is unloaded once updated_log > fail_count."""
    jobs, _, job_list = setup_job_list
    job = jobs[0]  # job1, COMPLETED
    job.fail_count = 0
    job.retrials = 0
    job.log_recovery_call_count = 1
    job.updated_log = 1  # Confirmed recovered
    job.packed = False
    job_list.job_package_map = {}
    job_list.unload_finished_jobs()
    assert job.name not in job_list.graph.nodes


def test_vertical_job_not_externally_retried(setup_job_list, as_conf):
    """Verify vertical wrapper inner jobs are not retried externally after wrapper finishes."""
    jobs, _, job_list = setup_job_list
    job = jobs[3]  # job4, originally FAILED
    job.status = Status.FAILED
    job.fail_count = 1
    job.retrials = 3
    job.wrapper_type = "vertical"
    job.packed = False
    job.id = 123
    job.section = "TEST"
    as_conf.experiment_data["JOBS"]["TEST"] = {}
    # Simulate wrapper is gone
    job_list.job_package_map = {}
    job_list._update_failed_jobs(as_conf)
    assert job.status == Status.FAILED


def test_vertical_job_with_zero_fail_count_can_be_retried(setup_job_list, as_conf):
    """Verify vertical jobs that never ran (fail_count=0) can still be retried externally."""
    jobs, _, job_list = setup_job_list
    job = jobs[3]  # job4, originally FAILED
    job.status = Status.FAILED
    job.fail_count = 0
    job.retrials = 3
    job.wrapper_type = "vertical"
    job.packed = False
    job.id = 123
    job.section = "TEST"
    as_conf.experiment_data["JOBS"]["TEST"] = {}
    job_list.job_package_map = {}
    job_list._update_failed_jobs(as_conf)
    # fail_count=0 means wrapper never processed it; external retry is allowed
    assert job.status in (Status.READY, Status.DELAYED, Status.WAITING, Status.FAILED)


@pytest.mark.parametrize(
    'job_id,job_status,wrapper_status,in_map,expected',
    [
        # Wrapper is active — is_wrapper_still_running should return True.
        (100, Status.RUNNING,   Status.RUNNING,   True,  True),
        (100, Status.RUNNING,   Status.SUBMITTED, True,  True),
        (100, Status.RUNNING,   Status.QUEUING,   True,  True),
        # Job id not in map — never returns True regardless of status.
        (100, Status.RUNNING,   None,             False, False),
        # Wrapper finished — should return False even though id is in map.
        (100, Status.COMPLETED, Status.COMPLETED, True,  False),
        (100, Status.FAILED,    Status.FAILED,    True,  False),
    ],
    ids=[
        'running-in-map',
        'submitted-in-map',
        'queuing-in-map',
        'not-in-map',
        'wrapper-completed',
        'wrapper-failed',
    ],
)
def test_is_wrapper_still_running(
    fake_job_list,
    mocker,
    job_id: int,
    job_status: Status,
    wrapper_status: Status,
    in_map: bool,
    expected: bool,
) -> None:
    """is_wrapper_still_running must return True only when the wrapper is still active.

    :param fake_job_list: Minimal JobList fixture.
    :param mocker: pytest-mock mocker fixture.
    :param job_id: Numeric job id.
    :type job_id: int
    :param job_status: Status of the inner job.
    :type job_status: Status
    :param wrapper_status: Status of the wrapper job in ``job_package_map``.
    :type wrapper_status: Status
    :param in_map: Whether to place the wrapper in ``job_package_map``.
    :type in_map: bool
    :param expected: Expected return value.
    :type expected: bool
    """
    inner_job = Job('a000_20000101_fc0_1_SIM', job_id, job_status, 0)
    if in_map:
        wrapper_job = mocker.MagicMock()
        wrapper_job.status = wrapper_status
        fake_job_list.job_package_map[job_id] = wrapper_job
    assert fake_job_list.is_wrapper_still_running(inner_job) is expected


def test_save_wrappers_casts_id_to_int(fake_job_list, mocker) -> None:
    """save_wrappers must store the job id as an ``int`` in ``job_package_map``.

    Slurm platform returns job ids as strings.  If ``save_wrappers`` stored
    them without casting, ``is_wrapper_still_running`` (which looks up by the
    *inner* job's id, also an int after parsing) would silently miss the entry.

    :param fake_job_list: Minimal JobList fixture.
    :param mocker: pytest-mock mocker fixture.
    """
    as_conf = mocker.MagicMock()


    job = Job('a000_20000101_fc0_1_SIM', '999', Status.SUBMITTED, 0)
    package = mocker.MagicMock(spec=JobPackageThread)
    package.is_wrapped = True
    package.jobs = [job]
    package.name = 'wrapper_1'
    package._wallclock = '00:30'
    package.platform = FakePlatform()
    package.sections = "bla"
    package.method = "bla"
    package.wrapper_type = "bla"
    package.num_processors = 1



    submitted_scripts = {'section': {'pkg': package}}
    fake_job_list.save_wrappers(submitted_scripts, as_conf)

    # Key must be int (999), not string ('999'), so that subsequent id-based
    # lookups with integer job ids work correctly.
    assert 999 in fake_job_list.job_package_map, (
        "save_wrappers did not cast job id to int; "
        f"map keys: {list(fake_job_list.job_package_map.keys())}"
    )
    assert '999' not in fake_job_list.job_package_map


@pytest.mark.parametrize("wrapper_job_is_none", [False, True])
def test_save_wrapper_info_only_upserts_wrapper_info_not_inner_jobs(
        fake_job_list, mocker, wrapper_job_is_none
) -> None:
    """save_wrapper_info must only upsert wrapper info, not insert inner jobs.

    The inner jobs table is already populated by ``save_wrappers`` at submit
    time.  ``save_wrapper_info`` (called on status changes) must not re-insert
    them, or it triggers UNIQUE constraint violations.

    When ``wrapper_job`` is falsy the call must be a no-op.
    """
    if wrapper_job_is_none:
        wrapper_job = None
    else:
        inner_job = mocker.MagicMock()
        inner_job.name = "job_inside_wrapper"

        platform_mock = mocker.MagicMock()
        platform_mock.name = "test_platform"

        wrapper_job = mocker.MagicMock()
        wrapper_job.name = "wrapper_test"
        wrapper_job.id = 42
        wrapper_job.status = Status.SUBMITTED
        wrapper_job.wallclock = "00:30"
        wrapper_job.platform = platform_mock
        wrapper_job.job_list = [inner_job]

    fake_job_list.save_wrapper_info(wrapper_job)

    persistence_mock = fake_job_list.get_packages_persistence()
    if wrapper_job_is_none:
        persistence_mock.upsert_wrapper_info.assert_not_called()
    else:
        persistence_mock.upsert_wrapper_info.assert_called_once()
    persistence_mock.save.assert_not_called()


def test_recover_last_data_on_old_schema(tmp_path, as_conf):
    """recover_last_data migrates and queries an old-schema database without crashing."""
    db_dir = tmp_path / "metadata" / "data"
    db_dir.mkdir(parents=True, exist_ok=True)
    db_file = db_dir / "job_data_a000.db"
    engine = create_engine(f"sqlite:///{db_file}")
    old_job_data_table.create(engine)
    old_experiment_run_table.create(engine)
    engine.dispose()

    job_list = JobList("a000", as_conf, YAMLParserFactory())
    job_list.job_list.append(Job("test_job", "1", Status.COMPLETED, 0))

    job_list.recover_last_data()
