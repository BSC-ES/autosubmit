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
from unittest.mock import patch

import networkx
import pytest
from sqlalchemy import create_engine

from autosubmit.config.yamlparser import YAMLParserFactory
from autosubmit.database.db_manager_job_list import _edge_satisfied
from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_dict import DicJobs
from autosubmit.job.job_list import JobList
from autosubmit.job.job_packages import JobPackageThread
from autosubmit.job.template import Language
from test._oldschema import old_experiment_run_table, old_job_data_table
from test.unit.conftest import FakePlatform

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
    jobs, _edges, job_list = setup_job_list
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
def test_load(as_conf: Any, setup_job_list: Any, tmp_path: Any, full_load: bool, load_failed_jobs) -> None:
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
    _jobs, _edges, job_list = setup_job_list
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
        # Active jobs
        view_original = [job for job in job_list.get_job_list() if job.status in statuses]
        childs = []
        for job in view_original:
            for child in job.children:
                if child.name in {j.name for j in view_original}:
                    continue
                edge = job_list.graph.edges.get((job.name, child.name), {})
                if _edge_satisfied(
                    parent_status=Status.VALUE_TO_KEY.get(job.status, ''),
                    min_trigger_status=edge.get("min_trigger_status", "COMPLETED"),
                    fail_ok=bool(edge.get("fail_ok", False)) if edge.get("fail_ok") is not None else False,
                    from_step=edge.get("from_step", 0),
                    child_checkpoint_step=child.current_checkpoint_step or 0,
                ):
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
    _jobs, _edges, job_list = setup_job_list
    completed = job_list.get_completed()
    for job in completed:
        assert job.status == Status.COMPLETED


def test_get_in_queue(setup_job_list):
    _jobs, _edges, job_list = setup_job_list

    in_queue = job_list.get_in_queue()

    for job in in_queue:
        assert job.status in [Status.QUEUING, Status.SUBMITTED, Status.RUNNING, Status.UNKNOWN, Status.HELD]


def test_get_active(setup_job_list):
    _jobs, _edges, job_list = setup_job_list
    active = job_list.get_active()
    for job in active:
        assert job.status in [Status.QUEUING, Status.SUBMITTED, Status.RUNNING, Status.UNKNOWN, Status.HELD,
                              Status.READY, Status.DELAYED]


def test_get_job_by_name_returns_the_expected_job(setup_job_list):
    jobs, _edges, job_list = setup_job_list

    for job in jobs:
        retrieved_job = job_list.get_job_by_name(job.name)
        assert retrieved_job is not None
        assert retrieved_job.name == job.name
        assert retrieved_job.id == job.id
        assert retrieved_job.status == job.status


def test_sort_by_name_returns_the_list_of_jobs_well_sorted(setup_job_list):
    _jobs, _edges, job_list = setup_job_list
    sorted_by_name = job_list.sort_by_name()

    for i in range(len(sorted_by_name) - 1):
        assert sorted_by_name[i].name <= sorted_by_name[i + 1].name


def test_sort_by_id_returns_the_list_of_jobs_well_sorted(setup_job_list):
    _jobs, _edges, job_list = setup_job_list
    sorted_by_id = job_list.sort_by_id()

    for i in range(len(sorted_by_id) - 1):
        assert sorted_by_id[i].id <= sorted_by_id[i + 1].id


def test_sort_by_type_returns_the_list_of_jobs_well_sorted(setup_job_list):
    _jobs, _edges, job_list = setup_job_list
    sorted_by_type = job_list.sort_by_type()

    for i in range(len(sorted_by_type) - 1):
        assert sorted_by_type[i].type <= sorted_by_type[i + 1].type


def test_sort_by_status_returns_the_list_of_jobs_well_sorted(setup_job_list):
    _jobs, _edges, job_list = setup_job_list
    sorted_by_status = job_list.sort_by_status()

    for i in range(len(sorted_by_status) - 1):
        assert sorted_by_status[i].status <= sorted_by_status[i + 1].status


def test_that_create_job_method_calls_dic_jobs_method_with_increasing_priority(mocker):
    # arrange
    dic_mock = mocker.Mock()
    dic_mock.read_section = mocker.Mock()
    dic_mock.experiment_data = {"JOBS": {'fake-section-1': {}, 'fake-section-2': {}}}
    # act
    JobList._create_jobs(dic_mock, 0, Language.BASH)

    # arrange
    dic_mock.read_section.assert_any_call('fake-section-1', 0, Language.BASH)
    dic_mock.read_section.assert_any_call('fake-section-2', 1, Language.BASH)


def test_run_only_selected_members(setup_job_list, as_conf):
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


def test_get_jobs_by_section_db(setup_job_list):
    """get_jobs_by_section_db delegates to dbmanager with correct parameters."""
    _, _, job_list = setup_job_list
    with patch.object(job_list.dbmanager, 'select_job_names_by_sections', return_value={"job1"}) as mock:
        result = job_list.get_jobs_by_section_db(
            ["SEC1"], banned_jobs=["banned"], get_only_non_completed=True, status_filter="READY",
        )
        assert result == {"job1"}
        mock.assert_called_once_with(
            sections=["SEC1"],
            exclude_names={"banned"},
            exclude_completed=True,
            status_filter="READY",
        )


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
    package._num_processors = 1



    submitted_scripts = {'section': {'pkg': package}}
    fake_job_list.save_wrappers(submitted_scripts, as_conf)

    # Key must be int (999), not string ('999'), so that subsequent id-based
    # lookups with integer job ids work correctly.
    assert 999 in fake_job_list.job_package_map, (
        "save_wrappers did not cast job id to int; "
        f"map keys: {list(fake_job_list.job_package_map.keys())}"
    )
    assert '999' not in fake_job_list.job_package_map


@pytest.mark.parametrize(
    "parent_statuses,fail_ok,expected",
    [
        ([], False, True),
        ([Status.COMPLETED], False, True),
        ([Status.COMPLETED, Status.COMPLETED], False, True),
        ([Status.COMPLETED, Status.SKIPPED], False, True),
        ([Status.FAILED], True, True),
        ([Status.FAILED, Status.COMPLETED], True, True),
        ([Status.FAILED], False, False),
        ([Status.FAILED, Status.RUNNING], True, False),
        ([Status.COMPLETED, Status.FAILED], True, True),
        ([Status.COMPLETED, Status.FAILED], False, False),
    ],
    ids=[
        "no_parents",
        "all_completed",
        "all_completed_multiple",
        "all_completed_or_skipped",
        "single_failed_fail_ok",
        "mixed_failed_fail_ok_and_completed",
        "single_failed_no_fail_ok",
        "all_failed_fail_ok",
        "mixed_fail_ok",
        "mixed_no_fail_ok_blocks",
    ]
)
def test_update_waiting_and_delayed_jobs(
    as_conf,
    tmp_path,
    parent_statuses: Any,
    fail_ok: bool,
    expected: bool,
) -> None:
    """Test _update_waiting_and_delayed_jobs with different parent/fail_ok combinations."""
    job_list = JobList("a000", as_conf, YAMLParserFactory())
    job_list.graph = networkx.DiGraph()
    child = Job("child", 99, Status.WAITING, 0)
    job_list.add_job(child)

    for i, status in enumerate(parent_statuses):
        parent = Job(f"parent{i}", i, status, 0)
        job_list.add_job(parent)
        job_list.graph.add_edge(parent.name, child.name, fail_ok=fail_ok)

    job_list.fill_parents_children()
    job_list._update_waiting_and_delayed_jobs()

    if expected:
        assert child.status == Status.READY
    else:
        assert child.status == Status.WAITING


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
    job_list.add_job(Job("test_job", "1", Status.COMPLETED, 0))

    job_list.recover_last_data()


def test_recover_logs_skips_jobs_without_valid_id_or_submit_time(as_conf, mocker):
    """Jobs without a valid scheduler id or submit time are not handed to log recovery."""
    job_list = JobList("a000", as_conf, YAMLParserFactory(), disable_save=True)
    invalid = Job("bad_job", None, Status.COMPLETED, 0)
    valid = Job("good_job", "42", Status.FAILED, 0)
    valid.submit_time_timestamp = "20200101000000"
    for job in (invalid, valid):
        job_list.add_job(job)

    mocked_recover_log = mocker.patch.object(Job, "recover_log", return_value=None)
    mocked_save = mocker.patch.object(job_list, "save_jobs")

    assert job_list.recover_logs() is True

    assert invalid.updated_log == invalid.fail_count + 1
    assert valid.updated_log == 0
    assert mocked_recover_log.call_count == 1
    mocked_save.assert_called_once()


def test_recover_last_data_restores_and_marks_finished_jobs(as_conf, mocker):
    """recover_last_data leaves meaningful jobs untouched and restores/marks the rest."""
    job_list = JobList("a000", as_conf, YAMLParserFactory(), disable_save=True)
    meaningful = Job("meaningful", "5", Status.COMPLETED, 0)
    meaningful.submit_time_timestamp = "20200101000000"
    meaningful.updated_log = 0
    lost = Job("lost", None, Status.FAILED, 0)
    job_list.add_job(meaningful)
    job_list.add_job(lost)

    mocked_history = mocker.patch("autosubmit.job.job_list.ExperimentHistory")
    mocked_history.return_value.manager.get_jobs_data_last_row.return_value = {
        lost.name: {"job_id": 7, "out": "out.log", "err": "err.log", "submit": 1600000000},
    }

    job_list.recover_last_data([meaningful, lost])

    assert meaningful.id == "5"
    assert meaningful.updated_log == 0
    assert lost.id == 7
    assert lost.local_logs == "out.log"
    assert lost.remote_logs == "err.log"
    assert lost.has_valid_submit_time()
    assert lost.updated_log == lost.fail_count + 1


def _mock_platform(mocker, connected: bool = True, name: str = "pl"):
    platform = mocker.MagicMock()
    platform.name = name
    platform.connected = connected
    platform.serial_platform = platform
    return platform


def test_update_from_file_applies_case_insensitive_changes(as_conf, mocker, tmp_path):
    """update_from_file parses job names and statuses case-insensitively and archives the file."""
    job_list = JobList("a000", as_conf, YAMLParserFactory(), disable_save=True)
    job = Job("MyJob", "1", Status.WAITING, 0)
    job_list.add_job(job)
    update_path = tmp_path / "updated_list_a000.txt"
    job_list._update_file_path = update_path
    update_path.write_text("myjob COMPLETED\n", encoding="utf-8")
    mocked_change = mocker.patch("autosubmit.job.job_list.change_jobs_status")

    assert job_list.update_from_file() is True
    assert mocked_change.call_args.args[0] == [(job, Status.COMPLETED)]
    assert not update_path.exists()
    assert list(tmp_path.glob("updated_list_a000.txt_*"))


@pytest.mark.parametrize(
    "line, expected_status",
    [
        ("job1 RUNNING", Status.WAITING),  # active targets are rejected
        ("job1 BOGUS", Status.WAITING),  # unknown status is skipped
        ("ghost COMPLETED", None),  # unknown job is skipped
    ],
    ids=["active-target-rejected", "unknown-status", "unknown-job"],
)
def test_update_from_file_skips_invalid_entries(as_conf, mocker, tmp_path, line, expected_status):
    """update_from_file warns and skips entries it cannot apply."""
    mocker.patch("autosubmit.config.basicconfig.BasicConfig.LOCAL_ROOT_DIR", str(tmp_path))
    job_list = JobList("a000", as_conf, YAMLParserFactory(), disable_save=True)
    job = Job("job1", "1", Status.WAITING, 0)
    job_list.add_job(job)
    update_path = tmp_path / "updated_list_a000.txt"
    job_list._update_file_path = update_path
    update_path.write_text(f"{line}\n", encoding="utf-8")

    assert job_list.update_from_file(store_change=False) is True
    if expected_status is not None:
        assert job.status == expected_status
    else:
        assert job.status == Status.WAITING


def test_update_from_file_cancels_active_job_when_platform_connected(as_conf, mocker, tmp_path):
    """Active jobs are cancelled (via the shared helper) only when their platform is reachable."""
    job_list = JobList("a000", as_conf, YAMLParserFactory(), disable_save=True)
    platform = _mock_platform(mocker, connected=True)
    active = Job("job_active", "77", Status.RUNNING, 0)
    active.platform = platform
    job_list.add_job(active)
    update_path = tmp_path / "updated_list_a000.txt"
    job_list._update_file_path = update_path
    update_path.write_text("job_active COMPLETED\n", encoding="utf-8")

    assert job_list.update_from_file(store_change=False) is True
    assert active.status == Status.COMPLETED
    platform.cancel_jobs.assert_called_once_with(["77"])


def test_update_from_file_skips_active_job_when_platform_not_connected(as_conf, mocker, tmp_path):
    """Active jobs on unreachable platforms keep their status."""
    job_list = JobList("a000", as_conf, YAMLParserFactory(), disable_save=True)
    platform = _mock_platform(mocker, connected=False)
    active = Job("job_active", "77", Status.RUNNING, 0)
    active.platform = platform
    job_list.add_job(active)
    update_path = tmp_path / "updated_list_a000.txt"
    job_list._update_file_path = update_path
    update_path.write_text("job_active COMPLETED\n", encoding="utf-8")

    assert job_list.update_from_file(store_change=False) is True
    assert active.status == Status.RUNNING
    platform.cancel_jobs.assert_not_called()


def test_update_from_file_applies_change_to_job_stored_only_in_database(as_conf, mocker, tmp_path):
    """update_from_file resolves jobs absent from memory against the database, resets the
    per-attempt state for re-runnable targets and does not load them into the graph."""
    mocker.patch("autosubmit.config.basicconfig.BasicConfig.LOCAL_ROOT_DIR", str(tmp_path))
    job_list = JobList("a000", as_conf, YAMLParserFactory())
    stored = Job("STOREDJOB", 9, Status.COMPLETED, 0)
    job_list.dbmanager.save_jobs([stored])
    update_path = tmp_path / "updated_list_a000.txt"
    job_list._update_file_path = update_path
    update_path.write_text("storedjob WAITING\n", encoding="utf-8")

    assert job_list.update_from_file() is True

    row = job_list.dbmanager.load_job_by_name("STOREDJOB")
    assert row["status"] == "WAITING"
    assert row["id"] is None
    assert row["fail_count"] == 0
    assert row["updated_log"] == 0
    assert job_list.get_job_by_name("STOREDJOB") is None
    assert not update_path.exists()


@pytest.mark.parametrize(
    "line, expected",
    [
        ("", None),  
        ("   \n", None),
        ("# a comment", None),
        ("   # indented comment", None),
        ("job1", None),
        ("job1 COMPLETED trailing", ("JOB1", "COMPLETED")),
        ("myjob waiting", ("MYJOB", "WAITING")),
    ],
    ids=["blank", "whitespace", "comment", "indented-comment", "missing-status",
         "extra-tokens", "uppercased"],
)
def test_parse_update_line(line, expected):
    """_parse_update_line strips, uppercases and ignores blank/comment/malformed lines."""
    assert JobList._parse_update_line(line, "updated_list_a000.txt") == expected


@pytest.mark.parametrize(
    "memory_job, db_node, lookup_name, expected_name, expected_from_db",
    [
        pytest.param(Job("MEM", 1, Status.WAITING, 0), None, "MEM", "MEM", False,
                     id="in-memory-first"),
        pytest.param(None, Job("ONLYDB", 9, Status.COMPLETED, 0).__getstate__(),
                     "ONLYDB", "ONLYDB", True, id="from-database"),
        pytest.param(None, None, "GHOST", None, False, id="unknown-everywhere"),
    ],
)
def test_resolve_update_job(as_conf, mocker, memory_job, db_node, lookup_name,
                            expected_name, expected_from_db):
    """_resolve_update_job checks memory first, then the database, and yields None when unknown."""
    job_list = JobList("a000", as_conf, YAMLParserFactory(), disable_save=True)
    index = {memory_job.name.upper(): memory_job} if memory_job else {}
    mocked_db = mocker.patch.object(job_list.dbmanager, "load_job_by_name", return_value=db_node)

    job, from_db = job_list._resolve_update_job(lookup_name, index)

    assert from_db is expected_from_db
    if expected_name is None:
        assert job is None
    elif memory_job is not None:
        assert job is memory_job
        mocked_db.assert_not_called()
    else:
        assert job is not None
        assert job.name == expected_name
        assert job.status == Status.COMPLETED


def test_persist_unloaded_job_changes_splits_reset_and_edge_updates(as_conf, mocker):
    """Unloaded changes are saved with reset counters for re-runnable targets and reconcile
    the DB edges of final targets, keeping everything else untouched."""
    job_list = JobList("a000", as_conf, YAMLParserFactory())
    mocked_save = mocker.patch.object(job_list.dbmanager, "save_jobs")
    mocked_edges = mocker.patch.object(job_list.dbmanager, "update_outgoing_edges_completion")
    rerunnable = Job("RERUN", None, Status.READY, 0)
    final = Job("DONE", 5, Status.COMPLETED, 0)
    other = Job("UNKNOWN_STATE", None, Status.UNKNOWN, 0)
    pairs = [(rerunnable, Status.READY), (final, Status.COMPLETED), (other, Status.UNKNOWN)]

    job_list._persist_unloaded_job_changes(pairs)

    mocked_save.assert_has_calls([
        mocker.call([rerunnable], reset_log_counters=True),
        mocker.call([final, other], reset_log_counters=False),
    ])
    mocked_edges.assert_called_once_with("DONE", "COMPLETED")


def test_persist_unloaded_job_changes_skipped_when_saving_disabled(as_conf, mocker):
    """Unloaded changes are not persisted when saving is disabled."""
    job_list = JobList("a000", as_conf, YAMLParserFactory(), disable_save=True)
    mocked_save = mocker.patch.object(job_list.dbmanager, "save_jobs")
    mocked_edges = mocker.patch.object(job_list.dbmanager, "update_outgoing_edges_completion")

    job_list._persist_unloaded_job_changes([(Job("X", None, Status.COMPLETED, 0), Status.COMPLETED)])

    mocked_save.assert_not_called()
    mocked_edges.assert_not_called()


def test_update_from_file_returns_false_without_update_file(as_conf, tmp_path):
    """update_from_file returns False and changes nothing when no update file exists."""
    job_list = JobList("a000", as_conf, YAMLParserFactory(), disable_save=True)
    job_list._update_file_path = tmp_path / "updated_list_a000.txt"

    assert job_list.update_from_file() is False


@pytest.mark.parametrize("store_change, file_kept", [(False, True), (True, False)])
def test_update_from_file_store_change_controls_archiving(as_conf, tmp_path, store_change, file_kept):
    """store_change controls whether the update file is archived after processing."""
    job_list = JobList("a000", as_conf, YAMLParserFactory(), disable_save=True)
    job = Job("job1", "1", Status.WAITING, 0)
    job_list.add_job(job)
    update_path = tmp_path / "updated_list_a000.txt"
    job_list._update_file_path = update_path
    update_path.write_text("job1 COMPLETED\n", encoding="utf-8")

    assert job_list.update_from_file(store_change=store_change) is True
    assert job.status == Status.COMPLETED
    assert update_path.exists() is file_kept
    assert bool(list(tmp_path.glob("updated_list_a000.txt_*"))) is not file_kept


def test_collect_update_changes_ignores_unchanged_targets(as_conf, mocker):
    """Lines targeting the job's current status are ignored, producing no change."""
    job_list = JobList("a000", as_conf, YAMLParserFactory(), disable_save=True)
    update_path = mocker.MagicMock()
    update_path.name = "updated_list_a000.txt"
    job_list._update_file_path = update_path
    stored = Job("DUP", 5, Status.COMPLETED, 0)
    mocker.patch.object(job_list.dbmanager, "load_job_by_name", return_value=stored.__getstate__())

    pairs, unloaded_pairs = job_list._collect_update_changes(["DUP COMPLETED"])

    assert pairs == []
    assert unloaded_pairs == []


def test_collect_update_changes_duplicate_lines_last_wins(as_conf, mocker):
    """When a job appears more than once only the last line is applied."""
    job_list = JobList("a000", as_conf, YAMLParserFactory(), disable_save=True)
    update_path = mocker.MagicMock()
    update_path.name = "updated_list_a000.txt"
    job_list._update_file_path = update_path
    stored = Job("DUP", 5, Status.WAITING, 0)
    mocker.patch.object(job_list.dbmanager, "load_job_by_name", return_value=stored.__getstate__())

    pairs, unloaded_pairs = job_list._collect_update_changes(
        ["DUP COMPLETED", "dup WAITING", "DUP FAILED"]
    )

    assert len(pairs) == 1
    assert len(unloaded_pairs) == 1
    assert pairs[0][0] is not None
    assert pairs[0][1] == Status.FAILED
