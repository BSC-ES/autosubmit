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

"""Tests for ``autosubmit.monitor`` package."""

from datetime import datetime
from os import utime
from pathlib import Path
from subprocess import CalledProcessError, SubprocessError
from time import time
from typing import Any, Optional, Tuple

import pytest
from _pytest._py.path import LocalPath

from autosubmit.job.job import Job
from autosubmit.job.job_common import Status
from autosubmit.job.job_grouping import JobGrouping
from autosubmit.job.job_list import JobList
from autosubmit.monitor.monitor import (
    _check_final_status, _check_node_exists, _color_status, _create_node, _display_file,
    _display_file_xdg, clean_plot, clean_stats, Monitor
)
from autosubmitconfigparser.config.yamlparser import YAMLParserFactory
from log.log import AutosubmitCritical

_EXPID = 't000'


def test_display_files_fallback(mocker):
    """Test that verifies if ``xdg-open`` fails the code will call ``mimeopen``."""
    mocked_check_output = mocker.patch('autosubmit.monitor.monitor.subprocess.check_output')
    mocked_check_output.side_effect = CalledProcessError(0, 'cmd')

    with pytest.raises(CalledProcessError):
        _display_file_xdg('botifarra')

    assert mocked_check_output.call_count == 2
    assert mocked_check_output.call_args_list == [
        mocker.call(['xdg-open', 'botifarra']),
        mocker.call(['mimeopen', 'botifarra'])
    ]


def test_display_file_linux(mocker):
    """Test that verifies if ``xdg-open`` fails the code will call ``mimeopen``."""
    mocked_check_output = mocker.patch('autosubmit.monitor.monitor.subprocess.check_output')
    mocked_check_output.side_effect = CalledProcessError(0, 'cmd')

    mocker.patch('autosubmit.monitor.monitor.platform', 'linux')

    with pytest.raises(CalledProcessError):
        _display_file('botifarra')

    assert mocked_check_output.call_count == 2
    assert mocked_check_output.call_args_list == [
        mocker.call(['xdg-open', 'botifarra']),
        mocker.call(['mimeopen', 'botifarra'])
    ]


def test_display_file_macos(mocker):
    """Test that verifies if ``xdg-open`` fails the code will call ``mimeopen``."""
    mocked_check_output = mocker.patch('autosubmit.monitor.monitor.subprocess.check_output')
    mocked_check_output.side_effect = CalledProcessError(0, 'cmd')

    mocker.patch('autosubmit.monitor.monitor.platform', 'darwin')

    with pytest.raises(CalledProcessError):
        _display_file('botifarra')

    assert mocked_check_output.call_count == 3
    assert mocked_check_output.call_args_list == [
        mocker.call(['open', 'botifarra']),
        mocker.call(['xdg-open', 'botifarra']),
        mocker.call(['mimeopen', 'botifarra'])
    ]


def test_color_status():
    """Test that the colors are returned correctly for a given status."""
    assert _color_status(Status.WAITING) == 'gray'
    assert _color_status(1984) == _color_status(Status.UNKNOWN)


@pytest.mark.parametrize(
    'job_name,groups,hide_groups,expected_skipped',
    [
        ('', None, True, False),
        ('jordi', {'jobs': {'eulalia': [], 'pau': []}}, True, False),
        ('jordi', {'jobs': {'jordi': ['group']}}, False, False),
        ('jordi', {'jobs': {'jordi': ['group']}}, True, True),
        ('jordi', {'jobs': {'jordi': ['group', 'one more']}}, False, True)
    ],
    ids=[
        'no groups',
        'not in group',
        'within group, single job group element, not hiding',
        'within group, single job group element, but hiding',
        'within group, multiple job group elements, not hiding'
    ]
)
def test_check_node_exists(
        job_name: str,
        groups: dict[str, Any],
        hide_groups: bool,
        expected_skipped: bool,
        mocker):
    node = mocker.MagicMock()
    exp = mocker.MagicMock()
    exp.get_node.return_value = node

    job = mocker.MagicMock()
    job.name = job_name

    r = _check_node_exists(exp, job, groups, hide_groups)

    assert r == (node, expected_skipped)


@pytest.mark.parametrize(
    'job_name,groups,hide_groups,expected_group',
    [
        ('roses', None, True, 'roses'),
        ('roses', {'jobs': {'violets': []}}, True, 'roses'),
        ('roses', {'jobs': {'roses': []}}, True, None),
        ('roses', {'jobs': {'roses': ['group_one', 'group_two']}}, False, None),
        ('roses', {'jobs': {'roses': ['group_one']}, 'status': {'group_one': Status.SUSPENDED}}, False, 'group_one'),
        ('roses', {'jobs': {'roses': ['"group_one"']}, 'status': {'"group_one"': Status.SUSPENDED}}, False, 'group_one')
    ],
    ids=[
        'no groups',
        'not in the group',
        'within group, but hiding',
        'within group, not hiding, but more than one element',
        'within group, not hiding, single element, no quotes',
        'within group, not hiding, single element, quotes will be replaced'
    ]
)
def test_create_node(job_name: str, groups: dict[str, Any], hide_groups: bool, expected_group: Optional[str], mocker):
    job = mocker.MagicMock()
    job.name = job_name

    node = _create_node(job, groups, hide_groups)

    if expected_group is None:
        assert node is None
    else:
        assert node.get_name() == expected_group


@pytest.mark.parametrize(
    'job_name,edge_info,expected',
    [
        ('', {}, (None, None)),
        ('needle', {'WAITING': {'not needle': []}}, (None, None)),
        ('needle', {'WAITING': {'needle': [0, 0]}}, ('gray', None)),
        ('needle', {'WAITING': {'needle': [0, 'great good']}}, ('gray', 'great good'))
    ],
    ids=[
        'no child edge info',
        'job name not found in edge info',
        'job name found, but label is zero',
        'job name found, label is not zero'
    ]
)
def test_check_final_status(job_name: str, edge_info: dict[str, Any], expected: Tuple[Any, Any], mocker):
    job = mocker.MagicMock()
    job.name = job_name

    child = mocker.MagicMock()
    child.edge_info = edge_info

    t = _check_final_status(job, child)

    assert t == expected


def test_clean_plot(mocker, tmp_path: LocalPath):
    """Verify that ``clean_plot`` deletes the plot files.

     It must leave the latest 2 plots, and the statistics files untouched."""
    expid = 't000'
    search_dir = Path(tmp_path / expid / 'plot')
    search_dir.mkdir(parents=True, exist_ok=True)

    for i, stats_file in enumerate([
        'plot_a.pdf',
        f'{expid}_statistics_19900101.csv',
        'plot_b.pdf',
        f'{expid}_statistics_19900102.csv',
        'plot_c.pdf',
        f'{expid}_statistics_19900103.csv',
    ]):
        Path(search_dir / stats_file).touch()
        # Here we set the atime, mtime tuple to the enumeration index, so that
        # the generated files do not have the same milliseconds, making this test
        # deterministic.
        utime(Path(search_dir / stats_file), (i, i))

    assert len(list(Path(search_dir).iterdir())) == 6

    mocker.patch('autosubmit.monitor.monitor.BasicConfig.LOCAL_ROOT_DIR', str(tmp_path))

    clean_plot(expid)

    assert len(list(Path(search_dir).iterdir())) == 5
    assert Path(search_dir / f'{expid}_statistics_19900101.csv').exists()
    assert Path(search_dir / f'{expid}_statistics_19900102.csv').exists()
    # plot_a.pdf had the earliest atime, mtime, so it was deleted and only the two newest kept.
    assert Path(search_dir / 'plot_b.pdf').exists()
    assert Path(search_dir / 'plot_c.pdf').exists()


def test_clean_stats(mocker, tmp_path: LocalPath):
    """Verify that ``clean_plot`` deletes the *statistics* plot files.

     It must leave the latest 2 statistics files, and the plot files untouched."""
    expid = 't000'
    search_dir = Path(tmp_path / expid / 'plot')
    search_dir.mkdir(parents=True, exist_ok=True)

    for i, stats_file in enumerate([
        'plot_a.pdf',
        f'{expid}_statistics_19900101.csv',
        'plot_b.pdf',
        f'{expid}_statistics_19900102.csv',
        'plot_c.pdf',
        f'{expid}_statistics_19900103.csv',
    ]):
        Path(search_dir / stats_file).touch()
        # Here we set the atime, mtime tuple to the enumeration index, so that
        # the generated files do not have the same milliseconds, making this test
        # deterministic.
        utime(Path(search_dir / stats_file), (i, i))

    assert len(list(Path(search_dir).iterdir())) == 6

    mocker.patch('autosubmit.monitor.monitor.BasicConfig.LOCAL_ROOT_DIR', str(tmp_path))

    clean_stats(expid)

    assert len(list(Path(search_dir).iterdir())) == 5
    # <EXPID>_statistics_19900101.csv had the earliest atime, mtime, so it was deleted and only the two newest kept.
    assert Path(search_dir / f'{expid}_statistics_19900102.csv').exists()
    assert Path(search_dir / f'{expid}_statistics_19900103.csv').exists()
    assert Path(search_dir / 'plot_a.pdf').exists()
    assert Path(search_dir / 'plot_b.pdf').exists()
    assert Path(search_dir / 'plot_c.pdf').exists()


@pytest.mark.parametrize(
    'jobs',
    [
        [],
        [
            Job(f'job{i}', f'job{i}', Status.WAITING, None, None)
            for i in range(3)
        ]
    ]
)
def test_create_tree_list(jobs):
    """Verify that we can create a tree list."""
    expid = 't000'
    packages = []
    groups = {}
    hide_groups = False

    graph = Monitor().create_tree_list(
        expid,
        jobs,
        packages,
        groups,
        hide_groups
    )

    assert len(graph.get_subgraphs()) == 2

    experiments_subgraph = graph.get_subgraph('Experiment')
    assert experiments_subgraph
    assert len(experiments_subgraph[0].get_nodes()) == len(jobs)
    assert len(experiments_subgraph[0].obj_dict['nodes']) == len(jobs)

    # The legend subgraph must have the same number of nodes as the number of statuses
    # as we add one entry per status to the legend.
    legend_subgraph = graph.get_subgraph('Legend')
    assert legend_subgraph
    assert len(legend_subgraph[0].get_nodes()) == len(Status.KEY_TO_VALUE.keys())


def test_create_tree_list_grouped_jobs():
    """Verify that we can create a tree list when using grouped jobs."""
    expid = 't000'
    packages = []
    hide_groups = False

    jobs = []
    for i in range(3):
        job = Job(f'job{i}', f'job{i}', Status.WAITING, None, None)
        job.date = datetime.strptime('20240101', '%Y%M%d')
        jobs.append(job)

    job_list = JobList(expid, None, None, None)
    job_list._job_list = jobs

    job_grouping = JobGrouping(
        group_by='date',
        jobs=jobs.copy(),  # TODO: Why does ``group_jobs`` destroy the list ``jobs``??
        job_list=job_list,
        expand_list=None
    )

    groups = job_grouping.group_jobs()

    graph = Monitor().create_tree_list(
        expid,
        jobs,
        packages,
        groups,
        hide_groups
    )

    assert len(graph.get_subgraphs()) == 2

    experiments_subgraph = graph.get_subgraph('Experiment')
    assert experiments_subgraph
    assert len(experiments_subgraph[0].get_nodes()) == len(jobs)
    # NOTE: This is the main difference from the test above,
    #       as everything will be grouped by the same date.
    assert len(experiments_subgraph[0].obj_dict['nodes']) == 1


@pytest.mark.parametrize(
    "output_format,show,display_error,error_raised",
    [
        ('png', True, None, None),
        ('pdf', True, None, None),
        ('ps', False, None, None),
        ('ps', True, CalledProcessError(1, 'test'), None),
        ('svg', True, None, None),
        ('txt', False, None, None),
        (None, False, None, AutosubmitCritical)
    ]
)
def test_generate_output(
        output_format: str,
        show: bool,
        display_error: Optional[SubprocessError],
        error_raised: Optional[BaseException],
        autosubmit_exp,
        mocker
):
    """Test that monitor generates its output in different formats."""
    mocked_log = mocker.patch('autosubmit.monitor.monitor.Log')

    exp = autosubmit_exp(_EXPID, experiment_data={})
    exp_path = Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR) / _EXPID

    job_list_persistence = exp.autosubmit._get_job_list_persistence(_EXPID, exp.as_conf)
    job_list = JobList(_EXPID, exp.as_conf, YAMLParserFactory(), job_list_persistence)
    date_list = exp.as_conf.get_date_list()
    # TODO: we can probably simplify our code, so that ``date_format`` is calculated more easily...
    date_format = ''
    if exp.as_conf.get_chunk_size_unit() == 'hour':
        date_format = 'H'
    for date in date_list:
        if date.hour > 1:
            date_format = 'H'
        if date.minute > 1:
            date_format = 'M'
    wrapper_jobs = {}
    job_list.generate(
        exp.as_conf,
        date_list,
        exp.as_conf.get_member_list(),
        exp.as_conf.get_num_chunks(),
        exp.as_conf.get_chunk_ini(),
        exp.as_conf.load_parameters(),
        date_format,
        exp.as_conf.get_retrials(),
        exp.as_conf.get_default_job_type(),
        wrapper_jobs,
        run_only_members=exp.as_conf.get_member_list(run_only=True),
        force=True,
        create=True)

    monitor = Monitor()
    if error_raised:
        with pytest.raises(error_raised):
            monitor.generate_output(
                expid=_EXPID,
                joblist=job_list.get_job_list(),
                path=exp_path / f'tmp/LOG_{_EXPID}',
                output_format=output_format,
                show=show,
                groups=None,
                job_list_object=job_list
            )
    else:
        mock_display_file = mocker.patch('autosubmit.monitor.monitor._display_file')
        if display_error:
            mock_display_file.side_effect = display_error

        monitor.generate_output(
            expid=_EXPID,
            joblist=job_list.get_job_list(),
            path=exp_path / f'tmp/LOG_{_EXPID}',
            output_format=output_format,
            show=show,
            groups=None,
            job_list_object=job_list
        )

        assert mock_display_file.called == show
        if display_error:
            assert mocked_log.printlog.call_count > 0
            logged_message = mocked_log.printlog.call_args_list[-1].args[0]
            assert 'could not be opened' in logged_message

        if output_format == 'txt':
            plots_dir = Path(exp_path, 'status')
        else:
            plots_dir = Path(exp_path, 'plot')
        plots = list(plots_dir.iterdir())

        assert len(plots) == 1
        assert plots[0].name.endswith(output_format)

        # TODO: txt is creating an empty file, whereas the other formats create
        #       something that tells the user what are the jobs in the workflow.
        #       So txt format gives less information to the user, thus the 0 size.
        if output_format != 'txt':
            assert plots[0].stat().st_size > 0


@pytest.mark.parametrize(
    'error_msg',
    [
        'Unexpected',
        'Something something, GraphViz, something else'
    ]
)
def test_generate_output_unexpected_error(error_msg: str, mocker):
    """Test to verify how monitor behaves upon unexpected exceptions."""
    mocked_log = mocker.patch('autosubmit.monitor.monitor.Log')

    monitor = Monitor()
    mocked_create_tree_list = mocker.patch.object(monitor, 'create_tree_list', autospec=True)
    mocked_create_tree_list.side_effect = BaseException(error_msg)
    monitor.generate_output(_EXPID, None, None, None, None, False, None, None, None)

    assert mocked_create_tree_list.call_count == 1

    assert mocked_log.printlog.call_count > 0
    logged_message = mocked_log.printlog.call_args_list[-1].args[0]
    assert 'Specified output does not have an available viewer installed' in logged_message


@pytest.mark.parametrize(
    'jobs,classictxt,status_dir_exists,has_joblist,expected_text,expected_lines_count',
    [
        ([], True, False, False, '', 1),
        ([Job('dummy', 1, Status.WAITING)], True, True, False, 'dummy WAITING', 2),
        ([Job('dummy', 1, Status.FAILED)], True, True, False, 'dummy FAILED', 2),
        ([Job('dummy', 1, Status.WAITING)], False, True, False, "Writing jobs, they're grouped by", 3),
        (
                [Job('dummy', 1, Status.COMPLETED), Job('goofy', 1, Status.RUNNING)],
                False, True, True, "String representation of Job List", 3
        )
    ],
    ids=[
        'no jobs, nothing written',
        'jobs, classic, no jobs completed nor finished, one line written',
        'jobs, classic, one job finished, one line written',
        'no job list, not classic, two lines one being a log',
        'job list, not classic, two lines one being a note about a Job List used',
    ]
)
def test_generate_output_txt(jobs: list[Job], classictxt: bool, status_dir_exists: bool, has_joblist: bool,
                             expected_text: str, expected_lines_count: int, tmp_path, autosubmit_config, mocker):
    time_str = 20250429_1200
    mocker.patch('autosubmit.monitor.monitor.time.strftime', return_value=time_str)

    as_conf = autosubmit_config(_EXPID, experiment_data={})
    status_path = Path(as_conf.basic_config.LOCAL_ROOT_DIR, _EXPID, 'status')
    if status_dir_exists:
        status_path.mkdir(parents=True)
    else:
        status_path.unlink(missing_ok=True)

    status_file = status_path / f'{_EXPID}_{time_str}.txt'

    job_list_object = None
    if has_joblist:
        job_list_object = JobList(_EXPID, as_conf, YAMLParserFactory(), None)

    monitor = Monitor()
    monitor.generate_output_txt(_EXPID, joblist=jobs, path=str(tmp_path), classictxt=classictxt,
                                job_list_object=job_list_object)

    assert status_file.exists()

    status_file_content = status_file.read_text()
    assert len(status_file_content.split('\n')) == expected_lines_count
    assert expected_text in status_file_content


def test_generate_output_txt_job_with_children(tmp_path, autosubmit_config, mocker):
    """Test that writing job recursively works."""
    time_str = 20250429_1200
    mocker.patch('autosubmit.monitor.monitor.time.strftime', return_value=time_str)

    as_conf = autosubmit_config(_EXPID, experiment_data={})
    status_path = Path(as_conf.basic_config.LOCAL_ROOT_DIR, _EXPID, 'status')
    status_file = status_path / f'{_EXPID}_{time_str}.txt'

    parent_job = Job('parent', 1, Status.RUNNING)
    child_1 = Job('child_1', 2, Status.SUBMITTED)
    child_2 = Job('child_2', 3, Status.FAILED)

    parent_job.children.add(child_1)
    parent_job.children.add(child_2)
    child_1.parents.add(parent_job)
    child_2.parents.add(parent_job)

    jobs = [parent_job, child_1, child_2]

    monitor = Monitor()
    monitor.generate_output_txt(_EXPID, joblist=jobs, path=str(tmp_path), classictxt=False,job_list_object=None)

    assert status_file.exists()

    status_file_content = status_file.read_text()
    assert len(status_file_content.split('\n')) == 5

    assert 'parent RUNNING' in status_file_content
    assert 'child_1 SUBMITTED' in status_file_content
    assert 'child_2 FAILED' in status_file_content


@pytest.mark.parametrize(
    'job_statuses,hide,expected',
    [
        ([], False, False),
        ([], True, False),
        ([Status.COMPLETED, Status.COMPLETED, Status.COMPLETED, Status.FAILED], True, False),
        ([Status.FAILED], False, True)
    ],
    ids=[
        'no jobs, no report',
        'no jobs, hiding, no report',
        'jobs, but hiding, no report',
        'jobs, not hiding, output produced'
    ]
)
def test_generate_output_stats(job_statuses: list[int], hide: bool, expected: bool, autosubmit_config, mocker):
    """Test writing the output of stats."""
    # Ignore ``_display_file`` as it would try to open a file when running the tests (normally headless).
    mocked_display_file = mocker.patch('autosubmit.monitor.monitor._display_file')

    conf_jobs = []  # for YAML
    for i in range(len(job_statuses)):
        job_name = f'job_{i}'
        conf_jobs.append({
            job_name: {
                'SCRIPT': 'echo "ok"',
                'RUNNING': 'once'
            }
        })

    as_conf = autosubmit_config(_EXPID, experiment_data={'JOBS': conf_jobs})

    # We have to call this after ``autosubmit_config`` so ``BasicConfig`` values are mocked!
    jobs = []  # to be used in this test later
    for i, job_status in enumerate(job_statuses):
        job_name = f'{_EXPID}_job_{i}'
        job = Job(job_name, i, Status.VALUE_TO_KEY[job_status])
        # Without a processors we get a non-iterable error later on!
        job.processors = '1'
        jobs.append(job)

    basic_config = as_conf.basic_config
    total_stats_path = Path(as_conf.basic_config.LOCAL_ROOT_DIR, _EXPID, basic_config.LOCAL_TMP_DIR)

    date_time1 = '20240101000000'
    date_time2 = '20240101010000'
    date_time3 = '20240101020000'

    for job in jobs:
        job_total_stats = Path(total_stats_path, f'{job.name}_TOTAL_STATS')
        job_total_stats.write_text(f'{date_time1} {date_time2} {date_time3} '
                                   f'{Status.VALUE_TO_KEY[Status.KEY_TO_VALUE[job.status]]} \n')

    monitor = Monitor()
    assert expected == monitor.generate_output_stats(_EXPID, jobs, hide=hide, queue_time_fixes={})

    if not hide and expected:
        assert mocked_display_file.call_count == 1
