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

"""Tests for ``autosubmit stats`` and related commands."""

from os import chdir, utime
from pathlib import Path
from subprocess import check_output

import pytest

from autosubmit.scripts.autosubmit import main
from log.log import AutosubmitCritical

_EXPID = 't000'


def test_clean_plots(autosubmit_exp, mocker):
    """Test cleaning plots in an Autosubmit experiment.

    The test case contains five files, four plots, and one statistics files.

    Cleaning plots without specifying the ``--stats`` results in only plot files
    deleted (anything that contains "statistics" in the name is kept).

    So, the command will keep the statistics file, as well as the two newest
    plots. Everything else will be deleted.
    """
    exp = autosubmit_exp(_EXPID, experiment_data={})

    plots_dir = Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR, f'{_EXPID}/plot/')

    for i, plot in enumerate([
        'plot_a.pdf', 'plot_b.pdf', 'plot_c.pdf', 'plot_d.pdf', f'{_EXPID}_statistics_1.pdf'
    ]):
        p = Path(plots_dir / plot)
        p.touch()
        utime(str(p), (i, i))

    mocker.patch('sys.argv', ['autosubmit', 'clean', _EXPID, '--plot'])
    main()

    plots = list(plots_dir.iterdir())

    # keeps statistics file, and two newest plot files
    assert len(plots) == 3
    assert Path(plots_dir / 'plot_c.pdf').exists()
    assert Path(plots_dir / 'plot_d.pdf').exists()


def test_clean_stats(autosubmit_exp, mocker):
    """Test cleaning statistics files in an Autosubmit experiment.

    The test case contains five files, four plots, and one statistics files.

    Cleaning statistics files without specifying the ``-plots`` results
    in only statistics files deleted (anything that contains "statistics"
    in the name is kept).

    So, the command will keep the plot file, as well as the two newest
    statistics files. Everything else will be deleted.
    """
    exp = autosubmit_exp(_EXPID, experiment_data={})

    plots_dir = Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR, f'{_EXPID}/plot/')

    for i, plot in enumerate([
        'plot_a.pdf', 'plot_b.pdf', 'plot_c.pdf', 'plot_d.pdf', f'{_EXPID}_statistics_1.pdf',
        f'{_EXPID}_statistics_2.pdf', f'{_EXPID}_statistics_3.pdf'
    ]):
        p = Path(plots_dir / plot)
        p.touch()
        utime(str(p), (i, i))

    mocker.patch('sys.argv', ['autosubmit', 'clean', _EXPID, '--stats'])
    main()

    plots = list(plots_dir.iterdir())

    # keeps plot files, and two newest statistics files
    assert len(plots) == 6
    assert Path(plots_dir / f'{_EXPID}_statistics_2.pdf').exists()
    assert Path(plots_dir / f'{_EXPID}_statistics_3.pdf').exists()


def test_clean_dummy_project(autosubmit_exp, mocker):
    """Test cleaning an Autosubmit experiment dummy project.

    Autosubmit only deletes Git projects, so it must simply log and exit,
    without touching plot or statistics files.
    """
    mocked_log = mocker.patch('autosubmit.autosubmit.Log')

    exp = autosubmit_exp(_EXPID, experiment_data={
        'PROJECT': {
            'PROJECT_TYPE': 'none'
        }
    })

    Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR, _EXPID, ).mkdir(exist_ok=True)

    plots_dir = Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR, f'{_EXPID}/plot/')

    for i, plot in enumerate([
        'plot_a.pdf', 'plot_b.pdf', 'plot_c.pdf', 'plot_d.pdf', f'{_EXPID}_statistics_1.pdf',
        f'{_EXPID}_statistics_2.pdf', f'{_EXPID}_statistics_3.pdf'
    ]):
        p = Path(plots_dir / plot)
        p.touch()
        utime(str(p), (i, i))

    mocker.patch('sys.argv', ['autosubmit', 'clean', _EXPID, '--project'])
    main()

    plots = list(plots_dir.iterdir())

    # keeps plots and statistics files
    assert len(plots) == 7

    assert mocked_log.info.called
    assert 'No project to clean' in mocked_log.info.call_args_list[-1].args[0]


@pytest.mark.parametrize(
    'clean_git_return,main_exit_code,expected_result,expected_info',
    [
        (True, 0, 'Git project cleaned', None),
        (False, 1, None, 'Cleaning GIT directory'),
    ],
    ids=[
        'Best scenario',
        'clean_git fails, so no result logged'
    ]
)
def test_clean_git_project(
        clean_git_return: bool,
        main_exit_code: int,
        expected_result: str,
        expected_info: str,
        autosubmit_exp,
        mocker,
        tmp_path):
    """Test cleaning an Autosubmit experiment Git project.

    Autosubmit only deletes Git projects, so it must simply log and exit,
    without touching plot or statistics files.
    """
    mocked_log = mocker.patch('autosubmit.autosubmit.Log')

    # TODO: Bug in AutosubmitConfigParser, can be deleted once it's fixed,
    #       https://github.com/BSC-ES/autosubmit-config-parser/issues/87.
    mocked_autosubmit_config = mocker.patch('autosubmit.autosubmit.AutosubmitConfig.set_git_project_commit')
    mocked_autosubmit_config.return_value = True

    mocked_autosubmit_git = mocker.patch('autosubmit.autosubmit.AutosubmitGit')
    mocked_autosubmit_git_obj = mocker.MagicMock()
    mocked_autosubmit_git.return_value = mocked_autosubmit_git_obj
    mocked_autosubmit_git_obj.clean_git.return_value = clean_git_return

    git_project = Path(tmp_path / 'tmp_git_project')
    git_project.mkdir()
    Path(git_project, 'test.sh').touch()

    chdir(git_project)
    check_output(['git', 'init'])
    check_output(['git', 'checkout', '-b', 'test'])
    check_output(['git', 'add', '.'])
    check_output(['git', 'commit', '-m', 'Initial commit'])

    exp = autosubmit_exp(_EXPID, experiment_data={
        'PROJECT': {
            'PROJECT_TYPE': 'git',
            'PROJECT_DESTINATION': 'git_project'
        },
        'GIT': {
            'PROJECT_ORIGIN': str(git_project),
            'PROJECT_BRANCH': 'test'
        }
    })

    Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR, _EXPID, ).mkdir(exist_ok=True)

    plots_dir = Path(exp.as_conf.basic_config.LOCAL_ROOT_DIR, f'{_EXPID}/plot/')

    for i, plot in enumerate([
        'plot_a.pdf', 'plot_b.pdf', 'plot_c.pdf', 'plot_d.pdf', f'{_EXPID}_statistics_1.pdf',
        f'{_EXPID}_statistics_2.pdf', f'{_EXPID}_statistics_3.pdf'
    ]):
        p = Path(plots_dir / plot)
        p.touch()
        utime(str(p), (i, i))

    mocker.patch('sys.argv', ['autosubmit', 'clean', _EXPID, '--project'])
    assert main_exit_code == main()

    plots = list(plots_dir.iterdir())

    # keeps plots and statistics files
    assert len(plots) == 7

    assert expected_result or expected_info

    if expected_result:
        assert mocked_log.result.called
        assert expected_result in mocked_log.result.call_args_list[-1].args[0]
    if expected_info:
        assert mocked_log.info.called
        assert expected_info in mocked_log.info.call_args_list[-1].args[0]


def test_clean_git_project_error(autosubmit_exp, mocker):
    """Test cleaning an Autosubmit experiment Git project when an error occurs."""
    autosubmit_exp(_EXPID, experiment_data={})

    mocked_autosubmit_config = mocker.patch('autosubmit.autosubmit.AutosubmitConfig.get_project_type')
    mocked_autosubmit_config.side_effect = AutosubmitCritical

    mocker.patch('sys.argv', ['autosubmit', 'clean', _EXPID, '--project'])
    assert 0 != main()
