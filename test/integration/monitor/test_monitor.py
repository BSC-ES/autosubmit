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

from pathlib import Path
from subprocess import CalledProcessError, SubprocessError
from typing import Optional

import pytest

from autosubmit.config.yamlparser import YAMLParserFactory
from autosubmit.job.job_list import JobList
from autosubmit.job.job_list_persistence import get_job_list_persistence
from autosubmit.log.log import AutosubmitCritical
from autosubmit.monitor.monitor import (
    Monitor
)

# noinspection PyProtectedMember

_EXPID = 't000'


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

    job_list_persistence = get_job_list_persistence(_EXPID, exp.as_conf)
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
                path=str(exp_path / f'tmp/LOG_{_EXPID}'),
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
            path=str(exp_path / f'tmp/LOG_{_EXPID}'),
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
