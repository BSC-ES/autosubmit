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

from pathlib import Path
from typing import Callable

import pytest
from pytest_mock import MockerFixture

from autosubmit.autosubmit import Autosubmit

_EXPIDS = ['z000', 'z001']


@pytest.mark.parametrize(
    'input_experiment_list,get_from_user,not_described',
    [
        (' '.join(_EXPIDS), '', False),  # It accepts expids separated by spaces,
        (','.join(_EXPIDS), '', False),  # or by commas,
        (_EXPIDS[0], '', False),  # or a single experiment ID.
        ('zzzz', '', True),  # An expid that does not exist.
        ('', '', True),  # If nothing is provided.
    ]
)
def test_describe(
        input_experiment_list,
        get_from_user,
        not_described,
        autosubmit_exp: Callable,
        create_as_conf: Callable,
        mocker: MockerFixture) -> None:
    Log = mocker.patch('autosubmit.autosubmit.Log')

    exp = None
    expids = input_experiment_list.replace(',', ' ').split(' ')
    for expid in expids:
        if expid not in _EXPIDS:
            continue
        exp = autosubmit_exp(expid)

        basic_config = mocker.MagicMock()
        # TODO: Whenever autosubmitconfigparser gets released with BasicConfig.expid_dir() and similar functions, the following line and a lot of mocks need to be removed. This line is especially delicate because it "overmocks" the basic_config mock, thus making the last assertion of this file "assert f'Location: {exp.exp_path}' in log_result_output" a dummy assertion. The rest of the test is still useful.
        basic_config.expid_dir.side_effect = lambda expid: exp.exp_path
        config_values = {
            'LOCAL_ROOT_DIR': str(exp.exp_path.parent),
            'LOCAL_ASLOG_DIR': str(exp.aslogs_dir)
        }

        for key, value in config_values.items():
            basic_config.__setattr__(key, value)

        basic_config.get.side_effect = lambda key, default='': config_values.get(key, default)
        for basic_config_location in [
            'autosubmit.autosubmit.BasicConfig',
            'autosubmitconfigparser.config.configcommon.BasicConfig'
        ]:
            # TODO: Better design how ``BasicConfig`` is used/injected/IOC/etc..
            mocker.patch(basic_config_location, basic_config)

        mocked_get_submitter = mocker.patch.object(Autosubmit, '_get_submitter')
        submitter = mocker.MagicMock()

        mocked_get_submitter.return_value = submitter
        submitter.platforms = [1, 2]

        get_experiment_descrip = mocker.patch('autosubmit.autosubmit.get_experiment_descrip')
        get_experiment_descrip.return_value = [[f'{expid} description']]

        create_as_conf(
            autosubmit_exp=exp,
            yaml_files=[
                Path(__file__).resolve().parent / "files/fake-jobs.yml",
                Path(__file__).resolve().parent / "files/fake-platforms.yml"
            ],
            experiment_data={
                'DEFAULT': {
                    'HPCARCH': 'ARM'
                }
            }
        )

    Autosubmit.describe(
        input_experiment_list=input_experiment_list,
        get_from_user=get_from_user
    )

    # Log.printlog is only called when an experiment is not described
    # TODO: We could re-design the class to make this behaviour clearer.
    assert Log.printlog.call_count == (1 if not_described else 0)

    if exp and not not_described:
        log_result_output = [
            line_tuple.args[0].format(line_tuple.args[1]) for line_tuple in Log.result.mock_calls
        ]
        root_dir = exp.exp_path.parent
        for expid in expids:
            assert f'Location: {exp.exp_path}' in log_result_output
