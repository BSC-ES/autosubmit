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

"""Integration tests for detail updater."""

from autosubmit.experiment.detail_updater import ExperimentDetails


def test_details_properties(autosubmit_exp, mocker):
    mocker.patch('autosubmit.experiment.detail_updater.ExperimentDetailsRepository')
    exp = autosubmit_exp()
    exp_details = ExperimentDetails(exp.expid, init_reload=False)

    exp_details.exp_id = 0

    mock_as_conf = mocker.MagicMock()
    mock_as_conf.get_project_type.return_value = "git"
    mock_as_conf.get_git_project_origin.return_value = "my_git_origin"
    mock_as_conf.get_git_project_branch.return_value = "my_git_branch"
    mock_as_conf.get_platform.return_value = "my_platform"

    exp_details.as_conf = mock_as_conf

    assert exp_details.hpc == "my_platform"

    assert exp_details.model == "my_git_origin"
    assert exp_details.branch == "my_git_branch"
