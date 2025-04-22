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

import pytest

from autosubmit.statistics.jobs_stat import _calculate_processing_elements, _estimate_requested_nodes

"""Tests for ``autosubmit.statistics.jobs_stat``."""


@pytest.mark.parametrize(
    'nodes,processors,tasks,processors_per_node,expected',
    [
        ('10', '5', '2', '1', 10),
        ('sushi', '5', '2', '1', 3),
        ('sushi', '5', 'rice', 'algae', 1),
        ('sushi', '1', 'rice', '5', 1),
        ('sushi', '5', 'rice', '1', 5)
    ],
    ids=[
        'nodes is digit, so we get nodes',
        'nodes is not digit, but tasks is, so we get ceil of processors by tasks',
        'nodes is not digit, tasks is not digit, processors per node is not digit, so we get 1',
        'nodes is not digit, tasks is not digit, processors per node is digit but processors is less '
        'than processors per node, so we get 1',
        'nodes is not digit, tasks is not digit, processors per node is digit and processors is greater '
        'than processors per node, so we get ceil of processors by processors per node'
    ]
)
def test_estimate_requested_nodes(nodes, processors, tasks, processors_per_node, expected):
    requested_nodes = _estimate_requested_nodes(nodes, processors, tasks, processors_per_node)
    assert requested_nodes == expected


@pytest.mark.parametrize(
    'nodes,processors,tasks,processors_per_node,exclusive,log_called,expected',
    [
        ('10', '5', '2', '2', False, False, 20),
        ('', '5', '2', '2', False, False, 6),
        ('', '5', '', '2', False, False, 6),
        ('', '5', '', '10', False, False, 5),
        ('', '3', '10', '', False, True, 3),
        ('1', '3', '', '', False, True, 3),
        ('', '10', '', '', False, False, 10),
    ],
    ids=[
        'processors per node is digit, nodes as well, multiply both',
        'processors per node is digit, nodes is not, exclusive, multiply estimated by processors per node',
        'processors per node is digit, nodes is not, not exclusive, estimated is one,'
        'processors greater than processors per node, multiply estimated by processors per node',
        'processors per node is digit, nodes is not, not exclusive, estimated is one',
        'processors per node is not digit, and tasks is digit, return processors',
        'processors per node is not digit, tasks is not digit, and nodes is digit, return processors',
        'processors per node is not digit, tasks is not digit, nodes is not digit, return processors'
    ]
)
def test_calculate_processing_elements(nodes, processors, tasks, processors_per_node, exclusive,
                                       log_called, expected, mocker):
    log_warning = mocker.patch('autosubmit.statistics.jobs_stat.Log.warning')
    processing_elements = _calculate_processing_elements(nodes, processors, tasks, processors_per_node, exclusive)
    assert processing_elements == expected
    assert log_warning.called == log_called
