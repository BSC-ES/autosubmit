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

def test_load_parameters(autosubmit_config):
    as_conf = autosubmit_config(
        expid='a000',
        experiment_data={'VAR': {"DEEP_VAR": ["%NOTFOUND%", "%TEST%", "%TEST2%"]}})
    as_conf.experiment_data.update({'d': '%d%', 'd_': '%d_%', 'Y': '%Y%', 'Y_': '%Y_%',
                                    'M': '%M%', 'M_': '%M_%', 'm': '%m%', 'm_': '%m_%'})
    parameters = as_conf.load_parameters()
    assert parameters['VAR.DEEP_VAR'] == ['%NOTFOUND%', '%TEST%', '%TEST2%']
