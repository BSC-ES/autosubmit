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

import collections
import pytest
import textwrap
from math import ceil

from autosubmit.platforms.wrappers.wrapper_builder import PythonWrapperBuilder


@pytest.fixture
def job_scripts():
    return ['JOB_1', 'JOB_2', 'JOB_3']


def _create_nodelist(num_cores):
    num_nodes = int(ceil(num_cores / float(48)))

    node_list = []

    for i in range(num_nodes):
        node_list.append('node_' + str(i))
    return node_list


def test_job_less_than_48_cores_standard(job_scripts):
    num_processors = 60
    jobs_resources = {'MACHINEFILES': 'STANDARD', 'JOB': {'PROCESSORS': '20', 'TASKS': '48'},
                      'PROCESSORS_PER_NODE': '48'}

    wrapper_builder = PythonWrapperBuilder(header_directive='', jobs_scripts=job_scripts,
                                           num_processors=num_processors, expid='a000',
                                           jobs_resources=jobs_resources, threads='1', retrials=0,
                                           wallclock_by_level=None, num_processors_value=num_processors)

    nodes = _create_nodelist(num_processors)
    cores_list = wrapper_builder.build_cores_list()
    machinefiles_code = wrapper_builder.get_machinefile_function().replace("_NEWLINE_", '\\n')

    result = dict()

    script = textwrap.dedent("""
    from math import ceil
    
    all_nodes = {0}
    section = 'JOB'
    {1}
    machinefiles_dict = dict()
    for job in {2}:
    {3}
        machinefiles_dict[job] = machines
    """).format(nodes, cores_list, job_scripts, wrapper_builder._indent(machinefiles_code, 4))

    exec(script, result)

    machinefiles_dict = result["machinefiles_dict"]
    all_machines = list()
    for job, machines in machinefiles_dict.items():
        machines = machines.split("\n")[:-1]
        job_section = job.split("_")[0]
        job_cores = int(jobs_resources[job_section]['PROCESSORS'])
        assert job_cores == len(machines)
        all_machines += machines

    machines_count = collections.Counter(all_machines)
    for count in list(machines_count.values()):
        assert count <= int(jobs_resources['PROCESSORS_PER_NODE'])


def test_job_more_than_48_cores_standard(job_scripts):
    num_processors = 150
    jobs_resources = {'MACHINEFILES': 'STANDARD', 'JOB': {'PROCESSORS': '50', 'TASKS': '48'},
                      'PROCESSORS_PER_NODE': '48'}

    wrapper_builder = PythonWrapperBuilder(header_directive='', jobs_scripts=job_scripts,
                                           num_processors=num_processors, expid='a000',
                                           jobs_resources=jobs_resources, threads='1', retrials=0,
                                           wallclock_by_level=None, num_processors_value=num_processors)

    nodes = _create_nodelist(num_processors)
    cores_list = wrapper_builder.build_cores_list()
    machinefiles_code = wrapper_builder.get_machinefile_function().replace("_NEWLINE_", '\\n')

    result = dict()

    script = textwrap.dedent("""
    from math import ceil

    all_nodes = {0}
    section = 'JOB'
    {1}
    machinefiles_dict = dict()
    for job in {2}:
    {3}
        machinefiles_dict[job] = machines
    """).format(nodes, cores_list, job_scripts, wrapper_builder._indent(machinefiles_code, 4))

    exec(script, result)
    machinefiles_dict = result["machinefiles_dict"]
    for job, machines in machinefiles_dict.items():
        machines = machines.split("\n")[:-1]
        job_section = job.split("_")[0]
        job_cores = int(jobs_resources[job_section]['PROCESSORS'])
        assert len(machines) == job_cores
