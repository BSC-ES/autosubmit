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

import textwrap
from ruamel.yaml import YAML
from ruamel.yaml.scalarstring import PreservedScalarString
from io import StringIO

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from autosubmit.job.job import Job

class FluxYAMLGenerator:
    """
    Generate a YAML file to submit a job to a Flux system given its specifications.
    """
    def __init__(self, job: 'Job', parameters: dict):
        self.job = job
        self.parameters = parameters

    # TODO: [ENGINES] Add support for heterogeneous jobs
    def generate_template(self, template: str) -> str:
        """
        Generate the Flux Jobspec YAML representation for the job.
        """
        job_yaml = FluxYAML()

        # Extract job parameters
        log_path = self.parameters['HPCLOGDIR']
        job_name = self.parameters['JOBNAME']
        job_section = self.parameters['TASKTYPE']
        expid = self.parameters['DEFAULT.EXPID']
        wallclock = self._wallclock_to_seconds(self.parameters['WALLCLOCK'])
        
        nslots = int(self.parameters['PROCESSORS']) if self.parameters['PROCESSORS'] else 0
        num_nodes = int(self.parameters['NODES']) if self.parameters['NODES'] else 0
        num_cores = int(self.parameters['THREADS']) if self.parameters['THREADS'] else 0
        tasks_per_node = int(self.parameters['TASKS']) if self.parameters['TASKS'] else 0
        mem = int(self.parameters['MEMORY']) if self.parameters['MEMORY'] else 0
        mem_per_core = int(self.parameters['MEMORY_PER_TASK']) if self.parameters['MEMORY_PER_TASK'] else 0
        exclusive = self.parameters['EXCLUSIVE'].lower() == 'true'

        # When using vertical wrappers, output files paths will be replaced in runtime
        output_file = f"{log_path}/{job_name}.cmd.out.0"
        error_file = f"{log_path}/{job_name}.cmd.err.0"

        # Populate the YAML
        job_yaml.add_slot(nslots=nslots, num_nodes=num_nodes, num_cores=num_cores, exclusive=exclusive, mem_per_node_gb=mem, mem_per_core_gb=mem_per_core, tasks_per_node=tasks_per_node)
        job_yaml.add_task(count_per_slot=1)

        job_yaml.set_attributes(duration=wallclock, cwd=log_path, job_name=job_name, output_file=output_file, error_file=error_file, script_content=template)

        # Compose template
        return self._get_script_section_header(job_section, expid) + "\n" + job_yaml.generate()
    
    def _wallclock_to_seconds(self, wallclock: str) -> int:
        """Convert wallclock time in format HH:MM to total seconds."""
        h, m = map(int, wallclock.split(':'))
        return h * 3600 + m * 60

    def _get_script_section_header(self, tasktype: str, expid: str) -> str:
        return textwrap.dedent(f"""\
###############################################################################
#                   {tasktype} {expid} EXPERIMENT
###############################################################################
           """)

class FluxYAML(object):
    """
    Class to generate a Flux Jobspec YAML representation following RFC 25.

    Steps to create a Jobspec YAML:
    1. Initialize a FluxYAML object.
    2. Use 'add_slot' to define resources.
    3. Use 'add_task' to define tasks.
    4. Use 'set_attributes' to define job attributes.
    5. Call 'generate' to produce the YAML string.

    Note: Jobspec Version 1 only supports a single resource and task, as defined 
    by 'Specific Resource Graph Restrictions' and 'Tasks' sections in RFC 25.
    """
    def __init__(self):
        # Jobspec attributes
        self.version = 1
        self.resources = []
        self.tasks = []
        self.attributes = {}

    def generate(self) -> str:
        """
        Generates the YAML representation of the job specification.
        It uses the previously set resources, tasks, and attributes.
        
        :return: YAML string of the job specification.
        :rtype: str

        :raises ValueError: If required sections are missing (see 
        'Jobspec Language Definition' in Flux RFC 25)
        """
        # Check that required sections are present
        if not self.resources or not self.tasks or not self.attributes:
            raise ValueError("Resources, tasks, and attributes must be defined before generating the Jobspec YAML")

        # Build the YAML
        yaml = YAML()
        yaml.default_flow_style = False
        jobspec = {
            'resources': self.resources,
            'tasks': self.tasks,
            'attributes': self.attributes,
            'version': self.version
        }
        stream = StringIO()
        yaml.dump(jobspec, stream)
        return stream.getvalue()

    # TODO: [ENGINES] URGENT: Implement correct resource parameters mapping
    def add_slot(self, label: str = 'task', nslots: int = 1, num_nodes: int = 0, num_cores: int = 0, 
                 exclusive: bool = False, mem_per_node_mb: int = 0, mem_per_core_mb: int = 0,
                 tasks_per_node: int = 0) -> int:
        """
        Adds a slot resource to the job specification.
        
        :param label: Label for the resource.
        :param nslots: Number of slots.
        :param num_nodes: Number of nodes.
        :param num_cores: Number of cores.
        :param exclusive: Whether the node is exclusive.
        :param mem_per_node_mb: Memory per node in MB.
        :param mem_per_core_mb: Memory per core in MB.
        :param tasks_per_node: Number of tasks per node.

        :return: Index of the added slot resource.
        :rtype: int

        :raises ValueError: If no resources are specified.
        """
        if num_nodes == 0 and num_cores == 0:
            raise ValueError("No resources to add")
        
        resource = {
            'type': 'slot',
            'label': label,
            'count': nslots
        }
        
        if num_nodes > 0:
            node = {
                'type': 'node',
                'count': num_nodes,
                'exclusive': exclusive,
            }
            if mem_per_node_mb > 0:
                node['with'].append({
                    'type': 'memory',
                    'count': mem_per_node_mb,
                    'unit': 'MB'
                })

        if num_cores > 0:
            core = {
                'type': 'core',
                'count': num_cores,
            }
            if mem_per_core_mb > 0:
                core['with'].append({
                    'type': 'memory',
                    'count': mem_per_core_mb,
                    'unit': 'MB'
                })

        if num_nodes > 0 and num_cores > 0:
            node['with'].append(core)
            resource['with'].append(node)
        elif num_nodes > 0:
            resource['with'].append(node)
        elif num_cores > 0:
            resource['with'].append(core)
        
        self.resources.append(resource)
        return len(self.resources) - 1
    
    def add_task(self, slot_label: str = 'task', count_per_slot: int = 0, count_total: int = 0) -> int:
        """
        Adds a task to the job specification.

        :param slot_label: Label of the slot resource to bind the task to.
        :param count_per_slot: Number of task instances per slot.
        :param count_total: Total number of task instances.

        :return: Index of the added task.
        :rtype: int
        
        :raises ValueError: If both or none of count_per_slot and count_total are set.
        """
        if count_per_slot > 0 and count_total > 0:
            raise ValueError("Cannot set both count_per_slot and count_total simultaneously")
        elif count_per_slot == 0 and count_total == 0:
            raise ValueError("Either count_per_slot or count_total must be specified")
        
        if count_per_slot > 0:
            count = {
                'per_slot': count_per_slot,
            }
        elif count_total > 0:
            count = {
                'total': count_total,
            }

        task = {
            'command': ["{{tmpdir}}/script"],
            'slot': slot_label,
            'count': count
        }

        self.tasks.append(task)
        return len(self.tasks) - 1
    
    def set_attributes(self, duration: int, cwd: str, job_name: str, output_file: str, 
                       error_file: str, script_content: str) -> None:
        """
        Sets the attributes section of the job specification.

        :param duration: Job duration in seconds.
        :param cwd: Current working directory.
        :param job_name: Name of the job.
        :param output_file: Path to the output file.
        :param error_file: Path to the error file.
        :param script_content: Content of the script.

        :return: None
        """
        self.attributes['system'] = {
            'duration': duration,
            'cwd': cwd,
            'job': {
                'name': job_name
            },
            'shell': {
                'options': {
                    'output': {
                        'stdout': {
                            'type': 'file',
                            'path': output_file
                        },
                        'stderr': {
                            'type': 'file',
                            'path': error_file
                        }
                    }
                }
            },
            'files': {
                'script': {
                    'mode': 33216,
                    'data': PreservedScalarString(script_content),
                    'encoding': 'utf-8'
                }
            }
        }
