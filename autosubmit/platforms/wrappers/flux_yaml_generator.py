# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
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

from autosubmit.log.log import AutosubmitCritical, Log


class FluxYAMLGenerator:
    """
    Generate a YAML file to submit a job to a Flux instance given its parameters.

    :param parameters: Dictionary containing job parameters.
    """

    def __init__(self, parameters: dict):
        self.parameters = parameters

    # TODO: [ENGINES] Add support for heterogeneous jobs
    def generate_template(self, template: str) -> str:
        """
        Generate the Flux Jobspec YAML representation for the job.

        :param template: The script content to be included in the job.

        :return: The complete Flux Jobspec YAML.
        :rtype: str
        """
        # Extract job parameters
        log_path = self.parameters['CURRENT_LOGDIR']
        job_name = self.parameters['JOBNAME']
        job_section = self.parameters['TASKTYPE']
        expid = self.parameters['DEFAULT.EXPID']
        wallclock = self._wallclock_to_seconds(self.parameters['WALLCLOCK'])
        ntasks = int(self.parameters['PROCESSORS']) if self.parameters['PROCESSORS'] else 0
        num_nodes = int(self.parameters['NODES']) if self.parameters['NODES'] else 0
        num_cores = int(self.parameters['THREADS']) if self.parameters['THREADS'] else 0
        tasks_per_node = int(self.parameters['TASKS']) if self.parameters['TASKS'] else 0
        mem = int(self.parameters['MEMORY']) if self.parameters['MEMORY'] else 0
        mem_per_core = int(self.parameters['MEMORY_PER_TASK']) if self.parameters['MEMORY_PER_TASK'] else 0
        exclusive = self.parameters['EXCLUSIVE']
        if isinstance(exclusive, str):
            exclusive = exclusive.lower() in ('true', '1', 'yes')

        # When using vertical wrappers, output files paths will be replaced in runtime
        output_file = f"{log_path}/{job_name}.cmd.out.0"
        error_file = f"{log_path}/{job_name}.cmd.err.0"

        # Create and populate the YAML
        job_yaml = FluxYAML(job_name)
        task_count = job_yaml.add_resource(label="task", ntasks=ntasks, num_nodes=num_nodes, num_cores=num_cores,
                                           exclusive=exclusive, mem_per_node_mb=mem, mem_per_core_mb=mem_per_core,
                                           tasks_per_node=tasks_per_node)
        if task_count > 0:
            job_yaml.add_task(resource_label="task", count_total=task_count)
        else:
            job_yaml.add_task(resource_label="task", count_per_slot=1)
        job_yaml.set_attributes(duration=wallclock, cwd=log_path, job_name=job_name, output_file=output_file,
                                error_file=error_file, script_content=template)

        # Compose template
        return self._get_script_section_header(job_section, expid) + "\n" + job_yaml.generate()

    def _wallclock_to_seconds(self, wallclock: str) -> int:
        """
        Convert wallclock time in format HH:MM to total seconds.

        :param wallclock: Wallclock in HH:MM format.

        :return: Total wallclock in seconds.
        :rtype: int
        """
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
    by 'Specific Resource Graph Restrictions' and 'Tasks' sections in Flux RFC 25.

    :param job_name: Name of the job.
    """

    def __init__(self, job_name: str):
        # Jobspec attributes
        self.version = 1
        self.resources = []
        self.tasks = []
        self.attributes = {}
        self.job_name = job_name

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

    def add_resource(self, label: str = "default", ntasks: int = 1, num_nodes: int = 0, num_cores: int = 1,
                     exclusive: bool = False, mem_per_node_mb: int = 0, mem_per_core_mb: int = 0,
                     tasks_per_node: int = 0) -> int:
        """
        Adds a resource to the job specification.

        :param label: Label for the resource.
        :param ntasks: Number of slots.
        :param num_nodes: Number of nodes.
        :param num_cores: Number of cores.
        :param exclusive: Whether the node is exclusive.
        :param mem_per_node_mb: Memory per node in MB.
        :param mem_per_core_mb: Memory per core in MB.
        :param tasks_per_node: Number of tasks per node.

        :return: The total count to be assigned to the corresponding task. Else, zero.
        :rtype: int

        :raises ValueError: If any input parameter is invalid.
        :raises AutosubmitCritical: If the resource request is not accepted.
        """
        # Check input validity
        if ntasks < 0 or num_nodes < 0 or num_cores < 0 or mem_per_node_mb < 0 or mem_per_core_mb < 0 or tasks_per_node < 0:
            raise ValueError(f"Some resource request for {self.job_name} is negative.")
        if label is None or label.strip() == "":
            raise ValueError(f"Resource label for job {self.job_name} cannot be empty.")
        
        # Core count must be always set, according to Flux RFC 25
        if num_cores == 0:
            Log.warning(f"Job {self.job_name} has been asigned zero cores, which is not permitted. Defaulting to 1")
            num_cores = 1

        # Create node, slot and core resources by mapping the parameters. Node is optional
        node = None
        if num_nodes > 0 and tasks_per_node > 0:
            nslots = tasks_per_node
        elif num_nodes > 0:
            nslots = 1
        elif tasks_per_node > 0:
            nslots = tasks_per_node
            num_nodes = 0
        elif ntasks > 0:
            nslots = ntasks
        elif ntasks == 0:
            nslots = 1
        else:
            raise AutosubmitCritical(f"The current resource request for {self.job_name} is not accepted \
                                     when wrapped with the Flux method. If you consider this is a mistake, \
                                     please report your case to the Autosubmit developers")

        # Compose resources
        if num_nodes > 0:
            node = self._compose_node_resource(count=num_nodes, exclusive=exclusive,
                                               mem_per_node_mb=mem_per_node_mb)
        if nslots > 0:
            slot = self._compose_slot_resource(label=label, count=nslots)
        if num_cores > 0:
            core, memory = self._compose_core_resource(count=num_cores, mem_per_core_mb=mem_per_core_mb)

        # Build the resource hierarchy
        slot['with'].append(core)
        if memory:
            slot['with'].append(memory)
        resource = slot
        if node:
            node['with'].append(slot)
            resource = node
        self.resources.append(resource)

        # Calculate the total count that must be assigned to the corresponding task
        task_count = 0
        if ntasks > 1:
            if num_nodes > 0 or tasks_per_node > 0:
                task_count = ntasks
            else:
                task_count = 0

        return task_count

    def add_task(self, resource_label: str = "default", count_per_slot: int = 0, count_total: int = 0) -> None:
        """
        Adds a task to the job specification.

        :param resource_label: Label of the slot resource to bind the task to.
        :param count_per_slot: Number of task instances per slot.
        :param count_total: Total number of task instances.

        :return: None

        :raises ValueError: If any input parameter is invalid or if both or none of 
        count_per_slot and count_total are set.
        """
        # Check input validity
        if count_per_slot < 0 or count_total < 0:
            raise ValueError("Task count values cannot be negative")
        if resource_label is None or resource_label.strip() == "":
            raise ValueError("Resource label for task cannot be empty.")

        # Check that only one of the count parameters is set
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
            'slot': resource_label,
            'count': count
        }

        self.tasks.append(task)

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

        :raises ValueError: If any input parameter is invalid.
        """
        # Validate inputs
        if duration <= 0:
            raise ValueError("Job duration must be greater than zero")
        if cwd is None or cwd.strip() == "":
            raise ValueError("Current working directory cannot be empty")
        if job_name is None or job_name.strip() == "":
            raise ValueError("Job name cannot be empty")
        if output_file is None or output_file.strip() == "":
            raise ValueError("Output file path cannot be empty")
        if error_file is None or error_file.strip() == "":
            raise ValueError("Error file path cannot be empty")
        if script_content is None or script_content.strip() == "":
            raise ValueError("Script content cannot be empty")
        
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

    def _compose_node_resource(self, count: int = 0, exclusive: bool = False, mem_per_node_mb: int = 0) -> dict:
        """
        Composes a node resource dictionary.

        :param count: Number of nodes.
        :param exclusive: Whether the node is exclusive.
        :param mem_per_node_mb: Memory per node in MB.

        :return: Node resource dictionary.
        :rtype: dict

        :raises ValueError: If the count value is not greater than zero.
        """
        if count <= 0:
            raise ValueError("Node count must be greater than zero to compose a node resource")

        node = {
            'type': 'node',
            'count': count,
            'exclusive': exclusive,
            'with': []
        }
        if mem_per_node_mb > 0:
            node['with'].append({
                'type': 'memory',
                'count': mem_per_node_mb,
                'unit': 'MB'
            })
        return node

    def _compose_slot_resource(self, label: str = "default", count: int = 0) -> dict:
        """
        Composes a slot resource dictionary.

        :param label: Label for the slot.
        :param count: Number of slots.

        :return: Slot resource dictionary.
        :rtype: dict

        :raises ValueError: If any input parameter is invalid.
        """
        if count <= 0:
            raise ValueError("Slot count must be greater than zero to compose a slot resource")
        if label is None or label.strip() == "":
            raise ValueError("Slot label cannot be empty.")

        slot = {
            'type': 'slot',
            'label': label,
            'count': count,
            'with': []
        }
        return slot

    def _compose_core_resource(self, count: int = 0, mem_per_core_mb: int = 0) -> dict:
        """
        Composes a core resource dictionary.

        :param count: Number of cores.
        :param mem_per_core_mb: Memory per core in MB.

        :return: Core resource dictionary.
        :rtype: dict

        :raises ValueError: If any input parameter is invalid.
        """
        if count <= 0:
            raise ValueError("Core count must be greater than zero to compose a core resource")
        if mem_per_core_mb < 0:
            raise ValueError("Memory per core cannot be negative.")

        core = {
            'type': 'core',
            'count': count
        }
        memory = None
        if mem_per_core_mb > 0:
            memory = {
                'type': 'memory',
                'count': mem_per_core_mb,
                'unit': 'MB'
            }
        return core, memory
