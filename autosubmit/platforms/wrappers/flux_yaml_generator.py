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

from ruamel.yaml import YAML
from ruamel.yaml.scalarstring import PreservedScalarString
from io import StringIO

class FluxYAML:
    def __init__(self):
        # Jobspec attributes
        self.version = 1
        self.resources = []
        self.tasks = []
        self.attributes = {}

    # TODO: [ENGINES] Add support for heterogeneous jobs
    def add_slot(self, label='task', nslots=1, num_nodes=0, num_cores=0, exclusive=False, mem_per_node_gb=0, mem_per_core_gb=0):
        if num_nodes == 0 and num_cores == 0:
            raise ValueError("No resources to add")
        if num_nodes != 0 and exclusive:
            raise ValueError("Exclusive flag can only be set for 'node' resources")
        
        resource = {
            'type': 'slot',
            'label': label,
            'count': nslots,
            'with': [],
        }
        
        if num_nodes > 0:
            node = {
                'type': 'node',
                'count': num_nodes,
                'exclusive': exclusive,
            }
            if mem_per_node_gb > 0:
                node['with'].append({
                    'type': 'memory',
                    'count': mem_per_node_gb,
                    'unit': 'GB'
                })

        if num_cores > 0:
            core = {
                'type': 'core',
                'count': num_cores,
            }
            if mem_per_core_gb > 0:
                core['with'].append({
                    'type': 'memory',
                    'count': mem_per_core_gb,
                    'unit': 'GB'
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
    
    def add_task(self, slot_label='task', count_per_slot=0, count_per_node=0):
        if count_per_slot > 0 and count_per_node > 0:
            raise ValueError("Cannot set both count_per_slot and count_per_node")
        elif count_per_slot == 0 and count_per_node == 0:
            raise ValueError("Either count_per_slot or count_per_node must be specified")
        
        if count_per_slot > 0:
            count = {
                'per_slot': count_per_slot,
            }
        elif count_per_node > 0:
            count = {
                'per_node': count_per_node,
            }

        task = {
            'command': ["{{tmpdir}}/script"],
            'slot': slot_label,
            'count': count
        }

        self.tasks.append(task)
        return len(self.tasks) - 1
    
    def set_attributes(self, duration, cwd, job_name, output_file, error_file, script_content):
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

    def generate_yaml(self):
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
