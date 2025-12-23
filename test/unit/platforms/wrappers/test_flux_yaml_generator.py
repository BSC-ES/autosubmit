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

import pytest

from autosubmit.platforms.wrappers.flux_yaml_generator import FluxYAMLGenerator, FluxYAML

# Test cases for the FluxYAMLGenerator class.
def test_generate_template():
    """ Tests the generation of a Flux jobspec. """
    parameters = {
        'CURRENT_LOGDIR': '/tmp/logs',
        'JOBNAME': 'a000_20250101_fc0_1_SIM',
        'TASKTYPE': 'SIM',
        'DEFAULT.EXPID': 'a000',
        'WALLCLOCK': '00:01',
        'PROCESSORS': '10',
        'NODES': '2',
        'THREADS': '2',
        'TASKS': '5',
        'MEMORY': '0',
        'MEMORY_PER_TASK': '0',
        'EXCLUSIVE': False
    }
    generator = FluxYAMLGenerator(parameters)
    assert generator.generate_template("echo hello\n") is not None
    parameters['NODES'] = '0'
    parameters['TASKS'] = '0'
    generator = FluxYAMLGenerator(parameters)
    assert generator.generate_template("echo hello\n") is not None

# Test cases for the FluxYAML class.
@pytest.fixture
def yaml():
    """ Creates a FluxYAML object and yields it to the test. """
    return FluxYAML("a000_20250101_fc0_1_SIM")

def _add_attributes_to_jobspec(yaml: FluxYAML, duration: int = 60, cwd: str = "/tmp", 
                    job_name: str = "a000_20250101_fc0_1_SIM", output_file: str = "/tmp/out", 
                    error_file: str = "/tmp/err", script_content: str = "echo hello\n"):
    yaml.set_attributes(
        duration=duration,
        cwd=cwd,
        job_name=job_name,
        output_file=output_file,
        error_file=error_file,
        script_content=script_content,
    )

def test_generate_sections():
    yaml = FluxYAML("a000_20250101_fc0_1_SIM")

    # No sections
    with pytest.raises(ValueError):
        yaml.generate()

    # Only resources
    yaml.add_resource(label="task", ntasks=1, num_nodes=0, num_cores=1)
    with pytest.raises(ValueError):
        yaml.generate()

    # Only tasks
    yaml = FluxYAML("a000_20250101_fc0_1_SIM")
    yaml.add_task(resource_label="task", count_per_slot=1)
    with pytest.raises(ValueError):
        yaml.generate()

    # Only attributes
    yaml = FluxYAML("a000_20250101_fc0_1_SIM")
    _add_attributes_to_jobspec(yaml)
    with pytest.raises(ValueError):
        yaml.generate()

    # Resources + tasks
    yaml = FluxYAML("a000_20250101_fc0_1_SIM")
    yaml.add_resource(label="task", ntasks=1, num_nodes=0, num_cores=1)
    yaml.add_task(resource_label="task", count_per_slot=1)
    with pytest.raises(ValueError):
        yaml.generate()

    # Resources + attributes
    yaml = FluxYAML("a000_20250101_fc0_1_SIM")
    yaml.add_resource(label="task", ntasks=1, num_nodes=0, num_cores=1)
    _add_attributes_to_jobspec(yaml)
    with pytest.raises(ValueError):
        yaml.generate()

    # Tasks + attributes
    yaml = FluxYAML("a000_20250101_fc0_1_SIM")
    yaml.add_task(resource_label="task", count_per_slot=1)
    _add_attributes_to_jobspec(yaml)
    with pytest.raises(ValueError):
        yaml.generate()

    # Resources + tasks + attributes (valid case)
    yaml.add_resource(label="task", ntasks=1, num_nodes=0, num_cores=1)
    
    # Generate and check existence of sections
    jobspec = yaml.generate()
    assert isinstance(jobspec, str)
    assert "resources:" in jobspec
    assert "tasks:" in jobspec
    assert "attributes:" in jobspec
    assert "version:" in jobspec

def test_add_resource_invalid_inputs(yaml):
    # Negative ntasks
    with pytest.raises(ValueError):
        yaml.add_resource(label="task", ntasks=-1)

    # Negative num_nodes
    with pytest.raises(ValueError):
        yaml.add_resource(label="task", num_nodes=-1)

    # Negative num_cores
    with pytest.raises(ValueError):
        yaml.add_resource(label="task", num_cores=-1)

    # Negative mem_per_node_mb
    with pytest.raises(ValueError):
        yaml.add_resource(label="task", mem_per_node_mb=-1)

    # Negative mem_per_core_mb
    with pytest.raises(ValueError):
        yaml.add_resource(label="task", mem_per_core_mb=-1)

    # Negative tasks_per_node
    with pytest.raises(ValueError):
        yaml.add_resource(label="task", tasks_per_node=-1)

    # Empty label
    with pytest.raises(ValueError):
        yaml.add_resource(label="  ")

    # None label
    with pytest.raises(ValueError):
        yaml.add_resource(label=None)

def test_add_resource(yaml): # TODO: [ENGINES] Expand to check generated resource structure
    # Both nodes and nodes_per_node set, and cores set to zero
    assert yaml.add_resource(label="task", ntasks=8, num_nodes=2, num_cores=0, tasks_per_node=4) == 8

    # Only nodes set
    assert yaml.add_resource(label="task2", ntasks=8, num_nodes=2, num_cores=1) == 8

    # Only tasks_per_node set
    assert yaml.add_resource(label="task3", ntasks=8, num_cores=2, tasks_per_node=4) == 8

    # No nodes nor tasks_per_node set, only ntasks
    assert yaml.add_resource(label="task4", ntasks=8, num_cores=1) == 0
    assert yaml.add_resource(label="task5", ntasks=0, num_cores=1) == 0

    # Mem per node and per core set
    assert yaml.add_resource(label="task6", ntasks=8, num_nodes=2, num_cores=2, mem_per_node_mb=2048, mem_per_core_mb=1024) == 8

def test_add_task_invalid_inputs(yaml):
    # Both counts negative
    with pytest.raises(ValueError):
        yaml.add_task(resource_label="task", count_per_slot=-1, count_total=-1)

    # Negative per_slot
    with pytest.raises(ValueError):
        yaml.add_task(resource_label="task", count_per_slot=-1)

    # Negative total
    with pytest.raises(ValueError):
        yaml.add_task(resource_label="task", count_total=-1)

    # Empty label
    with pytest.raises(ValueError):
        yaml.add_task(resource_label="  ", count_per_slot=1)

    # None label
    with pytest.raises(ValueError):
        yaml.add_task(resource_label=None, count_per_slot=1)

    # Both counts set
    with pytest.raises(ValueError):
        yaml.add_task(resource_label="task", count_per_slot=1, count_total=1)

    # None counts set
    with pytest.raises(ValueError):
        yaml.add_task(resource_label="task")

def test_add_task(yaml):
    # count_per_slot set
    yaml.add_task(resource_label="task1", count_per_slot=1)
    assert len(yaml.tasks) == 1

    # count_total set
    yaml.add_task(resource_label="task2", count_total=1)
    assert len(yaml.tasks) == 2

def test_set_attributes_invalid_inputs(yaml):
    # Negative duration
    with pytest.raises(ValueError):
        _add_attributes_to_jobspec(yaml, duration=-10)

    # Zero duration
    with pytest.raises(ValueError):
        _add_attributes_to_jobspec(yaml, duration=0)

    # Empty cwd
    with pytest.raises(ValueError):
        _add_attributes_to_jobspec(yaml, cwd="   ")

    # None cwd
    with pytest.raises(ValueError):
        _add_attributes_to_jobspec(yaml, cwd=None)

    # Empty job name
    with pytest.raises(ValueError):
        _add_attributes_to_jobspec(yaml, job_name="   ")

    # None job name
    with pytest.raises(ValueError):
        _add_attributes_to_jobspec(yaml, job_name=None)

    # Empty output file
    with pytest.raises(ValueError):
        _add_attributes_to_jobspec(yaml, output_file="   ")

    # None output file
    with pytest.raises(ValueError):
        _add_attributes_to_jobspec(yaml, output_file=None)

    # Empty error file
    with pytest.raises(ValueError):
        _add_attributes_to_jobspec(yaml, error_file="   ")

    # None error file
    with pytest.raises(ValueError):
        _add_attributes_to_jobspec(yaml, error_file=None)

    # Empty job name
    with pytest.raises(ValueError):
        _add_attributes_to_jobspec(yaml, job_name="   ")

    # None job name
    with pytest.raises(ValueError):
        _add_attributes_to_jobspec(yaml, job_name=None)

    # Empty output file
    with pytest.raises(ValueError):
        _add_attributes_to_jobspec(yaml, output_file="   ")

    # None output file
    with pytest.raises(ValueError):
        _add_attributes_to_jobspec(yaml, output_file=None)

    # Empty error file
    with pytest.raises(ValueError):
        _add_attributes_to_jobspec(yaml, error_file="   ")

    # None error file
    with pytest.raises(ValueError):
        _add_attributes_to_jobspec(yaml, error_file=None)

    # Empty script content
    with pytest.raises(ValueError):
        _add_attributes_to_jobspec(yaml, script_content="   ")

    # None script content
    with pytest.raises(ValueError):
        _add_attributes_to_jobspec(yaml, script_content=None)

    # None script content
    with pytest.raises(ValueError):
        _add_attributes_to_jobspec(yaml, script_content=None)

def test_set_attributes(yaml):
    _add_attributes_to_jobspec(yaml)
    assert yaml.attributes['system']['duration'] == 60
    assert yaml.attributes['system']['cwd'] == "/tmp"
    assert yaml.attributes['system']['job']['name'] == "a000_20250101_fc0_1_SIM"
    assert yaml.attributes['system']['shell']['options']['output']['stdout']['path'] == "/tmp/out"
    assert yaml.attributes['system']['shell']['options']['output']['stderr']['path'] == "/tmp/err"
    assert yaml.attributes['system']['files']['script']['data'] == "echo hello\n"

def test_compose_node_resource_invalid_inputs(yaml):
    # Zero count
    with pytest.raises(ValueError):
        yaml._compose_node_resource()

    # Negative count
    with pytest.raises(ValueError):
        yaml._compose_node_resource(count=-1)

    # Negative mem_per_node_mb
    with pytest.raises(ValueError):
        yaml._compose_node_resource(mem_per_node_mb=-1)

def test_compose_node_resource(yaml):
    # count and mem_per_node_mb set
    node_dict = yaml._compose_node_resource(count=4, mem_per_node_mb=2048)
    assert "'count': 4" in str(node_dict)
    assert "'memory', 'count': 2048" in str(node_dict)


def test_compose_slot_resource_invalid_inputs(yaml):
    # Zero count
    with pytest.raises(ValueError):
        yaml._compose_slot_resource(label="task", count=0)

    # Negative count
    with pytest.raises(ValueError):
        yaml._compose_slot_resource(label="task", count=-1)
    # Empty label
    with pytest.raises(ValueError):
        yaml._compose_slot_resource(label="   ", count=1)

    # None label
    with pytest.raises(ValueError):
        yaml._compose_slot_resource(label=None, count=1)

def test_compose_slot_resource(yaml):
    slot_dict = yaml._compose_slot_resource(label="task", count=4)
    assert "'count': 4" in str(slot_dict)
    assert "'type': 'slot'" in str(slot_dict)
    assert "'label': 'task'" in str(slot_dict)

def test_compose_core_resource_invalid_inputs(yaml):
    # Zero count
    with pytest.raises(ValueError):
        yaml._compose_core_resource(count=0)

    # Negative count
    with pytest.raises(ValueError):
        yaml._compose_core_resource(count=-1)

    # Negative mem_per_core_mb
    with pytest.raises(ValueError):
        yaml._compose_core_resource(count=1, mem_per_core_mb=-1)

def test_compose_core_resource(yaml):
    # mem_per_core_mb set
    core_dict = yaml._compose_core_resource(count=4, mem_per_core_mb=1024)
    assert "'count': 4" in str(core_dict)
    assert "'memory', 'count': 1024" in str(core_dict)

    # Only count set
    core_dict = yaml._compose_core_resource(count=2)
    assert "'count': 2" in str(core_dict)
    assert "memory" not in str(core_dict)