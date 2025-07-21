import os
from pathlib import Path
from functools import cached_property
import warnings
import re
import yaml

from autosubmitconfigparser.config.configcommon import AutosubmitConfig

from autosubmit.job.job_list import JobList
from autosubmit.generators import AbstractGenerator
from autosubmit.platforms.platform import Platform
from autosubmit.platforms.paramiko_platform import ParamikoPlatform

"""The AiiDA generator for Autosubmit."""

# Autosubmit Task name separator (not to be confused with task and chunk name separator).
DEFAULT_SEPARATOR = '_'


class Generator(AbstractGenerator):
    """Generates an aiida workflow script that initializes all required AiiDA resources.

    The generated file is structures as the following:
    * header: Information about the generation of the file
    * imports: All required imports
    * init: Initialization of all python resources that need to be instantiated once for the whole script
    * create_orm_nodes: Creation of all AiiDA's object-relational mapping (ORM) nodes covering the creation of computer and codes
    * workgraph_tasks: Creation of the AiiDA-WorkGraph tasks
    * workgraph_deps: Linking of the dependencies between the AiiDA-WorkGraph tasks
    * workgraph_submission: Submission of the AiiDA-WorkGraph
    """
    # AiiDA Slurm options
    # --cpus-per-task -> num_cores_per_mpiproc
    # --ntasks-per-node -> num_mpiprocs_per_machine
    # --nodes -> num_machines
    # --mem -> max_memory_kb (with some translation)
    # --qos -> qos
    # --partition -> queue_name
    #
    # Autosubmit Slurm options
    # --cpus-per-task -> NUMTHREADS (job)
    # --ntasks-per-node -> TASKS (job)
    # --nodes -> NODES (job)
    # --mem -> MEMORY (job)
    # --qos -> CURRENT_QUEUE (job)
    # --partition -> PARTITION (job)
    #
    SUPPORTED_JOB_KEYWORDS = ["NUMTHREADS", "TASKS", "NODES", "WALLCLOCK", "MEMORY", "PLATFORM",  # these we need to transfer to aiida
                              "DEPENDENCIES", "FILE", "RUNNING"]  # these are resolved by autosubmit internally

    SUPPORTED_PLATFORM_KEYWORDS = ["TYPE", "HOST", "USER", "QUEUE", "SCRATCH_DIR", "MAX_WALLCLOCK",  #  these we need to transfer to aiida
                                    "PROJECT"]   # these are resolved by autosubmit internally
    
    def __init__(self, job_list: JobList, as_conf: AutosubmitConfig, output_dir: str):
        if not (output_path := Path(output_dir)).exists():
            raise ValueError(f"Given `output_dir` {output_path} does not exist.")
        self._output_path = (output_path / job_list.expid).absolute()
        self._output_path.mkdir(exist_ok=True)
        self._job_list = job_list
        self._as_conf = as_conf

    @classmethod
    def generate(cls, job_list: JobList, as_conf: AutosubmitConfig, output_dir: str) -> None:
        self = cls(job_list, as_conf, output_dir)
        self._validate()
        workflow_script = self._generate_workflow_script()
        (self._output_path / "submit_aiida_workflow.py").write_text(workflow_script)
        (self._output_path / "README.md").write_text(self._generate_readme())
        
    @staticmethod
    def get_engine_name() -> str:
        return "AiiDA"

    @staticmethod
    def add_parse_args(parser) -> None:
        parser.add_argument('-o', '--output_dir', dest="output_dir", default=".", help='Output directory')

    def _validate(self) -> None:
        """Validate jobs"""
        for job_name, job_conf in self._as_conf.jobs_data.items():
            for key in job_conf.keys():
                if key not in Generator.SUPPORTED_JOB_KEYWORDS:
                    msg = f"Found in job {job_name} configuration file key {key} that is not officially supported for AiiDA generator. It might result in an error."
                    warnings.warn(msg)
        ## validate platforms
        for platform_name, platform_conf in self._as_conf.platforms_data.items():
            # only validate platforms that are used in jobs
            if platform_name in self._platforms_used_in_job.keys():
                for key, value in platform_conf.items():
                    if key not in Generator.SUPPORTED_PLATFORM_KEYWORDS and value != '':
                        msg = f"Found in platform {platform_name} configuration file key {key} that is not supported for AiiDA generator."
                        warnings.warn(msg)


    @cached_property
    def _platforms_used_in_job(self) -> dict[str, Platform]:
        platforms_used_in_jobs = {}
        for job in self._job_list.get_all():
            platforms_used_in_jobs[job.platform.name] = job.platform
        return platforms_used_in_jobs

    def _generate_workflow_script(self) -> str:
        """Generates a PyFlow workflow using Autosubmit database.

        The ``autosubmit create`` command must have been already executed prior
        to calling this function. This is so that the jobs are correctly loaded
        to produce the PyFlow workflow.

        :param job_list: ``JobList`` Autosubmit object, that contains the parameters, jobs, and graph
        :param as_conf: Autosubmit configuration
        :param options: a list of strings with arguments (equivalent to sys.argv), passed to argparse
        """
        header = self._generate_header_section()
        imports = self._generate_imports_section()
        init = self._generate_init_section()
        create_orm_nodes = self._generate_create_orm_nodes_section()
        workgraph_tasks = self._generate_workgraph_tasks_section()
        workgraph_deps = self._generate_workgraph_deps_section()
        workgraph_submission = self._generate_workgraph_submission_section()
        return header + imports + init + create_orm_nodes + workgraph_tasks + workgraph_deps + workgraph_submission


    def _generate_header_section(self) -> str:
        return f"""# HEADER
# This is in autogenerated file from {self._job_list.expid}
# The computer and codes are defined for the following platforms:
# {list(self._platforms_used_in_job.keys())}

"""

    def _generate_imports_section(self) -> str:
        return """# IMPORTS
import aiida
from aiida import orm
from aiida_workgraph import WorkGraph
from aiida.orm.utils.builders.computer import ComputerBuilder
from aiida.common.exceptions import NotExistent
from pathlib import Path
import yaml

"""

    def _generate_init_section(self) -> str:
        return """# INIT 
aiida.load_profile()
wg = WorkGraph()
tasks = {}

"""

    def _generate_create_orm_nodes_section(self) -> str:
        # aiida computer

        code_section = "# CREATE_ORM_NODES"
        for platform in self._platforms_used_in_job.values():

            from autosubmit.platforms.locplatform import LocalPlatform
            from autosubmit.platforms.slurmplatform import SlurmPlatform
            if isinstance(platform, LocalPlatform):                       
                computer_setup = {
                    "label": f"{platform.name}",
                    "hostname": f"{platform.host}",
                    "work_dir": f"{platform.scratch}",
                    "description": "",
                    "transport": "core.local",
                    "scheduler": "core.direct",
                    "mpirun_command": "mpirun -np {tot_num_mpiprocs}",
                    "mpiprocs_per_machine": 1,
                    "default_memory_per_machine": None,
                    "append_text": "",
                    "prepend_text": "",
                    "use_double_quotes": False,
                    "shebang": "#!/bin/bash",
                }
            elif isinstance(platform, SlurmPlatform):
                computer_setup = {
                    "label": f"{platform.name}",
                    "hostname": f"{platform.host}",
                    "work_dir": f"{platform.scratch}",
                    #username": f"{platform.user}", does not work
                    "description": "",
                    "transport": "core.ssh",
                    "scheduler": "core.slurm",
                    "mpirun_command": "mpirun -np {tot_num_mpiprocs}",
                    "mpiprocs_per_machine": platform.processors_per_node,
                    "default_memory_per_machine": None, # This is specified in the task option
                    "append_text": "", 
                    "prepend_text": "",
                    "use_double_quotes": False,
                    "shebang": "#!/bin/bash",
                }
            else:
                raise ValueError(f"Platform type {platform} not supported for engine aiida.")

            computer_setup_path = Path(self._output_path / f"{platform.name}/{platform.name}-setup.yml")
            computer_setup_path.parent.mkdir(exist_ok=True)
            computer_setup_path.write_text(yaml.dump(computer_setup))
            create_computer = f"""
try:
    computer = orm.load_computer("{platform.name}")
    print(f"Loaded computer {{computer.label!r}}")
except NotExistent:
    setup_path = Path("{computer_setup_path}")
    config_kwargs = yaml.safe_load(setup_path.read_text())
    computer = ComputerBuilder(**config_kwargs).new().store()
    computer.configure(safe_interval=5.0)
    computer.set_minimum_job_poll_interval(5.0)

    from aiida.transports.plugins.ssh import SshTransport
    from aiida.transports import cli as transport_cli
    default_kwargs = {{name: transport_cli.transport_option_default(name, computer) for name in SshTransport.get_valid_auth_params()}}
    default_kwargs["port"] = int(default_kwargs["port"])
    default_kwargs["timeout"] = float(default_kwargs["timeout"])
    default_kwargs["username"] = "{platform.user}"
    default_kwargs['key_filename'] = "{os.environ["HOME"]}/.ssh/id_rsa"
    computer.configure(user=orm.User.collection.get_default(), **default_kwargs)
    print(f"Created and stored computer {{computer.label}}")"""

            # aiida bash code to run script
            code_setup = {
                "computer": f"{platform.name}",
                "filepath_executable": "/bin/bash",
                "label": "bash",
                "description": '',
                "default_calc_job_plugin": 'core.shell',
                "prepend_text": '',
                "append_text": '',
                "use_double_quotes": False,
                "with_mpi": False
            }

            code_setup_path =  Path(self._output_path / f"{platform.name}/bash@{platform.name}-setup.yml")
            code_setup_path.parent.mkdir(exist_ok=True)
            code_setup_path.write_text(yaml.dump(code_setup))
            create_code =  f"""
try:
    bash_code = orm.load_code("bash@{platform.name}")
except NotExistent:
    setup_path = Path("{code_setup_path}")
    setup_kwargs = yaml.safe_load(setup_path.read_text())
    setup_kwargs["computer"] = orm.load_computer(setup_kwargs["computer"])
    bash_code = orm.InstalledCode(**setup_kwargs).store()
    print(f"Created and stored bash@{{computer.label}}")"""
            code_section += create_computer + create_code
        code_section += "\n\n"
        return code_section

    def _generate_workgraph_tasks_section(self):
        code_section = "# WORKGRAPH_TASKS"

        for job in self._job_list.get_all():
            script_name = job.create_script(self._as_conf)
            script_path = Path(job._tmp_path, script_name)
            script_text = open(script_path).read()
            # Let's drop the Autosubmit header and tailed.
            trimmed_script_text = re.findall(
                r'# Autosubmit job(.*)# Autosubmit tailer',
                script_text,
                flags=re.DOTALL | re.MULTILINE)[0][1:-1] 
            trimmed_script_path = self._output_path / script_name
            trimmed_script_path.write_text(trimmed_script_text)
            create_task = f"""
tasks["{job.name}"] = wg.add_task(
    "workgraph.shelljob",
    name = "{job.name}",
    command = orm.load_code("bash@{job.platform.name}"),
    arguments = ["{{script}}"],
    nodes = {{"script": orm.SinglefileData("{trimmed_script_path}")}}
)"""
            if job.memory != "":
                create_task += f"""
tasks["{job.name}"].set({{"metadata.options.max_memory_kb": {job.memory}}})"""
            if job.platform.max_wallclock is None:
                if job.parameters["WALLCLOCK"] != "":
                    wallclock_seconds = int(ParamikoPlatform.parse_time(job.wallclock).total_seconds())
                else:
                    wallclock_seconds = int(ParamikoPlatform.parse_time(job.platform.wallclock).total_seconds())
                create_task += f"""
tasks["{job.name}"].set({{"metadata.options.max_wallclock_seconds": {wallclock_seconds}}})"""
            if job.partition != "":
                create_task += f"""
tasks["{job.name}"].set({{"metadata.options.queue_name": "{job.partition}"}})"""
            

            code_section += create_task
        code_section += "\n\n"
        return code_section

    def _generate_workgraph_deps_section(self) -> str:
        code_section = "# WORKGRAPH_DEPS"
        for edge in self._job_list.graph.edges:
            code_section += f"""
tasks["{edge[1]}"].waiting_on.add(tasks["{edge[0]}"])"""
        code_section += "\n\n"
        return code_section


    def _generate_workgraph_submission_section(self) -> str:
        return """# WORKGRAPH_SUBMISSION 
wg.run()"""

    def _generate_readme(self) -> str:
        return f"""### Meta-information
This file has been auto generated from expid {self._job_list.expid}.

### Instructions
To run the workflow please ensure that you have AiiDA installed and configured.
For that please refer to the
[installation section of AiiDA](https://aiida.readthedocs.io/projects/aiida-core/en/stable/installation/index.html)
Then you can run the generated workflow with:
```bash
python submit_aiida_workflow.py
```
The workflow is compatible with aiida-core~=2.6.0 and aiida-workgraph==0.5.2.

### Node Creation

The workflow creates the computer and code nodes as required from the
autosubmit config. If the computer and code nodes have been already created for
example through a previous run, they are loaded. If you have changed the
computer parameters in your autosubmit config, then you need to delete the
corresponding compute and code nodes. Note that this has the disadvantage thit
it will delete all calculations that are associated with these nodes.
It is therefore recommended to use the JOB paramaters in autosubmit to override
certain computer configs.
"""


__all__ = [
    'Generator'
]
