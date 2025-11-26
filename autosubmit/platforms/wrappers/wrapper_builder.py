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
import os
import random
import string
import textwrap
from autosubmit.log.log import AutosubmitCritical

from typing import List


class WrapperDirector:
    """
    Construct an object using the Builder interface.
    """

    def __init__(self):
        self._builder = None

    def construct(self, builder):
        self._builder = builder

        header = self._builder.build_header()
        job_thread = self._builder.build_job_thread()
        # if "bash" not in header[0:15]:

        main = self._builder.build_main()
        # else:
        #    nodes,main = self._builder.build_main() #What to do with nodes?
        # change to WrapperScript object
        wrapper_script = header + job_thread + main
        wrapper_script = wrapper_script.replace("_NEWLINE_", '\\n')

        return wrapper_script


class WrapperBuilder(object):

    def __init__(self, **kwargs):
        if "retrials" in list(kwargs.keys()):
            self.retrials = kwargs['retrials']
        self.header_directive = kwargs['header_directive']
        self.job_scripts = kwargs['jobs_scripts']
        self.threads = kwargs['threads']
        self.num_procs = kwargs['num_processors']
        self.num_procs_value = kwargs['num_processors_value']
        self.expid = kwargs['expid']
        self.jobs_resources = kwargs.get('jobs_resources', dict())
        self.allocated_nodes = kwargs.get('allocated_nodes', '')
        self.machinefiles_name = ''
        self.machinefiles_indent = 0
        self.exit_thread = ''
        self.custom_env_setup = kwargs['wrapper_data'].custom_env_setup
        if "wallclock_by_level" in list(kwargs.keys()):
            self.wallclock_by_level = kwargs['wallclock_by_level']
        self.working_dir = kwargs.get('working_dir', '')

    def build_header(self):
        return textwrap.dedent(self.header_directive) + self.build_imports()

    def build_imports(self):
        pass  # pragma: no cover

    def build_job_thread(self):
        pass  # pragma: no cover

    # hybrids
    def build_joblist_thread(self, **kwargs):
        pass  # pragma: no cover

    # horizontal and hybrids
    def build_nodes_list(self):
        pass  # pragma: no cover

    def build_machinefiles(self):
        pass  # pragma: no cover

    def get_machinefile_function(self):
        machinefile_function = ""
        if 'MACHINEFILES' in self.jobs_resources and self.jobs_resources['MACHINEFILES']:
            machinefile_function = self.jobs_resources['MACHINEFILES']

            self.machinefiles_name = "jobname"

            if machinefile_function == 'COMPONENTS':
                return self.build_machinefiles_components()
            else:
                return self.build_machinefiles_standard()
        return machinefile_function

    def build_machinefiles_standard(self):
        pass  # pragma: no cover

    def build_machinefiles_components(self):
        pass  # pragma: no cover

    def build_machinefiles_components_alternate(self):
        pass  # pragma: no cover

    def build_sequential_threads_launcher(self, **kwargs):
        pass  # pragma: no cover

    def build_parallel_threads_launcher(self, **kwargs):
        pass  # pragma: no cover

    # all should override -> abstract!
    def build_main(self):
        pass  # pragma: no cover

    def _indent(self, text, amount, ch=' '):
        padding = amount * ch
        return ''.join(padding + line for line in text.splitlines(True))

class FluxWrapperBuilder(WrapperBuilder):
    # TODO: [ENGINES] Check error handling behavior
    # TODO: [ENGINES] Command "flux start" is being called multiple times
    # TODO: [ENGINES] Is it necessary to pass the run_id to the inner jobs?
    """
    The FluxWrapperBuilder is the responsible for generating the wrapper script
    that will be submitted to Slurm to initialize Flux and submit the inner jobs
    to the Flux scheduler inside the allocation.
    
    This is a special implementation because we use Flux as a wrapper engine inside 
    Slurm allocations, not as a platform.
    """
    def build_imports(self):
        return ""
    
    def build_job_thread(self):
        return ""
    
    def build_main(self):
        if not self.job_scripts:
            raise AutosubmitCritical("No job scripts found for building the Flux wrapper.")
        
        return textwrap.dedent("""
        # Flux script generation
        cat << 'EOF' > flux_runner.py
        {0}
        EOF

        # Grant execution permission to the generated script
        chmod +x flux_runner.py

        # Load user environment
        {1}

        # Instantiate Flux within the allocated resources and run the jobs
        srun --cpu-bind=none flux start --verbose=2 python flux_runner.py
        """).format(self._generate_flux_script(), self._custom_environmet_setup(), '\n'.ljust(13))
    
    def _generate_flux_script(self):
        """
        Each Flux wrapper type implements its own script given the dependencies
        of its inner jobs.
        """
        pass  # pragma: no cover

    def _custom_environmet_setup(self):
        # TODO: [ENGINES] Delete hardcoded flux environment setup
        commands = self.custom_env_setup
        if commands == '':
            commands = "# No commands provided"
        return textwrap.dedent("""\
            {0}
            """).format(commands, '\n'.ljust(0))

class FluxVerticalWrapperBuilder(FluxWrapperBuilder):
    # TODO: [ENGINES] Check retrial behavior
    # TODO: [ENGINES] Stat file generation, retry management...
    def _generate_flux_script(self):
        return textwrap.dedent("""
            import os
            import flux
            import flux.job

            handle = flux.Flux()
            job_scripts={0}

            for job_script in job_scripts:
                print("Submitting job script: " + job_script)
                jobspec = flux.job.JobspecV1.from_yaml_file(job_script)
                jobid = flux.job.submit(handle, jobspec, waitable=True)
                print("RESOURCE COUNTS :" + str(jobspec.resource_counts()))
                print("RESOURCES: " + str(jobspec.resources))
                print("Waiting for job " + str(jobid) + " to finish...")
                print(flux.job.wait(handle, jobid))

                job_meta = flux.job.get_job(handle, jobid)
                print("JOB META: " + str(job_meta))

                completed_path = job_script.replace('.cmd', '_COMPLETED')
                if not os.path.exists(completed_path):
                    print("The job " + job_script + " has FAILED")
                    open(job_script.replace('.cmd', '_FAILED'),'w').close()
                    open("WRAPPER_FAILED",'w').close()
                    exit(1)
                else:
                    print("The job " + job_script + " has been COMPLETED")
            """).format(self.job_scripts, '\n'.ljust(13))
        # return textwrap.dedent("""
        # max_retries={1}
        # scripts="{0}"

        # for job_script in $scripts; do
        #     fail_count=0
        #     completed=0

        #     while [ $fail_count -le $max_retries ] && [ $completed -eq 0 ]; do
        #         # Job info
        #         job_name=$(basename "$job_script" .cmd)
        #         output_log="${{job_name}}.cmd.out.${{fail_count}}"
        #         error_log="${{job_name}}.cmd.err.${{fail_count}}"

        #         # Submit the job
        #         job_id=$(flux batch --output=$output_log --error=$error_log $job_script)

        #         # Wait for the job to finish
        #         flux job wait $job_id

        #         # Create the STAT file
        #         start=$(flux job info $job_id eventlog | grep '"name":"start"' | sed -n 's/.*"timestamp":\\([^,}}]*\\).*/\\1/p')
        #         finish=$(flux job info $job_id eventlog | grep '"name":"finish"' | sed -n 's/.*"timestamp":\\([^,}}]*\\).*/\\1/p')
        #         stat_filename="${{job_name}}_STAT_${{fail_count}}"
        #         echo "${{start%.*}}" > $stat_filename
        #         echo "${{finish%.*}}" >> $stat_filename

        #         # Check if the job completed successfully
        #         if [ -f "${{job_name}}_COMPLETED" ]; then
        #             echo "The job $job_name has been COMPLETED"
        #             completed=1
        #         else
        #             echo "The job $job_name has FAILED"
        #             fail_count=$((fail_count + 1))
        #         fi
        #     done

        #     if [ $completed -eq 0 ]; then
        #         touch "${{job_name}}_FAILED"
        #         touch "WRAPPER_FAILED"
        #         exit 1
        #     fi
        # done
        # """).format(' '.join(str(s) for s in self.job_scripts), self.retrials, '\n'.ljust(13))
    
class FluxHorizontalWrapperBuilder(FluxWrapperBuilder):
    def _generate_flux_script(self):
        raise NotImplementedError("Horizontal wrappers with the Flux method are not implemented yet.")
        # return textwrap.dedent("""
        # declare -A job_ids
        # scripts="{0}"

        # # Submit the jobs
        # for job_script in $scripts; do
        #     job_name=$(basename "$job_script" .cmd)
        #     output_log="${{job_name}}.cmd.out.0"
        #     error_log="${{job_name}}.cmd.err.0"

        #     job_ids[$job_name]=$(flux batch --output=$output_log --error=$error_log $job_script)
        # done

        # # Wait for the jobs to finish
        # wrapper_failed=0
        # for job_script in $scripts; do
        #     job_name=$(basename "$job_script" .cmd)
        #     flux job wait ${{job_ids[$job_name]}}

        #     # Check if the job completed successfully
        #     if [ -f "${{job_name}}_COMPLETED" ]; then
        #         echo "The job $job_name has been COMPLETED"
        #     else
        #         echo "The job $job_name has FAILED"
        #         touch "${{job_name}}_FAILED"
        #         wrapper_failed=1
        #     fi
        # done

        # if [ $wrapper_failed -eq 1 ]; then
        #     touch "WRAPPER_FAILED"
        #     exit 1
        # fi
        # """).format(' '.join(str(s) for s in self.job_scripts), '\n'.ljust(13))

class FluxVerticalHorizontalWrapperBuilder(FluxWrapperBuilder):
    # TODO: [ENGINES] Observe what happens if a single job arrives. Is it inside another list?
    def _generate_flux_script(self):
        raise NotImplementedError("Vertical-Horizontal wrappers with the Flux method are not implemented yet.")
        # scripts_str = ''
        # for i, job_list in enumerate(self.job_scripts):
        #     scripts_str += f"""scripts[{i}]="{' '.join(str(s) for s in job_list).strip()}"\n"""

        # return textwrap.dedent("""
        # # Job scripts per inner vertical wrapper
        # declare -A scripts
        # {0}

        # execute_vertical_wrapper()
        # {{
        #     scripts=$1
        #     for job_script in $scripts; do
        #         job_name=$(basename "$job_script" .cmd)
        #         output_log="${{job_name}}.cmd.out.0"
        #         error_log="${{job_name}}.cmd.err.0"

        #         # Submit the job
        #         job_id=$(flux batch --output=$output_log --error=$error_log $job_script)

        #         # Wait for the job to finish
        #         flux job wait $job_id

        #         # Check if the job completed successfully
        #         if [ -f "${{job_name}}_COMPLETED" ]; then
        #             echo "The job $job_name has been COMPLETED"
        #         else
        #             echo "The job $job_name has FAILED"
        #             touch "${{job_name}}_FAILED"
        #             touch "WRAPPER_FAILED"
        #             return 1
        #         fi
        #     done

        #     return 0
        # }}

        # # Execute vertical wrappers in parallel
        # for ((i = 0; i < ${{#scripts[@]}}; i++)); do
        #     execute_vertical_wrapper "${{scripts[$i]}}" &
        # done

        # # Wait for all vertical wrappers (subprocesses) to finish
        # wait
        # """).format(scripts_str, '\n'.ljust(13))

class FluxHorizontalVerticalWrapperBuilder(FluxWrapperBuilder):
    def _generate_flux_script(self):
        raise NotImplementedError("Horizontal-Vertical wrappers with the Flux method are not implemented yet.")
        # scripts_str = ''
        # for i, job_list in enumerate(self.job_scripts):
        #     scripts_str += f"""scripts[{i}]="{' '.join(str(s) for s in job_list).strip()}"\n"""

        # return textwrap.dedent("""
        # # Job scripts per inner horizontal wrapper
        # declare -A job_ids
        # declare -A scripts
        # {0}

        # execute_horizontal_wrapper()
        # {{
        #     scripts=$1

        #     # Submit the jobs
        #     for job_script in $scripts; do
        #         job_name=$(basename "$job_script" .cmd)
        #         output_log="${{job_name}}.cmd.out.0"
        #         error_log="${{job_name}}.cmd.err.0"

        #         job_ids[$job_name]=$(flux batch --output=$output_log --error=$error_log $job_script)
        #     done

        #     # Wait for the jobs to finish
        #     wrapper_failed=0
        #     for job_script in $scripts; do
        #         job_name=$(basename "$job_script" .cmd)
        #         flux job wait ${{job_ids[$job_name]}}

        #         # Check if the job completed successfully
        #         if [ -f "${{job_name}}_COMPLETED" ]; then
        #             echo "The job $job_name has been COMPLETED"
        #         else
        #             echo "The job $job_name has FAILED"
        #             touch "${{job_name}}_FAILED"
        #             wrapper_failed=1
        #         fi
        #     done

        #     if [ $wrapper_failed -eq 1 ]; then
        #         touch "WRAPPER_FAILED"
        #     fi
        # }}

        # # Execute horizontal wrappers
        # for ((i = 0; i < ${{#scripts[@]}}; i++)); do
        #     execute_horizontal_wrapper "${{scripts[$i]}}"

        #     if [ -f "WRAPPER_FAILED" ]; then
        #         exit 1
        #     fi
        # done
        # """).format(scripts_str, '\n'.ljust(13))

class PythonWrapperBuilder(WrapperBuilder):
    def get_random_alphanumeric_string(self, letters_count, digits_count):
        sample_str = ''.join((random.choice(string.ascii_letters) for i in range(letters_count)))
        sample_str += ''.join((random.choice(string.digits) for i in range(digits_count)))

        # Convert string to list and shuffle it to mix letters and digits
        sample_list = list(sample_str)
        random.shuffle(sample_list)
        final_string = ''.join(sample_list)
        return final_string

    def build_imports(self):
        random_alphanumeric = self.get_random_alphanumeric_string(5, 5)
        return textwrap.dedent(f"""
        import os
        import sys
        #from bscearth.utils.date import date2str 
        from threading import Thread
        from subprocess import getstatusoutput
        from datetime import datetime
        import time
        from math import ceil
        from collections import OrderedDict
        import copy
        class Unbuffered(object):
           def __init__(self, stream):
               self.stream = stream
           def write(self, data):
               self.stream.write(data)
               self.stream.flush()
           def writelines(self, datas):
               self.stream.writelines(datas)
               self.stream.flush()
           def __getattr__(self, attr):
               return getattr(self.stream, attr)
        sys.stdout = Unbuffered(sys.stdout)
        node_id = "{random_alphanumeric}"
        wrapper_id = "{random_alphanumeric}_FAILED"
        # Defining scripts to be run
        scripts= {str(self.job_scripts)}
        {os.linesep.ljust(13)}""")

    def build_job_thread(self):
        return textwrap.dedent(f"""
        class JobThread(Thread):
            def __init__ (self, template, id_run):
                Thread.__init__(self)
                self.template = template
                self.id_run = id_run

            def run(self):
                jobname = self.template.replace('.cmd', '')
                out = f"{self.working_dir}/{{str(self.template)}}.out.0"
                err = f"{self.working_dir}/{{str(self.template)}}.err.0"
                template_path = f"{self.working_dir}/{{self.template}}"
                print(out+"\\n")
                print(err+"\\n")
                command = f"chmod +x {{template_path}}; {{template_path}} > {{out}} 2> {{err}}"
                print(command)
                (self.status) = getstatusoutput(command)
        {os.linesep.ljust(13)}""")

    # hybrids
    def build_joblist_thread(self):
        pass

    # horizontal and hybrids
    def build_nodes_list(self):
        return self.get_nodes() + self.build_cores_list()

    def get_nodes(self):
        return textwrap.dedent(f"""
        # Getting the list of allocated nodes
        {self.allocated_nodes}
        os.system("mkdir -p machinefiles")

        with open(f"node_list_{{node_id}}", 'r') as file:
             all_nodes = file.read()
        os.remove(f"node_list_{{node_id}}")


        all_nodes = all_nodes.split("_NEWLINE_")
        if all_nodes[-1] == "":
            all_nodes = all_nodes[:-1]
        print(all_nodes)
        {os.linesep.ljust(13)}""")

    def build_cores_list(self):
        return textwrap.dedent(f"""
total_cores = {self.num_procs_value}
jobs_resources = {str(self.jobs_resources)}
processors_per_node = int(jobs_resources['PROCESSORS_PER_NODE'])
idx = 0
all_cores = []
while total_cores > 0:
    if processors_per_node > 0:
        processors_per_node -= 1
        total_cores -= 1
        all_cores.append(all_nodes[idx])
    else:
        if idx < len(all_nodes)-1:
            idx += 1
        processors_per_node = int(jobs_resources['PROCESSORS_PER_NODE'])
processors_per_node = int(jobs_resources['PROCESSORS_PER_NODE'])

        {os.linesep.ljust(13)}""")

    def build_machinefiles(self):
        machinefile_function = self.get_machinefile_function()
        if machinefile_function:
            return self.get_machinefile_function() + self._indent(self.write_machinefiles(), self.machinefiles_indent)
        return ""

    def build_machinefiles_standard(self):
        return textwrap.dedent(f"""
            machines = str()
            cores = int(jobs_resources[section]['PROCESSORS'])
            tasks = int(jobs_resources[section]['TASKS'])
            nodes = int(ceil(int(cores)/float(tasks)))
            if tasks < processors_per_node:
                cores = tasks
            job_cores = cores
            while nodes > 0:
                while cores > 0:
                    if len(all_cores) > 0:
                        node = all_cores.pop(0)
                        if node:
                            machines += node +"_NEWLINE_"
                            cores -= 1
                for rest in range(processors_per_node-tasks):
                    if len(all_cores) > 0:
                        all_cores.pop(0)
                nodes -= 1
                if tasks < processors_per_node:
                    cores = job_cores
        {os.linesep.ljust(13)}""")

    def _create_components_dict(self):
        return textwrap.dedent(f"""
        xio_procs = int(jobs_resources[section]['COMPONENTS']['XIO_NUMPROC'])
        rnf_procs = int(jobs_resources[section]['COMPONENTS']['RNF_NUMPROC'])
        ifs_procs = int(jobs_resources[section]['COMPONENTS']['IFS_NUMPROC'])
        nem_procs = int(jobs_resources[section]['COMPONENTS']['NEM_NUMPROC'])

        components = OrderedDict([
            ('XIO', xio_procs),
            ('RNF', rnf_procs),
            ('IFS', ifs_procs),
            ('NEM', nem_procs)
        ])

        jobs_resources[section]['COMPONENTS'] = components
        {os.linesep.ljust(13)}""")

    def build_machinefiles_components(self):
        return textwrap.dedent(f"""
        {self._create_components_dict()}

        machines = str()
        for component, cores in jobs_resources[section]['COMPONENTS'].items():        
            while cores > 0:
                if len(all_cores) > 0:
                    node = all_cores.pop(0)
                    if node:
                        machines += node +"_NEWLINE_"
                        cores -= 1
        {os.linesep.ljust(13)}""")

    def write_machinefiles(self):
        return textwrap.dedent(f"""
        machines = "_NEWLINE_".join([s for s in machines.split("_NEWLINE_") if s])
        with open("machinefiles/machinefile_"+{self.machinefiles_name}, "w") as machinefile:
            machinefile.write(machines)
        {os.linesep.ljust(13)}""")

    def build_sequential_threads_launcher(self, jobs_list, thread, footer=True):
        sequential_threads_launcher = textwrap.dedent(f"""
        failed_wrapper = os.path.join(os.getcwd(),wrapper_id)
        for i in range(len({jobs_list})):
            current = {thread}
            current.start()
            current.join()
        {os.linesep.ljust(13)}""")

        if footer:
            sequential_threads_launcher += self._indent(textwrap.dedent(f"""
            completed_filename = {jobs_list}[i].replace('.cmd', '_COMPLETED')
            completed_path = os.path.join(os.getcwd(), completed_filename)
            failed_filename = {jobs_list}[i].replace('.cmd', '_FAILED')
            failed_path = os.path.join(os.getcwd(), failed_filename)
            failed_wrapper = os.path.join(os.getcwd(), wrapper_id)
            if os.path.exists(completed_path):
                print(datetime.now(), "The job ", current.template," has been COMPLETED")
            else:
                open(failed_wrapper,'w').close()
                open(failed_path, 'w').close()
                print(datetime.now(), "The job ", current.template," has FAILED")

                #{1}
            {os.linesep.ljust(13)}"""), 4)
            sequential_threads_launcher += self._indent(textwrap.dedent(f"""
                if os.path.exists(failed_wrapper):
                    os.remove(os.path.join(os.getcwd(),wrapper_id))
                    wrapper_failed = os.path.join(os.getcwd(),"WRAPPER_FAILED")
                    open(wrapper_failed, 'w').close()
                    os._exit(1)

            {os.linesep.ljust(13)}"""), 4)
        else:
            sequential_threads_launcher += self._indent(textwrap.dedent(f"""
                if os.path.exists(failed_wrapper):
                    os.remove(os.path.join(os.getcwd(),wrapper_id))
                    wrapper_failed = os.path.join(os.getcwd(),"WRAPPER_FAILED")
                    open(wrapper_failed, 'wb').close()
                    os._exit(1)

            {os.linesep.ljust(13)}"""), 4)
        return sequential_threads_launcher

    def build_parallel_threads_launcher(self, jobs_list, thread, footer=True):
        parallel_threads_launcher = textwrap.dedent(f"""
pid_list = []
for i in range(len({jobs_list})):
    print("Starting job ", {jobs_list}[i])
    if type({jobs_list}[i]) != list:
        job = {jobs_list}[i]
        jobname = job.replace(".cmd", '')
        section = jobname.split('_')[-1]
    {self._indent(self.build_machinefiles(), 8)}
    current = {thread}({jobs_list}[i], i+self.id_run)
    pid_list.append(current)
    current.start()
# Waiting until all scripts finish
for i in range(len(pid_list)):
    pid = pid_list[i]
    pid.join()
        {os.linesep.ljust(13)}""")
        if footer:
            parallel_threads_launcher += self._indent(textwrap.dedent(f"""
        completed_filename = {jobs_list}[i].replace('.cmd', '_COMPLETED')
        completed_path = os.path.join(os.getcwd(), completed_filename)
        failed_filename = {jobs_list}[i].replace('.cmd', '_FAILED')
        failed_path = os.path.join(os.getcwd(),failed_filename)
        failed_wrapper = os.path.join(os.getcwd(),wrapper_id)
        if os.path.exists(completed_path):
            print(datetime.now(), "The job ", pid.template," has been COMPLETED")
        else:
            open(failed_wrapper, 'w').close()
            open(failed_path, 'w').close()
            print(datetime.now(), "The job ", pid.template," has FAILED")
                    {os.linesep.ljust(13)}"""), 4)
        return parallel_threads_launcher

    def build_parallel_threads_launcher_horizontal(self, jobs_list, thread, footer=True):
        parallel_threads_launcher = textwrap.dedent(f"""
pid_list = []
for i in range(len({jobs_list})):
    print("Starting job ", {jobs_list}[i])

    if type({jobs_list}[i]) != list:
        job = {jobs_list}[i]
        jobname = job.replace(".cmd", '')
        section = jobname.split('_')[-1]

    {self._indent(self.build_machinefiles(), 8)}
    current = {thread}({jobs_list}[i], i)
    pid_list.append(current)
    current.start()

# Waiting until all scripts finish
for i in range(len(pid_list)):
    pid = pid_list[i]
    pid.join()
        {os.linesep.ljust(13)}""")
        if footer:
            parallel_threads_launcher += self._indent(textwrap.dedent(f"""
        completed_filename = {jobs_list}[i].replace('.cmd', '_COMPLETED')
        completed_path = os.path.join(os.getcwd(), completed_filename)
        failed_filename = {jobs_list}[i].replace('.cmd', '_FAILED')
        failed_path = os.path.join(os.getcwd(),failed_filename)
        failed_wrapper = os.path.join(os.getcwd(),wrapper_id)
        Failed = False
        if os.path.exists(completed_path):
            print((datetime.now(), "The job ", pid.template," has been COMPLETED"))
        else:
            open(failed_wrapper, 'w').close()
            open(failed_path, 'w').close()
            print((datetime.now(), "The job ", pid.template," has FAILED"))
                    {os.linesep.ljust(13)}"""), 4)

        return parallel_threads_launcher

    def build_parallel_threads_launcher_vertical_horizontal(self, jobs_list, thread, footer=True):
        parallel_threads_launcher = textwrap.dedent(f"""
pid_list = []
for i in range(len({jobs_list})):
    print("Starting job ", {jobs_list}[i])

    if type({jobs_list}[i]) != list:
        job = {jobs_list}[i]
        jobname = job.replace(".cmd", '')
        section = jobname.split('_')[-1]

    {self._indent(self.build_machinefiles(), 8)}
    current = {thread}({jobs_list}[i], i)
    pid_list.append(current)
    current.start()

# Waiting until all scripts finish
for i in range(len(pid_list)):
    pid = pid_list[i]
    pid.join()
        {os.linesep.ljust(13)}""")
        if footer:
            parallel_threads_launcher += self._indent(textwrap.dedent(f"""
        completed_filename = {jobs_list}[i].replace('.cmd', '_COMPLETED')
        completed_path = os.path.join(os.getcwd(), completed_filename)
        failed_filename = {jobs_list}[i].replace('.cmd', '_FAILED')
        failed_path = os.path.join(os.getcwd(),failed_filename)
        failed_wrapper = os.path.join(os.getcwd(),wrapper_id)
        Failed = False
        if os.path.exists(completed_path):
            print((datetime.now(), "The job ", pid.template," has been COMPLETED"))
        else:
            open(failed_wrapper, 'w').close()
            open(failed_path, 'w').close()
            print((datetime.now(), "The job ", pid.template," has FAILED"))
                    {os.linesep.ljust(13)}"""), 4)

        return parallel_threads_launcher

    # all should override -> abstract!
    def build_main(self):
        pass

    def dependency_directive(self):
        pass

    def queue_directive(self):
        pass

    def _indent(self, text, amount, ch=' '):
        padding = amount * ch
        return ''.join(padding + line for line in text.splitlines(True))


class PythonVerticalWrapperBuilder(PythonWrapperBuilder):

    def build_sequential_threads_launcher(self, jobs_list: List[str], thread: str, footer: bool = True) -> str:
        """
        Builds a part of the vertical wrapper cmd launcher script.
        This script writes the start and finish time of each inner_job.

        :param jobs_list: List of job scripts.
        :type jobs_list: List[str]
        :param thread: inner_job to be executed.
        :type thread: str
        :param footer: If True, includes the footer in the script. Defaults to True.
        :type footer: bool
        :return: Part of the final vertical wrapper script.
        :rtype: str
        """
        sequential_threads_launcher = textwrap.dedent(f"""
        failed_wrapper = os.path.join(os.getcwd(),wrapper_id)
        retrials = {self.retrials}
        total_steps = 0
        try: 
            print("JOB.ID:"+ os.getenv('SLURM_JOBID'))
        except:
            print("JOB.ID")
        for i in range(len({jobs_list})):
            job_retrials = retrials
            completed = False
            fail_count = 0
            while fail_count <= job_retrials and not completed:
                current = {thread}
                current.start()
                start = int(time.time())
                current.join({str(self.wallclock_by_level)})
                total_steps = total_steps + 1
        {os.linesep.ljust(13)}""")

        if footer:
            sequential_threads_launcher += self._indent(textwrap.dedent(f"""
                completed_filename = {jobs_list}[i].replace('.cmd', '_COMPLETED')
                completed_path = os.path.join(os.getcwd(), completed_filename)
                failed_filename = {jobs_list}[i].replace('.cmd', '_FAILED')
                failed_path = os.path.join(os.getcwd(), failed_filename)
                failed_wrapper = os.path.join(os.getcwd(), wrapper_id)
                finish = int(time.time())
                stat_filename = {jobs_list}[i].replace(".cmd", f"_STAT_{{fail_count}}")
                stat_path_tmp = os.path.join(os.getcwd(),f"{{stat_filename}}.tmp")
                print(f"Completed_file:{{completed_path}}")
                print(f"Writting:{{stat_path_tmp}}")
                print(f"[Start:{{start}}, Finish:{{finish}}, Fail_count:{{fail_count}}]")
                with open(f"{{stat_path_tmp}}", "w") as file:
                    file.write(f"{{start}}\\n")
                    file.write(f"{{finish}}\\n")
                if os.path.exists(completed_path):
                    completed = True
                    print(datetime.now(), "The job ", current.template," has been COMPLETED")
                else:
                    print(datetime.now(), "The job ", current.template," has FAILED")
                    #{self.exit_thread}
                fail_count = fail_count + 1

            {os.linesep.ljust(13)}"""), 8)
            sequential_threads_launcher += self._indent(textwrap.dedent(f"""
            from pathlib import Path
            fail_count = 0 
            while fail_count <= job_retrials:
                try:
                    stat_filename = {jobs_list}[i].replace(".cmd", f"_STAT_{{fail_count}}")
                    stat_path_tmp = os.path.join(os.getcwd(),f"{{stat_filename}}.tmp")
                    Path(stat_path_tmp).replace(stat_path_tmp.replace(".tmp",""))
                except:
                    print(f"Couldn't write the stat file:{{stat_path_tmp}}")
                fail_count = fail_count + 1
            if not os.path.exists(completed_path):
                open(failed_wrapper,'wb').close()
                open(failed_path, 'wb').close()

            if os.path.exists(failed_wrapper):
                os.remove(os.path.join(os.getcwd(),wrapper_id))
                print("WRAPPER_FAILED")
                wrapper_failed = os.path.join(os.getcwd(),"WRAPPER_FAILED")
                open(wrapper_failed, 'wb').close()
                os._exit(1)
        {os.linesep.ljust(13)}"""), 4)
        return sequential_threads_launcher

    def build_job_thread(self):  # fastlook
        return textwrap.dedent(f"""
        class JobThread(Thread):
            def __init__ (self, template, id_run, retrials, fail_count):
                Thread.__init__(self)
                self.template = template
                self.id_run = id_run
                self.retrials = retrials
                self.fail_count = fail_count

            def run(self):
                print("\\n")
                jobname = self.template.replace('.cmd', '')
                out = str(self.template) + ".out." + str(self.fail_count)
                err = str(self.template) + ".err." + str(self.fail_count)
                out_path = os.path.join(os.getcwd(), out)
                err_path = os.path.join(os.getcwd(), err)
                template_path = os.path.join(os.getcwd(), self.template)
                command = f"chmod +x {{template_path}}; timeout {str(self.wallclock_by_level)} {{template_path}} > {{out_path}} 2> {{err_path}}"
                print(command)
                getstatusoutput(command)



        {os.linesep.ljust(13)}""")

    def build_main(self):
        self.exit_thread = "os._exit(1)"
        return self.build_sequential_threads_launcher("scripts", "JobThread(scripts[i], i, retrials, fail_count)")


class PythonHorizontalWrapperBuilder(PythonWrapperBuilder):

    def build_main(self):
        nodelist = self.build_nodes_list()
        # threads_launcher = self.build_parallel_threads_launcher("scripts", "JobThread")
        threads_launcher = self.build_parallel_threads_launcher_horizontal("scripts", "JobThread")
        return nodelist + threads_launcher


class PythonVerticalHorizontalWrapperBuilder(PythonWrapperBuilder):

    def build_joblist_thread(self):
        return textwrap.dedent(f"""
        class JobListThread(Thread):
            def __init__ (self, jobs_list, id_run):
                Thread.__init__(self)
                self.jobs_list = jobs_list
                self.id_run = id_run

            def run(self):
                {self._indent(self.build_sequential_threads_launcher("self.jobs_list", "JobThread(self.jobs_list[i], i)"),
                              8)}
        {os.linesep.ljust(13)}""")

    def build_main(self):
        self.exit_thread = "sys.exit()"
        joblist_thread = self.build_joblist_thread()
        nodes_list = self.build_nodes_list()
        # threads_launcher = self.build_parallel_threads_launcher("scripts", "JobListThread", footer=False)
        threads_launcher = self.build_parallel_threads_launcher_vertical_horizontal("scripts", "JobListThread",
                                                                                    footer=False)

        return joblist_thread + nodes_list + threads_launcher


class PythonHorizontalVerticalWrapperBuilder(PythonWrapperBuilder):
    def build_parallel_threads_launcher_horizontal_vertical(self, jobs_list, thread, footer=True):
        parallel_threads_launcher = textwrap.dedent(f"""
pid_list = []
for i in range(len({jobs_list})):
    print("Starting job ", {jobs_list}[i])
    if type({jobs_list}[i]) != list:
        job = {jobs_list}[i]
        jobname = job.replace(".cmd", '')
        section = jobname.split('_')[-1]

    {self._indent(self.build_machinefiles(), 8)}
    current = {thread}({jobs_list}[i], i+self.id_run)
    pid_list.append(current)
    current.start()

# Waiting until all scripts finish
for i in range(len(pid_list)):
    pid = pid_list[i]
    pid.join()
        {os.linesep.ljust(13)}""")
        if footer:
            parallel_threads_launcher += self._indent(textwrap.dedent(f"""
        completed_filename = {jobs_list}[i].replace('.cmd', '_COMPLETED')
        completed_path = os.path.join(os.getcwd(), completed_filename)
        failed_filename = {jobs_list}[i].replace('.cmd', '_FAILED')
        failed_path = os.path.join(os.getcwd(),failed_filename)
        failed_wrapper = os.path.join(os.getcwd(),wrapper_id)
        if os.path.exists(completed_path):
            print(datetime.now(), "The job ", pid.template," has been COMPLETED")
        else:
            open(failed_wrapper, 'wb').close()
            open(failed_path, 'wb').close()
            print(datetime.now(), "The job ", pid.template," has FAILED")
            {os.linesep.ljust(13)}"""), 4)
        return parallel_threads_launcher

    def build_joblist_thread(self):
        return textwrap.dedent(f"""
        class JobListThread(Thread):
            def __init__ (self, jobs_list, id_run, all_cores):
                Thread.__init__(self)
                self.jobs_list = jobs_list
                self.id_run = id_run
                self.all_cores = all_cores

            def run(self):
                all_cores = self.all_cores
                {self._indent(self.build_parallel_threads_launcher_horizontal_vertical("self.jobs_list", "JobThread"), 8)}
        {os.linesep.ljust(13)}""")

    def build_main(self):
        nodes_list = self.build_nodes_list()
        self.exit_thread = "os._exit(1)"
        joblist_thread = self.build_joblist_thread()
        threads_launcher = self.build_sequential_threads_launcher("scripts",
                                                                  "JobListThread(scripts[i], i*(len(scripts[i])), "
                                                                  "copy.deepcopy(all_cores))", footer=False)
        return joblist_thread + nodes_list + threads_launcher


class BashWrapperBuilder(WrapperBuilder):

    def build_imports(self):
        return ""

    def build_main(self):
        return textwrap.dedent(f"""
        # Initializing variables
        scripts="{' '.join(str(s) for s in self.job_scripts)}"
        i=0
        pids=""
        {os.linesep.ljust(13)}""")

    def build_job_thread(self):
        return textwrap.dedent(f"""
        execute_script()
            {{
                out="$1.out"
                err="$1.err"
                bash $1 > $out 2> $err &
                pid=$!
            }}
        {os.linesep.ljust(13)}""")

    def build_sequential_threads_launcher(self):
        return textwrap.dedent(f"""
        for script in $scripts; do
            execute_script "$SCRATCH/{self.expid}/LOG_{self.expid}/$script" $i
            wait $pid
            if [ $? -eq 0 ]; then
                echo "The job $script has been COMPLETED"
            else
                echo "The job $script has FAILED"
                exit 1
            fi
            i=$((i+1))
        done
        {os.linesep.ljust(13)}""")

    def build_parallel_threads_launcher(self):
        return textwrap.dedent(f"""
        for script in $scripts; do
            execute_script "$SCRATCH/{self.expid}/LOG_{self.expid}/$script" $i
            pids+="$pid "
            i=$((i+1))
        done

        # Waiting until all scripts finish
        for pid in $pids; do
            wait $pid
            if [ $? -eq 0 ]; then
                echo "The job $pid has been COMPLETED"
            else
                echo "The job $pid has FAILED"
            fi
        done
        {os.linesep.ljust(13)}""")


class BashVerticalWrapperBuilder(BashWrapperBuilder):

    def build_main(self):
        return super(BashVerticalWrapperBuilder, self).build_main() + self.build_sequential_threads_launcher()


class BashHorizontalWrapperBuilder(BashWrapperBuilder):

    def build_main(self):
        return super(BashHorizontalWrapperBuilder, self).build_main() + self.build_parallel_threads_launcher()


# SRUN CLASSES
class SrunWrapperBuilder(WrapperBuilder):

    def build_imports(self):
        pass

    # hybrids
    def build_joblist_thread(self):
        pass

    def build_job_thread(self):
        return textwrap.dedent(""" """)

    # horizontal and hybrids
    def build_nodes_list(self):
        return self.get_nodes() + self.build_cores_list()

    def get_nodes(self):
        return textwrap.dedent(f"""
        # Getting the list of allocated nodes
        {self.allocated_nodes}
        os.system("mkdir -p machinefiles")

        with open(f"node_list_{{node_id}}", 'r') as file:
             all_nodes = file.read()
        os.remove(f"node_list_{{node_id}}")

        all_nodes = all_nodes.split("_NEWLINE_")
        if all_nodes[-1] == "":
            all_nodes = all_nodes[:-1]
        print(all_nodes)
        {os.linesep.ljust(13)}""")

    def build_cores_list(self):
        return textwrap.dedent(f"""
total_cores = {self.num_procs_value}
jobs_resources = {str(self.jobs_resources)}
processors_per_node = int(jobs_resources['PROCESSORS_PER_NODE'])
idx = 0
all_cores = []
while total_cores > 0:
    if processors_per_node > 0:
        processors_per_node -= 1
        total_cores -= 1
        all_cores.append(all_nodes[idx])
    else:
        if idx < len(all_nodes)-1:
            idx += 1
        processors_per_node = int(jobs_resources['PROCESSORS_PER_NODE'])

processors_per_node = int(jobs_resources['PROCESSORS_PER_NODE'])
        {os.linesep.ljust(13)}""")

    def build_machinefiles(self):
        machinefile_function = self.get_machinefile_function()
        if machinefile_function:
            return self.get_machinefile_function() + self._indent(self.write_machinefiles(), self.machinefiles_indent)
        return ""

    def build_machinefiles_standard(self):
        return textwrap.dedent(f"""
            machines = str()
            cores = int(jobs_resources[section]['PROCESSORS'])
            tasks = int(jobs_resources[section]['TASKS'])
            nodes = int(ceil(int(cores)/float(tasks)))
            if tasks < processors_per_node:
                cores = tasks
            job_cores = cores
            while nodes > 0:
                while cores > 0:
                    if len(all_cores) > 0:
                        node = all_cores.pop(0)
                        if node:
                            machines += node +"_NEWLINE_"
                            cores -= 1
                for rest in range(processors_per_node-tasks):
                    if len(all_cores) > 0:
                        all_cores.pop(0)
                nodes -= 1
                if tasks < processors_per_node:
                    cores = job_cores
        {os.linesep.ljust(13)}""")

    def _create_components_dict(self):
        return textwrap.dedent(f"""
        xio_procs = int(jobs_resources[section]['COMPONENTS']['XIO_NUMPROC'])
        rnf_procs = int(jobs_resources[section]['COMPONENTS']['RNF_NUMPROC'])
        ifs_procs = int(jobs_resources[section]['COMPONENTS']['IFS_NUMPROC'])
        nem_procs = int(jobs_resources[section]['COMPONENTS']['NEM_NUMPROC'])

        components = OrderedDict([
            ('XIO', xio_procs),
            ('RNF', rnf_procs),
            ('IFS', ifs_procs),
            ('NEM', nem_procs)
        ])

        jobs_resources[section]['COMPONENTS'] = components
        {os.linesep.ljust(13)}""")

    def build_machinefiles_components(self):
        return textwrap.dedent(f"""
        {self._create_components_dict()}

        machines = str()
        for component, cores in jobs_resources[section]['COMPONENTS'].items():        
            while cores > 0:
                if len(all_cores) > 0:
                    node = all_cores.pop(0)
                    if node:
                        machines += node +"_NEWLINE_"
                        cores -= 1
        {os.linesep.ljust(13)}""")

    def write_machinefiles(self):
        return textwrap.dedent(f"""
        machines = "_NEWLINE_".join([s for s in machines.split("_NEWLINE_") if s])
        with open("machinefiles/machinefile_"+{self.machinefiles_name}, "w") as machinefile:
            machinefile.write(machines)
        {os.linesep.ljust(13)}""")

    def build_srun_launcher(self, jobs_list, footer=True):
        pass

    # all should override -> abstract!
    def build_main(self):
        pass

    def dependency_directive(self):
        pass

    def queue_directive(self):
        pass

    def _indent(self, text, amount, ch=' '):
        padding = amount * ch
        return ''.join(padding + line for line in text.splitlines(True))


class SrunHorizontalWrapperBuilder(SrunWrapperBuilder):
    def build_imports(self):
        scripts_bash = "("
        for script in self.job_scripts:
            scripts_bash += str("\"" + script + "\"") + " "
        scripts_bash += ")"
        return textwrap.dedent(f"""
        # Defining scripts to be run
        declare -a scripts={str(scripts_bash)}
        {os.linesep.ljust(13)}""")

    def build_srun_launcher(self, jobs_list, footer=True):
        srun_launcher = textwrap.dedent(f"""
        i=0
        suffix=".cmd"
        for template in "${{{jobs_list}[@]}}"; do
            jobname=${{template%"$suffix"}}
            out="${{template}}.out" 
            err="${{template}}.err"
            srun --ntasks=1 --cpus-per-task={self.threads} $template > $out 2> $err &
            sleep "0.2"
            ((i=i+1))
        done
        wait
        {os.linesep.ljust(13)}""")
        if footer:
            srun_launcher += self._indent(textwrap.dedent(f"""
        for template in "${{{jobs_list}[@]}}"; do
            suffix_completed=".COMPLETED"
            completed_filename=${{template%"$suffix"}}
            completed_filename="$completed_filename"_COMPLETED
            completed_path=${{PWD}}/$completed_filename
            if [ -f "$completed_path" ];
            then
                echo "`date '+%d/%m/%Y_%H:%M:%S'` $template has been COMPLETED"
            else
                echo "`date '+%d/%m/%Y_%H:%M:%S'` $template has FAILED" 
            fi
        done
            {os.linesep.ljust(13)}"""), 0)
        return srun_launcher

    def build_main(self):
        nodelist = self.build_nodes_list()
        srun_launcher = self.build_srun_launcher("scripts")
        return nodelist, srun_launcher


class SrunVerticalHorizontalWrapperBuilder(SrunWrapperBuilder):
    def build_imports(self):
        scripts_bash = textwrap.dedent("""
        # Defining scripts to be run""")
        list_index = 0
        scripts_array_vars = "( "
        scripts_array_index = "( "
        for scripts in self.job_scripts:
            built_array = "("
            for script in scripts:
                built_array += str("\"" + script + "\"") + " "
            built_array += ")"
            scripts_bash += textwrap.dedent(f"""
            declare -a scripts_{str(list_index)}={str(built_array)}
            {os.linesep.ljust(13)}""")
            scripts_array_vars += f"\"scripts_{list_index}\" "
            scripts_array_index += f"\"{list_index}\" "
            list_index += 1
        scripts_array_vars += ")"
        scripts_array_index += ")"
        scripts_bash += textwrap.dedent(f"""
                   declare -a scripts_list={str(scripts_array_vars)}
                   declare -a scripts_index={str(scripts_array_index)}
                   {os.linesep.ljust(13)}""")

        total_threads = float(len(self.job_scripts))
        n_threads = float(self.threads)
        core = []
        for thread in range(int(n_threads)):
            core.append(0x0)

        core[0] = 0x1
        horizontal_wrapper_size = int(total_threads)
        srun_mask_values = []
        for job_id in range(horizontal_wrapper_size):
            for thread in range(1, int(n_threads)):
                core[thread] = core[thread - 1] * 2
            job_mask = 0x0
            for thr_mask in core:
                job_mask = job_mask + thr_mask
            srun_mask_values.append(str(hex(job_mask)))
            if job_id > 0:
                core[0] = core[0] << int(n_threads)
            else:
                core[0] = job_mask + 0x1

        mask_array = "( "
        for mask in srun_mask_values:
            mask_array += str("\"" + mask + "\"") + " "
        mask_array += ")"
        scripts_bash += textwrap.dedent(f"""
                declare -a job_mask_array={mask_array}
                {os.linesep.ljust(13)}""")

        return scripts_bash

    def build_srun_launcher(self, jobs_list, footer=True):
        srun_launcher = textwrap.dedent(f"""
        suffix=".cmd"
        suffix_completed=".COMPLETED"
        aux_scripts=("${{{jobs_list}[@]}}")
        prev_script="empty"
        as_index=0
        horizontal_size=${{#scripts_index[@]}}
        scripts_size=${{#scripts_0[@]}}
        while [ "${{#aux_scripts[@]}}" -gt 0 ]; do
            i_list=0
            for script_list in "${{{jobs_list}[@]}}"; do
                declare -i job_index=${{scripts_index[$i_list]}}
                declare -n scripts=$script_list

                declare -n prev_horizontal_scripts=$prev_script
                if [ $job_index -ne -1 ]; then
                    for horizontal_job in "${{scripts[@]:$job_index}}"; do
                        template=$horizontal_job
                        jobname=${{template%"$suffix"}}
                        as_index=0
                        multiplication_result=$(($i_list*$scripts_size))
                        as_index=$((multiplication_result+$job_index))
                        out="${{template}}.out"
                        err="${{template}}.err"
                        if [ $job_index -eq 0 ]; then
                            prev_template=$template
                        else
                            #prev_template=${{prev_horizontal_scripts[$job_index]}}
                            prev_template=${{scripts[((job_index-1))]}}
                        fi
                        completed_filename=${{prev_template%"$suffix"}}
                        completed_filename="$completed_filename"_COMPLETED
                        completed_path=${{PWD}}/$completed_filename
                        if [ $job_index -eq 0 ] || [ -f "$completed_path" ]; then #If first horizontal wrapper or last wrapper is completed
                            srun -N1 --ntasks=1 --cpus-per-task={self.threads} --cpu-bind=verbose,mask_cpu:job_mask_array[$job_index]  --distribution=block:block $template > $out 2> $err &
                            job_index=$(($job_index+1))

                        else
                            break
                        fi
                    done
                    if [ $job_index -ge "${{#scripts[@]}}" ];  then
                        unset aux_scripts[$i_list]
                        job_index=-1
                    fi
                fi
                prev_script=("${{script_list[@]}}")
                scripts_index[$i_list]=$job_index
                i_list=$((i_list+1)) # check next list ( needed for save list index )
            done
        done
        wait
        {os.linesep.ljust(13)}""")

        return srun_launcher

    def build_main(self):
        nodelist = self.build_nodes_list()
        srun_launcher = self.build_srun_launcher("scripts_list")
        return nodelist, srun_launcher
