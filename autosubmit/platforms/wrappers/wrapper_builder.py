#!/usr/bin/env python3

# Copyright 2015-2020 Earth Sciences Department, BSC-CNS

# This file is part of Autosubmit.

# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

import random
import string
import textwrap

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
        #if "bash" not in header[0:15]:

        main = self._builder.build_main()
        #else:
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
        if "wallclock_by_level" in list(kwargs.keys()):
            self.wallclock_by_level = kwargs['wallclock_by_level']

    def build_header(self):
        return textwrap.dedent(self.header_directive) + self.build_imports()

    def build_imports(self):
        pass
    def build_job_thread(self):
        pass
    # hybrids
    def build_joblist_thread(self, **kwargs):
        pass
    # horizontal and hybrids
    def build_nodes_list(self):
        pass
    def build_machinefiles(self):
        pass
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
        pass
    def build_machinefiles_components(self):
        pass
    def build_machinefiles_components_alternate(self):
        pass
    def build_sequential_threads_launcher(self, **kwargs):
        pass
    def build_parallel_threads_launcher(self, **kwargs):
        pass
    # all should override -> abstract!
    def build_main(self):
        pass
    def _indent(self, text, amount, ch=' '):
        padding = amount * ch
        return ''.join(padding + line for line in text.splitlines(True))
class PythonWrapperBuilder(WrapperBuilder):
    def get_random_alphanumeric_string(self,letters_count, digits_count):
        sample_str = ''.join((random.choice(string.ascii_letters) for i in range(letters_count)))
        sample_str += ''.join((random.choice(string.digits) for i in range(digits_count)))

        # Convert string to list and shuffle it to mix letters and digits
        sample_list = list(sample_str)
        random.shuffle(sample_list)
        final_string = ''.join(sample_list)
        return final_string
    def build_imports(self):

        return textwrap.dedent("""
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
        node_id = "{1}"
        wrapper_id = "{1}_FAILED"
        # Defining scripts to be run
        scripts= {0}
        """).format(str(self.job_scripts), self.get_random_alphanumeric_string(5,5),'\n'.ljust(13))

    def build_job_thread(self):
        return textwrap.dedent("""
        class JobThread(Thread):
            def __init__ (self, template, id_run):
                Thread.__init__(self)
                self.template = template
                self.id_run = id_run

            def run(self):
                jobname = self.template.replace('.cmd', '')
                out = str(self.template) + ".out." + str(0)
                err = str(self.template) + ".err." + str(0)
                print(out+"\\n")
                command = "./" + str(self.template) + " " + str(self.id_run) + " " + os.getcwd()
                (self.status) = getstatusoutput(command + " > " + out + " 2> " + err)
        """).format('\n'.ljust(13))

    # hybrids
    def build_joblist_thread(self):
        pass

    # horizontal and hybrids
    def build_nodes_list(self):
        return self.get_nodes() + self.build_cores_list()

    def get_nodes(self):
        return textwrap.dedent("""
        # Getting the list of allocated nodes
        {0}
        os.system("mkdir -p machinefiles")

        with open("node_list_{{0}}".format(node_id), 'r') as file:
             all_nodes = file.read()
        os.remove("node_list_{{0}}".format(node_id))


        all_nodes = all_nodes.split("_NEWLINE_")
        if all_nodes[-1] == "":
            all_nodes = all_nodes[:-1]
        print(all_nodes)
        """).format(self.allocated_nodes, '\n'.ljust(13))

    def build_cores_list(self):
        return textwrap.dedent("""
total_cores = {0}
jobs_resources = {1}
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

        """).format(self.num_procs_value, str(self.jobs_resources), '\n'.ljust(13))

    def build_machinefiles(self):
        machinefile_function = self.get_machinefile_function()
        if machinefile_function:
            return self.get_machinefile_function() + self._indent(self.write_machinefiles(), self.machinefiles_indent)
        return ""

    def build_machinefiles_standard(self):
        return textwrap.dedent("""
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
        """).format('\n'.ljust(13))

    def _create_components_dict(self):
        return textwrap.dedent("""
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
        """).format('\n'.ljust(13))

    def build_machinefiles_components(self):
        return textwrap.dedent("""
        {0}
        
        machines = str()
        for component, cores in jobs_resources[section]['COMPONENTS'].items():        
            while cores > 0:
                if len(all_cores) > 0:
                    node = all_cores.pop(0)
                    if node:
                        machines += node +"_NEWLINE_"
                        cores -= 1
        """).format(self._create_components_dict(), '\n'.ljust(13))

    def write_machinefiles(self):
        return textwrap.dedent("""
        machines = "_NEWLINE_".join([s for s in machines.split("_NEWLINE_") if s])
        with open("machinefiles/machinefile_"+{0}, "w") as machinefile:
            machinefile.write(machines)
        """).format(self.machinefiles_name, '\n'.ljust(13))

    def build_sequential_threads_launcher(self, jobs_list, thread, footer=True):
        sequential_threads_launcher = textwrap.dedent("""
        failed_wrapper = os.path.join(os.getcwd(),wrapper_id)
        for i in range(len({0})):
            current = {1}
            current.start()
            current.join()
        """).format(jobs_list, thread, '\n'.ljust(13))

        if footer:
            sequential_threads_launcher += self._indent(textwrap.dedent("""
            completed_filename = {0}[i].replace('.cmd', '_COMPLETED')
            completed_path = os.path.join(os.getcwd(), completed_filename)
            failed_filename = {0}[i].replace('.cmd', '_FAILED')
            failed_path = os.path.join(os.getcwd(), failed_filename)
            failed_wrapper = os.path.join(os.getcwd(), wrapper_id)
            if os.path.exists(completed_path):
                print(datetime.now(), "The job ", current.template," has been COMPLETED")
            else:
                open(failed_wrapper,'w').close()
                open(failed_path, 'w').close()
                print(datetime.now(), "The job ", current.template," has FAILED")

                #{1}
            """).format(jobs_list, self.exit_thread, '\n'.ljust(13)), 4)
            sequential_threads_launcher += self._indent(textwrap.dedent("""
                if os.path.exists(failed_wrapper):
                    os.remove(os.path.join(os.getcwd(),wrapper_id))
                    wrapper_failed = os.path.join(os.getcwd(),"WRAPPER_FAILED")
                    open(wrapper_failed, 'w').close()
                    os._exit(1)

            """).format(jobs_list, self.exit_thread, '\n'.ljust(13)), 4)
        else:
            sequential_threads_launcher += self._indent(textwrap.dedent("""
                if os.path.exists(failed_wrapper):
                    os.remove(os.path.join(os.getcwd(),wrapper_id))
                    wrapper_failed = os.path.join(os.getcwd(),"WRAPPER_FAILED")
                    open(wrapper_failed, 'wb').close()
                    os._exit(1)

            """).format(jobs_list, self.exit_thread, '\n'.ljust(13)), 4)
        return sequential_threads_launcher

    def build_parallel_threads_launcher(self, jobs_list, thread, footer=True):
        parallel_threads_launcher = textwrap.dedent("""
pid_list = []
for i in range(len({0})):
    print("Starting job ", {0}[i])
    if type({0}[i]) != list:
        job = {0}[i]
        jobname = job.replace(".cmd", '')
        section = jobname.split('_')[-1]
    {2}
    current = {1}({0}[i], i+self.id_run)
    pid_list.append(current)
    current.start()
# Waiting until all scripts finish
for i in range(len(pid_list)):
    pid = pid_list[i]
    pid.join()
        """).format(jobs_list, thread, self._indent(self.build_machinefiles(), 8), '\n'.ljust(13))
        if footer:
            parallel_threads_launcher += self._indent(textwrap.dedent("""
        completed_filename = {0}[i].replace('.cmd', '_COMPLETED')
        completed_path = os.path.join(os.getcwd(), completed_filename)
        failed_filename = {0}[i].replace('.cmd', '_FAILED')
        failed_path = os.path.join(os.getcwd(),failed_filename)
        failed_wrapper = os.path.join(os.getcwd(),wrapper_id)
        if os.path.exists(completed_path):
            print(datetime.now(), "The job ", pid.template," has been COMPLETED")
        else:
            open(failed_wrapper, 'w').close()
            open(failed_path, 'w').close()
            print(datetime.now(), "The job ", pid.template," has FAILED")
                    """).format(jobs_list, self.exit_thread, '\n'.ljust(13)), 4)
        return parallel_threads_launcher
    def build_parallel_threads_launcher_horizontal(self, jobs_list, thread, footer=True):
        parallel_threads_launcher = textwrap.dedent("""
pid_list = []
for i in range(len({0})):
    print("Starting job ", {0}[i])

    if type({0}[i]) != list:
        job = {0}[i]
        jobname = job.replace(".cmd", '')
        section = jobname.split('_')[-1]

    {2}
    current = {1}({0}[i], i)
    pid_list.append(current)
    current.start()

# Waiting until all scripts finish
for i in range(len(pid_list)):
    pid = pid_list[i]
    pid.join()
        """).format(jobs_list, thread, self._indent(self.build_machinefiles(), 8), '\n'.ljust(13))
        if footer:
            parallel_threads_launcher += self._indent(textwrap.dedent("""
        completed_filename = {0}[i].replace('.cmd', '_COMPLETED')
        completed_path = os.path.join(os.getcwd(), completed_filename)
        failed_filename = {0}[i].replace('.cmd', '_FAILED')
        failed_path = os.path.join(os.getcwd(),failed_filename)
        failed_wrapper = os.path.join(os.getcwd(),wrapper_id)
        Failed = False
        if os.path.exists(completed_path):
            print((datetime.now(), "The job ", pid.template," has been COMPLETED"))
        else:
            open(failed_wrapper, 'w').close()
            open(failed_path, 'w').close()
            print((datetime.now(), "The job ", pid.template," has FAILED"))
                    """).format(jobs_list, self.exit_thread, '\n'.ljust(13)), 4)

        return parallel_threads_launcher
    def build_parallel_threads_launcher_vertical_horizontal(self, jobs_list, thread, footer=True):
        parallel_threads_launcher = textwrap.dedent("""
pid_list = []
for i in range(len({0})):
    print("Starting job ", {0}[i])

    if type({0}[i]) != list:
        job = {0}[i]
        jobname = job.replace(".cmd", '')
        section = jobname.split('_')[-1]

    {2}
    current = {1}({0}[i], i)
    pid_list.append(current)
    current.start()

# Waiting until all scripts finish
for i in range(len(pid_list)):
    pid = pid_list[i]
    pid.join()
        """).format(jobs_list, thread, self._indent(self.build_machinefiles(), 8), '\n'.ljust(13))
        if footer:
            parallel_threads_launcher += self._indent(textwrap.dedent("""
        completed_filename = {0}[i].replace('.cmd', '_COMPLETED')
        completed_path = os.path.join(os.getcwd(), completed_filename)
        failed_filename = {0}[i].replace('.cmd', '_FAILED')
        failed_path = os.path.join(os.getcwd(),failed_filename)
        failed_wrapper = os.path.join(os.getcwd(),wrapper_id)
        Failed = False
        if os.path.exists(completed_path):
            print((datetime.now(), "The job ", pid.template," has been COMPLETED"))
        else:
            open(failed_wrapper, 'w').close()
            open(failed_path, 'w').close()
            print((datetime.now(), "The job ", pid.template," has FAILED"))
                    """).format(jobs_list, self.exit_thread, '\n'.ljust(13)), 4)

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
        Args:
            jobs_list (List[str]): List of job scripts.
            thread (str): inner_job to be executed.
            footer (bool): If True, includes the footer in the script. Defaults to True.

        Returns:
            str: Part of the final vertical wrapper script.
        """
        sequential_threads_launcher = textwrap.dedent("""
        failed_wrapper = os.path.join(os.getcwd(),wrapper_id)
        retrials = {2}
        total_steps = 0
        try: 
            print("JOB.ID:"+ os.getenv('SLURM_JOBID'))
        except:
            print("JOB.ID")
        for i in range(len({0})):
            job_retrials = retrials
            completed = False
            fail_count = 0
            while fail_count <= job_retrials and not completed:
                current = {1}
                current.start()
                start = int(time.time())
                current.join({3})
                total_steps = total_steps + 1
        """).format(jobs_list, thread, self.retrials, str(self.wallclock_by_level),'\n'.ljust(13))

        if footer:
            sequential_threads_launcher += self._indent(textwrap.dedent("""
                completed_filename = {0}[i].replace('.cmd', '_COMPLETED')
                completed_path = os.path.join(os.getcwd(), completed_filename)
                failed_filename = {0}[i].replace('.cmd', '_FAILED')
                failed_path = os.path.join(os.getcwd(), failed_filename)
                failed_wrapper = os.path.join(os.getcwd(), wrapper_id)
                finish = int(time.time())
                stat_filename = {0}[i].replace(".cmd", f"_STAT_{{fail_count}}")
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
                    #{1}
                fail_count = fail_count + 1

            """).format(jobs_list, self.exit_thread, '\n'.ljust(13)), 8)
            sequential_threads_launcher += self._indent(textwrap.dedent("""
            from pathlib import Path
            fail_count = 0 
            while fail_count <= job_retrials:
                try:
                    stat_filename = {0}[i].replace(".cmd", f"_STAT_{{fail_count}}")
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
            """).format(jobs_list, self.exit_thread, '\n'.ljust(13)), 4)
        return sequential_threads_launcher

    def build_job_thread(self): # fastlook
        return textwrap.dedent("""
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
                command = f"chmod +x {{template_path}}; timeout {0} {{template_path}} > {{out_path}} 2> {{err_path}}"
                print(command)
                getstatusoutput(command)
                

                
        """).format(str(self.wallclock_by_level),'\n'.ljust(13))
    def build_main(self):
        self.exit_thread = "os._exit(1)"
        return self.build_sequential_threads_launcher("scripts", "JobThread(scripts[i], i, retrials, fail_count)")
class PythonHorizontalWrapperBuilder(PythonWrapperBuilder):

    def build_main(self):
        nodelist = self.build_nodes_list()
        #threads_launcher = self.build_parallel_threads_launcher("scripts", "JobThread")
        threads_launcher = self.build_parallel_threads_launcher_horizontal("scripts", "JobThread")
        return nodelist + threads_launcher
class PythonVerticalHorizontalWrapperBuilder(PythonWrapperBuilder):

    def build_joblist_thread(self):
        return textwrap.dedent("""
        class JobListThread(Thread):
            def __init__ (self, jobs_list, id_run):
                Thread.__init__(self)
                self.jobs_list = jobs_list
                self.id_run = id_run

            def run(self):
                {0}
        """).format(
            self._indent(self.build_sequential_threads_launcher("self.jobs_list", "JobThread(self.jobs_list[i], i)"),
                         8), '\n'.ljust(13))

    def build_main(self):
        self.exit_thread = "sys.exit()"
        joblist_thread = self.build_joblist_thread()
        nodes_list = self.build_nodes_list()
        #threads_launcher = self.build_parallel_threads_launcher("scripts", "JobListThread", footer=False)
        threads_launcher = self.build_parallel_threads_launcher_vertical_horizontal("scripts", "JobListThread", footer=False)

        return joblist_thread + nodes_list + threads_launcher
class PythonHorizontalVerticalWrapperBuilder(PythonWrapperBuilder):
    def build_parallel_threads_launcher_horizontal_vertical(self, jobs_list, thread, footer=True):
        parallel_threads_launcher = textwrap.dedent("""
pid_list = []
for i in range(len({0})):
    print("Starting job ", {0}[i])
    if type({0}[i]) != list:
        job = {0}[i]
        jobname = job.replace(".cmd", '')
        section = jobname.split('_')[-1]

    {2}
    current = {1}({0}[i], i+self.id_run)
    pid_list.append(current)
    current.start()

# Waiting until all scripts finish
for i in range(len(pid_list)):
    pid = pid_list[i]
    pid.join()
        """).format(jobs_list, thread, self._indent(self.build_machinefiles(), 8), '\n'.ljust(13))
        if footer:
            parallel_threads_launcher += self._indent(textwrap.dedent("""
        completed_filename = {0}[i].replace('.cmd', '_COMPLETED')
        completed_path = os.path.join(os.getcwd(), completed_filename)
        failed_filename = {0}[i].replace('.cmd', '_FAILED')
        failed_path = os.path.join(os.getcwd(),failed_filename)
        failed_wrapper = os.path.join(os.getcwd(),wrapper_id)
        if os.path.exists(completed_path):
            print(datetime.now(), "The job ", pid.template," has been COMPLETED")
        else:
            open(failed_wrapper, 'wb').close()
            open(failed_path, 'wb').close()
            print(datetime.now(), "The job ", pid.template," has FAILED")
                    """).format(jobs_list, self.exit_thread, '\n'.ljust(13)), 4)
        return parallel_threads_launcher
    def build_joblist_thread(self):
        return textwrap.dedent("""
        class JobListThread(Thread):
            def __init__ (self, jobs_list, id_run, all_cores):
                Thread.__init__(self)
                self.jobs_list = jobs_list
                self.id_run = id_run
                self.all_cores = all_cores

            def run(self):
                all_cores = self.all_cores
                {0}
        """).format(
            self._indent(self.build_parallel_threads_launcher_horizontal_vertical("self.jobs_list", "JobThread"), 8), '\n'.ljust(13))

    def build_main(self):
        nodes_list = self.build_nodes_list()
        self.exit_thread = "os._exit(1)"
        joblist_thread = self.build_joblist_thread()
        threads_launcher = self.build_sequential_threads_launcher("scripts", "JobListThread(scripts[i], i*(len(scripts[i])), "
                                                                             "copy.deepcopy(all_cores))", footer=False)
        return joblist_thread + nodes_list + threads_launcher
class BashWrapperBuilder(WrapperBuilder):

    def build_imports(self):
        return ""

    def build_main(self):

        return textwrap.dedent("""
        # Initializing variables
        scripts="{0}"
        i=0
        pids=""
        """).format(' '.join(str(s) for s in self.job_scripts), '\n'.ljust(13))

    def build_job_thread(self):
        return textwrap.dedent("""
        execute_script()
            {{
                out="$1.out"
                err="$1.err"
                bash $1 > $out 2> $err &
                pid=$!
            }}
        """).format('\n'.ljust(13))

    def build_sequential_threads_launcher(self):
        return textwrap.dedent("""
        for script in $scripts; do
            execute_script "$SCRATCH/{0}/LOG_{0}/$script" $i
            wait $pid
            if [ $? -eq 0 ]; then
                echo "The job $script has been COMPLETED"
            else
                echo "The job $script has FAILED"
                exit 1
            fi
            i=$((i+1))
        done
        """).format(self.expid, '\n'.ljust(13))

    def build_parallel_threads_launcher(self):
        return textwrap.dedent("""
        for script in $scripts; do
            execute_script "$SCRATCH/{0}/LOG_{0}/$script" $i
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
        """).format(self.expid, '\n'.ljust(13))
class BashVerticalWrapperBuilder(BashWrapperBuilder):

    def build_main(self):
        return super(BashVerticalWrapperBuilder, self).build_main() + self.build_sequential_threads_launcher()
class BashHorizontalWrapperBuilder(BashWrapperBuilder):

    def build_main(self):
        return super(BashHorizontalWrapperBuilder, self).build_main() + self.build_parallel_threads_launcher()
#SRUN CLASSES
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

        return textwrap.dedent("""
        # Getting the list of allocated nodes
        {0}
        os.system("mkdir -p machinefiles")

        with open("node_list_{{0}}".format(node_id), 'r') as file:
             all_nodes = file.read()
        os.remove("node_list_{{0}}".format(node_id))

        all_nodes = all_nodes.split("_NEWLINE_")
        if all_nodes[-1] == "":
            all_nodes = all_nodes[:-1]
        print(all_nodes)
        """).format(self.allocated_nodes, '\n'.ljust(13))

    def build_cores_list(self):
        return textwrap.dedent("""
total_cores = {0}
jobs_resources = {1}
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
        """).format(self.num_procs_value, str(self.jobs_resources), '\n'.ljust(13))

    def build_machinefiles(self):
        machinefile_function = self.get_machinefile_function()
        if machinefile_function:
            return self.get_machinefile_function() + self._indent(self.write_machinefiles(), self.machinefiles_indent)
        return ""

    def build_machinefiles_standard(self):
        return textwrap.dedent("""
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
        """).format('\n'.ljust(13))

    def _create_components_dict(self):
        return textwrap.dedent("""
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
        """).format('\n'.ljust(13))

    def build_machinefiles_components(self):
        return textwrap.dedent("""
        {0}

        machines = str()
        for component, cores in jobs_resources[section]['COMPONENTS'].items():        
            while cores > 0:
                if len(all_cores) > 0:
                    node = all_cores.pop(0)
                    if node:
                        machines += node +"_NEWLINE_"
                        cores -= 1
        """).format(self._create_components_dict(), '\n'.ljust(13))

    def write_machinefiles(self):
        return textwrap.dedent("""
        machines = "_NEWLINE_".join([s for s in machines.split("_NEWLINE_") if s])
        with open("machinefiles/machinefile_"+{0}, "w") as machinefile:
            machinefile.write(machines)
        """).format(self.machinefiles_name, '\n'.ljust(13))

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
            scripts_bash+=str("\""+script+"\"")+" "
        scripts_bash += ")"
        return textwrap.dedent("""
        # Defining scripts to be run
        declare -a scripts={0}
        """).format(str(scripts_bash), '\n'.ljust(13))

    def build_srun_launcher(self, jobs_list, footer=True):
        srun_launcher = textwrap.dedent("""
        i=0
        suffix=".cmd"
        for template in "${{{0}[@]}}"; do
            jobname=${{template%"$suffix"}}
            out="${{template}}.out" 
            err="${{template}}.err"
            srun --ntasks=1 --cpus-per-task={1} $template > $out 2> $err &
            sleep "0.2"
            ((i=i+1))
        done
        wait
        """).format(jobs_list, self.threads, '\n'.ljust(13))
        if footer:
            srun_launcher += self._indent(textwrap.dedent("""
        for template in "${{{0}[@]}}"; do
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
            """).format(jobs_list, self.exit_thread, '\n'.ljust(13)),0)
        return srun_launcher

    def build_main(self):
        nodelist = self.build_nodes_list()
        srun_launcher = self.build_srun_launcher("scripts")
        return nodelist, srun_launcher
class SrunVerticalHorizontalWrapperBuilder(SrunWrapperBuilder):
    def build_imports(self):
        scripts_bash = textwrap.dedent("""
        # Defining scripts to be run""")
        list_index=0
        scripts_array_vars = "( "
        scripts_array_index = "( "
        for scripts in self.job_scripts:
            built_array = "("
            for script in scripts:
                built_array+= str("\"" + script + "\"") + " "
            built_array += ")"
            scripts_bash+=textwrap.dedent("""
            declare -a scripts_{0}={1}
            """).format(str(list_index),str(built_array), '\n'.ljust(13))
            scripts_array_vars += "\"scripts_{0}\" ".format(list_index)
            scripts_array_index += "\"0\" ".format(list_index)
            list_index += 1
        scripts_array_vars += ")"
        scripts_array_index += ")"
        scripts_bash += textwrap.dedent("""
                   declare -a scripts_list={0}
                   declare -a scripts_index={1}
                   """).format(str(scripts_array_vars),str(scripts_array_index), '\n'.ljust(13))

        total_threads = float(len(self.job_scripts))
        n_threads = float(self.threads)
        core = []
        for thread in range(int(n_threads)):
            core.append(0x0)

        core[0] = 0x1
        horizontal_wrapper_size=int(total_threads)
        srun_mask_values = []
        for job_id in range(horizontal_wrapper_size):
            for thread in range(1, int(n_threads)):
                core[thread] = core[thread-1]*2
            job_mask = 0x0
            for thr_mask in core:
                job_mask = job_mask + thr_mask
            srun_mask_values.append(str(hex(job_mask)))
            if job_id > 0:
                    core[0]=core[0] << int(n_threads)
            else:
                    core[0]=job_mask+0x1

        mask_array = "( "
        for mask in srun_mask_values:
            mask_array += str("\"" + mask + "\"") + " "
        mask_array += ")"
        scripts_bash += textwrap.dedent("""
                declare -a job_mask_array={0}
                """).format(mask_array, '\n'.ljust(13))

        return scripts_bash



    def build_srun_launcher(self, jobs_list, footer=True):
        srun_launcher = textwrap.dedent("""
        suffix=".cmd"
        suffix_completed=".COMPLETED"
        aux_scripts=("${{{0}[@]}}")
        prev_script="empty"
        as_index=0
        horizontal_size=${{#scripts_index[@]}}
        scripts_size=${{#scripts_0[@]}}
        while [ "${{#aux_scripts[@]}}" -gt 0 ]; do
            i_list=0
            for script_list in "${{{0}[@]}}"; do
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
                            srun -N1 --ntasks=1 --cpus-per-task={1} --cpu-bind=verbose,mask_cpu:job_mask_array[$job_index]  --distribution=block:block $template > $out 2> $err &
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
        """).format(jobs_list, self.threads, '\n'.ljust(13))

        return srun_launcher

    def build_main(self):
        nodelist = self.build_nodes_list()
        srun_launcher = self.build_srun_launcher("scripts_list")
        return nodelist, srun_launcher
