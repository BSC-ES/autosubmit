#!/usr/bin/env python3

# Copyright 2017-2020 Earth Sciences Department, BSC-CNS

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

import textwrap
from typing import TYPE_CHECKING, Union

from autosubmit.log.log import Log

if TYPE_CHECKING:
    from autosubmit.job.job import Job


def check_directive(directive: str, job_parameters: Union[dict, list, str], het: int = -1) -> bool:
    """
    Returns if directive the directive exists and has value

    :param directive: Which directive needs to be found
    :type directive: str
    :param job_parameters: dict, list, or str that needs to be validated if directive exists in order to be accessed
    :type job_parameters: Union[dict, list, str]
    :param het: Value of the interation in which the value will be validated for specific directive
    :type het: int
    :return: queue directive
    :rtype: bool
    """
    if het > -1 and directive in job_parameters:
        if (isinstance(job_parameters[directive], dict) and het in job_parameters[directive]
                and job_parameters[directive][het] != '' and job_parameters[directive][het] != '0'
                and job_parameters[directive][het] != '1'):
            return True
    else:
        if (job_parameters is not None and directive in job_parameters and job_parameters[directive] != ''
                and job_parameters[directive] != '0' and job_parameters[directive] != '1'):
            return True
    return False


class SlurmHeader(object):
    """Class to handle the SLURM headers of a job"""

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_queue_directive(self, job: 'Job', parameters: dict = None, het=-1):
        """
        Returns queue directive for the specified job

        :param job: job to create directive `QUEUE` for SLURM HEADER
        :type job: Job
        :param parameters: set of values found in the config files used to generate the values of the SLURM HEADER
        :type parameters: dict
        :param het: int
        :return: `CURRENT_QUEUE` directive
        :rtype: str
        """
        # There is no queue, so directive is empty
        if check_directive('CURRENT_QUEUE', job.het, het):
            return f"SBATCH --qos={job.het['CURRENT_QUEUE'][het]}"
        elif check_directive('CURRENT_QUEUE', job.het):
            return f"SBATCH --qos={job.het['CURRENT_QUEUE']}"
        if check_directive('CURRENT_QUEUE', parameters):
            return f"SBATCH --qos={parameters['CURRENT_QUEUE']}"
        Log.warning(f"No QUEUE was found for the JOB: {job.name}")
        return ""

    def get_processors_directive(self, job: 'Job', het: int = -1) -> str:
        """
        Returns processors directive for the specified job

        Since for LUMI platform it is mandatory the nodes to be set, it forces to use either Nodes or Processor
        this way ensuring that it wont ever be empty unless both variables are not set.

        :param job: job to create directive `PROCESSORS` for SLURM HEADER
        :type job: Job
        :param het: Value of the interation in which the value will be validated for specific directive
        :type het: int
        :return: `NODES` or `PROCESSORS` directive
        :rtype: str
        """
        job_nodes = 0
        if het > -1 and 'NODES' in job.het and check_directive('NODES', job.het, het):
            job_nodes = job.het['NODES'][het]
        if job_nodes <= 1 and 'PROCESSORS' in job.het and check_directive('PROCESSORS', job.het, het):
            return f"SBATCH -n {job.het['PROCESSORS'][het]}"
        if job.nodes != "":
            job_nodes = job.nodes
        if (job.processors != '' or job.processors != '0' or job.processors != '1') and int(job_nodes) < 1:
            return f"SBATCH -n {job.processors}"
        return ""

    def get_partition_directive(self, job: 'Job', het: int = -1) -> str:
        """
        Returns partition directive for the specified job

        :param job: job to create directive `PARTITION` for SLURM HEADER
        :type job: Job
        :param het: Value of the interation in which the value will be validated for specific directive
        :type het: int
        :return: `PARTITION` directive
        :rtype: str
        """
        if check_directive('PARTITION', job.het, het):
            return f"SBATCH --partition={job.het['PARTITION'][het]}"
        if check_directive('PARTITION', job.het, -1):
            return f"SBATCH --partition={job.het['PARTITION']}"
        elif job.partition != '':
            return f"SBATCH --partition={job.partition}"
        return ""

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_account_directive(self, job: 'Job', parameters: dict = None, het=-1) -> str:
        """
        Returns account directive for the specified job

        :param job: job to create directive `CURRENT_PROJ` for SLURM HEADER
        :type job: Job
        :param parameters: set of values found in the config files used to generate the values of the SLURM HEADER
        :type parameters: dict
        :param het: Value of the interation in which the value will be validated for specific directive
        :type het: int
        :return: `CURRENT_PROJ` directive
        :rtype: str
        """
        if check_directive('CURRENT_PROJ', job.het, het):
            return f"SBATCH -A {job.het['CURRENT_PROJ'][het]}"
        if check_directive('CURRENT_PROJ', parameters):
                return f"SBATCH -A {parameters['CURRENT_PROJ']}"
        return ""

    def get_exclusive_directive(self, job: 'Job', parameters: dict = None, het=-1) -> str:
        """
        Returns account directive for the specified job

        :param job: job to create directive `EXCLUSIVE` for SLURM HEADER
        :type job: Job
        :param parameters: set of values found in the config files used to generate the values of the SLURM HEADER
        :type parameters: dict
        :param het: Value of the interation in which the value will be validated for specific directive
        :type het: int
        :return: `EXCLUSIVE` directive
        :rtype: str
        """
        if het > -1 and 'EXCLUSIVE' in job.het and str(parameters['EXCLUSIVE']).lower() == 'true':
            return "SBATCH --exclusive"
        elif parameters is not None and 'EXCLUSIVE' in parameters and bool(parameters['EXCLUSIVE']):
            return "SBATCH --exclusive"
        return ""

    def get_nodes_directive(self, job: 'Job', parameters: dict = None, het=-1) -> str:
        """
        Returns nodes directive for the specified job

        :param job: job to create directive `NODES` for SLURM HEADER
        :type job: Job
        :param parameters: set of values found in the config files used to generate the values of the SLURM HEADER
        :type parameters: dict
        :param het: Value of the interation in which the value will be validated for specific directive
        :type het: int
        :return: `NODES` directive
        :rtype: str
        """
        if check_directive('NODES', job.het, het):
            return f"SBATCH --nodes={job.het['NODES'][het]}"
        elif check_directive('NODES', parameters):
            return f"SBATCH --nodes={parameters['NODES']}"
        return ""

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_memory_directive(self, job: 'Job', parameters: dict = None, het=-1):
        """
        Returns memory directive for the specified job

        :param job: job to create directive `MEMORY` for SLURM HEADER
        :type job: Job
        :param parameters: set of values found in the config files used to generate the values of the SLURM HEADER
        :type parameters: dict
        :param het: Value of the interation in which the value will be validated for specific directive
        :type het: int
        :return: `MEMORY` directive
        :rtype: str
        """
        if check_directive('MEMORY', job.het, het):
            return f"SBATCH --mem={job.het['MEMORY'][het]}"
        elif check_directive('MEMORY', parameters):
            return f"SBATCH --mem={parameters['MEMORY']}"
        return ""

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_memory_per_task_directive(self, job: 'Job', parameters: dict = None, het=-1):
        """
        Returns memory per task directive for the specified job

        :param job: job to create directive `MEMORY_PER_TASK` for SLURM HEADER
        :type job: Job
        :param parameters: set of values found in the config files used to generate the values of the SLURM HEADER
        :type parameters: dict
        :param het: Value of the interation in which the value will be validated for specific directive
        :type het: int
        :return: `MEMORY_PER_TASK` directive
        :rtype: str
        """
        if check_directive('MEMORY_PER_TASK', job.het, het):
            return f"SBATCH --mem-per-cpu={job.het['MEMORY_PER_TASK'][het]}"
        if check_directive('MEMORY_PER_TASK', parameters):
            return f"SBATCH --mem-per-cpu={parameters['MEMORY_PER_TASK']}"
        return ""

    def get_threads_per_task(self, job: 'Job', parameters: dict = None, het=-1):
        """
        Returns threads per task directive for the specified job

        :param job: job to create directive `NUMTHREADS` for SLURM HEADER
        :type job: Job
        :param parameters: set of values found in the config files used to generate the values of the SLURM HEADER
        :type parameters: dict
        :param het: Value of the interation in which the value will be validated for specific directive
        :type het: int
        :return: `NUMTHREADS` directive
        :rtype: str
        """
        # There is no threads per task, so directive is empty
        if check_directive('NUMTHREADS', job.het, het):
            return f"SBATCH --cpus-per-task={job.het['NUMTHREADS'][het]}"
        elif check_directive('NUMTHREADS', parameters):
                return f"SBATCH --cpus-per-task={parameters['NUMTHREADS']}"
        return ""

    # noinspection PyMethodMayBeStatic,PyUnusedLocal

    def get_reservation_directive(self, job: 'Job', parameters: dict = None, het=-1):
        """
        Returns reservation directive for the specified job

        :param job: job to create directive `RESERVATION` for SLURM HEADER
        :type job: Job
        :param parameters: set of values found in the config files used to generate the values of the SLURM HEADER
        :type parameters: dict
        :param het: Value of the interation in which the value will be validated for specific directive
        :type het: int
        :return: `RESERVATION` directive
        :rtype: str
        """
        if check_directive('RESERVATION', job.het, het):
            return f"SBATCH --reservation={job.het['RESERVATION'][het]}"
        elif check_directive('RESERVATION', parameters):
            return f"SBATCH --reservation={parameters['RESERVATION']}"
        return ""

    def get_custom_directives(self, job: 'Job', parameters: dict = None, het=-1) -> str:
        """
        Returns custom directives for the specified job

        :param job: job to create directive `CUSTOM_DIRECTIVES` for SLURM HEADER
        :type job: Job
        :param parameters: set of values found in the config files used to generate the values of the SLURM HEADER
        :type parameters: dict
        :param het: Value of the interation in which the value will be validated for specific directive
        :type het: int
        :return: `CUSTOM_DIRECTIVES` directive
        :rtype: str
        """
        # There is no custom directives, so directive is empty
        if check_directive('CUSTOM_DIRECTIVES', job.het, het):
            return '\n'.join(str(s) for s in job.het['CUSTOM_DIRECTIVES'][het])
        elif check_directive('CUSTOM_DIRECTIVES', parameters):
            return '\n'.join(str(s) for s in parameters['CUSTOM_DIRECTIVES'])
        return ""

    def get_tasks_per_node(self, job: 'Job', parameters: dict = None, het=-1) -> str:
        """
        Returns memory per task directive for the specified job

        :param job: job to create directive `TASKS` for SLURM HEADER
        :type job: Job
        :param parameters: set of values found in the config files used to generate the values of the SLURM HEADER
        :type parameters: dict
        :param het: Value of the interation in which the value will be validated for specific directive
        :type het: int
        :return: `TASKS` per node directive
        :rtype: str
        """
        if check_directive('TASKS', job.het, het):
                return f"SBATCH --ntasks-per-node={job.het['TASKS'][het]}"
        elif check_directive('TASKS', parameters):
            return f"SBATCH --ntasks-per-node={parameters['TASKS']}"
        return ""

    def wrapper_header(self, **kwargs):

        wr_header = f"""
###############################################################################
#              {kwargs["name"].split("_")[0] + "_Wrapper"}
###############################################################################
"""
        if kwargs["wrapper_data"].het.get("HETSIZE", 1) <= 1:
            wr_header += f"""
#SBATCH -J {kwargs["name"]}
{kwargs["queue"]}
{kwargs["partition"]}
{kwargs["dependency"]}
#SBATCH -A {kwargs["project"]}
#SBATCH --output={kwargs["name"]}.out
#SBATCH --error={kwargs["name"]}.err
#SBATCH -t {kwargs["wallclock"]}:00
{kwargs["threads"]}
{kwargs["nodes"]}
{kwargs["num_processors"]}
{kwargs["tasks"]}
{kwargs["exclusive"]}
{kwargs["custom_directives"]}
{kwargs.get("reservation", "#")}
#
    """
        else:
            wr_header = self.calculate_wrapper_het_header(kwargs["wrapper_data"])
        if kwargs["method"] == 'srun':
            language = kwargs["executable"]
            if language is None or len(language) == 0:
                language = "#!/bin/bash"
            return language + wr_header
        else:
            language = kwargs["executable"]
            if language is None or len(language) == 0 or "bash" in language:
                language = "#!/usr/bin/env python3"
            return language + wr_header

    def hetjob_common_header(self, hetsize, wrapper=None):
        if not wrapper:
            header = textwrap.dedent("""\
                    
                    ###############################################################################
                    #                   %TASKTYPE% %DEFAULT.EXPID% EXPERIMENT
                    ###############################################################################
                    #                   Common directives
                    ###############################################################################
                    #
                    #SBATCH -t %WALLCLOCK%:00
                    #SBATCH -J %JOBNAME%
                    #SBATCH --output=%CURRENT_SCRATCH_DIR%/%CURRENT_PROJ_DIR%/%CURRENT_USER%/%DEFAULT.EXPID%/LOG_%DEFAULT.EXPID%/%OUT_LOG_DIRECTIVE%
                    #SBATCH --error=%CURRENT_SCRATCH_DIR%/%CURRENT_PROJ_DIR%/%CURRENT_USER%/%DEFAULT.EXPID%/LOG_%DEFAULT.EXPID%/%ERR_LOG_DIRECTIVE%
                    #%X11%
                    #
                        """)
        else:
            header = f"""
###############################################################################
#              {wrapper.name.split("_")[0] + "_Wrapper"}
###############################################################################
#SBATCH -J {wrapper.name}
#SBATCH --output={wrapper._platform.remote_log_dir}/{wrapper.name}.out
#SBATCH --error={wrapper._platform.remote_log_dir}/{wrapper.name}.err
#SBATCH -t {wrapper.wallclock}:00
#
###########################################################################################
"""
        for components in range(hetsize):
            header += textwrap.dedent(f"""\
            ###############################################################################
            #                 HET_GROUP:{components} 
            ###############################################################################
            #%QUEUE_DIRECTIVE_{components}%
            #%PARTITION_DIRECTIVE_{components}%
            #%ACCOUNT_DIRECTIVE_{components}%
            #%MEMORY_DIRECTIVE_{components}%
            #%MEMORY_PER_TASK_DIRECTIVE_{components}%
            #%THREADS_PER_TASK_DIRECTIVE_{components}%
            #%NODES_DIRECTIVE_{components}%
            #%NUMPROC_DIRECTIVE_{components}%
            #%RESERVATION_DIRECTIVE_{components}%
            #%TASKS_PER_NODE_DIRECTIVE_{components}%
            %CUSTOM_DIRECTIVES_{components}%
            #SBATCH hetjob
            """)
        return header

    def calculate_wrapper_het_header(self, wr_job) -> str:
        # TODO is this function actually being used?
        hetsize = wr_job.het["HETSIZE"]
        header = self.hetjob_common_header(hetsize, wr_job)
        for components in range(hetsize):
            header = header.replace(
                f'%QUEUE_DIRECTIVE_{components}%', self.get_queue_directive(wr_job, het=components))
            header = header.replace(
                f'%PARTITION_DIRECTIVE_{components}%', self.get_partition_directive(wr_job, components))
            header = header.replace(
                f'%ACCOUNT_DIRECTIVE_{components}%', self.get_account_directive(wr_job, het=components))
            header = header.replace(
                f'%MEMORY_DIRECTIVE_{components}%', self.get_memory_directive(wr_job, het=components))
            header = header.replace(
                f'%MEMORY_PER_TASK_DIRECTIVE_{components}%', self.get_memory_per_task_directive(wr_job, het=components))
            header = header.replace(
                f'%THREADS_PER_TASK_DIRECTIVE_{components}%', self.get_threads_per_task(wr_job, het=components))
            header = header.replace(
                f'%NODES_DIRECTIVE_{components}%', self.get_nodes_directive(wr_job, het=components))
            header = header.replace(
                f'%NUMPROC_DIRECTIVE_{components}%', self.get_processors_directive(wr_job, components))
            header = header.replace(
                f'%RESERVATION_DIRECTIVE_{components}%', self.get_reservation_directive(wr_job, het=components))
            header = header.replace(
                f'%TASKS_PER_NODE_DIRECTIVE_{components}%', self.get_tasks_per_node(wr_job, het=components))
            header = header.replace(
                f'%CUSTOM_DIRECTIVES_{components}%', self.get_custom_directives(wr_job, het=components))
        header = header[:-len("#SBATCH hetjob\n")]  # last element

        return header


    def calculate_het_header(self, job: 'Job', parameters: dict):
        header = self.hetjob_common_header(hetsize=job.het["HETSIZE"])
        header = header.replace("%TASKTYPE%", job.section)
        header = header.replace("%DEFAULT.EXPID%", job.expid)
        header = header.replace("%WALLCLOCK%", job.wallclock)
        header = header.replace("%JOBNAME%", job.name)

        if job.x11:
            header = header.replace(
                '%X11%', "SBATCH --x11=batch")
        else:
            header = header.replace('%X11%', "#")

        for components in range(job.het['HETSIZE']):
            header = header.replace(
                f'%QUEUE_DIRECTIVE_{components}%', self.get_queue_directive(job, parameters, components))
            header = header.replace(
                f'%PARTITION_DIRECTIVE_{components}%', self.get_partition_directive(job, components))
            header = header.replace(
                f'%ACCOUNT_DIRECTIVE_{components}%', self.get_account_directive(job, parameters, components))
            header = header.replace(
                f'%MEMORY_DIRECTIVE_{components}%', self.get_memory_directive(job, parameters, components))
            header = header.replace(
                f'%MEMORY_PER_TASK_DIRECTIVE_{components}%',
                self.get_memory_per_task_directive(job, parameters, components))
            header = header.replace(
                f'%THREADS_PER_TASK_DIRECTIVE_{components}%', self.get_threads_per_task(job, parameters, components))
            header = header.replace(
                f'%NODES_DIRECTIVE_{components}%', self.get_nodes_directive(job, parameters, components))
            header = header.replace(
                f'%NUMPROC_DIRECTIVE_{components}%', self.get_processors_directive(job, components))
            header = header.replace(
                f'%RESERVATION_DIRECTIVE_{components}%', self.get_reservation_directive(job, parameters, components))
            header = header.replace(
                f'%TASKS_PER_NODE_DIRECTIVE_{components}%', self.get_tasks_per_node(job, parameters, components))
            header = header.replace(
                f'%CUSTOM_DIRECTIVES_{components}%', self.get_custom_directives(job, parameters, components))
        header = header[:-len("#SBATCH hetjob\n")]  # last element

        return header

    SERIAL = textwrap.dedent("""\
###############################################################################
#                   %TASKTYPE% %DEFAULT.EXPID% EXPERIMENT
###############################################################################
#
#%QUEUE_DIRECTIVE%
#%PARTITION_DIRECTIVE%
#%EXCLUSIVE_DIRECTIVE%
#%ACCOUNT_DIRECTIVE%
#%MEMORY_DIRECTIVE%
#%THREADS_PER_TASK_DIRECTIVE%
#%TASKS_PER_NODE_DIRECTIVE%
#%NODES_DIRECTIVE%
#%NUMPROC_DIRECTIVE%
#%RESERVATION_DIRECTIVE%
#SBATCH -t %WALLCLOCK%:00
#SBATCH -J %JOBNAME%
#SBATCH --output=%CURRENT_SCRATCH_DIR%/%CURRENT_PROJ_DIR%/%CURRENT_USER%/%DEFAULT.EXPID%/LOG_%DEFAULT.EXPID%/%OUT_LOG_DIRECTIVE%
#SBATCH --error=%CURRENT_SCRATCH_DIR%/%CURRENT_PROJ_DIR%/%CURRENT_USER%/%DEFAULT.EXPID%/LOG_%DEFAULT.EXPID%/%ERR_LOG_DIRECTIVE%
%CUSTOM_DIRECTIVES%
#%X11%
#
###############################################################################
           """)

    PARALLEL = textwrap.dedent("""\
###############################################################################
#                   %TASKTYPE% %DEFAULT.EXPID% EXPERIMENT
###############################################################################
#
#%QUEUE_DIRECTIVE%
#%PARTITION_DIRECTIVE%
#%EXCLUSIVE_DIRECTIVE%
#%ACCOUNT_DIRECTIVE%
#%MEMORY_DIRECTIVE%
#%MEMORY_PER_TASK_DIRECTIVE%
#%THREADS_PER_TASK_DIRECTIVE%
#%NODES_DIRECTIVE%
#%NUMPROC_DIRECTIVE%
#%RESERVATION_DIRECTIVE%
#%TASKS_PER_NODE_DIRECTIVE%
#SBATCH -t %WALLCLOCK%:00
#SBATCH -J %JOBNAME%
#SBATCH --output=%CURRENT_SCRATCH_DIR%/%CURRENT_PROJ_DIR%/%CURRENT_USER%/%DEFAULT.EXPID%/LOG_%DEFAULT.EXPID%/%OUT_LOG_DIRECTIVE%
#SBATCH --error=%CURRENT_SCRATCH_DIR%/%CURRENT_PROJ_DIR%/%CURRENT_USER%/%DEFAULT.EXPID%/LOG_%DEFAULT.EXPID%/%ERR_LOG_DIRECTIVE%
%CUSTOM_DIRECTIVES%
#%X11%
#
###############################################################################
    """)
