#!/usr/bin/env python

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


class SlurmHeader(object):
    """Class to handle the SLURM headers of a job"""

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_queue_directive(self, job):
        """
        Returns queue directive for the specified job

        :param job: job to create queue directive for
        :type job: Job
        :return: queue directive
        :rtype: str
        """
        # There is no queue, so directive is empty
        if job.parameters['CURRENT_QUEUE'] == '':
            return ""
        else:
            return "SBATCH --qos={0}".format(job.parameters['CURRENT_QUEUE'])

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_account_directive(self, job):
        """
        Returns account directive for the specified job

        :param job: job to create account directive for
        :type job: Job
        :return: account directive
        :rtype: str
        """
        # There is no account, so directive is empty
        if job.parameters['CURRENT_PROJ'] != '':
            return "SBATCH -A {0}".format(job.parameters['CURRENT_PROJ'])
        return ""

    def get_numproc_directive(self, job):
        """
        Returns numproc directive for the specified job

        :param job: job to create numproc directive for
        :type job: Job
        :return: numproc directive
        :rtype: str
        """
        # There is no numproc, so directive is empty
        nodes = job.parameters.get('NODES', 0)
        procs = job.parameters.get('NUMPROC', 1)
        if nodes and procs and int(procs) <= 1:
            return ""
        else:
            return "SBATCH -n {0}".format(job.parameters['NUMPROC'])


    def get_nodes_directive(self, job):
        """
        Returns nodes directive for the specified job
        :param job: job to create nodes directive for
        :type job: Job
        :return: nodes directive
        :rtype: str
        """
        # There is no account, so directive is empty
        nodes = job.parameters.get('NODES',"")
        if nodes != '':
            return "SBATCH -N {0}".format(nodes)
        return ""
    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_memory_directive(self, job):
        """
        Returns memory directive for the specified job

        :param job: job to create memory directive for
        :type job: Job
        :return: memory directive
        :rtype: str
        """
        # There is no memory, so directive is empty
        if job.parameters['MEMORY'] != '':
            return "SBATCH --mem {0}".format(job.parameters['MEMORY'])
        return ""

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_memory_per_task_directive(self, job):
        """
        Returns memory per task directive for the specified job

        :param job: job to create memory per task directive for
        :type job: Job
        :return: memory per task directive
        :rtype: str
        """
        # There is no memory per task, so directive is empty
        if job.parameters['MEMORY_PER_TASK'] != '':
            return "SBATCH --mem-per-cpu {0}".format(job.parameters['MEMORY_PER_TASK'])
        return ""

    def get_threads_per_task(self, job):
        if job.parameters['NUMTHREADS'] == '':
            return ""
        else:
            return "SBATCH --cpus-per-task={0}".format(job.parameters['NUMTHREADS'])

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_custom_directives(self, job):
        """
        Returns custom directives for the specified job

        :param job: job to create custom directive for
        :type job: Job
        :return: custom directives
        :rtype: str
        """
        # There is no custom directives, so directive is empty
        if job.parameters['CUSTOM_DIRECTIVES'] != '':
            return '\n'.join(str(s) for s in job.parameters['CUSTOM_DIRECTIVES'])
        return ""



    def get_tasks_per_node(self, job):
        """
        Returns memory per task directive for the specified job

        :param job: job to create tasks per node directive for
        :type job: Job
        :return: tasks per node directive
        :rtype: str
        """
        if job.parameters['TASKS'].isdigit() and int(job.parameters['TASKS']) > 1:
            return "SBATCH --ntasks-per-node={0}".format(job.parameters['TASKS'])
        return ""

    SERIAL = textwrap.dedent("""\
###############################################################################
#                   %TASKTYPE% %EXPID% EXPERIMENT
###############################################################################
#
#%QUEUE_DIRECTIVE%
#%ACCOUNT_DIRECTIVE%
#%MEMORY_DIRECTIVE%
#%THREADS_PER_TASK_DIRECTIVE%
#%TASKS_PER_NODE_DIRECTIVE%
#%NODES_DIRECTIVE%
#%NUMPROC_DIRECTIVE%
#SBATCH -t %WALLCLOCK%:00
#SBATCH -J %JOBNAME%
#SBATCH --output=%CURRENT_SCRATCH_DIR%/%CURRENT_PROJ%/%CURRENT_USER%/%EXPID%/LOG_%EXPID%/%OUT_LOG_DIRECTIVE%
#SBATCH --error=%CURRENT_SCRATCH_DIR%/%CURRENT_PROJ%/%CURRENT_USER%/%EXPID%/LOG_%EXPID%/%ERR_LOG_DIRECTIVE%
#SBATCH --output=%CURRENT_SCRATCH_DIR%/%CURRENT_PROJ_DIR%/%CURRENT_USER%/%DEFAULT.EXPID%/LOG_%DEFAULT.EXPID%/%OUT_LOG_DIRECTIVE%
#SBATCH --error=%CURRENT_SCRATCH_DIR%/%CURRENT_PROJ_DIR%/%CURRENT_USER%/%DEFAULT.EXPID%/LOG_%DEFAULT.EXPID%/%ERR_LOG_DIRECTIVE%
%CUSTOM_DIRECTIVES%
#%X11%
#
###############################################################################
           """)

    PARALLEL = textwrap.dedent("""\
###############################################################################
#                   %TASKTYPE% %EXPID% EXPERIMENT
###############################################################################
#
#%QUEUE_DIRECTIVE%
#%ACCOUNT_DIRECTIVE%
#%MEMORY_DIRECTIVE%
#%MEMORY_PER_TASK_DIRECTIVE%
#%THREADS_PER_TASK_DIRECTIVE%
#%NODES_DIRECTIVE%
#%NUMPROC_DIRECTIVE%
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
