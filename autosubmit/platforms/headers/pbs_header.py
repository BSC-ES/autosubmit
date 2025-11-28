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


class PBSHeader(object):
    """Class to handle the PBS headers of a job"""

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_queue_directive(self, job, parameters, het=-1):
        """Returns queue directive for the specified job

        :param parameters:
        :param job: job to create queue directive for
        :type job: Job
        :param het:
        :return: queue directive
        :rtype: str
        """
        if parameters['CURRENT_QUEUE'] != '':
            return f"PBS -q {parameters['CURRENT_QUEUE']}"
        return ""

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_custom_directives(self, job, parameters, het=-1):
        """Returns custom directives for the specified job

        :param parameters:
        :param job: job to create custom directive for
        :type job: Job
        :param het:
        :return: custom directives
        :rtype: str
        """
        if parameters['CUSTOM_DIRECTIVES'] != '':
            return '\n'.join(str(s) for s in parameters['CUSTOM_DIRECTIVES'])
        return ""

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_account_directive(self, job, parameters, het=-1):
        """Returns account directive for the specified job

        :param parameters:
        :param job: job to create account directive for
        :type job: Job
        :param het:
        :return: account directive
        :rtype: str
        """
        if parameters['CURRENT_PROJ'] != '':
            return f"PBS -W group_list={parameters['CURRENT_PROJ']}"
        return ""

    @staticmethod
    def get_nodes_directive(job, parameters, het=-1):
        """Returns nodes directive for the specified job

        :param parameters:
        :param job: job to create nodes directive for
        :type job: Job
        :param het:
        :return: nodes directive
        :rtype: str
        """
        if parameters['NODES'] != '' and int(parameters['NODES']) > 0:
            return f"PBS -l select={parameters['NODES']}"
        return ""

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_memory_directive(self, job, parameters, het=-1):
        """Returns memory directive for the specified job

        :param parameters:
        :param job: job to create memory directive for
        :type job: Job
        :param het:
        :return: memory directive
        :rtype: str
        """
        if parameters['MEMORY'] != '':
            return f":mem={parameters['MEMORY']}"
        return ""

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_memory_per_task_directive(self, job, parameters, het=-1):
        """Returns memory per task directive for the specified job

        :param parameters:
        :param job: job to create memory per task directive for
        :type job: Job
        :param het:
        :return: memory per task directive
        :rtype: str
        """
        if parameters['MEMORY_PER_TASK'] != '':
            return f":vmem={parameters['MEMORY_PER_TASK']}"
        return ""

    @staticmethod
    def get_threads_per_task(job, parameters, het=-1):
        """Returns threads per task directive for the specified job

        :param parameters:
        :param job: job to create threads per task directive for
        :type job: Job
        :param het:
        :return: threads per task directive
        :rtype: str
        """
        if parameters['NUMTHREADS'] != '' and int(parameters['NUMTHREADS']) > 1:
            return f":ompthreads={parameters['NUMTHREADS']}"
        return ""

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_reservation_directive(self, job, parameters, het=-1):
        """Returns reservation directive for the specified job

        :param parameters:
        :param job:
        :param het:
        :return:
        """
        if parameters['RESERVATION'] != '':
            return f"PBS -W x={parameters['RESERVATION']}"
        return ""

    @staticmethod
    def get_tasks_per_node(job, parameters, het=-1):
        """Returns memory per task directive for the specified job

        :param parameters:
        :param job: job to create tasks per node directive for
        :type job: Job
        :param het:
        :return: tasks per node directive
        :rtype: str
        """
        if int(parameters['TASKS']) > 1:
            return f":mpiprocs={parameters['TASKS']}"
        return ""

    HEADER = textwrap.dedent("""\
            ###############################################################################
            #                         %TASKTYPE% %DEFAULT.EXPID% EXPERIMENT
            ###############################################################################
            #
            #!/bin/sh --login
            #
            #%NODES_DIRECTIVE%%TASKS_PER_NODE_DIRECTIVE%%THREADS_PER_TASK_DIRECTIVE%%MEMORY_DIRECTIVE%
            #%QUEUE_DIRECTIVE%
            #%ACCOUNT_DIRECTIVE%
            #PBS -l walltime=%WALLCLOCK%:00
            #PBS -N %JOBNAME%
            #PBS -o %CURRENT_SCRATCH_DIR%/%CURRENT_PROJ_DIR%/%CURRENT_USER%/%DEFAULT.EXPID%/LOG_%DEFAULT.EXPID%/%OUT_LOG_DIRECTIVE%
            #PBS -e %CURRENT_SCRATCH_DIR%/%CURRENT_PROJ_DIR%/%CURRENT_USER%/%DEFAULT.EXPID%/LOG_%DEFAULT.EXPID%/%ERR_LOG_DIRECTIVE%
            %CUSTOM_DIRECTIVES%
            #
            ###############################################################################            
            """)
