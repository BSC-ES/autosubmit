#!/usr/bin/env python3

# Copyright 2023 Earth Sciences Department, BSC-CNS

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


class PJMHeader(object):
    """Class to handle the PJM headers of a job"""

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_queue_directive(self, job, parameters):
        """
        Returns queue directive for the specified job

        :param job: job to create queue directive for
        :type job: Job
        :return: queue directive
        :rtype: str
        """
        # There is no queue, so directive is empty
        if parameters['CURRENT_QUEUE'] == '':
            return ""
        else:
            return "PJM -L rscgrp={0}".format(parameters['CURRENT_QUEUE'])

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_account_directive(self, job, parameters):
        """
        Returns account directive for the specified job

        :param job: job to create account directive for
        :type job: Job
        :return: account directive
        :rtype: str
        """
        # wallet,account group_name. source: nkl.cc.u-tokyo.ac.jp
        if parameters['CURRENT_PROJ'] != '':
            return "PJM -g {0}".format(parameters['CURRENT_PROJ'])
        return ""

    def get_nodes_directive(self, job, parameters):
        """
        Returns nodes directive for the specified job
        :param job: job to create nodes directive for
        :type job: Job
        :return: nodes directive
        :rtype: str
        """
        # There is no account, so directive is empty
        nodes = parameters.get('NODES', "")
        if nodes != '':
            return "PJM -L node={0}".format(nodes)
        return ""

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_memory_directive(self, job, parameters):
        """
        Returns memory directive for the specified job

        :param job: job to create memory directive for
        :type job: Job
        :return: memory directive
        :rtype: str
        """
        if parameters['MEMORY'] != '':
            return "PJM --node-mem={0}".format(parameters['MEMORY'])
        return ""

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_memory_per_task_directive(self, job, parameters):
        """
        Returns memory per task directive for the specified job

        :param job: job to create memory per task directive for
        :type job: Job
        :return: memory per task directive
        :rtype: str
        """
        if parameters['MEMORY_PER_TASK'] != '':
            return "PJM --core-mem={0}".format(parameters['MEMORY_PER_TASK'])
        return ""

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_custom_directives(self, job, parameters):
        """
        Returns custom directives for the specified job

        :param job: job to create custom directive for
        :type job: Job
        :return: custom directives
        :rtype: str
        """
        # There is no custom directives, so directive is empty
        if parameters['CUSTOM_DIRECTIVES'] != '':
            return '\n'.join(str(s) for s in parameters['CUSTOM_DIRECTIVES'])
        return ""

    def get_tasks_directive(self, job, het=-1):
        """
        Returns tasks per node directive for the specified job

        :param job: job to create tasks per node directive for
        :type job: Job
        :return: tasks per node directive
        :rtype: str
        """
        if int(parameters['TASKS']) > 1:
            return "max-proc-per-node={0}".format(parameters['TASKS'])
        return ""

    def get_shape_directive(self, job, parameters):
        """
        Returns shape directive for the specified job
        :param job:
        :return:
        """
        if parameters['SHAPE'] != '':
            return "PJM --mpi 'shape={0}'".format(parameters['SHAPE'])
        return ""

    def get_tasks_per_node(self, job, parameters):
        """
        Returns tasks per node directive for the specified job

        :param job: job to create tasks per node directive for
        :type job: Job
        :return: tasks per node directive
        :rtype: str
        """
        if int(parameters['TASKS']) > 1:
            return "max-proc-per-node={0}".format(parameters['TASKS'])
        return ""

    def get_threads_per_task(self, job, parameters, het=-1):
        """
        Returns threads per task directive for the specified job

        :param job: job to create threads per task directive for
        :type job: Job
        :return: threads per task directive
        :rtype: str
        """
        # There is no threads per task, so directive is empty
        if het > -1 and len(job.het['NUMTHREADS']) > 0:
            if job.het['NUMTHREADS'][het] != '':
                return f"export OMP_NUM_THREADS={job.het['NUMTHREADS'][het]}"
        else:
            if parameters['NUMTHREADS'] != '':
                return "export OMP_NUM_THREADS={0}".format(parameters['NUMTHREADS'])
        return ""

    SERIAL = textwrap.dedent("""\
###############################################################################
#                   %TASKTYPE% %DEFAULT.EXPID% EXPERIMENT
###############################################################################
#
#PJM -N %JOBNAME%
#PJM -L elapse=%WALLCLOCK%:00
#%QUEUE_DIRECTIVE%
#%ACCOUNT_DIRECTIVE%
#%MEMORY_DIRECTIVE%
%CUSTOM_DIRECTIVES%
#%SHAPE_DIRECTIVE%
#%NODES_DIRECTIVE%
#PJM -o %CURRENT_SCRATCH_DIR%/%CURRENT_PROJ_DIR%/%CURRENT_USER%/%DEFAULT.EXPID%/LOG_%DEFAULT.EXPID%/%OUT_LOG_DIRECTIVE%
#PJM -e %CURRENT_SCRATCH_DIR%/%CURRENT_PROJ_DIR%/%CURRENT_USER%/%DEFAULT.EXPID%/LOG_%DEFAULT.EXPID%/%ERR_LOG_DIRECTIVE%
#%X11%
%THREADS_PER_TASK_DIRECTIVE%
###############################################################################
           """)

    PARALLEL = textwrap.dedent("""\
###############################################################################
#                   %TASKTYPE% %DEFAULT.EXPID% EXPERIMENT
###############################################################################
#
#PJM -N %JOBNAME%
#%NODES_DIRECTIVE%
#PJM --mpi "proc=%NUMPROC%"
#PJM --mpi "%TASKS_PER_NODE_DIRECTIVE%"
#PJM -L elapse=%WALLCLOCK%:00
#%QUEUE_DIRECTIVE%
#%ACCOUNT_DIRECTIVE%
#%MEMORY_DIRECTIVE%
#%MEMORY_PER_TASK_DIRECTIVE%
#%SHAPE_DIRECTIVE%
#PJM -o %CURRENT_SCRATCH_DIR%/%CURRENT_PROJ_DIR%/%CURRENT_USER%/%DEFAULT.EXPID%/LOG_%DEFAULT.EXPID%/%OUT_LOG_DIRECTIVE%
#PJM -e %CURRENT_SCRATCH_DIR%/%CURRENT_PROJ_DIR%/%CURRENT_USER%/%DEFAULT.EXPID%/LOG_%DEFAULT.EXPID%/%ERR_LOG_DIRECTIVE%
%CUSTOM_DIRECTIVES%
%THREADS_PER_TASK_DIRECTIVE%
###############################################################################
    """)
