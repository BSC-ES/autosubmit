#!/usr/bin/env python3

# Copyright 2015-2025 Earth Sciences Department, BSC-CNS

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
from autosubmit.log.log import Log

class FluxOverSlurmHeader(object):
    """
    Class to handle the header of a job that runs in a Flux instance inside a Slurm allocation.
    """

    def get_proccesors_directive(self, job, parameters, het=-1):
        """
        Returns processors directive for the specified job

        :param job: job to create processors directive for
        :type job: Job
        :return: processors directive
        :rtype: str
        """
        if het > -1 and len(job.het['NODES']) > 0:
            if job.het['NODES'][het] == '':
                job_nodes = 0
            else:
                job_nodes = job.het['NODES'][het]
            if len(job.het['PROCESSORS']) == 0 or job.het['PROCESSORS'][het] == '' or job.het['PROCESSORS'][
                het] == '1' and int(job_nodes) > 0:
                return ""
            else:
                return "FLUX: --nslots {0}".format(job.het['PROCESSORS'][het])
        if job.nodes == "":
            job_nodes = 0
        else:
            job_nodes = job.nodes
        if job.processors == '' or job.processors == '1' and int(job_nodes) > 0:
            return ""
        else:
            return "FLUX: --nslots {0}".format(job.processors)

    def get_nodes_directive(self, job, parameters, het=-1):
        """
        Returns nodes directive for the specified job
        :param job: job to create nodes directive for
        :type job: Job
        :return: nodes directive
        :rtype: str
        """
        if het > -1 and len(job.het['NODES']) > 0:
            if job.het['NODES'][het] != '':
                return "FLUX: --nodes {0}".format(job.het['NODES'][het])
        else:
            if parameters['NODES'] != '':
                return "FLUX: --nodes {0}".format(parameters['NODES'])
        return ""

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_memory_directive(self, job, parameters, het=-1):
        # TODO: [ENGINES] Implement this method
        Log.warning("Directive 'mem' is not currently supported for Flux jobs. Ignoring it")
        return ""

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_memory_per_task_directive(self, job, parameters, het=-1):
        # TODO: [ENGINES] Implement this method
        Log.warning("Directive 'mem-per-cpu' is not currently supported for Flux jobs. Ignoring it")
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
                return "FLUX: --cores-per-slot {0}".format(job.het['NUMTHREADS'][het])
        else:
            if parameters['NUMTHREADS'] != '':
                return "FLUX: --cores-per-slot {0}".format(parameters['NUMTHREADS'])
        return ""

    def get_custom_directives(self, job, parameters, het=-1):
        Log.warning("Jobs within a wrapper using the Flux method do not currently support custom directives.")
        return ""

    def get_tasks_per_node(self, job, parameters, het=-1):
        """
        Returns tasks per node directive for the specified job

        :param job: job to create tasks per node directive for
        :type job: Job
        :return: tasks per node directive
        :rtype: str
        """
        if het > -1 and len(job.het['TASKS']) > 0:
            if int(job.het['TASKS'][het]):
                return "FLUX: --tasks-per-node={0}".format(job.het['TASKS'][het])
        elif int(parameters['TASKS']) > 0:
                return "FLUX: --tasks-per-node={0}".format(parameters['TASKS'])
        return ""
    
    def calculate_het_header(self, job, parameters):
        Log.warning("Heterogeneous configurations are not currently supported for Flux jobs. Ignoring them")
        return ""
    
    def get_wallclock_directive(self, job, parameters):
        """
        Returns wallclock directive for the specified job

        :param job: job to create wallclock directive for
        :type job: Job
        :return: wallclock directive
        :rtype: str
        """
        wallclock = parameters['WALLCLOCK']
        h, m = wallclock.split(':')
        wallclock = int(h) * 60 + int(m)
        return "FLUX: --time-limit {0}m".format(wallclock)

    def get_exclusive_directive(self, job, parameters, het=-1):
        """
        Returns account directive for the specified job

        :param job: job to create account directive for
        :type job: Job
        :return: account directive
        :rtype: str
        """
        # TODO: [ENGINES] Flux does not admit exclusive nodes if "nodes" are not specified
        if self.get_nodes_directive(job, parameters, het) == "":
            Log.warning("""Flux does not admit exclusive nodes if "nodes" are not specified. Ignoring it""")

        if str(parameters['EXCLUSIVE']).lower() == 'true':
            return "FLUX: --exclusive"
        return ""

    SERIAL = textwrap.dedent("""\
###############################################################################
#                   %TASKTYPE% %DEFAULT.EXPID% EXPERIMENT
###############################################################################
#
# %NUMPROC_DIRECTIVE%
# %THREADS_PER_TASK_DIRECTIVE%
# %NODES_DIRECTIVE%
# %WALLCLOCK_DIRECTIVE%
# %TASKS_PER_NODE_DIRECTIVE%
# FLUX: --job-name %JOBNAME%
# FLUX: --output %CURRENT_SCRATCH_DIR%/%CURRENT_PROJ_DIR%/%CURRENT_USER%/%DEFAULT.EXPID%/LOG_%DEFAULT.EXPID%/%OUT_LOG_DIRECTIVE%
# FLUX: --error %CURRENT_SCRATCH_DIR%/%CURRENT_PROJ_DIR%/%CURRENT_USER%/%DEFAULT.EXPID%/LOG_%DEFAULT.EXPID%/%ERR_LOG_DIRECTIVE%
# FLUX: --flags waitable
#
###############################################################################
           """)

    PARALLEL = SERIAL # TODO: [ENGINES] Differentiate serial and parallel headers if needed later

# TODO: [ENGINES] --output and --error directives will be overwritten in the wrapper builders