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

        :param job: job to create queue directive for
        :type job: Job
        :return: queue directive
        :rtype: str
        """
        # There is no queue, so directive is empty
        if het > -1 and len(job.het['CURRENT_QUEUE']) > 0:
            if job.het['CURRENT_QUEUE'][het] != '':
                return f"qsub -l qos={job.het['CURRENT_QUEUE'][het]}"
        else:
            if parameters['CURRENT_QUEUE'] != '':
                return f"qsub -l qos={parameters['CURRENT_QUEUE']}"
        return ""


    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_custom_directives(self, job, parameters, het=-1):
        """Returns custom directives for the specified job

        :param job: job to create custom directive for
        :type job: Job
        :return: custom directives
        :rtype: str
        """
        # There is no custom directives, so directive is empty
        if het > -1 and len(job.het['CUSTOM_DIRECTIVES']) > 0:
            if job.het['CUSTOM_DIRECTIVES'][het] != '':
                return '\n'.join(str(s) for s in job.het['CUSTOM_DIRECTIVES'][het])
        else:
            if parameters['CUSTOM_DIRECTIVES'] != '':
                return '\n'.join(str(s) for s in parameters['CUSTOM_DIRECTIVES'])
        return ""


    def get_proccesors_directive(self, job, parameters, het=-1):
        """Returns processors directive for the specified job

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
                return f"qsub -l ppn={job.het['PROCESSORS'][het]}"
        if job.nodes == "":
            job_nodes = 0
        else:
            job_nodes = job.nodes
        if job.processors == '' or job.processors == '1' and int(job_nodes) > 0:
            return ""
        else:
            return f"qsub -l ppn={job.processors}"


    def get_partition_directive(self, job, parameters, het=-1):
        """Returns partition directive for the specified job

        :param job: job to create partition directive for
        :type job: Job
        :return: partition directive
        :rtype: str
        """
        if het > -1 and len(job.het['PARTITION']) > 0:
            if job.het['PARTITION'][het] != '':
                return f"qsub -q={job.het['PARTITION'][het]}"
        else:
            if job.partition != '':
                return f"qsub -q={job.partition}"
        return ""


    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_account_directive(self, job, parameters, het=-1):
        """Returns account directive for the specified job

        :param job: job to create account directive for
        :type job: Job
        :return: account directive
        :rtype: str
        """
        if het > -1 and len(job.het['CURRENT_PROJ']) > 0:
            if job.het['CURRENT_PROJ'][het] != '':
                return f"qsub -q debug-g -l select=1:mpiprocs=2 -W group_list={job.het['CURRENT_PROJ'][het]} ./a00b/LOG_a00b/a00b_LOCAL_SETUP.cmd"
        else:
            if parameters['CURRENT_PROJ'] != '':
                return f"qsub -q debug-g -l select=1:mpiprocs=2 -W group_list={parameters['CURRENT_PROJ']} ./a00b/LOG_a00b/a00b_LOCAL_SETUP.cmd"
        return ""


    def get_exclusive_directive(self, job, parameters, het=-1):
        """Returns account directive for the specified job

        :param job: job to create account directive for
        :type job: Job
        :return: account directive
        :rtype: str
        """
        if het > -1 and len(job.het['EXCLUSIVE']) > 0:
            if str(parameters['EXCLUSIVE']).lower() == 'true':
                return "qsub -l naccesspolicy=singlejob"
        else:
            if str(parameters['EXCLUSIVE']).lower() == 'true':
                return "qsub -l naccesspolicy=singlejob"
        return ""


    def get_nodes_directive(self, job, parameters, het=-1):
        """Returns nodes directive for the specified job

        :param job: job to create nodes directive for
        :type job: Job
        :return: nodes directive
        :rtype: str
        """
        if het > -1 and len(job.het['NODES']) > 0:
            if job.het['NODES'][het] != '':
                return f"qsub -l nodes={job.het['NODES'][het]}"
        else:
            if parameters['NODES'] != '':
                return f"qsub -l nodes={parameters['NODES']}"
        return ""


    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_memory_directive(self, job, parameters, het=-1):
        """Returns memory directive for the specified job

        :param job: job to create memory directive for
        :type job: Job
        :return: memory directive
        :rtype: str
        """
        if het > -1 and len(job.het['MEMORY']) > 0:
            if job.het['MEMORY'][het] != '':
                return f"qsub -l mem={job.het['MEMORY'][het]}"
        else:
            if parameters['MEMORY'] != '':
                return f"qsub -l mem={parameters['MEMORY']}"
        return ""


    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_memory_per_task_directive(self, job, parameters, het=-1):
        """Returns memory per task directive for the specified job

        :param job: job to create memory per task directive for
        :type job: Job
        :return: memory per task directive
        :rtype: str
        """
        if het > -1 and len(job.het['MEMORY_PER_TASK']) > 0:
            if job.het['MEMORY_PER_TASK'][het] != '':
                return f"qsub -l vmem={job.het['MEMORY_PER_TASK'][het]}"
        else:
            if parameters['MEMORY_PER_TASK'] != '':
                return f"qsub -l vmem={parameters['MEMORY_PER_TASK']}"
        return ""


    def get_threads_per_task(self, job, parameters, het=-1):
        """Returns threads per task directive for the specified job

        :param job: job to create threads per task directive for
        :type job: Job
        :return: threads per task directive
        :rtype: str
        """
        # There is no threads per task, so directive is empty
        if het > -1 and len(job.het['NUMTHREADS']) > 0:
            if job.het['NUMTHREADS'][het] != '':
                return f"qsub -l ppn={job.het['NUMTHREADS'][het]}"
        else:
            if parameters['NUMTHREADS'] != '':
                return f"qsub -l ppn={parameters['NUMTHREADS']}"
        return ""


    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def get_reservation_directive(self, job, parameters, het=-1):
        """Returns reservation directive for the specified job

        :param job:
        :param het:
        :return:
        """

        if het > -1 and len(job.het['RESERVATION']) > 0:
            if job.het['RESERVATION'][het] != '':
                return f"qsub -W x={job.het['RESERVATION'][het]}"
        else:
            if parameters['RESERVATION'] != '':
                return f"qsub -W x={parameters['RESERVATION']}"
        return ""


    def get_tasks_per_node(self, job, parameters, het=-1):
        """Returns memory per task directive for the specified job

        :param job: job to create tasks per node directive for
        :type job: Job
        :return: tasks per node directive
        :rtype: str
        """
        if het > -1 and len(job.het['TASKS']) > 0:
            if int(job.het['TASKS'][het]):
                return f"qsub -l nodes={job.het['TASKS'][het]}:ppn={job.het['TASKS'][het]}"
        else:
            if int(parameters['TASKS']) > 1:
                return f"qsub -l nodes={parameters['TASKS']}:ppn={parameters['TASKS']}"
        return ""


    def hetjob_common_header(self, hetsize, wrapper=None):
        header = textwrap.dedent("""\

                ###############################################################################
                #                   %TASKTYPE% %DEFAULT.EXPID% EXPERIMENT
                ###############################################################################
                #                   Common directives
                ###############################################################################
                #
                #PBS -q debug-g
                #PBS -l select=1
                #PBS -l walltime=%WALLCLOCK%:00
                #PBS -N %JOBNAME%
                #PBS -o %CURRENT_SCRATCH_DIR%/%CURRENT_PROJ_DIR%/%CURRENT_USER%/%DEFAULT.EXPID%/LOG_%DEFAULT.EXPID%/%OUT_LOG_DIRECTIVE%
                #PBS -e %CURRENT_SCRATCH_DIR%/%CURRENT_PROJ_DIR%/%CURRENT_USER%/%DEFAULT.EXPID%/LOG_%DEFAULT.EXPID%/%ERR_LOG_DIRECTIVE%
                #PBS -W group_list=group1
                #PBS -j oe
                #
                    """)


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
            #PBS hetjob
            """)
        return header


    def calculate_het_header(self, job, parameters):
        header = self.hetjob_common_header(hetsize=job.het["HETSIZE"])
        header = header.replace("%TASKTYPE%", job.section)
        header = header.replace("%DEFAULT.EXPID%", job.expid)
        header = header.replace("%WALLCLOCK%", job.wallclock)
        header = header.replace("%JOBNAME%", job.name)

        if job.x11:
            header = header.replace(
                '%X11%', "qsub -X")
        else:
            header = header.replace('%X11%', "#")

        for components in range(job.het['HETSIZE']):
            header = header.replace(
                f'%QUEUE_DIRECTIVE_{components}%', self.get_queue_directive(job, parameters, components))
            header = header.replace(
                f'%PARTITION_DIRECTIVE_{components}%', self.get_partition_directive(job, parameters, components))
            header = header.replace(
                f'%ACCOUNT_DIRECTIVE_{components}%', self.get_account_directive(job, parameters, components))
            header = header.replace(
                f'%MEMORY_DIRECTIVE_{components}%', self.get_memory_directive(job, parameters, components))
            header = header.replace(
                f'%MEMORY_PER_TASK_DIRECTIVE_{components}%',
                self.get_memory_per_task_directive(job, parameters, components))
            header = header.replace(
                f'%THREADS_PER_TASK_DIRECTIVE_{components}%',
                self.get_threads_per_task(job, parameters, components))
            header = header.replace(
                f'%NODES_DIRECTIVE_{components}%', self.get_nodes_directive(job, parameters, components))
            header = header.replace(
                f'%NUMPROC_DIRECTIVE_{components}%', self.get_proccesors_directive(job, parameters, components))
            header = header.replace(
                f'%RESERVATION_DIRECTIVE_{components}%',
                self.get_reservation_directive(job, parameters, components))
            header = header.replace(
                f'%TASKS_PER_NODE_DIRECTIVE_{components}%', self.get_tasks_per_node(job, parameters, components))
            header = header.replace(
                f'%CUSTOM_DIRECTIVES_{components}%', self.get_custom_directives(job, parameters, components))
        header = header[:-len("#PBS hetjob\n")]  # last element

        return header

    SERIAL = textwrap.dedent("""\
            ###############################################################################
            #                         %TASKTYPE% %DEFAULT.EXPID% EXPERIMENT
            ###############################################################################
            #
            #!/bin/sh --login
            #PBS -N %JOBNAME%
            #PBS -l walltime=%WALLCLOCK%
            #PBS -e %CURRENT_SCRATCH_DIR%/%CURRENT_PROJ%/%CURRENT_USER%/%DEFAULT.EXPID%/LOG_%DEFAULT.EXPID%
            #PBS -o %CURRENT_SCRATCH_DIR%/%CURRENT_PROJ%/%CURRENT_USER%/%DEFAULT.EXPID%/LOG_%DEFAULT.EXPID%
            %CUSTOM_DIRECTIVES%
            #
            ###############################################################################
            """)

    PARALLEL = textwrap.dedent("""\
            ###############################################################################
            #                         %TASKTYPE% %DEFAULT.EXPID% EXPERIMENT
            ###############################################################################
            #
            #!/bin/sh --login
            #PBS -N %JOBNAME%
            #PBS -l walltime=%WALLCLOCK%
            #PBS -e %CURRENT_SCRATCH_DIR%/%CURRENT_PROJ%/%CURRENT_USER%/%DEFAULT.EXPID%/LOG_%DEFAULT.EXPID%
            #PBS -o %CURRENT_SCRATCH_DIR%/%CURRENT_PROJ%/%CURRENT_USER%/%DEFAULT.EXPID%/LOG_%DEFAULT.EXPID%
            %CUSTOM_DIRECTIVES%
            #
            ###############################################################################
            """)