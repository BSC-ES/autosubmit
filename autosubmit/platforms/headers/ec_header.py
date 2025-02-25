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


class EcHeader(object):
    """Class to handle the ECMWF headers of a job"""

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

    # noinspection PyPep8
    SERIAL = textwrap.dedent("""\
            ###############################################################################
            #                   %TASKTYPE% %DEFAULT.EXPID% EXPERIMENT
            ###############################################################################
            #
            #@ shell            = /usr/bin/ksh
            #@ class            = ns
            #@ job_type         = serial
            #@ job_name         = %JOBNAME%
            #@ output           = %CURRENT_SCRATCH_DIR%/%CURRENT_PROJ%/%CURRENT_USER%/%DEFAULT.EXPID%/LOG_%DEFAULT.EXPID%/$(job_name).$(jobid).out
            #@ error            = %CURRENT_SCRATCH_DIR%/%CURRENT_PROJ%/%CURRENT_USER%/%DEFAULT.EXPID%/LOG_%DEFAULT.EXPID%/$(job_name).$(jobid).err
            #@ notification     = error
            #@ resources        = ConsumableCpus(1) ConsumableMemory(1200mb)
            #@ wall_clock_limit = %WALLCLOCK%:00
            #@ platforms
            %CUSTOM_DIRECTIVES%
            #
            ###############################################################################
            """)

    # noinspection PyPep8
    PARALLEL = textwrap.dedent("""\
            ###############################################################################
            #                   %TASKTYPE% %DEFAULT.EXPID% EXPERIMENT
            ###############################################################################
            #
            #@ shell            = /usr/bin/ksh
            #@ class            = np
            #@ job_type         = parallel
            #@ job_name         = %JOBNAME%
            #@ output           = %CURRENT_SCRATCH_DIR%/%CURRENT_PROJ%/%CURRENT_USER%/%DEFAULT.EXPID%/LOG_%DEFAULT.EXPID%/$(job_name).$(jobid).out
            #@ error            = %CURRENT_SCRATCH_DIR%/%CURRENT_PROJ%/%CURRENT_USER%/%DEFAULT.EXPID%/LOG_%DEFAULT.EXPID%/$(job_name).$(jobid).err
            #@ notification     = error
            #@ resources        = ConsumableCpus(1) ConsumableMemory(1200mb)
            #@ ec_smt           = no
            #@ total_tasks      = %NUMPROC%
            #@ wall_clock_limit = %WALLCLOCK%:00
            #@ platforms
            %CUSTOM_DIRECTIVES%
            #
            ###############################################################################
            """)
