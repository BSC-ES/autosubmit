#!/usr/bin/env python

# Copyright 2014 Climate Forecasting Unit, IC3

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
# along with Autosubmit.  If not, see <http: www.gnu.org / licenses / >.


import os


from log.log import Log,AutosubmitCritical,AutosubmitError
from autosubmit.config.basicConfig import BasicConfig
from autosubmit.config.config_common import AutosubmitConfig
from submitter import Submitter
from autosubmit.platforms.psplatform import PsPlatform
from autosubmit.platforms.lsfplatform import LsfPlatform
from autosubmit.platforms.pbsplatform import PBSPlatform
from autosubmit.platforms.sgeplatform import SgePlatform
from autosubmit.platforms.ecplatform import EcPlatform
from autosubmit.platforms.slurmplatform import SlurmPlatform
from autosubmit.platforms.pjmplatform import PJMPlatform
from autosubmit.platforms.locplatform import LocalPlatform
from autosubmit.platforms.paramiko_platform import ParamikoPlatformException


class ParamikoSubmitter(Submitter):
    """
    Class to manage the experiments platform
    """

    def load_platforms_migrate(self, asconf, retries=5):
        pass  # Add all info related to migrate

    def load_local_platform(self, asconf):
        platforms = dict()
        # Build Local Platform Object
        local_platform = LocalPlatform(asconf.expid, 'local', BasicConfig)
        local_platform.max_wallclock = asconf.get_max_wallclock()
        local_platform.max_processors = asconf.get_max_processors()
        local_platform.max_waiting_jobs = asconf.get_max_waiting_jobs()
        local_platform.total_jobs = asconf.get_total_jobs()
        local_platform.scratch = os.path.join(
            BasicConfig.LOCAL_ROOT_DIR, asconf.expid, BasicConfig.LOCAL_TMP_DIR)
        local_platform.temp_dir = os.path.join(
            BasicConfig.LOCAL_ROOT_DIR, 'ASlogs')
        local_platform.root_dir = os.path.join(
            BasicConfig.LOCAL_ROOT_DIR, local_platform.expid)
        local_platform.host = 'localhost'
        # Add object to entry in dictionary
        platforms['local'] = local_platform
        platforms['LOCAL'] = local_platform
        self.platforms = platforms

    def load_platforms(self, asconf, retries=5):
        """
        Create all the platforms object that will be used by the experiment

        :param retries: retries in case creation of service fails
        :param asconf: autosubmit config to use
        :type asconf: AutosubmitConfig
        :return: platforms used by the experiment
        :rtype: dict
        """
        raise_message=""
        platforms_used = list()
        hpcarch = asconf.get_platform()
        platforms_used.append(hpcarch)

        # Traverse jobs defined in jobs_.conf and add platforms found if not already included
        job_parser = asconf.jobs_parser
        for job in job_parser.sections():
            hpc = job_parser.get_option(job, 'PLATFORM', hpcarch).lower()
            if hpc not in platforms_used:
                platforms_used.append(hpc)

        parser = asconf.platforms_parser
        # Declare platforms dictionary, key: Platform Name, Value: Platform Object
        platforms = dict()

        # Build Local Platform Object
        local_platform = LocalPlatform(asconf.expid, 'local', BasicConfig)
        local_platform.max_wallclock = asconf.get_max_wallclock()
        local_platform.max_processors = asconf.get_max_processors()
        local_platform.max_waiting_jobs = asconf.get_max_waiting_jobs()
        local_platform.total_jobs = asconf.get_total_jobs()
        local_platform.scratch = os.path.join(
            BasicConfig.LOCAL_ROOT_DIR, asconf.expid, BasicConfig.LOCAL_TMP_DIR)
        local_platform.temp_dir = os.path.join(
            BasicConfig.LOCAL_ROOT_DIR, 'ASlogs')
        local_platform.root_dir = os.path.join(
            BasicConfig.LOCAL_ROOT_DIR, local_platform.expid)
        local_platform.host = 'localhost'
        # Add object to entry in dictionary
        platforms['local'] = local_platform
        platforms['LOCAL'] = local_platform

        # parser is the platforms parser that represents platforms_.conf
        # Traverse sections []
        for section in parser.sections():
            # Consider only those included in the list of jobs
            if section.lower() not in platforms_used:
                continue

            platform_type = parser.get_option(section, 'TYPE', '').lower()
            platform_version = parser.get_option(section, 'VERSION', '')
            try:
                if platform_type == 'pbs':
                    remote_platform = PBSPlatform(
                        asconf.expid, section.lower(), BasicConfig, platform_version)
                elif platform_type == 'sge':
                    remote_platform = SgePlatform(
                        asconf.expid, section.lower(), BasicConfig)
                elif platform_type == 'ps':
                    remote_platform = PsPlatform(
                        asconf.expid, section.lower(), BasicConfig)
                elif platform_type == 'lsf':
                    remote_platform = LsfPlatform(
                        asconf.expid, section.lower(), BasicConfig)
                elif platform_type == 'ecaccess':
                    remote_platform = EcPlatform(
                        asconf.expid, section.lower(), BasicConfig, platform_version)
                elif platform_type == 'slurm':
                    remote_platform = SlurmPlatform(
                        asconf.expid, section.lower(), BasicConfig)
                elif platform_type == 'pjm':
                    remote_platform = PJMPlatform(
                        asconf.expid, section, BasicConfig)
                else:
                    raise Exception(
                        "Queue type not specified on platform {0}".format(section))

            except ParamikoPlatformException as e:
                Log.error("Queue exception: {0}".format(e.message))
                return None
            # Set the type and version of the platform found
            remote_platform.type = platform_type
            remote_platform._version = platform_version

            # Concatenating host + project and adding to the object
            if parser.get_option(section, 'ADD_PROJECT_TO_HOST', '').lower() == 'true':
                host = '{0}-{1}'.format(parser.get_option(section, 'HOST', None),
                                        parser.get_option(section, 'PROJECT', None))
            else:
                host = parser.get_option(section, 'HOST', None)

            remote_platform.host = host
            remote_platform.shape = parser.get_option(section,'SHAPE','')
            # Retrieve more configurations settings and save them in the object
            remote_platform.max_wallclock = parser.get_option(section, 'MAX_WALLCLOCK',
                                                              asconf.get_max_wallclock())
            remote_platform.max_processors = parser.get_option(section, 'MAX_PROCESSORS',
                                                               asconf.get_max_processors())
            remote_platform.max_waiting_jobs = int(parser.get_option(section, 'MAX_WAITING_JOBS',
                                                                     asconf.get_max_waiting_jobs()))
            totaljobs = int(parser.get_option(section, 'TOTALJOBS', asconf.get_total_jobs()))
            total_jobs = int(parser.get_option(section, 'TOTAL_JOBS',
                                                               asconf.get_total_jobs()))
            remote_platform.total_jobs = min(min(totaljobs, total_jobs),asconf.get_total_jobs())
            remote_platform.hyperthreading = parser.get_option(section, 'HYPERTHREADING',
                                                               'false').lower()
            remote_platform.project = parser.get_option(
                section, 'PROJECT', None)
            remote_platform.budget = parser.get_option(
                section, 'BUDGET', remote_platform.project)
            remote_platform.reservation = parser.get_option(
                section, 'RESERVATION', '')
            remote_platform.exclusivity = parser.get_option(
                section, 'EXCLUSIVITY', '').lower()
            remote_platform.user = parser.get_option(section, 'USER', None)
            remote_platform.scratch = parser.get_option(
                section, 'SCRATCH_DIR', "")
            remote_platform.project_dir = parser.get_option(
                section, 'SCRATCH_PROJECT_DIR', remote_platform.project)
            remote_platform.temp_dir = parser.get_option(
                section, 'TEMP_DIR', None)
            remote_platform._default_queue = parser.get_option(
                section, 'QUEUE', None)

            remote_platform.ec_queue = parser.get_option(section,'EC_QUEUE', "hpc")
            remote_platform._serial_queue = parser.get_option(
                section, 'SERIAL_QUEUE', None)
            remote_platform.processors_per_node = parser.get_option(section, 'PROCESSORS_PER_NODE',
                                                                    None)
            remote_platform.custom_directives = parser.get_option(section, 'CUSTOM_DIRECTIVES',
                                                                  None)

            if remote_platform.custom_directives is not None and remote_platform.custom_directives != '' and remote_platform.custom_directives != 'None':
                Log.debug("Custom directives from platform.conf: {0}".format(
                    remote_platform.custom_directives))
            remote_platform.scratch_free_space = parser.get_option(section, 'SCRATCH_FREE_SPACE',
                                                                   None)
            try:
                remote_platform.root_dir = os.path.join(remote_platform.scratch, remote_platform.project,
                                                        remote_platform.user, remote_platform.expid)
                remote_platform.update_cmds()
                platforms[section.lower()] = remote_platform

            except:
                raise_message = "Error in platform.conf: SCRATCH_DIR, PROJECT, USER, EXPID must be defined for platform {0}".format(section)
            # Executes update_cmds() from corresponding Platform Object
            # Save platform into result dictionary

        for section in parser.sections():
            # if this section is included in platforms
            if parser.has_option(section, 'SERIAL_PLATFORM'):
                platforms[section.lower()].serial_platform = platforms[parser.get_option(section,
                                                                                         'SERIAL_PLATFORM',
                                                                                         None)]
                if  platforms[section.lower()].serial_platform is not None:
                    platforms[section.lower()].serial_platform = platforms[section.lower()].serial_platform.lower()

        self.platforms = platforms
        if raise_message != "":
            raise AutosubmitError(raise_message)
