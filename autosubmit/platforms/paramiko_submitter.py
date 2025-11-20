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

"""Code for handling submitting jobs to platforms."""


import os
from collections import defaultdict
from typing import Optional, Union, TYPE_CHECKING

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.log.log import Log, AutosubmitError, AutosubmitCritical
from autosubmit.platforms.ecplatform import EcPlatform
from autosubmit.platforms.locplatform import LocalPlatform
from autosubmit.platforms.paramiko_platform import ParamikoPlatformException
from autosubmit.platforms.pjmplatform import PJMPlatform
from autosubmit.platforms.psplatform import PsPlatform
from autosubmit.platforms.slurmplatform import SlurmPlatform

if TYPE_CHECKING:
    from autosubmit.config.configcommon import AutosubmitConfig
    from autosubmit.platforms.paramiko_platform import ParamikoPlatform


def _get_platforms_used(hpcarch: str, jobs_data: dict) -> set[str]:
    """Traverse jobs defined in jobs configurations."""
    platforms_used = {hpcarch}
    for job in jobs_data:
        job_platform = jobs_data[job].get('PLATFORM', '').upper()
        if job_platform and job_platform not in platforms_used:
            platforms_used.add(job_platform)

    return platforms_used


def _get_serial_platforms(platforms_used: set[str], platforms_data: dict) -> dict[str, list]:
    """Traverse used platforms and then look for serial platforms."""
    serial_platforms = defaultdict(list)
    for platform in list(platforms_used):
        hpc: Optional[str] = platforms_data.get(platform, {}).get("SERIAL_PLATFORM", None)
        if hpc:
            serial_platforms[hpc].append(platform)
            if hpc not in platforms_used:
                platforms_used.add(hpc)

    return serial_platforms


def _get_host(section_host: str, add_project_to_host: bool, project: str) -> str:
    """Get the section host.

    If ``add_project_to_host`` is ``False`` we return the section host provided,
    stripping spaces (from head and tail).

    Otherwise, if the host name does not contain commas, we return a single string with
    the host name, a hyphen, and the project.

    If the host name does contain commas, in that case we will create a list with all the
    host names appending the project to host.

    :param section_host: The section host name.
    :param add_project_to_host: If ``True``, we will add the project to the host name.
    :param project: The project name.
    """
    host = section_host
    if add_project_to_host:
        if host.find(",") == -1:
            host = f'{host}-{project}'
        else:
            host_list = host.split(",")
            host_aux = ""
            for ip in host_list:
                host_aux += f'{ip}-{project},'
            host = host_aux[:-1]

    return host.strip(" ")


def _get_platform_by_type(platform_type: str, expid: str, platform_name: str, experiment_data: dict,
                          platform_version: str, auth_password: Optional[str]) -> Optional['ParamikoPlatform']:
    if platform_type == 'ps':
        return PsPlatform(expid, platform_name, experiment_data)
    elif platform_type == 'ecaccess':
        return EcPlatform(expid, platform_name, experiment_data, platform_version)
    elif platform_type == 'slurm':
        return SlurmPlatform(expid, platform_name, experiment_data, auth_password=auth_password)
    elif platform_type == 'pjm':
        return PJMPlatform(expid, platform_name, experiment_data)

    return None


# TODO: This doesn't need a class if we just return ``platforms``.
class ParamikoSubmitter:
    """Class to manage the experiments Paramiko platforms."""

    def __init__(self, as_conf: 'AutosubmitConfig', auth_password: Optional[str] = None,
                       local_auth_password=None):
        self.platforms: Optional[dict[str, 'ParamikoPlatform']] = None
        self.load_platforms(as_conf=as_conf, auth_password=auth_password, local_auth_password=local_auth_password)

    def load_local_platform(self, as_conf: 'AutosubmitConfig', experiment_data: Optional[dict] = None,
                            auth_password: Optional[str] = None) -> None:
        """Create the local platform.

        :param as_conf: Autosubmit configuration.
        :param experiment_data: Experiment configuration. Uses ``BasicConfig().props()`` by default.
        :param auth_password: Optional auth password for 2FA.
        """
        if experiment_data is None:
            experiment_data = BasicConfig().props()

        # Build Local Platform Object
        local_platform = LocalPlatform(as_conf.expid, 'local', experiment_data, auth_password=auth_password)
        local_platform.max_wallclock = as_conf.get_max_wallclock()
        local_platform.max_processors = as_conf.get_max_processors()
        local_platform.max_waiting_jobs = as_conf.get_max_waiting_jobs()
        local_platform.total_jobs = as_conf.get_total_jobs()
        local_platform.scratch = os.path.join(BasicConfig.LOCAL_ROOT_DIR, as_conf.expid, BasicConfig.LOCAL_TMP_DIR)
        local_platform.temp_dir = os.path.join(BasicConfig.LOCAL_ROOT_DIR, 'ASlogs')
        local_platform.root_dir = os.path.join(BasicConfig.LOCAL_ROOT_DIR, local_platform.expid)
        local_platform.host = 'localhost'
        # Add an object to entry in dictionary
        self.platforms = {
            'local': local_platform,
            'LOCAL': local_platform
        }

    def load_platforms(self, as_conf: 'AutosubmitConfig', auth_password: Optional[str] = None,
                       local_auth_password=None) -> None:
        """Create all the platform's object that will be used by the experiment."""
        exp_data: dict = as_conf.experiment_data
        platforms_used: set[str] = _get_platforms_used(
            hpcarch=as_conf.get_platform(),
            jobs_data=exp_data.get('JOBS', {})
        )
        platforms_data: dict = exp_data.get('PLATFORMS', {})
        platforms_serial_in_parallel: dict[str, list] = _get_serial_platforms(
            platforms_used=platforms_used,
            platforms_data=platforms_data
        )

        # Build Local Platform Object
        self.load_local_platform(as_conf, exp_data, local_auth_password)

        raise_message = None

        # parser is the platform's parser that represents platforms_.conf
        # Traverse sections [] considering only those included in the list of jobs
        platform_data_used = {k: v for k, v in platforms_data.items() if k.upper() in platforms_used}
        for platform_used, section_platform in platform_data_used.items():
            platform_type = section_platform.get('TYPE', '<not defined>').lower()
            platform_version = section_platform.get('VERSION', '')

            try:
                section_name = platform_used.upper()
                remote_platform = _get_platform_by_type(
                    platform_type, as_conf.expid, platform_used, exp_data, platform_version, auth_password)
                if remote_platform is None:
                    raise AutosubmitCritical(
                        f"PLATFORMS.{section_name}.TYPE: {platform_type} for {section_name} is not supported", 7012)
            except ParamikoPlatformException as e:
                # This is raised only by the ``EcPlatform`` if the underlying platform type is missing.
                Log.error(f"Queue exception: {str(e)}")
                return

            # Set the type and version of the platform found
            remote_platform.type = platform_type
            remote_platform._version = platform_version

            # Concatenating the host with a project and adding to the object
            add_project_to_host: Union[str, bool] = section_platform.get('ADD_PROJECT_TO_HOST', False)
            add_project_to_host: bool = str(add_project_to_host).lower() != "false"
            section_project = section_platform.get('PROJECT', "")
            section_host = section_platform.get('HOST', "")
            remote_platform.host = _get_host(section_host, add_project_to_host, section_project)

            # Retrieve more configurations settings and save them in the object
            remote_platform.max_wallclock = section_platform.get('MAX_WALLCLOCK', "2:00")
            remote_platform.max_processors = section_platform.get('MAX_PROCESSORS', as_conf.get_max_processors())
            other_max_waiting_jobs = section_platform.get('MAXWAITINGJOBS', as_conf.get_max_waiting_jobs())
            remote_platform.max_waiting_jobs = section_platform.get('MAX_WAITING_JOBS', other_max_waiting_jobs)
            total_jobs = section_platform.get('TOTALJOBS', as_conf.get_total_jobs())
            remote_platform.total_jobs = section_platform.get('TOTAL_JOBS', total_jobs)
            remote_platform.hyperthreading = str(section_platform.get('HYPERTHREADING', False)).lower()
            remote_platform.project = section_platform.get('PROJECT', "")
            remote_platform.budget = section_platform.get('BUDGET', "")
            remote_platform.reservation = section_platform.get('RESERVATION', "")
            remote_platform.exclusivity = section_platform.get('EXCLUSIVITY', "")
            remote_platform.user = section_platform.get('USER', "")
            remote_platform.scratch = section_platform.get('SCRATCH_DIR', "")
            remote_platform.shape = section_platform.get('SHAPE', "")
            remote_platform.project_dir = section_platform.get('SCRATCH_PROJECT_DIR', remote_platform.project)
            remote_platform.temp_dir = section_platform.get('TEMP_DIR', "")
            remote_platform._default_queue = section_platform.get('QUEUE', "")
            remote_platform._partition = section_platform.get('PARTITION', "")
            remote_platform._serial_queue = section_platform.get('SERIAL_QUEUE', "")
            remote_platform._serial_partition = section_platform.get('SERIAL_PARTITION', "")

            remote_platform.ec_queue = section_platform.get('EC_QUEUE', "hpc")

            remote_platform.processors_per_node = section_platform.get('PROCESSORS_PER_NODE', "1")
            remote_platform.custom_directives = section_platform.get('CUSTOM_DIRECTIVES', "")
            if len(remote_platform.custom_directives) > 0:
                Log.debug(f'Custom directives for {platform_used}: {remote_platform.custom_directives}')
            remote_platform.scratch_free_space = str(section_platform.get('SCRATCH_FREE_SPACE', False)).lower()
            try:
                remote_platform.root_dir = os.path.join(remote_platform.scratch, remote_platform.project,
                                                        remote_platform.user, remote_platform.expid)
                # FIXME: Why is ``update_cmds`` not in ``ParamikoPlatform``? Base classes have it defined...
                #        Probably a bug (even if harmless).
                remote_platform.update_cmds()

                self.platforms[platform_used] = remote_platform
            except Exception as e:
                raise_message = (f"Error in the definition of PLATFORM in YAML: SCRATCH_DIR, PROJECT, USER, "
                                 f"EXPID must be defined for platform {platform_used}: {str(e)}")

        for serial, platforms_with_serial_options in platforms_serial_in_parallel.items():
            for platform_used in platforms_with_serial_options:
                self.platforms[platform_used].serial_platform = self.platforms[serial]

        if raise_message:
            raise AutosubmitError(raise_message)

