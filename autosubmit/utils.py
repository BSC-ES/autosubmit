# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
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

from pathlib import Path

from ruamel.yaml import YAML

from autosubmit.config.basicconfig import BasicConfig


def as_conf_default_values(autosubmit_version: str, exp_id: str, hpc: str = "", minimal_configuration: bool = False,
                           git_repo: str = "", git_branch: str = "main", git_as_conf: str = "") -> None:
    """Replace default values in as_conf files.

    :param autosubmit_version: autosubmit version
    :param exp_id: experiment id
    :param hpc: platform
    :param minimal_configuration: minimal configuration
    :param git_repo: path to project git repository
    :param git_branch: main branch
    :param git_as_conf: path to as_conf file in git repository
    :return: None
    """
    # open and replace values
    yaml = YAML(typ='rt')
    for as_conf_file in Path(BasicConfig.LOCAL_ROOT_DIR, f"{exp_id}/conf").iterdir():
        as_conf_file_name = as_conf_file.name.lower()
        if as_conf_file_name.endswith(('.yml', '.yaml')):
            with open(as_conf_file, 'r+') as file:
                yaml_data = yaml.load(file)
                if 'CONFIG' in yaml_data:
                    yaml_data['CONFIG']['AUTOSUBMIT_VERSION'] = autosubmit_version

                if 'MAIL' in yaml_data:
                    yaml_data['MAIL']['NOTIFICATIONS'] = False
                    yaml_data['MAIL']['TO'] = ""

                if 'DEFAULT' in yaml_data:
                    yaml_data['DEFAULT']['EXPID'] = exp_id
                    if hpc != "":
                        yaml_data['DEFAULT']['HPCARCH'] = hpc
                    elif not yaml_data['DEFAULT']['HPCARCH']:
                        yaml_data['DEFAULT']['HPCARCH'] = "local"

                if 'LOCAL' in yaml_data:
                    yaml_data['LOCAL']['PROJECT_PATH'] = ""

                if 'GIT' in yaml_data:
                    if git_repo != "":
                        yaml_data['GIT']['PROJECT_ORIGIN'] = f'{git_repo}'
                    if git_branch != "":
                        yaml_data['GIT']['PROJECT_BRANCH'] = f'{git_branch}'

                if minimal_configuration:
                    if 'DEFAULT' in yaml_data:
                        yaml_data['DEFAULT']['CUSTOM_CONFIG'] = f'"%PROJDIR%/{git_as_conf}"'

            yaml.dump(yaml_data, as_conf_file)
