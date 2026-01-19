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

"""Configuration utility code."""

import shutil
from pathlib import Path

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.config.upgrade_scripts import ini_to_yaml
from autosubmit.log.log import Log

__all__ = [
    'copy_as_config'
]


def copy_as_config(expid: str, copy_expid: str):
    target_exp_conf_path = Path(BasicConfig.LOCAL_ROOT_DIR, expid, 'conf')
    copy_exp_conf_path = Path(BasicConfig.LOCAL_ROOT_DIR, copy_expid, 'conf')
    for conf_file in copy_exp_conf_path.iterdir():
        # Copy only relevant files
        target_new_file = Path(target_exp_conf_path, conf_file.name.replace(copy_expid, expid))
        if conf_file.name.lower().endswith((".conf", ".yml", ".yaml")):
            shutil.copy(conf_file, target_new_file)
        # If ends with ``.conf`` convert it to an Autosubmit 4 YAML file.
        if conf_file.name.endswith(".conf"):
            try:
                ini_to_yaml(target_exp_conf_path, target_new_file)
            except Exception as e:
                Log.warning(f"Error converting {target_new_file} to YAML: {str(e)}")
