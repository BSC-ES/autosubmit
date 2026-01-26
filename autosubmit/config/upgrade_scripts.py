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

"""Code to handle upgrading Autosubmit scripts between AS versions."""

import locale
import os
import re
import shutil
from pathlib import Path
from typing import Optional

from configobj import ConfigObj
from ruamel.yaml import YAML

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.config.yamlparser import YAMLParserFactory
from autosubmit.database.db_common import (
    update_experiment_description_version
)
from autosubmit.experiment.experiment_common import check_ownership
from autosubmit.helpers.version import get_version
from autosubmit.log.log import Log

__all__ = [
    'ini_to_yaml',
    'upgrade_scripts'
]


def ini_to_yaml(ini_file: Path) -> Path:
    root_dir = ini_file.parent
    # Read the file name from command line argument
    input_file = str(ini_file)
    backup_path = root_dir / Path(ini_file.name + "_AS_v3_backup")
    if not backup_path.exists():
        Log.info(f"Backup stored at {backup_path}")
        shutil.copyfile(ini_file, backup_path)
    # Read key=value property configs in python dictionary

    content = open(input_file, 'r', encoding=locale.getlocale()[1]).read()
    regex = r"\=( )*\[[\[\]\'_0-9.\"#A-Za-z \-,]*\]"

    matches = re.finditer(regex, content, flags=re.IGNORECASE)

    for matchNum, match in enumerate(matches, start=1):
        print(match.group())
        subs_string = "= " + "\"" + match.group()[2:] + "\""
        regex_sub = match.group()
        content = re.sub(re.escape(regex_sub), subs_string, content)

    open(input_file, 'w', encoding=locale.getlocale()[1]).write(content)
    config_dict = ConfigObj(input_file, stringify=True, list_values=False, interpolation=False, unrepr=False)

    # Store the result in yaml_dict
    yaml_dict: dict = {}

    for key, value in config_dict.items():
        config_keys = key.split(".")

        for config_key in reversed(config_keys):
            value = {config_key: value}

        yaml_dict = _update_dict(yaml_dict, value)

    final_dict = {}
    if input_file.find("platform") != -1:
        final_dict["PLATFORMS"] = yaml_dict
    elif input_file.find("job") != -1:
        final_dict["JOBS"] = yaml_dict
    else:
        final_dict = yaml_dict
    # Write resultant dictionary to the yaml file
    yaml_file_path = Path(root_dir, f'{ini_file.stem}.yml')
    with open(input_file, 'w', encoding=locale.getlocale()[1]) as yaml_file:
        yaml = YAML()
        yaml.dump(final_dict, yaml_file)
        ini_file.rename(yaml_file_path)

    return yaml_file_path


# Based on http://stackoverflow.com/a/3233356
def _update_dict(original_dict: dict, updated_dict: dict) -> dict:
    for k, v in updated_dict.items():
        if isinstance(v, dict):
            r = _update_dict(original_dict.get(k, {}), v)
            original_dict[k] = r
        else:
            original_dict[k] = updated_dict[k]
    return original_dict


def upgrade_scripts(expid: str, files: Optional[list[str]] = None) -> bool:
    """Upgrade scripts from Autosubmit 3 to 4."""

    if not files:
        files = ('*.conf', '*.CONF')

    Log.info("Checking if experiment exists...")
    check_ownership(expid, raise_error=True)
    as_conf = AutosubmitConfig(expid, BasicConfig, YAMLParserFactory())
    as_conf.reload(force_load=True)
    as_conf.check_conf_files()
    as_conf.load_parameters()

    as_version = get_version()
    Log.info(f"Changing experiment {expid} version from {as_conf.get_version()} to {as_version}")

    exp_conf_dir = Path(BasicConfig.LOCAL_ROOT_DIR) / expid / "conf"
    ini_files = {f for pattern in files for f in exp_conf_dir.rglob(pattern)}

    # TODO: Use tqdm to show the user the progress?
    Log.info(f"Converting {len(ini_files)} INI files (.conf) into YAML files (.yml)")
    yaml_files = []
    for ini_file in ini_files:
        yaml_file = Path(ini_file.stem + ".yml")
        if yaml_file.exists():
            Log.debug(f'INI file {ini_file} not upgraded. YAML file already exists: {yaml_file}')
            continue

        Log.debug(f'Converting INI file {ini_file} into YAML file: {yaml_file}')
        try:
            ini_to_yaml(ini_file)
            yaml_files.append(yaml_file)
        except Exception as e:
            Log.warning(f'Failed to convert INI file {ini_file} into {yaml_file}: {e}')

    # Update files in conf/ folder.
    exp_project_dir = Path(as_conf.basic_config.LOCAL_ROOT_DIR) / expid / "proj"
    Log.info(f"Fixing placeholder variables (%_%) inside the new {len(yaml_files)} YAML files")
    for yaml_file in yaml_files:
        template_path = exp_project_dir / Path(f).name
        try:
            w, s = _update_old_script(exp_project_dir, template_path, as_conf)
            if w != "":
                warnings += f"Warnings for: {template_path.name}\n{w}\n"
            if s != "":
                substituted += f"Variables changed for: {template_path.name}\n{s}\n"
        except Exception as e:
            Log.printlog(f"Couldn't read {template_path} template.\ntrace:{str(e)}")

    # Update files in proj/ folder.
    exp_project_dir = Path(as_conf.get_project_dir())
    template_path = Path()

    Log.info("Looking for %_% variables inside templates")
    for section, value in as_conf.jobs_data.items():
        try:
            template_path = exp_project_dir / Path(value.get("FILE", ""))
            w, s = _update_old_script(template_path.parent, template_path, as_conf)
            if w != "":
                warnings.append(f"Warnings for: {template_path.name}\n{w}\n")
            if s != "":
                substituted.append(f"Variables changed for: {template_path.name}\n{s}\n")
        except Exception as e:
            Log.printlog(f"Couldn't read {template_path} template.\ntrace:{str(e)}")

    # Commit.
    as_conf.set_version(as_version)
    update_experiment_description_version(expid, version=as_version)

    return True


def _update_old_script(
        root_dir: Path,
        template_path: Path,
        as_conf: AutosubmitConfig
) -> tuple[list[str], list[str]]:
    """Backs up the configuration and tries to update them.

    Returns a tuple with warnings, and substituted values.

    Lower case variables are replaced by upper case variables.

    :return: A tuple with a list of warnings and substituted values.
    """
    # Do a backup and tries to update
    warnings = []
    substituted = []
    Log.info(f"Checking {template_path}")
    if template_path.exists():
        backup_path = root_dir / Path(template_path.name + "_AS_v3_backup_placeholders")
        if not backup_path.exists():
            Log.info(f"Backup stored at {backup_path}")
            shutil.copyfile(template_path, backup_path)
        with open(template_path, 'r', encoding=locale.getlocale()[1]) as f:
            template_content = f.read()
        # Look for %_%
        variables = re.findall('%(?<!%%)[a-zA-Z0-9_.-]+%(?!%%)', template_content, flags=re.IGNORECASE)
        variables = [variable[1:-1].upper() for variable in variables]
        results = {}
        # Change format
        for old_format_key in variables:
            for key in as_conf.load_parameters().keys():
                key_affix = key.split(".")[-1]
                if key_affix == old_format_key:
                    if old_format_key not in results:
                        results[old_format_key] = set()

                    results[old_format_key].add("%" + key.strip("'") + "%")
        for key, new_key in results.items():
            if len(new_key) > 1:
                if list(new_key)[0].find("JOBS") > -1 or list(new_key)[0].find("PLATFORMS") > -1:
                    pass
                else:
                    warnings.append(f"{key} couldn't translate to {new_key} since it is a duplicate variable. "
                                    f"Please chose one of the keys value.")
            else:
                new_key = new_key.pop().upper()
                substituted.append(f"{key.upper()} translated to {new_key}")
                template_content = re.sub('%(?<!%%)' + key + '%(?!%%)', new_key, template_content, flags=re.I)
        # write_it
        # Deletes unused keys from confs
        if template_path.name.lower().find("autosubmit") > -1:
            template_content = re.sub('(?m)^( )*(EXPID:)( )*[a-zA-Z0-9._-]*(\n)*', "", template_content, flags=re.I)
        # Write final result
        with open(template_path, "w") as f:
            f.write(template_content)

    if not warnings and not substituted:
        Log.result(f"Completed check for {template_path}.\nNo %_% variables found.")
    else:
        Log.result(f"Completed check for {template_path}")
